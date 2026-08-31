// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type staticPeerProvider struct {
	name    string
	resolve func(context.Context, *PeerResolutionContext) (*PeerIdentityResult, error)
}

func (p staticPeerProvider) Provider() string { return p.name }
func (p staticPeerProvider) Resolve(ctx context.Context, resolution *PeerResolutionContext) (*PeerIdentityResult, error) {
	return p.resolve(ctx, resolution)
}

func testPeerIdentity(t *testing.T, provider, subject string) *PeerIdentity {
	t.Helper()
	identity, err := NewPeerIdentity(PeerIdentityOptions{
		Provider: provider, EvidenceSource: "test", Assurance: IdentityAssuranceCryptographicPeer,
		Issuer: "spiffe://example.org", Transport: "tcp", SubjectKind: PeerSubjectWorkload,
		SubjectKey: subject, SubjectStability: SubjectStabilityStable, SubjectVerified: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func TestPeerIdentityGoldenVector(t *testing.T) {
	identity := testPeerIdentity(t, "spiffe", "spiffe://example.org/workload")
	principal, err := identity.CanonicalPrincipal()
	if err != nil {
		t.Fatal(err)
	}
	const expectedPrincipal = "peer/spiffe/spiffe%3A%2F%2Fexample.org/spiffe%3A%2F%2Fexample.org%2Fworkload"
	if principal != expectedPrincipal {
		t.Fatalf("principal = %q", principal)
	}
	result, _ := NewAvailablePeerIdentityResult("spiffe", identity)
	evidence, _ := NewPeerEvidenceSet(result)
	const expectedDigest = "948ce118ddd5f212e7bfd62e13ffdba0675397c56a43060e98656965389e5367"
	if digest := evidence.BindingDigest([]string{"spiffe"}, nil); digest != expectedDigest {
		t.Fatalf("digest = %q", digest)
	}
}

func TestBindingDigestIgnoresRoutingTopology(t *testing.T) {
	withTopology := func(source, proxy string, capabilities map[string]any) *PeerIdentity {
		identity, err := NewPeerIdentity(PeerIdentityOptions{
			Provider: "spiffe", EvidenceSource: "test", Assurance: IdentityAssuranceCryptographicPeer,
			Issuer: "spiffe://example.org", Transport: "tcp", SubjectKind: PeerSubjectWorkload,
			SubjectKey: "spiffe://example.org/workload", SubjectStability: SubjectStabilityStable,
			SubjectVerified: true, SourceAddress: source, ProxyAddress: proxy, Capabilities: capabilities,
		})
		if err != nil {
			t.Fatal(err)
		}
		return identity
	}
	digest := func(identity *PeerIdentity) string {
		result, _ := NewAvailablePeerIdentityResult("spiffe", identity)
		evidence, _ := NewPeerEvidenceSet(result)
		return evidence.BindingDigest([]string{"spiffe"}, nil)
	}
	first := digest(withTopology("100.64.0.1:40001", "10.0.0.10", nil))
	second := digest(withTopology("100.64.0.1:49999", "10.0.0.11", nil))
	if first != second {
		t.Fatal("routing topology changed authorization binding")
	}
	changed := digest(withTopology("100.64.0.1:49999", "10.0.0.11", map[string]any{"query.farm/run": []any{}}))
	if first == changed {
		t.Fatal("capability change did not change authorization binding")
	}
}

func TestPeerEvidenceSnapshotsStructuredValues(t *testing.T) {
	attributes := map[string]any{"roles": []any{"reader"}}
	identity, err := NewPeerIdentity(PeerIdentityOptions{
		Provider: "test", EvidenceSource: "test", Assurance: IdentityAssuranceLocalDaemon,
		Issuer: "test://issuer", Transport: "tcp", Attributes: attributes,
	})
	if err != nil {
		t.Fatal(err)
	}
	attributes["roles"].([]any)[0] = "writer"
	if got := identity.Attributes()["roles"].([]any)[0]; got != "reader" {
		t.Fatalf("snapshotted role = %v", got)
	}
}

func TestAnyOfSkipsUnavailableAndSubjectlessProviders(t *testing.T) {
	unavailable, _ := NewPeerIdentityResult("first", PeerIdentityUnavailable)
	subjectless, err := NewPeerIdentity(PeerIdentityOptions{
		Provider: "capabilities", EvidenceSource: "test", Assurance: IdentityAssuranceLocalDaemon,
		Issuer: "test://issuer", Transport: "tcp",
	})
	if err != nil {
		t.Fatal(err)
	}
	subjectlessResult, _ := NewAvailablePeerIdentityResult("capabilities", subjectless)
	stable := testPeerIdentity(t, "second", "spiffe://example.org/workload")
	stableResult, _ := NewAvailablePeerIdentityResult("second", stable)
	evidence, _ := NewPeerEvidenceSet(unavailable, subjectlessResult, stableResult)
	policy, _ := AnyOfPeerIdentities("first", "capabilities", "second")
	auth, err := policy(evidence, Anonymous())
	if err != nil {
		t.Fatal(err)
	}
	if !auth.Authenticated || auth.Domain != "second" {
		t.Fatalf("auth = %#v", auth)
	}
}

func TestAnyOfRejectsAmbiguityBeforeApplicationFallback(t *testing.T) {
	first := testPeerIdentity(t, "spiffe", "spiffe://example.org/one")
	second := testPeerIdentity(t, "spiffe", "spiffe://example.org/two")
	result, _ := NewAvailablePeerIdentityResult("spiffe", first, second)
	evidence, _ := NewPeerEvidenceSet(result)
	policy, _ := AnyOfPeerIdentities("spiffe")
	if _, err := policy(evidence, &AuthContext{Domain: "bearer", Authenticated: true, Principal: "alice"}); err == nil {
		t.Fatal("ambiguous peer evidence was accepted")
	}
}

func TestAllOfBindingIncludesApplicationIdentity(t *testing.T) {
	identity := testPeerIdentity(t, "spiffe", "spiffe://example.org/workload")
	result, _ := NewAvailablePeerIdentityResult("spiffe", identity)
	evidence, _ := NewPeerEvidenceSet(result)
	policy, err := AllOfPeerIdentities([]string{"spiffe"}, "", func(*AuthContext, map[string]*PeerIdentity) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	alice, _ := policy(evidence, &AuthContext{Domain: "bearer", Authenticated: true, Principal: "alice"})
	bob, _ := policy(evidence, &AuthContext{Domain: "bearer", Authenticated: true, Principal: "bob"})
	if alice.Claims["peer_evidence_binding"] == bob.Claims["peer_evidence_binding"] {
		t.Fatal("application principal was not bound")
	}
}

func TestPeerEvidenceBindingChangesAllStatefulIdentities(t *testing.T) {
	identity := testPeerIdentity(t, "spiffe", "spiffe://example.org/workload")
	result, _ := NewAvailablePeerIdentityResult("spiffe", identity)
	evidence, _ := NewPeerEvidenceSet(result)
	bound, err := RequirePeerIdentity("spiffe")(evidence, Anonymous())
	if err != nil {
		t.Fatal(err)
	}
	if string(stateTokenAad(bound)) == string(stateTokenAad(Anonymous())) {
		t.Fatal("cursor AAD did not bind peer evidence")
	}
	if string(callTokenAad(bound)) == string(callTokenAad(Anonymous())) {
		t.Fatal("call AAD did not bind peer evidence")
	}
	if principalKeyFromAuth(bound) == principalKeyFromAuth(Anonymous()) {
		t.Fatal("sticky/cache key did not bind peer evidence")
	}
}

func TestRequireAcceptsCapabilityOnlyEvidence(t *testing.T) {
	identity, err := NewPeerIdentity(PeerIdentityOptions{
		Provider: "tailscale", EvidenceSource: "serve", Assurance: IdentityAssuranceConfiguredProxy,
		Issuer: "tailnet:test", Transport: "http", CapabilitiesVerified: true,
		Capabilities: map[string]any{"query.farm/can-run": []any{map[string]any{"worker": "analytics"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := NewAvailablePeerIdentityResult("tailscale", identity)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := NewPeerEvidenceSet(result)
	if err != nil {
		t.Fatal(err)
	}
	auth := &AuthContext{Domain: "bearer", Authenticated: true, Principal: "alice"}
	bound, err := RequirePeerIdentity("tailscale")(evidence, auth)
	if err != nil {
		t.Fatal(err)
	}
	if !bound.Authenticated || bound.Principal != "alice" {
		t.Fatalf("require changed application identity: %#v", bound)
	}
	if _, err := PeerIdentityPrimary("tailscale")(evidence, Anonymous()); err == nil {
		t.Fatal("primary accepted capability-only evidence")
	}
}

func TestPeerIdentityRejectsUnknownEnumValues(t *testing.T) {
	if _, err := NewPeerIdentityResult("spiffe", PeerIdentityStatus("invald")); err == nil {
		t.Fatal("unknown provider status was accepted")
	}
	if _, err := NewPeerIdentity(PeerIdentityOptions{
		Provider: "spiffe", EvidenceSource: "test", Assurance: IdentityAssurance("maybe"),
		Issuer: "spiffe://example.org", Transport: "tcp",
	}); err == nil {
		t.Fatal("unknown assurance was accepted")
	}
}

func TestPeerIdentityRejectsInvalidUnicodeAndJSON(t *testing.T) {
	invalidUTF8 := string([]byte{0xff})
	if _, err := NewPeerIdentity(PeerIdentityOptions{
		Provider: "spiffe", EvidenceSource: "test", Assurance: IdentityAssuranceCryptographicPeer,
		Issuer: "spiffe://example.org", Transport: "tcp", SubjectKey: invalidUTF8,
	}); err == nil {
		t.Fatal("invalid UTF-8 subject was accepted")
	}
	if _, err := NewPeerIdentity(PeerIdentityOptions{
		Provider: "spiffe", EvidenceSource: "test", Assurance: IdentityAssuranceCryptographicPeer,
		Issuer: "spiffe://example.org", Transport: "tcp", Attributes: map[string]any{"score": math.NaN()},
	}); err == nil {
		t.Fatal("non-finite JSON number was accepted")
	}
	if _, err := NewPeerResolutionContext("http", PeerResolutionOptions{
		Headers: map[string][]string{"X-Identity": {invalidUTF8}},
	}); err == nil {
		t.Fatal("invalid UTF-8 header was accepted")
	}
}

func TestPeerIdentityJSONDepthAndDetachedRead(t *testing.T) {
	root := map[string]any{}
	cursor := root
	for i := 0; i < 17; i++ {
		next := map[string]any{}
		cursor["next"] = next
		cursor = next
	}
	if _, err := NewPeerIdentity(PeerIdentityOptions{
		Provider: "test", EvidenceSource: "test", Assurance: IdentityAssuranceLocalDaemon,
		Issuer: "test://issuer", Transport: "tcp", Attributes: root,
	}); err == nil {
		t.Fatal("over-depth JSON evidence was accepted")
	}

	identity := testPeerIdentity(t, "spiffe", "spiffe://example.org/雪")
	first := identity.Attributes()
	first["mutated"] = true
	if _, exists := identity.Attributes()["mutated"]; exists {
		t.Fatal("identity attributes accessor leaked mutable storage")
	}
}

func TestHTTPPeerIdentityProvidersResolveConcurrently(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	provider := func(name string) staticPeerProvider {
		result, err := NewPeerIdentityResult(name, PeerIdentityNoMatch)
		if err != nil {
			t.Fatal(err)
		}
		return staticPeerProvider{name: name, resolve: func(_ context.Context, _ *PeerResolutionContext) (*PeerIdentityResult, error) {
			started <- name
			<-release
			return result, nil
		}}
	}
	h := NewHttpServer(NewServer())
	h.SetPeerIdentityProviders(provider("first"), provider("second"))
	h.SetPeerResolutionTimeout(time.Second)
	done := make(chan *requestIdentity, 1)
	go func() {
		done <- h.authenticateIdentity(
			httptest.NewRecorder(),
			httptest.NewRequest(http.MethodPost, "http://worker.test/whoami", nil),
		)
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(250 * time.Millisecond):
			close(release)
			t.Fatal("providers did not start concurrently")
		}
	}
	close(release)
	select {
	case resolved := <-done:
		if resolved == nil || resolved.evidence.Status("first") != PeerIdentityNoMatch ||
			resolved.evidence.Status("second") != PeerIdentityNoMatch {
			t.Fatalf("resolved evidence = %#v", resolved)
		}
	case <-time.After(time.Second):
		t.Fatal("concurrent provider resolution did not complete")
	}
}

func TestHTTPPeerIdentityResolvesAuthorityAndAuthenticates(t *testing.T) {
	identity := testPeerIdentity(t, "spiffe", "spiffe://example.org/workload")
	result, _ := NewAvailablePeerIdentityResult("spiffe", identity)
	provider := staticPeerProvider{name: "spiffe", resolve: func(_ context.Context, resolution *PeerResolutionContext) (*PeerIdentityResult, error) {
		if resolution.Authority() != "worker.example.test" {
			t.Fatalf("authority = %q", resolution.Authority())
		}
		if resolution.DestinationAddress() != "" {
			t.Fatalf("untrusted Host became destination = %q", resolution.DestinationAddress())
		}
		if resolution.ImmediatePeer() != "192.0.2.10" {
			t.Fatalf("immediate peer = %q", resolution.ImmediatePeer())
		}
		return result, nil
	}}
	h := NewHttpServer(NewServer())
	h.SetAuthenticate(func(*http.Request) (*AuthContext, error) {
		return nil, NewAuthFailure(AuthReasonMissingCredential, "bearer required")
	})
	h.SetPeerIdentityProviders(provider)
	h.SetPeerAuthenticationPolicy(PeerIdentityPrimary("spiffe"))
	req := httptest.NewRequest(http.MethodPost, "http://worker.example.test/whoami", nil)
	req.RemoteAddr = "192.0.2.10:1234"
	recorder := httptest.NewRecorder()
	resolved := h.authenticateIdentity(recorder, req)
	if resolved == nil || !resolved.auth.Authenticated || len(resolved.evidence.Identities()) != 1 {
		t.Fatalf("resolved identity = %#v, response = %d", resolved, recorder.Code)
	}
}

func TestObservationCannotBypassRequiredApplicationAuth(t *testing.T) {
	h := NewHttpServer(NewServer())
	h.SetAuthenticate(func(*http.Request) (*AuthContext, error) {
		return nil, NewAuthFailure(AuthReasonMissingCredential, "bearer required")
	})
	h.SetPeerAuthenticationPolicy(ObservePeerIdentity)
	recorder := httptest.NewRecorder()
	resolved := h.authenticateIdentity(recorder, httptest.NewRequest(http.MethodPost, "http://worker.test/whoami", nil))
	if resolved != nil || recorder.Code != http.StatusUnauthorized {
		t.Fatalf("observation bypassed required auth: resolved=%#v status=%d", resolved, recorder.Code)
	}
}

func TestHTTPProviderTimeoutIsEvidenceUnavailableForComposition(t *testing.T) {
	validApplication := func(*http.Request) (*AuthContext, error) {
		return &AuthContext{Domain: "bearer", Authenticated: true, Principal: "alice"}, nil
	}
	slowProvider := staticPeerProvider{name: "slow", resolve: func(context.Context, *PeerResolutionContext) (*PeerIdentityResult, error) {
		time.Sleep(100 * time.Millisecond) // deliberately ignores cancellation
		return NewPeerIdentityResult("slow", PeerIdentityNoMatch)
	}}
	anyOf, err := AnyOfPeerIdentities("slow")
	if err != nil {
		t.Fatal(err)
	}
	for name, policy := range map[string]PeerAuthenticationPolicy{
		"observe": ObservePeerIdentity,
		"any_of":  anyOf,
	} {
		t.Run(name, func(t *testing.T) {
			h := NewHttpServer(NewServer())
			h.SetAuthenticate(validApplication)
			h.SetPeerIdentityProviders(slowProvider)
			h.SetPeerAuthenticationPolicy(policy)
			h.SetPeerResolutionTimeout(10 * time.Millisecond)
			recorder := httptest.NewRecorder()
			resolved := h.authenticateIdentity(recorder, httptest.NewRequest(http.MethodPost, "http://worker.test/whoami", nil))
			if resolved == nil || !resolved.auth.Authenticated || resolved.auth.Principal != "alice" {
				t.Fatalf("valid application auth did not survive provider timeout: resolved=%#v status=%d", resolved, recorder.Code)
			}
			if got := resolved.evidence.Status("slow"); got != PeerIdentityUnavailable {
				t.Fatalf("slow provider status = %q", got)
			}
		})
	}
}

func TestHTTPProviderUnavailableFailsRequiredPolicies(t *testing.T) {
	provider := staticPeerProvider{name: "slow", resolve: func(context.Context, *PeerResolutionContext) (*PeerIdentityResult, error) {
		return nil, NewAuthUnavailable("identity authority restarting")
	}}
	for name, policy := range map[string]PeerAuthenticationPolicy{
		"require": RequirePeerIdentity("slow"),
		"primary": PeerIdentityPrimary("slow"),
	} {
		t.Run(name, func(t *testing.T) {
			h := NewHttpServer(NewServer())
			h.SetPeerIdentityProviders(provider)
			h.SetPeerAuthenticationPolicy(policy)
			recorder := httptest.NewRecorder()
			if resolved := h.authenticateIdentity(recorder, httptest.NewRequest(http.MethodPost, "http://worker.test/whoami", nil)); resolved != nil {
				t.Fatalf("unavailable provider satisfied %s: %#v", name, resolved)
			}
			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d", recorder.Code)
			}
		})
	}
}

func TestHTTPProviderCapacityIsUnavailableWithoutAuthDowngrade(t *testing.T) {
	var calls atomic.Int32
	provider := staticPeerProvider{name: "slow", resolve: func(context.Context, *PeerResolutionContext) (*PeerIdentityResult, error) {
		calls.Add(1)
		time.Sleep(100 * time.Millisecond) // retain the only permit after timeout
		return NewPeerIdentityResult("slow", PeerIdentityNoMatch)
	}}
	policy, err := AnyOfPeerIdentities("slow")
	if err != nil {
		t.Fatal(err)
	}
	h := NewHttpServer(NewServer())
	h.SetAuthenticate(func(*http.Request) (*AuthContext, error) {
		return &AuthContext{Domain: "bearer", Authenticated: true, Principal: "alice"}, nil
	})
	h.SetPeerIdentityProviders(provider)
	h.SetPeerProviderConcurrency(1)
	h.SetPeerResolutionTimeout(10 * time.Millisecond)
	h.SetPeerAuthenticationPolicy(policy)

	for request := 0; request < 2; request++ {
		recorder := httptest.NewRecorder()
		resolved := h.authenticateIdentity(recorder, httptest.NewRequest(http.MethodPost, "http://worker.test/whoami", nil))
		if resolved == nil || resolved.evidence.Status("slow") != PeerIdentityUnavailable {
			t.Fatalf("request %d did not preserve application auth with unavailable evidence: %#v", request, resolved)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("capacity-exhausted request launched provider: calls=%d", calls.Load())
	}
}

func TestHTTPInvalidApplicationCredentialNeverFallsBackToPeerProvider(t *testing.T) {
	var calls atomic.Int32
	provider := staticPeerProvider{name: "peer", resolve: func(context.Context, *PeerResolutionContext) (*PeerIdentityResult, error) {
		calls.Add(1)
		return NewPeerIdentityResult("peer", PeerIdentityNoMatch)
	}}
	policy, err := AnyOfPeerIdentities("peer")
	if err != nil {
		t.Fatal(err)
	}
	h := NewHttpServer(NewServer())
	h.SetAuthenticate(func(*http.Request) (*AuthContext, error) {
		return nil, NewAuthFailure(AuthReasonInvalidCredential, "bad bearer")
	})
	h.SetPeerIdentityProviders(provider)
	h.SetPeerAuthenticationPolicy(policy)
	recorder := httptest.NewRecorder()
	if resolved := h.authenticateIdentity(recorder, httptest.NewRequest(http.MethodPost, "http://worker.test/whoami", nil)); resolved != nil {
		t.Fatalf("invalid application credential fell back: %#v", resolved)
	}
	if recorder.Code != http.StatusUnauthorized || calls.Load() != 0 {
		t.Fatalf("status=%d provider calls=%d", recorder.Code, calls.Load())
	}
}

func TestHTTPPeerProviderErrorDetailIsRedactedBeforeLogging(t *testing.T) {
	const secret = "secret-localapi-token-and-capability"
	for name, original := range map[string]error{
		"runtime": fmt.Errorf("provider failed with %s", secret),
		"reject":  NewAuthFailure(AuthReasonInvalidCredential, secret),
	} {
		t.Run(name, func(t *testing.T) {
			safe := redactedPeerProviderError(original)
			if strings.Contains(safe.Error(), secret) {
				t.Fatalf("provider-controlled detail survived redaction: %v", safe)
			}
		})
	}
}

func TestHTTPPeerExtensionPanicsAreContainedAndRedacted(t *testing.T) {
	const secret = "secret-provider-policy-panic-value"
	previousLogger := slog.Default()
	var logs bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(previousLogger)

	tests := []struct {
		name      string
		configure func(*HttpServer)
	}{
		{
			name: "provider",
			configure: func(h *HttpServer) {
				h.SetPeerIdentityProviders(staticPeerProvider{
					name: "panic-provider",
					resolve: func(context.Context, *PeerResolutionContext) (*PeerIdentityResult, error) {
						panic(secret)
					},
				})
			},
		},
		{
			name: "policy",
			configure: func(h *HttpServer) {
				h.SetPeerAuthenticationPolicy(func(*PeerEvidenceSet, *AuthContext) (*AuthContext, error) {
					panic(secret)
				})
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			logs.Reset()
			h := NewHttpServer(NewServer())
			testCase.configure(h)
			recorder := httptest.NewRecorder()
			if resolved := h.authenticateIdentity(recorder, httptest.NewRequest(http.MethodPost, "http://worker.test/whoami", nil)); resolved != nil {
				t.Fatalf("panicking extension was accepted: %#v", resolved)
			}
			if recorder.Code != http.StatusInternalServerError {
				t.Fatalf("status=%d want=%d", recorder.Code, http.StatusInternalServerError)
			}
			if strings.Contains(logs.String(), secret) || strings.Contains(recorder.Body.String(), secret) {
				t.Fatalf("panic value escaped redaction: log=%q body=%q", logs.String(), recorder.Body.String())
			}
		})
	}
}

func TestHTTPPeerPolicyErrorsAreClassifiedAndRedacted(t *testing.T) {
	const secret = "secret-policy-capability-certificate"
	previousLogger := slog.Default()
	var logs bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(previousLogger)

	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantReason AuthReason
		wantRetry  string
	}{
		{"unavailable", &AuthUnavailableError{Detail: secret, RetryAfter: 17}, http.StatusServiceUnavailable, "", "17"},
		{"rejected", NewAuthFailure(AuthReasonInsufficientScope, secret), http.StatusUnauthorized, AuthReasonInsufficientScope, ""},
		{"generic", fmt.Errorf("policy failed with %s", secret), http.StatusInternalServerError, "", ""},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			logs.Reset()
			h := NewHttpServer(NewServer())
			h.SetPeerAuthenticationPolicy(func(*PeerEvidenceSet, *AuthContext) (*AuthContext, error) {
				return nil, testCase.err
			})
			recorder := httptest.NewRecorder()
			if resolved := h.authenticateIdentity(recorder, httptest.NewRequest(http.MethodPost, "http://worker.test/whoami", nil)); resolved != nil {
				t.Fatalf("policy error was accepted: %#v", resolved)
			}
			if recorder.Code != testCase.wantStatus {
				t.Fatalf("status=%d want=%d", recorder.Code, testCase.wantStatus)
			}
			if got := recorder.Header().Get(HeaderAuthReason); got != string(testCase.wantReason) {
				t.Fatalf("reason=%q want=%q", got, testCase.wantReason)
			}
			if got := recorder.Header().Get("Retry-After"); got != testCase.wantRetry {
				t.Fatalf("Retry-After=%q want=%q", got, testCase.wantRetry)
			}
			if strings.Contains(logs.String(), secret) || strings.Contains(recorder.Body.String(), secret) {
				t.Fatalf("policy-controlled detail escaped redaction: log=%q body=%q", logs.String(), recorder.Body.String())
			}
		})
	}
}

func TestHTTPApplicationAuthLogsNeverContainCallbackDetail(t *testing.T) {
	const secret = "secret-application-token-verifier-state"
	previousLogger := slog.Default()
	var logs bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(previousLogger)

	for _, testCase := range []struct {
		name             string
		err              error
		wantStatus       int
		wantPublicDetail bool
	}{
		{"unavailable", &AuthUnavailableError{Detail: secret, RetryAfter: 11}, http.StatusServiceUnavailable, false},
		{"generic", fmt.Errorf("authenticator failed with %s", secret), http.StatusInternalServerError, false},
		// AuthFailure.Detail is explicitly public response text. It remains on
		// the wire for compatibility, but must still never enter normal logs.
		{"classified_public_rejection", NewAuthFailure(AuthReasonInvalidCredential, secret), http.StatusUnauthorized, true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			logs.Reset()
			h := NewHttpServer(NewServer())
			h.SetAuthenticate(func(*http.Request) (*AuthContext, error) { return nil, testCase.err })
			recorder := httptest.NewRecorder()
			if resolved := h.authenticateIdentity(recorder, httptest.NewRequest(http.MethodPost, "http://worker.test/whoami", nil)); resolved != nil {
				t.Fatalf("application auth error was accepted: %#v", resolved)
			}
			if recorder.Code != testCase.wantStatus {
				t.Fatalf("status=%d want=%d", recorder.Code, testCase.wantStatus)
			}
			if strings.Contains(logs.String(), secret) {
				t.Fatalf("application callback detail reached logs: %q", logs.String())
			}
			if got := strings.Contains(recorder.Body.String(), secret); got != testCase.wantPublicDetail {
				t.Fatalf("public detail presence=%v want=%v body=%q", got, testCase.wantPublicDetail, recorder.Body.String())
			}
		})
	}
}
