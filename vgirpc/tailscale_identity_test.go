// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func tailscaleTestResolution(t *testing.T, options PeerResolutionOptions) *PeerResolutionContext {
	t.Helper()
	resolution, err := NewPeerResolutionContext("http", options)
	if err != nil {
		t.Fatal(err)
	}
	return resolution
}

func tailscaleTestWhoIs(tagged bool) []byte {
	tags := []string{}
	if tagged {
		tags = []string{"tag:batch-worker"}
	}
	value := map[string]any{
		"Node": map[string]any{
			"StableID": "n123CNTRL", "Name": "client.example.ts.net.", "Tags": tags,
		},
		"UserProfile": map[string]any{
			"ID": 123, "LoginName": "alice@example.com", "DisplayName": "Alice Architect",
		},
		"CapMap": map[string]any{"example.com/cap/run": []any{map[string]any{"queue": "blue"}}},
	}
	body, _ := json.Marshal(value)
	return body
}

func TestTailscaleServeUserLoginIsNotStable(t *testing.T) {
	provider, err := NewTailscaleServeIdentityProvider(TailscaleServeOptions{
		Issuer: "tailnet:example", TrustedProxyAddresses: []string{"127.0.0.1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := provider.Resolve(context.Background(), tailscaleTestResolution(t, PeerResolutionOptions{
		ImmediatePeer: "::ffff:127.0.0.1",
		Headers: map[string][]string{
			"Tailscale-User-Login": {"alice@example.com"},
			"Tailscale-User-Name":  {"=?utf-8?q?Ferris_B=C3=BCller?="},
		},
	}))
	if err != nil || result.Status() != PeerIdentityAvailable {
		t.Fatalf("Resolve() = (%v, %v)", result, err)
	}
	identity := result.Identities()[0]
	if identity.SubjectKind() != PeerSubjectUser || identity.SubjectKey() != "login:alice@example.com" ||
		identity.SubjectStability() != SubjectStabilityLogin || !identity.SubjectVerified() {
		t.Fatalf("unexpected user identity: %#v", identity)
	}
	if got := identity.Attributes()["user_display_name"]; got != "Ferris Büller" {
		t.Fatalf("display name = %#v", got)
	}
	evidence, _ := NewPeerEvidenceSet(result)
	if _, err := PeerIdentityPrimary(tailscaleProvider)(evidence, Anonymous()); err == nil {
		t.Fatal("Serve login unexpectedly eligible for stable-subject primary authentication")
	}
}

func TestTailscaleServeCapabilityOnlyAndFunnel(t *testing.T) {
	provider, err := NewTailscaleServeIdentityProvider(TailscaleServeOptions{
		Issuer: "tailnet:example", TrustedProxyAddresses: []string{"127.0.0.1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	capability := mime.QEncoding.Encode("utf-8", `{"example.com/cap/monitoring":[{"role":"🐿️"}]}`)
	result, err := provider.Resolve(context.Background(), tailscaleTestResolution(t, PeerResolutionOptions{
		ImmediatePeer: "127.0.0.1",
		Headers:       map[string][]string{"Tailscale-App-Capabilities": {capability}},
	}))
	if err != nil || result.Status() != PeerIdentityAvailable {
		t.Fatalf("capability Resolve() = (%v, %v)", result, err)
	}
	identity := result.Identities()[0]
	if identity.SubjectKind() != PeerSubjectUnknown || identity.SubjectKey() != "" || !identity.CapabilitiesVerified() {
		t.Fatalf("unexpected capability-only identity: %#v", identity)
	}
	entries := identity.Capabilities()["example.com/cap/monitoring"].([]any)
	if entries[0].(map[string]any)["role"] != "🐿️" {
		t.Fatalf("capability = %#v", entries)
	}
	funnel, err := provider.Resolve(context.Background(), tailscaleTestResolution(t, PeerResolutionOptions{
		ImmediatePeer: "127.0.0.1",
		Headers: map[string][]string{
			"Tailscale-Funnel-Request": {"?1"}, "Tailscale-User-Login": {"alice@example.com"},
		},
	}))
	if err != nil || funnel.Status() != PeerIdentityNotApplicable {
		t.Fatalf("Funnel Resolve() = (%v, %v)", funnel, err)
	}
}

func TestTailscaleServeRejectsUntrustedAndMalformedEvidence(t *testing.T) {
	provider, err := NewTailscaleServeIdentityProvider(TailscaleServeOptions{
		Issuer: "tailnet:example", TrustedProxyAddresses: []string{"127.0.0.1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	untrusted, err := provider.Resolve(context.Background(), tailscaleTestResolution(t, PeerResolutionOptions{
		ImmediatePeer: "127.0.0.2", Headers: map[string][]string{"Tailscale-User-Login": {"admin@example.com"}},
	}))
	if err != nil || untrusted.Status() != PeerIdentityUntrustedProxy {
		t.Fatalf("untrusted Resolve() = (%v, %v)", untrusted, err)
	}

	tests := []map[string][]string{
		{"Tailscale-User-Login": {"one@example.com", "two@example.com"}},
		{"Tailscale-User-Login": {"=?utf-8?b?YWxpY2U=?="}},
		{"Tailscale-User-Login": {"alice\x1f@example.com"}},
		{"Tailscale-User-Login": {"alice@example.com"}, "Tailscale-User-Name": {"=?utf-8?q?Alice=7FAdmin?="}},
		{"Tailscale-User-Name": {"Alice"}},
		{"Tailscale-App-Capabilities": {`{"example.com/cap/run":[],"example.com/cap/run":[]}`}},
		{"Tailscale-App-Capabilities": {`{"example.com/cap/run":["admin"]}`}},
		{"Tailscale-Funnel-Request": {"true"}},
	}
	for index, headers := range tests {
		result, err := provider.Resolve(context.Background(), tailscaleTestResolution(t, PeerResolutionOptions{
			ImmediatePeer: "127.0.0.1", Headers: headers,
		}))
		if err != nil || result.Status() != PeerIdentityInvalid {
			t.Errorf("case %d Resolve() = (%v, %v)", index, result, err)
		}
	}
}

func TestTailscaleLocalAPIUserServiceScopeAndNoCache(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		if request.Host != tailscaleLocalAPIHost {
			t.Errorf("Host = %q", request.Host)
		}
		username, password, ok := request.BasicAuth()
		if !ok || username != "" || password != "secret" {
			t.Errorf("BasicAuth() = (%q, %q, %v)", username, password, ok)
		}
		if got := request.URL.Query(); got.Get("addr") != "100.64.0.10:4242" || got.Get("proto") != "tcp" ||
			got.Get("svc_name") != "svc:analytics" || got.Has("dst_ip") {
			t.Errorf("query = %v", got)
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write(tailscaleTestWhoIs(false))
	}))
	defer server.Close()
	provider, err := NewTailscaleLocalAPIIdentityProvider(TailscaleLocalAPIOptions{
		Issuer: "tailnet:example", Endpoint: server.URL, Password: "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	if provider.transport.Proxy != nil {
		t.Fatal("LocalAPI transport unexpectedly honors a proxy function")
	}
	resolution := tailscaleTestResolution(t, PeerResolutionOptions{
		ImmediatePeer: "100.64.0.10", SourceEndpoint: "100.64.0.10:4242",
		DestinationAddress: "192.0.2.20:9400", ServiceName: "svc:analytics",
	})
	first, err := provider.Resolve(context.Background(), resolution)
	if err != nil {
		t.Fatal(err)
	}
	second, err := provider.Resolve(context.Background(), resolution)
	if err != nil || second.Status() != PeerIdentityAvailable || requests.Load() != 2 {
		t.Fatalf("second Resolve/no-cache = (%v, %v), requests=%d", second, err, requests.Load())
	}
	identity := first.Identities()[0]
	if identity.SubjectKey() != "user:123" || identity.SubjectStability() != SubjectStabilityStable {
		t.Fatalf("identity subject = %q/%q", identity.SubjectKey(), identity.SubjectStability())
	}
	if identity.SourceAddress() != "100.64.0.10" {
		t.Fatalf("identity source address = %q", identity.SourceAddress())
	}
	target := identity.Attributes()["capability_target"].(map[string]any)
	if target["kind"] != "service" || target["value"] != "svc:analytics" {
		t.Fatalf("capability target = %#v", target)
	}
}

func TestTailscaleLocalAPIUnixTaggedNode(t *testing.T) {
	path := filepath.Join(os.TempDir(), fmt.Sprintf("vgi-ts-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(path) })
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Query().Get("dst_ip") != "2001:db8::8" {
			t.Errorf("query = %v", request.URL.Query())
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write(tailscaleTestWhoIs(true))
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	provider, err := NewTailscaleLocalAPIIdentityProvider(TailscaleLocalAPIOptions{
		Issuer: "tailnet:example", UnixSocket: path,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := provider.Resolve(context.Background(), tailscaleTestResolution(t, PeerResolutionOptions{
		ImmediatePeer: "127.0.0.1", AssertedPeer: "100.64.0.10:4242", DestinationAddress: "[2001:db8::8]:443",
	}))
	if err != nil || result.Status() != PeerIdentityAvailable {
		t.Fatalf("Resolve() = (%v, %v)", result, err)
	}
	identity := result.Identities()[0]
	if identity.SubjectKind() != PeerSubjectTaggedNode || identity.SubjectKey() != "node:n123CNTRL" {
		t.Fatalf("tagged identity = %q/%q", identity.SubjectKind(), identity.SubjectKey())
	}
	if _, found := identity.Attributes()["user_id"]; found {
		t.Fatal("tagged node incorrectly retained UserProfile as caller")
	}
}

func TestTailscaleLocalAPIStatusOutcomes(t *testing.T) {
	for status, expected := range map[int]PeerIdentityStatus{
		http.StatusForbidden:          PeerIdentityPermissionDenied,
		http.StatusNotFound:           PeerIdentityNoMatch,
		http.StatusBadRequest:         PeerIdentityInvalid,
		http.StatusServiceUnavailable: PeerIdentityUnavailable,
	} {
		t.Run(fmt.Sprintf("status-%d", status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				response.Header().Set("Content-Type", "application/json")
				response.WriteHeader(status)
				_, _ = response.Write([]byte(`{}`))
			}))
			defer server.Close()
			provider, err := NewTailscaleLocalAPIIdentityProvider(TailscaleLocalAPIOptions{Issuer: "tailnet:example", Endpoint: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			result, err := provider.Resolve(context.Background(), tailscaleTestResolution(t, PeerResolutionOptions{ImmediatePeer: "100.64.0.1:1"}))
			if err != nil || result.Status() != expected {
				t.Fatalf("Resolve() = (%v, %v), want %s", result, err, expected)
			}
		})
	}
}

func TestTailscaleLocalAPIRejectsAdversarialJSON(t *testing.T) {
	bodies := [][]byte{
		[]byte(`{"Node":{},"Node":{},"UserProfile":{"ID":1}}`),
		[]byte(`{"Node":{"Tags":[]},"UserProfile":{"ID":1},"CapMap":[]}`),
		[]byte(`{"Node":{"StableID":"bad\ud800","Tags":["tag:worker"]},"CapMap":{}}`),
		[]byte(`{"Node":{"Tags":[]},"UserProfile":{"ID":1},"CapMap":{"example.com/cap/run":[NaN]}}`),
		append([]byte(`{"Node":{"Tags":[]},"UserProfile":{"ID":1},"CapMap":{"bad`), append([]byte{0xff}, []byte(`":[]}}`)...)...),
		[]byte(`not-json`),
	}
	for index, body := range bodies {
		t.Run(fmt.Sprintf("case-%d", index), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				response.Header().Set("Content-Type", "application/json")
				_, _ = response.Write(body)
			}))
			defer server.Close()
			provider, _ := NewTailscaleLocalAPIIdentityProvider(TailscaleLocalAPIOptions{Issuer: "tailnet:example", Endpoint: server.URL})
			result, err := provider.Resolve(context.Background(), tailscaleTestResolution(t, PeerResolutionOptions{ImmediatePeer: "100.64.0.1:1"}))
			if err != nil || result.Status() != PeerIdentityInvalid {
				t.Fatalf("Resolve() = (%v, %v)", result, err)
			}
		})
	}
}

func TestTailscaleJSONDepthCountAndUnicodeBounds(t *testing.T) {
	tooDeep := []byte(strings.Repeat("[", 18) + "0" + strings.Repeat("]", 18))
	tooMany := []byte("[" + strings.Repeat("null,", 4096) + "null]")
	for name, body := range map[string][]byte{
		"depth":               tooDeep,
		"count":               tooMany,
		"lone-high-surrogate": []byte(`{"value":"\ud800"}`),
		"lone-low-surrogate":  []byte(`{"value":"\udc00"}`),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeTailscaleJSON(body, 65_536); err == nil {
				t.Fatal("adversarial JSON unexpectedly accepted")
			}
		})
	}
	value, err := decodeTailscaleJSON([]byte(`{"value":"\ud83d\udc3f"}`), 65_536)
	if err != nil || value.(map[string]any)["value"] != "🐿" {
		t.Fatalf("valid surrogate pair = (%#v, %v)", value, err)
	}
}

func TestTailscaleLocalAPIRejectsDuplicateContentType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Add("Content-Type", "application/json")
		response.Header().Add("Content-Type", "application/json")
		_, _ = response.Write(tailscaleTestWhoIs(false))
	}))
	defer server.Close()
	provider, _ := NewTailscaleLocalAPIIdentityProvider(TailscaleLocalAPIOptions{Issuer: "tailnet:example", Endpoint: server.URL})
	result, err := provider.Resolve(context.Background(), tailscaleTestResolution(t, PeerResolutionOptions{ImmediatePeer: "100.64.0.1:1"}))
	if err != nil || result.Status() != PeerIdentityInvalid {
		t.Fatalf("Resolve() = (%v, %v)", result, err)
	}
}

func TestTailscaleLocalAPIBoundsAndDeadline(t *testing.T) {
	t.Run("body", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
			response.Header().Set("Content-Type", "application/json")
			_, _ = response.Write([]byte(strings.Repeat("x", 128)))
		}))
		defer server.Close()
		provider, _ := NewTailscaleLocalAPIIdentityProvider(TailscaleLocalAPIOptions{
			Issuer: "tailnet:example", Endpoint: server.URL, MaxResponseBytes: 64,
		})
		result, err := provider.Resolve(context.Background(), tailscaleTestResolution(t, PeerResolutionOptions{ImmediatePeer: "100.64.0.1:1"}))
		if err != nil || result.Status() != PeerIdentityInvalid {
			t.Fatalf("Resolve() = (%v, %v)", result, err)
		}
	})
	t.Run("headers", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
			response.Header().Set("X-Oversized", strings.Repeat("x", 2048))
			response.Header().Set("Content-Type", "application/json")
			_, _ = response.Write(tailscaleTestWhoIs(false))
		}))
		defer server.Close()
		provider, _ := NewTailscaleLocalAPIIdentityProvider(TailscaleLocalAPIOptions{
			Issuer: "tailnet:example", Endpoint: server.URL, MaxResponseHeaderBytes: 512,
		})
		result, err := provider.Resolve(context.Background(), tailscaleTestResolution(t, PeerResolutionOptions{ImmediatePeer: "100.64.0.1:1"}))
		if err != nil || result.Status() != PeerIdentityInvalid {
			t.Fatalf("Resolve() = (%v, %v)", result, err)
		}
	})
	t.Run("deadline", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
			time.Sleep(200 * time.Millisecond)
			response.Header().Set("Content-Type", "application/json")
			_, _ = response.Write(tailscaleTestWhoIs(false))
		}))
		defer server.Close()
		provider, _ := NewTailscaleLocalAPIIdentityProvider(TailscaleLocalAPIOptions{
			Issuer: "tailnet:example", Endpoint: server.URL, Timeout: 30 * time.Millisecond,
		})
		started := time.Now()
		result, err := provider.Resolve(context.Background(), tailscaleTestResolution(t, PeerResolutionOptions{ImmediatePeer: "100.64.0.1:1"}))
		elapsed := time.Since(started)
		if err != nil || result.Status() != PeerIdentityUnavailable || elapsed > 150*time.Millisecond {
			t.Fatalf("Resolve() = (%v, %v) after %s", result, err, elapsed)
		}
	})
}

func TestTailscaleLocalAPIConfigurationAndScopeValidation(t *testing.T) {
	invalid := []TailscaleLocalAPIOptions{
		{},
		{Issuer: "tailnet:example", UnixSocket: "/tmp/ts.sock", Endpoint: "http://127.0.0.1"},
		{Issuer: "tailnet:example", Endpoint: "https://127.0.0.1"},
		{Issuer: "tailnet:example", Endpoint: "http://user@127.0.0.1"},
		{Issuer: "tailnet:example", UnixSocket: "/tmp/ts.sock", Password: "secret"},
	}
	for index, options := range invalid {
		if _, err := NewTailscaleLocalAPIIdentityProvider(options); err == nil {
			t.Errorf("invalid options case %d succeeded", index)
		}
	}
	provider, err := NewTailscaleLocalAPIIdentityProvider(TailscaleLocalAPIOptions{Issuer: "tailnet:example", Endpoint: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	absent, err := provider.Resolve(context.Background(), tailscaleTestResolution(t, PeerResolutionOptions{}))
	if err != nil || absent.Status() != PeerIdentityNotApplicable {
		t.Fatalf("absent Resolve() = (%v, %v)", absent, err)
	}
	invalidScope, err := provider.Resolve(context.Background(), tailscaleTestResolution(t, PeerResolutionOptions{
		ImmediatePeer: "100.64.0.1:1", ServiceName: "svc:not.a-label",
	}))
	if err != nil || invalidScope.Status() != PeerIdentityInvalid {
		t.Fatalf("invalid scope Resolve() = (%v, %v)", invalidScope, err)
	}
}

func TestTailscaleLocalAPIOfficialQueryEncoding(t *testing.T) {
	values := url.Values{"addr": {"[2001:db8::1]:443"}, "proto": {"tcp"}, "svc_name": {"svc:worker"}}
	if got := values.Encode(); !strings.Contains(got, "svc_name=svc%3Aworker") {
		t.Fatalf("query encoding = %q", got)
	}
}
