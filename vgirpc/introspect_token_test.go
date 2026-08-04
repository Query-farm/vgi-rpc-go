// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Token-introspection guards the shared conformance group structurally cannot
// reach. It drives a subprocess worker over the wire, so it can assert what
// comes back but never what the worker wrote to its own log, and it configures
// one fixed worker, so it can never observe a construction-time refusal or an
// authenticate chain mid-outage.

package vgirpc

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const (
	testIntrospector = "introspector@example"
	testSubjectToken = "opaque-subject-credential-9d2f"
)

// captureLogs redirects the default slog logger for the duration of fn.
func captureLogs(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(prev)
	fn()
	return buf.String()
}

// newIntrospectTestServer returns a running server whose resolver answers for
// testSubjectToken and fails transiently for "unavailable-credential".
func newIntrospectTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	h := newTestHttpServer(t)
	h.SetAuthenticate(func(r *http.Request) (*AuthContext, error) {
		principal := r.Header.Get("X-Test-Principal")
		if principal == "" {
			return Anonymous(), nil
		}
		return &AuthContext{Domain: "test", Authenticated: true, Principal: principal}, nil
	})
	if err := h.EnableTokenIntrospection(TokenIntrospectionConfig{
		Principals: []string{testIntrospector},
		Resolver: func(credential string) (TokenIdentity, bool, error) {
			switch credential {
			case testSubjectToken:
				return TokenIdentity{Principal: "subject@example", TokenName: "laptop"}, true, nil
			case "unavailable-credential":
				return TokenIdentity{}, false, NewAuthUnavailable("credential store unreachable")
			}
			return TokenIdentity{}, false, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	h.InitPages()
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts
}

// postIntrospect posts credential as principal and returns status and body.
func postIntrospect(t *testing.T, ts *httptest.Server, principal, credential string) (int, string) {
	t.Helper()
	body, err := json.Marshal(map[string]string{"token": credential})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+IntrospectEndpoint, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if principal != "" {
		req.Header.Set("X-Test-Principal", principal)
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(got)
}

// TestIntrospectionNeverLogsCredential covers guard 6. The conformance group
// asserts the credential is absent from every response; only an in-process test
// can assert it is absent from the log too, and the log is where a bearer token
// is most likely to end up by accident — one "%v of the request" away.
func TestIntrospectionNeverLogsCredential(t *testing.T) {
	ts := newIntrospectTestServer(t)

	for _, tc := range []struct {
		name       string
		principal  string
		credential string
		wantStatus int
	}{
		{"resolved", testIntrospector, testSubjectToken, http.StatusOK},
		{"unresolved", testIntrospector, "no-such-credential", http.StatusNotFound},
		{"jws shaped", testIntrospector, "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhIn0.c2ln", http.StatusNotFound},
		{"unavailable", testIntrospector, "unavailable-credential", http.StatusServiceUnavailable},
		{"not an introspector", "someone-else", testSubjectToken, http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var status int
			var body string
			logged := captureLogs(t, func() {
				status, body = postIntrospect(t, ts, tc.principal, tc.credential)
			})
			if status != tc.wantStatus {
				t.Fatalf("status: got %d, want %d (body %q)", status, tc.wantStatus, body)
			}
			if strings.Contains(logged, tc.credential) {
				t.Errorf("the credential reached the log:\n%s", logged)
			}
			if strings.Contains(body, tc.credential) {
				t.Errorf("the credential was echoed in the response: %q", body)
			}
			// A digest is what makes the record useful without being the
			// credential; a port that satisfies the rule by logging nothing
			// leaves an operator no way to correlate one credential's
			// failures. Not asserted on the refusals that happen before the
			// body is read — there is no credential to digest yet.
			if tc.name != "not an introspector" && !strings.Contains(logged, TokenDigest(tc.credential)) {
				t.Errorf("expected the credential's digest in the log:\n%s", logged)
			}
		})
	}
}

// TestAuthUnavailablePropagatesThroughChain covers the definitive/transient
// split. The chain advances past "not my credential"; an outage reported the
// same way would be advanced past too, and the caller would either see a 401
// from the end of the chain (a re-login storm, negative-cached) or — as here,
// with a later authenticator that accepts — be authenticated as somebody else
// entirely while the real authority was down.
func TestAuthUnavailablePropagatesThroughChain(t *testing.T) {
	h := newTestHttpServer(t)
	h.SetAuthenticate(ChainAuthenticate(
		// Declines: the chain must advance.
		func(*http.Request) (*AuthContext, error) {
			return nil, &RpcError{Type: "ValueError", Message: "not my credential"}
		},
		// Cannot answer: the chain must stop.
		func(*http.Request) (*AuthContext, error) {
			return nil, &AuthUnavailableError{Detail: "sidecar restarting", RetryAfter: 3}
		},
		// Would accept. Reaching it at all is the bug.
		func(*http.Request) (*AuthContext, error) {
			return &AuthContext{Domain: "test", Authenticated: true, Principal: "fallback@example"}, nil
		},
	))
	if err := h.EnableTokenIntrospection(TokenIntrospectionConfig{
		Principals: []string{"fallback@example"},
		Resolver: func(string) (TokenIdentity, bool, error) {
			t.Error("the resolver ran during an authentication outage")
			return TokenIdentity{}, false, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	h.InitPages()
	ts := httptest.NewServer(h)
	defer ts.Close()

	status, body := postIntrospect(t, ts, "", testSubjectToken)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d, want 503 (body %q) — a transient failure must not be reported as a rejection", status, body)
	}

	// Retry-After is the half that makes 503 actionable: without it a caller
	// has no schedule and falls back to hammering or giving up.
	req, err := http.NewRequest(http.MethodPost, ts.URL+IntrospectEndpoint, strings.NewReader(`{"token":"x"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Retry-After"); got != "3" {
		t.Errorf("Retry-After: got %q, want %q", got, "3")
	}
}

// TestEnableTokenIntrospectionRequiresAllowlist covers guard 3 at its only
// observable moment. A worker misconfigured this way never starts, so the
// conformance suite has nothing to connect to and cannot make the assertion.
func TestEnableTokenIntrospectionRequiresAllowlist(t *testing.T) {
	resolver := func(string) (TokenIdentity, bool, error) { return TokenIdentity{}, false, nil }

	for _, tc := range []struct {
		name string
		cfg  TokenIntrospectionConfig
	}{
		{"no principals", TokenIntrospectionConfig{Resolver: resolver}},
		{"empty principal", TokenIntrospectionConfig{Resolver: resolver, Principals: []string{""}}},
		{"no resolver", TokenIntrospectionConfig{Principals: []string{testIntrospector}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHttpServer(t)
			if err := h.EnableTokenIntrospection(tc.cfg); err == nil {
				t.Fatal("expected an error: authentication alone must not grant introspection")
			}
			if h.introspect != nil {
				t.Fatal("a rejected configuration must leave the endpoint resolving nothing")
			}
		})
	}
}
