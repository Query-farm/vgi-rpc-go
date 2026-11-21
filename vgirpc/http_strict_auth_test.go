// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

const strictChallenge = `Bearer error="invalid_token"`

// strictServer is a strict-auth server whose authenticator accepts exactly
// "Bearer good" and counts its calls.
func strictServer(t *testing.T) (*HttpServer, *atomic.Int64) {
	t.Helper()
	var calls atomic.Int64
	h := newTestHttpServer(t)
	h.SetAuthenticate(func(r *http.Request) (*AuthContext, error) {
		calls.Add(1)
		switch r.Header.Get("Authorization") {
		case "":
			return nil, NewAuthFailure(AuthReasonMissingCredential, "no credential")
		case "Bearer good":
			return &AuthContext{Domain: "test", Authenticated: true, Principal: "alice"}, nil
		default:
			return nil, NewAuthFailure(AuthReasonInvalidCredential, "bad credential")
		}
	})
	h.SetStrictAuth(true)
	if err := h.SetWWWAuthenticate(strictChallenge); err != nil {
		t.Fatal(err)
	}
	h.SetCorsOrigins("*")
	if err := h.InitIdentityBearer(); err != nil {
		t.Fatal(err)
	}
	h.InitPages()
	return h, &calls
}

func serve(h *HttpServer, req *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func assertStrict401(t *testing.T, w *httptest.ResponseRecorder, reason AuthReason) {
	t.Helper()
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("WWW-Authenticate"); got != strictChallenge {
		t.Fatalf("WWW-Authenticate = %q", got)
	}
	if got := w.Header().Get(HeaderAuthReason); got != string(reason) {
		t.Fatalf("%s = %q, want %q", HeaderAuthReason, got, reason)
	}
	if w.Header().Get(supportedEncodingsHeader) == "" {
		t.Fatalf("401 lacks the capability headers")
	}
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("401 lacks open CORS (Access-Control-Allow-Origin=%q)", got)
	}
}

func TestStrictAuthHeadHealthIsAnonymous(t *testing.T) {
	h, calls := strictServer(t)
	w := serve(h, httptest.NewRequest(http.MethodHead, "/health", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("HEAD /health: expected 200, got %d", w.Code)
	}
	if w.Header().Get(supportedEncodingsHeader) == "" {
		t.Fatal("HEAD /health lacks the capability headers")
	}
	if calls.Load() != 0 {
		t.Fatalf("HEAD /health ran the authenticator %d times", calls.Load())
	}
}

func TestStrictAuthEverythingElseNeedsCredential(t *testing.T) {
	h, _ := strictServer(t)
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/health"},
		{http.MethodOptions, "/health"}, // not a preflight: no Access-Control-Request-Method
		{http.MethodGet, "/"},
		{http.MethodGet, "/describe"},
		{http.MethodGet, "/no/such/path"},
		{http.MethodGet, "/.well-known/oauth-protected-resource"},
		{http.MethodPost, "/vgi.v3/catalog_attach"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			req.Header.Set("Origin", "https://shell.example.com")
			assertStrict401(t, serve(h, req), AuthReasonMissingCredential)
		})
	}
}

func TestStrictAuthInvalidCredential(t *testing.T) {
	h, _ := strictServer(t)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("Authorization", "Bearer forged")
	assertStrict401(t, serve(h, req), AuthReasonInvalidCredential)
}

func TestStrictAuthPreflightIsAnonymous(t *testing.T) {
	h, calls := strictServer(t)
	req := httptest.NewRequest(http.MethodOptions, "/vgi.v3/catalog_attach", nil)
	req.Header.Set("Origin", "https://shell.example.com")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "authorization")
	w := serve(h, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("preflight: expected 204, got %d", w.Code)
	}
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("preflight Access-Control-Allow-Origin = %q", got)
	}
	if calls.Load() != 0 {
		t.Fatalf("preflight ran the authenticator %d times", calls.Load())
	}
}

func TestStrictAuthCredentialedGetHealth(t *testing.T) {
	h, _ := strictServer(t)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("Authorization", "Bearer good")
	if w := serve(h, req); w.Code != http.StatusOK {
		t.Fatalf("GET /health with a credential: expected 200, got %d", w.Code)
	}
}

// readTracker records whether anything read the request body.
type readTracker struct{ read atomic.Bool }

func (r *readTracker) Read(p []byte) (int, error) {
	r.read.Store(true)
	return strings.NewReader("x").Read(p)
}

func TestStrictAuthRunsBeforeBodyIsRead(t *testing.T) {
	h, _ := strictServer(t)
	h.SetMaxRequestBytes(4)
	body := &readTracker{}
	req := httptest.NewRequest(http.MethodPost, "/vgi.v3/catalog_attach", body)
	req.Header.Set("Content-Type", arrowContentType)
	req.Header.Set("Content-Encoding", "zstd")
	req.ContentLength = 1 << 20 // over the limit: a size check would answer 413
	assertStrict401(t, serve(h, req), AuthReasonMissingCredential)
	if body.read.Load() {
		t.Fatal("the request body was read before authentication")
	}
}

func TestStrictAuthAuthenticatesOnce(t *testing.T) {
	h, calls := strictServer(t)
	req := httptest.NewRequest(http.MethodPost, "/vgi.v3/no_such_method", strings.NewReader(""))
	req.Header.Set("Content-Type", arrowContentType)
	req.Header.Set("Authorization", "Bearer good")
	w := serve(h, req)
	if w.Code == http.StatusUnauthorized {
		t.Fatalf("credentialed request got 401")
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("authenticator ran %d times for one request, want 1", n)
	}
}

func TestStrictAuthConfigurationChecks(t *testing.T) {
	t.Run("no authenticator", func(t *testing.T) {
		h := newTestHttpServer(t)
		h.SetStrictAuth(true)
		if err := h.InitIdentityBearer(); err == nil {
			t.Fatal("strict auth without an authenticator started")
		}
		// And fails closed if served anyway.
		if w := serve(h, httptest.NewRequest(http.MethodGet, "/health", nil)); w.Code != http.StatusInternalServerError {
			t.Fatalf("unconfigured strict server answered %d, want 500", w.Code)
		}
	})
	t.Run("oauth metadata", func(t *testing.T) {
		h := newTestHttpServer(t)
		h.SetAuthenticate(func(*http.Request) (*AuthContext, error) { return Anonymous(), nil })
		if err := h.SetOAuthResourceMetadata(&OAuthResourceMetadata{
			Resource:             "https://api.example.com/vgi",
			AuthorizationServers: []string{"https://auth.example.com"},
		}); err != nil {
			t.Fatal(err)
		}
		h.SetStrictAuth(true)
		if err := h.InitIdentityBearer(); err == nil {
			t.Fatal("strict auth with OAuth resource metadata started")
		}
	})
	t.Run("peer policy", func(t *testing.T) {
		h := newTestHttpServer(t)
		h.SetAuthenticate(func(*http.Request) (*AuthContext, error) { return Anonymous(), nil })
		h.SetPeerAuthenticationPolicy(ObservePeerIdentity)
		h.SetStrictAuth(true)
		if err := h.InitIdentityBearer(); err == nil {
			t.Fatal("strict auth with a peer authentication policy started")
		}
	})
}

func TestSetWWWAuthenticate(t *testing.T) {
	h := newTestHttpServer(t)
	for _, bad := range []string{"", "  ", "Bearer\r\nX-Injected: 1"} {
		if err := h.SetWWWAuthenticate(bad); err == nil {
			t.Fatalf("SetWWWAuthenticate(%q) accepted", bad)
		}
	}
	if err := h.SetWWWAuthenticate(strictChallenge); err != nil {
		t.Fatal(err)
	}
	if err := h.SetOAuthResourceMetadata(&OAuthResourceMetadata{
		Resource:             "https://api.example.com/vgi",
		AuthorizationServers: []string{"https://auth.example.com"},
	}); err == nil {
		t.Fatal("OAuth resource metadata accepted beside an explicit challenge")
	}

	h2 := newTestHttpServer(t)
	if err := h2.SetOAuthResourceMetadata(&OAuthResourceMetadata{
		Resource:             "https://api.example.com/vgi",
		AuthorizationServers: []string{"https://auth.example.com"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := h2.SetWWWAuthenticate(strictChallenge); err == nil {
		t.Fatal("explicit challenge accepted beside OAuth resource metadata")
	}
}

// Without strict auth nothing changes: GET /health stays anonymous.
func TestNonStrictHealthStaysAnonymous(t *testing.T) {
	h := newTestHttpServer(t)
	h.SetAuthenticate(func(*http.Request) (*AuthContext, error) {
		return nil, NewAuthFailure(AuthReasonMissingCredential, "no")
	})
	h.InitPages()
	if w := serve(h, httptest.NewRequest(http.MethodGet, "/health", nil)); w.Code != http.StatusOK {
		t.Fatalf("GET /health without strict auth: expected 200, got %d", w.Code)
	}
}
