// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Regression test: HEAD /health must answer 200 so capability discovery
// doesn't degrade. The C++ client probes /health with HEAD (the mandatory,
// auth-exempt capability discovery endpoint); a 405 carries no
// Content-Length and isn't the 200 the protocol expects, so discovery would
// silently fall back to defaults. Mirrors the Python reference's
// test_head_health regression (vgi-rpc 2858d29); in Go this holds because
// net/http's ServeMux matches HEAD against GET patterns and the transport
// strips the body, so the test guards those semantics.

package vgirpc

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHeadHealthMatchesGet(t *testing.T) {
	h := newTestHttpServer(t)
	h.SetMaxRequestBytes(1024 * 1024) // advertise a capability header
	h.InitPages()

	ts := httptest.NewServer(h)
	defer ts.Close()

	get, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	getBody, err := io.ReadAll(get.Body)
	_ = get.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if get.StatusCode != http.StatusOK {
		t.Fatalf("GET /health: expected 200, got %d", get.StatusCode)
	}

	head, err := http.Head(ts.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	headBody, err := io.ReadAll(head.Body)
	_ = head.Body.Close()
	if err != nil {
		t.Fatal(err)
	}

	if head.StatusCode != http.StatusOK {
		t.Fatalf("HEAD /health: expected 200, got %d", head.StatusCode)
	}
	if len(headBody) != 0 {
		t.Fatalf("HEAD /health: expected empty body, got %d bytes", len(headBody))
	}
	if head.ContentLength != int64(len(getBody)) {
		t.Fatalf("HEAD /health: expected Content-Length %d, got %d", len(getBody), head.ContentLength)
	}
	// Verb-independent discovery: the same Content-Type and capability
	// headers as GET.
	for _, header := range []string{"Content-Type", supportedEncodingsHeader, maxRequestBytesHeader} {
		if got, want := head.Header.Get(header), get.Header.Get(header); got != want {
			t.Fatalf("HEAD /health: expected %s=%q (as on GET), got %q", header, want, got)
		}
	}
}

// TestRpcMethodNamedLikeHealthDoesNotBypassAuth is an adversarial-review
// regression: the sibling Python implementation's HTTP auth middleware
// exempted a path via an unanchored `path.startswith(exempt_prefix)` check,
// so an RPC method merely NAMED with the exempt prefix's tail (e.g.
// "healthbogus", dispatched through Python's wildcard {prefix}/{method}
// route) matched the same check and skipped authentication entirely — a
// full auth bypass reachable just by a method's name.
//
// This Go server has no equivalent centralized "is this path exempt from
// auth" check to get wrong: /health is a fixed, exact net/http.ServeMux
// pattern (registered independently of the RPC {method} wildcard route,
// which Go's enhanced ServeMux routing — used here via go.mod's Go 1.26 —
// anchors to a full path segment rather than a substring), and every RPC
// handler (handleUnary, handleStreamInit, handleStreamExchange) calls
// h.authenticate() as its first statement regardless of the method name.
// This test proves a POST to a path that merely STARTS WITH "/health"
// (but isn't the health route) is dispatched as an ordinary RPC call and
// still requires authentication — i.e. the vulnerability class does not
// exist here.
func TestRpcMethodNamedLikeHealthDoesNotBypassAuth(t *testing.T) {
	h := newTestHttpServer(t)
	h.SetAuthenticate(func(r *http.Request) (*AuthContext, error) {
		return nil, &RpcError{Type: "ValueError", Message: "unauthorized"}
	})
	h.InitPages()

	for _, path := range []string{"/healthbogus", "/health_evil", "/healthy"} {
		req := httptest.NewRequest("POST", path, nil)
		req.Header.Set("Content-Type", arrowContentType)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Fatalf("POST %s: expected 401 (auth must run before dispatch), got %d", path, w.Code)
		}
	}

	// The exact health route itself is still the deliberate, documented
	// exemption (it never calls h.authenticate() at all).
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /health: expected 200, got %d", w.Code)
	}
}
