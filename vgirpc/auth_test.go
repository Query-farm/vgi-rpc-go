// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestAuthUnavailablePropagatesThroughChain covers the definitive/transient
// split. The chain advances past "not my credential"; an outage reported the
// same way would be advanced past too, and the caller would either see a 401
// from the end of the chain (a re-login storm, negative-cached) or — as here,
// with a later authenticator that accepts — be authenticated as somebody else
// entirely while the real authority was down.
//
// An in-process test because the conformance worker configures one fixed
// authenticator, so no wire case can put an authority mid-outage in the middle
// of a chain.
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
			t.Error("the chain advanced past an authority that could not answer")
			return &AuthContext{Domain: "test", Authenticated: true, Principal: "fallback@example"}, nil
		},
	))
	h.InitPages()
	ts := httptest.NewServer(h)
	defer ts.Close()

	// Any RPC route: every one authenticates before it looks at the path or
	// the body, so the answer here is the chain's and nothing else's.
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/"+IdentityProtocolName+"/introspect_token",
		strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", arrowContentType)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d, want 503 — a transient failure must not be reported as a rejection", resp.StatusCode)
	}
	// Retry-After is the half that makes 503 actionable: without it a caller
	// has no schedule and falls back to hammering or giving up.
	if got := resp.Header.Get("Retry-After"); got != "3" {
		t.Errorf("Retry-After: got %q, want %q", got, "3")
	}
}
