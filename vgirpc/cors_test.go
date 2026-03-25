// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCorsPreflightBypassesAuth(t *testing.T) {
	h := newTestHttpServer(t)
	h.SetCorsOrigins("*")
	h.SetAuthenticate(func(r *http.Request) (*AuthContext, error) {
		return nil, &RpcError{Type: "ValueError", Message: "unauthorized"}
	})
	h.InitPages()

	req := httptest.NewRequest("OPTIONS", "/some_method", nil)
	req.Header.Set("Origin", "http://example.com")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("expected Access-Control-Allow-Origin=*, got %q", got)
	}
}

func TestCorsHeadersOnPost(t *testing.T) {
	h := newTestHttpServer(t)
	h.SetCorsOrigins("https://example.com")
	h.InitPages()

	req := httptest.NewRequest("POST", "/test_method", nil)
	req.Header.Set("Content-Type", "application/vnd.apache.arrow.stream")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "https://example.com" {
		t.Fatalf("expected Access-Control-Allow-Origin=https://example.com, got %q", got)
	}
}

func TestNoCorsHeadersByDefault(t *testing.T) {
	h := newTestHttpServer(t)
	h.InitPages()

	req := httptest.NewRequest("POST", "/test_method", nil)
	req.Header.Set("Content-Type", "application/vnd.apache.arrow.stream")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("expected no CORS header, got %q", got)
	}
}
