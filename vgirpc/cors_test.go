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
	if got := w.Header().Get("Access-Control-Allow-Headers"); got != "Content-Type, Authorization" {
		t.Fatalf("expected Access-Control-Allow-Headers=Content-Type, Authorization, got %q", got)
	}
	if got := w.Header().Get("Access-Control-Expose-Headers"); got != "WWW-Authenticate, X-Request-ID, X-VGI-Content-Encoding, X-VGI-RPC-Error" {
		t.Fatalf("expected Access-Control-Expose-Headers=WWW-Authenticate, X-Request-ID, X-VGI-Content-Encoding, X-VGI-RPC-Error, got %q", got)
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
	if got := w.Header().Get("Access-Control-Expose-Headers"); got != "WWW-Authenticate, X-Request-ID, X-VGI-Content-Encoding, X-VGI-RPC-Error" {
		t.Fatalf("expected Access-Control-Expose-Headers=WWW-Authenticate, X-Request-ID, X-VGI-Content-Encoding, X-VGI-RPC-Error, got %q", got)
	}
}

func TestCorsExposeHeadersOnPreflight(t *testing.T) {
	h := newTestHttpServer(t)
	h.SetCorsOrigins("*")
	h.InitPages()

	req := httptest.NewRequest("OPTIONS", "/test_method", nil)
	req.Header.Set("Origin", "http://example.com")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if got := w.Header().Get("Access-Control-Expose-Headers"); got != "WWW-Authenticate, X-Request-ID, X-VGI-Content-Encoding, X-VGI-RPC-Error" {
		t.Fatalf("expected Access-Control-Expose-Headers=WWW-Authenticate, X-Request-ID, X-VGI-Content-Encoding, X-VGI-RPC-Error, got %q", got)
	}
}

func TestCorsMaxAgeDefault(t *testing.T) {
	h := newTestHttpServer(t)
	h.SetCorsOrigins("*")
	h.InitPages()

	req := httptest.NewRequest("OPTIONS", "/test_method", nil)
	req.Header.Set("Origin", "http://example.com")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if got := w.Header().Get("Access-Control-Max-Age"); got != "7200" {
		t.Fatalf("expected Access-Control-Max-Age=7200, got %q", got)
	}
}

func TestCorsMaxAgeCustom(t *testing.T) {
	h := newTestHttpServer(t)
	h.SetCorsOrigins("*")
	h.SetCorsMaxAge(3600)
	h.InitPages()

	req := httptest.NewRequest("OPTIONS", "/test_method", nil)
	req.Header.Set("Origin", "http://example.com")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if got := w.Header().Get("Access-Control-Max-Age"); got != "3600" {
		t.Fatalf("expected Access-Control-Max-Age=3600, got %q", got)
	}
}

func TestCorsMaxAgeZeroOmitsHeader(t *testing.T) {
	h := newTestHttpServer(t)
	h.SetCorsOrigins("*")
	h.SetCorsMaxAge(0)
	h.InitPages()

	req := httptest.NewRequest("OPTIONS", "/test_method", nil)
	req.Header.Set("Origin", "http://example.com")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if got := w.Header().Get("Access-Control-Max-Age"); got != "" {
		t.Fatalf("expected no Access-Control-Max-Age header, got %q", got)
	}
}

func TestCorsMaxAgeNotOnPost(t *testing.T) {
	h := newTestHttpServer(t)
	h.SetCorsOrigins("*")
	h.InitPages()

	req := httptest.NewRequest("POST", "/test_method", nil)
	req.Header.Set("Content-Type", "application/vnd.apache.arrow.stream")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if got := w.Header().Get("Access-Control-Max-Age"); got != "" {
		t.Fatalf("expected no Access-Control-Max-Age on POST, got %q", got)
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
