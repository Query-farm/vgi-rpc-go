// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// coreExposeHeaders is the stable prefix of Access-Control-Expose-Headers. The
// server may append further VGI capability headers (max-response-bytes, sticky,
// …); tests assert the prefix so they don't break as that list grows.
const coreExposeHeaders = "WWW-Authenticate, X-Request-ID, X-VGI-Content-Encoding, X-VGI-RPC-Error"

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
	if got := w.Header().Get("Access-Control-Expose-Headers"); !strings.HasPrefix(got, coreExposeHeaders) {
		t.Fatalf("expected Access-Control-Expose-Headers to start with %q, got %q", coreExposeHeaders, got)
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
	if got := w.Header().Get("Access-Control-Expose-Headers"); !strings.HasPrefix(got, coreExposeHeaders) {
		t.Fatalf("expected Access-Control-Expose-Headers to start with %q, got %q", coreExposeHeaders, got)
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

	if got := w.Header().Get("Access-Control-Expose-Headers"); !strings.HasPrefix(got, coreExposeHeaders) {
		t.Fatalf("expected Access-Control-Expose-Headers to start with %q, got %q", coreExposeHeaders, got)
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

func TestCorsAllowHeadersEchoesRequest(t *testing.T) {
	h := newTestHttpServer(t)
	h.SetCorsOrigins("*")
	h.InitPages()

	// Preflight requesting a non-default header should be echoed back, so any
	// client request header is permitted under the wildcard origin.
	req := httptest.NewRequest("OPTIONS", "/test_method", nil)
	req.Header.Set("Origin", "http://example.com")
	req.Header.Set("Access-Control-Request-Headers", "content-type,authorization,x-vgi-custom")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if got := w.Header().Get("Access-Control-Allow-Headers"); got != "content-type,authorization,x-vgi-custom" {
		t.Fatalf("expected Allow-Headers to echo the request, got %q", got)
	}
}

func TestCorsResourcePolicyForCrossOriginIsolation(t *testing.T) {
	h := newTestHttpServer(t)
	h.SetCorsOrigins("*")
	h.InitPages()

	// Both the preflight and the actual response must carry CORP: cross-origin
	// so a cross-origin-isolated (COEP: require-corp) page can fetch this worker.
	for _, method := range []string{"OPTIONS", "POST"} {
		req := httptest.NewRequest(method, "/test_method", nil)
		req.Header.Set("Origin", "https://app.example.com")
		if method == "POST" {
			req.Header.Set("Content-Type", "application/vnd.apache.arrow.stream")
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if got := w.Header().Get("Cross-Origin-Resource-Policy"); got != "cross-origin" {
			t.Fatalf("%s: expected Cross-Origin-Resource-Policy=cross-origin, got %q", method, got)
		}
	}
}

func TestCorsAllowHeadersFallbackWhenNoneRequested(t *testing.T) {
	h := newTestHttpServer(t)
	h.SetCorsOrigins("*")
	h.InitPages()

	req := httptest.NewRequest("OPTIONS", "/test_method", nil)
	req.Header.Set("Origin", "http://example.com")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if got := w.Header().Get("Access-Control-Allow-Headers"); got != "Content-Type, Authorization" {
		t.Fatalf("expected fallback Allow-Headers, got %q", got)
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
