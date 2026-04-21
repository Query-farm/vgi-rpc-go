// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestCallContext_SetCookie_DisabledSink verifies that SetCookie returns
// errCookieNotUnaryHTTP when the sink has not been enabled (i.e. the call
// is not a unary HTTP request).
func TestCallContext_SetCookie_DisabledSink(t *testing.T) {
	ctx := &CallContext{Ctx: context.Background()}
	err := ctx.SetCookie("sid", "abc", CookieAttrs{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, errCookieNotUnaryHTTP) {
		t.Fatalf("expected errCookieNotUnaryHTTP, got %v", err)
	}
}

// TestCallContext_DeleteCookie_DisabledSink verifies that DeleteCookie
// returns an error on non-HTTP-unary paths.
func TestCallContext_DeleteCookie_DisabledSink(t *testing.T) {
	ctx := &CallContext{Ctx: context.Background()}
	err := ctx.DeleteCookie("sid", "/", "")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestCallContext_SetCookie_Queued verifies that SetCookie appends a
// CookieSpec to the pending queue when the sink is enabled.
func TestCallContext_SetCookie_Queued(t *testing.T) {
	ctx := &CallContext{Ctx: context.Background()}
	ctx.enableCookieSink()
	if err := ctx.SetCookie("sid", "v1", CookieAttrs{MaxAge: 60, HttpOnly: true, Path: "/"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := ctx.DeleteCookie("old", "/", ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cookies := ctx.drainCookies()
	if len(cookies) != 2 {
		t.Fatalf("expected 2 cookies, got %d", len(cookies))
	}
	if cookies[0].Name != "sid" || cookies[0].Value != "v1" || cookies[0].MaxAge != 60 || !cookies[0].HttpOnly {
		t.Errorf("unexpected first cookie: %+v", cookies[0])
	}
	if cookies[1].Name != "old" || !cookies[1].Delete || cookies[1].Path != "/" {
		t.Errorf("unexpected second cookie: %+v", cookies[1])
	}
	// Drain must have cleared the queue.
	if got := ctx.drainCookies(); len(got) != 0 {
		t.Errorf("expected queue to be cleared, got %d cookies", len(got))
	}
}

// TestBuildHTTPCookies verifies that incoming Cookie headers are parsed
// into the transport_metadata-adjacent cookies map.
func TestBuildHTTPCookies(t *testing.T) {
	r := httptest.NewRequest("POST", "/x", nil)
	r.AddCookie(&http.Cookie{Name: "sid", Value: "abc"})
	r.AddCookie(&http.Cookie{Name: "theme", Value: "dark"})
	got := buildHTTPCookies(r)
	if got["sid"] != "abc" {
		t.Errorf("expected sid=abc, got %q", got["sid"])
	}
	if got["theme"] != "dark" {
		t.Errorf("expected theme=dark, got %q", got["theme"])
	}

	rEmpty := httptest.NewRequest("POST", "/x", nil)
	if buildHTTPCookies(rEmpty) != nil {
		t.Errorf("expected nil for no-cookie request")
	}
}

// TestApplyResponseCookies verifies that queued CookieSpec entries render
// as Set-Cookie headers via net/http's encoding.
func TestApplyResponseCookies(t *testing.T) {
	w := httptest.NewRecorder()
	applyResponseCookies(w, []CookieSpec{
		{
			Name:  "sid",
			Value: "xyz",
			CookieAttrs: CookieAttrs{
				MaxAge:   60,
				Path:     "/",
				Secure:   true,
				HttpOnly: true,
				SameSite: http.SameSiteLaxMode,
			},
		},
		{
			Name:   "old",
			Delete: true,
			CookieAttrs: CookieAttrs{
				Path: "/",
			},
		},
	})
	headers := w.Header().Values("Set-Cookie")
	if len(headers) != 2 {
		t.Fatalf("expected 2 Set-Cookie headers, got %d: %v", len(headers), headers)
	}
	// First cookie should carry the attributes.
	got := headers[0]
	for _, attr := range []string{"sid=xyz", "Path=/", "Max-Age=60", "HttpOnly", "Secure", "SameSite=Lax"} {
		if !contains(got, attr) {
			t.Errorf("first cookie missing %q in %q", attr, got)
		}
	}
	// Second cookie is a deletion (empty value + past expiry or Max-Age<=0).
	del := headers[1]
	if !contains(del, "old=") || !contains(del, "Max-Age=0") {
		t.Errorf("expected deletion cookie, got %q", del)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
