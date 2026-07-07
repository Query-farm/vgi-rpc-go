// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

// makeUnsignedJWT builds a JWT-shaped string (header.payload.signature) whose
// payload carries the given claims. The signature is not validated by the
// display-identity path, so it is a fixed placeholder.
func makeUnsignedJWT(claims map[string]any) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`))
	body, _ := json.Marshal(claims)
	payload := base64.RawURLEncoding.EncodeToString(body)
	return header + "." + payload + ".sig"
}

func TestIdentityCookieValue(t *testing.T) {
	idToken := makeUnsignedJWT(map[string]any{
		"sub":                "user-123",
		"email":              "alice@example.com",
		"preferred_username": "alice",
		"name":               "Alice Example",
		"picture":            "https://example.com/a.png",
		"aud":                "should-be-dropped",
		"exp":                1234567890,
	})

	got := identityCookieValue(idToken)
	if got == "" {
		t.Fatal("identityCookieValue returned empty for a valid id_token")
	}

	raw, err := base64.RawURLEncoding.DecodeString(got)
	if err != nil {
		t.Fatalf("cookie value is not base64url: %v", err)
	}
	var ident map[string]any
	if err := json.Unmarshal(raw, &ident); err != nil {
		t.Fatalf("cookie payload is not JSON: %v", err)
	}

	// Only the display claims survive; non-display claims are dropped.
	for _, k := range identityClaims {
		if _, ok := ident[k]; !ok {
			t.Errorf("expected display claim %q in identity cookie", k)
		}
	}
	if _, ok := ident["aud"]; ok {
		t.Errorf("non-display claim 'aud' leaked into identity cookie")
	}
	if _, ok := ident["exp"]; ok {
		t.Errorf("non-display claim 'exp' leaked into identity cookie")
	}
	if ident["email"] != "alice@example.com" {
		t.Errorf("email = %v", ident["email"])
	}
}

func TestIdentityCookieValueEmpty(t *testing.T) {
	if v := identityCookieValue(""); v != "" {
		t.Errorf("empty id_token: got %q, want empty", v)
	}
	if v := identityCookieValue("not-a-jwt"); v != "" {
		t.Errorf("malformed id_token: got %q, want empty", v)
	}
	// A well-formed JWT carrying no display claims yields no cookie.
	noDisplay := makeUnsignedJWT(map[string]any{"aud": "x", "iss": "y"})
	if v := identityCookieValue(noDisplay); v != "" {
		t.Errorf("no-display-claims id_token: got %q, want empty", v)
	}
}
