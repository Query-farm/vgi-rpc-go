// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
)

// ---------------------------------------------------------------------------
// PKCE helpers (RFC 7636)
// ---------------------------------------------------------------------------

// generateCodeVerifier generates a URL-safe random code verifier (RFC 7636 S4.1).
func generateCodeVerifier() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("vgirpc: crypto/rand failure: %v", err))
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

// generateCodeChallenge computes S256 code challenge from a code verifier (RFC 7636 S4.2).
func generateCodeChallenge(verifier string) string {
	h := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(h[:])
}

// generateStateNonce generates a random state nonce for CSRF protection.
func generateStateNonce() string {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("vgirpc: crypto/rand failure: %v", err))
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

// ---------------------------------------------------------------------------
// Derived HMAC key
// ---------------------------------------------------------------------------

// deriveSessionKey derives a separate HMAC key for OAuth session cookies.
func deriveSessionKey(signingKey []byte) []byte {
	mac := hmac.New(sha256.New, signingKey)
	mac.Write([]byte("oauth-pkce-session"))
	return mac.Sum(nil)
}
