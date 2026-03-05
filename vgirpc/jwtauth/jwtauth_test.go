// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package jwtauth

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// testKeyServer starts an httptest.Server serving a JWKS containing the given
// RSA public key. Returns the server (caller must close) and the JWKS URI.
func testKeyServer(t *testing.T, key *rsa.PublicKey, kid string) *httptest.Server {
	t.Helper()
	jwks := map[string]any{
		"keys": []map[string]any{
			{
				"kty": "RSA",
				"kid": kid,
				"use": "sig",
				"alg": "RS256",
				"n":   base64URLEncode(key.N.Bytes()),
				"e":   base64URLEncode(big.NewInt(int64(key.E)).Bytes()),
			},
		},
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(jwks)
	}))
}

func base64URLEncode(data []byte) string {
	return base64.RawURLEncoding.EncodeToString(data)
}

func generateTestToken(t *testing.T, key *rsa.PrivateKey, kid string, claims jwt.MapClaims) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = kid
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}
	return signed
}

func TestNewAuthenticateFunc_ValidJWT(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	kid := "test-key-1"
	ts := testKeyServer(t, &key.PublicKey, kid)
	defer ts.Close()

	authFunc, cleanup, err := NewAuthenticateFunc(JWTAuthConfig{
		Issuer:   "https://auth.example.com",
		Audience: "https://api.example.com",
		JWKSURI:  ts.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	tokenStr := generateTestToken(t, key, kid, jwt.MapClaims{
		"iss": "https://auth.example.com",
		"aud": "https://api.example.com",
		"sub": "user123",
		"exp": jwt.NewNumericDate(time.Now().Add(time.Hour)),
		"iat": jwt.NewNumericDate(time.Now()),
	})

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer "+tokenStr)

	auth, err := authFunc(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !auth.Authenticated {
		t.Error("expected authenticated=true")
	}
	if auth.Domain != "jwt" {
		t.Errorf("expected domain=jwt, got %s", auth.Domain)
	}
	if auth.Principal != "user123" {
		t.Errorf("expected principal=user123, got %s", auth.Principal)
	}
	if auth.Claims["sub"] != "user123" {
		t.Errorf("expected sub claim, got %v", auth.Claims["sub"])
	}
}

func TestNewAuthenticateFunc_ExpiredJWT(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	kid := "test-key-1"
	ts := testKeyServer(t, &key.PublicKey, kid)
	defer ts.Close()

	authFunc, cleanup, err := NewAuthenticateFunc(JWTAuthConfig{
		Issuer:   "https://auth.example.com",
		Audience: "https://api.example.com",
		JWKSURI:  ts.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	tokenStr := generateTestToken(t, key, kid, jwt.MapClaims{
		"iss": "https://auth.example.com",
		"aud": "https://api.example.com",
		"sub": "user123",
		"exp": jwt.NewNumericDate(time.Now().Add(-time.Hour)),
		"iat": jwt.NewNumericDate(time.Now().Add(-2 * time.Hour)),
	})

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer "+tokenStr)

	_, err = authFunc(req)
	if err == nil {
		t.Fatal("expected error for expired token")
	}
}

func TestNewAuthenticateFunc_WrongAudience(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	kid := "test-key-1"
	ts := testKeyServer(t, &key.PublicKey, kid)
	defer ts.Close()

	authFunc, cleanup, err := NewAuthenticateFunc(JWTAuthConfig{
		Issuer:   "https://auth.example.com",
		Audience: "https://api.example.com",
		JWKSURI:  ts.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	tokenStr := generateTestToken(t, key, kid, jwt.MapClaims{
		"iss": "https://auth.example.com",
		"aud": "https://wrong.example.com",
		"sub": "user123",
		"exp": jwt.NewNumericDate(time.Now().Add(time.Hour)),
	})

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer "+tokenStr)

	_, err = authFunc(req)
	if err == nil {
		t.Fatal("expected error for wrong audience")
	}
}

func TestNewAuthenticateFunc_WrongIssuer(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	kid := "test-key-1"
	ts := testKeyServer(t, &key.PublicKey, kid)
	defer ts.Close()

	authFunc, cleanup, err := NewAuthenticateFunc(JWTAuthConfig{
		Issuer:   "https://auth.example.com",
		Audience: "https://api.example.com",
		JWKSURI:  ts.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	tokenStr := generateTestToken(t, key, kid, jwt.MapClaims{
		"iss": "https://wrong-issuer.example.com",
		"aud": "https://api.example.com",
		"sub": "user123",
		"exp": jwt.NewNumericDate(time.Now().Add(time.Hour)),
	})

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer "+tokenStr)

	_, err = authFunc(req)
	if err == nil {
		t.Fatal("expected error for wrong issuer")
	}
}

func TestNewAuthenticateFunc_MissingBearer(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	kid := "test-key-1"
	ts := testKeyServer(t, &key.PublicKey, kid)
	defer ts.Close()

	authFunc, cleanup, err := NewAuthenticateFunc(JWTAuthConfig{
		Issuer:   "https://auth.example.com",
		Audience: "https://api.example.com",
		JWKSURI:  ts.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	req := httptest.NewRequest("GET", "/", nil)
	_, err = authFunc(req)
	if err == nil {
		t.Fatal("expected error for missing Authorization header")
	}
}

func TestNewAuthenticateFunc_InvalidToken(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	kid := "test-key-1"
	ts := testKeyServer(t, &key.PublicKey, kid)
	defer ts.Close()

	authFunc, cleanup, err := NewAuthenticateFunc(JWTAuthConfig{
		Issuer:   "https://auth.example.com",
		Audience: "https://api.example.com",
		JWKSURI:  ts.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer not-a-valid-jwt")

	_, err = authFunc(req)
	if err == nil {
		t.Fatal("expected error for invalid token")
	}
}

func TestNewAuthenticateFunc_CustomPrincipalClaim(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	kid := "test-key-1"
	ts := testKeyServer(t, &key.PublicKey, kid)
	defer ts.Close()

	authFunc, cleanup, err := NewAuthenticateFunc(JWTAuthConfig{
		Issuer:         "https://auth.example.com",
		Audience:       "https://api.example.com",
		JWKSURI:        ts.URL,
		PrincipalClaim: "email",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	tokenStr := generateTestToken(t, key, kid, jwt.MapClaims{
		"iss":   "https://auth.example.com",
		"aud":   "https://api.example.com",
		"sub":   "user123",
		"email": "user@example.com",
		"exp":   jwt.NewNumericDate(time.Now().Add(time.Hour)),
	})

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer "+tokenStr)

	auth, err := authFunc(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if auth.Principal != "user@example.com" {
		t.Errorf("expected principal=user@example.com, got %s", auth.Principal)
	}
}

