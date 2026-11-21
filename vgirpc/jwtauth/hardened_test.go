// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package jwtauth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/golang-jwt/jwt/v5"
)

const (
	hIssuer   = "https://auth.orchard.test"
	hAudience = "https://desk.orchard.test/d/dep_123"
)

// fakeClock replaces timeNow for the duration of a test.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func useFakeClock(t *testing.T) *fakeClock {
	t.Helper()
	c := &fakeClock{now: time.Now()}
	prev := timeNow
	timeNow = c.Now
	t.Cleanup(func() { timeNow = prev })
	return c
}

// jwksServer serves a mutable JWKS with controllable failures and
// Cache-Control, counting fetches.
type jwksServer struct {
	mu           sync.Mutex
	keys         []map[string]any
	fail         bool
	cacheControl string
	fetches      atomic.Int64
	srv          *httptest.Server
}

func newJWKSServer(t *testing.T, keys ...map[string]any) *jwksServer {
	t.Helper()
	s := &jwksServer{keys: keys}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		s.fetches.Add(1)
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.fail {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		if s.cacheControl != "" {
			w.Header().Set("Cache-Control", s.cacheControl)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": s.keys})
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *jwksServer) set(fn func(s *jwksServer)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(s)
}

func ecJWK(key *ecdsa.PublicKey, kid string) map[string]any {
	return map[string]any{
		"kty": "EC", "crv": "P-256", "kid": kid, "use": "sig", "alg": "ES256",
		"x": base64.RawURLEncoding.EncodeToString(ecPoint(key)[1:33]),
		"y": base64.RawURLEncoding.EncodeToString(ecPoint(key)[33:65]),
	}
}

func rsaJWK(key *rsa.PublicKey, kid string) map[string]any {
	return map[string]any{
		"kty": "RSA", "kid": kid, "use": "sig", "alg": "RS256",
		"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
	}
}

func passClaims(overrides map[string]any) jwt.MapClaims {
	now := time.Now().Unix()
	claims := jwt.MapClaims{
		"iss": hIssuer, "aud": hAudience, "sub": "user_01M7KNJYC18T", "kind": "user",
		"email": "ada@example.com", "email_verified": true, "name": "Ada Lovelace",
		"org_id": "org_01M7", "org_name": "Acme Analytics", "client_id": "vgi-duckdb",
		"iat": now, "nbf": now, "exp": now + 900, "jti": "jti-1",
	}
	for k, v := range overrides {
		if v == nil {
			delete(claims, k)
		} else {
			claims[k] = v
		}
	}
	return claims
}

func signPass(t *testing.T, method jwt.SigningMethod, key any, kid, typ string, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(method, claims)
	if kid != "" {
		tok.Header["kid"] = kid
	}
	if typ == "" {
		delete(tok.Header, "typ")
	} else {
		tok.Header["typ"] = typ
	}
	s, err := tok.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// orchardConfig is the configuration Orchard hosted mode uses.
func orchardConfig(jwksURI string) JWTAuthConfig {
	return JWTAuthConfig{
		Issuer: hIssuer, Audience: []string{hAudience}, JWKSURI: jwksURI, Domain: "orchard",
		Algorithms: []string{"ES256"}, RequiredTyp: "at+jwt", RequireExp: true, SingleAudience: true,
		Leeway: 60 * time.Second, JWKSMaxAge: 10 * time.Minute, JWKSRefreshMinInterval: time.Minute,
		JWKSFetchTimeout: 10 * time.Second,
	}
}

func call(t *testing.T, fn vgirpc.AuthenticateFunc, token string) (*vgirpc.AuthContext, error) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return fn(req)
}

func wantReason(t *testing.T, err error, reason vgirpc.AuthReason) {
	t.Helper()
	var failure *vgirpc.AuthFailure
	if !errors.As(err, &failure) {
		t.Fatalf("expected AuthFailure(%s), got %T %v", reason, err, err)
	}
	if failure.Reason != reason {
		t.Fatalf("reason = %s, want %s", failure.Reason, reason)
	}
}

func wantUnavailable(t *testing.T, err error) {
	t.Helper()
	var unavailable *vgirpc.AuthUnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("expected AuthUnavailableError, got %T %v", err, err)
	}
}

type fixture struct {
	ec   *ecdsa.PrivateKey
	rsa  *rsa.PrivateKey
	jwks *jwksServer
	auth vgirpc.AuthenticateFunc
}

func newFixture(t *testing.T, mutate func(*JWTAuthConfig)) *fixture {
	t.Helper()
	ec, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	rk, _ := rsa.GenerateKey(rand.Reader, 2048)
	jwks := newJWKSServer(t, ecJWK(&ec.PublicKey, "desk-es256-1"), rsaJWK(&rk.PublicKey, "desk-rs256-1"))
	cfg := orchardConfig(jwks.srv.URL)
	if mutate != nil {
		mutate(&cfg)
	}
	auth, cleanup, err := NewAuthenticateFunc(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cleanup)
	return &fixture{ec: ec, rsa: rk, jwks: jwks, auth: auth}
}

func (f *fixture) pass(t *testing.T, overrides map[string]any) string {
	return signPass(t, jwt.SigningMethodES256, f.ec, "desk-es256-1", "at+jwt", passClaims(overrides))
}

func TestHardenedValidPassKeepsJSONTypes(t *testing.T) {
	f := newFixture(t, nil)
	ac, err := call(t, f.auth, f.pass(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	if ac.Principal != "user_01M7KNJYC18T" || ac.Domain != "orchard" || !ac.Authenticated {
		t.Fatalf("unexpected auth context %+v", ac)
	}
	if v, ok := ac.Claims["email_verified"].(bool); !ok || !v {
		t.Fatalf("email_verified = %#v, want bool true", ac.Claims["email_verified"])
	}
	if _, ok := ac.Claims["exp"].(int64); !ok {
		t.Fatalf("exp = %#v (%T), want int64", ac.Claims["exp"], ac.Claims["exp"])
	}
	for _, name := range []string{"kind", "email", "name", "org_id", "org_name", "client_id", "jti"} {
		if _, ok := ac.Claims[name].(string); !ok {
			t.Fatalf("claim %s = %#v, want string", name, ac.Claims[name])
		}
	}
}

func TestHardenedTypVariants(t *testing.T) {
	f := newFixture(t, nil)
	for _, typ := range []string{"at+jwt", "AT+JWT", "application/at+jwt", "Application/AT+JWT"} {
		tok := signPass(t, jwt.SigningMethodES256, f.ec, "desk-es256-1", typ, passClaims(nil))
		if _, err := call(t, f.auth, tok); err != nil {
			t.Fatalf("typ %q rejected: %v", typ, err)
		}
	}
}

func TestHardenedRejections(t *testing.T) {
	f := newFixture(t, nil)
	now := time.Now().Unix()
	hmacKey := []byte(strings.Repeat("k", 32))
	cases := map[string]func() string{
		"garbage":   func() string { return "not-a-jwt" },
		"wrong_aud": func() string { return f.pass(t, map[string]any{"aud": "https://desk.orchard.test/d/other"}) },
		"multi_aud": func() string {
			return f.pass(t, map[string]any{"aud": []string{hAudience, "https://desk.orchard.test/d/other"}})
		},
		"no_aud":    func() string { return f.pass(t, map[string]any{"aud": nil}) },
		"wrong_iss": func() string { return f.pass(t, map[string]any{"iss": "https://evil.test"}) },
		"no_exp":    func() string { return f.pass(t, map[string]any{"exp": nil}) },
		"future_nbf": func() string {
			return f.pass(t, map[string]any{"nbf": now + 600})
		},
		"wrong_typ": func() string {
			return signPass(t, jwt.SigningMethodES256, f.ec, "desk-es256-1", "JWT", passClaims(nil))
		},
		"no_typ": func() string {
			return signPass(t, jwt.SigningMethodES256, f.ec, "desk-es256-1", "", passClaims(nil))
		},
		"rs256": func() string {
			return signPass(t, jwt.SigningMethodRS256, f.rsa, "desk-rs256-1", "at+jwt", passClaims(nil))
		},
		"hs256": func() string {
			return signPass(t, jwt.SigningMethodHS256, hmacKey, "desk-es256-1", "at+jwt", passClaims(nil))
		},
		"none": func() string {
			return signPass(t, jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType, "desk-es256-1", "at+jwt", passClaims(nil))
		},
		"tampered": func() string {
			parts := strings.Split(f.pass(t, nil), ".")
			raw, _ := base64.RawURLEncoding.DecodeString(parts[1])
			var claims map[string]any
			_ = json.Unmarshal(raw, &claims)
			claims["org_id"] = "org_EVIL"
			forged, _ := json.Marshal(claims)
			return parts[0] + "." + base64.RawURLEncoding.EncodeToString(forged) + "." + parts[2]
		},
		"other_key": func() string {
			other, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			return signPass(t, jwt.SigningMethodES256, other, "desk-es256-1", "at+jwt", passClaims(nil))
		},
	}
	for name, mk := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := call(t, f.auth, mk())
			wantReason(t, err, vgirpc.AuthReasonInvalidCredential)
		})
	}
}

func TestHardenedExpiredAndLeeway(t *testing.T) {
	f := newFixture(t, nil)
	now := time.Now().Unix()
	_, err := call(t, f.auth, f.pass(t, map[string]any{"iat": now - 1200, "nbf": now - 1200, "exp": now - 120}))
	wantReason(t, err, vgirpc.AuthReasonExpiredCredential)
	// 30 s past exp is inside the 60 s leeway.
	if _, err := call(t, f.auth, f.pass(t, map[string]any{"iat": now - 1200, "nbf": now - 1200, "exp": now - 30})); err != nil {
		t.Fatalf("pass inside the leeway rejected: %v", err)
	}
	// nbf 30 s in the future is inside the leeway too.
	if _, err := call(t, f.auth, f.pass(t, map[string]any{"nbf": now + 30})); err != nil {
		t.Fatalf("nbf inside the leeway rejected: %v", err)
	}
}

func TestHardenedMissingAndNonBearer(t *testing.T) {
	f := newFixture(t, nil)
	_, err := call(t, f.auth, "")
	wantReason(t, err, vgirpc.AuthReasonMissingCredential)
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Authorization", "Basic YWxhZGRpbjpvcGVuc2VzYW1l")
	_, err = f.auth(req)
	wantReason(t, err, vgirpc.AuthReasonInvalidCredential)
}

func TestHardenedAlgorithmsRefused(t *testing.T) {
	for _, algs := range [][]string{{"HS256"}, {"ES256", "hs512"}, {"none"}, {}, {" "}} {
		cfg := orchardConfig("http://127.0.0.1:9/jwks")
		cfg.Algorithms = algs
		if _, _, err := NewAuthenticateFunc(cfg); err == nil {
			t.Fatalf("algorithms %q accepted", algs)
		}
	}
}

func TestHardenedJWKSMaxAgeFailsClosed(t *testing.T) {
	clock := useFakeClock(t)
	f := newFixture(t, nil)
	if _, err := call(t, f.auth, f.pass(t, nil)); err != nil {
		t.Fatal(err)
	}
	if n := f.jwks.fetches.Load(); n != 1 {
		t.Fatalf("fetches = %d, want 1", n)
	}
	// Within the max age the cached keys answer, even with the endpoint down.
	f.jwks.set(func(s *jwksServer) { s.fail = true })
	clock.Advance(9 * time.Minute)
	if _, err := call(t, f.auth, f.pass(t, nil)); err != nil {
		t.Fatalf("cached keys refused inside the max age: %v", err)
	}
	// Past it, the keys are discarded and the failing re-fetch is a 503.
	clock.Advance(2 * time.Minute)
	_, err := call(t, f.auth, f.pass(t, nil))
	wantUnavailable(t, err)
	// Still 503 on the next request: never stale keys.
	_, err = call(t, f.auth, f.pass(t, nil))
	wantUnavailable(t, err)
	// Recovery as soon as the endpoint answers.
	f.jwks.set(func(s *jwksServer) { s.fail = false })
	if _, err := call(t, f.auth, f.pass(t, nil)); err != nil {
		t.Fatalf("not recovered after the JWKS came back: %v", err)
	}
}

func TestHardenedCacheControlShorterWins(t *testing.T) {
	clock := useFakeClock(t)
	f := newFixture(t, nil)
	f.jwks.set(func(s *jwksServer) { s.cacheControl = "public, max-age=30" })
	if _, err := call(t, f.auth, f.pass(t, nil)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(31 * time.Second)
	f.jwks.set(func(s *jwksServer) { s.fail = true })
	_, err := call(t, f.auth, f.pass(t, nil))
	wantUnavailable(t, err)
}

func TestHardenedCacheControlLongerIgnored(t *testing.T) {
	clock := useFakeClock(t)
	f := newFixture(t, nil)
	f.jwks.set(func(s *jwksServer) { s.cacheControl = "max-age=86400" })
	if _, err := call(t, f.auth, f.pass(t, nil)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(11 * time.Minute)
	f.jwks.set(func(s *jwksServer) { s.fail = true })
	_, err := call(t, f.auth, f.pass(t, nil))
	wantUnavailable(t, err)
}

func TestHardenedUnknownKIDRefetchRateLimited(t *testing.T) {
	clock := useFakeClock(t)
	f := newFixture(t, nil)
	if _, err := call(t, f.auth, f.pass(t, nil)); err != nil {
		t.Fatal(err)
	}
	rotated, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tok := signPass(t, jwt.SigningMethodES256, rotated, "desk-es256-2", "at+jwt", passClaims(nil))

	// Inside the interval since the last fetch: rejected without a fetch.
	clock.Advance(10 * time.Second)
	_, err := call(t, f.auth, tok)
	wantReason(t, err, vgirpc.AuthReasonInvalidCredential)
	if n := f.jwks.fetches.Load(); n != 1 {
		t.Fatalf("fetches = %d, want 1 (rate limited)", n)
	}

	// The key is published; after the interval one re-fetch picks it up.
	f.jwks.set(func(s *jwksServer) { s.keys = append(s.keys, ecJWK(&rotated.PublicKey, "desk-es256-2")) })
	clock.Advance(time.Minute)
	if _, err := call(t, f.auth, tok); err != nil {
		t.Fatalf("rotated key not picked up: %v", err)
	}
	if n := f.jwks.fetches.Load(); n != 2 {
		t.Fatalf("fetches = %d, want 2", n)
	}

	// A still-unknown kid right after is rate limited again.
	stranger, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	_, err = call(t, f.auth, signPass(t, jwt.SigningMethodES256, stranger, "desk-es256-9", "at+jwt", passClaims(nil)))
	wantReason(t, err, vgirpc.AuthReasonInvalidCredential)
	if n := f.jwks.fetches.Load(); n != 2 {
		t.Fatalf("fetches = %d, want 2", n)
	}
}

func TestHardenedFirstFetchFailureIs503(t *testing.T) {
	f := newFixture(t, nil)
	f.jwks.set(func(s *jwksServer) { s.fail = true })
	_, err := call(t, f.auth, f.pass(t, nil))
	wantUnavailable(t, err)
}

func TestCacheControlMaxAge(t *testing.T) {
	for in, want := range map[string]int{
		"max-age=30": 30, "public, max-age=600": 600, `max-age="45"`: 45, "MAX-AGE=5, must-revalidate": 5,
		"s-maxage=10": -1, "no-store": -1, "": -1,
	} {
		got, ok := cacheControlMaxAge(in)
		if want < 0 {
			if ok {
				t.Fatalf("%q: got %v, want none", in, got)
			}
			continue
		}
		if !ok || got != time.Duration(want)*time.Second {
			t.Fatalf("%q: got %v %v, want %ds", in, got, ok, want)
		}
	}
}

// The default (unhardened) verifier is unchanged: ValueError RpcErrors.
func TestDefaultConfigUnchanged(t *testing.T) {
	cfg := JWTAuthConfig{Issuer: hIssuer, Audience: []string{hAudience}, JWKSURI: "http://127.0.0.1:9/jwks"}
	if cfg.hardened() {
		t.Fatal("a plain configuration selected the hardened verifier")
	}
}

// ecPoint is the uncompressed P-256 point 0x04 || X || Y.
func ecPoint(key *ecdsa.PublicKey) []byte {
	b, err := key.Bytes()
	if err != nil || len(b) != 65 {
		panic("unexpected P-256 public key encoding")
	}
	return b
}

func TestHardenedNonKeySetIs503(t *testing.T) {
	ec, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	for _, body := range []string{`{"foo": 1}`, `[]`, `not json`, `{"keys": "x"}`} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(body))
		}))
		auth, _, err := NewAuthenticateFunc(orchardConfig(srv.URL))
		if err != nil {
			t.Fatal(err)
		}
		tok := signPass(t, jwt.SigningMethodES256, ec, "desk-es256-1", "at+jwt", passClaims(nil))
		_, err = call(t, auth, tok)
		wantUnavailable(t, err)
		srv.Close()
	}
}
