// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Sealed grants against the reference's byte-exact vectors
// (vgi_rpc/conformance/grant_token_vectors.json, vendored in testdata). The
// HTTP behaviour -- which authenticator answers, 401 vs 503, the chain order --
// is asserted by the shared suite's TestSealedGrants, TestSealedGrantRejections,
// TestGrantPrefixRouting and TestResolveTokenBearer against the grant worker;
// these cover the pure functions and the Go-API guards.

type grantVectorFile struct {
	Defaults struct {
		VerifyKeys []string `json:"verify_keys_b64"`
		Audience   string   `json:"audience"`
		MaxTTL     int64    `json:"max_ttl_seconds"`
		Skew       int64    `json:"clock_skew_seconds"`
		Now        int64    `json:"now"`
	} `json:"defaults"`
	Mint []struct {
		Name       string `json:"name"`
		MintingKey string `json:"minting_key_b64"`
		Audience   string `json:"audience"`
		MaxTTL     int64  `json:"max_ttl_seconds"`
		NonceHex   string `json:"nonce_hex"`
		Now        int64  `json:"now"`
		Request    struct {
			Principal  string   `json:"principal"`
			Scopes     []string `json:"scopes"`
			Purpose    string   `json:"purpose"`
			TTLSeconds int64    `json:"ttl_seconds"`
			GrantID    string   `json:"grant_id"`
		} `json:"request"`
		Claims struct {
			IssuedAt  int64 `json:"issued_at"`
			ExpiresAt int64 `json:"expires_at"`
		} `json:"claims"`
		KIDHex     string `json:"kid_hex"`
		AADHex     string `json:"aad_hex"`
		PayloadHex string `json:"payload_hex"`
		Token      string `json:"token"`
	} `json:"mint"`
	Accept []grantVectorCase `json:"accept"`
	Reject []grantVectorCase `json:"reject"`
}

type grantVectorCase struct {
	Name       string   `json:"name"`
	Token      string   `json:"token"`
	Now        *int64   `json:"now"`
	VerifyKeys []string `json:"verify_keys_b64"`
	Audience   *string  `json:"audience"`
	Expired    bool     `json:"expired"`
}

func loadGrantVectors(t *testing.T) grantVectorFile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "grant_token_vectors.json"))
	if err != nil {
		t.Fatal(err)
	}
	// The vendored copy must not drift from the reference when one is at hand.
	if repo := os.Getenv("VGI_RPC_PYTHON_REPO"); repo != "" {
		if ref, err := os.ReadFile(filepath.Join(repo, "vgi_rpc", "conformance", "grant_token_vectors.json")); err == nil &&
			string(ref) != string(raw) {
			t.Fatal("testdata/grant_token_vectors.json differs from the reference's; re-copy it")
		}
	}
	var v grantVectorFile
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestGrantMintVectors(t *testing.T) {
	v := loadGrantVectors(t)
	if len(v.Mint) == 0 {
		t.Fatal("no mint vectors")
	}
	for _, m := range v.Mint {
		keys, err := ParseGrantKeys([]string{m.MintingKey}, m.Audience, m.MaxTTL)
		if err != nil {
			t.Fatalf("%s: %v", m.Name, err)
		}
		nonce, _ := hex.DecodeString(m.NonceHex)
		now := m.Now
		token, claims, err := MintGrantToken(keys, MintGrantOptions{
			Principal: m.Request.Principal, Scopes: m.Request.Scopes, Purpose: m.Request.Purpose,
			TTLSeconds: m.Request.TTLSeconds, Now: &now, GrantID: m.Request.GrantID, Nonce: nonce,
		})
		if err != nil {
			t.Fatalf("%s: mint: %v", m.Name, err)
		}
		if token != m.Token {
			t.Errorf("%s: token\n got  %s\n want %s", m.Name, token, m.Token)
		}
		if claims.IssuedAt != m.Claims.IssuedAt || claims.ExpiresAt != m.Claims.ExpiresAt {
			t.Errorf("%s: lifetime %d..%d, want %d..%d", m.Name, claims.IssuedAt, claims.ExpiresAt,
				m.Claims.IssuedAt, m.Claims.ExpiresAt)
		}
		key, _ := base64.StdEncoding.DecodeString(m.MintingKey)
		kid := GrantKeyID(key)
		if hex.EncodeToString(kid) != m.KIDHex {
			t.Errorf("%s: kid %x, want %s", m.Name, kid, m.KIDHex)
		}
		if got := hex.EncodeToString(keys.aad(kid)); got != m.AADHex {
			t.Errorf("%s: aad %s, want %s", m.Name, got, m.AADHex)
		}
		payload, _ := encodeGrantPayload(claims)
		if hex.EncodeToString(payload) != m.PayloadHex {
			t.Errorf("%s: payload %x, want %s", m.Name, payload, m.PayloadHex)
		}
		// And it verifies under the key that minted it.
		if _, err := VerifyGrantToken(keys, token, time.Unix(m.Now+1, 0)); err != nil {
			t.Errorf("%s: own token does not verify: %v", m.Name, err)
		}
	}
}

func grantCaseKeys(t *testing.T, v grantVectorFile, c grantVectorCase) (*GrantKeys, time.Time) {
	t.Helper()
	verify := v.Defaults.VerifyKeys
	if c.VerifyKeys != nil {
		verify = c.VerifyKeys
	}
	audience := v.Defaults.Audience
	if c.Audience != nil {
		audience = *c.Audience
	}
	keys, err := ParseGrantKeys(verify, audience, v.Defaults.MaxTTL)
	if err != nil {
		t.Fatalf("%s: %v", c.Name, err)
	}
	if v.Defaults.Skew != DefaultGrantClockSkewSeconds {
		t.Fatalf("vector skew %d differs from DefaultGrantClockSkewSeconds", v.Defaults.Skew)
	}
	now := v.Defaults.Now
	if c.Now != nil {
		now = *c.Now
	}
	return keys, time.Unix(now, 0)
}

func TestGrantAcceptVectors(t *testing.T) {
	v := loadGrantVectors(t)
	for _, c := range v.Accept {
		keys, now := grantCaseKeys(t, v, c)
		if _, err := VerifyGrantToken(keys, c.Token, now); err != nil {
			t.Errorf("%s: rejected: %v", c.Name, err)
		}
	}
}

func TestGrantRejectVectors(t *testing.T) {
	v := loadGrantVectors(t)
	if len(v.Reject) == 0 {
		t.Fatal("no reject vectors")
	}
	for _, c := range v.Reject {
		keys, now := grantCaseKeys(t, v, c)
		_, err := VerifyGrantToken(keys, c.Token, now)
		var invalid *GrantInvalidError
		if !errors.As(err, &invalid) {
			t.Errorf("%s: got %v, want a GrantInvalidError", c.Name, err)
			continue
		}
		if invalid.Expired != c.Expired {
			t.Errorf("%s: expired=%v, want %v (%s)", c.Name, invalid.Expired, c.Expired, invalid.Detail)
		}
	}
}

func TestGrantKeyConfigurationIsValidatedAtStartup(t *testing.T) {
	good := base64.StdEncoding.EncodeToString(make([]byte, 32))
	other := base64.RawStdEncoding.EncodeToString([]byte(strings.Repeat("k", 32)))
	if keys, err := GrantKeysFromValues(nil, "", "", ""); keys != nil || err != nil {
		t.Fatalf("no keys must mean grants off, got %v, %v", keys, err)
	}
	if keys, err := GrantKeysFromValues(nil, good+" , "+other, "aud", "120"); err != nil ||
		keys.Audience() != "aud" || keys.MaxTTLSeconds() != 120 || len(keys.keys) != 2 {
		t.Fatalf("env keys: %v, %v", keys, err)
	}
	if keys, _ := GrantKeysFromValues([]string{other}, good, "", ""); string(keys.keys[0]) != strings.Repeat("k", 32) {
		t.Fatal("--grant-key must replace the environment's keys")
	}
	for name, args := range map[string][4]string{
		"short key":    {"", base64.StdEncoding.EncodeToString(make([]byte, 31)), "", ""},
		"not base64":   {"", "!!!!", "", ""},
		"duplicate":    {"", good + "," + good, "", ""},
		"zero ttl":     {"", good, "", "0"},
		"negative ttl": {"", good, "", "-5"},
		"bad ttl":      {"", good, "", "soon"},
	} {
		if _, err := GrantKeysFromValues(nil, args[1], args[2], args[3]); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// --- IdentityImpl: sealed mint, and grants never minting grants ---

func grantTestKeys(t *testing.T) *GrantKeys {
	t.Helper()
	k := make([]byte, 32)
	for i := range k {
		k[i] = byte(0x10 + i)
	}
	keys, err := NewGrantKeys([][]byte{k}, "aud", 3600)
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func TestSealedMintWhenNoHookAndGrantsCannotMintGrants(t *testing.T) {
	keys := grantTestKeys(t)
	impl, err := NewIdentity(IdentityConfig{GrantKeys: keys})
	if err != nil {
		t.Fatal(err)
	}
	if got := impl.OfferedMethods(); len(got) != 1 || got[0] != "issue_grant" {
		t.Fatalf("grant keys alone must host issue_grant, offered %v", got)
	}
	fresh := idAuth("alice", true, float64(time.Now().Unix()))
	grant, err := impl.IssueGrant("nightly", []string{"read"}, 10_000_000, idCtx(fresh))
	if err != nil {
		t.Fatal(err)
	}
	claims, err := VerifyGrantToken(keys, grant.Token, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if claims.Principal != "alice" || claims.ExpiresAt-claims.IssuedAt != 3600 || claims.GrantID != grant.GrantID ||
		grant.ExpiresAt != float64(claims.ExpiresAt) {
		t.Fatalf("minted claims %+v for grant %+v", claims, grant)
	}
	if _, err := impl.IssueGrant("x", nil, 0, idCtx(fresh)); !isKind(err, "grant_refused") {
		t.Fatalf("ttl 0: %v, want grant_refused", err)
	}

	// Authenticate with that grant, then ask for another: stale_auth.
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.Header.Set("Authorization", "Bearer "+grant.Token)
	auth, err := GrantAuthenticate(keys)(r)
	if err != nil {
		t.Fatal(err)
	}
	if _, has := auth.Claims["auth_time"]; has || auth.Domain != GrantAuthDomain {
		t.Fatalf("grant AuthContext %+v must carry no auth_time", auth)
	}
	if _, err := impl.IssueGrant("child", nil, 60, idCtx(auth)); !isKind(err, "stale_auth") {
		t.Fatalf("a grant minted a grant: %v", err)
	}
}

func isKind(err error, kind string) bool { return ErrorKindOf(err) == kind }

// --- The chain: prefix routing, no fall-through, resolver guards ---

func bearerRequest(token string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	return r
}

func TestIdentityBearerChain(t *testing.T) {
	keys := grantTestKeys(t)
	resolverCalls := 0
	resolver := func(token string) (TokenIdentity, bool, error) {
		resolverCalls++
		switch token {
		case "unknown":
			return TokenIdentity{}, false, nil
		case "down":
			return TokenIdentity{}, false, &IdentityUnavailableError{RetryAfter: 5}
		case "auth-down":
			return TokenIdentity{}, false, &AuthUnavailableError{RetryAfter: 7}
		}
		return TokenIdentity{Principal: "resolved:" + token, TokenName: "n"}, true, nil
	}
	chain, err := ComposeIdentityAuthenticate(nil, keys, resolver, false)
	if err != nil {
		t.Fatal(err)
	}
	good, _, _ := MintGrantToken(keys, MintGrantOptions{Principal: "alice", TTLSeconds: 60})
	expiredNow := time.Now().Unix() - 3000
	expired, _, _ := MintGrantToken(keys, MintGrantOptions{Principal: "alice", TTLSeconds: 60, Now: &expiredNow})
	body := good[len(GrantTokenPrefix):]

	auth, err := chain(bearerRequest(good))
	if err != nil || auth.Domain != GrantAuthDomain || auth.Principal != "alice" {
		t.Fatalf("good grant: %+v, %v", auth, err)
	}

	// A bad vgig1. token is a 401 that stops the chain: the resolver, which
	// answers for almost anything, must not be reached.
	for name, tc := range map[string]struct {
		token  string
		reason AuthReason
	}{
		"tampered": {good[:len(good)-6] + flipChar(good[len(good)-6]) + good[len(good)-5:], AuthReasonInvalidCredential},
		"padded":   {good + "=", AuthReasonInvalidCredential},
		"expired":  {expired, AuthReasonExpiredCredential},
		"garbage":  {GrantTokenPrefix + "!!", AuthReasonInvalidCredential},
	} {
		resolverCalls = 0
		_, err := chain(bearerRequest(tc.token))
		var failure *AuthFailure
		if !errors.As(err, &failure) || failure.Reason != tc.reason {
			t.Errorf("%s: %v, want AuthFailure %s", name, err, tc.reason)
		}
		if resolverCalls != 0 {
			t.Errorf("%s: a vgig1. token fell through to resolve_token", name)
		}
	}

	// Only the exact prefix routes to the verifier: these reach the resolver.
	for _, token := range []string{"vgig2." + body, body, "VGIG1." + body} {
		auth, err := chain(bearerRequest(token))
		if err != nil || auth.Domain != TokenAuthDomain {
			t.Errorf("%.10q...: %+v, %v; want domain token", token, auth, err)
		}
	}

	// The resolver's guards and outcomes.
	resolverCalls = 0
	for _, token := range []string{"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.c2lnbmF0dXJl", strings.Repeat("x", MaxTokenBytes+1), "   "} {
		if _, err := chain(bearerRequest(token)); err == nil {
			t.Errorf("%.12q...: accepted", token)
		}
	}
	if resolverCalls != 0 {
		t.Error("a JWS, over-long or blank token reached resolve_token")
	}
	if _, err := chain(bearerRequest("unknown")); err == nil {
		t.Error("an unresolved bearer was accepted")
	}
	for token, retry := range map[string]int{"down": 5, "auth-down": 7} {
		_, err := chain(bearerRequest(token))
		var unavailable *AuthUnavailableError
		if !errors.As(err, &unavailable) || unavailable.retryAfterSeconds() != retry {
			t.Errorf("%s: %v, want AuthUnavailableError with Retry-After %d", token, err, retry)
		}
	}
	if auth, err := chain(bearerRequest("")); err != nil || auth.Authenticated {
		t.Errorf("no credential must stay anonymous: %+v, %v", auth, err)
	}
	auth, err = chain(bearerRequest("opaque"))
	if err != nil || auth.Principal != "resolved:opaque" || auth.Claims["token_name"] != "n" {
		t.Errorf("resolved bearer: %+v, %v", auth, err)
	}
	if _, has := auth.Claims["auth_time"]; has {
		t.Error("a resolved bearer carries auth_time")
	}
}

func flipChar(c byte) string {
	if c == 'A' {
		return "B"
	}
	return "A"
}

// A deployment authenticator that depends on proxy evidence cannot have
// alternatives OR-ed beside it: the HttpServer refuses to compose.
func TestIdentityBearerRefusesBesideAProxyGate(t *testing.T) {
	s := NewServer()
	impl, err := NewIdentity(IdentityConfig{GrantKeys: grantTestKeys(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := RegisterIdentity(s, impl); err != nil {
		t.Fatal(err)
	}
	h := NewHttpServer(s)
	h.SetAuthenticate(func(*http.Request) (*AuthContext, error) { return Anonymous(), nil })
	h.SetProxyAuthHeaders("X-Proxy-Identity")
	if err := h.InitIdentityBearer(); !errors.Is(err, ErrIdentityBearerBesideProxyGate) {
		t.Fatalf("InitIdentityBearer = %v, want the refusal", err)
	}

	optOut := NewHttpServer(s)
	optOut.SetAuthenticate(func(*http.Request) (*AuthContext, error) { return Anonymous(), nil })
	optOut.SetProxyAuthHeaders("X-Proxy-Identity")
	optOut.SetIdentityBearer(false)
	if err := optOut.InitIdentityBearer(); err != nil {
		t.Fatalf("an explicit opt-out must start: %v", err)
	}
}
