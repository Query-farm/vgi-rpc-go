// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// Golden vectors produced by the Python reference implementation. Verifying
// these is the only thing that proves Go frames the canonical string
// identically — a port can round-trip perfectly against itself while framing
// the MAC input differently from every other language.
const (
	goldenToken  = "v1.conformance-proxy.1700000000.Q0ZPUk1BTkNFTk9OQ0UxMQ.XQ2QBf35oajjaP7HIas3OfyEvNhyXTTptbrxWFxWk3I"
	goldenOrigin = "conformance-origin"
	goldenKid    = "conformance-proxy"
	goldenTime   = 1700000000
	// derive_secret(bytes(range(32)), "prod-use1", "worker-a")
	goldenDerived = "af85db125b8270bc0a0971736340dc8476ba70e1fad472b72b68ba739bd1cd94"
)

func goldenSecret(t *testing.T) []byte {
	t.Helper()
	raw, err := hex.DecodeString(strings.Repeat("11", 32))
	if err != nil {
		t.Fatalf("decode secret: %v", err)
	}
	return raw
}

func testConfig(t *testing.T, secret []byte, now int64) *ProofConfig {
	t.Helper()
	return &ProofConfig{
		Mode:        ProofModeRequire,
		OriginID:    goldenOrigin,
		Secrets:     map[string]ProofSecret{goldenKid: {Secret: secret, Label: goldenKid}},
		SkewSeconds: 30,
		Now:         func() time.Time { return time.Unix(now, 0) },
	}
}

func TestVerifiesPythonMintedToken(t *testing.T) {
	claims, err := VerifyProof(goldenToken, testConfig(t, goldenSecret(t), goldenTime), nil)
	if err != nil {
		t.Fatalf("cross-language token rejected: %v", err)
	}
	if claims["verified"] != "true" || claims["proxy"] != goldenKid {
		t.Fatalf("unexpected claims: %v", claims)
	}
}

func TestDerivationMatchesPython(t *testing.T) {
	base := make([]byte, 32)
	for i := range base {
		base[i] = byte(i)
	}
	got, err := DeriveProofSecret(base, "prod-use1", "worker-a")
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if hex.EncodeToString(got) != goldenDerived {
		t.Fatalf("derivation diverged from Python:\n got %s\nwant %s", hex.EncodeToString(got), goldenDerived)
	}
}

func TestDerivationSeparatorIsUnambiguous(t *testing.T) {
	base := make([]byte, 32)
	a, _ := DeriveProofSecret(base, "ab", "c.d")
	b, _ := DeriveProofSecret(base, "a", "b.c.d")
	if hex.EncodeToString(a) == hex.EncodeToString(b) {
		t.Fatal("component boundaries can be shifted between proxy and origin ids")
	}
}

func TestMintVerifyRoundTrip(t *testing.T) {
	secret := goldenSecret(t)
	token, err := MintProof(secret, goldenKid, goldenOrigin, goldenTime, "Q0ZPUk1BTkNFTk9OQ0UxMQ")
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	if token != goldenToken {
		t.Fatalf("Go mint diverged from Python:\n got %s\nwant %s", token, goldenToken)
	}
}

func TestMalformedRejected(t *testing.T) {
	cfg := testConfig(t, goldenSecret(t), goldenTime)
	cases := map[string]string{
		"empty":         "",
		"not dotted":    "garbage",
		"four fields":   "v1.a.b.c",
		"six fields":    "v1.a.b.c.d.e",
		"wrong version": "v2." + goldenKid + ".1.Q0ZPUk1BTkNFTk9OQ0UxMQ." + strings.Repeat("A", 43),
		"kid charset":   "v1.bad!kid.1.Q0ZPUk1BTkNFTk9OQ0UxMQ." + strings.Repeat("A", 43),
		"ts charset":    "v1." + goldenKid + ".xyz.Q0ZPUk1BTkNFTk9OQ0UxMQ." + strings.Repeat("A", 43),
		"nonce charset": "v1." + goldenKid + ".1.short." + strings.Repeat("A", 43),
		"mac charset":   "v1." + goldenKid + ".1.Q0ZPUk1BTkNFTk9OQ0UxMQ.!!!",
		"oversized":     "v1." + strings.Repeat("x", 600),
	}
	for name, token := range cases {
		claims, err := VerifyProof(token, cfg, nil)
		if err == nil {
			t.Errorf("%s: accepted %v", name, claims)
			continue
		}
		pe, ok := err.(*ProofError)
		if !ok || pe.Reason != "malformed" {
			t.Errorf("%s: expected malformed, got %v", name, err)
		}
	}
}

func TestUnknownKidRejected(t *testing.T) {
	cfg := testConfig(t, goldenSecret(t), goldenTime)
	cfg.Secrets = map[string]ProofSecret{"other": {Secret: goldenSecret(t), Label: "other"}}
	_, err := VerifyProof(goldenToken, cfg, nil)
	pe, ok := err.(*ProofError)
	if !ok || pe.Reason != "unknown_kid" {
		t.Fatalf("expected unknown_kid, got %v", err)
	}
}

func TestWrongOriginRejected(t *testing.T) {
	// Audience binding: the origin id is folded into the MAC but never
	// transmitted, so it cannot be adjusted by the caller.
	cfg := testConfig(t, goldenSecret(t), goldenTime)
	cfg.OriginID = "some-other-worker"
	_, err := VerifyProof(goldenToken, cfg, nil)
	pe, ok := err.(*ProofError)
	if !ok || pe.Reason != "bad_mac" {
		t.Fatalf("expected bad_mac, got %v", err)
	}
}

func TestTimeWindowIsTwoSided(t *testing.T) {
	secret := goldenSecret(t)
	// The future case is what catches a verifier checking only an upper
	// bound, which would let a future-dated proof pass indefinitely.
	for name, now := range map[string]int64{
		"expired":       goldenTime + 91,
		"not_yet_valid": goldenTime - 91,
	} {
		_, err := VerifyProof(goldenToken, testConfig(t, secret, now), nil)
		pe, ok := err.(*ProofError)
		if !ok || pe.Reason != name {
			t.Errorf("%s: expected %s, got %v", name, name, err)
		}
	}
	if _, err := VerifyProof(goldenToken, testConfig(t, secret, goldenTime+20), nil); err != nil {
		t.Errorf("inside window rejected: %v", err)
	}
}

func TestMacFramingMustBeSeparated(t *testing.T) {
	// A MAC computed over concatenated-without-separators fields must not
	// verify. Catches a port whose crypto is right but whose framing is not —
	// the failure a self-round-trip cannot see.
	secret := goldenSecret(t)
	bad := append([]byte("vgi.proxy.proof.v1"), []byte(goldenKid+"1700000000Q0ZPUk1BTkNFTk9OQ0UxMQ"+goldenOrigin)...)
	mac := hmacSum(secret, bad)
	token := strings.Join([]string{"v1", goldenKid, "1700000000", "Q0ZPUk1BTkNFTk9OQ0UxMQ", proofB64(mac)}, ".")
	_, err := VerifyProof(token, testConfig(t, secret, goldenTime), nil)
	pe, ok := err.(*ProofError)
	if !ok || pe.Reason != "bad_mac" {
		t.Fatalf("expected bad_mac for mis-framed canonical string, got %v", err)
	}
}

func TestReplayRejected(t *testing.T) {
	cache := newNonceCache(30*time.Second, 100, func() time.Time { return time.Unix(goldenTime, 0) })
	cfg := testConfig(t, goldenSecret(t), goldenTime)
	if _, err := VerifyProof(goldenToken, cfg, cache); err != nil {
		t.Fatalf("first use rejected: %v", err)
	}
	_, err := VerifyProof(goldenToken, cfg, cache)
	pe, ok := err.(*ProofError)
	if !ok || pe.Reason != "replayed" {
		t.Fatalf("expected replayed, got %v", err)
	}
}

func TestNonceCacheCapacityIsHard(t *testing.T) {
	// A TTL bounds how long an entry lives, never how many arrive inside the
	// window, so a TTL-only cache is a remote memory-exhaustion vector.
	now := time.Unix(goldenTime, 0)
	cache := newNonceCache(time.Hour, 10, func() time.Time { return now })
	for i := 0; i < 500; i++ {
		cache.checkAndAdd(strings.Repeat("a", 20) + string(rune('A'+i%26)) + string(rune('a'+i/26)))
	}
	if got := len(cache.entries); got > 10 {
		t.Fatalf("capacity cap not enforced: %d entries", got)
	}
}

func TestNonceCacheConcurrentReplayAdmitsOne(t *testing.T) {
	// Test-and-set must be atomic: a separate contains-then-add would let two
	// goroutines both observe "not seen" and both be served.
	cache := newNonceCache(time.Minute, 1000, nil)
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := 0
	start := make(chan struct{})
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if cache.checkAndAdd("contested-nonce-value") {
				mu.Lock()
				accepted++
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()
	if accepted != 1 {
		t.Fatalf("expected exactly one acceptance, got %d", accepted)
	}
}

func TestProofAuthenticateModes(t *testing.T) {
	secret := goldenSecret(t)
	base := ProofConfig{
		OriginID:    goldenOrigin,
		Secrets:     map[string]ProofSecret{goldenKid: {Secret: secret, Label: goldenKid}},
		SkewSeconds: 30,
		Now:         func() time.Time { return time.Unix(goldenTime, 0) },
	}

	t.Run("require rejects missing", func(t *testing.T) {
		cfg := base
		cfg.Mode = ProofModeRequire
		fn, err := ProofAuthenticate(cfg, nil)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		if _, err := fn(httptest.NewRequest("POST", "/x", nil)); err == nil {
			t.Fatal("missing proof accepted")
		}
	})

	t.Run("require accepts valid and attributes", func(t *testing.T) {
		cfg := base
		cfg.Mode = ProofModeRequire
		fn, _ := ProofAuthenticate(cfg, nil)
		r := httptest.NewRequest("POST", "/x", nil)
		r.Header.Set(ProofHeader, goldenToken)
		ctx, err := fn(r)
		if err != nil {
			t.Fatalf("valid proof rejected: %v", err)
		}
		if ctx.Principal != goldenKid {
			t.Fatalf("expected attribution to %s, got %q", goldenKid, ctx.Principal)
		}
	})

	t.Run("require message does not echo input", func(t *testing.T) {
		cfg := base
		cfg.Mode = ProofModeRequire
		fn, _ := ProofAuthenticate(cfg, nil)
		r := httptest.NewRequest("POST", "/x", nil)
		r.Header.Set(ProofHeader, "v1.attacker-controlled.1.Q0ZPUk1BTkNFTk9OQ0UxMQ."+strings.Repeat("A", 43))
		_, err := fn(r)
		if err == nil || strings.Contains(err.Error(), "attacker-controlled") {
			t.Fatalf("rejection reflected caller input: %v", err)
		}
	})

	t.Run("allow does not deny", func(t *testing.T) {
		cfg := base
		cfg.Mode = ProofModeAllow
		fn, _ := ProofAuthenticate(cfg, nil)
		ctx, err := fn(httptest.NewRequest("POST", "/x", nil))
		if err != nil {
			t.Fatalf("allow mode denied: %v", err)
		}
		claims := ctx.Claims[proofClaimsKey].(map[string]any)
		if claims["verified"] != "false" || claims["reason"] != "no_proof" {
			t.Fatalf("allow mode lost the outcome: %v", claims)
		}
	})

	t.Run("off refuses to build", func(t *testing.T) {
		cfg := base
		cfg.Mode = ProofModeOff
		if _, err := ProofAuthenticate(cfg, nil); err == nil {
			t.Fatal("off mode should install no gate rather than a passing one")
		}
	})
}

// The capability header is what lets a proxy distinguish an enforcing worker
// from one that silently ignores the proof — the misconfiguration that makes
// the whole feature a no-op. Only require mode may claim it; allow never
// denies, so advertising there would be a lie, and an unconditional emission
// would pass the require case while failing this one.
func TestProxyProofRequiredCapabilityHeader(t *testing.T) {
	for _, tc := range []struct {
		mode ProofMode
		want string
	}{
		{ProofModeRequire, "true"},
		{ProofModeAllow, ""},
		{ProofModeOff, ""},
	} {
		t.Run(string(tc.mode), func(t *testing.T) {
			h := newTestHttpServer(t)
			h.SetCorsOrigins("*")
			// Exactly the wiring the conformance worker performs.
			h.SetProxyProofRequired(tc.mode == ProofModeRequire)
			h.InitPages()

			ts := httptest.NewServer(h)
			defer ts.Close()

			resp, err := http.Get(ts.URL + "/health")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()

			if got := resp.Header.Get(ProofRequiredHeader); got != tc.want {
				t.Fatalf("%s: expected %s=%q, got %q", tc.mode, ProofRequiredHeader, tc.want, got)
			}
			// A header a browser client cannot read is not advertised.
			exposed := strings.Contains(resp.Header.Get("Access-Control-Expose-Headers"), ProofRequiredHeader)
			if exposed != (tc.want != "") {
				t.Fatalf("%s: expected %s in Access-Control-Expose-Headers=%v, got %v",
					tc.mode, ProofRequiredHeader, tc.want != "", exposed)
			}
		})
	}
}

func TestParseProofSecrets(t *testing.T) {
	parsed, err := ParseProofSecrets("prod-use1:" + strings.Repeat("11", 32))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed["prod-use1"].Label != "prod-use1" {
		t.Fatalf("kid should double as the label, got %q", parsed["prod-use1"].Label)
	}
	for _, bad := range []string{"prod-use1", "prod-use1:zz", "bad!kid:" + strings.Repeat("11", 32), ""} {
		if _, err := ParseProofSecrets(bad); err == nil {
			t.Errorf("accepted malformed spec %q", bad)
		}
	}
}

func hmacSum(key, msg []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(msg)
	return m.Sum(nil)
}
