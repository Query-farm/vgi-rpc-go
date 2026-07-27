// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"container/list"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Proxy proof: HMAC evidence that a request arrived through a trusted proxy.
//
// A proxy mints a per-request HMAC-SHA256 over a timestamp, a fresh nonce and
// the worker's own identifier, keyed by a secret shared only with that worker.
// The proof establishes the hop, never the caller: it is ANDed with whatever
// authenticates the user rather than replacing it.
//
// Unlike a forwarded assertion about what happened at a TLS terminator, a
// proof cannot be produced by someone who merely reaches the worker directly —
// without the secret there is nothing to replay.
//
// The normative contract is vgi-rpc/docs/proxy-proof-spec.md.

// ProofHeader carries the proof on the wire.
const ProofHeader = "VGI-Proxy-Proof"

// ProofRequiredHeader advertises that this worker rejects unproofed requests.
const ProofRequiredHeader = "VGI-Proxy-Proof-Required"

const (
	proofVersion      = "v1"
	proofMaxHeaderLen = 512
	proofSecretLen    = 32
	proofClaimsKey    = "vgi_proxy_proof"
)

var proofDomainPrefix = []byte("vgi.proxy.proof.v1")

var (
	// Charsets are load-bearing, not cosmetic: the canonical string is
	// NUL-separated, so framing is only unambiguous because no field can
	// contain a NUL (and kid cannot contain the '.' separating wire fields).
	proofKidRe    = regexp.MustCompile(`\A[A-Za-z0-9_-]{1,64}\z`)
	proofTsRe     = regexp.MustCompile(`\A[0-9]{1,20}\z`)
	proofNonceRe  = regexp.MustCompile(`\A[A-Za-z0-9_-]{22}\z`)
	proofOriginRe = regexp.MustCompile(`\A[A-Za-z0-9._:/-]{1,255}\z`)
	// The MAC is charset-checked rather than left to the base64 decoder.
	// Decoders disagree about invalid input across languages, and the reason
	// code is part of the wire contract.
	proofMacRe = regexp.MustCompile(`\A[A-Za-z0-9_-]{43}\z`)
)

// ProofMode selects how strictly a worker treats the proof header.
type ProofMode string

const (
	// ProofModeOff installs no gate at all — zero per-request cost.
	ProofModeOff ProofMode = "off"
	// ProofModeAllow verifies and records but never denies. A rollout lever.
	ProofModeAllow ProofMode = "allow"
	// ProofModeRequire rejects a request whose proof does not verify.
	ProofModeRequire ProofMode = "require"
)

// ProofSecret is one accepted key and the proxy label it attributes to.
type ProofSecret struct {
	Secret []byte
	Label  string
}

// ProofConfig configures worker-side proof verification.
type ProofConfig struct {
	Mode ProofMode
	// OriginID is this worker's identifier. Folded into every MAC but never
	// transmitted, so a proof minted for another worker cannot verify here.
	OriginID string
	// Secrets maps a key id to its secret and proxy label.
	Secrets map[string]ProofSecret
	// SkewSeconds is the half-width of the timestamp acceptance window.
	SkewSeconds int
	// ReplayCapacity bounds the nonce cache. Zero uses the default.
	ReplayCapacity int
	// DisableReplayCache turns off nonce tracking, leaving only the
	// timestamp window to bound replay.
	DisableReplayCache bool
	// Now is injectable for tests; nil means time.Now.
	Now func() time.Time
}

const defaultReplayCapacity = 100_000

// ProofError is a proof rejection carrying its reason code.
type ProofError struct {
	Reason string
	Detail string
}

func (e *ProofError) Error() string { return e.Reason + ": " + e.Detail }

func proofB64(raw []byte) string {
	return base64.RawURLEncoding.EncodeToString(raw)
}

// DeriveProofSecret derives the secret shared between one proxy and one worker.
//
// A worker is configured with its derived secret only, never the base key —
// otherwise it could mint proofs its siblings would accept.
func DeriveProofSecret(baseKey []byte, proxyID, originID string) ([]byte, error) {
	if len(baseKey) != proofSecretLen {
		return nil, fmt.Errorf("base key must be exactly %d bytes, got %d", proofSecretLen, len(baseKey))
	}
	if !proofOriginRe.MatchString(proxyID) {
		return nil, fmt.Errorf("proxyID must match %s, got %q", proofOriginRe, proxyID)
	}
	if !proofOriginRe.MatchString(originID) {
		return nil, fmt.Errorf("originID must match %s, got %q", proofOriginRe, originID)
	}
	// NUL-separated, and neither identifier may contain NUL — so ("a", "b\x00c")
	// cannot collide with ("a\x00b", "c").
	msg := append([]byte("vgi.proxy.proof.v1/"), []byte(proxyID)...)
	msg = append(msg, 0)
	msg = append(msg, []byte(originID)...)
	mac := hmac.New(sha256.New, baseKey)
	mac.Write(msg)
	return mac.Sum(nil), nil
}

// proofCanonicalString builds the MAC input.
//
// originID is folded in but never transmitted: the worker supplies its own,
// which is what binds a proof to one audience.
func proofCanonicalString(kid, ts, nonce, originID string) []byte {
	parts := [][]byte{proofDomainPrefix, []byte(kid), []byte(ts), []byte(nonce), []byte(originID)}
	total := 0
	for _, p := range parts {
		total += len(p) + 1
	}
	out := make([]byte, 0, total)
	for i, p := range parts {
		if i > 0 {
			out = append(out, 0)
		}
		out = append(out, p...)
	}
	return out
}

// MintProof produces a proof token. Primarily for tests and for clients that
// front a worker; production workers only verify.
func MintProof(secret []byte, kid, originID string, now int64, nonce string) (string, error) {
	if !proofKidRe.MatchString(kid) {
		return "", fmt.Errorf("kid must match %s, got %q", proofKidRe, kid)
	}
	if !proofOriginRe.MatchString(originID) {
		return "", fmt.Errorf("originID must match %s, got %q", proofOriginRe, originID)
	}
	ts := strconv.FormatInt(now, 10)
	mac := hmac.New(sha256.New, secret)
	mac.Write(proofCanonicalString(kid, ts, nonce, originID))
	return strings.Join([]string{proofVersion, kid, ts, nonce, proofB64(mac.Sum(nil))}, "."), nil
}

// VerifyProof checks a proof token against the configured secrets.
//
// Cheap rejections run before any MAC is computed, so an unparseable header
// costs a few regex matches rather than a hash.
func VerifyProof(token string, cfg *ProofConfig, cache *nonceCache) (map[string]any, error) {
	if len(token) > proofMaxHeaderLen {
		return nil, &ProofError{"malformed", "proof header too long"}
	}
	parts := strings.Split(token, ".")
	if len(parts) != 5 {
		return nil, &ProofError{"malformed", fmt.Sprintf("expected 5 fields, got %d", len(parts))}
	}
	version, kid, tsRaw, nonce, macB64 := parts[0], parts[1], parts[2], parts[3], parts[4]
	if version != proofVersion {
		return nil, &ProofError{"malformed", "unsupported version " + version}
	}
	if !proofKidRe.MatchString(kid) {
		return nil, &ProofError{"malformed", "kid charset"}
	}
	if !proofTsRe.MatchString(tsRaw) {
		return nil, &ProofError{"malformed", "ts charset"}
	}
	if !proofNonceRe.MatchString(nonce) {
		return nil, &ProofError{"malformed", "nonce charset"}
	}
	if !proofMacRe.MatchString(macB64) {
		return nil, &ProofError{"malformed", "mac charset"}
	}

	entry, ok := cfg.Secrets[kid]
	if !ok {
		return nil, &ProofError{"unknown_kid", "no secret for kid"}
	}

	// Two-sided. Checking only the upper bound would let a far-future
	// timestamp pass forever.
	nowFn := cfg.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	ts, err := strconv.ParseInt(tsRaw, 10, 64)
	if err != nil {
		return nil, &ProofError{"malformed", "ts not an integer"}
	}
	age := nowFn().Unix() - ts
	skew := int64(cfg.SkewSeconds)
	if age > skew {
		return nil, &ProofError{"expired", fmt.Sprintf("age=%ds", age)}
	}
	if -age > skew {
		return nil, &ProofError{"not_yet_valid", fmt.Sprintf("age=%ds", age)}
	}

	mac := hmac.New(sha256.New, entry.Secret)
	mac.Write(proofCanonicalString(kid, tsRaw, nonce, cfg.OriginID))
	expected := mac.Sum(nil)
	received, err := base64.RawURLEncoding.DecodeString(macB64)
	if err != nil {
		return nil, &ProofError{"malformed", "mac is not base64url"}
	}
	// kid is public, so selecting one candidate secret is a safe branch; only
	// the resulting MAC needs the constant-time compare. subtle rather than
	// hmac.Equal to match the comparison used everywhere else in this package.
	if subtle.ConstantTimeCompare(received, expected) != 1 {
		return nil, &ProofError{"bad_mac", "signature mismatch"}
	}

	if cache != nil && !cache.checkAndAdd(nonce) {
		return nil, &ProofError{"replayed", "nonce already seen"}
	}

	return map[string]any{
		"verified":  "true",
		"proxy":     entry.Label,
		"kid":       kid,
		"origin_id": cfg.OriginID,
		"reason":    "ok",
	}, nil
}

// nonceCache is a bounded, TTL-expiring set of recently-seen nonces.
//
// The capacity cap is not optional: a TTL bounds how long an entry lives, never
// how many arrive inside the window, so a TTL-only cache is a remote
// memory-exhaustion vector.
type nonceCache struct {
	mu       sync.Mutex
	ttl      time.Duration
	capacity int
	order    *list.List
	entries  map[string]*list.Element
	now      func() time.Time
}

type nonceEntry struct {
	nonce     string
	expiresAt time.Time
}

func newNonceCache(ttl time.Duration, capacity int, now func() time.Time) *nonceCache {
	if now == nil {
		now = time.Now
	}
	return &nonceCache{
		ttl:      ttl,
		capacity: capacity,
		order:    list.New(),
		entries:  make(map[string]*list.Element),
		now:      now,
	}
}

// checkAndAdd atomically reports whether a nonce is fresh, remembering it if so.
//
// Test and insert are one locked operation: a separate contains-then-add would
// let two concurrent replays both observe "not seen" and both be accepted.
func (c *nonceCache) checkAndAdd(nonce string) bool {
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()

	// Uniform TTL means insertion order is expiry order, so expired entries
	// are always a prefix and this sweep is exact.
	for c.order.Len() > 0 {
		front := c.order.Front()
		if front.Value.(*nonceEntry).expiresAt.After(now) {
			break
		}
		delete(c.entries, front.Value.(*nonceEntry).nonce)
		c.order.Remove(front)
	}

	if _, seen := c.entries[nonce]; seen {
		return false
	}
	// Evict oldest rather than refuse: a burst past capacity is an
	// availability problem, not an authentication one, and the timestamp
	// window still bounds the evicted nonce's usefulness.
	for c.order.Len() >= c.capacity {
		front := c.order.Front()
		delete(c.entries, front.Value.(*nonceEntry).nonce)
		c.order.Remove(front)
	}
	c.entries[nonce] = c.order.PushBack(&nonceEntry{nonce: nonce, expiresAt: now.Add(c.ttl)})
	return true
}

// ProofAuthenticate wraps an authenticate callback with a proof precondition.
//
// The gate runs first; on failure inner is never invoked. This is an AND, not
// an alternative — do not pass a proof gate to ChainAuthenticate, whose
// first-success-wins semantics would let any later credential bypass it.
//
// inner may be nil: proof alone means "only my proxy may call this worker",
// with user identity handled upstream.
func ProofAuthenticate(cfg ProofConfig, inner AuthenticateFunc) (AuthenticateFunc, error) {
	if cfg.Mode == ProofModeOff {
		return nil, fmt.Errorf("ProofAuthenticate called with mode=off; install no gate instead")
	}
	if cfg.Mode != ProofModeAllow && cfg.Mode != ProofModeRequire {
		return nil, fmt.Errorf("unknown proof mode %q", cfg.Mode)
	}
	if !proofOriginRe.MatchString(cfg.OriginID) {
		return nil, fmt.Errorf("OriginID must match %s, got %q", proofOriginRe, cfg.OriginID)
	}
	if len(cfg.Secrets) == 0 {
		return nil, fmt.Errorf("at least one secret is required in %q mode", cfg.Mode)
	}
	for kid, entry := range cfg.Secrets {
		if !proofKidRe.MatchString(kid) {
			return nil, fmt.Errorf("kid must match %s, got %q", proofKidRe, kid)
		}
		if len(entry.Secret) != proofSecretLen {
			return nil, fmt.Errorf("secret for kid %q must be %d bytes, got %d", kid, proofSecretLen, len(entry.Secret))
		}
	}
	if cfg.SkewSeconds <= 0 {
		return nil, fmt.Errorf("SkewSeconds must be positive, got %d", cfg.SkewSeconds)
	}

	capacity := cfg.ReplayCapacity
	if capacity <= 0 {
		capacity = defaultReplayCapacity
	}
	var cache *nonceCache
	if !cfg.DisableReplayCache {
		cache = newNonceCache(time.Duration(cfg.SkewSeconds)*time.Second, capacity, cfg.Now)
	}
	required := cfg.Mode == ProofModeRequire
	local := cfg

	return func(r *http.Request) (*AuthContext, error) {
		claims, perr := verifyRequestProof(r, &local, cache)
		if perr != nil {
			if required {
				// Uniform message: the caller controls kid, so echoing any
				// detail would reflect attacker-supplied text.
				return nil, &RpcError{Type: "PermissionError", Message: "proxy proof required"}
			}
			claims = map[string]any{
				"verified":  "false",
				"proxy":     "",
				"kid":       "",
				"origin_id": local.OriginID,
				"reason":    perr.Reason,
			}
		}
		if inner == nil {
			return &AuthContext{
				Domain:        proofClaimsKey,
				Authenticated: true,
				Principal:     claims["proxy"].(string),
				Claims:        map[string]any{proofClaimsKey: claims},
			}, nil
		}
		ctx, err := inner(r)
		if err != nil {
			return nil, err
		}
		merged := make(map[string]any, len(ctx.Claims)+1)
		for k, v := range ctx.Claims {
			merged[k] = v
		}
		merged[proofClaimsKey] = claims
		return &AuthContext{
			Domain:        ctx.Domain,
			Authenticated: ctx.Authenticated,
			Principal:     ctx.Principal,
			Claims:        merged,
		}, nil
	}, nil
}

// SetProxyProofRequired advertises ProofRequiredHeader on every response, so
// a proxy can tell it is minting proofs for a worker that actually checks
// them — the misconfiguration that otherwise turns the feature into a no-op.
//
// Operator-declared rather than derived from the gate: ProofAuthenticate is
// installed through SetAuthenticate as an opaque AuthenticateFunc, which the
// server cannot introspect for its mode. Set it only for ProofModeRequire —
// allow mode never denies, so it must not claim to.
//
// Advertisement only: it enables and enforces nothing.
func (h *HttpServer) SetProxyProofRequired(required bool) {
	h.proxyProofRequired = required
}

func verifyRequestProof(r *http.Request, cfg *ProofConfig, cache *nonceCache) (map[string]any, *ProofError) {
	values := r.Header.Values(ProofHeader)
	if len(values) == 0 || values[0] == "" {
		return nil, &ProofError{"no_proof", "header absent"}
	}
	if len(values) > 1 || strings.Contains(values[0], ",") {
		return nil, &ProofError{"malformed", "multiple proof headers"}
	}
	claims, err := VerifyProof(values[0], cfg, cache)
	if err != nil {
		if pe, ok := err.(*ProofError); ok {
			return nil, pe
		}
		return nil, &ProofError{"malformed", err.Error()}
	}
	return claims, nil
}

// ParseProofSecrets parses a "kid:hex,kid:hex" specification.
//
// The kid doubles as the proxy's label, so attribution needs no extra config.
// Any malformed entry fails the whole parse rather than silently dropping one
// proxy's access.
func ParseProofSecrets(raw string) (map[string]ProofSecret, error) {
	out := make(map[string]ProofSecret)
	for _, chunk := range strings.Split(raw, ",") {
		item := strings.TrimSpace(chunk)
		if item == "" {
			continue
		}
		kid, hexSecret, found := strings.Cut(item, ":")
		if !found {
			return nil, fmt.Errorf("expected 'kid:hex', got %q", item)
		}
		if !proofKidRe.MatchString(kid) {
			return nil, fmt.Errorf("kid must match %s, got %q", proofKidRe, kid)
		}
		secret, err := hex.DecodeString(strings.ToLower(hexSecret))
		if err != nil || len(secret) != proofSecretLen {
			return nil, fmt.Errorf("secret for kid %q must be %d hex chars", kid, proofSecretLen*2)
		}
		out[kid] = ProofSecret{Secret: secret, Label: kid}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no secrets parsed")
	}
	return out, nil
}
