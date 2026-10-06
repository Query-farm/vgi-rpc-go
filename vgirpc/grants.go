// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/crypto/chacha20poly1305"
)

// Sealed grants: the framework's own issue_grant credential, and its verifier.
//
// vgi_rpc.Identity.v1's issue_grant mints a standing delegation that unattended
// automation later presents as an ordinary bearer. Until this file nothing
// accepted one: the loop was open in every port. A sealed grant closes it with
// no storage and no author code. When a deployment configures a grant key, the
// framework mints grants itself (unless the worker supplies its own MintGrant)
// and accepts them back as bearer credentials on HTTP. When it does not,
// nothing changes -- absent beats hosted-and-refusing.
//
// Normative: IDENTITY_V1_SPEC.md §9 and WIRE_PROTOCOL.md §16 in the reference.
// Every port mints and verifies byte-identically, pinned by
// vgi_rpc/conformance/grant_token_vectors.json:
//
//	token    = "vgig1." base64url_nopad( kid(8) || envelope )
//	kid      = SHA-256("vgi_rpc.grant.kid.v1" 0x00 || key)[0:8]
//	envelope = 0x01 || nonce(24) || XChaCha20-Poly1305(payload, aad)
//	aad      = "vgi_rpc.grant.v1" 0x00 || kid || UTF-8(audience)
//	payload  = issued_at i64 || expires_at i64 || grant_id || principal ||
//	           purpose || scope_count u16 || scopes     (little-endian;
//	           strings are u16 length || UTF-8)
//
// The envelope is the state token's (http_state.go): same cipher, same
// version || nonce || ciphertext+tag layout. The 32-byte key is used directly.
//
// Not individually revocable: a sealed grant is valid until it expires. The
// levers are a short maximum lifetime with re-issue, and removing a key.

// GrantTokenPrefix starts every sealed grant. The format version is in the
// prefix, so an incompatible format is routed elsewhere, never half-parsed.
const GrantTokenPrefix = "vgig1."

// Environment variables a deployment configures sealed grants with.
const (
	// GrantKeysEnv is a comma-separated list of standard-base64 32-byte keys,
	// minting key first.
	GrantKeysEnv = "VGI_RPC_GRANT_KEYS"
	// GrantAudienceEnv is the audience bound into every grant (default "").
	GrantAudienceEnv = "VGI_RPC_GRANT_AUDIENCE"
	// GrantMaxTTLEnv caps a grant's lifetime in seconds (default 7 days).
	GrantMaxTTLEnv = "VGI_RPC_GRANT_MAX_TTL_SECONDS"
)

// DefaultGrantMaxTTLSeconds is the longest lifetime a grant may have unless
// the deployment says otherwise. Short on purpose: expiry is the only
// revocation a sealed grant has.
const DefaultGrantMaxTTLSeconds = 7 * 24 * 3600

// DefaultGrantClockSkewSeconds allows for clocks disagreeing between the
// minting and the verifying worker.
const DefaultGrantClockSkewSeconds = 60

const (
	grantKeyLen          = 32
	grantKIDLen          = 8
	grantEnvelopeVersion = 1
	grantMaxField        = 0xFFFF
	grantKIDDomain       = "vgi_rpc.grant.kid.v1\x00"
	grantAADDomain       = "vgi_rpc.grant.v1\x00"
)

var grantB64URL = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// GrantInvalidError reports a vgig1. token that could not be accepted.
//
// One type for every cause -- malformed, wrong key, wrong audience, tampered,
// expired -- so a caller cannot tell a forged token from a stale one except by
// Expired, which is set only once the token was proven authentic and so tells
// a forger nothing.
type GrantInvalidError struct {
	Detail  string
	Expired bool
}

func (e *GrantInvalidError) Error() string { return e.Detail }

func grantInvalid(detail string) error { return &GrantInvalidError{Detail: detail} }

// GrantKeyID returns the 8-byte id a token names its sealing key with.
func GrantKeyID(key []byte) []byte {
	sum := sha256.Sum256(append([]byte(grantKIDDomain), key...))
	return sum[:grantKIDLen]
}

// GrantKeys is a deployment's sealed-grant configuration. The first key
// mints; every key verifies. Rotation: add the new key first, keep the old
// one after it until every grant it minted has expired, then drop it.
//
// Build one with [NewGrantKeys], [ParseGrantKeys] or [GrantKeysFromEnv]; they
// validate, and a deployment refuses to start on a key it misread.
type GrantKeys struct {
	keys             [][]byte
	audience         string
	maxTTLSeconds    int64
	clockSkewSeconds int64
}

// NewGrantKeys validates raw 32-byte keys (minting key first). maxTTLSeconds
// 0 means [DefaultGrantMaxTTLSeconds].
func NewGrantKeys(keys [][]byte, audience string, maxTTLSeconds int64) (*GrantKeys, error) {
	if maxTTLSeconds == 0 {
		maxTTLSeconds = DefaultGrantMaxTTLSeconds
	}
	if len(keys) == 0 {
		return nil, errors.New("vgirpc: grant configuration needs at least one key")
	}
	seen := make(map[string]bool, len(keys))
	out := make([][]byte, len(keys))
	for i, k := range keys {
		if len(k) != grantKeyLen {
			return nil, fmt.Errorf("vgirpc: grant key #%d is %d bytes; exactly %d are required", i+1, len(k), grantKeyLen)
		}
		id := string(GrantKeyID(k))
		if seen[id] {
			return nil, errors.New("vgirpc: grant keys must be distinct")
		}
		seen[id] = true
		out[i] = append([]byte(nil), k...)
	}
	if maxTTLSeconds < 0 {
		return nil, errors.New("vgirpc: grant max TTL must be positive")
	}
	if len(audience) > grantMaxField {
		return nil, errors.New("vgirpc: grant audience is too long")
	}
	return &GrantKeys{
		keys:             out,
		audience:         audience,
		maxTTLSeconds:    maxTTLSeconds,
		clockSkewSeconds: DefaultGrantClockSkewSeconds,
	}, nil
}

// ParseGrantKeys decodes standard-base64 keys (padding optional), minting key
// first. A key that is not base64 of exactly 32 bytes is an error.
func ParseGrantKeys(encoded []string, audience string, maxTTLSeconds int64) (*GrantKeys, error) {
	keys := make([][]byte, 0, len(encoded))
	for i, text := range encoded {
		s := strings.TrimSpace(text)
		s = strings.TrimRight(s, "=")
		key, err := base64.RawStdEncoding.Strict().DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("vgirpc: grant key #%d is not valid base64", i+1)
		}
		if len(key) != grantKeyLen {
			return nil, fmt.Errorf("vgirpc: grant key #%d decodes to %d bytes; exactly %d are required", i+1, len(key), grantKeyLen)
		}
		keys = append(keys, key)
	}
	return NewGrantKeys(keys, audience, maxTTLSeconds)
}

// GrantKeysFromEnv reads VGI_RPC_GRANT_KEYS, VGI_RPC_GRANT_AUDIENCE and
// VGI_RPC_GRANT_MAX_TTL_SECONDS. It returns (nil, nil) when no key is set --
// grants off -- and an error for a malformed key or lifetime, which must stop
// the worker at startup.
func GrantKeysFromEnv() (*GrantKeys, error) {
	return GrantKeysFromValues(nil, os.Getenv(GrantKeysEnv), os.Getenv(GrantAudienceEnv), os.Getenv(GrantMaxTTLEnv))
}

// GrantKeysFromValues merges CLI keys (cliKeys, e.g. repeated --grant-key,
// first mints) with the environment's comma-separated keys and settings. CLI
// keys, when given, replace the environment's. Returns (nil, nil) when no key
// is configured.
func GrantKeysFromValues(cliKeys []string, envKeys, audience, maxTTL string) (*GrantKeys, error) {
	keys := cliKeys
	if len(keys) == 0 {
		for _, part := range strings.Split(envKeys, ",") {
			if strings.TrimSpace(part) != "" {
				keys = append(keys, part)
			}
		}
	}
	if len(keys) == 0 {
		return nil, nil
	}
	ttl := int64(DefaultGrantMaxTTLSeconds)
	if s := strings.TrimSpace(maxTTL); s != "" {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("vgirpc: %s=%q is not an integer", GrantMaxTTLEnv, s)
		}
		if v <= 0 {
			return nil, fmt.Errorf("vgirpc: %s must be positive", GrantMaxTTLEnv)
		}
		ttl = v
	}
	return ParseGrantKeys(keys, audience, ttl)
}

// Audience returns the audience bound into every grant.
func (k *GrantKeys) Audience() string { return k.audience }

// MaxTTLSeconds returns the ceiling on a grant's lifetime.
func (k *GrantKeys) MaxTTLSeconds() int64 { return k.maxTTLSeconds }

func (k *GrantKeys) aad(kid []byte) []byte {
	out := make([]byte, 0, len(grantAADDomain)+len(kid)+len(k.audience))
	out = append(out, grantAADDomain...)
	out = append(out, kid...)
	return append(out, k.audience...)
}

// GrantClaims is what a verified grant says.
type GrantClaims struct {
	Principal string
	Scopes    []string
	Purpose   string
	GrantID   string
	IssuedAt  int64
	ExpiresAt int64
}

func appendGrantText(buf []byte, s string) ([]byte, error) {
	if len(s) > grantMaxField {
		return nil, errors.New("vgirpc: grant field longer than 65535 bytes")
	}
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(s)))
	return append(buf, s...), nil
}

func encodeGrantPayload(c GrantClaims) ([]byte, error) {
	if len(c.Scopes) > grantMaxField {
		return nil, errors.New("vgirpc: too many scopes")
	}
	buf := binary.LittleEndian.AppendUint64(nil, uint64(c.IssuedAt))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(c.ExpiresAt))
	var err error
	for _, s := range []string{c.GrantID, c.Principal, c.Purpose} {
		if buf, err = appendGrantText(buf, s); err != nil {
			return nil, err
		}
	}
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(c.Scopes)))
	for _, s := range c.Scopes {
		if buf, err = appendGrantText(buf, s); err != nil {
			return nil, err
		}
	}
	return buf, nil
}

// decodeGrantPayload parses strictly: exact lengths, valid UTF-8, no trailing
// bytes.
func decodeGrantPayload(p []byte) (GrantClaims, error) {
	pos := 0
	take := func(n int) ([]byte, error) {
		if pos+n > len(p) {
			return nil, grantInvalid("grant payload is truncated")
		}
		chunk := p[pos : pos+n]
		pos += n
		return chunk, nil
	}
	text := func() (string, error) {
		lenBytes, err := take(2)
		if err != nil {
			return "", err
		}
		raw, err := take(int(binary.LittleEndian.Uint16(lenBytes)))
		if err != nil {
			return "", err
		}
		if !utf8.Valid(raw) {
			return "", grantInvalid("grant payload is not UTF-8")
		}
		return string(raw), nil
	}
	var c GrantClaims
	times, err := take(16)
	if err != nil {
		return c, err
	}
	c.IssuedAt = int64(binary.LittleEndian.Uint64(times[:8]))
	c.ExpiresAt = int64(binary.LittleEndian.Uint64(times[8:]))
	if c.GrantID, err = text(); err != nil {
		return c, err
	}
	if c.Principal, err = text(); err != nil {
		return c, err
	}
	if c.Purpose, err = text(); err != nil {
		return c, err
	}
	countBytes, err := take(2)
	if err != nil {
		return c, err
	}
	count := int(binary.LittleEndian.Uint16(countBytes))
	c.Scopes = make([]string, 0, count)
	for range count {
		s, err := text()
		if err != nil {
			return c, err
		}
		c.Scopes = append(c.Scopes, s)
	}
	if pos != len(p) {
		return c, grantInvalid("grant payload has trailing bytes")
	}
	return c, nil
}

// grantB64URLStrict decodes unpadded base64url, refusing any non-canonical
// spelling: re-encoding must give the same text, so one token has one
// spelling.
func grantB64URLStrict(text string) ([]byte, error) {
	if !grantB64URL.MatchString(text) || len(text)%4 == 1 {
		return nil, grantInvalid("grant token is not unpadded base64url")
	}
	raw, err := base64.RawURLEncoding.DecodeString(text)
	if err != nil || base64.RawURLEncoding.EncodeToString(raw) != text {
		return nil, grantInvalid("grant token is not canonical base64url")
	}
	return raw, nil
}

// MintGrantOptions describes one grant to mint. Now, GrantID and Nonce exist
// for test vectors; leave them zero in production.
type MintGrantOptions struct {
	Principal  string
	Scopes     []string
	Purpose    string
	TTLSeconds int64
	// Now overrides the clock (Unix seconds) when non-nil.
	Now *int64
	// GrantID overrides the random 32-hex grant id when non-empty.
	GrantID string
	// Nonce fixes the 24-byte nonce. Test vectors only.
	Nonce []byte
}

// MintGrantToken mints a sealed grant with the first configured key. The
// lifetime is min(TTLSeconds, max TTL); a non-positive TTLSeconds is an error.
func MintGrantToken(keys *GrantKeys, opts MintGrantOptions) (string, GrantClaims, error) {
	if opts.TTLSeconds <= 0 {
		return "", GrantClaims{}, errors.New("vgirpc: ttl_seconds must be positive")
	}
	issued := time.Now().Unix()
	if opts.Now != nil {
		issued = *opts.Now
	}
	grantID := opts.GrantID
	if grantID == "" {
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			return "", GrantClaims{}, err
		}
		grantID = hex.EncodeToString(id[:])
	}
	claims := GrantClaims{
		Principal: opts.Principal,
		Scopes:    append([]string{}, opts.Scopes...),
		Purpose:   opts.Purpose,
		GrantID:   grantID,
		IssuedAt:  issued,
		ExpiresAt: issued + min(opts.TTLSeconds, keys.maxTTLSeconds),
	}
	payload, err := encodeGrantPayload(claims)
	if err != nil {
		return "", GrantClaims{}, err
	}
	key := keys.keys[0]
	kid := GrantKeyID(key)
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return "", GrantClaims{}, err
	}
	nonce := opts.Nonce
	if nonce == nil {
		nonce = make([]byte, chacha20poly1305.NonceSizeX)
		if _, err := rand.Read(nonce); err != nil {
			return "", GrantClaims{}, err
		}
	} else if len(nonce) != chacha20poly1305.NonceSizeX {
		return "", GrantClaims{}, errors.New("vgirpc: grant nonce must be 24 bytes")
	}
	raw := make([]byte, 0, grantKIDLen+1+len(nonce)+len(payload)+chacha20poly1305.Overhead)
	raw = append(raw, kid...)
	raw = append(raw, grantEnvelopeVersion)
	raw = append(raw, nonce...)
	raw = aead.Seal(raw, nonce, payload, keys.aad(kid))
	return GrantTokenPrefix + base64.RawURLEncoding.EncodeToString(raw), claims, nil
}

// VerifyGrantToken verifies a sealed grant and returns its claims.
//
// Order is normative: prefix, length, canonical base64url, key id, AEAD open,
// strict payload, then lifetime -- the lifetime is inside the ciphertext, so it
// is trusted only after the tag verified. Every failure is a
// [*GrantInvalidError]; Expired is set only for an authentic grant outside its
// lifetime.
func VerifyGrantToken(keys *GrantKeys, token string, now time.Time) (GrantClaims, error) {
	if !strings.HasPrefix(token, GrantTokenPrefix) {
		return GrantClaims{}, grantInvalid("not a sealed grant")
	}
	if len(token) > MaxTokenBytes {
		return GrantClaims{}, grantInvalid("grant token is too long")
	}
	raw, err := grantB64URLStrict(token[len(GrantTokenPrefix):])
	if err != nil {
		return GrantClaims{}, err
	}
	if len(raw) < grantKIDLen+1+chacha20poly1305.NonceSizeX+chacha20poly1305.Overhead {
		return GrantClaims{}, grantInvalid("grant failed verification")
	}
	kid, envelope := raw[:grantKIDLen], raw[grantKIDLen:]
	var key []byte
	for _, k := range keys.keys {
		if string(GrantKeyID(k)) == string(kid) {
			key = k
			break
		}
	}
	if key == nil {
		return GrantClaims{}, grantInvalid("grant was sealed with a key this deployment does not hold")
	}
	if envelope[0] != grantEnvelopeVersion {
		return GrantClaims{}, grantInvalid("grant failed verification")
	}
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return GrantClaims{}, err
	}
	nonce := envelope[1 : 1+chacha20poly1305.NonceSizeX]
	payload, err := aead.Open(nil, nonce, envelope[1+chacha20poly1305.NonceSizeX:], keys.aad(kid))
	if err != nil {
		return GrantClaims{}, grantInvalid("grant failed verification")
	}
	claims, err := decodeGrantPayload(payload)
	if err != nil {
		return GrantClaims{}, err
	}
	if claims.Principal == "" {
		return GrantClaims{}, grantInvalid("grant names no principal")
	}
	if claims.ExpiresAt <= claims.IssuedAt || claims.ExpiresAt-claims.IssuedAt > keys.maxTTLSeconds {
		return GrantClaims{}, grantInvalid("grant lifetime exceeds this deployment's maximum")
	}
	current := float64(now.UnixNano()) / 1e9
	skew := float64(keys.clockSkewSeconds)
	if float64(claims.IssuedAt) > current+skew {
		return GrantClaims{}, &GrantInvalidError{Detail: "grant is not yet valid", Expired: true}
	}
	if current >= float64(claims.ExpiresAt)+skew {
		return GrantClaims{}, &GrantInvalidError{Detail: "grant has expired", Expired: true}
	}
	return claims, nil
}

// SealedMintGrant returns a [GrantMinter] issuing sealed grants -- what
// [NewIdentity] installs when GrantKeys is configured and MintGrant is nil.
func SealedMintGrant(keys *GrantKeys) GrantMinter {
	return func(principal, purpose string, scopes []string, ttlSeconds int64) (IssuedGrant, error) {
		if ttlSeconds <= 0 {
			return IssuedGrant{}, &GrantRefusedError{Detail: "ttl_seconds must be positive"}
		}
		token, claims, err := MintGrantToken(keys, MintGrantOptions{
			Principal: principal, Scopes: scopes, Purpose: purpose, TTLSeconds: ttlSeconds,
		})
		if err != nil {
			return IssuedGrant{}, &GrantRefusedError{Detail: err.Error()}
		}
		return IssuedGrant{Token: token, ExpiresAt: float64(claims.ExpiresAt), GrantID: claims.GrantID}, nil
	}
}
