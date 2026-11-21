// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package jwtauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MicahParks/jwkset"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/golang-jwt/jwt/v5"
)

// defaultAlgorithms is the allow-list when JWTAuthConfig.Algorithms is nil.
var defaultAlgorithms = []string{"RS256", "RS384", "RS512", "ES256", "ES384", "ES512"}

// maxJWKSBytes bounds a JWKS response body.
const maxJWKSBytes = 1 << 20

// timeNow is the clock; tests replace it.
var timeNow = time.Now

var maxAgeRE = regexp.MustCompile(`(?i)(?:^|,)\s*max-age\s*=\s*"?(\d+)"?\s*(?:,|$)`)

// errUnknownKID marks a token whose kid is not in the current key set.
var errUnknownKID = errors.New("unknown key id")

// hardened reports whether cfg opts into the hardened verifier.
func (cfg *JWTAuthConfig) hardened() bool {
	return cfg.Algorithms != nil || cfg.RequiredTyp != "" || cfg.RequireExp || cfg.SingleAudience ||
		cfg.Leeway != 0 || cfg.JWKSMaxAge != 0 || cfg.JWKSRefreshMinInterval != 0 || cfg.JWKSFetchTimeout != 0 ||
		cfg.HTTPClient != nil
}

// validateAlgorithms refuses an empty list, "none" and HMAC algorithms: an
// HMAC algorithm checked against a JWKS lets anyone holding the public key
// mint tokens (key confusion).
func validateAlgorithms(algorithms []string) ([]string, error) {
	if len(algorithms) == 0 {
		return nil, fmt.Errorf("jwtauth: algorithms must not be empty")
	}
	out := make([]string, 0, len(algorithms))
	for _, alg := range algorithms {
		alg = strings.TrimSpace(alg)
		lowered := strings.ToLower(alg)
		if alg == "" {
			return nil, fmt.Errorf("jwtauth: empty algorithm name")
		}
		if strings.HasPrefix(lowered, "none") || strings.HasPrefix(lowered, "hs") {
			return nil, fmt.Errorf("jwtauth: algorithm %q is not allowed for JWKS-verified tokens", alg)
		}
		out = append(out, alg)
	}
	return out, nil
}

// normalizeTyp compares media types case-insensitively with the
// "application/" prefix optional (RFC 7515 §4.1.9, RFC 9068 §2.1).
func normalizeTyp(v string) string {
	return strings.TrimPrefix(strings.ToLower(strings.TrimSpace(v)), "application/")
}

// cacheControlMaxAge returns the max-age directive of a Cache-Control value.
func cacheControlMaxAge(v string) (time.Duration, bool) {
	m := maxAgeRE.FindStringSubmatch(v)
	if m == nil {
		return 0, false
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil || n < 0 {
		return 0, false
	}
	const maxSeconds = 365 * 24 * 60 * 60 // cap absurd values; the max age bounds it anyway
	if n > maxSeconds {
		n = maxSeconds
	}
	return time.Duration(n) * time.Second, true
}

// keySet is one fetched JWKS.
type keySet struct {
	byKID     map[string]jwkset.JWK
	all       []jwkset.JWK
	expiresAt time.Time // zero: never
}

func (k *keySet) usable(now time.Time) bool {
	return k != nil && (k.expiresAt.IsZero() || now.Before(k.expiresAt))
}

// jwksCache holds the keys with a hard maximum age and never serves stale
// keys: once they expire they are discarded, and a failed re-fetch fails
// verification (503) rather than falling back.
type jwksCache struct {
	uri            string
	client         *http.Client
	maxAge         time.Duration // 0: no bound
	refreshMin     time.Duration // 0: no rate limit
	fetchTimeout   time.Duration
	current        atomic.Pointer[keySet]
	mu             sync.Mutex
	lastFetch      time.Time
	lastFetchValid bool
}

// fetchLocked fetches the JWKS and replaces the cache. Caller holds mu.
func (c *jwksCache) fetchLocked(ctx context.Context) (*keySet, error) {
	started := timeNow()
	c.lastFetch = started
	c.lastFetchValid = true
	keys, cacheControl, err := c.fetch(ctx)
	if err != nil {
		// Never fall back to stale keys: drop an expired entry so nothing
		// can read it, and make the caller fail closed.
		if !c.current.Load().usable(timeNow()) {
			c.current.Store(nil)
		}
		slog.Warn("jwtauth: JWKS fetch failed", "err", err)
		return nil, &vgirpc.AuthUnavailableError{Detail: "JWKS endpoint unavailable"}
	}
	if c.maxAge > 0 {
		ttl := c.maxAge
		if advertised, ok := cacheControlMaxAge(cacheControl); ok && advertised < ttl {
			ttl = advertised
		}
		// Measured from when the fetch started, so a slow response cannot
		// stretch the bound.
		keys.expiresAt = started.Add(ttl)
	}
	c.current.Store(keys)
	return keys, nil
}

func (c *jwksCache) fetch(ctx context.Context) (*keySet, string, error) {
	if c.fetchTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.fetchTimeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.uri, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, "", fmt.Errorf("JWKS endpoint answered HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxJWKSBytes+1))
	if err != nil {
		return nil, "", err
	}
	if len(body) > maxJWKSBytes {
		return nil, "", fmt.Errorf("JWKS response exceeds %d bytes", maxJWKSBytes)
	}
	var marshal jwkset.JWKSMarshal
	if err := json.Unmarshal(body, &marshal); err != nil {
		return nil, "", fmt.Errorf("JWKS response is not a JWK set: %w", err)
	}
	ks := &keySet{byKID: make(map[string]jwkset.JWK, len(marshal.Keys))}
	for _, m := range marshal.Keys {
		// Public keys only: symmetric and private material is not unmarshaled.
		key, err := jwkset.NewJWKFromMarshal(m, jwkset.JWKMarshalOptions{}, jwkset.JWKValidateOptions{})
		if err != nil || m.USE == jwkset.UseEnc {
			continue // an unsupported, malformed or encryption key is skipped, not fatal
		}
		ks.all = append(ks.all, key)
		if m.KID != "" {
			ks.byKID[m.KID] = key
		}
	}
	return ks, resp.Header.Get("Cache-Control"), nil
}

// keys returns keys within their maximum age, fetching if needed.
func (c *jwksCache) keys(ctx context.Context) (*keySet, error) {
	if ks := c.current.Load(); ks.usable(timeNow()) {
		return ks, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if ks := c.current.Load(); ks.usable(timeNow()) {
		return ks, nil
	}
	return c.fetchLocked(ctx)
}

// refreshForUnknownKID re-fetches after an unknown kid. It returns nil keys
// (and no error) when rate limited. seen is the set the caller decoded
// against: if another goroutine refreshed meanwhile, its result is reused
// rather than stampeding the endpoint during a key rotation.
func (c *jwksCache) refreshForUnknownKID(ctx context.Context, seen *keySet) (*keySet, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := timeNow()
	if ks := c.current.Load(); ks != nil && ks != seen && ks.usable(now) {
		return ks, nil
	}
	if c.refreshMin > 0 && c.lastFetchValid && now.Sub(c.lastFetch) < c.refreshMin {
		return nil, nil
	}
	return c.fetchLocked(ctx)
}

// newHardenedAuthenticateFunc builds the verifier used when any hardening
// option is set. See [JWTAuthConfig].
func newHardenedAuthenticateFunc(cfg JWTAuthConfig) (vgirpc.AuthenticateFunc, func(), error) {
	algorithms := defaultAlgorithms
	if cfg.Algorithms != nil {
		var err error
		if algorithms, err = validateAlgorithms(cfg.Algorithms); err != nil {
			return nil, nil, err
		}
	}
	expectedTyp := ""
	if cfg.RequiredTyp != "" {
		if expectedTyp = normalizeTyp(cfg.RequiredTyp); expectedTyp == "" {
			return nil, nil, fmt.Errorf("jwtauth: required typ must not be blank")
		}
	}
	for name, d := range map[string]time.Duration{
		"leeway": cfg.Leeway, "jwks max age": cfg.JWKSMaxAge,
		"jwks refresh min interval": cfg.JWKSRefreshMinInterval, "jwks fetch timeout": cfg.JWKSFetchTimeout,
	} {
		if d < 0 {
			return nil, nil, fmt.Errorf("jwtauth: %s must not be negative", name)
		}
	}
	client := cfg.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	cache := &jwksCache{
		uri:          cfg.JWKSURI,
		client:       client,
		maxAge:       cfg.JWKSMaxAge,
		refreshMin:   cfg.JWKSRefreshMinInterval,
		fetchTimeout: cfg.JWKSFetchTimeout,
	}
	accepted := append([]string(nil), cfg.Audience...)

	parserOpts := []jwt.ParserOption{
		jwt.WithValidMethods(algorithms),
		jwt.WithIssuer(cfg.Issuer),
		jwt.WithIssuedAt(),
		jwt.WithJSONNumber(),
		jwt.WithLeeway(cfg.Leeway),
	}
	if cfg.RequireExp {
		parserOpts = append(parserOpts, jwt.WithExpirationRequired())
	}
	parser := jwt.NewParser(parserOpts...)

	verify := func(tokenStr string, ks *keySet) (jwt.MapClaims, error) {
		token, err := parser.Parse(tokenStr, func(t *jwt.Token) (any, error) {
			alg := t.Method.Alg()
			kid, _ := t.Header["kid"].(string)
			if kid == "" {
				// No kid: try every key whose declared algorithm allows it.
				// Verification is cryptographic, so trying each is safe.
				set := jwt.VerificationKeySet{}
				for _, k := range ks.all {
					if a := string(k.Marshal().ALG); a == "" || a == alg {
						set.Keys = append(set.Keys, k.Key())
					}
				}
				if len(set.Keys) == 0 {
					return nil, errUnknownKID
				}
				return set, nil
			}
			k, ok := ks.byKID[kid]
			if !ok {
				return nil, errUnknownKID
			}
			if a := string(k.Marshal().ALG); a != "" && a != alg {
				return nil, fmt.Errorf("key %q is for %s, token is %s", kid, a, alg)
			}
			return k.Key(), nil
		})
		if err != nil {
			return nil, err
		}
		if expectedTyp != "" {
			typ, _ := token.Header["typ"].(string)
			if typ == "" || normalizeTyp(typ) != expectedTyp {
				return nil, fmt.Errorf("JOSE header typ must be %q", cfg.RequiredTyp)
			}
		}
		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			return nil, fmt.Errorf("unexpected claims type")
		}
		aud, err := claims.GetAudience()
		if err != nil || len(aud) == 0 || !audienceIntersects(aud, accepted) {
			return nil, fmt.Errorf("token audience does not match any accepted audience")
		}
		if cfg.SingleAudience && len(aud) != 1 {
			return nil, fmt.Errorf("token names %d audiences; exactly one is accepted", len(aud))
		}
		return claims, nil
	}

	reject := func(err error) error {
		// Expired is separated from invalid because the two call for
		// different actions: refresh and retry, versus re-authenticate.
		if errors.Is(err, jwt.ErrTokenExpired) {
			return vgirpc.NewAuthFailure(vgirpc.AuthReasonExpiredCredential, "token expired")
		}
		slog.Debug("jwtauth: token rejected", "err", err)
		return vgirpc.NewAuthFailure(vgirpc.AuthReasonInvalidCredential, "invalid token")
	}

	authFunc := func(r *http.Request) (*vgirpc.AuthContext, error) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			return nil, vgirpc.NewAuthFailure(vgirpc.AuthReasonMissingCredential, "missing Authorization header")
		}
		if !strings.HasPrefix(authHeader, "Bearer ") {
			return nil, vgirpc.NewAuthFailure(vgirpc.AuthReasonInvalidCredential, "Authorization header is not a Bearer credential")
		}
		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")

		// A JWKS fetch outlives the request that triggered it: one caller
		// hanging up must not fail (and rate-limit) the fetch for the rest.
		fetchCtx := context.WithoutCancel(r.Context())
		ks, err := cache.keys(fetchCtx)
		if err != nil {
			return nil, err
		}
		claims, err := verify(tokenStr, ks)
		if err != nil && errors.Is(err, errUnknownKID) {
			refreshed, ferr := cache.refreshForUnknownKID(fetchCtx, ks)
			if ferr != nil {
				return nil, ferr
			}
			if refreshed == nil {
				return nil, reject(err)
			}
			claims, err = verify(tokenStr, refreshed)
		}
		if err != nil {
			return nil, reject(err)
		}

		out := make(map[string]any, len(claims))
		for k, v := range claims {
			out[k] = jsonTyped(v)
		}
		principal := ""
		switch v := out[cfg.PrincipalClaim].(type) {
		case nil:
		case string:
			principal = v
		default:
			principal = fmt.Sprintf("%v", v)
		}
		return &vgirpc.AuthContext{
			Domain:        cfg.Domain,
			Authenticated: true,
			Principal:     principal,
			Claims:        out,
		}, nil
	}
	return authFunc, func() {}, nil
}

// jsonTyped converts json.Number values (recursively) to int64 when the
// number is integral and fits, float64 otherwise, so claims keep their JSON
// types: exp is an integer, email_verified a bool.
func jsonTyped(v any) any {
	switch x := v.(type) {
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i
		}
		if f, err := x.Float64(); err == nil {
			return f
		}
		return x.String()
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = jsonTyped(e)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = jsonTyped(e)
		}
		return out
	default:
		return v
	}
}
