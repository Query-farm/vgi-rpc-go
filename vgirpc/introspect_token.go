// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"time"
)

// Token introspection: resolving an opaque bearer credential to a principal.
//
// A reverse proxy that terminates the only public listener has to know *which
// principal a credential authenticates as* before it can authorize anything —
// that principal becomes the policy principal, the row-rule literal and the
// bind parameter of every entitlement query. When the credential is opaque the
// proxy holds no local copy of it, so it has to ask the worker.
//
// The answer is an identity assertion made by the thing being protected, and
// the asker acts on it with credentials the worker does not hold: storage
// credentials on a data-plane host, service-credential attachments in an
// entitlement resolver, policy-tier selection. "Trust it as much as you trust
// the worker" is therefore the wrong frame — it must be trusted *more*,
// because it steers privileges the worker never has. Every guard here follows
// from that: the route resolves nothing unless explicitly enabled, the
// allowlist has no permissive default, JWS-shaped subjects never reach the
// resolver, rejections are byte-identical, and the credential is digested
// rather than logged.
//
// What comes back is deliberately tiny: a principal, a display name for the
// credential, and how long the answer may be cached. It never returns claims —
// a pass-through claims field would let a worker choose its caller's tenant
// routing, its row scope and its policy branch.
//
// It is also not "replay the credential through the worker's own
// [AuthenticateFunc]", which is the attractive design and breaks four ways: a
// precondition gate such as [ProofAuthenticate] makes the replay
// unimplementable; it would run the worker's independently-configured
// audience/issuer set, so a credential the *asker* rejected could be accepted
// here; cookie- and mTLS/IP-derived identity cannot be replayed at all, and a
// synthesized request carries the proxy's own address, silently elevating any
// address-allowlist member; and it invents a fake-request contract every
// future authenticator would have to honour with no type to enforce it. The
// resolver is a narrow callable instead.
//
// The normative contract is vgi-rpc/docs/WIRE_PROTOCOL.md §16.

// IntrospectEndpoint is the introspection path, appended to the server's
// prefix. Fixed rather than configurable: it is the de-facto contract existing
// proxy clients already speak.
const IntrospectEndpoint = "/__introspect_token__"

// IntrospectEnabledHeader advertises that this worker can introspect, so a
// proxy preflights at boot rather than discovering at first login that the
// worker it depends on cannot answer. Emitted only when enabled — absent, never
// "false".
const IntrospectEnabledHeader = "VGI-Token-Introspection"

const (
	// Hard cap on the request body. The generic request-size limit would
	// otherwise admit megabytes into a JSON parse for a body whose only
	// legitimate content is one credential.
	introspectMaxBodyBytes = 8192
	// Cap on a credential we will even attempt to resolve. Anything longer is
	// not a bearer token; refusing early keeps a resolver from being handed
	// megabytes.
	introspectMaxTokenChars = 4096

	introspectDefaultTTLSeconds = 300
	introspectDefaultRateLimit  = 20
)

// introspectJWSShaped matches three dot-separated base64url segments — a JWS.
// Such a credential is validated locally against a key set and MUST NOT be
// routed here: doing so hands a third party a bearer token the asker may itself
// have rejected, and an expired access token is still live at its issuer for
// other resources.
var introspectJWSShaped = regexp.MustCompile(`\A[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*\z`)

// TokenDigest returns the SHA-256 hex digest of a credential, for diagnostics.
//
// The credential itself must never reach a log, a span or an error message. A
// digest is stable enough to correlate one credential's failures across records
// without being the credential. Exported so a [TokenResolver] can log the same
// identifier the server does.
func TokenDigest(credential string) string {
	sum := sha256.Sum256([]byte(credential))
	return hex.EncodeToString(sum[:])
}

// TokenIdentity is the identity an opaque credential authenticates as.
type TokenIdentity struct {
	// Principal is the canonical principal. Return it in the exact form this
	// worker would itself derive, so an asker that normalises differently does
	// not authorize as one identity while the worker serves another.
	Principal string
	// TokenName is a human-readable name for the credential, for audit trails.
	// Never the credential.
	TokenName string
	// TTLSeconds is how long the answer may be cached. The caller does the
	// caching; this endpoint holds none of its own. Treat it as an
	// authorization window, because for any path the asker serves without
	// re-presenting the credential it is exactly that. Zero or negative falls
	// back to the configured default.
	TTLSeconds int
}

// TokenResolver resolves an opaque credential to the identity it authenticates
// as. Report an unresolvable credential with ok=false — unknown, expired and
// malformed are one answer on the wire, because reporting which would confirm
// that a guessed credential exists.
//
// Return a non-nil error only when the answer is not *knowable*: a backing
// store that is down is not the same as a credential that is unknown, and a
// caller that negative-caches the second must not cache the first. Such a
// failure surfaces as 503 with Retry-After. Prefer [NewAuthUnavailable] so the
// Retry-After hint is yours to choose.
//
// A resolver must never return claims, and must never log the credential; use
// [TokenDigest].
type TokenResolver func(credential string) (identity TokenIdentity, ok bool, err error)

// TokenIntrospectionConfig configures [HttpServer.EnableTokenIntrospection].
type TokenIntrospectionConfig struct {
	// Resolver resolves the posted credential. Required.
	Resolver TokenResolver
	// Principals may introspect. Required and non-empty: there is **no
	// permissive default**. Authentication and introspection are different
	// capabilities, and a deployment where any valid credential may introspect
	// lets any user test guesses of any other user's credential at unlimited
	// rate, and resolve a stolen one to its owner.
	Principals []string
	// DefaultTTLSeconds is reported when a resolved identity names no TTL of
	// its own. Zero means 300.
	DefaultTTLSeconds int
	// RateLimitPerSecond bounds introspections per caller. Zero means 20. It
	// does not close the oracle an allowlisted-but-compromised caller still
	// has, it lowers the ceiling on how fast that caller converts guesses to
	// answers.
	RateLimitPerSecond int
}

// tokenIntrospection is the validated, request-time form of
// [TokenIntrospectionConfig]. Its presence on an [HttpServer] is what "enabled"
// means — nothing else in the server consults the config.
type tokenIntrospection struct {
	resolver   TokenResolver
	principals map[string]bool
	defaultTTL int
	limiter    *introspectRateLimiter
}

// EnableTokenIntrospection turns on POST {prefix}/__introspect_token__.
//
// Off unless called, so no worker grows a credential-to-identity oracle by
// upgrading a dependency. Validation happens here rather than on the first
// request, so a misconfiguration fails at boot rather than at a proxy's first
// preflight.
func (h *HttpServer) EnableTokenIntrospection(cfg TokenIntrospectionConfig) error {
	if cfg.Resolver == nil {
		return fmt.Errorf("vgirpc: EnableTokenIntrospection requires a Resolver")
	}
	principals := make(map[string]bool, len(cfg.Principals))
	for _, p := range cfg.Principals {
		if p != "" {
			principals[p] = true
		}
	}
	if len(principals) == 0 {
		return fmt.Errorf("vgirpc: EnableTokenIntrospection requires at least one principal in Principals. " +
			"Introspection is a distinct capability from authentication: allowing any authenticated " +
			"caller lets any user resolve any other user's credential to its owner")
	}
	ttl := cfg.DefaultTTLSeconds
	if ttl <= 0 {
		ttl = introspectDefaultTTLSeconds
	}
	rate := cfg.RateLimitPerSecond
	if rate <= 0 {
		rate = introspectDefaultRateLimit
	}
	h.introspect = &tokenIntrospection{
		resolver:   cfg.Resolver,
		principals: principals,
		defaultTTL: ttl,
		limiter:    newIntrospectRateLimiter(rate, time.Second),
	}
	return nil
}

// introspectRateLimiter is a fixed-window request limiter keyed by caller.
//
// Fixed-window rather than a token bucket: a window admits at most twice the
// rate across a boundary, which is a rounding error at this scale, and the
// state is one integer per caller rather than a float that has to be aged.
type introspectRateLimiter struct {
	mu          sync.Mutex
	perWindow   int
	window      time.Duration
	windowStart time.Time
	counts      map[string]int
}

func newIntrospectRateLimiter(perWindow int, window time.Duration) *introspectRateLimiter {
	return &introspectRateLimiter{
		perWindow: perWindow,
		window:    window,
		counts:    make(map[string]int),
	}
}

// allow reports whether key may make a request in the current window.
func (l *introspectRateLimiter) allow(key string) bool {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.windowStart) >= l.window {
		// Whole-map reset rather than per-key ageing: a caller cycling keys
		// cannot grow the map beyond one window's worth.
		clear(l.counts)
		l.windowStart = now
	}
	if l.counts[key] >= l.perWindow {
		return false
	}
	l.counts[key]++
	return true
}

// introspectResponse is the closed response set. Three keys, and a claims field
// is not merely omitted but unrepresentable — adding one has to be a deliberate
// change to this type, which is the point.
type introspectResponse struct {
	Principal  string `json:"principal"`
	TokenName  string `json:"token_name"`
	TTLSeconds int    `json:"ttl_seconds"`
}

// handleIntrospectToken serves POST {prefix}/__introspect_token__.
//
// Two rejection axes, deliberately distinguishable from each other and
// deliberately uniform within themselves:
//
//   - 403 — the caller may not introspect.
//   - 404 — the *subject* credential did not resolve. Unknown, expired and
//     malformed are one answer.
//
// Both are definitive: a caller may cache them. Anything transient reaches the
// caller as 503 so it is retried rather than cached.
func (h *HttpServer) handleIntrospectToken(w http.ResponseWriter, r *http.Request) {
	cfg := h.introspect
	if cfg == nil {
		// The oracle is absent in every sense that matters: no resolver is
		// held and nothing is looked up. What this answer adds is *finality*.
		// Unrouted, the path falls through to the generic {method} route,
		// which rejects a JSON body with 415 — and a caller that classifies
		// 401/403/404 as definitive and everything else as transient reads
		// that as "try again later" and retries forever against a worker that
		// will never support the feature. A misconfiguration should stop, not
		// spin. Unauthenticated on purpose: "this worker does not introspect"
		// is not a secret, and a caller needs to learn it at preflight rather
		// than after arranging credentials.
		writeIntrospectRefusal(w, http.StatusNotFound, "not_enabled")
		return
	}

	auth := h.authenticate(w, r)
	if auth == nil {
		return // already answered, definitively
	}

	// Caller authorization first: an unauthorized caller must learn nothing
	// about a subject credential, including how long looking it up took.
	caller := auth.Principal
	if !auth.Authenticated || !cfg.principals[caller] {
		slog.Warn("introspection refused: caller is not an introspector",
			"remote_addr", r.RemoteAddr, "principal", caller, "authenticated", auth.Authenticated)
		writeIntrospectRefusal(w, http.StatusForbidden, "not_an_introspector")
		return
	}

	if !cfg.limiter.allow(caller) {
		slog.Warn("introspection rate limit exceeded", "remote_addr", r.RemoteAddr, "principal", caller)
		w.Header().Set("Retry-After", "1")
		writeIntrospectRefusal(w, http.StatusTooManyRequests, "rate_limited")
		return
	}

	credential, ok := readIntrospectToken(r)
	if !ok {
		// Indistinguishable from an unresolvable credential: a malformed body
		// is not worth a separate signal, and giving one lets a caller probe
		// the parser.
		writeIntrospectRefusal(w, http.StatusNotFound, "unresolved")
		return
	}
	digest := TokenDigest(credential)

	if introspectJWSShaped.MatchString(credential) {
		// Refused without ever reaching the resolver. A JWS arriving here is
		// either a caller bug or an attempt to have this worker vouch for a
		// token its asker already rejected.
		slog.Warn("introspection refused: JWS-shaped subject", "principal", caller, "token_digest", digest)
		writeIntrospectRefusal(w, http.StatusNotFound, "unresolved")
		return
	}

	identity, resolved, err := cfg.resolver(credential)
	if err != nil {
		// "I could not find out" is not "it is bad". 503 so the caller
		// retries rather than poisoning its negative cache with an outage.
		retryAfter := defaultAuthRetryAfterSeconds
		var unavailable *AuthUnavailableError
		if errors.As(err, &unavailable) {
			retryAfter = unavailable.retryAfterSeconds()
		}
		slog.Warn("introspection unavailable", "principal", caller, "token_digest", digest, "err", err)
		w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
		writeIntrospectRefusal(w, http.StatusServiceUnavailable, "unavailable")
		return
	}
	if !resolved {
		slog.Info("introspection: credential did not resolve", "principal", caller, "token_digest", digest)
		writeIntrospectRefusal(w, http.StatusNotFound, "unresolved")
		return
	}

	ttl := identity.TTLSeconds
	if ttl <= 0 {
		ttl = cfg.defaultTTL
	}
	slog.Info("introspection: resolved",
		"principal", caller, "token_digest", digest, "resolved_principal", identity.Principal)
	body, err := json.Marshal(introspectResponse{
		Principal:  identity.Principal,
		TokenName:  identity.TokenName,
		TTLSeconds: ttl,
	})
	if err != nil {
		// Unreachable for a struct of strings and an int, but a 500 here is
		// transient, which is the honest classification of "I resolved it and
		// then failed to say so".
		http.Error(w, "introspection encoding failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	// A credential's resolution can change; nothing here may sit in a shared
	// cache. The caller caches, for ttl_seconds, keyed by its own credential.
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if _, werr := w.Write(body); werr != nil {
		slog.Debug("http: response write failed", "route", "introspect", "err", werr)
	}
}

// writeIntrospectRefusal writes a refusal carrying no detail about why. The
// body is a function of the error code alone, so two refusals of the same kind
// are byte-identical.
func writeIntrospectRefusal(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if _, err := fmt.Fprintf(w, `{"error":%q}`, code); err != nil {
		slog.Debug("http: response write failed", "route", "introspect", "err", err)
	}
}

// readIntrospectToken returns the subject credential, or ok=false when the body
// is unusable. Every rejection here is reported as an unresolved credential, so
// nothing about the parser is observable.
func readIntrospectToken(r *http.Request) (string, bool) {
	if r.ContentLength > introspectMaxBodyBytes {
		return "", false
	}
	// One byte past the cap so an unset Content-Length cannot smuggle a larger
	// body past the check above.
	raw, err := io.ReadAll(io.LimitReader(r.Body, introspectMaxBodyBytes+1))
	if err != nil || len(raw) > introspectMaxBodyBytes {
		return "", false
	}
	var body struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return "", false
	}
	if body.Token == "" || len(body.Token) > introspectMaxTokenChars {
		return "", false
	}
	return body.Token, true
}
