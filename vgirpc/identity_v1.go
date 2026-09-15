// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// vgi_rpc.Identity.v1 -- resolving a credential, and minting a grant.
//
// Identity lives here, at the RPC layer, rather than in any application
// protocol: a bearer token is not a VGI concept, the auth primitives it builds
// on (AuthContext, ChainAuthenticate, AuthUnavailableError) are already here,
// and implementing it once is the whole point. It was previously an HTTP JSON
// route, POST {prefix}/__introspect_token__ (still served; see
// introspect_token.go), which meant it existed only on one transport and had to
// be hand-written in every port.
//
// Two methods share this file's guards, and they are guarded *differently* on
// purpose.
//
// introspect_token answers "which principal is this credential" for a reverse
// proxy that terminates the only public listener. The answer is an identity
// assertion made by the thing being protected, which the asker then acts on
// using credentials the worker does not hold -- storage credentials,
// entitlement lookups, policy-tier selection. "Trust it as much as you trust
// the worker" is the wrong frame: it must be trusted *more*. So every rejection
// is uniform, the caller must be on an allowlist with no permissive default, a
// JWS-shaped subject never reaches the resolver, and the whole thing is rate
// limited.
//
// issue_grant mints a credential for the *calling* user, so it is not an oracle
// about anybody else. It therefore needs no allowlist and no rate limit, and
// its rejections are deliberately *actionable*: a console that cannot tell
// "your login is too old" from "no" cannot know to re-prompt.
//
// Errors carry a stable ErrorKind. That is load-bearing rather than decorative:
// these used to be a bespoke HTTP route whose callers classified
// definitive-vs-transient on the HTTP status (404 vs 503). As protocol methods
// every handler exception surfaces the same way, so error_kind is now the
// *only* signal a caller has. A caller that negative-caches a transient failure
// locks out valid users; one that retries a definitive rejection hammers the
// worker.

// IdentityProtocolName is the wire name of the identity protocol.
//
// Framework-owned, under the reserved vgi_rpc. prefix, so an application cannot
// register something else under the name a proxy trusts for identity answers.
const IdentityProtocolName = "vgi_rpc.Identity.v1"

// MaxTokenBytes caps a credential we will even attempt to resolve. Anything
// longer is not a bearer token; refusing early keeps a resolver from being
// handed megabytes.
//
// The unit is UTF-8 BYTES, and the name says so because the ports reached for
// three different units and none of them said which: codepoints (Python, Rust),
// UTF-16 code units (Java, C#, TypeScript), bytes (Go, C++). All three agree for
// an ASCII credential -- which every real bearer token is -- so the divergence
// is invisible today and only appears on a multibyte one. Bytes is what the
// purpose implies (what a resolver would actually have to handle) and the most
// conservative of the three, so standardising on it can only refuse earlier.
// Go's len(string) is already bytes, so this is a name that stopped leaving the
// unit to the reader, not a change of behaviour.
const MaxTokenBytes = 4096

// DefaultTokenTTLSeconds is how long a resolved identity may be cached when the
// answer names no lifetime of its own.
//
// It is a default for an ABSENT value -- the decode-side default of the
// ttl_seconds column, and what [NewTokenIdentity] supplies -- and it is never a
// coercion applied to a lifetime a resolver actually returned. See
// [NewTokenIdentity] for why that distinction is a security property rather
// than pedantry.
const DefaultTokenTTLSeconds = 300

// DefaultIntrospectRateLimit is the per-caller, per-second introspection
// ceiling applied when a deployment names none.
const DefaultIntrospectRateLimit = 20

// DefaultMaxAuthAge is how recently a caller must have authenticated to mint a
// grant, when a deployment names no ceiling of its own.
const DefaultMaxAuthAge = 900 * time.Second

// defaultIdentityRetryAfter is the retry hint on an IdentityUnavailableError
// that names none. Short on purpose: a hint to retry, not a backoff schedule.
const defaultIdentityRetryAfter = 5

// identityJWSShaped matches three dot-separated base64url segments -- a JWS.
// Such a credential is validated locally against a key set and MUST NOT be
// routed to a resolver: doing so sends a bearer token the asker may itself have
// rejected (expired, wrong audience) to a third party that might accept it.
//
// Applied to the TRIMMED credential (see [RejectJWSShaped]), which is what makes
// the anchors here uncontroversial rather than load-bearing.
var identityJWSShaped = regexp.MustCompile(`\A[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*\z`)

// ---------------------------------------------------------------------------
// Error taxonomy
//
// The ErrorKind strings are the wire contract, not an implementation detail.
// ---------------------------------------------------------------------------

// IntrospectionRefusedError reports that the caller may not introspect.
//
// Definitive: a caller may cache this. Authentication is not the same
// capability as introspection -- a deployment where any valid credential may
// introspect lets any user test guesses of any other user's credential at
// unlimited rate, and resolve a stolen one to its owner.
type IntrospectionRefusedError struct {
	Detail string
}

func (e *IntrospectionRefusedError) Error() string {
	if e.Detail == "" {
		return "introspection refused"
	}
	return e.Detail
}

// ErrorKind returns the stable machine-readable category.
func (e *IntrospectionRefusedError) ErrorKind() string { return "introspection_refused" }

// ErrorType is the wire-stable exception class name, matching the reference
// port so a cross-language client sees one name for one condition.
func (e *IntrospectionRefusedError) ErrorType() string { return "IntrospectionRefusedError" }

// TokenUnresolvedError reports that the subject credential did not resolve.
//
// Definitive, and deliberately uniform: unknown, expired and malformed are one
// answer, because reporting which would confirm that a guessed credential
// exists.
type TokenUnresolvedError struct {
	Detail string
}

func (e *TokenUnresolvedError) Error() string {
	if e.Detail == "" {
		return "unresolved"
	}
	return e.Detail
}

// ErrorKind returns the stable machine-readable category.
func (e *TokenUnresolvedError) ErrorKind() string { return "token_unresolved" }

// ErrorType is the wire-stable exception class name.
func (e *TokenUnresolvedError) ErrorType() string { return "TokenUnresolvedError" }

// StaleAuthError reports that the caller has not authenticated recently enough
// to mint a grant.
//
// Definitive but *actionable*, unlike the introspection rejections: this is
// always about the caller themselves, so naming the reason leaks nothing and is
// the only way a console learns to re-prompt.
type StaleAuthError struct {
	Detail string
}

func (e *StaleAuthError) Error() string {
	if e.Detail == "" {
		return "stale authentication"
	}
	return e.Detail
}

// ErrorKind returns the stable machine-readable category.
func (e *StaleAuthError) ErrorKind() string { return "stale_auth" }

// ErrorType is the wire-stable exception class name.
func (e *StaleAuthError) ErrorType() string { return "StaleAuthError" }

// GrantRefusedError reports that the worker declined to mint this grant.
//
// Definitive. The worker holds the policy; the framework only asked.
type GrantRefusedError struct {
	Detail string
}

func (e *GrantRefusedError) Error() string {
	if e.Detail == "" {
		return "grant refused"
	}
	return e.Detail
}

// ErrorKind returns the stable machine-readable category.
func (e *GrantRefusedError) ErrorKind() string { return "grant_refused" }

// ErrorType is the wire-stable exception class name.
func (e *GrantRefusedError) ErrorType() string { return "GrantRefusedError" }

// IdentityUnavailableError reports that the answer is not *knowable* -- a
// backing store is down, a 5xx upstream.
//
// Transient, and distinct from a definitive rejection: a caller that
// negative-caches "unknown" must not cache this.
//
// Deliberately NOT an *RpcError of type "ValueError", which is the Go shape of
// the same hazard the Python reference avoids by not subclassing ValueError:
// [ChainAuthenticate] advances to the next authenticator on a ValueError
// *RpcError, so a sidecar outage reported as one reads as "not my credential,
// try the next" and emerges as a 401 from the end of the chain -- turning a
// thirty-second blip into a fleet-wide re-login. It is also not a
// [TokenUnresolvedError], so errors.As cannot collapse the two.
type IdentityUnavailableError struct {
	Detail string
	// RetryAfter is the advertised wait, in seconds. Zero means the package
	// default.
	RetryAfter int
}

// NewIdentityUnavailable builds an [IdentityUnavailableError] with the default
// retry hint.
func NewIdentityUnavailable(detail string) *IdentityUnavailableError {
	return &IdentityUnavailableError{Detail: detail}
}

func (e *IdentityUnavailableError) Error() string {
	if e.Detail == "" {
		return "identity lookup unavailable"
	}
	return e.Detail
}

// ErrorKind returns the stable machine-readable category.
func (e *IdentityUnavailableError) ErrorKind() string { return "identity_unavailable" }

// ErrorType is the wire-stable exception class name.
func (e *IdentityUnavailableError) ErrorType() string { return "IdentityUnavailableError" }

// RetryAfterSeconds returns the advertised wait, defaulted.
func (e *IdentityUnavailableError) RetryAfterSeconds() int {
	if e.RetryAfter > 0 {
		return e.RetryAfter
	}
	return defaultIdentityRetryAfter
}

// ---------------------------------------------------------------------------
// Rate limiting
// ---------------------------------------------------------------------------

// RateLimiter is a fixed-window request limiter, keyed by caller.
//
// Present because introspection is a credential-to-identity oracle even when
// correctly restricted: an allowlisted caller whose own credential leaks can
// still test guesses. Rate limiting does not close that, it bounds it.
//
// Fixed-window rather than a token bucket: a window admits at most twice the
// rate across a boundary, which is a rounding error here, and the state is one
// integer per caller rather than a float that has to be aged.
//
// Safe for concurrent use: every server transport dispatches calls from more
// than one goroutine, so a limiter that raced would admit more than its ceiling
// under exactly the load it exists to bound.
type RateLimiter struct {
	mu          sync.Mutex
	perWindow   int
	window      time.Duration
	windowStart time.Time
	counts      map[string]int
}

// NewRateLimiter builds a limiter admitting perWindow requests per window.
func NewRateLimiter(perWindow int, window time.Duration) *RateLimiter {
	return &RateLimiter{
		perWindow: perWindow,
		window:    window,
		counts:    make(map[string]int),
	}
}

// Allow reports whether key may make a request in the current window.
func (l *RateLimiter) Allow(key string) bool {
	return l.allowAt(key, time.Now())
}

// allowAt is [RateLimiter.Allow] with the clock supplied, so a test can pin
// window behaviour without sleeping.
func (l *RateLimiter) allowAt(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.windowStart) >= l.window {
		// Whole-map reset rather than per-key ageing: an attacker cycling keys
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

// ---------------------------------------------------------------------------
// Guards
// ---------------------------------------------------------------------------

// NormalisePrincipals validates the introspector allowlist.
//
// There is no permissive default: "any authenticated caller" is precisely the
// configuration that turns introspection into an open oracle, so it cannot be
// reached by omission. An empty or absent allowlist is an error at
// construction, not at the first call -- a worker that would refuse every
// introspection should fail to start rather than serve traffic until someone
// tries.
func NormalisePrincipals(principals []string) (map[string]bool, error) {
	allowed := make(map[string]bool, len(principals))
	for _, p := range principals {
		if p != "" {
			allowed[p] = true
		}
	}
	if len(allowed) == 0 {
		return nil, fmt.Errorf(
			"introspect_principals must name at least one principal. " +
				"Introspection is a distinct capability from authentication: " +
				"allowing any authenticated caller lets any user resolve any " +
				"other user's credential to its owner")
	}
	return allowed, nil
}

// CheckIntrospector returns the caller principal, or refuses.
//
// Checked before anything touches the subject credential: an unauthorized
// caller must not learn anything about it, including how long it took.
func CheckIntrospector(auth *AuthContext, principals map[string]bool) (string, error) {
	if auth == nil {
		auth = Anonymous()
	}
	caller := auth.Principal
	if !auth.Authenticated || !principals[caller] {
		return "", &IntrospectionRefusedError{Detail: "caller is not an introspector"}
	}
	return caller, nil
}

// RejectJWSShaped refuses a JWS-shaped subject before it reaches a resolver.
//
// Empty, over-long and JWS-shaped are all one answer, identical to the answer
// an unknown credential gets: distinguishing them would tell a caller probing
// the guard which of its guesses was closer.
//
// The shape test runs against the whitespace-TRIMMED credential, while the
// resolver still receives what the caller actually sent. Without trimming,
// "aaa.bbb.ccc\n" is not JWS-shaped to a strict matcher and gets routed onward
// -- precisely what this guard exists to stop -- and Go's \A..\z anchors are
// strict. Anchor semantics are the least portable corner of seven regex
// dialects and the ports split three ways on that one input; the reference
// happened to refuse it only because Python's $ matches before a single
// trailing newline, and it admitted two. Trimming first is the rule that
// survives translation, because it depends on no dialect at all -- and it can
// only ADD refusals, never remove one.
//
// A whitespace-only credential is refused: it is not a credential.
//
// The length check stays on the ORIGINAL. What arrived is what a resolver would
// have to handle, and trimming must not talk a megabyte of padding down into
// the allowance.
//
// Trimming is for the shape test ONLY. Rewriting a credential before resolving
// it would make the worker answer about a string the caller never sent.
func RejectJWSShaped(token string) error {
	candidate := strings.TrimSpace(token)
	if candidate == "" || len(token) > MaxTokenBytes || identityJWSShaped.MatchString(candidate) {
		return &TokenUnresolvedError{Detail: "unresolved"}
	}
	return nil
}

// CheckFreshness returns the caller's auth_time, or refuses if it is missing or
// stale.
//
// A credential with no verifiable auth_time cannot mint. That single rule is
// what stops a grant being used to mint another grant: a grant is not an
// IdP-issued token, so it carries no auth_time, so the lineage cannot escape
// the identity provider. It also makes subprocess, unix and TCP transports fail
// closed for free -- there is no authenticated principal there at all.
//
// A static bearer proves a machine holds a secret, never that a human just
// authenticated, so it is refused here too.
//
// Warning: auth_time is an OIDC claim meaning *when this session began*, which
// can be arbitrarily old while still present and cryptographically valid.
// Requiring it is not the same as requiring a recent login: the deployment must
// send max_age (or an appropriate acr) at the authorize endpoint for this guard
// to mean what it says.
func CheckFreshness(auth *AuthContext, maxAuthAge time.Duration) (float64, error) {
	return checkFreshnessAt(auth, maxAuthAge, time.Now())
}

// checkFreshnessAt is [CheckFreshness] with the clock supplied.
func checkFreshnessAt(auth *AuthContext, maxAuthAge time.Duration, now time.Time) (float64, error) {
	if auth == nil {
		auth = Anonymous()
	}
	if !auth.Authenticated || auth.Principal == "" {
		return 0, &StaleAuthError{Detail: "caller is not authenticated"}
	}
	raw, ok := auth.Claims["auth_time"]
	if !ok || raw == nil {
		return 0, &StaleAuthError{
			Detail: "credential carries no auth_time; only a recently authenticated user may mint a grant",
		}
	}
	authTime, ok := claimAsSeconds(raw)
	if !ok {
		return 0, &StaleAuthError{Detail: "credential carries an unusable auth_time"}
	}
	age := float64(now.UnixNano())/1e9 - authTime
	if age > maxAuthAge.Seconds() {
		return 0, &StaleAuthError{Detail: fmt.Sprintf(
			"last authentication was %.0fs ago, which exceeds the %.0fs ceiling "+
				"for minting a grant; re-authenticate", age, maxAuthAge.Seconds())}
	}
	return authTime, nil
}

// claimAsSeconds coerces an auth_time claim to a Unix timestamp.
//
// A claim map is filled by whichever decoder ran -- encoding/json yields
// float64, a JWT library may yield json.Number, and a header-derived context
// may yield a string -- so the guard accepts every numeric spelling rather than
// refusing a valid credential because of how it was parsed. Anything that is
// not a number is refused: a claim that cannot be read is not a claim that can
// be trusted.
func claimAsSeconds(raw any) (float64, bool) {
	switch v := raw.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint64:
		return float64(v), true
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(v, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

// ---------------------------------------------------------------------------
// The payloads
// ---------------------------------------------------------------------------

// IssuedGrant is a standing delegation credential.
//
// Token is the credential, and it is OPAQUE to the framework -- the worker owns
// the format entirely (a sealed envelope, a database row, or a credential
// brokered from the IdP are all equally valid and equally invisible here).
// Never parsed, never logged.
//
// ExpiresAt is the Unix timestamp after which the worker will stop honouring
// the grant. Required *because* the framework cannot enforce it: the real
// lifetime lives inside the opaque token, so this is a declaration rather than
// an enforcement. A worker that must state a lifetime has thought about one.
//
// GrantID is a correlation handle for the audit trail. Not a credential and not
// secret -- it is what ties a mint record to later use.
type IssuedGrant struct {
	Token     string  `vgirpc:"token"`
	ExpiresAt float64 `vgirpc:"expires_at"`
	GrantID   string  `vgirpc:"grant_id,default="`
}

// introspectTokenParams is the single parameter of introspect_token.
//
// The field is declared non-null utf8, and its name is part of the protocol
// hash: a port that spells it differently hosts a different protocol.
type introspectTokenParams struct {
	Token string `vgirpc:"token"`
}

// issueGrantParams are the parameters of issue_grant.
//
// There is deliberately NO subject parameter. The subject is always the
// caller's authenticated principal, so cross-subject minting is closed by
// construction rather than by a check that could be forgotten in one of several
// ports. Do not add one.
//
// Scopes is a list whose ITEM is nullable -- list<item?:utf8>. That is part of
// the wire shape and therefore of the protocol hash; a port that makes the item
// non-null hosts a different protocol under the same name.
type issueGrantParams struct {
	Purpose    string   `vgirpc:"purpose"`
	Scopes     []string `vgirpc:"scopes"`
	TTLSeconds int64    `vgirpc:"ttl_seconds"`
}

// GrantMinter mints a standing delegation credential for the calling principal.
//
// OAuth cannot express durable delegation: it fuses the grant, the credential
// and the session into one refresh token, so an IdP shortening session lifetime
// shortens the grant. This is the durable record -- minted while the user is
// present, presented later by unattended automation as an ordinary bearer.
//
// ttlSeconds is a request, not an instruction: the minter may return a shorter
// lifetime, and the returned ExpiresAt is authoritative. scopes are opaque to
// the framework, which neither interprets nor validates them.
//
// Return a [GrantRefusedError] to decline; return an
// *[IdentityUnavailableError] when the answer is not knowable, so the caller
// retries rather than caching a refusal.
type GrantMinter func(principal, purpose string, scopes []string, ttlSeconds int64) (IssuedGrant, error)

// ---------------------------------------------------------------------------
// The implementation
// ---------------------------------------------------------------------------

// IdentityConfig configures [NewIdentity].
type IdentityConfig struct {
	// ResolveToken resolves an opaque credential. Requires IntrospectPrincipals.
	ResolveToken TokenResolver
	// MintGrant mints a grant for the calling principal.
	MintGrant GrantMinter
	// IntrospectPrincipals may call introspect_token. Required whenever
	// ResolveToken is supplied; there is no permissive default.
	IntrospectPrincipals []string
	// IntrospectRateLimit is introspections allowed per caller per second.
	// Zero means [DefaultIntrospectRateLimit].
	IntrospectRateLimit int
	// MaxAuthAge is how recently a caller must have authenticated to mint a
	// grant. Zero means [DefaultMaxAuthAge].
	MaxAuthAge time.Duration
}

// IdentityImpl applies this file's guards, then delegates to worker-supplied
// hooks.
//
// The framework owns the guards and owns none of the policy. It decides who may
// ask, how often, and what shape of credential is refused outright; the worker
// decides what a credential resolves to and whether a grant is minted. That
// split is deliberate -- the guards are the part that is identical in every
// deployment and catastrophic to get wrong, and the policy is the part that is
// different in every deployment and cannot be guessed.
//
// A method whose hook is absent is not registered at all, so the protocol a
// server hosts describes what it actually does. See [IdentityImpl.OfferedMethods].
type IdentityImpl struct {
	resolveToken TokenResolver
	mintGrant    GrantMinter
	principals   map[string]bool
	limiter      *RateLimiter
	maxAuthAge   time.Duration
}

// NewIdentity builds the implementation, validating the configuration.
//
// Validation happens here rather than on the first call: a worker that would
// refuse every introspection should fail to start rather than serve traffic
// until someone tries.
func NewIdentity(cfg IdentityConfig) (*IdentityImpl, error) {
	impl := &IdentityImpl{
		resolveToken: cfg.ResolveToken,
		mintGrant:    cfg.MintGrant,
		maxAuthAge:   cfg.MaxAuthAge,
	}
	if impl.maxAuthAge <= 0 {
		impl.maxAuthAge = DefaultMaxAuthAge
	}
	rate := cfg.IntrospectRateLimit
	if rate <= 0 {
		rate = DefaultIntrospectRateLimit
	}
	impl.limiter = NewRateLimiter(rate, time.Second)
	if cfg.ResolveToken != nil {
		allowed, err := NormalisePrincipals(cfg.IntrospectPrincipals)
		if err != nil {
			return nil, err
		}
		impl.principals = allowed
	} else {
		impl.principals = map[string]bool{}
	}
	return impl, nil
}

// OfferedMethods returns, sorted, the methods this deployment can actually
// answer.
//
// A method whose hook is absent is not registered, so the protocol a server
// hosts describes what it does. A worker that resolves credentials but does not
// mint grants offers introspect_token and not issue_grant, and a client learns
// that from reflection rather than by calling and reading an error.
func (i *IdentityImpl) OfferedMethods() []string {
	var offered []string
	if i.resolveToken != nil {
		offered = append(offered, "introspect_token")
	}
	if i.mintGrant != nil {
		offered = append(offered, "issue_grant")
	}
	sort.Strings(offered)
	return offered
}

// IntrospectToken resolves token, after checking the caller may ask.
//
// The guard ORDER here is load-bearing and must not be reordered for tidiness.
// Authorization and rate limiting come before anything looks at the subject
// credential -- before the length check, before the JWS check -- so an
// unauthorized caller learns nothing about it, including how long looking at it
// took. A caller off the allowlist presenting an over-long or JWS-shaped token
// must still get introspection_refused, never token_unresolved.
func (i *IdentityImpl) IntrospectToken(token string, ctx *CallContext) (TokenIdentity, error) {
	if i.resolveToken == nil {
		// The belt to registration's braces: the method is not hosted when the
		// hook is absent, and refuses if a caller reaches it anyway.
		return TokenIdentity{}, &IntrospectionRefusedError{Detail: "this worker does not resolve credentials"}
	}

	auth := Anonymous()
	if ctx != nil && ctx.Auth != nil {
		auth = ctx.Auth
	}
	caller, err := CheckIntrospector(auth, i.principals)
	if err != nil {
		return TokenIdentity{}, err
	}
	if !i.limiter.Allow(caller) {
		return TokenIdentity{}, &IntrospectionRefusedError{Detail: "introspection rate limit exceeded"}
	}
	if err := RejectJWSShaped(token); err != nil {
		return TokenIdentity{}, err
	}

	identity, ok, err := i.resolveToken(token)
	if err != nil {
		// "I could not find out" is not "it is bad". Propagated as-is so the
		// transient/definitive distinction survives to the caller.
		return TokenIdentity{}, err
	}
	if !ok {
		// Uniform with malformed and expired: reporting which would confirm
		// that a guessed credential exists.
		return TokenIdentity{}, &TokenUnresolvedError{Detail: "unresolved"}
	}
	// Returned exactly as the resolver built it, TTLSeconds included. A zero
	// here is NOT normalised up to DefaultTokenTTLSeconds: see
	// [NewTokenIdentity].
	return identity, nil
}

// IssueGrant mints a grant for the caller, after checking they authenticated
// recently.
//
// The subject is the caller, never a parameter: cross-subject minting is closed
// by construction rather than by a check that could be forgotten in one of
// several ports.
func (i *IdentityImpl) IssueGrant(purpose string, scopes []string, ttlSeconds int64, ctx *CallContext) (IssuedGrant, error) {
	if i.mintGrant == nil {
		return IssuedGrant{}, &GrantRefusedError{Detail: "this worker does not mint grants"}
	}
	auth := Anonymous()
	if ctx != nil && ctx.Auth != nil {
		auth = ctx.Auth
	}
	if _, err := CheckFreshness(auth, i.maxAuthAge); err != nil {
		return IssuedGrant{}, err
	}
	return i.mintGrant(auth.Principal, purpose, scopes, ttlSeconds)
}

// ---------------------------------------------------------------------------
// Server wiring
// ---------------------------------------------------------------------------

// RegisterIdentity hosts vgi_rpc.Identity.v1 on s, narrowed to the methods the
// deployment configured.
//
// Call it AFTER [RegisterReflection] so identity appears in reflection's own
// output. (Reflection reads the live binding table in this port, so the order
// is not load-bearing here -- but it is in ports whose reflection snapshots at
// registration, and a port-to-port difference in what a server advertises is
// exactly the drift these protocols exist to prevent.)
//
// If neither hook is configured the protocol is not registered at all, and
// RegisterIdentity reports no error: absent beats routed-and-refusing, and it is
// what keeps a dependency upgrade from growing a credential-to-identity oracle
// on every existing worker.
func RegisterIdentity(s *Server, impl *IdentityImpl) error {
	if impl == nil {
		return fmt.Errorf("vgirpc: RegisterIdentity requires an *IdentityImpl; build one with NewIdentity")
	}
	offered := impl.OfferedMethods()
	if len(offered) == 0 {
		return nil
	}

	sub := NewServer()
	sub.SetServiceName(IdentityProtocolName)
	for _, name := range offered {
		switch name {
		case "introspect_token":
			Unary(sub, name, func(_ context.Context, cc *CallContext, p introspectTokenParams) (TokenIdentity, error) {
				return impl.IntrospectToken(p.Token, cc)
			})
		case "issue_grant":
			Unary(sub, name, func(_ context.Context, cc *CallContext, p issueGrantParams) (IssuedGrant, error) {
				return impl.IssueGrant(p.Purpose, p.Scopes, p.TTLSeconds, cc)
			})
		}
	}

	hash, err := bindingHash(IdentityProtocolName, sub.methods)
	if err != nil {
		return err
	}
	// Not version-exempt, unlike reflection: identity is not the protocol a
	// version-mismatched client calls to learn what mismatched, so it has no
	// claim on being reachable across a version gap.
	return s.AddProtocol(&protocolBinding{
		Name:    IdentityProtocolName,
		Methods: sub.methods,
		Hash:    hash,
		Impl:    sub,
	}, true)
}
