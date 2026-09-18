// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"crypto/sha256"
	"encoding/hex"
)

// Token introspection: resolving an opaque bearer credential to a principal.
//
// A reverse proxy that terminates the only public listener has to know *which
// principal a credential authenticates as* before it can authorize anything —
// that principal becomes the policy principal, the row-rule literal and the
// bind parameter of every entitlement query. When the credential is opaque the
// proxy holds no local copy of it, so it has to ask the worker, and it asks
// through vgi_rpc.Identity.v1's introspect_token (identity_v1.go), which owns
// every guard. This file holds only the types that method shares with the
// worker-supplied resolver.
//
// The pre-0.46 HTTP JSON route, POST {prefix}/__introspect_token__, is retired
// and not served, and neither is its VGI-Token-Introspection capability
// header: a client learns whether a worker introspects from reflection
// (vgi_rpc.Identity.v1 hosted, introspect_token in its description).
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

// TokenDigest returns the SHA-256 hex digest of a credential, for diagnostics.
//
// The credential itself must never reach a log, a span or an error message. A
// digest is stable enough to correlate one credential's failures across records
// without being the credential. Exported so a [TokenResolver] has a stable
// identifier to log in its place.
func TokenDigest(credential string) string {
	sum := sha256.Sum256([]byte(credential))
	return hex.EncodeToString(sum[:])
}

// TokenIdentity is the identity an opaque credential authenticates as.
//
// It is the result payload of vgi_rpc.Identity.v1's introspect_token, and the
// vgirpc tags are what pin its wire shape for the protocol: principal utf8 nn,
// token_name utf8 nn defaulting to "", ttl_seconds int64 nn defaulting to 300.
// Field DECLARATION ORDER is part of the schema and therefore of the protocol
// hash.
//
// It NEVER carries claims, and the omission is the design rather than an
// oversight: a pass-through claims field would let a worker choose its caller's
// tenant routing, its row scope and its policy branch, and the asker derives
// everything it needs from the principal alone. Adding one has to be a
// deliberate change to this type, which is the point.
type TokenIdentity struct {
	// Principal is the canonical principal. Return it in the exact form this
	// worker would itself derive, so an asker that normalises differently does
	// not authorize as one identity while the worker serves another.
	Principal string `vgirpc:"principal"`
	// TokenName is a human-readable name for the credential, for audit trails.
	// Never the credential.
	TokenName string `vgirpc:"token_name,default="`
	// TTLSeconds is how long the answer may be cached. The caller does the
	// caching; the worker holds none of its own. Treat it as an
	// authorization window, because for any path the asker serves without
	// re-presenting the credential it is exactly that -- and therefore also the
	// revocation lag.
	//
	// Zero means "do not cache", and vgi_rpc.Identity.v1 honours that
	// verbatim. Build one with [NewTokenIdentity] to get the documented default
	// rather than the zero value.
	TTLSeconds int `vgirpc:"ttl_seconds,default=300"`
}

// NewTokenIdentity builds a [TokenIdentity] carrying the documented default
// cache lifetime, [DefaultTokenTTLSeconds].
//
// It exists because Go has a zero value where the reference implementation has
// an absent field, and the two must not be conflated. `ttl_seconds` is how long
// the asker may cache the answer, which for any path it serves without
// re-presenting the credential is an authorization window -- and therefore the
// revocation lag. So a resolver that returns 0 is saying "do not cache this",
// and the framework honours it: nothing normalises a supplied 0 up to 300,
// because doing so would silently convert "do not cache" into five minutes of
// continued access after revocation.
//
// That leaves the other gap -- a resolver that simply forgot to name a lifetime
// -- and this constructor is the place to close it, at construction, where the
// caller's intent is still known. A struct literal bypasses it and gets 0,
// which fails in the mild direction: more introspection traffic, never a longer
// window.
//
// Set TokenName (and a different TTLSeconds) on the result as needed.
func NewTokenIdentity(principal string) TokenIdentity {
	return TokenIdentity{Principal: principal, TTLSeconds: DefaultTokenTTLSeconds}
}

// TokenResolver resolves an opaque credential to the identity it authenticates
// as. Report an unresolvable credential with ok=false — unknown, expired and
// malformed are one answer on the wire, because reporting which would confirm
// that a guessed credential exists.
//
// Return a non-nil error only when the answer is not *knowable*: a backing
// store that is down is not the same as a credential that is unknown, and a
// caller that negative-caches the second must not cache the first. Return an
// *[IdentityUnavailableError] (see [NewIdentityUnavailable]) so the failure
// reaches the caller as the transient identity_unavailable kind with a retry
// hint, never as a definitive rejection.
//
// A resolver must never return claims, and must never log the credential; use
// [TokenDigest].
type TokenResolver func(credential string) (identity TokenIdentity, ok bool, err error)
