// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"net/http"
	"strings"
	"time"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// Fixed deployment policy for the shared vgi_rpc.Identity.v1 conformance
// group, transcribed from IDENTITY_CONFORMANCE_FIXTURE.md §9.
//
// vgi_rpc.Identity.v1 is almost entirely guards, and every guard reads
// deployment policy: who may introspect, what a credential resolves to,
// whether a grant is minted, how recently the caller authenticated. Against a
// worker whose allowlist and hooks are unknown no cross-port assertion exists,
// because every answer is explicable as policy. So the policy is pinned, and
// six ports configure it identically.
//
// Two rules shape everything below.
//
// **The resolver resolves almost everything.** Rejections are deliberately
// uniform -- unknown, expired, malformed and over-long are one answer -- which
// makes the obvious guard test prove nothing: an over-long credential is *also*
// an unknown one, so a probe using a credential the resolver does not know
// cannot distinguish "the cap refused it" from "the cap let it through and the
// resolver refused it". Delete the cap and such a test stays green. With a
// resolver that answers for whatever it is handed, a rejection can only have
// come from a guard -- and a guard that fails to fire produces a *success*,
// which uniformity cannot disguise.
//
// **Both hooks are pure functions of their arguments.** No clock, no counter,
// no shared state: the worker must answer identically on the first call and the
// thousandth, and on a runtime that dispatches the two methods on different
// goroutines. The one value that would otherwise need a clock -- a grant's
// expires_at -- is a fixed constant, so the wire value can be asserted exactly
// rather than within a tolerance.
//
// SECURITY: the authentication this fixture installs is derived from two
// request headers and is therefore trivially spoofable by anyone who can reach
// the port. It exists so six language ports get a deterministic authenticated
// caller without each standing up an identity provider. It is a TEST FIXTURE
// and must never be deployed.

// IdentityPrincipalHeader names the authenticated principal.
//
// Absent means UNAUTHENTICATED -- not "anonymous but authenticated". Health
// checks and capability probes must keep working without it, and the group
// relies on the absence to test fail-closed behaviour.
const IdentityPrincipalHeader = "X-Conformance-Principal"

// IdentityAuthTimeHeader carries the auth_time claim, placed in the claim map
// VERBATIM as a string and unparsed.
//
// Verbatim is load-bearing. A fixture that parses the value and drops it when
// parsing fails collapses "credential carries an unusable auth_time" into
// "credential carries no auth_time". Both answer stale_auth, so the test stays
// green while the property it names goes untested. The guard is what parses;
// the fixture only transports.
const IdentityAuthTimeHeader = "X-Conformance-Auth-Time"

// The principals the group sends.
const (
	// IdentityIntrospectorPrincipal is the single entry on the allowlist, so
	// "on the list" and "authenticated but not on the list" are both reachable.
	IdentityIntrospectorPrincipal = "conformance-introspector"
	// IdentityOutsiderPrincipal is authenticated and deliberately NOT
	// allowlisted.
	IdentityOutsiderPrincipal = "conformance-outsider"
)

// IdentityMaxAuthAge is the freshness ceiling the worker configures -- the
// documented default, in seconds.
const IdentityMaxAuthAge = 900

// IdentityIntrospectRateLimit is deliberately far above the default 20. Nearly
// every case in the group is an introspection, and a production-tuned limiter
// would fire mid-group with every resulting failure reading as the wrong guard.
// The limiter is not asserted by the shared group; it is covered port-locally
// (see TestIntrospectionIsRateLimited), where its refusal is distinguishable by
// message and so cannot be tested vacuously.
const IdentityIntrospectRateLimit = 100000

// What the resolver answers.
const (
	// identitySubjectPrincipal is the identity every resolvable credential maps
	// to.
	identitySubjectPrincipal = "subject@conformance.example"
	identitySubjectTokenName = "conformance-subject"
	identitySubjectTTL       = 300

	// identityTokenUnknown is the ONE credential this policy calls unknown.
	// Everything it has no rule for resolves, which is what makes a rejection
	// of anything else attributable to a guard.
	identityTokenUnknown = "conformance-unknown-token"
	// identityTokenUnavailable stands in for a backing store that cannot be
	// reached: unknowable rather than unknown. A caller may negative-cache the
	// second and must not cache the first.
	identityTokenUnavailable = "conformance-unavailable-token"
	// identityTokenZeroTTL resolves with ttl_seconds = 0. A resolver naming
	// zero is saying DO NOT CACHE THIS, and the tempting normalisation of <= 0
	// up to the 300 default silently converts that into five minutes of
	// continued access after revocation.
	identityTokenZeroTTL = "conformance-zero-ttl-token"
	// identityTokenMinimal resolves to an identity built with ONLY the
	// principal supplied, so token_name and ttl_seconds land on their
	// documented defaults rather than on values passed explicitly.
	identityTokenMinimal = "conformance-minimal-token"
	// identityTokenPaddedProbe carries two leading and two trailing ASCII
	// spaces and resolves to a DISTINGUISHABLE token_name. The shape test runs
	// on the trimmed credential while the resolver receives the untrimmed
	// original; a port that trims once, up front, and resolves the result
	// passes every other case in the group -- the padded credential still
	// resolves, just via the catch-all rule -- so this is the only thing that
	// can see it.
	identityTokenPaddedProbe     = "  conformance-padded-probe  "
	identityTokenPaddedProbeName = "conformance-padded"
)

// What the minter answers.
const (
	// identityGrantTokenPrefix is followed by the CALLER's principal, which is
	// how "the subject is the caller, never a parameter" becomes observable:
	// two callers making identical requests get two different tokens.
	identityGrantTokenPrefix = "conformance-grant-for:"
	// identityScopeSeparator precedes the echoed scope list, so the list's
	// round trip is visible in the response. An empty list yields a token
	// ending in the separator.
	identityScopeSeparator = "|"
	// identityGrantExpiresAt is fixed rather than now+ttl: a constant can be
	// asserted exactly, which also pins the float64 round trip. expires_at is a
	// declaration rather than an enforcement -- the real lifetime lives inside
	// the opaque token -- so nothing is lost. 2030-01-01T00:00:00Z.
	identityGrantExpiresAt = 1893456000.0
	// identityGrantID is the correlation handle a full grant carries.
	identityGrantID = "conformance-grant-id"
	// identityRefusedPurpose is the purpose this policy declines, so
	// grant_refused reaches the wire.
	identityRefusedPurpose = "conformance-refused"
	// identityMinimalPurpose mints a grant built WITHOUT grant_id, so the
	// field's documented default ("") is observable. Omitted, not passed as "".
	identityMinimalPurpose = "conformance-minimal"
)

// IdentityAuthenticate derives the caller's identity from the two conformance
// headers.
//
// Absent IdentityPrincipalHeader means unauthenticated, and nothing beyond
// auth_time goes into the claim map.
//
// SECURITY: trivially spoofable. Test fixture only; never deploy this.
func IdentityAuthenticate(r *http.Request) (*vgirpc.AuthContext, error) {
	principal := r.Header.Get(IdentityPrincipalHeader)
	if principal == "" {
		// Not "anonymous but authenticated": the group tests fail-closed
		// behaviour by omitting the header, and /health and the capability
		// probe run before anything authenticates.
		return vgirpc.Anonymous(), nil
	}
	auth := &vgirpc.AuthContext{
		Domain:        "conformance",
		Authenticated: true,
		Principal:     principal,
	}
	if raw, ok := r.Header[http.CanonicalHeaderKey(IdentityAuthTimeHeader)]; ok && len(raw) > 0 {
		// Verbatim, as a string, unparsed. See IdentityAuthTimeHeader.
		auth.Claims = map[string]any{"auth_time": raw[0]}
	}
	return auth, nil
}

// IdentityResolveToken resolves a credential under the fixed conformance
// policy. It is a pure function of its argument.
//
// Returns ok=false for the one credential this policy calls unknown, and a
// *vgirpc.IdentityUnavailableError for the one whose answer is not knowable.
// Everything else resolves -- see this file's header for why.
func IdentityResolveToken(credential string) (vgirpc.TokenIdentity, bool, error) {
	switch credential {
	case identityTokenUnavailable:
		return vgirpc.TokenIdentity{}, false, vgirpc.NewIdentityUnavailable("conformance: mapping store unreachable")
	case identityTokenUnknown:
		return vgirpc.TokenIdentity{}, false, nil
	case identityTokenZeroTTL:
		return vgirpc.TokenIdentity{
			Principal:  identitySubjectPrincipal,
			TokenName:  identitySubjectTokenName,
			TTLSeconds: 0,
		}, true, nil
	case identityTokenMinimal:
		// The constructor, so the other two fields land on their documented
		// defaults. Passing "" and 300 explicitly would test the wrong thing:
		// it would prove this fixture can spell the defaults, not that the
		// framework supplies them for an omitted field.
		return vgirpc.NewTokenIdentity(identitySubjectPrincipal), true, nil
	case identityTokenPaddedProbe:
		return vgirpc.TokenIdentity{
			Principal:  identitySubjectPrincipal,
			TokenName:  identityTokenPaddedProbeName,
			TTLSeconds: identitySubjectTTL,
		}, true, nil
	}
	return vgirpc.TokenIdentity{
		Principal:  identitySubjectPrincipal,
		TokenName:  identitySubjectTokenName,
		TTLSeconds: identitySubjectTTL,
	}, true, nil
}

// IdentityMintGrant mints a grant under the fixed conformance policy. It is a
// pure function of its arguments.
//
// ttlSeconds is deliberately ignored: it is a request, the returned ExpiresAt
// is authoritative, and honouring it would need a clock and make the value
// unassertable.
func IdentityMintGrant(principal, purpose string, scopes []string, ttlSeconds int64) (vgirpc.IssuedGrant, error) {
	_ = ttlSeconds
	if purpose == identityRefusedPurpose {
		return vgirpc.IssuedGrant{}, &vgirpc.GrantRefusedError{Detail: "conformance: this purpose is refused"}
	}
	token := identityGrantTokenPrefix + principal + identityScopeSeparator + strings.Join(scopes, ",")
	if purpose == identityMinimalPurpose {
		// GrantID omitted, so its documented default ("") is observable.
		return vgirpc.IssuedGrant{Token: token, ExpiresAt: identityGrantExpiresAt}, nil
	}
	return vgirpc.IssuedGrant{
		Token:     token,
		ExpiresAt: identityGrantExpiresAt,
		GrantID:   identityGrantID,
	}, nil
}

// IdentityMode selects which hooks the conformance worker configures, and
// therefore which methods it hosts.
type IdentityMode string

const (
	// IdentityModeOff configures no hook, so the protocol is not hosted at
	// all. This is what the plain conformance worker does, and what
	// TestIdentityAbsentByDefault asserts against it.
	IdentityModeOff IdentityMode = "off"
	// IdentityModeBoth configures the resolve hook and the mint hook.
	IdentityModeBoth IdentityMode = "both"
	// IdentityModeIntrospectOnly configures the resolve hook alone, so
	// issue_grant is absent rather than hosted-and-refusing -- and the
	// protocol hash narrows with it.
	IdentityModeIntrospectOnly IdentityMode = "introspect-only"
)

// IdentityConfigFor returns the configuration for one fixture mode, or ok=false
// for IdentityModeOff.
func IdentityConfigFor(mode IdentityMode) (vgirpc.IdentityConfig, bool) {
	cfg := vgirpc.IdentityConfig{
		IntrospectPrincipals: []string{IdentityIntrospectorPrincipal},
		IntrospectRateLimit:  IdentityIntrospectRateLimit,
		MaxAuthAge:           IdentityMaxAuthAge * time.Second,
	}
	switch mode {
	case IdentityModeBoth:
		cfg.ResolveToken = IdentityResolveToken
		cfg.MintGrant = IdentityMintGrant
	case IdentityModeIntrospectOnly:
		cfg.ResolveToken = IdentityResolveToken
	default:
		return vgirpc.IdentityConfig{}, false
	}
	return cfg, true
}
