// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

// vgi_rpc.Identity.v1 -- resolving a credential, and minting a grant.
//
// The two methods are guarded very differently and the difference is the point,
// so most of what is tested here is the *asymmetry*: introspection answers a
// question about somebody else's credential and is therefore an oracle that has
// to be locked down; issuance is always about the caller and therefore is not.
//
// Ported from the reference suite in
// vgi-rpc-python/tests/test_token_identity.py, plus the hash vectors and guard
// order the port contract (IDENTITY_V1_SPEC.md) pins.

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// idAuth builds a caller. authTime < 0 means the credential carries no
// auth_time claim at all -- which is what a static bearer, and a grant, look
// like.
func idAuth(principal string, authenticated bool, authTime float64) *AuthContext {
	claims := map[string]any{}
	if authTime >= 0 {
		claims["auth_time"] = authTime
	}
	return &AuthContext{
		Authenticated: authenticated,
		Principal:     principal,
		Domain:        "test",
		Claims:        claims,
	}
}

func idCtx(auth *AuthContext) *CallContext { return &CallContext{Auth: auth} }

// noAuthTime is the sentinel for idAuth: no auth_time claim.
const noAuthTime = -1.0

func idResolver(token string) (TokenIdentity, bool, error) {
	if token == "good" {
		// NewTokenIdentity, not a struct literal: the reference resolver names
		// no lifetime and gets 300 from a dataclass default, and the
		// constructor is where Go supplies the same thing.
		id := NewTokenIdentity("bob")
		id.TokenName = "ci-key"
		return id, true, nil
	}
	return TokenIdentity{}, false, nil
}

func idMinter(principal, _ string, _ []string, ttlSeconds int64) (IssuedGrant, error) {
	return IssuedGrant{
		Token:     "grant-for-" + principal,
		ExpiresAt: float64(time.Now().Unix() + ttlSeconds),
		GrantID:   "g1",
	}, nil
}

// mustIdentity builds an IdentityImpl, failing the test on a configuration
// error.
func mustIdentity(t *testing.T, cfg IdentityConfig) *IdentityImpl {
	t.Helper()
	impl, err := NewIdentity(cfg)
	if err != nil {
		t.Fatalf("NewIdentity: %v", err)
	}
	return impl
}

// introspectingIdentity is the configuration introspection tests share.
func introspectingIdentity(t *testing.T, cfg IdentityConfig) *IdentityImpl {
	t.Helper()
	cfg.ResolveToken = idResolver
	cfg.IntrospectPrincipals = []string{"proxy"}
	return mustIdentity(t, cfg)
}

// idServer is an application server with reflection and, when impl is non-nil,
// identity registered after it.
func idServer(t *testing.T, impl *IdentityImpl) *Server {
	t.Helper()
	s := NewServer()
	s.SetServiceName("demo.App.v1")
	Unary(s, "echo", func(_ context.Context, _ *CallContext, p reflEcho) (string, error) {
		return p.Value, nil
	})
	if err := RegisterReflection(s); err != nil {
		t.Fatalf("RegisterReflection: %v", err)
	}
	if impl != nil {
		// After reflection, so identity appears in reflection's output.
		if err := RegisterIdentity(s, impl); err != nil {
			t.Fatalf("RegisterIdentity: %v", err)
		}
	}
	return s
}

// ---------------------------------------------------------------------------
// Registration: absent beats routed-and-refusing
// ---------------------------------------------------------------------------

// A dependency upgrade must not grow a credential-to-identity oracle on every
// worker that already exists.
func TestIdentityIsAbsentByDefault(t *testing.T) {
	if _, ok := idServer(t, nil).bindings()[IdentityProtocolName]; ok {
		t.Fatal("identity must not be hosted unless a deployment asks for it")
	}
}

// What the server hosts describes what it actually does. A worker that resolves
// credentials but does not mint grants offers one method, and a client learns
// that from reflection rather than by calling and reading an error.
func TestIdentityOffersOnlyMethodsWithHooks(t *testing.T) {
	cases := []struct {
		name string
		cfg  IdentityConfig
		want []string
	}{
		{"resolve only", IdentityConfig{ResolveToken: idResolver, IntrospectPrincipals: []string{"proxy"}},
			[]string{"introspect_token"}},
		{"mint only", IdentityConfig{MintGrant: idMinter}, []string{"issue_grant"}},
		{"both", IdentityConfig{ResolveToken: idResolver, IntrospectPrincipals: []string{"proxy"}, MintGrant: idMinter},
			[]string{"introspect_token", "issue_grant"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mustIdentity(t, tc.cfg).OfferedMethods()
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("OfferedMethods() = %v, want %v", got, tc.want)
			}
		})
	}
}

// Narrowing the method set narrows the binding with it.
func TestIdentityBindingCarriesOnlyTheOfferedMethods(t *testing.T) {
	s := idServer(t, mustIdentity(t, IdentityConfig{MintGrant: idMinter}))
	got := sortedKeys(s.bindings()[IdentityProtocolName].Methods)
	if !reflect.DeepEqual(got, []string{"issue_grant"}) {
		t.Errorf("hosted methods = %v, want [issue_grant]", got)
	}
}

// Neither hook configured means the protocol is not registered at all -- and
// that is not an error, it is the default posture.
func TestIdentityWithNoHooksIsNotRegistered(t *testing.T) {
	s := idServer(t, mustIdentity(t, IdentityConfig{}))
	if _, ok := s.bindings()[IdentityProtocolName]; ok {
		t.Error("a deployment configuring neither hook must host nothing")
	}
}

// Framework-owned, so an application cannot register something else under the
// name a proxy trusts for identity answers.
func TestIdentityClaimsTheReservedPrefix(t *testing.T) {
	if !strings.HasPrefix(IdentityProtocolName, "vgi_rpc.") {
		t.Errorf("%q must claim the reserved prefix", IdentityProtocolName)
	}
	if err := ValidateProtocolName(IdentityProtocolName, false); err == nil {
		t.Error("an application must not be able to claim the identity protocol name")
	}
}

// Registered after reflection, so a client discovers it the same way it
// discovers everything else rather than by being told out of band.
func TestIdentityAppearsInReflection(t *testing.T) {
	s := idServer(t, introspectingIdentity(t, IdentityConfig{MintGrant: idMinter}))

	found := false
	for _, p := range s.listProtocols().Protocols {
		if p.Protocol == IdentityProtocolName {
			found = true
			if len(p.ProtocolHash) != 64 {
				t.Errorf("hash = %q, want 64 hex chars", p.ProtocolHash)
			}
		}
	}
	if !found {
		t.Fatalf("list_protocols omitted identity; have %v", sortedKeys(s.bindings()))
	}

	desc, err := s.describeProtocol(IdentityProtocolName)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, m := range desc.Methods {
		names[m.Name] = true
		if !m.HasReturn {
			t.Errorf("%s must declare has_return", m.Name)
		}
		if m.HasHeader {
			t.Errorf("%s must not declare a header", m.Name)
		}
		if m.MethodType != "unary" {
			t.Errorf("%s type = %q, want unary", m.Name, m.MethodType)
		}
	}
	if !names["introspect_token"] || !names["issue_grant"] {
		t.Errorf("describe omitted a method: %v", names)
	}
}

// ---------------------------------------------------------------------------
// The wire shape: the hash vectors from the port contract
// ---------------------------------------------------------------------------

// identityHash builds a server with the given hooks and returns the protocol
// hash of the identity binding it hosts.
func identityHash(t *testing.T, cfg IdentityConfig) (string, []byte) {
	t.Helper()
	s := idServer(t, mustIdentity(t, cfg))
	b, ok := s.bindings()[IdentityProtocolName]
	if !ok {
		t.Fatalf("identity not hosted; have %v", sortedKeys(s.bindings()))
	}
	preimage, err := CanonicalDescription(IdentityProtocolName, hashMethodsOf(b.Methods))
	if err != nil {
		t.Fatal(err)
	}
	return b.Hash, preimage
}

// The cross-port contract. These digests are computed by the reference
// implementation and are not negotiable: a mismatch means this port and that one
// would disagree about whether they speak the same protocol, and the single
// most likely cause is the nullability of the scopes list ITEM.
//
// The two single-method digests are not decoration: they prove method-level
// narrowing actually narrows the hash rather than hosting a method that refuses.
func TestIdentityProtocolHashVectors(t *testing.T) {
	both := IdentityConfig{ResolveToken: idResolver, IntrospectPrincipals: []string{"proxy"}, MintGrant: idMinter}
	introspectOnly := IdentityConfig{ResolveToken: idResolver, IntrospectPrincipals: []string{"proxy"}}
	mintOnly := IdentityConfig{MintGrant: idMinter}

	cases := []struct {
		name string
		cfg  IdentityConfig
		want string
	}{
		{"both", both, "8317f2ad8e2476bb99e8b94800ab79b19a8cf0c6bdd6d66c2d82bd62ffbe69d5"},
		{"introspect_token only", introspectOnly, "27b75bef22e4c70baab92a5188a473506b89055d2cb2b58cc187f6fe7a436385"},
		{"issue_grant only", mintOnly, "c71b12f453310139b6b6a445378064661c52711d03ae1e4fba29b8f7976ef4d8"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, preimage := identityHash(t, tc.cfg)
			if got != tc.want {
				// A failure is a JSON diff, not a guess.
				t.Errorf("hash = %s, want %s\npreimage: %s", got, tc.want, preimage)
			}
		})
	}
}

// The canonical preimage, spelled out, so a digest mismatch is diagnosed by
// reading rather than by bisecting. `scopes` is list<item?:utf8> -- the item IS
// nullable, and a port that gets that wrong hosts a different protocol under the
// same name.
func TestIdentityCanonicalPreimage(t *testing.T) {
	_, preimage := identityHash(t, IdentityConfig{
		ResolveToken:         idResolver,
		IntrospectPrincipals: []string{"proxy"},
		MintGrant:            idMinter,
	})
	const want = `{"methods":[{"has_header":false,"has_return":true,"name":"introspect_token",` +
		`"params":[{"name":"token","nullable":false,"type":"utf8"}],` +
		`"result":[{"name":"result","nullable":false,"type":"binary"}],"type":"unary"},` +
		`{"has_header":false,"has_return":true,"name":"issue_grant",` +
		`"params":[{"name":"purpose","nullable":false,"type":"utf8"},` +
		`{"name":"scopes","nullable":false,"type":"list<item?:utf8>"},` +
		`{"name":"ttl_seconds","nullable":false,"type":"int64"}],` +
		`"result":[{"name":"result","nullable":false,"type":"binary"}],"type":"unary"}],` +
		`"protocol":"vgi_rpc.Identity.v1"}`
	if string(preimage) != want {
		t.Errorf("preimage =\n  %s\nwant\n  %s", preimage, want)
	}
}

// Narrowing the method set narrows the hash: a server offering half the methods
// is not offering the same surface, and a client comparing hashes must see that.
func TestIdentityNarrowingChangesTheHash(t *testing.T) {
	both, _ := identityHash(t, IdentityConfig{
		ResolveToken: idResolver, IntrospectPrincipals: []string{"proxy"}, MintGrant: idMinter,
	})
	one, _ := identityHash(t, IdentityConfig{MintGrant: idMinter})
	if both == one {
		t.Error("hosting one method must not hash the same as hosting two")
	}
}

// The primary conformance protocol's hash is verified across every port. It must
// not move because identity was added next to it.
func TestRegisteringIdentityLeavesThePrimaryHashAlone(t *testing.T) {
	plain := idServer(t, nil).canonicalHash()
	withIdentity := idServer(t, mustIdentity(t, IdentityConfig{MintGrant: idMinter})).canonicalHash()
	if plain != withIdentity {
		t.Errorf("primary hash moved: %s -> %s", plain, withIdentity)
	}
}

// ---------------------------------------------------------------------------
// Introspection is locked down
//
// The answer is an identity assertion the asker acts on with its own
// credentials. "Trust it as much as you trust the worker" is the wrong frame:
// the asker trusts it *more*, because it authorizes with credentials the worker
// does not hold.
// ---------------------------------------------------------------------------

// The happy path, for the reverse proxy the method exists for.
func TestIntrospectionResolvesForAnAllowlistedCaller(t *testing.T) {
	got, err := introspectingIdentity(t, IdentityConfig{}).
		IntrospectToken("good", idCtx(idAuth("proxy", true, noAuthTime)))
	if err != nil {
		t.Fatalf("introspect: %v", err)
	}
	if got.Principal != "bob" || got.TokenName != "ci-key" {
		t.Errorf("identity = %+v", got)
	}
	// The lifetime the resolver built with NewTokenIdentity reaches the caller
	// intact.
	if got.TTLSeconds != DefaultTokenTTLSeconds {
		t.Errorf("ttl = %d, want %d", got.TTLSeconds, DefaultTokenTTLSeconds)
	}
}

// A lifetime a resolver actually supplied is never overridden, and a supplied
// zero least of all.
//
// ttl_seconds is how long the asker may cache the answer, which for any path it
// serves without re-presenting the credential is an authorization window --
// and therefore the revocation lag. Normalising 0 up to 300 would silently turn
// a resolver saying "do not cache this" into five minutes of continued access
// after revocation. The framework has no business making that trade on a
// worker's behalf; the tag default of 300 is for an ABSENT column on the decode
// side, not a coercion applied to a value a hook set.
func TestIntrospectionHonoursAnExplicitZeroTTL(t *testing.T) {
	cases := []struct {
		name string
		ttl  int
	}{
		{"explicit zero means do not cache", 0},
		{"a short lifetime is not rounded up", 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resolver := func(string) (TokenIdentity, bool, error) {
				return TokenIdentity{Principal: "bob", TTLSeconds: tc.ttl}, true, nil
			}
			impl := mustIdentity(t, IdentityConfig{
				ResolveToken:         resolver,
				IntrospectPrincipals: []string{"proxy"},
			})
			got, err := impl.IntrospectToken("good", idCtx(idAuth("proxy", true, noAuthTime)))
			if err != nil {
				t.Fatalf("introspect: %v", err)
			}
			if got.TTLSeconds != tc.ttl {
				t.Errorf("ttl = %d, want %d -- the framework must not override a lifetime the resolver set",
					got.TTLSeconds, tc.ttl)
			}
		})
	}
}

// The constructor is the only place the documented default is supplied, and a
// struct literal deliberately bypasses it: forgetting a TTL then fails in the
// mild direction (more introspection traffic) rather than by extending an
// authorization window.
func TestNewTokenIdentitySuppliesTheDocumentedDefault(t *testing.T) {
	if got := NewTokenIdentity("bob"); got.TTLSeconds != DefaultTokenTTLSeconds {
		t.Errorf("NewTokenIdentity ttl = %d, want %d", got.TTLSeconds, DefaultTokenTTLSeconds)
	}
	if got := (TokenIdentity{Principal: "bob"}); got.TTLSeconds != 0 {
		t.Errorf("a struct literal must keep its zero value, got %d", got.TTLSeconds)
	}
}

// Authentication is not the same capability as introspection. A deployment
// where any valid credential may introspect lets any user test guesses of any
// other user's credential at unlimited rate, and resolve a stolen one to its
// owner.
func TestIntrospectionRefusesACallerOffTheAllowlist(t *testing.T) {
	for _, caller := range []string{"alice", ""} {
		t.Run("caller="+caller, func(t *testing.T) {
			_, err := introspectingIdentity(t, IdentityConfig{}).
				IntrospectToken("good", idCtx(idAuth(caller, true, noAuthTime)))
			assertIntrospectionRefused(t, err)
		})
	}
}

// Subprocess, unix and TCP transports carry no authenticated principal.
func TestIntrospectionRefusesAnUnauthenticatedCaller(t *testing.T) {
	_, err := introspectingIdentity(t, IdentityConfig{}).
		IntrospectToken("good", idCtx(idAuth("proxy", false, noAuthTime)))
	assertIntrospectionRefused(t, err)
}

// An absent CallContext is an unauthenticated one, not a nil dereference: a
// framework-owned guard that panics is a guard that can be turned into a denial
// of service.
func TestIntrospectionRefusesAnAbsentContext(t *testing.T) {
	if _, err := introspectingIdentity(t, IdentityConfig{}).IntrospectToken("good", nil); err == nil {
		t.Fatal("expected a refusal")
	} else {
		assertIntrospectionRefused(t, err)
	}
}

// An unauthorized caller learns nothing, including how long it took.
func TestIntrospectionRefusalPrecedesTheResolver(t *testing.T) {
	var seen []string
	spy := func(token string) (TokenIdentity, bool, error) {
		seen = append(seen, token)
		return TokenIdentity{}, false, nil
	}
	impl := mustIdentity(t, IdentityConfig{ResolveToken: spy, IntrospectPrincipals: []string{"proxy"}})
	_, err := impl.IntrospectToken("secret", idCtx(idAuth("mallory", true, noAuthTime)))
	assertIntrospectionRefused(t, err)
	if len(seen) != 0 {
		t.Error("the resolver must not see a credential from an unauthorized caller")
	}
}

// The guard ORDER is load-bearing, so it is pinned rather than left to reading.
//
// Authorization and rate limiting come before the length and JWS checks, which
// means an unauthorized caller presenting an over-long or JWS-shaped token still
// gets introspection_refused and never token_unresolved. Reordering for tidiness
// would turn the difference between the two answers into a side channel telling
// an unauthorized caller something about the subject credential.
func TestIntrospectionAuthorizationPrecedesEverySubjectCheck(t *testing.T) {
	impl := introspectingIdentity(t, IdentityConfig{})
	for _, token := range []string{"", "aaa.bbb.ccc", strings.Repeat("x", MaxTokenChars+1)} {
		_, err := impl.IntrospectToken(token, idCtx(idAuth("mallory", true, noAuthTime)))
		var refused *IntrospectionRefusedError
		if !errors.As(err, &refused) {
			t.Errorf("token %.12q: got %T (%v), want *IntrospectionRefusedError -- "+
				"a subject check ran before the authorization check", token, err, err)
		}
	}
}

// Unknown, malformed and over-long are one answer. Distinguishing them would
// confirm that a guessed credential exists.
func TestIntrospectionRejectionsAreUniform(t *testing.T) {
	impl := introspectingIdentity(t, IdentityConfig{})
	for _, token := range []string{"", "unknown", strings.Repeat("x", MaxTokenChars+1)} {
		_, err := impl.IntrospectToken(token, idCtx(idAuth("proxy", true, noAuthTime)))
		var unresolved *TokenUnresolvedError
		if !errors.As(err, &unresolved) {
			t.Fatalf("token %.12q: got %T (%v), want *TokenUnresolvedError", token, err, err)
		}
		if unresolved.Error() != "unresolved" {
			t.Errorf("token %.12q: message = %q, want the same %q every other rejection carries",
				token, unresolved.Error(), "unresolved")
		}
	}
}

// Routing a JWS onward hands a third party a token the asker may itself have
// rejected -- expired, wrong audience -- which turns this method into a
// laundering step. A JWS is validated locally against a key set instead.
func TestIntrospectionNeverRoutesAJWSToTheResolver(t *testing.T) {
	var seen []string
	spy := func(token string) (TokenIdentity, bool, error) {
		seen = append(seen, token)
		return TokenIdentity{Principal: "bob"}, true, nil
	}
	impl := mustIdentity(t, IdentityConfig{ResolveToken: spy, IntrospectPrincipals: []string{"proxy"}})
	_, err := impl.IntrospectToken("aaa.bbb.ccc", idCtx(idAuth("proxy", true, noAuthTime)))
	var unresolved *TokenUnresolvedError
	if !errors.As(err, &unresolved) {
		t.Fatalf("got %T (%v), want *TokenUnresolvedError", err, err)
	}
	if len(seen) != 0 {
		t.Errorf("a JWS reached the resolver: %v", seen)
	}
}

// A caller that negative-caches "unknown" must not cache this. Cache an outage
// and a worker restart takes the fleet down for the cache's lifetime; retry a
// rejection and the worker is hammered.
func TestIdentityUnavailableIsTransientNotDefinitive(t *testing.T) {
	down := func(string) (TokenIdentity, bool, error) {
		return TokenIdentity{}, false, NewIdentityUnavailable("store is down")
	}
	impl := mustIdentity(t, IdentityConfig{ResolveToken: down, IntrospectPrincipals: []string{"proxy"}})
	_, err := impl.IntrospectToken("good", idCtx(idAuth("proxy", true, noAuthTime)))

	var unavailable *IdentityUnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("got %T (%v), want *IdentityUnavailableError", err, err)
	}
	if unavailable.RetryAfterSeconds() <= 0 {
		t.Error("a transient failure must carry a retry hint")
	}

	// It must not be catchable as the definitive rejection...
	var unresolved *TokenUnresolvedError
	if errors.As(err, &unresolved) {
		t.Error("an outage must not be catchable as a definitive token_unresolved")
	}
	// ...nor as the ValueError RpcError that ChainAuthenticate advances on. A
	// sidecar outage read as "not my credential, try the next" emerges as a 401
	// from the end of the chain and restarts every session in the fleet over a
	// thirty-second blip.
	var rpcErr *RpcError
	if errors.As(err, &rpcErr) && rpcErr.Type == "ValueError" {
		t.Error("an outage must not present as the ValueError the authenticate chain falls through on")
	}
}

// Bounds, rather than closes, the oracle an allowlisted caller still has: a
// caller whose own credential leaks can still test guesses.
func TestIntrospectionIsRateLimited(t *testing.T) {
	impl := introspectingIdentity(t, IdentityConfig{IntrospectRateLimit: 2})
	ctx := idCtx(idAuth("proxy", true, noAuthTime))
	for i := range 2 {
		if got, err := impl.IntrospectToken("good", ctx); err != nil || got.Principal != "bob" {
			t.Fatalf("call %d: %+v, %v", i, got, err)
		}
	}
	_, err := impl.IntrospectToken("good", ctx)
	var refused *IntrospectionRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("got %T (%v), want *IntrospectionRefusedError", err, err)
	}
	if !strings.Contains(refused.Error(), "rate limit") {
		t.Errorf("message = %q, want it to name the rate limit", refused.Error())
	}
}

// There is no permissive default, so it cannot be reached by omission -- and it
// is refused at construction, so a worker that would refuse every introspection
// fails to start rather than serving traffic until someone tries.
func TestIntrospectionAllowlistIsMandatory(t *testing.T) {
	for _, principals := range [][]string{nil, {}, {""}} {
		_, err := NewIdentity(IdentityConfig{ResolveToken: idResolver, IntrospectPrincipals: principals})
		if err == nil {
			t.Fatalf("principals %v: expected a construction error", principals)
		}
		if !strings.Contains(err.Error(), "at least one principal") {
			t.Errorf("principals %v: message = %q", principals, err)
		}
	}
}

func assertIntrospectionRefused(t *testing.T, err error) {
	t.Helper()
	var refused *IntrospectionRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("got %T (%v), want *IntrospectionRefusedError", err, err)
	}
}

// ---------------------------------------------------------------------------
// Issuance is not an oracle
// ---------------------------------------------------------------------------

// The happy path: a present user minting their own standing grant.
func TestIssuanceMintsForTheCaller(t *testing.T) {
	impl := mustIdentity(t, IdentityConfig{MintGrant: idMinter})
	grant, err := impl.IssueGrant("reports", []string{"read"}, 3600,
		idCtx(idAuth("alice", true, float64(time.Now().Unix()))))
	if err != nil {
		t.Fatalf("issue_grant: %v", err)
	}
	if grant.Token != "grant-for-alice" {
		t.Errorf("token = %q", grant.Token)
	}
	if grant.ExpiresAt <= float64(time.Now().Unix()) {
		t.Errorf("expires_at = %v, want a future timestamp", grant.ExpiresAt)
	}
}

// Cross-subject minting is closed by construction, not by a check. A check is
// something one of several ports can forget; a missing parameter is not.
func TestIssuanceSubjectIsTheCallerAndIsNotAParameter(t *testing.T) {
	params := reflect.TypeOf(issueGrantParams{})
	for i := range params.NumField() {
		name := tagName(params.Field(i).Tag.Get("vgirpc"))
		if name == "subject" || name == "principal" {
			t.Errorf("issue_grant must not take a %q parameter", name)
		}
	}
	// And the same at the Go call boundary, so no wrapper can reintroduce one.
	method, ok := reflect.TypeOf(&IdentityImpl{}).MethodByName("IssueGrant")
	if !ok {
		t.Fatal("IdentityImpl.IssueGrant is missing")
	}
	// receiver, purpose, scopes, ttl_seconds, ctx.
	if got := method.Type.NumIn(); got != 5 {
		t.Errorf("IssueGrant takes %d arguments; a subject parameter may have been added", got)
	}
}

// Unlike introspection -- and the asymmetry is the whole design. Issuance is
// always about the caller themselves, so "any authenticated caller" is not an
// open oracle there.
func TestIssuanceNeedsNoAllowlist(t *testing.T) {
	impl, err := NewIdentity(IdentityConfig{MintGrant: idMinter})
	if err != nil {
		t.Fatalf("minting must not require an allowlist: %v", err)
	}
	if got := impl.OfferedMethods(); !reflect.DeepEqual(got, []string{"issue_grant"}) {
		t.Errorf("OfferedMethods() = %v", got)
	}
}

// ---------------------------------------------------------------------------
// Freshness: a credential with no verifiable auth_time cannot mint
// ---------------------------------------------------------------------------

func freshnessIdentity(t *testing.T) *IdentityImpl {
	t.Helper()
	return mustIdentity(t, IdentityConfig{MintGrant: idMinter, MaxAuthAge: 900 * time.Second})
}

func assertStaleAuth(t *testing.T, err error, wantSubstring string) {
	t.Helper()
	var stale *StaleAuthError
	if !errors.As(err, &stale) {
		t.Fatalf("got %T (%v), want *StaleAuthError", err, err)
	}
	if !strings.Contains(stale.Error(), wantSubstring) {
		t.Errorf("message = %q, want it to contain %q", stale.Error(), wantSubstring)
	}
}

// A static bearer proves a machine holds a secret, never that a human just
// logged in.
func TestFreshnessRefusesAbsentAuthTime(t *testing.T) {
	_, err := freshnessIdentity(t).IssueGrant("p", nil, 60, idCtx(idAuth("alice", true, noAuthTime)))
	assertStaleAuth(t, err, "no auth_time")
}

// Naming the reason leaks nothing here: it is always about the caller. A console
// that cannot tell "your login is too old" from "no" cannot know to re-prompt.
func TestFreshnessRefusesStaleAuthTimeActionably(t *testing.T) {
	_, err := freshnessIdentity(t).IssueGrant("p", nil, 60,
		idCtx(idAuth("alice", true, float64(time.Now().Unix())-5000)))
	assertStaleAuth(t, err, "re-authenticate")
}

// The ceiling is a ceiling, not an equality.
func TestFreshnessAcceptsARecentLogin(t *testing.T) {
	grant, err := freshnessIdentity(t).IssueGrant("p", nil, 60,
		idCtx(idAuth("alice", true, float64(time.Now().Unix())-10)))
	if err != nil {
		t.Fatalf("issue_grant: %v", err)
	}
	if grant.Token != "grant-for-alice" {
		t.Errorf("token = %q", grant.Token)
	}
}

// A claim map is filled by whichever decoder ran, so every numeric spelling of
// auth_time must work -- and anything that is not a number must be refused,
// because a claim that cannot be read is not a claim that can be trusted.
func TestFreshnessReadsEveryNumericSpellingOfAuthTime(t *testing.T) {
	now := time.Now()
	recent := float64(now.Unix()) - 10
	impl := freshnessIdentity(t)

	accepted := []any{recent, int64(recent), int(recent), json.Number(strconv.FormatFloat(recent, 'f', 0, 64)),
		strconv.FormatFloat(recent, 'f', 0, 64)}
	for _, raw := range accepted {
		auth := &AuthContext{Authenticated: true, Principal: "alice", Claims: map[string]any{"auth_time": raw}}
		if _, err := impl.IssueGrant("p", nil, 60, idCtx(auth)); err != nil {
			t.Errorf("auth_time %T(%v): %v", raw, raw, err)
		}
	}
	for _, raw := range []any{"not-a-number", []string{"1"}, true} {
		auth := &AuthContext{Authenticated: true, Principal: "alice", Claims: map[string]any{"auth_time": raw}}
		_, err := impl.IssueGrant("p", nil, 60, idCtx(auth))
		assertStaleAuth(t, err, "unusable auth_time")
	}
}

// The lineage cannot escape the identity provider. A grant is not an IdP-issued
// token, so it carries no auth_time, so presenting one here fails the freshness
// check. That single rule is what stops indefinite self-renewal.
func TestAGrantCannotMintAnotherGrant(t *testing.T) {
	grantBearer := idAuth("alice", true, noAuthTime) // no auth_time: this is what a grant looks like
	_, err := freshnessIdentity(t).IssueGrant("p", nil, 60, idCtx(grantBearer))
	assertStaleAuth(t, err, "auth_time")
}

// Subprocess, unix and TCP have no authenticated principal at all, so they fail
// closed for free.
func TestUnauthenticatedTransportFailsClosed(t *testing.T) {
	_, err := freshnessIdentity(t).IssueGrant("p", nil, 60, idCtx(idAuth("", false, noAuthTime)))
	assertStaleAuth(t, err, "not authenticated")
	// Including when no context reached the handler at all.
	_, err = freshnessIdentity(t).IssueGrant("p", nil, 60, nil)
	assertStaleAuth(t, err, "not authenticated")
}

// ---------------------------------------------------------------------------
// Absent hooks: the belt to registration's braces
// ---------------------------------------------------------------------------

// Refused rather than crashing, for a caller that reached it anyway.
func TestIntrospectionWithoutAResolver(t *testing.T) {
	impl := mustIdentity(t, IdentityConfig{MintGrant: idMinter})
	_, err := impl.IntrospectToken("good", idCtx(idAuth("proxy", true, noAuthTime)))
	var refused *IntrospectionRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("got %T (%v), want *IntrospectionRefusedError", err, err)
	}
	if !strings.Contains(refused.Error(), "does not resolve") {
		t.Errorf("message = %q", refused.Error())
	}
}

// Same, on the other side.
func TestIssuanceWithoutAMinter(t *testing.T) {
	impl := introspectingIdentity(t, IdentityConfig{})
	_, err := impl.IssueGrant("p", nil, 60, idCtx(idAuth("alice", true, float64(time.Now().Unix()))))
	var refused *GrantRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("got %T (%v), want *GrantRefusedError", err, err)
	}
	if !strings.Contains(refused.Error(), "does not mint") {
		t.Errorf("message = %q", refused.Error())
	}
}

// ---------------------------------------------------------------------------
// Diagnostics
// ---------------------------------------------------------------------------

// Stable enough to correlate one credential's failures across records; not the
// credential. It must never reach a log, a span, or an error message.
func TestTokenDigestIsNotTheToken(t *testing.T) {
	const secret = "secret"
	digest := TokenDigest(secret)
	if digest == secret {
		t.Error("the digest must not be the credential")
	}
	if again := TokenDigest(secret); again != digest {
		t.Errorf("the digest must be stable: %q then %q", digest, again)
	}
	if TokenDigest("other") == digest {
		t.Error("two credentials must not share a digest")
	}
	if len(digest) != 64 {
		t.Errorf("digest length = %d, want 64", len(digest))
	}
}

// The error_kind strings are the only definitive/transient signal a caller has.
//
// These were an HTTP route whose callers classified on the status code (404 vs
// 503). As protocol methods every handler exception surfaces the same way, so
// error_kind carries the whole distinction: a caller that negative-caches a
// transient failure locks out valid users, and one that retries a definitive
// rejection hammers the worker.
func TestIdentityErrorKindsAreStable(t *testing.T) {
	cases := []struct {
		err  errorKindCarrier
		want string
	}{
		{&IntrospectionRefusedError{}, "introspection_refused"},
		{&TokenUnresolvedError{}, "token_unresolved"},
		{&StaleAuthError{}, "stale_auth"},
		{&GrantRefusedError{}, "grant_refused"},
		{&IdentityUnavailableError{}, "identity_unavailable"},
	}
	for _, tc := range cases {
		if got := tc.err.ErrorKind(); got != tc.want {
			t.Errorf("%T.ErrorKind() = %q, want %q", tc.err, got, tc.want)
		}
	}
}

// The kind reaches the wire, which is the only reason it exists.
func TestIdentityErrorKindsReachTheWire(t *testing.T) {
	for _, err := range []error{
		&IntrospectionRefusedError{Detail: "caller is not an introspector"},
		&TokenUnresolvedError{Detail: "unresolved"},
		&StaleAuthError{Detail: "caller is not authenticated"},
		&GrantRefusedError{Detail: "this worker does not mint grants"},
		&IdentityUnavailableError{Detail: "store is down"},
	} {
		carrier, ok := err.(errorKindCarrier)
		if !ok {
			t.Fatalf("%T does not advertise an error kind", err)
		}
		extra := buildErrorExtra(err, false)
		wantType := err.(errorTypeCarrier).ErrorType()
		if !strings.Contains(extra, `"exception_type":"`+wantType+`"`) {
			t.Errorf("%T: log_extra = %s, want exception_type %q", err, extra, wantType)
		}
		if carrier.ErrorKind() == "" {
			t.Errorf("%T: empty error kind", err)
		}
	}
}

// The credential must never appear in what a guard says. A message carrying it
// would put it in every log line and span the refusal produces.
func TestIdentityRefusalsNeverEchoTheCredential(t *testing.T) {
	const secret = "super-secret-credential"
	impl := introspectingIdentity(t, IdentityConfig{})
	for _, auth := range []*AuthContext{idAuth("mallory", true, noAuthTime), idAuth("proxy", true, noAuthTime)} {
		_, err := impl.IntrospectToken(secret, idCtx(auth))
		if err == nil {
			t.Fatal("expected a refusal")
		}
		if strings.Contains(err.Error(), secret) {
			t.Errorf("the credential reached an error message: %v", err)
		}
	}
}

// ---------------------------------------------------------------------------
// The rate limiter
// ---------------------------------------------------------------------------

// Fixed-window, because the state is one integer per caller rather than a float
// that has to be aged.
func TestRateLimiterAdmitsUpToTheLimit(t *testing.T) {
	limiter := NewRateLimiter(3, time.Second)
	now := time.Unix(100, 0)
	var got []bool
	for range 4 {
		got = append(got, limiter.allowAt("a", now))
	}
	if !reflect.DeepEqual(got, []bool{true, true, true, false}) {
		t.Errorf("admissions = %v, want [true true true false]", got)
	}
}

// A new window resets the count.
func TestRateLimiterWindowRolls(t *testing.T) {
	limiter := NewRateLimiter(1, time.Second)
	if !limiter.allowAt("a", time.Unix(100, 0)) {
		t.Error("the first request in a window must be admitted")
	}
	if limiter.allowAt("a", time.Unix(100, 500_000_000)) {
		t.Error("a second request inside the window must be refused")
	}
	if !limiter.allowAt("a", time.Unix(101, 500_000_000)) {
		t.Error("the window must roll")
	}
}

// One caller exhausting its budget must not refuse another.
func TestRateLimiterCallersAreIndependent(t *testing.T) {
	limiter := NewRateLimiter(1, time.Second)
	now := time.Unix(100, 0)
	if !limiter.allowAt("a", now) || !limiter.allowAt("b", now) {
		t.Error("two callers each have their own budget")
	}
	if limiter.allowAt("a", now) {
		t.Error("a caller's own budget must still be enforced")
	}
}

// Whole-map reset rather than per-key ageing, so an attacker cycling keys cannot
// grow the map without bound between sweeps.
func TestRateLimiterCyclingKeysCannotGrowTheMap(t *testing.T) {
	limiter := NewRateLimiter(1, time.Second)
	now := time.Unix(100, 0)
	for i := range 1000 {
		limiter.allowAt("k"+strconv.Itoa(i), now)
	}
	limiter.allowAt("fresh", time.Unix(200, 0))
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	if len(limiter.counts) != 1 {
		t.Errorf("map holds %d keys after a window roll, want 1", len(limiter.counts))
	}
}

// Every transport dispatches from more than one goroutine, so a limiter that
// raced would admit more than its ceiling under exactly the load it exists to
// bound. Run with -race.
func TestRateLimiterIsGoroutineSafe(t *testing.T) {
	limiter := NewRateLimiter(1_000_000, time.Hour)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				limiter.Allow("k" + strconv.Itoa(i%16))
			}
		}()
	}
	wg.Wait()
}

// The payloads must actually cross the wire, not merely describe a schema.
//
// A result type whose Go shape the serializer cannot build is a failure that
// only appears on the first real call -- registration validates the schema, not
// the encoding -- so the round trip is pinned here.
func TestIdentityPayloadsRoundTripThroughTheResultColumn(t *testing.T) {
	cases := []struct {
		name  string
		value any
	}{
		{"TokenIdentity", TokenIdentity{Principal: "bob", TokenName: "ci-key", TTLSeconds: 300}},
		{"IssuedGrant", IssuedGrant{Token: "grant-for-alice", ExpiresAt: 1.7e9, GrantID: "g1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema, err := SchemaForResult(reflect.TypeOf(tc.value))
			if err != nil {
				t.Fatalf("result schema: %v", err)
			}
			if schema.NumFields() != 1 || schema.Field(0).Name != "result" ||
				schema.Field(0).Type.ID() != arrow.BINARY || schema.Field(0).Nullable {
				t.Fatalf("result column = %v, want a single non-null binary 'result'", schema)
			}
			batch, err := serializeResult(schema, tc.value)
			if err != nil {
				t.Fatalf("serialize: %v", err)
			}
			defer batch.Release()
			if batch.NumRows() != 1 {
				t.Errorf("rows = %d, want 1", batch.NumRows())
			}
		})
	}
}

// ---------------------------------------------------------------------------
// The JWS shape test survives translation
// ---------------------------------------------------------------------------

// Whitespace must not be a way to walk a JWS past the guard.
//
// This exists because the ports diverged here and the reference was the
// accident: Python's `$` matches before a single trailing newline, so
// "aaa.bbb.ccc\n" was refused there while Go's \A..\z and JavaScript's unflagged
// `$` matched strictly and routed it straight to the resolver -- the one outcome
// the guard exists to prevent. Python was not even self-consistent, refusing one
// trailing newline and admitting two. Testing the trimmed form is the rule that
// means the same thing in seven regex dialects.
func TestPaddingDoesNotSmuggleAJWSPastTheGuard(t *testing.T) {
	for _, token := range []string{
		"aaa.bbb.ccc",
		"aaa.bbb.ccc\n",
		"aaa.bbb.ccc\n\n",
		"  aaa.bbb.ccc  ",
		"\taaa.bbb.ccc\r\n",
	} {
		t.Run(strconv.Quote(token), func(t *testing.T) {
			var unresolved *TokenUnresolvedError
			if err := RejectJWSShaped(token); !errors.As(err, &unresolved) {
				t.Errorf("got %v, want *TokenUnresolvedError -- no amount of surrounding "+
					"whitespace may make a JWS resolvable", err)
			}
		})
	}
}

// Whitespace-only never reaches a resolver either: it is not a credential.
func TestABlankCredentialIsNotACredential(t *testing.T) {
	for _, token := range []string{"", "   ", "\n", "\t\r\n"} {
		t.Run(strconv.Quote(token), func(t *testing.T) {
			var unresolved *TokenUnresolvedError
			if err := RejectJWSShaped(token); !errors.As(err, &unresolved) {
				t.Errorf("got %v, want *TokenUnresolvedError", err)
			}
		})
	}
}

// Trimming tightens the JWS test; it must not start refusing ordinary tokens.
// Two segments and four segments are not a JWS, whatever they look like.
func TestAnOpaqueCredentialStillReachesTheResolver(t *testing.T) {
	for _, token := range []string{"opaque-token", "a.b.c.d", "two.segments", "sk_live_abc123"} {
		t.Run(token, func(t *testing.T) {
			if err := RejectJWSShaped(token); err != nil {
				t.Errorf("an opaque credential was refused: %v", err)
			}
		})
	}
}

// The length check stays on the original. Trimming must not talk a megabyte of
// padding down into the allowance -- what arrived is what a resolver would have
// to handle.
func TestTheLengthCheckRunsOnTheUntrimmedCredential(t *testing.T) {
	padded := strings.Repeat(" ", MaxTokenChars) + "opaque" + strings.Repeat(" ", MaxTokenChars)
	var unresolved *TokenUnresolvedError
	if err := RejectJWSShaped(padded); !errors.As(err, &unresolved) {
		t.Errorf("got %v, want *TokenUnresolvedError for a credential over the byte cap", err)
	}
}

// Trimming is for the shape test only -- never for what is resolved. Rewriting
// a credential before resolving it would make the worker answer about a string
// the caller never sent.
func TestTheResolverReceivesTheCredentialUnmodified(t *testing.T) {
	const padded = "  padded-opaque-token  "
	var seen []string
	recording := func(token string) (TokenIdentity, bool, error) {
		seen = append(seen, token)
		return NewTokenIdentity("p"), true, nil
	}
	impl := mustIdentity(t, IdentityConfig{
		ResolveToken:         recording,
		IntrospectPrincipals: []string{"proxy"},
	})
	if _, err := impl.IntrospectToken(padded, idCtx(idAuth("proxy", true, noAuthTime))); err != nil {
		t.Fatalf("introspect: %v", err)
	}
	if !reflect.DeepEqual(seen, []string{padded}) {
		t.Errorf("resolver saw %q, want the credential exactly as it arrived (%q)", seen, padded)
	}
}
