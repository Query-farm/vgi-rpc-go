// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"errors"
	"net/http"
	"strings"
	"time"
)

// Bearer authenticators that close the vgi_rpc.Identity.v1 loop
// (WIRE_PROTOCOL.md §16, "Accepting identity credentials"):
//
//   - [GrantAuthenticate] accepts the framework's own sealed grants;
//   - [ResolveTokenAuthenticate] asks the worker's resolve_token hook.
//
// [ComposeIdentityAuthenticate] puts them after the deployment's own
// authenticator in the normative order -- the deployment's (JWT, static...),
// then sealed grants (a cheap prefix check), then resolve_token -- and
// [HttpServer] does so automatically when its server hosts Identity.v1 with
// grant keys or a resolver.
//
// Routing is by prefix and strict in both directions. A token without the
// "vgig1." prefix never reaches the grant verifier. A token with it that does
// not verify is refused outright (401) and never reaches resolve_token: a
// forged or stale grant must not get a second chance from a resolver that may
// answer for anything.

// Auth domains of identity-authenticated requests.
const (
	// GrantAuthDomain is AuthContext.Domain for a sealed-grant bearer.
	GrantAuthDomain = "grant"
	// TokenAuthDomain is AuthContext.Domain for a resolve_token bearer.
	TokenAuthDomain = "token"
)

// notMine is the chain's "try the next authenticator" signal.
func notMine(detail string) error {
	return &RpcError{Type: "ValueError", Message: detail}
}

func bearerOf(r *http.Request) (string, error) {
	header := r.Header.Get("Authorization")
	if header == "" {
		return "", notMine("Missing Authorization header")
	}
	if !strings.HasPrefix(header, "Bearer ") {
		return "", notMine("Authorization header is not a Bearer credential")
	}
	return header[len("Bearer "):], nil
}

// GrantAuthenticate accepts sealed grants minted with keys.
//
// The AuthContext has Domain "grant", the grant's principal, and claims
// {grant_id, scopes, purpose} -- and no auth_time, so a grant-authenticated
// caller cannot issue_grant: grants never mint grants. A bearer without the
// "vgig1." prefix passes to the next authenticator; one with it that does not
// verify is an [*AuthFailure] (invalid_credential, or expired_credential for an
// authentic grant outside its lifetime), which stops the chain.
func GrantAuthenticate(keys *GrantKeys) AuthenticateFunc {
	return func(r *http.Request) (*AuthContext, error) {
		token, err := bearerOf(r)
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(token, GrantTokenPrefix) {
			return nil, notMine("not a sealed grant")
		}
		claims, err := VerifyGrantToken(keys, token, time.Now())
		if err != nil {
			reason := AuthReasonInvalidCredential
			var invalid *GrantInvalidError
			if errors.As(err, &invalid) && invalid.Expired {
				reason = AuthReasonExpiredCredential
			}
			return nil, NewAuthFailure(reason, "sealed grant rejected: "+err.Error())
		}
		scopes := make([]any, len(claims.Scopes))
		for i, s := range claims.Scopes {
			scopes[i] = s
		}
		return &AuthContext{
			Domain:        GrantAuthDomain,
			Authenticated: true,
			Principal:     claims.Principal,
			Claims: map[string]any{
				"grant_id": claims.GrantID,
				"scopes":   scopes,
				"purpose":  claims.Purpose,
			},
		}, nil
	}
}

// ResolveTokenAuthenticate accepts bearers the worker's resolve_token hook
// resolves: Domain "token", the resolved principal, claims {token_name}, and
// no auth_time.
//
// Unknown (ok=false) passes to the next authenticator, ending in 401 if
// nothing accepts. An outage -- [*AuthUnavailableError] or
// [*IdentityUnavailableError] from the hook -- is a 503 with the hook's
// Retry-After, never a 401. The hook never sees a "vgig1." token, a JWS-shaped
// token, a blank one, or one over [MaxTokenBytes]: the introspection shape
// guards apply.
func ResolveTokenAuthenticate(resolve TokenResolver) AuthenticateFunc {
	return func(r *http.Request) (*AuthContext, error) {
		token, err := bearerOf(r)
		if err != nil {
			return nil, err
		}
		if strings.HasPrefix(token, GrantTokenPrefix) {
			return nil, notMine("sealed grants are not resolved by resolve_token")
		}
		if RejectJWSShaped(token) != nil {
			return nil, notMine("bearer credential is not resolvable")
		}
		identity, ok, err := resolve(token)
		if err != nil {
			var authErr *AuthUnavailableError
			if errors.As(err, &authErr) {
				return nil, authErr
			}
			var idErr *IdentityUnavailableError
			if errors.As(err, &idErr) {
				detail := idErr.Detail
				if detail == "" {
					detail = "identity lookup unavailable"
				}
				return nil, &AuthUnavailableError{Detail: detail, RetryAfter: idErr.RetryAfterSeconds()}
			}
			return nil, err
		}
		if !ok {
			return nil, notMine("bearer credential did not resolve")
		}
		return &AuthContext{
			Domain:        TokenAuthDomain,
			Authenticated: true,
			Principal:     identity.Principal,
			Claims:        map[string]any{"token_name": identity.TokenName},
		}, nil
	}
}

// anonymousWithoutCredentials keeps an unauthenticated deployment
// unauthenticated for a request carrying no credential; one carrying a
// credential nothing accepted is a 401.
func anonymousWithoutCredentials(r *http.Request) (*AuthContext, error) {
	if r.Header.Get("Authorization") != "" {
		return nil, NewAuthFailure(AuthReasonInvalidCredential, "bearer credential not accepted")
	}
	return Anonymous(), nil
}

// ErrIdentityBearerBesideProxyGate is returned by [ComposeIdentityAuthenticate]
// when the deployment's authentication depends on proxy-injected evidence.
var ErrIdentityBearerBesideProxyGate = errors.New(
	"vgirpc: this server's authentication depends on proxy-injected evidence, and accepting sealed " +
		"grants or resolve_token bearers would be an OR beside it that bypasses that requirement. " +
		"Compose it yourself (gate, then ChainAuthenticate(inner, GrantAuthenticate(keys), " +
		"ResolveTokenAuthenticate(hook))) and call HttpServer.SetIdentityBearer(false)")

// ComposeIdentityAuthenticate appends the identity bearer authenticators
// after authenticate: authenticate, then sealed grants (when keys is
// non-nil), then resolve (when non-nil). With neither, authenticate is
// returned unchanged. With a nil authenticate, a request with no
// Authorization header stays anonymous and one whose bearer nothing accepts
// is a 401.
//
// authenticate must return a ValueError *RpcError for a credential it does
// not recognise; one that answers anonymous for everything ends the chain
// first. proxyDependent says authenticate relies on proxy-injected evidence
// (a proxy-proof gate, mTLS headers); OR-ing alternatives beside it would
// bypass that, so the composition is refused with
// [ErrIdentityBearerBesideProxyGate].
func ComposeIdentityAuthenticate(authenticate AuthenticateFunc, keys *GrantKeys, resolve TokenResolver, proxyDependent bool) (AuthenticateFunc, error) {
	var members []AuthenticateFunc
	if keys != nil {
		members = append(members, GrantAuthenticate(keys))
	}
	if resolve != nil {
		members = append(members, ResolveTokenAuthenticate(resolve))
	}
	if len(members) == 0 {
		return authenticate, nil
	}
	if authenticate == nil {
		return ChainAuthenticate(append(members, anonymousWithoutCredentials)...), nil
	}
	if proxyDependent {
		return nil, ErrIdentityBearerBesideProxyGate
	}
	return ChainAuthenticate(append([]AuthenticateFunc{authenticate}, members...)...), nil
}
