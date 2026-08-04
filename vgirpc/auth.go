// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import "net/http"

// AuthContext carries authentication information for the current request.
// It is available to method handlers via [CallContext.Auth] and to dispatch
// hooks via [DispatchInfo.Auth]. It is never nil — unauthenticated requests
// receive [Anonymous].
type AuthContext struct {
	// Domain is the authentication scheme (e.g. "bearer", "jwt").
	// Empty when unauthenticated.
	Domain string
	// Authenticated is true when the request was successfully authenticated.
	Authenticated bool
	// Principal is the caller identity (username, service account, token subject).
	Principal string
	// Claims holds arbitrary claims extracted from the auth token.
	Claims map[string]any
}

// anonymous is the package-level singleton returned by Anonymous().
var anonymous = &AuthContext{}

// Anonymous returns a shared AuthContext representing an unauthenticated caller.
func Anonymous() *AuthContext {
	return anonymous
}

// RequireAuthenticated returns nil if the caller is authenticated, or a
// PermissionError [RpcError] otherwise.
func (a *AuthContext) RequireAuthenticated() error {
	if a.Authenticated {
		return nil
	}
	return &RpcError{Type: "PermissionError", Message: "Authentication required"}
}

// AuthenticateFunc extracts an [AuthContext] from an HTTP request.
// Return a non-nil error to reject the request:
//   - *[RpcError] with Type "ValueError" or "PermissionError" → HTTP 401
//   - *[AuthUnavailableError] → HTTP 503 + Retry-After
//   - any other error → HTTP 500
type AuthenticateFunc func(r *http.Request) (*AuthContext, error)

// defaultAuthRetryAfterSeconds is the Retry-After hint on a 503 from an
// authenticator that could not answer. Short on purpose: it is a hint to
// retry, not a backoff schedule.
const defaultAuthRetryAfterSeconds = 5

// AuthUnavailableError reports that an authenticator could not determine
// whether a credential is valid — a timeout, a transport failure, a 5xx from
// a remote authority. It is not a rejection.
//
// "The credential is bad" and "I could not find out whether the credential is
// bad" are different answers, and collapsing them is expensive in both
// directions. An outage that surfaces as 401 makes every caller
// re-authenticate at once against a service that is merely down — the DuckDB
// extension treats a second 401 after a refresh as fatal, so a thirty-second
// blip becomes a fleet-wide re-login storm. And a caller that negative-caches
// rejections will cache the outage.
//
// Deliberately not an *[RpcError] of type "ValueError", which is the whole
// point: [ChainAuthenticate] advances to the next authenticator on that type,
// so an outage reported as one reads as "this credential isn't mine, try the
// next" and emerges as a 401 from the end of the chain. Returned as this type
// it propagates, and the HTTP server renders 503 with Retry-After.
//
// Return it for transport failures, timeouts and 5xx from a remote authority.
// Do not return it for a credential the authority answered about.
type AuthUnavailableError struct {
	// Detail is operator-facing text. It must never carry the credential.
	Detail string
	// RetryAfter is the advertised Retry-After, in seconds. Zero means the
	// package default.
	RetryAfter int
}

// NewAuthUnavailable builds an [AuthUnavailableError] with the default
// Retry-After. Convenience for the common
// `return nil, NewAuthUnavailable("...")` shape.
func NewAuthUnavailable(detail string) *AuthUnavailableError {
	return &AuthUnavailableError{Detail: detail}
}

func (e *AuthUnavailableError) Error() string {
	if e.Detail == "" {
		return "authentication service unavailable"
	}
	return e.Detail
}

// retryAfterSeconds returns the advertised Retry-After, defaulted.
func (e *AuthUnavailableError) retryAfterSeconds() int {
	if e.RetryAfter > 0 {
		return e.RetryAfter
	}
	return defaultAuthRetryAfterSeconds
}
