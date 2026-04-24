// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"net/http"
	"strings"
)

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const (
	sessionCookieName       = "_vgi_oauth_session"
	authCookieName          = "_vgi_auth"
	sessionCookieVersion    = 4
	sessionMaxAge           = 600  // 10 minutes
	authCookieDefaultMaxAge = 3600 // 1 hour
	maxOriginalURLLen       = 2048
	pkceHMACLen             = 32
)

// defaultAllowedReturnOrigin is the default origin allowed for _vgi_return_to redirects.
const defaultAllowedReturnOrigin = "https://cupola.query-farm.services"

// ---------------------------------------------------------------------------
// OAuthPkceConfig
// ---------------------------------------------------------------------------

// OAuthPkceConfig configures the browser-based OAuth PKCE login flow.
type OAuthPkceConfig struct {
	// Scope is the OAuth scope to request. Defaults to "openid email".
	Scope string

	// AllowedReturnOrigins lists additional origins allowed for _vgi_return_to
	// redirects. The default origin (cupola.query-farm.services) is always included.
	AllowedReturnOrigins []string
}

// pkceScopeFromMetadata derives the OAuth scope string used by the PKCE
// middleware. It prefers the space-joined scopes_supported advertised in the
// resource metadata so that authorization requests match what the server
// publishes, falls back to the caller-provided configScope, and finally to
// "openid email" when neither is set.
func pkceScopeFromMetadata(meta *OAuthResourceMetadata, configScope string) string {
	if meta != nil && len(meta.ScopesSupported) > 0 {
		return strings.Join(meta.ScopesSupported, " ")
	}
	if configScope != "" {
		return configScope
	}
	return "openid email"
}

// oauthPkceState holds internal runtime state for the OAuth PKCE flow.
type oauthPkceState struct {
	sessionKey           []byte
	oidcDiscovery        func() (authEndpoint, tokenEndpoint string, ok bool)
	clientID             string
	clientSecret         string
	useIDToken           bool
	secureCookie         bool
	redirectURI          string
	prefix               string
	scope                string
	allowedReturnOrigins map[string]bool
	userInfoHTML         []byte
}

// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// CookieAuthenticate
// ---------------------------------------------------------------------------

// CookieAuthenticate returns an AuthenticateFunc that reads a bearer token
// from the named cookie and delegates validation to the inner authenticator
// by cloning the request with an Authorization: Bearer header.
//
// Intended for use with ChainAuthenticate:
//
//	auth := ChainAuthenticate(
//	    origAuth,                                  // tries Authorization header
//	    CookieAuthenticate(origAuth, "_vgi_auth"), // falls back to cookie
//	)
func CookieAuthenticate(inner AuthenticateFunc, cookieName string) AuthenticateFunc {
	return func(r *http.Request) (*AuthContext, error) {
		cookie, err := r.Cookie(cookieName)
		if err != nil || cookie.Value == "" {
			return nil, &RpcError{
				Type:    "ValueError",
				Message: "No auth cookie",
			}
		}
		// Clone the request and inject the cookie token as an Authorization header
		r2 := r.Clone(r.Context())
		r2.Header.Set("Authorization", "Bearer "+cookie.Value)
		return inner(r2)
	}
}
