// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"crypto/subtle"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// HTTP handlers on HttpServer
// ---------------------------------------------------------------------------

// handleOAuthCallback handles GET {prefix}/_oauth/callback.
// Validates state, exchanges code for token, sets auth cookie, redirects.
func (h *HttpServer) handleOAuthCallback(w http.ResponseWriter, r *http.Request) {
	pkce := h.pkce
	retryURL := pkce.prefix
	if retryURL == "" {
		retryURL = "/"
	}

	// Check for authorization server error
	if errParam := r.URL.Query().Get("error"); errParam != "" {
		errorDesc := r.URL.Query().Get("error_description")
		if errorDesc == "" {
			errorDesc = errParam
		}
		slog.Warn("OAuth callback error", "error", errParam, "error_description", errorDesc)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		writePkcePage(w, oauthErrorPage(
			"The authorization server returned an error.",
			errorDesc,
			retryURL,
		))
		return
	}

	// Extract code and state from query
	code := r.URL.Query().Get("code")
	state := r.URL.Query().Get("state")
	if code == "" || state == "" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		writePkcePage(w, oauthErrorPage(
			"Missing authorization code or state parameter.",
			"",
			retryURL,
		))
		return
	}

	// Read and validate session cookie
	sessionCookie, err := r.Cookie(sessionCookieName)
	if err != nil || sessionCookie.Value == "" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		writePkcePage(w, oauthErrorPage(
			"Session cookie missing or expired. Please try again.",
			"",
			retryURL,
		))
		return
	}

	codeVerifier, expectedState, originalURL, returnTo, err := unpackOAuthCookie(
		sessionCookie.Value, pkce.sessionKey, sessionMaxAge,
	)
	if err != nil {
		slog.Warn("OAuth session cookie invalid", "error", err)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		writePkcePage(w, oauthErrorPage(
			"Session expired or invalid. Please try again.",
			"",
			retryURL,
		))
		return
	}

	// CSRF: validate state matches
	if subtle.ConstantTimeCompare([]byte(state), []byte(expectedState)) != 1 {
		slog.Warn("OAuth state mismatch")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		writePkcePage(w, oauthErrorPage(
			"State mismatch — possible CSRF. Please try again.",
			"",
			retryURL,
		))
		return
	}

	// Discover token endpoint
	_, tokenEndpoint, ok := pkce.oidcDiscovery()
	if !ok {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusBadGateway)
		writePkcePage(w, oauthErrorPage(
			"Could not reach the authorization server.",
			"OIDC discovery failed.",
			retryURL,
		))
		return
	}

	// Exchange code for token
	token, tokenMaxAge, refreshToken, err := exchangeCodeForToken(
		tokenEndpoint, code, pkce.redirectURI, codeVerifier,
		pkce.clientID, pkce.clientSecret, pkce.useIDToken,
	)
	if err != nil {
		slog.Warn("OAuth token exchange failed", "error", err)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusBadGateway)
		writePkcePage(w, oauthErrorPage(
			"Token exchange with the authorization server failed.",
			err.Error(),
			retryURL,
		))
		return
	}

	slog.Info("OAuth PKCE authentication successful")

	// Clear session cookie (path must match where it was set)
	clearSessionCookie := &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		MaxAge:   -1,
		Path:     pkce.prefix + "/_oauth/",
		Secure:   pkce.secureCookie,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	}

	// External frontend: redirect with token + OAuth metadata in URL fragment
	if returnTo != "" {
		separator := "#"
		if strings.Contains(returnTo, "#") {
			separator = "&"
		}
		fragmentParams := []string{"token=" + token}
		if refreshToken != "" {
			fragmentParams = append(fragmentParams, "refresh_token="+url.QueryEscape(refreshToken))
		}
		fragmentParams = append(fragmentParams, "token_endpoint="+url.QueryEscape(tokenEndpoint))
		fragmentParams = append(fragmentParams, "client_id="+url.QueryEscape(pkce.clientID))
		if pkce.clientSecret != "" {
			fragmentParams = append(fragmentParams, "client_secret="+url.QueryEscape(pkce.clientSecret))
		}
		if pkce.useIDToken {
			fragmentParams = append(fragmentParams, "use_id_token=true")
		}
		redirectURL := returnTo + separator + strings.Join(fragmentParams, "&")
		slog.Info("OAuth redirecting to external frontend", "return_to", strings.SplitN(returnTo, "?", 2)[0])

		http.SetCookie(w, clearSessionCookie)
		w.Header().Set("Location", redirectURL)
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.WriteHeader(http.StatusFound)
		return
	}

	// Same-origin: redirect to original page with cookies
	originalURL = validateOriginalURL(originalURL, pkce.prefix)

	cookiePath := pkce.prefix
	if cookiePath == "" {
		cookiePath = "/"
	}

	// Auth cookie (JS-readable for WASM — no HttpOnly)
	http.SetCookie(w, &http.Cookie{
		Name:     authCookieName,
		Value:    token,
		MaxAge:   tokenMaxAge,
		Path:     cookiePath,
		Secure:   pkce.secureCookie,
		HttpOnly: false,
		SameSite: http.SameSiteLaxMode,
	})
	// Clear session cookie
	http.SetCookie(w, clearSessionCookie)

	w.Header().Set("Location", originalURL)
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.WriteHeader(http.StatusFound)
}

// handleOAuthLogout handles GET {prefix}/_oauth/logout.
// Clears the auth cookie and redirects to the landing page.
// writePkcePage writes an OAuth HTML page body, logging (at debug level)

func (h *HttpServer) handleOAuthLogout(w http.ResponseWriter, r *http.Request) {
	pkce := h.pkce

	cookiePath := pkce.prefix
	if cookiePath == "" {
		cookiePath = "/"
	}

	http.SetCookie(w, &http.Cookie{
		Name:     authCookieName,
		Value:    "",
		MaxAge:   -1,
		Path:     cookiePath,
		Secure:   pkce.secureCookie,
		HttpOnly: false,
	})

	redirectTarget := pkce.prefix
	if redirectTarget == "" {
		redirectTarget = "/"
	}
	http.Redirect(w, r, redirectTarget, http.StatusFound)
}

// pkceRedirectToOAuth generates PKCE parameters, packs a session cookie,
// and redirects the user to the authorization endpoint.
func (h *HttpServer) pkceRedirectToOAuth(w http.ResponseWriter, r *http.Request) {
	pkce := h.pkce

	// Discover authorization endpoint
	authEndpoint, _, ok := pkce.oidcDiscovery()
	if !ok {
		slog.Warn("PKCE redirect skipped: OIDC discovery failed")
		return // Fall through to normal 401
	}

	// Generate PKCE parameters
	codeVerifier := generateCodeVerifier()
	codeChallenge := generateCodeChallenge(codeVerifier)
	stateNonce := generateStateNonce()

	// Capture original URL
	originalURL := r.URL.Path
	if r.URL.RawQuery != "" {
		originalURL = originalURL + "?" + r.URL.RawQuery
	}
	originalURL = validateOriginalURL(originalURL, pkce.prefix)

	// Check for external frontend return URL
	returnTo := validateReturnTo(r.URL.Query().Get("_vgi_return_to"), pkce.allowedReturnOrigins)

	// Pack session cookie
	cookieValue := packOAuthCookie(
		codeVerifier, stateNonce, originalURL, returnTo,
		pkce.sessionKey, time.Now().Unix(),
	)

	// Build authorization URL
	params := url.Values{
		"response_type":         {"code"},
		"client_id":             {pkce.clientID},
		"redirect_uri":          {pkce.redirectURI},
		"code_challenge":        {codeChallenge},
		"code_challenge_method": {"S256"},
		"state":                 {stateNonce},
		"scope":                 {pkce.scope},
	}
	// When redirecting to an external frontend, request offline access so
	// Google returns a refresh_token.
	if returnTo != "" {
		params.Set("access_type", "offline")
		params.Set("prompt", "consent")
	}
	authURL := authEndpoint + "?" + params.Encode()

	slog.Debug("OAuth PKCE redirect", "auth_endpoint", authEndpoint, "original_url", originalURL)

	// Set session cookie
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    cookieValue,
		MaxAge:   sessionMaxAge,
		Path:     pkce.prefix + "/_oauth/",
		Secure:   pkce.secureCookie,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})

	// Redirect to authorization endpoint
	w.Header().Set("Location", authURL)
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.WriteHeader(http.StatusFound)
}

// isJWTExpired decodes a JWT's exp claim and returns true if the token is expired.
// Returns false if the token is not a JWT or can't be decoded.

func (h *HttpServer) pkceEarlyReturnRedirect(w http.ResponseWriter, r *http.Request) bool {
	pkce := h.pkce

	returnTo := validateReturnTo(r.URL.Query().Get("_vgi_return_to"), pkce.allowedReturnOrigins)
	if returnTo == "" {
		return false
	}

	// Check for existing auth token in cookie
	tokenCookie, err := r.Cookie(authCookieName)
	if err != nil || tokenCookie.Value == "" {
		return false // Not authenticated — let normal flow handle it
	}

	// Don't redirect with an expired token — let the OAuth flow run again
	if isJWTExpired(tokenCookie.Value) {
		return false
	}

	// Already authenticated with a return_to — redirect back with the token
	separator := "#"
	if strings.Contains(returnTo, "#") {
		separator = "&"
	}
	fragmentParams := []string{"token=" + tokenCookie.Value}
	redirectURL := returnTo + separator + strings.Join(fragmentParams, "&")

	slog.Info("OAuth already authenticated, redirecting to external frontend",
		"return_to", strings.SplitN(returnTo, "?", 2)[0])

	w.Header().Set("Location", redirectURL)
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.WriteHeader(http.StatusFound)
	return true
}

// wrapPageWithPkce returns a handler that wraps a page handler with PKCE
// authentication. On browser GETs:
//  1. If already authenticated with _vgi_return_to, redirect immediately.
//  2. Try to authenticate. On success, serve the page.
//  3. On auth failure, redirect to OAuth login.
func (h *HttpServer) wrapPageWithPkce(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Check for early return redirect (already authenticated + _vgi_return_to)
		if h.pkceEarlyReturnRedirect(w, r) {
			return
		}

		// Try to authenticate
		if h.authenticateFunc != nil {
			auth, err := h.authenticateFunc(r)
			if err != nil {
				// Only redirect browsers (Accept: text/html)
				accept := r.Header.Get("Accept")
				if strings.Contains(accept, "text/html") {
					h.pkceRedirectToOAuth(w, r)
					return
				}
				// Non-browser: return 401
				if h.wwwAuthenticate != "" {
					w.Header().Set("WWW-Authenticate", h.wwwAuthenticate)
				}
				http.Error(w, "Authentication required", http.StatusUnauthorized)
				return
			}
			// Authenticated successfully — serve the page
			_ = auth
		}

		handler(w, r)
	}
}
