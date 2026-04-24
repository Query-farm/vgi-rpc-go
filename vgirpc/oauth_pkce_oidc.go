// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// OIDC discovery cache
// ---------------------------------------------------------------------------

// createOIDCDiscovery creates a thread-safe callable that lazily caches OIDC discovery.
// Returns a function that returns (authEndpoint, tokenEndpoint, ok).
func createOIDCDiscovery(issuer string) func() (string, string, bool) {
	var (
		once          sync.Once
		authEndpoint  string
		tokenEndpoint string
		ok            bool
	)
	return func() (string, string, bool) {
		once.Do(func() {
			discoveryURL := strings.TrimRight(issuer, "/") + "/.well-known/openid-configuration"
			client := &http.Client{Timeout: 10 * time.Second}
			resp, err := client.Get(discoveryURL)
			if err != nil {
				slog.Warn("OIDC discovery failed", "issuer", issuer, "error", err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				slog.Warn("OIDC discovery returned non-200", "issuer", issuer, "status", resp.StatusCode)
				return
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				slog.Warn("OIDC discovery read error", "issuer", issuer, "error", err)
				return
			}
			var data struct {
				AuthorizationEndpoint string `json:"authorization_endpoint"`
				TokenEndpoint         string `json:"token_endpoint"`
			}
			if err := json.Unmarshal(body, &data); err != nil {
				slog.Warn("OIDC discovery JSON parse error", "issuer", issuer, "error", err)
				return
			}
			authEndpoint = data.AuthorizationEndpoint
			tokenEndpoint = data.TokenEndpoint
			ok = true
			slog.Debug("OIDC discovery complete", "issuer", issuer, "auth_endpoint", authEndpoint, "token_endpoint", tokenEndpoint)
		})
		return authEndpoint, tokenEndpoint, ok
	}
}

// ---------------------------------------------------------------------------
// Token exchange
// ---------------------------------------------------------------------------

// exchangeCodeForToken exchanges an authorization code for a token.
// Returns (token, maxAge, refreshToken, err).
func exchangeCodeForToken(tokenEndpoint, code, redirectURI, codeVerifier, clientID, clientSecret string, useIDToken bool) (token string, maxAge int, refreshToken string, err error) {
	formData := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {redirectURI},
		"code_verifier": {codeVerifier},
		"client_id":     {clientID},
	}
	if clientSecret != "" {
		formData.Set("client_secret", clientSecret)
	}

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.PostForm(tokenEndpoint, formData)
	if err != nil {
		return "", 0, "", fmt.Errorf("token exchange failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, "", fmt.Errorf("token exchange failed: reading response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", 0, "", fmt.Errorf("token exchange failed: HTTP %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp struct {
		AccessToken  string `json:"access_token"`
		IDToken      string `json:"id_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    *int   `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", 0, "", fmt.Errorf("token exchange failed: parsing response: %w", err)
	}

	refreshToken = tokenResp.RefreshToken

	if useIDToken {
		token = tokenResp.IDToken
		if token == "" {
			return "", 0, "", fmt.Errorf("token response missing id_token")
		}
		// Derive max_age from the id_token's exp claim
		maxAge = deriveIDTokenMaxAge(token)
		return token, maxAge, refreshToken, nil
	}

	token = tokenResp.AccessToken
	if token == "" {
		return "", 0, "", fmt.Errorf("token response missing access_token")
	}
	if tokenResp.ExpiresIn != nil {
		maxAge = *tokenResp.ExpiresIn
	} else {
		maxAge = authCookieDefaultMaxAge
	}
	return token, maxAge, refreshToken, nil
}

// deriveIDTokenMaxAge extracts the exp claim from a JWT id_token and returns
// the remaining lifetime in seconds, with a minimum of 60 seconds.
// Falls back to authCookieDefaultMaxAge on any error.
func deriveIDTokenMaxAge(token string) int {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return authCookieDefaultMaxAge
	}
	// Decode JWT payload (add padding if needed)
	payload := parts[1]
	switch len(payload) % 4 {
	case 2:
		payload += "=="
	case 3:
		payload += "="
	}
	decoded, err := base64.URLEncoding.DecodeString(payload)
	if err != nil {
		// Try without padding
		decoded, err = base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			return authCookieDefaultMaxAge
		}
	}
	var claims struct {
		Exp *float64 `json:"exp"`
	}
	if err := json.Unmarshal(decoded, &claims); err != nil || claims.Exp == nil {
		return authCookieDefaultMaxAge
	}
	remaining := int(*claims.Exp) - int(time.Now().Unix())
	if remaining < 60 {
		remaining = 60
	}
	return remaining
}

// ---------------------------------------------------------------------------
// Original URL validation
// ---------------------------------------------------------------------------

// validateOriginalURL validates that the original URL is relative and within the expected prefix.
func validateOriginalURL(u, prefix string) string {
	if len(u) > maxOriginalURLLen {
		u = u[:maxOriginalURLLen]
	}
	parsed, err := url.Parse(u)
	if err != nil {
		if prefix != "" {
			return prefix
		}
		return "/"
	}
	if parsed.Scheme != "" || parsed.Host != "" {
		// Not a relative URL — fall back to the prefix root
		if prefix != "" {
			return prefix
		}
		return "/"
	}
	if prefix != "" && !strings.HasPrefix(u, prefix) {
		if prefix != "" {
			return prefix
		}
		return "/"
	}
	return u
}

// isLocalhost checks if a hostname is localhost.
func isLocalhost(hostname string) bool {
	return hostname == "localhost" || hostname == "127.0.0.1" || hostname == "[::1]"
}

// validateReturnTo validates an external return-to URL against an origin allowlist.
// Returns the URL if it matches an allowed origin or is localhost, otherwise returns empty string.
func validateReturnTo(u string, allowedOrigins map[string]bool) string {
	if u == "" || len(u) > 2048 {
		return ""
	}
	parsed, err := url.Parse(u)
	if err != nil {
		return ""
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return ""
	}
	if parsed.Host == "" {
		return ""
	}
	// localhost with any port is always allowed
	hostname := parsed.Hostname()
	if isLocalhost(hostname) && parsed.Scheme == "http" {
		return u
	}
	// Check against allowlist (scheme + host, ignoring path)
	origin := fmt.Sprintf("%s://%s", parsed.Scheme, parsed.Hostname())
	if allowedOrigins[origin] {
		return u
	}
	// Also try with explicit port
	if parsed.Port() != "" {
		originWithPort := fmt.Sprintf("%s://%s:%s", parsed.Scheme, parsed.Hostname(), parsed.Port())
		if allowedOrigins[originWithPort] {
			return u
		}
	}
	return ""
}

func isJWTExpired(token string) bool {
	parts := strings.SplitN(token, ".", 3)
	if len(parts) < 2 {
		return false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}
	var claims struct {
		Exp *float64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Exp == nil {
		return false
	}
	return time.Now().Unix() >= int64(*claims.Exp)
}

// pkceEarlyReturnRedirect checks if the user is already authenticated and has
// a _vgi_return_to parameter. If so, redirects with the token in the URL
