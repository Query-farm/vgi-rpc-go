// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
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
// PKCE helpers (RFC 7636)
// ---------------------------------------------------------------------------

// generateCodeVerifier generates a URL-safe random code verifier (RFC 7636 S4.1).
func generateCodeVerifier() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("vgirpc: crypto/rand failure: %v", err))
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

// generateCodeChallenge computes S256 code challenge from a code verifier (RFC 7636 S4.2).
func generateCodeChallenge(verifier string) string {
	h := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(h[:])
}

// generateStateNonce generates a random state nonce for CSRF protection.
func generateStateNonce() string {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("vgirpc: crypto/rand failure: %v", err))
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

// ---------------------------------------------------------------------------
// Derived HMAC key
// ---------------------------------------------------------------------------

// deriveSessionKey derives a separate HMAC key for OAuth session cookies.
func deriveSessionKey(signingKey []byte) []byte {
	mac := hmac.New(sha256.New, signingKey)
	mac.Write([]byte("oauth-pkce-session"))
	return mac.Sum(nil)
}

// ---------------------------------------------------------------------------
// Signed session cookie (stores code_verifier + state + original URL + return_to)
// ---------------------------------------------------------------------------

// packOAuthCookie packs PKCE session data into a signed, base64-encoded cookie value.
//
// Wire format v4:
//
//	[1B version=4] [8B created_at uint64 LE]
//	[2B cv_len uint16 LE] [cv_len bytes code_verifier]
//	[2B state_len uint16 LE] [state_len bytes state_nonce]
//	[2B url_len uint16 LE] [url_len bytes original_url]
//	[2B rt_len uint16 LE] [rt_len bytes return_to]
//	[32B HMAC-SHA256(session_key, all above)]
func packOAuthCookie(verifier, state, originalURL, returnTo string, sessionKey []byte, createdAt int64) string {
	cvBytes := []byte(verifier)
	stateBytes := []byte(state)
	urlBytes := []byte(originalURL)
	rtBytes := []byte(returnTo)

	// Calculate total payload size: 1 + 8 + 4*(2) + len(fields)
	payloadLen := 1 + 8 + 2 + len(cvBytes) + 2 + len(stateBytes) + 2 + len(urlBytes) + 2 + len(rtBytes)
	payload := make([]byte, 0, payloadLen)

	// Version
	payload = append(payload, sessionCookieVersion)

	// Created at (uint64 LE)
	var ts [8]byte
	binary.LittleEndian.PutUint64(ts[:], uint64(createdAt))
	payload = append(payload, ts[:]...)

	// Code verifier
	var lenBuf [2]byte
	binary.LittleEndian.PutUint16(lenBuf[:], uint16(len(cvBytes)))
	payload = append(payload, lenBuf[:]...)
	payload = append(payload, cvBytes...)

	// State nonce
	binary.LittleEndian.PutUint16(lenBuf[:], uint16(len(stateBytes)))
	payload = append(payload, lenBuf[:]...)
	payload = append(payload, stateBytes...)

	// Original URL
	binary.LittleEndian.PutUint16(lenBuf[:], uint16(len(urlBytes)))
	payload = append(payload, lenBuf[:]...)
	payload = append(payload, urlBytes...)

	// Return to
	binary.LittleEndian.PutUint16(lenBuf[:], uint16(len(rtBytes)))
	payload = append(payload, lenBuf[:]...)
	payload = append(payload, rtBytes...)

	// HMAC
	mac := hmac.New(sha256.New, sessionKey)
	mac.Write(payload)
	sig := mac.Sum(nil)

	// base64 URL encoding WITH padding (to match Python's urlsafe_b64encode)
	return base64.URLEncoding.EncodeToString(append(payload, sig...))
}

// unpackOAuthCookie unpacks and verifies a signed OAuth session cookie.
// Returns (verifier, state, originalURL, returnTo, err).
func unpackOAuthCookie(cookieValue string, sessionKey []byte, maxAge int) (verifier, state, originalURL, returnTo string, err error) {
	raw, err := base64.URLEncoding.DecodeString(cookieValue)
	if err != nil {
		// Also try RawURL (no padding) for robustness
		raw, err = base64.RawURLEncoding.DecodeString(cookieValue)
		if err != nil {
			return "", "", "", "", fmt.Errorf("malformed session cookie")
		}
	}

	// Minimum: version(1) + timestamp(8) + 4*length(2) + HMAC(32) = 49
	if len(raw) < 49 {
		return "", "", "", "", fmt.Errorf("session cookie too short")
	}

	// Verify HMAC before inspecting payload
	payload := raw[:len(raw)-pkceHMACLen]
	receivedMAC := raw[len(raw)-pkceHMACLen:]
	mac := hmac.New(sha256.New, sessionKey)
	mac.Write(payload)
	expectedMAC := mac.Sum(nil)
	if subtle.ConstantTimeCompare(receivedMAC, expectedMAC) != 1 {
		return "", "", "", "", fmt.Errorf("session cookie signature mismatch")
	}

	// Parse payload
	version := payload[0]
	if version != sessionCookieVersion {
		return "", "", "", "", fmt.Errorf("unexpected session cookie version: %d", version)
	}

	createdAt := binary.LittleEndian.Uint64(payload[1:9])
	if maxAge > 0 {
		age := time.Now().Unix() - int64(createdAt)
		if age < 0 || age > int64(maxAge) {
			return "", "", "", "", fmt.Errorf("session cookie expired (age=%ds, max=%ds)", age, maxAge)
		}
	}

	pos := 9

	// Code verifier
	if pos+2 > len(payload) {
		return "", "", "", "", fmt.Errorf("malformed session cookie: truncated")
	}
	cvLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
	pos += 2
	if pos+cvLen > len(payload) {
		return "", "", "", "", fmt.Errorf("malformed session cookie: truncated")
	}
	verifier = string(payload[pos : pos+cvLen])
	pos += cvLen

	// State nonce
	if pos+2 > len(payload) {
		return "", "", "", "", fmt.Errorf("malformed session cookie: truncated")
	}
	stateLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
	pos += 2
	if pos+stateLen > len(payload) {
		return "", "", "", "", fmt.Errorf("malformed session cookie: truncated")
	}
	state = string(payload[pos : pos+stateLen])
	pos += stateLen

	// Original URL
	if pos+2 > len(payload) {
		return "", "", "", "", fmt.Errorf("malformed session cookie: truncated")
	}
	urlLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
	pos += 2
	if pos+urlLen > len(payload) {
		return "", "", "", "", fmt.Errorf("malformed session cookie: truncated")
	}
	originalURL = string(payload[pos : pos+urlLen])
	pos += urlLen

	// Return to
	if pos+2 > len(payload) {
		return "", "", "", "", fmt.Errorf("malformed session cookie: truncated")
	}
	rtLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
	pos += 2
	if pos+rtLen > len(payload) {
		return "", "", "", "", fmt.Errorf("malformed session cookie: truncated")
	}
	returnTo = string(payload[pos : pos+rtLen])

	return verifier, state, originalURL, returnTo, nil
}

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

// ---------------------------------------------------------------------------
// Error HTML page
// ---------------------------------------------------------------------------

var oauthErrorHTMLTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Authentication Error &mdash; vgi-rpc</title>
` + fontImports + `
` + errorPageStyle + `
</head>
<body>
<div class="logo">
  <img src="` + logoURL + `" alt="vgi-rpc logo">
</div>
<h1>Authentication Error</h1>
<p>%s</p>
%s
<p><a href="%s">Try again</a></p>
<footer>
  Powered by <a href="https://vgi-rpc.query.farm"><code>vgi-rpc</code></a>
</footer>
</body>
</html>`

// oauthErrorPage renders a user-friendly OAuth error page.
func oauthErrorPage(message, detail, retryURL string) []byte {
	detailHTML := ""
	if detail != "" {
		detailHTML = fmt.Sprintf(`<div class="detail">%s</div>`, html.EscapeString(detail))
	}
	return []byte(fmt.Sprintf(oauthErrorHTMLTemplate,
		html.EscapeString(message),
		detailHTML,
		html.EscapeString(retryURL),
	))
}

// ---------------------------------------------------------------------------
// User-info JS snippet for landing/describe pages
// ---------------------------------------------------------------------------

const userInfoStyle = `#vgi-user-info {
  position: fixed; top: 12px; right: 16px; z-index: 1000;
  font-family: 'Inter', system-ui, sans-serif; font-size: 0.85em;
  display: flex; align-items: center; gap: 8px;
  background: #fff; border: 1px solid #e0ddd0; border-radius: 20px;
  padding: 4px 14px 4px 6px; box-shadow: 0 2px 8px rgba(0,0,0,0.06);
}
#vgi-user-info img {
  width: 26px; height: 26px; border-radius: 50%;
}
#vgi-user-info .email { color: #2c2c1e; font-weight: 500; }
#vgi-user-info a {
  color: #6b6b5a; text-decoration: none; margin-left: 4px;
  font-size: 0.9em;
}
#vgi-user-info a:hover { color: #8b0000; }`

// userInfoScriptTemplate uses %%s placeholders that get filled by fmt.Sprintf.
// The JavaScript uses single-quoted strings and raw escapes.
const userInfoScriptTemplate = `(function() {
  var c = document.cookie.match('(^|;)\\s*%s=([^;]+)');
  if (!c) return;
  try {
    var parts = c[2].split('.');
    var payload = JSON.parse(atob(parts[1].replace(/-/g,'+').replace(/_/g,'/')));
    var el = document.getElementById('vgi-user-info');
    if (!el) return;
    var html = '';
    if (payload.picture) html += '<img src="' + payload.picture + '" alt="">';
    html += '<span class="email">' + (payload.email || payload.sub || '') + '</span>';
    html += '<a href="%s">Sign out</a>';
    el.innerHTML = html;
  } catch(e) {}
})();`

// buildUserInfoHTML returns the HTML snippet (style + div + script) for user info display.
func buildUserInfoHTML(prefix string) []byte {
	logoutURL := prefix + "/_oauth/logout"
	script := fmt.Sprintf(userInfoScriptTemplate, authCookieName, logoutURL)
	return []byte(fmt.Sprintf(
		"<style>%s</style>\n<div id=\"vgi-user-info\"></div>\n<script>%s</script>",
		userInfoStyle, script,
	))
}

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
// if the client disconnected mid-response.
func writePkcePage(w http.ResponseWriter, body []byte) {
	if _, err := w.Write(body); err != nil {
		slog.Debug("oauth: response write failed", "err", err)
	}
}

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
// fragment. Returns true if a redirect was issued.
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
