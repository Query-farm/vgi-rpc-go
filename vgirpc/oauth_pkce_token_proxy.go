// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// allowedTokenGrantTypes lists the OAuth grant types the token proxy will
// forward. Other grants (e.g. client_credentials) would let an SPA caller
// obtain confidential-client tokens through the proxy and are rejected.
var allowedTokenGrantTypes = map[string]bool{
	"authorization_code": true,
	"refresh_token":      true,
}

// handleOAuthTokenProxy handles POST {prefix}/_oauth/token.
//
// SPA PKCE clients cannot safely hold a client_secret, but some IdPs
// (notably Google) reject PKCE token-endpoint requests from "Web
// application" clients without one. This handler accepts PKCE token-exchange
// and refresh-token requests from a browser, injects the configured
// server-side client_secret, and forwards the request to the IdP's real
// token_endpoint. The IdP's response is returned verbatim (status code +
// JSON body).
func (h *HttpServer) handleOAuthTokenProxy(w http.ResponseWriter, r *http.Request) {
	pkce := h.pkce
	tokenProxySetCORS(w, r, pkce)

	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Max-Age", "7200")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	ctype := strings.ToLower(strings.TrimSpace(strings.SplitN(r.Header.Get("Content-Type"), ";", 2)[0]))
	if ctype != "application/x-www-form-urlencoded" {
		writeTokenProxyError(w, http.StatusUnsupportedMediaType,
			"invalid_request", "Content-Type must be application/x-www-form-urlencoded")
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64*1024))
	if err != nil {
		writeTokenProxyError(w, http.StatusBadRequest, "invalid_request", "Could not read request body")
		return
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		writeTokenProxyError(w, http.StatusBadRequest, "invalid_request", "Could not parse form body")
		return
	}

	grantType := form.Get("grant_type")
	if !allowedTokenGrantTypes[grantType] {
		writeTokenProxyError(w, http.StatusBadRequest,
			"unsupported_grant_type",
			"grant_type must be authorization_code or refresh_token")
		return
	}

	if submitted := form.Get("client_id"); submitted != "" && submitted != pkce.clientID {
		writeTokenProxyError(w, http.StatusBadRequest,
			"invalid_client", "client_id does not match the configured client")
		return
	}

	_, tokenEndpoint, ok := pkce.oidcDiscovery()
	if !ok {
		writeTokenProxyError(w, http.StatusBadGateway,
			"server_error", "Authorization server discovery failed")
		return
	}

	upstream := url.Values{}
	upstream.Set("grant_type", grantType)
	upstream.Set("client_id", pkce.clientID)
	if pkce.clientSecret != "" {
		upstream.Set("client_secret", pkce.clientSecret)
	}
	for _, key := range []string{"code", "code_verifier", "redirect_uri", "refresh_token", "scope"} {
		if v := form.Get(key); v != "" {
			upstream.Set(key, v)
		}
	}

	httpClient := &http.Client{Timeout: 15 * time.Second}
	resp, err := httpClient.PostForm(tokenEndpoint, upstream)
	if err != nil {
		slog.Warn("OAuth token proxy: upstream request failed", "error", err)
		writeTokenProxyError(w, http.StatusBadGateway,
			"server_error", "Upstream token endpoint failed: "+err.Error())
		return
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		writeTokenProxyError(w, http.StatusBadGateway,
			"server_error", "Reading upstream response failed: "+err.Error())
		return
	}

	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	} else {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(resp.StatusCode)
	if _, err := w.Write(respBody); err != nil {
		slog.Debug("OAuth token proxy: write failed", "error", err)
	}
}

// tokenProxySetCORS sets Access-Control-Allow-Origin when the request's Origin
// is in the allowlist (or is localhost). Always sets Vary: Origin.
func tokenProxySetCORS(w http.ResponseWriter, r *http.Request, pkce *oauthPkceState) {
	w.Header().Add("Vary", "Origin")
	origin := r.Header.Get("Origin")
	if origin == "" {
		return
	}
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Host == "" {
		return
	}
	if isLocalhost(parsed.Hostname()) && parsed.Scheme == "http" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		return
	}
	if pkce.allowedReturnOrigins[origin] {
		w.Header().Set("Access-Control-Allow-Origin", origin)
	}
}

// writeTokenProxyError writes a JSON error response in the OAuth 2.0 error
// format ({"error": ..., "error_description": ...}).
func writeTokenProxyError(w http.ResponseWriter, status int, errCode, description string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	body, _ := json.Marshal(map[string]string{
		"error":             errCode,
		"error_description": description,
	})
	_, _ = w.Write(body)
}
