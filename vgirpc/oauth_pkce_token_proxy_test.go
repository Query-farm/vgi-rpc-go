// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// mockIdP returns a test server that serves a mock OIDC discovery document
// pointing at its own /token endpoint. The captured form values from the most
// recent /token POST are stored under the returned mutex.
type mockIdP struct {
	mu       sync.Mutex
	server   *httptest.Server
	captured url.Values
	// Configurable response from /token
	respStatus int
	respBody   []byte
}

func newMockIdP(t *testing.T) *mockIdP {
	t.Helper()
	idp := &mockIdP{
		respStatus: http.StatusOK,
		respBody:   []byte(`{"access_token":"new-access","token_type":"Bearer","expires_in":3600}`),
	}
	mux := http.NewServeMux()
	idp.server = httptest.NewServer(mux)
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		body := map[string]string{
			"issuer":                 idp.server.URL,
			"authorization_endpoint": idp.server.URL + "/authorize",
			"token_endpoint":         idp.server.URL + "/token",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		idp.mu.Lock()
		idp.captured = cloneForm(r.PostForm)
		status := idp.respStatus
		body := idp.respBody
		idp.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(body)
	})
	t.Cleanup(idp.server.Close)
	return idp
}

func cloneForm(in url.Values) url.Values {
	out := url.Values{}
	for k, v := range in {
		dup := make([]string, len(v))
		copy(dup, v)
		out[k] = dup
	}
	return out
}

// makeProxyTestServer wires an HttpServer with the token-proxy enabled and
// returns the test server plus the IdP mock.
func makeProxyTestServer(t *testing.T) (*httptest.Server, *mockIdP) {
	t.Helper()
	idp := newMockIdP(t)

	srv := NewServer()
	hs, err := NewHttpServerWithKey(srv, []byte("test-signing-key-32-bytes-long!!"))
	if err != nil {
		t.Fatalf("NewHttpServerWithKey: %v", err)
	}
	hs.SetPrefix("/vgi")
	hs.SetAuthenticate(func(r *http.Request) (*AuthContext, error) {
		// Always fails; PKCE wires cookie auth on top, but we don't need it
		// for token-proxy tests since the proxy route is exempt from auth.
		return nil, &RpcError{Type: "ValueError", Message: "unauth"}
	})
	if err := hs.SetOAuthResourceMetadata(&OAuthResourceMetadata{
		Resource:             "http://localhost:8000/vgi",
		AuthorizationServers: []string{idp.server.URL},
		ClientID:             "my-client-id",
		ClientSecret:         "my-client-secret",
	}); err != nil {
		t.Fatalf("SetOAuthResourceMetadata: %v", err)
	}
	if err := hs.SetOAuthPkce(OAuthPkceConfig{}); err != nil {
		t.Fatalf("SetOAuthPkce: %v", err)
	}
	hs.InitPages()

	ts := httptest.NewServer(hs)
	t.Cleanup(ts.Close)
	return ts, idp
}

func TestTokenProxy_WellKnownAdvertisesTokenEndpoint(t *testing.T) {
	ts, _ := makeProxyTestServer(t)
	resp, err := http.Get(ts.URL + "/.well-known/oauth-protected-resource/vgi")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if got := body["token_endpoint"]; got != "http://localhost:8000/vgi/_oauth/token" {
		t.Errorf("token_endpoint = %v", got)
	}
}

func TestTokenProxy_OptionsPreflight(t *testing.T) {
	ts, _ := makeProxyTestServer(t)
	req, _ := http.NewRequest(http.MethodOptions, ts.URL+"/vgi/_oauth/token", nil)
	req.Header.Set("Origin", "https://cupola.query-farm.services")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "https://cupola.query-farm.services" {
		t.Errorf("ACAO = %q", got)
	}
	if !strings.Contains(resp.Header.Get("Access-Control-Allow-Methods"), "POST") {
		t.Errorf("ACAM missing POST: %q", resp.Header.Get("Access-Control-Allow-Methods"))
	}
}

func TestTokenProxy_AuthorizationCodeForwardsWithInjectedSecret(t *testing.T) {
	ts, idp := makeProxyTestServer(t)
	body := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {"abc"},
		"code_verifier": {"v"},
		"redirect_uri":  {"https://x/cb"},
		"client_id":     {"my-client-id"},
	}
	resp, err := http.Post(ts.URL+"/vgi/_oauth/token",
		"application/x-www-form-urlencoded", strings.NewReader(body.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	idp.mu.Lock()
	defer idp.mu.Unlock()
	if got := idp.captured.Get("client_secret"); got != "my-client-secret" {
		t.Errorf("upstream client_secret = %q (proxy must inject it)", got)
	}
	if got := idp.captured.Get("client_id"); got != "my-client-id" {
		t.Errorf("upstream client_id = %q", got)
	}
	if got := idp.captured.Get("code"); got != "abc" {
		t.Errorf("upstream code = %q", got)
	}
	if got := idp.captured.Get("code_verifier"); got != "v" {
		t.Errorf("upstream code_verifier = %q", got)
	}
	if got := idp.captured.Get("redirect_uri"); got != "https://x/cb" {
		t.Errorf("upstream redirect_uri = %q", got)
	}
}

func TestTokenProxy_RefreshTokenForwardsWithInjectedSecret(t *testing.T) {
	ts, idp := makeProxyTestServer(t)
	body := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {"rtok"},
		"client_id":     {"my-client-id"},
		"scope":         {"openid"},
	}
	resp, err := http.Post(ts.URL+"/vgi/_oauth/token",
		"application/x-www-form-urlencoded", strings.NewReader(body.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	idp.mu.Lock()
	defer idp.mu.Unlock()
	if got := idp.captured.Get("grant_type"); got != "refresh_token" {
		t.Errorf("grant_type = %q", got)
	}
	if got := idp.captured.Get("refresh_token"); got != "rtok" {
		t.Errorf("refresh_token = %q", got)
	}
	if got := idp.captured.Get("scope"); got != "openid" {
		t.Errorf("scope = %q", got)
	}
	if got := idp.captured.Get("client_secret"); got != "my-client-secret" {
		t.Errorf("client_secret = %q (proxy must inject it)", got)
	}
}

func TestTokenProxy_MismatchedClientIDRejected(t *testing.T) {
	ts, _ := makeProxyTestServer(t)
	body := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {"abc"},
		"code_verifier": {"v"},
		"redirect_uri":  {"https://x/cb"},
		"client_id":     {"evil"},
	}
	resp, err := http.Post(ts.URL+"/vgi/_oauth/token",
		"application/x-www-form-urlencoded", strings.NewReader(body.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var bodyJSON map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&bodyJSON)
	if bodyJSON["error"] != "invalid_client" {
		t.Errorf("error = %q", bodyJSON["error"])
	}
}

func TestTokenProxy_UnsupportedGrantTypeRejected(t *testing.T) {
	ts, _ := makeProxyTestServer(t)
	body := url.Values{"grant_type": {"client_credentials"}}
	resp, err := http.Post(ts.URL+"/vgi/_oauth/token",
		"application/x-www-form-urlencoded", strings.NewReader(body.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var bodyJSON map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&bodyJSON)
	if bodyJSON["error"] != "unsupported_grant_type" {
		t.Errorf("error = %q", bodyJSON["error"])
	}
}

func TestTokenProxy_IdPErrorPassthrough(t *testing.T) {
	ts, idp := makeProxyTestServer(t)
	idp.mu.Lock()
	idp.respStatus = http.StatusBadRequest
	idp.respBody = []byte(`{"error":"invalid_grant","error_description":"bad code"}`)
	idp.mu.Unlock()

	body := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {"bad"},
		"code_verifier": {"v"},
		"redirect_uri":  {"https://x/cb"},
	}
	resp, err := http.Post(ts.URL+"/vgi/_oauth/token",
		"application/x-www-form-urlencoded", strings.NewReader(body.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(raw), "invalid_grant") {
		t.Errorf("body did not contain IdP error: %s", string(raw))
	}
}

func TestTokenProxy_WrongContentTypeRejected(t *testing.T) {
	ts, _ := makeProxyTestServer(t)
	resp, err := http.Post(ts.URL+"/vgi/_oauth/token",
		"application/json",
		strings.NewReader(`{"grant_type":"authorization_code"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}
