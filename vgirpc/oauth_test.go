// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestOAuthResourceMetadata_Validate(t *testing.T) {
	// Valid config
	m := &OAuthResourceMetadata{
		Resource:             "https://api.example.com/vgi",
		AuthorizationServers: []string{"https://auth.example.com"},
	}
	if err := m.Validate(); err != nil {
		t.Fatalf("expected valid, got %v", err)
	}

	// Empty Resource
	m2 := &OAuthResourceMetadata{
		AuthorizationServers: []string{"https://auth.example.com"},
	}
	if err := m2.Validate(); err == nil {
		t.Fatal("expected error for empty Resource")
	}

	// Empty AuthorizationServers
	m3 := &OAuthResourceMetadata{
		Resource: "https://api.example.com/vgi",
	}
	if err := m3.Validate(); err == nil {
		t.Fatal("expected error for empty AuthorizationServers")
	}
}

func TestOAuthResourceMetadata_ToJSON(t *testing.T) {
	m := &OAuthResourceMetadata{
		Resource:             "https://api.example.com/vgi",
		AuthorizationServers: []string{"https://auth.example.com"},
	}
	data, err := m.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatal(err)
	}
	if parsed["resource"] != "https://api.example.com/vgi" {
		t.Errorf("unexpected resource: %v", parsed["resource"])
	}
	// Optional fields should be omitted
	if _, ok := parsed["scopes_supported"]; ok {
		t.Error("scopes_supported should be omitted when empty")
	}
	if _, ok := parsed["resource_name"]; ok {
		t.Error("resource_name should be omitted when empty")
	}

	// With optional fields
	m2 := &OAuthResourceMetadata{
		Resource:             "https://api.example.com/vgi",
		AuthorizationServers: []string{"https://auth.example.com"},
		ScopesSupported:      []string{"read", "write"},
		ResourceName:         "My API",
	}
	data2, _ := m2.ToJSON()
	var parsed2 map[string]any
	json.Unmarshal(data2, &parsed2)
	if parsed2["resource_name"] != "My API" {
		t.Errorf("expected resource_name, got %v", parsed2["resource_name"])
	}
	scopes, ok := parsed2["scopes_supported"].([]any)
	if !ok || len(scopes) != 2 {
		t.Errorf("expected 2 scopes, got %v", parsed2["scopes_supported"])
	}
}

func newTestHttpServer(t *testing.T) *HttpServer {
	t.Helper()
	srv := NewServer()
	return NewHttpServer(srv)
}

func TestWellKnownEndpoint(t *testing.T) {
	h := newTestHttpServer(t)
	if err := h.SetOAuthResourceMetadata(&OAuthResourceMetadata{
		Resource:             "https://api.example.com/vgi",
		AuthorizationServers: []string{"https://auth.example.com"},
	}); err != nil {
		t.Fatal(err)
	}
	h.InitPages()

	req := httptest.NewRequest("GET", "/.well-known/oauth-protected-resource", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected application/json, got %s", ct)
	}
	if cc := w.Header().Get("Cache-Control"); cc != "public, max-age=60" {
		t.Errorf("expected Cache-Control header, got %s", cc)
	}

	var meta OAuthResourceMetadata
	if err := json.Unmarshal(w.Body.Bytes(), &meta); err != nil {
		t.Fatal(err)
	}
	if meta.Resource != "https://api.example.com/vgi" {
		t.Errorf("unexpected resource: %s", meta.Resource)
	}
}

func TestWellKnownEndpoint_NoMetadata(t *testing.T) {
	h := newTestHttpServer(t)
	h.InitPages()

	req := httptest.NewRequest("GET", "/.well-known/oauth-protected-resource", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestWellKnownEndpoint_BypassesAuth(t *testing.T) {
	h := newTestHttpServer(t)
	h.SetAuthenticate(func(r *http.Request) (*AuthContext, error) {
		return nil, &RpcError{Type: "ValueError", Message: "unauthorized"}
	})
	if err := h.SetOAuthResourceMetadata(&OAuthResourceMetadata{
		Resource:             "https://api.example.com/vgi",
		AuthorizationServers: []string{"https://auth.example.com"},
	}); err != nil {
		t.Fatal(err)
	}
	h.InitPages()

	req := httptest.NewRequest("GET", "/.well-known/oauth-protected-resource", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 (auth bypass), got %d", w.Code)
	}
}

func TestWWWAuthenticateHeader(t *testing.T) {
	h := newTestHttpServer(t)
	h.SetAuthenticate(func(r *http.Request) (*AuthContext, error) {
		return nil, &RpcError{Type: "ValueError", Message: "unauthorized"}
	})
	if err := h.SetOAuthResourceMetadata(&OAuthResourceMetadata{
		Resource:             "https://api.example.com/vgi",
		AuthorizationServers: []string{"https://auth.example.com"},
	}); err != nil {
		t.Fatal(err)
	}
	h.InitPages()

	req := httptest.NewRequest("POST", "/test_method", nil)
	req.Header.Set("Content-Type", "application/vnd.apache.arrow.stream")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
	wwwAuth := w.Header().Get("WWW-Authenticate")
	if wwwAuth == "" {
		t.Fatal("expected WWW-Authenticate header")
	}
	expected := `Bearer resource_metadata="https://api.example.com/.well-known/oauth-protected-resource/vgi"`
	if wwwAuth != expected {
		t.Errorf("expected %q, got %q", expected, wwwAuth)
	}
}

func TestWWWAuthenticateHeader_NoMetadata(t *testing.T) {
	h := newTestHttpServer(t)
	h.SetAuthenticate(func(r *http.Request) (*AuthContext, error) {
		return nil, &RpcError{Type: "ValueError", Message: "unauthorized"}
	})
	h.InitPages()

	req := httptest.NewRequest("POST", "/test_method", nil)
	req.Header.Set("Content-Type", "application/vnd.apache.arrow.stream")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
	if wwwAuth := w.Header().Get("WWW-Authenticate"); wwwAuth != "" {
		t.Errorf("expected no WWW-Authenticate header, got %q", wwwAuth)
	}
}
