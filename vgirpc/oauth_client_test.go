// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParseResourceMetadataURL(t *testing.T) {
	// Valid header
	url := ParseResourceMetadataURL(`Bearer resource_metadata="https://api.example.com/.well-known/oauth-protected-resource/vgi"`)
	if url != "https://api.example.com/.well-known/oauth-protected-resource/vgi" {
		t.Errorf("unexpected URL: %s", url)
	}

	// Missing resource_metadata
	if url := ParseResourceMetadataURL("Bearer realm=\"example\""); url != "" {
		t.Errorf("expected empty, got %s", url)
	}

	// Empty header
	if url := ParseResourceMetadataURL(""); url != "" {
		t.Errorf("expected empty, got %s", url)
	}

	// Malformed (no closing quote)
	if url := ParseResourceMetadataURL(`Bearer resource_metadata="https://example.com`); url != "" {
		t.Errorf("expected empty for malformed, got %s", url)
	}
}

func TestFetchOAuthResourceMetadata(t *testing.T) {
	meta := &OAuthResourceMetadata{
		Resource:             "https://api.example.com/vgi",
		AuthorizationServers: []string{"https://auth.example.com"},
		ScopesSupported:      []string{"read"},
	}
	data, _ := json.Marshal(meta)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/.well-known/oauth-protected-resource/vgi" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	}))
	defer ts.Close()

	result, err := FetchOAuthResourceMetadata(ts.URL+"/vgi")
	if err != nil {
		t.Fatal(err)
	}
	if result.Resource != "https://api.example.com/vgi" {
		t.Errorf("unexpected resource: %s", result.Resource)
	}
	if len(result.ScopesSupported) != 1 || result.ScopesSupported[0] != "read" {
		t.Errorf("unexpected scopes: %v", result.ScopesSupported)
	}
}

func TestFetchOAuthResourceMetadataFromURL(t *testing.T) {
	meta := &OAuthResourceMetadata{
		Resource:             "https://api.example.com/vgi",
		AuthorizationServers: []string{"https://auth.example.com"},
		ResourceName:         "Test API",
	}
	data, _ := json.Marshal(meta)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	}))
	defer ts.Close()

	result, err := FetchOAuthResourceMetadataFromURL(ts.URL + "/metadata")
	if err != nil {
		t.Fatal(err)
	}
	if result.ResourceName != "Test API" {
		t.Errorf("unexpected name: %s", result.ResourceName)
	}
}

func TestFetchOAuthResourceMetadata_NotFound(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer ts.Close()

	_, err := FetchOAuthResourceMetadata(ts.URL + "/vgi")
	if err == nil {
		t.Fatal("expected error for 404")
	}
}
