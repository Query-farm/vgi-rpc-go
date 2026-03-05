// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// FetchOAuthResourceMetadata fetches OAuth Protected Resource Metadata from the
// well-known URL derived from baseURL per RFC 9728. An optional http.Client may
// be provided; if nil, http.DefaultClient is used.
func FetchOAuthResourceMetadata(baseURL string, client ...*http.Client) (*OAuthResourceMetadata, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parsing base URL: %w", err)
	}
	path := strings.TrimSuffix(u.Path, "/")
	u.Path = wellKnownURL(path)
	return FetchOAuthResourceMetadataFromURL(u.String(), client...)
}

// FetchOAuthResourceMetadataFromURL fetches OAuth Protected Resource Metadata
// from an explicit URL. An optional http.Client may be provided; if nil,
// http.DefaultClient is used.
func FetchOAuthResourceMetadataFromURL(metadataURL string, client ...*http.Client) (*OAuthResourceMetadata, error) {
	c := http.DefaultClient
	if len(client) > 0 && client[0] != nil {
		c = client[0]
	}

	resp, err := c.Get(metadataURL)
	if err != nil {
		return nil, fmt.Errorf("fetching metadata: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metadata endpoint returned status %d", resp.StatusCode)
	}

	const maxMetadataSize = 1 << 20 // 1 MB
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxMetadataSize))
	if err != nil {
		return nil, fmt.Errorf("reading metadata body: %w", err)
	}

	var meta OAuthResourceMetadata
	if err := json.Unmarshal(body, &meta); err != nil {
		return nil, fmt.Errorf("parsing metadata JSON: %w", err)
	}
	return &meta, nil
}

// ParseResourceMetadataURL extracts the resource_metadata URL from a
// WWW-Authenticate header value. Returns an empty string if not found.
func ParseResourceMetadataURL(wwwAuthenticate string) string {
	const key = `resource_metadata="`
	idx := strings.Index(wwwAuthenticate, key)
	if idx == -1 {
		return ""
	}
	rest := wwwAuthenticate[idx+len(key):]
	end := strings.Index(rest, `"`)
	if end == -1 {
		return ""
	}
	return rest[:end]
}
