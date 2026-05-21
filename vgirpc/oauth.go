// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

// OAuthResourceMetadata describes an OAuth 2.0 Protected Resource per RFC 9728.
type OAuthResourceMetadata struct {
	Resource             string   `json:"resource"`
	AuthorizationServers []string `json:"authorization_servers"`

	ScopesSupported                   []string `json:"scopes_supported,omitempty"`
	BearerMethodsSupported            []string `json:"bearer_methods_supported,omitempty"`
	ResourceSigningAlgValuesSupported []string `json:"resource_signing_alg_values_supported,omitempty"`

	ResourceName          string `json:"resource_name,omitempty"`
	ResourceDocumentation string `json:"resource_documentation,omitempty"`
	ResourcePolicyURI     string `json:"resource_policy_uri,omitempty"`
	ResourceTosURI        string `json:"resource_tos_uri,omitempty"`

	// ClientID is a custom RFC 9728 extension that tells OAuth clients which
	// client_id to use when authenticating with the authorization server.
	ClientID string `json:"client_id,omitempty"`

	// UseIDTokenAsBearer is a custom RFC 9728 extension that tells OAuth
	// clients to use the OIDC id_token (instead of the access_token) as the
	// Bearer token.
	UseIDTokenAsBearer bool `json:"use_id_token_as_bearer,omitempty"`

	// ClientSecret is a custom RFC 9728 extension that provides the
	// client_secret to use when authenticating with the authorization server.
	ClientSecret string `json:"client_secret,omitempty"`

	// DeviceCodeClientID is a custom RFC 9728 extension that provides a
	// separate client_id for the device code flow.
	DeviceCodeClientID string `json:"device_code_client_id,omitempty"`

	// DeviceCodeClientSecret is a custom RFC 9728 extension that provides a
	// separate client_secret for the device code flow.
	DeviceCodeClientSecret string `json:"device_code_client_secret,omitempty"`
}

// Validate checks that required fields are present.
func (m *OAuthResourceMetadata) Validate() error {
	if m.Resource == "" {
		return fmt.Errorf("oauth resource metadata: resource is required")
	}
	if len(m.AuthorizationServers) == 0 {
		return fmt.Errorf("oauth resource metadata: authorization_servers is required")
	}
	if m.ClientID != "" && !clientIDPattern.MatchString(m.ClientID) {
		return fmt.Errorf("oauth resource metadata: client_id contains invalid characters")
	}
	if m.ClientSecret != "" && !clientIDPattern.MatchString(m.ClientSecret) {
		return fmt.Errorf("oauth resource metadata: client_secret contains invalid characters")
	}
	if m.DeviceCodeClientID != "" && !clientIDPattern.MatchString(m.DeviceCodeClientID) {
		return fmt.Errorf("oauth resource metadata: device_code_client_id contains invalid characters")
	}
	if m.DeviceCodeClientSecret != "" && !clientIDPattern.MatchString(m.DeviceCodeClientSecret) {
		return fmt.Errorf("oauth resource metadata: device_code_client_secret contains invalid characters")
	}
	return nil
}

var clientIDPattern = regexp.MustCompile(`^[A-Za-z0-9\-._~]+$`)

// wellKnownURL builds the RFC 9728 well-known URL for this resource.
// The prefix is the path component of the resource URL (e.g. "/vgi").
func wellKnownURL(prefix string) string {
	return "/.well-known/oauth-protected-resource" + prefix
}

// buildWWWAuthenticate builds a WWW-Authenticate header value per RFC 9728.
func buildWWWAuthenticate(metadataURL string, m *OAuthResourceMetadata) string {
	s := fmt.Sprintf(`Bearer resource_metadata="%s"`, metadataURL)
	if m.ClientID != "" {
		s += fmt.Sprintf(`, client_id="%s"`, m.ClientID)
	}
	if m.UseIDTokenAsBearer {
		s += `, use_id_token_as_bearer="true"`
	}
	if m.ClientSecret != "" {
		s += fmt.Sprintf(`, client_secret="%s"`, m.ClientSecret)
	}
	if m.DeviceCodeClientID != "" {
		s += fmt.Sprintf(`, device_code_client_id="%s"`, m.DeviceCodeClientID)
	}
	if m.DeviceCodeClientSecret != "" {
		s += fmt.Sprintf(`, device_code_client_secret="%s"`, m.DeviceCodeClientSecret)
	}
	return s
}

// resourceMetadataURLFromResource derives the well-known metadata URL from a resource URL.
func resourceMetadataURLFromResource(resource string) (string, error) {
	u, err := url.Parse(resource)
	if err != nil {
		return "", fmt.Errorf("parsing resource URL: %w", err)
	}
	path := strings.TrimSuffix(u.Path, "/")
	u.Path = wellKnownURL(path)
	return u.String(), nil
}

// ToJSON serializes the metadata to JSON.
func (m *OAuthResourceMetadata) ToJSON() ([]byte, error) {
	return json.Marshal(m)
}
