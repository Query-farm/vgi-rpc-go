// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"crypto"
	_ "crypto/sha1"
	_ "crypto/sha256"
	_ "crypto/sha512"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// XfccElement represents a single element from an x-forwarded-client-cert header.
// Fields correspond to Envoy XFCC key names.
type XfccElement struct {
	// Hash is the certificate hash.
	Hash string
	// Cert is the URL-decoded PEM certificate (if present).
	Cert string
	// Subject is the certificate subject DN.
	Subject string
	// URI is the SAN URI (e.g. SPIFFE ID).
	URI string
	// DNS contains the SAN DNS names.
	DNS []string
	// By is the server certificate identity.
	By string
}

// ParseXfcc parses an x-forwarded-client-cert header value into a slice of
// [XfccElement]. It handles comma-separated elements (respecting quoted
// values), semicolon-separated key=value pairs, and URL-encoded Cert/URI/By
// fields.
func ParseXfcc(headerValue string) []XfccElement {
	var elements []XfccElement
	for _, rawElement := range splitRespectingQuotes(headerValue, ',') {
		rawElement = strings.TrimSpace(rawElement)
		if rawElement == "" {
			continue
		}
		pairs := splitRespectingQuotes(rawElement, ';')
		var elem XfccElement
		for _, pair := range pairs {
			pair = strings.TrimSpace(pair)
			if pair == "" {
				continue
			}
			eqIdx := strings.IndexByte(pair, '=')
			if eqIdx < 0 {
				continue
			}
			key := strings.ToLower(strings.TrimSpace(pair[:eqIdx]))
			value := strings.TrimSpace(pair[eqIdx+1:])
			// Strip surrounding quotes
			if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
				value = unescapeQuoted(value[1 : len(value)-1])
			}
			switch key {
			case "cert", "uri", "by":
				if decoded, err := url.QueryUnescape(value); err == nil {
					value = decoded
				}
			}
			switch key {
			case "hash":
				elem.Hash = value
			case "cert":
				elem.Cert = value
			case "subject":
				elem.Subject = value
			case "uri":
				elem.URI = value
			case "dns":
				elem.DNS = append(elem.DNS, value)
			case "by":
				elem.By = value
			}
		}
		elements = append(elements, elem)
	}
	return elements
}

func splitRespectingQuotes(text string, delimiter byte) []string {
	var parts []string
	var current strings.Builder
	inQuotes := false
	for i := 0; i < len(text); i++ {
		ch := text[i]
		switch {
		case ch == '"':
			inQuotes = !inQuotes
			current.WriteByte(ch)
		case ch == '\\' && inQuotes && i+1 < len(text):
			current.WriteByte(ch)
			i++
			current.WriteByte(text[i])
		case ch == delimiter && !inQuotes:
			parts = append(parts, current.String())
			current.Reset()
		default:
			current.WriteByte(ch)
		}
	}
	parts = append(parts, current.String())
	return parts
}

var (
	backslashEscape = regexp.MustCompile(`\\(.)`)
	unescapedComma  = regexp.MustCompile(`(?:\\.|[^,])+`)
)

func unescapeQuoted(text string) string {
	return backslashEscape.ReplaceAllString(text, "$1")
}

// extractCN extracts the CN value from an RFC 4514 or similar DN string.
func extractCN(subject string) string {
	// Split on unescaped commas
	for _, part := range unescapedComma.FindAllString(subject, -1) {
		part = strings.TrimSpace(part)
		if len(part) > 3 && strings.EqualFold(part[:3], "CN=") {
			return part[3:]
		}
	}
	return ""
}

// MtlsAuthenticateXfccConfig configures [MtlsAuthenticateXfcc].
type MtlsAuthenticateXfccConfig struct {
	// Validate is an optional callback that receives the selected XfccElement
	// and returns an AuthContext. When nil, the Subject field's CN is used as
	// principal.
	Validate func(elem XfccElement) (*AuthContext, error)
	// Domain is the authentication domain name. Defaults to "mtls" if empty.
	Domain string
	// SelectElement controls which element to use when multiple are present:
	// "first" (original client, default) or "last" (nearest proxy).
	SelectElement string
}

// MtlsAuthenticateXfcc returns an [AuthenticateFunc] that extracts client
// identity from the Envoy x-forwarded-client-cert header. No certificate
// parsing or cryptography is needed — identity is extracted from the
// structured text header.
//
// Warning: The reverse proxy MUST strip client-supplied
// x-forwarded-client-cert headers before forwarding. These factories trust
// the header unconditionally.
func MtlsAuthenticateXfcc(cfg MtlsAuthenticateXfccConfig) (AuthenticateFunc, error) {
	if cfg.Domain == "" {
		cfg.Domain = "mtls"
	}
	if cfg.SelectElement == "" {
		cfg.SelectElement = "first"
	}
	if cfg.SelectElement != "first" && cfg.SelectElement != "last" {
		return nil, fmt.Errorf("vgirpc: MtlsAuthenticateXfcc SelectElement must be \"first\" or \"last\", got %q", cfg.SelectElement)
	}
	return func(r *http.Request) (*AuthContext, error) {
		headerValue := r.Header.Get("X-Forwarded-Client-Cert")
		if headerValue == "" {
			return nil, &RpcError{
				Type:    "ValueError",
				Message: "Missing x-forwarded-client-cert header",
			}
		}
		elements := ParseXfcc(headerValue)
		if len(elements) == 0 {
			return nil, &RpcError{
				Type:    "ValueError",
				Message: "Empty x-forwarded-client-cert header",
			}
		}
		var elem XfccElement
		if cfg.SelectElement == "last" {
			elem = elements[len(elements)-1]
		} else {
			elem = elements[0]
		}
		if cfg.Validate != nil {
			return cfg.Validate(elem)
		}
		// Default: extract CN from Subject
		principal := extractCN(elem.Subject)
		claims := make(map[string]any)
		if elem.Hash != "" {
			claims["hash"] = elem.Hash
		}
		if elem.Subject != "" {
			claims["subject"] = elem.Subject
		}
		if elem.URI != "" {
			claims["uri"] = elem.URI
		}
		if len(elem.DNS) > 0 {
			claims["dns"] = elem.DNS
		}
		if elem.By != "" {
			claims["by"] = elem.By
		}
		return &AuthContext{
			Domain:        cfg.Domain,
			Authenticated: true,
			Principal:     principal,
			Claims:        claims,
		}, nil
	}, nil
}

// parseCertFromHeader extracts and parses a PEM certificate from a request header.
func parseCertFromHeader(r *http.Request, header string) (*x509.Certificate, error) {
	raw := r.Header.Get(header)
	if raw == "" {
		return nil, &RpcError{
			Type:    "ValueError",
			Message: fmt.Sprintf("Missing %s header", header),
		}
	}
	pemStr, err := url.QueryUnescape(raw)
	if err != nil {
		return nil, &RpcError{
			Type:    "ValueError",
			Message: "Header value is not properly URL-encoded",
		}
	}
	if !strings.HasPrefix(pemStr, "-----BEGIN CERTIFICATE-----") {
		return nil, &RpcError{
			Type:    "ValueError",
			Message: "Header value is not a PEM certificate",
		}
	}
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, &RpcError{
			Type:    "ValueError",
			Message: "Failed to decode PEM block",
		}
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, &RpcError{
			Type:    "ValueError",
			Message: fmt.Sprintf("Failed to parse certificate: %v", err),
		}
	}
	return cert, nil
}

// checkCertExpiry validates that a certificate is within its validity period.
func checkCertExpiry(cert *x509.Certificate) error {
	now := time.Now().UTC()
	if now.Before(cert.NotBefore) {
		return &RpcError{
			Type:    "ValueError",
			Message: "Certificate is not yet valid",
		}
	}
	if now.After(cert.NotAfter) {
		return &RpcError{
			Type:    "ValueError",
			Message: "Certificate has expired",
		}
	}
	return nil
}

// MtlsAuthenticateConfig configures [MtlsAuthenticate].
type MtlsAuthenticateConfig struct {
	// Validate receives the parsed x509.Certificate and returns an AuthContext.
	// Required.
	Validate func(cert *x509.Certificate) (*AuthContext, error)
	// Header is the HTTP header containing the URL-encoded PEM certificate.
	// Defaults to "X-SSL-Client-Cert" if empty.
	Header string
	// CheckExpiry when true verifies the certificate is within its validity
	// period before calling Validate.
	CheckExpiry bool
}

// MtlsAuthenticate returns an [AuthenticateFunc] that parses a PEM client
// certificate from a proxy header and delegates identity extraction to the
// user-supplied Validate callback.
//
// No certificate chain validation is performed — that is the proxy's
// responsibility.
//
// Warning: The reverse proxy MUST strip client-supplied certificate headers
// before forwarding. Failure to do so allows clients to forge certificate
// identity.
func MtlsAuthenticate(cfg MtlsAuthenticateConfig) (AuthenticateFunc, error) {
	if cfg.Validate == nil {
		return nil, fmt.Errorf("vgirpc: MtlsAuthenticate requires a Validate callback")
	}
	if cfg.Header == "" {
		cfg.Header = "X-SSL-Client-Cert"
	}
	return func(r *http.Request) (*AuthContext, error) {
		cert, err := parseCertFromHeader(r, cfg.Header)
		if err != nil {
			return nil, err
		}
		if cfg.CheckExpiry {
			if err := checkCertExpiry(cert); err != nil {
				return nil, err
			}
		}
		return cfg.Validate(cert)
	}, nil
}

// MtlsAuthenticateFingerprintConfig configures [MtlsAuthenticateFingerprint].
type MtlsAuthenticateFingerprintConfig struct {
	// Fingerprints maps lowercase hex fingerprint strings (no colons) to
	// AuthContext values. Required.
	Fingerprints map[string]*AuthContext
	// Header is the HTTP header containing the URL-encoded PEM certificate.
	// Defaults to "X-SSL-Client-Cert" if empty.
	Header string
	// Algorithm is the hash algorithm for fingerprinting: "sha256" (default),
	// "sha1", "sha384", or "sha512".
	Algorithm string
	// CheckExpiry when true verifies the certificate is within its validity
	// period.
	CheckExpiry bool
}

// MtlsAuthenticateFingerprint returns an [AuthenticateFunc] that computes
// the certificate fingerprint and looks it up in a fixed map.
// Fingerprints must be lowercase hex without colons.
//
// It panics if Algorithm is not one of "sha256", "sha1", "sha384", or "sha512".
func MtlsAuthenticateFingerprint(cfg MtlsAuthenticateFingerprintConfig) (AuthenticateFunc, error) {
	if cfg.Algorithm == "" {
		cfg.Algorithm = "sha256"
	}
	var hashFunc crypto.Hash
	switch cfg.Algorithm {
	case "sha256":
		hashFunc = crypto.SHA256
	case "sha1":
		hashFunc = crypto.SHA1
	case "sha384":
		hashFunc = crypto.SHA384
	case "sha512":
		hashFunc = crypto.SHA512
	default:
		return nil, fmt.Errorf("vgirpc: unsupported hash algorithm: %s", cfg.Algorithm)
	}
	return MtlsAuthenticate(MtlsAuthenticateConfig{
		Header:      cfg.Header,
		CheckExpiry: cfg.CheckExpiry,
		Validate: func(cert *x509.Certificate) (*AuthContext, error) {
			h := hashFunc.New()
			h.Write(cert.Raw)
			fp := hex.EncodeToString(h.Sum(nil))
			ac, ok := cfg.Fingerprints[fp]
			if !ok {
				return nil, &RpcError{
					Type:    "ValueError",
					Message: fmt.Sprintf("Unknown certificate fingerprint: %s", fp),
				}
			}
			return ac, nil
		},
	})
}

// MtlsAuthenticateSubjectConfig configures [MtlsAuthenticateSubject].
type MtlsAuthenticateSubjectConfig struct {
	// Header is the HTTP header containing the URL-encoded PEM certificate.
	// Defaults to "X-SSL-Client-Cert" if empty.
	Header string
	// Domain is the authentication domain name. Defaults to "mtls" if empty.
	Domain string
	// AllowedSubjects when non-nil restricts accepted certificates to those
	// whose Subject Common Name is in this set. A nil value accepts any
	// certificate — this is a deliberate security decision.
	AllowedSubjects map[string]struct{}
	// CheckExpiry when true verifies the certificate is within its validity
	// period.
	CheckExpiry bool
}

// MtlsAuthenticateSubject returns an [AuthenticateFunc] that extracts the
// Subject Common Name as principal and populates claims with the full
// subject DN, serial number (hex), and not_valid_after (RFC 3339).
func MtlsAuthenticateSubject(cfg MtlsAuthenticateSubjectConfig) (AuthenticateFunc, error) {
	if cfg.Domain == "" {
		cfg.Domain = "mtls"
	}
	return MtlsAuthenticate(MtlsAuthenticateConfig{
		Header:      cfg.Header,
		CheckExpiry: cfg.CheckExpiry,
		Validate: func(cert *x509.Certificate) (*AuthContext, error) {
			cn := cert.Subject.CommonName
			if cfg.AllowedSubjects != nil {
				if _, ok := cfg.AllowedSubjects[cn]; !ok {
					return nil, &RpcError{
						Type:    "ValueError",
						Message: fmt.Sprintf("Subject CN %q not in allowed subjects", cn),
					}
				}
			}
			serialHex := fmt.Sprintf("%x", cert.SerialNumber)
			notValidAfter := cert.NotAfter.UTC().Format(time.RFC3339)
			// Build RFC 4514-style DN from subject
			subjectDN := cert.Subject.String()
			return &AuthContext{
				Domain:        cfg.Domain,
				Authenticated: true,
				Principal:     cn,
				Claims: map[string]any{
					"subject_dn":     subjectDN,
					"serial":         serialHex,
					"not_valid_after": notValidAfter,
				},
			}, nil
		},
	})
}

