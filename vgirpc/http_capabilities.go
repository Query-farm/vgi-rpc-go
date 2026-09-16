// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// HTTPServerCapabilities is the capability snapshot returned by
// DiscoverCapabilities. Zero numeric fields mean the server did not advertise
// a limit.
//
// The byte caps and the budget flag are what the client enforces on itself;
// the rest describes optional server features a caller decides whether to use.
// They are read from the same response headers -- every response carries them,
// not just the OPTIONS probe -- so reporting only some of them left a Go caller
// unable to discover sticky sessions or upload URLs at all, while the server
// had been advertising both all along.
type HTTPServerCapabilities struct {
	MaxRequestBytes               int64
	MaxResponseBytes              int64
	MaxExternalizedResponseBytes  int64
	AcceptMaxResponseBytesSupport bool
	ExternalizationEnabled        bool
	// UploadURLSupport reports whether {prefix}/__upload_url__/init is routed.
	UploadURLSupport bool
	MaxUploadBytes   int64
	// SupportedEncodings are the lowercase wire tokens the server can decode
	// on a request body -- "zstd", "gzip", "identity" -- in the order
	// advertised. Empty when the server names none.
	SupportedEncodings []string
	StickyEnabled      bool
	// StickyDefaultTTL is in seconds, and 0 means unadvertised.
	StickyDefaultTTL int64
	// StickyEchoHeaders are the header names, prefix removed, that the server
	// will echo on a session-opening response for the client to replay.
	StickyEchoHeaders []string
}

func parseCapabilityDecimal(headers http.Header, name string) (int64, error) {
	values, present := headers[http.CanonicalHeaderKey(name)]
	if !present {
		return 0, nil
	}
	if len(values) != 1 {
		return 0, fmt.Errorf("vgirpc: capability %s must occur exactly once", name)
	}
	parser := parsePositiveSafeDecimal
	if name == maxResponseBytesHeader {
		parser = parseResponseBudgetDecimal
	}
	value, err := parser(values[0])
	if err != nil {
		return 0, fmt.Errorf("vgirpc: invalid capability %s: %w", name, err)
	}
	return value, nil
}

// ParseHTTPServerCapabilities validates capability headers from any server
// response. Decimal values use the cross-language safe range 1..2^53-1.
func ParseHTTPServerCapabilities(headers http.Header) (HTTPServerCapabilities, error) {
	maxRequest, err := parseCapabilityDecimal(headers, maxRequestBytesHeader)
	if err != nil {
		return HTTPServerCapabilities{}, err
	}
	maxResponse, err := parseCapabilityDecimal(headers, maxResponseBytesHeader)
	if err != nil {
		return HTTPServerCapabilities{}, err
	}
	supportValues, supportPresent := headers[http.CanonicalHeaderKey(acceptMaxResponseBytesSupportHeader)]
	if supportPresent && (len(supportValues) != 1 || supportValues[0] != "true") {
		return HTTPServerCapabilities{}, fmt.Errorf("vgirpc: capability %s must occur exactly once with value true", acceptMaxResponseBytesSupportHeader)
	}
	external := headers.Get(externalizationEnabledHeader)
	if external != "" && external != "true" && external != "false" {
		return HTTPServerCapabilities{}, fmt.Errorf("vgirpc: invalid capability %s=%q", externalizationEnabledHeader, external)
	}
	return HTTPServerCapabilities{
		MaxRequestBytes:               maxRequest,
		MaxResponseBytes:              maxResponse,
		MaxExternalizedResponseBytes:  capabilityDecimal(headers, maxExternalizedResponseBytesHeader),
		AcceptMaxResponseBytesSupport: supportPresent,
		ExternalizationEnabled:        external == "true",
		UploadURLSupport:              capabilityFlag(headers, uploadURLHeader),
		MaxUploadBytes:                capabilityDecimal(headers, maxUploadBytesHeader),
		SupportedEncodings:            capabilityList(headers, supportedEncodingsHeader),
		StickyEnabled:                 capabilityFlag(headers, stickyEnabledHeader),
		StickyDefaultTTL:              capabilityDecimal(headers, stickyDefaultTTLHeader),
		StickyEchoHeaders:             capabilityList(headers, stickyEchoHeadersHeader),
	}, nil
}

// capabilityDecimal reads an optional-feature capability header, reporting 0
// for absent, malformed, or out-of-range.
//
// Deliberately more forgiving than [parseCapabilityDecimal], which fails the
// whole response. That strictness is right for the two byte caps the client
// enforces on itself -- getting those wrong means sending or accepting a body
// nobody agreed to. These describe optional features instead, and this runs on
// every response: turning one malformed advertisement of a feature the caller
// never asked for into total unavailability is the wrong trade, so an
// unreadable value reads as "not advertised".
func capabilityDecimal(headers http.Header, name string) int64 {
	raw := strings.TrimSpace(headers.Get(name))
	if raw == "" {
		return 0
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return 0
	}
	return value
}

// capabilityFlag reads a boolean capability header.
//
// Anything other than "true" is false, including a malformed value: an
// optional feature a client cannot confidently read is a feature it should not
// use, and refusing the whole response over one unparseable optional flag
// would make a server unusable for every caller that never wanted the feature.
func capabilityFlag(headers http.Header, name string) bool {
	return strings.EqualFold(strings.TrimSpace(headers.Get(name)), "true")
}

// capabilityList splits a comma-separated capability header, dropping empties.
func capabilityList(headers http.Header, name string) []string {
	raw := strings.TrimSpace(headers.Get(name))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// DiscoverCapabilities probes OPTIONS /health and validates the advertised
// limits without dispatching an RPC method.
func (c *HttpClient) DiscoverCapabilities(ctx context.Context) (HTTPServerCapabilities, error) {
	if c == nil || c.closed.Load() {
		return HTTPServerCapabilities{}, fmt.Errorf("vgirpc: HTTP client is closed")
	}
	u := *c.baseURL
	u.Path = strings.TrimRight(c.baseURL.Path, "/") + c.prefix + "/health"
	req, err := http.NewRequestWithContext(ctx, http.MethodOptions, u.String(), nil)
	if err != nil {
		return HTTPServerCapabilities{}, fmt.Errorf("vgirpc: build capability request: %w", err)
	}
	req.Header = c.headers.Clone()
	req.Header.Set(acceptMaxResponseBytesHeader, strconv.FormatInt(c.acceptedMaxResponse, 10))
	resp, err := c.inner.Do(req)
	if err != nil {
		return HTTPServerCapabilities{}, fmt.Errorf("vgirpc: capability request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return HTTPServerCapabilities{}, &HTTPStatusError{StatusCode: resp.StatusCode, RequestID: resp.Header.Get(requestIDHeader)}
	}
	return ParseHTTPServerCapabilities(resp.Header)
}
