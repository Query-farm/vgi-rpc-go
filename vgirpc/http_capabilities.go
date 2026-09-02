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

// HTTPServerCapabilities is the bounded capability snapshot returned by
// DiscoverCapabilities. Zero numeric fields mean the server did not advertise
// a limit.
type HTTPServerCapabilities struct {
	MaxRequestBytes               int64
	MaxResponseBytes              int64
	AcceptMaxResponseBytesSupport bool
	ExternalizationEnabled        bool
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
		AcceptMaxResponseBytesSupport: supportPresent,
		ExternalizationEnabled:        external == "true",
	}, nil
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
