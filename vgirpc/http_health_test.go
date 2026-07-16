// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Regression test: HEAD /health must answer 200 so capability discovery
// doesn't degrade. The C++ client probes /health with HEAD (the mandatory,
// auth-exempt capability discovery endpoint); a 405 carries no
// Content-Length and isn't the 200 the protocol expects, so discovery would
// silently fall back to defaults. Mirrors the Python reference's
// test_head_health regression (vgi-rpc 2858d29); in Go this holds because
// net/http's ServeMux matches HEAD against GET patterns and the transport
// strips the body, so the test guards those semantics.

package vgirpc

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHeadHealthMatchesGet(t *testing.T) {
	h := newTestHttpServer(t)
	h.SetMaxRequestBytes(1024 * 1024) // advertise a capability header
	h.InitPages()

	ts := httptest.NewServer(h)
	defer ts.Close()

	get, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	getBody, err := io.ReadAll(get.Body)
	_ = get.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if get.StatusCode != http.StatusOK {
		t.Fatalf("GET /health: expected 200, got %d", get.StatusCode)
	}

	head, err := http.Head(ts.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	headBody, err := io.ReadAll(head.Body)
	_ = head.Body.Close()
	if err != nil {
		t.Fatal(err)
	}

	if head.StatusCode != http.StatusOK {
		t.Fatalf("HEAD /health: expected 200, got %d", head.StatusCode)
	}
	if len(headBody) != 0 {
		t.Fatalf("HEAD /health: expected empty body, got %d bytes", len(headBody))
	}
	if head.ContentLength != int64(len(getBody)) {
		t.Fatalf("HEAD /health: expected Content-Length %d, got %d", len(getBody), head.ContentLength)
	}
	// Verb-independent discovery: the same Content-Type and capability
	// headers as GET.
	for _, header := range []string{"Content-Type", supportedEncodingsHeader, maxRequestBytesHeader} {
		if got, want := head.Header.Get(header), get.Header.Get(header); got != want {
			t.Fatalf("HEAD /health: expected %s=%q (as on GET), got %q", header, want, got)
		}
	}
}
