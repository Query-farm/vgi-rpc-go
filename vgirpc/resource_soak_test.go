// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"runtime"
	"runtime/debug"
	"testing"
)

// TestHTTPUnaryLiveHeapPlateaus complements the black-box RSS soak. The Go
// runtime deliberately retains heap arenas, so RSS can rise in large steps
// even when every request object is dead. Force scavenging around the measured
// interval and assert the live heap and goroutine registry return near their
// post-warm-up baseline.
func TestHTTPUnaryLiveHeapPlateaus(t *testing.T) {
	handler := newBenchHTTPServer(t)
	body := encodeRequestBody(t, "add", benchAddParams{A: 1.5, B: 2.5})

	call := func() {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/add", bytes.NewReader(body))
		req.Header.Set("Content-Type", arrowContentType)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
	}

	for range 2_000 {
		call()
	}
	debug.FreeOSMemory()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)
	baselineGoroutines := runtime.NumGoroutine()

	for range 20_000 {
		call()
	}
	debug.FreeOSMemory()
	var final runtime.MemStats
	runtime.ReadMemStats(&final)

	const maxLiveHeapGrowth = 8 * 1024 * 1024
	growth := int64(final.HeapAlloc) - int64(baseline.HeapAlloc)
	t.Logf("post-GC live heap: baseline=%d final=%d growth=%d", baseline.HeapAlloc, final.HeapAlloc, growth)
	if growth > maxLiveHeapGrowth {
		t.Fatalf(
			"live heap retained %d bytes after 20,000 requests (limit %d; baseline=%d final=%d)",
			growth,
			maxLiveHeapGrowth,
			baseline.HeapAlloc,
			final.HeapAlloc,
		)
	}
	if growth := runtime.NumGoroutine() - baselineGoroutines; growth > 1 {
		t.Fatalf("goroutines retained %d after 20,000 requests", growth)
	}
}
