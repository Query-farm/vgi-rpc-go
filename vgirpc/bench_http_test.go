// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// End-to-end HTTP transport benchmarks: a full unary round trip through the
// real handler stack (body read, IPC decode, dispatch, result encode,
// capability headers, optional response compression).
//
// Benchmark-only; correctness lives in the Python conformance suite.

package vgirpc

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

type benchAddParams struct {
	A float64 `vgirpc:"a"`
	B float64 `vgirpc:"b"`
}

type benchGreetParams struct {
	Name string `vgirpc:"name"`
}

// newBenchHTTPServer builds an HttpServer with a couple of unary methods.
func newBenchHTTPServer(tb testing.TB) *HttpServer {
	tb.Helper()
	s := NewServer()
	Unary(s, "add", func(_ context.Context, _ *CallContext, p benchAddParams) (float64, error) {
		return p.A + p.B, nil
	})
	Unary(s, "greet", func(_ context.Context, _ *CallContext, p benchGreetParams) (string, error) {
		return "Hello, " + p.Name + "!", nil
	})
	return NewHttpServer(s)
}

// encodeRequestBody renders the wire bytes a client would POST.
func encodeRequestBody(tb testing.TB, method string, params any) []byte {
	tb.Helper()
	batch := buildParamsBatch(tb, params)
	defer batch.Release()

	var buf bytes.Buffer
	if err := WriteRequest(&buf, method, batch, ""); err != nil {
		tb.Fatalf("WriteRequest: %v", err)
	}
	return buf.Bytes()
}

// runUnary drives one full request/response cycle through the handler.
func runUnary(b *testing.B, h *HttpServer, path string, body []byte, acceptEncoding string) {
	b.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", arrowContentType)
	if acceptEncoding != "" {
		req.Header.Set("Accept-Encoding", acceptEncoding)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		b.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
}

func BenchmarkHTTPUnaryAdd(b *testing.B) {
	h := newBenchHTTPServer(b)
	body := encodeRequestBody(b, "add", benchAddParams{A: 1.5, B: 2.5})

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		runUnary(b, h, "/add", body, "")
	}
}

func BenchmarkHTTPUnaryGreet(b *testing.B) {
	h := newBenchHTTPServer(b)
	body := encodeRequestBody(b, "greet", benchGreetParams{Name: "world"})

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		runUnary(b, h, "/greet", body, "")
	}
}

// With compression enabled, the codec writer is the dominant per-request
// allocation unless it is pooled.
func BenchmarkHTTPUnaryZstd(b *testing.B) {
	h := newBenchHTTPServer(b)
	if err := h.SetCompressionLevel(3); err != nil {
		b.Fatal(err)
	}
	body := encodeRequestBody(b, "greet", benchGreetParams{Name: "world"})

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		runUnary(b, h, "/greet", body, "zstd")
	}
}

func BenchmarkHTTPUnaryGzip(b *testing.B) {
	h := newBenchHTTPServer(b)
	if err := h.SetCompressionLevel(3); err != nil {
		b.Fatal(err)
	}
	body := encodeRequestBody(b, "greet", benchGreetParams{Name: "world"})

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		runUnary(b, h, "/greet", body, "gzip")
	}
}

// Parallel run: surfaces pool contention and per-request goroutine churn
// that a serial benchmark hides.
func BenchmarkHTTPUnaryAddParallel(b *testing.B) {
	h := newBenchHTTPServer(b)
	body := encodeRequestBody(b, "add", benchAddParams{A: 1.5, B: 2.5})

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req := httptest.NewRequest(http.MethodPost, "/add", bytes.NewReader(body))
			req.Header.Set("Content-Type", arrowContentType)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				b.Errorf("status %d", rec.Code)
				return
			}
		}
	})
}

func BenchmarkHTTPUnaryZstdParallel(b *testing.B) {
	h := newBenchHTTPServer(b)
	if err := h.SetCompressionLevel(3); err != nil {
		b.Fatal(err)
	}
	body := encodeRequestBody(b, "greet", benchGreetParams{Name: "world"})

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req := httptest.NewRequest(http.MethodPost, "/greet", bytes.NewReader(body))
			req.Header.Set("Content-Type", arrowContentType)
			req.Header.Set("Accept-Encoding", "zstd")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				b.Errorf("status %d", rec.Code)
				return
			}
		}
	})
}

func BenchmarkHTTPHealth(b *testing.B) {
	h := newBenchHTTPServer(b)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("status %d", rec.Code)
		}
	}
}
