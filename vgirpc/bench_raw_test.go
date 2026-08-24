// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// End-to-end raw dispatch benchmarks. These omit the client and kernel socket
// costs so allocation and contention regressions inside the server stay
// visible independently of the benchmark host.

package vgirpc

import (
	"bytes"
	"context"
	"io"
	"testing"
)

func newBenchRawServer(tb testing.TB) *Server {
	tb.Helper()
	server := NewServer()
	server.SetProtocolVersion("2.0.0")
	Unary(server, "add", func(_ context.Context, _ *CallContext, p benchAddParams) (float64, error) {
		return p.A + p.B, nil
	})
	if err := server.notifyTransport(TransportKindUnix, nil); err != nil {
		tb.Fatalf("notifyTransport: %v", err)
	}
	return server
}

func rawBenchRequest(tb testing.TB) []byte {
	tb.Helper()
	batch := buildParamsBatch(tb, benchAddParams{A: 1.5, B: 2.5})
	defer batch.Release()
	var body bytes.Buffer
	if err := WriteRequest(&body, "add", batch, "2.0.0"); err != nil {
		tb.Fatalf("WriteRequest: %v", err)
	}
	return body.Bytes()
}

func BenchmarkRawUnaryAdd(b *testing.B) {
	server := newBenchRawServer(b)
	body := rawBenchRequest(b)
	reader := bytes.NewReader(body)
	connection := &shmConnState{}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		reader.Reset(body)
		if err := server.serveOne(context.Background(), reader, io.Discard, connection); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRawUnaryAddParallel(b *testing.B) {
	server := newBenchRawServer(b)
	body := rawBenchRequest(b)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		reader := bytes.NewReader(body)
		connection := &shmConnState{}
		for pb.Next() {
			reader.Reset(body)
			if err := server.serveOne(context.Background(), reader, io.Discard, connection); err != nil {
				b.Errorf("serveOne: %v", err)
				return
			}
		}
	})
}
