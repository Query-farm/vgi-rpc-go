// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
)

// egressRecorder measures what actually crossed the network for one HTTP
// request, and defers that request's access-log records until it knows.
//
// A record used to be written by the dispatch that produced the response,
// which is too early to know what the response weighed: response compression
// runs afterwards, in [compressResponseWriter.finish]. A record written at
// dispatch time can only report the uncompressed body, which is the wrong
// number for anything that costs money.
//
// So [HttpServer.ServeHTTP] installs one of these, dispatch appends its
// records to it, and it emits once the final body has been written —
// stamping response_bytes on the way out.
//
// Three numbers answering three different questions:
//
//   - request_bytes / response_bytes — what crossed the wire, after
//     compression. This is the egress figure.
//   - input_bytes / output_bytes — logical Arrow buffers, i.e. what the
//     worker processed. Unaffected by compression, and routinely orders of
//     magnitude larger.
//   - externalized_bytes — uploaded to external storage, which never touches
//     the HTTP body at all and is frequently the largest of the three.
//
// A crash between dispatch and response loses that request's records. The
// alternative — emit early and under-report — trades a rare loss for a
// permanently wrong number.
type egressRecorder struct {
	// requestID is the correlation id the transport minted or echoed, used
	// when the request batch carried none of its own.
	requestID string
	// requestBytes is the body as received: before decompression, and
	// therefore the number of bytes the peer actually sent.
	requestBytes  int64
	responseBytes atomic.Int64
	externalized  atomic.Int64

	mu      sync.Mutex
	pending []pendingAccessRecord
}

// pendingAccessRecord is one deferred record and the hook that owns it.
type pendingAccessRecord struct {
	hook   *AccessLogHook
	record map[string]any
}

type egressRecorderKey struct{}

// withEgressRecorder returns ctx carrying rec.
func withEgressRecorder(ctx context.Context, rec *egressRecorder) context.Context {
	return context.WithValue(ctx, egressRecorderKey{}, rec)
}

// egressRecorderFrom returns the recorder installed for the request in
// flight, or nil. Nil is the normal case for every non-HTTP transport, and
// means "emit inline" — the immediate-vs-deferred choice is made in one
// place, so a transport that installs no recorder keeps logging inline.
func egressRecorderFrom(ctx context.Context) *egressRecorder {
	if ctx == nil {
		return nil
	}
	rec, _ := ctx.Value(egressRecorderKey{}).(*egressRecorder)
	return rec
}

// countExternalizedBytes adds n uploaded bytes to the current call's
// externalisation total. A no-op when nothing is recording.
func countExternalizedBytes(ctx context.Context, n int64) {
	if rec := egressRecorderFrom(ctx); rec != nil {
		rec.externalized.Add(n)
	}
}

// queue defers a record until the response body exists.
func (rec *egressRecorder) queue(hook *AccessLogHook, record map[string]any) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.pending = append(rec.pending, pendingAccessRecord{hook: hook, record: record})
}

// flush stamps the final on-wire response size onto every deferred record and
// emits them. Called once, after the response body has been written.
func (rec *egressRecorder) flush() {
	rec.mu.Lock()
	pending := rec.pending
	rec.pending = nil
	rec.mu.Unlock()
	if len(pending) == 0 {
		return
	}
	responseBytes := rec.responseBytes.Load()
	for _, p := range pending {
		p.record["response_bytes"] = responseBytes
		p.hook.emit(p.record)
	}
}

// countingResponseWriter tallies the bytes handed to the underlying writer.
// It sits outside [compressResponseWriter], so what it counts is the body
// after compression — the bytes that actually leave the process.
type countingResponseWriter struct {
	http.ResponseWriter
	rec *egressRecorder
}

func (c *countingResponseWriter) Write(b []byte) (int, error) {
	n, err := c.ResponseWriter.Write(b)
	c.rec.responseBytes.Add(int64(n))
	return n, err
}

// Unwrap lets http.ResponseController reach the real writer, so wrapping does
// not cost a handler its Flush/SetWriteDeadline support.
func (c *countingResponseWriter) Unwrap() http.ResponseWriter {
	return c.ResponseWriter
}

// Flush forwards to the underlying writer when it supports flushing.
func (c *countingResponseWriter) Flush() {
	if f, ok := c.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
