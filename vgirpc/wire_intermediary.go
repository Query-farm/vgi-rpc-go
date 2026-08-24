// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Public wire helpers for VGI-RPC intermediaries.
//
// vgi-rpc-go's first-class role is the *server* ([Server] / [HttpServer]).
// A third role — an **intermediary** (proxy, router, gateway, test harness)
// — needs to read a request off the wire, rewrite it, re-frame it for
// forwarding, and inspect or synthesize responses, without standing up a
// full client or server. This file is the stable public surface for that
// role, mirroring the Python reference's vgi_rpc.wire module:
//
//   - [ReadRequest] / [WriteRequest] — parse and re-frame a request body.
//   - [WriteErrorResponse] — synthesize an in-band error stream.
//   - [FindStateToken] / [FindCallStateToken] / [FindStreamTokens] — extract
//     a stream's continuation tokens. Forwarding a continuation needs both.
//   - [FindProtocolVersion] — recover the stamped application protocol_version.
//   - [ReadUnaryResult] / [WriteUnaryResult] — unwrap/rewrap the unary-response
//     envelope without a typed decode.

package vgirpc

import (
	"bytes"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// WriteRequest frames a request as a complete IPC stream (schema + 1 batch +
// EOS) for forwarding — the inverse of [ReadRequest]. The batch's
// custom_metadata carries vgi_rpc.method and vgi_rpc.request_version; any
// custom metadata already attached to params is NOT carried over (mirroring
// the Python reference's wire.write_request, which frames from bare values).
//
// protocolVersion is the application protocol_version to stamp on the
// request, so a versioned server's dispatch-boundary check still sees the
// originating client's version. Pass "" to emit a request that is
// structurally exempt from that check (the key is omitted).
func WriteRequest(w io.Writer, method string, params arrow.RecordBatch, protocolVersion string) error {
	keys := []string{MetaMethod, MetaRequestVersion}
	vals := []string{method, ProtocolVersion}
	if protocolVersion != "" {
		keys = append(keys, MetaProtocolVersion)
		vals = append(vals, protocolVersion)
	}
	meta := arrow.NewMetadata(keys, vals)

	batchWithMeta := array.NewRecordBatchWithMetadata(
		params.Schema(), params.Columns(), params.NumRows(), meta)
	defer batchWithMeta.Release()

	writer := ipc.NewWriter(w, ipc.WithSchema(params.Schema()))
	writeErr := writer.Write(batchWithMeta)
	closeErr := writer.Close()
	if writeErr != nil {
		return fmt.Errorf("writing request batch: %w", writeErr)
	}
	return closeErr
}

// FindStateToken returns the stream-state continuation token carried in a
// request or response body, or nil when absent or unparseable.
//
// The token (key [MetaStreamState]) rides in a record batch's
// custom_metadata — not a header. Stream continuations recover their state
// from this token, never from headers, so an intermediary that wants to
// route or correlate a stream by it must read the batch metadata.
//
// Two body shapes are handled by one walk:
//
//   - an exchange request is a single IPC stream whose first batch carries
//     the token;
//   - a producer init/exchange response may be several concatenated IPC
//     streams (a header stream followed by the producer's data stream — the
//     header lives in its own stream with a different schema), so the token
//     can be in a later stream.
//
// Returns the first token found across all concatenated streams. Lock-step
// responses carry one continuation token per turn.
//
// The returned bytes are the metadata value verbatim (the base64 text the
// peer echoes back), not a decoded payload.
//
// This is the cursor half only. An intermediary that *forwards* a
// continuation — rather than merely routing or correlating one — must carry
// the call token too when the upstream server splits its stream state; use
// [FindStreamTokens].
func FindStateToken(data []byte) []byte {
	state, _ := FindStreamTokens(data)
	return state
}

// FindCallStateToken returns the stream's call token (key [MetaCallState])
// carried in a request or response body, or nil when absent or unparseable.
//
// Only a server that splits its stream state emits one, and only on the
// /init response; see [MetaCallState].
func FindCallStateToken(data []byte) []byte {
	_, callState := FindStreamTokens(data)
	return callState
}

// FindStreamTokens returns both of a stream's continuation tokens — the
// cursor (key [MetaStreamState]) and the call token (key [MetaCallState]) —
// from a request or response body, in one walk. Either may be nil when
// absent or unparseable.
//
// An intermediary forwarding a continuation needs both. A server that splits
// its stream state re-mints only the cursor per turn and never re-issues the
// call token, so the continuation the intermediary builds has to echo the
// call token it saw on /init. Forwarding the cursor alone works for as long
// as that server's call-state cache happens to hold the entry, and fails
// once it does not — a restarted upstream, an evicted entry, or a request
// balanced onto a node that never saw the /init. This port's own server does
// not split yet, so callState is nil against it.
//
// Body shapes and first-token semantics are as described on
// [FindStateToken].
func FindStreamTokens(data []byte) (state, callState []byte) {
	r := bytes.NewReader(data)
	for r.Len() > 0 {
		before := r.Len()
		s, c, err := scanStreamForTokens(r)
		if state == nil {
			state = s
		}
		if callState == nil {
			callState = c
		}
		// The call token rides the same /init frame as the cursor, so once
		// the cursor is in hand there is nothing left to walk for.
		if state != nil {
			return state, callState
		}
		if err != nil {
			return state, callState
		}
		if r.Len() == before { // no forward progress — avoid an infinite loop
			return state, callState
		}
	}
	return state, callState
}

// scanStreamForTokens reads one IPC stream off r, scanning batch
// custom_metadata for [MetaStreamState] and [MetaCallState]. Returns the
// tokens if found, or an error when the stream could not be opened or read
// to its EOS.
func scanStreamForTokens(r io.Reader) (state, callState []byte, err error) {
	reader, rerr := ipc.NewReader(r)
	if rerr != nil {
		return nil, nil, rerr
	}
	defer reader.Release()

	for reader.Next() {
		rb, ok := reader.RecordBatch().(arrow.RecordBatchWithMetadata)
		if !ok {
			continue
		}
		md := rb.Metadata()
		if call, found := md.GetValue(MetaCallState); found && call != "" && callState == nil {
			callState = []byte(call)
		}
		if token, found := md.GetValue(MetaStreamState); found && token != "" {
			return []byte(token), callState, nil
		}
	}
	return nil, callState, reader.Err()
}

// FindProtocolVersion returns the application protocol_version stamped on a
// request body, or "" when absent or unparseable.
//
// Scans the request batch's custom_metadata for [MetaProtocolVersion]. An
// intermediary that rewrites a request must recover and re-stamp this (see
// [WriteRequest]) so the backend's dispatch-boundary version check still
// sees the originating client's version. A request that never carried one
// is structurally exempt from the check.
func FindProtocolVersion(data []byte) string {
	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return ""
	}
	defer reader.Release()

	for reader.Next() {
		if rb, ok := reader.RecordBatch().(arrow.RecordBatchWithMetadata); ok {
			if version, found := rb.Metadata().GetValue(MetaProtocolVersion); found && version != "" {
				return version
			}
		}
	}
	return ""
}

// ReadUnaryResult unwraps a unary-RPC response body to its envelope schema
// and raw result bytes.
//
// A unary response is an IPC stream of zero or more leading log batches
// (zero-row, carrying [MetaLogLevel] in batch custom_metadata) followed by
// one data batch whose "result" column holds the serialized response
// object. This returns the envelope schema plus the raw result bytes — no
// typed decode — for an intermediary that inspects or rewrites the response
// and re-wraps it via [WriteUnaryResult].
//
// Lenient: ok is false for an error/empty/non-"result" stream — and for a
// result column that is not binary (a scalar-returning method) — so the
// caller can forward the body unchanged.
func ReadUnaryResult(data []byte) (schema *arrow.Schema, result []byte, ok bool) {
	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, nil, false
	}
	defer reader.Release()

	for reader.Next() {
		batch := reader.RecordBatch()
		if batch.NumRows() > 0 {
			indices := batch.Schema().FieldIndices("result")
			if len(indices) == 0 {
				return nil, nil, false
			}
			bin, isBinary := batch.Column(indices[0]).(*array.Binary)
			if !isBinary || bin.Len() == 0 {
				return nil, nil, false
			}
			// Copy out of the reader-owned buffer before Release.
			return batch.Schema(), bytes.Clone(bin.Value(0)), true
		}
		// Zero-row log batches (including EXCEPTION error batches) carry
		// MetaLogLevel; skip logs, and fall through to not-ok for anything
		// else (an error batch never precedes a result).
		if rb, isMeta := batch.(arrow.RecordBatchWithMetadata); isMeta {
			if level, found := rb.Metadata().GetValue(MetaLogLevel); found && level != string(LogException) {
				continue
			}
		}
		return nil, nil, false
	}
	return nil, nil, false
}

// WriteUnaryResult builds a unary-RPC response IPC stream wrapping
// resultBytes — the inverse of [ReadUnaryResult]. It emits a single data
// batch whose binary "result" column carries resultBytes under
// envelopeSchema, which must be a single binary field.
func WriteUnaryResult(w io.Writer, envelopeSchema *arrow.Schema, resultBytes []byte) error {
	if envelopeSchema.NumFields() != 1 || envelopeSchema.Field(0).Type.ID() != arrow.BINARY {
		return fmt.Errorf("unary result envelope must be a single binary field, got %s", envelopeSchema)
	}

	b := array.NewBinaryBuilder(defaultAllocator(), arrow.BinaryTypes.Binary)
	defer b.Release()
	b.Append(resultBytes)
	arr := b.NewArray()
	defer arr.Release()

	batch := array.NewRecordBatch(envelopeSchema, []arrow.Array{arr}, 1)
	defer batch.Release()

	writer := ipc.NewWriter(w, ipc.WithSchema(envelopeSchema))
	writeErr := writer.Write(batch)
	closeErr := writer.Close()
	if writeErr != nil {
		return fmt.Errorf("writing unary result batch: %w", writeErr)
	}
	return closeErr
}
