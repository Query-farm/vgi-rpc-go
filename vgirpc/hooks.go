// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
)

// Method type string constants for DispatchInfo.MethodType.
const (
	DispatchMethodUnary  = "unary"
	DispatchMethodStream = "stream"
)

// DispatchHook provides observability callpoints around RPC dispatch.
// Implementations must be safe for concurrent use (HTTP transport is concurrent).
type DispatchHook interface {
	OnDispatchStart(ctx context.Context, info DispatchInfo) (context.Context, HookToken)
	OnDispatchEnd(ctx context.Context, token HookToken, info DispatchInfo, stats *CallStatistics, err error)
}

// HookToken is an opaque value returned by OnDispatchStart and passed back to
// OnDispatchEnd. Only meaningful to the DispatchHook that created it.
type HookToken interface{}

// DispatchInfo carries method metadata passed to hooks.
type DispatchInfo struct {
	Method            string            // RPC method name
	MethodType        string            // DispatchMethodUnary or DispatchMethodStream
	ServerID          string            // Server identifier
	Protocol          string            // Logical service / protocol name (server.SetServiceName)
	ProtocolHash      string            // SHA-256 hex of canonical __describe__ payload (always required in access log)
	ProtocolVersion   string            // Operator-supplied free-form protocol-contract label (optional)
	RequestID         string            // Client-supplied request identifier
	TransportMetadata map[string]string // Transport-level metadata (IPC custom metadata or HTTP headers)
	Auth              *AuthContext      // Auth context for this dispatch; never nil
	RemoteAddr        string            // HTTP transport: remote IP:port. Empty otherwise.
	HTTPStatus        int               // HTTP transport: response status; 0 when not applicable.
	RequestData       []byte            // Self-contained Arrow IPC stream of the request batch (unary + stream init only).
	StreamID          string            // Stream lifecycle identifier (32-char lowercase hex); empty on unary.
	Cancelled         bool              // True when a stream was cancelled by the client.
}

// CallStatistics holds per-call I/O counters matching the Python CallStatistics.
type CallStatistics struct {
	InputBatches  int64
	OutputBatches int64
	InputRows     int64
	OutputRows    int64
	InputBytes    int64
	OutputBytes   int64
}

// RecordInput records one input batch with the given row count and buffer size.
func (s *CallStatistics) RecordInput(numRows, bufferBytes int64) {
	s.InputBatches++
	s.InputRows += numRows
	s.InputBytes += bufferBytes
}

// RecordOutput records one output batch with the given row count and buffer size.
func (s *CallStatistics) RecordOutput(numRows, bufferBytes int64) {
	s.OutputBatches++
	s.OutputRows += numRows
	s.OutputBytes += bufferBytes
}

// methodTypeString maps a MethodType to a dispatch type string constant.
func methodTypeString(t MethodType) string {
	if t == MethodUnary {
		return DispatchMethodUnary
	}
	return DispatchMethodStream
}

// batchBufferSize returns the total top-level buffer size in bytes across all
// columns in a record batch. This matches Python's get_total_buffer_size().
func batchBufferSize(batch arrow.RecordBatch) int64 {
	var total int64
	for i := int64(0); i < batch.NumCols(); i++ {
		col := batch.Column(int(i))
		for _, buf := range col.Data().Buffers() {
			if buf != nil {
				total += int64(buf.Len())
			}
		}
	}
	return total
}
