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
	RequestID         string            // Client-supplied request identifier
	TransportMetadata map[string]string // Transport-level metadata (IPC custom metadata or HTTP headers)
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
