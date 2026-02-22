// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// BatchKind classifies a received batch based on its metadata.
type BatchKind int

const (
	BatchData            BatchKind = iota // regular data batch
	BatchLog                              // client-directed log batch
	BatchError                            // error/exception batch
	BatchExternalPointer                  // external pointer reference
	BatchShmPointer                       // shared memory pointer
	BatchStateToken                       // HTTP stateful stream token
)

// Request represents a parsed RPC request from the wire.
type Request struct {
	Method    string
	Version   string
	RequestID string
	LogLevel  string
	Batch     arrow.RecordBatch
	Metadata  map[string]string
}

// ReadRequest reads one complete IPC stream from the reader and extracts
// the method name, version, and parameter values from the first batch.
func ReadRequest(r io.Reader) (*Request, error) {
	reader, err := ipc.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("reading request IPC stream: %w", err)
	}
	defer reader.Release()

	if !reader.Next() {
		if err := reader.Err(); err != nil {
			return nil, fmt.Errorf("reading request batch: %w", err)
		}
		return nil, io.EOF
	}

	batch := reader.RecordBatch()
	batch.Retain() // keep batch alive after reader is released

	// Extract custom metadata
	var meta arrow.Metadata
	if rb, ok := batch.(arrow.RecordBatchWithMetadata); ok {
		meta = rb.Metadata()
	}

	// Extract method name
	method, ok := meta.GetValue(MetaMethod)
	if !ok {
		batch.Release()
		return nil, &RpcError{
			Type:    "ProtocolError",
			Message: "Missing 'vgi_rpc.method' in request batch custom_metadata",
		}
	}

	// Validate request version
	version, ok := meta.GetValue(MetaRequestVersion)
	if !ok {
		batch.Release()
		return nil, &RpcError{
			Type:    "VersionError",
			Message: "Missing 'vgi_rpc.request_version' in request batch custom_metadata",
		}
	}
	if version != ProtocolVersion {
		batch.Release()
		return nil, &RpcError{
			Type:    "VersionError",
			Message: fmt.Sprintf("Unsupported request version %q, expected %q", version, ProtocolVersion),
		}
	}

	// Validate row count
	if batch.Schema().NumFields() > 0 && batch.NumRows() != 1 {
		batch.Release()
		return nil, &RpcError{
			Type:    "ProtocolError",
			Message: fmt.Sprintf("Expected 1 row in request batch, got %d", batch.NumRows()),
		}
	}

	requestID, _ := meta.GetValue(MetaRequestID)
	logLevel, _ := meta.GetValue(MetaLogLevel)

	// Drain remaining batches (read to EOS)
	for reader.Next() {
		// discard
	}

	// Convert metadata to map
	metaMap := make(map[string]string)
	for i := range meta.Len() {
		metaMap[meta.Keys()[i]] = meta.Values()[i]
	}

	return &Request{
		Method:    method,
		Version:   version,
		RequestID: requestID,
		LogLevel:  logLevel,
		Batch:     batch,
		Metadata:  metaMap,
	}, nil
}

// emptyBatch creates a zero-row batch with the given schema.
func emptyBatch(schema *arrow.Schema) arrow.RecordBatch {
	mem := memory.NewGoAllocator()
	cols := make([]arrow.Array, schema.NumFields())
	for i, f := range schema.Fields() {
		cols[i] = makeEmptyArray(mem, f.Type)
	}
	batch := array.NewRecordBatch(schema, cols, 0)
	for _, c := range cols {
		c.Release()
	}
	return batch
}

// makeEmptyArray creates a zero-length array of the given type.
func makeEmptyArray(mem memory.Allocator, dt arrow.DataType) arrow.Array {
	builder := array.NewBuilder(mem, dt)
	defer builder.Release()
	return builder.NewArray()
}

// writeLogBatch writes a zero-row batch with log metadata.
func writeLogBatch(w *ipc.Writer, schema *arrow.Schema, msg LogMessage, serverID, requestID string) error {
	keys := []string{MetaLogLevel, MetaLogMessage}
	vals := []string{string(msg.Level), msg.Message}

	if len(msg.Extras) > 0 {
		extraJSON, err := json.Marshal(msg.Extras)
		if err != nil {
			extraJSON = []byte(`{}`)
		}
		keys = append(keys, MetaLogExtra)
		vals = append(vals, string(extraJSON))
	}
	if serverID != "" {
		keys = append(keys, MetaServerID)
		vals = append(vals, serverID)
	}
	if requestID != "" {
		keys = append(keys, MetaRequestID)
		vals = append(vals, requestID)
	}

	meta := arrow.NewMetadata(keys, vals)
	batch := emptyBatch(schema)
	defer batch.Release()

	batchWithMeta := array.NewRecordBatchWithMetadata(schema, batch.Columns(), 0, meta)
	defer batchWithMeta.Release()

	return w.Write(batchWithMeta)
}

// writeErrorBatch writes a zero-row batch with EXCEPTION-level metadata.
func writeErrorBatch(w *ipc.Writer, schema *arrow.Schema, err error, serverID, requestID string) error {
	extraJSON := buildErrorExtra(err)

	keys := []string{MetaLogLevel, MetaLogMessage, MetaLogExtra}
	vals := []string{string(LogException), err.Error(), extraJSON}

	if serverID != "" {
		keys = append(keys, MetaServerID)
		vals = append(vals, serverID)
	}
	if requestID != "" {
		keys = append(keys, MetaRequestID)
		vals = append(vals, requestID)
	}

	meta := arrow.NewMetadata(keys, vals)
	batch := emptyBatch(schema)
	defer batch.Release()

	batchWithMeta := array.NewRecordBatchWithMetadata(schema, batch.Columns(), 0, meta)
	defer batchWithMeta.Release()

	return w.Write(batchWithMeta)
}

// WriteUnaryResponse writes a complete IPC stream containing log batches followed
// by a result batch. The stream is: schema + log batches + result batch + EOS.
func WriteUnaryResponse(w io.Writer, schema *arrow.Schema, logs []LogMessage,
	result arrow.RecordBatch, serverID, requestID string) error {

	writer := ipc.NewWriter(w, ipc.WithSchema(schema))
	defer writer.Close()

	// Write log batches first
	for _, logMsg := range logs {
		if err := writeLogBatch(writer, schema, logMsg, serverID, requestID); err != nil {
			return fmt.Errorf("writing log batch: %w", err)
		}
	}

	// Write result batch
	return writer.Write(result)
}

// WriteErrorResponse writes a complete IPC stream containing just an error batch.
func WriteErrorResponse(w io.Writer, schema *arrow.Schema, err error, serverID, requestID string) error {
	writer := ipc.NewWriter(w, ipc.WithSchema(schema))
	defer writer.Close()

	return writeErrorBatch(writer, schema, err, serverID, requestID)
}

// WriteVoidResponse writes a complete IPC stream with logs and a zero-row empty-schema response.
func WriteVoidResponse(w io.Writer, logs []LogMessage, serverID, requestID string) error {
	schema := arrow.NewSchema(nil, nil)
	batch := emptyBatch(schema)
	defer batch.Release()

	return WriteUnaryResponse(w, schema, logs, batch, serverID, requestID)
}
