// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ProducerState is the interface for producer stream state objects.
// Produce is called once per tick. It must either emit exactly one data batch
// via out.Emit/EmitArrays/EmitPyDict, or call out.Finish() to signal end-of-stream.
type ProducerState interface {
	Produce(ctx context.Context, out *OutputCollector, callCtx *CallContext) error
}

// ExchangeState is the interface for exchange stream state objects.
// Exchange is called once per input batch. It must emit exactly one data batch.
// It must NOT call out.Finish().
type ExchangeState interface {
	Exchange(ctx context.Context, input arrow.RecordBatch, out *OutputCollector, callCtx *CallContext) error
}

// StreamResult is returned by producer/exchange handler functions.
// It holds the output schema, state, and optional header and input schema.
type StreamResult struct {
	// OutputSchema defines the Arrow schema for batches emitted by the stream.
	OutputSchema *arrow.Schema
	// State holds the stream's mutable state object. It must implement
	// [ProducerState] for producer methods or [ExchangeState] for exchange
	// methods. The server calls Produce or Exchange on this object in the
	// lockstep streaming loop.
	State interface{}
	// InputSchema defines the Arrow schema for client-sent input batches.
	// It is nil for producer methods (which receive empty tick batches).
	InputSchema *arrow.Schema
	// Header is an optional [ArrowSerializable] value sent as a separate IPC
	// stream before the main data stream begins. Set to nil if no header.
	Header ArrowSerializable
}

// OutputCollector accumulates output batches during a produce/exchange call.
// It enforces that exactly one data batch is emitted per call (plus any number
// of log batches). Batches are stored in order because interleaving order
// matters for the wire protocol (logs must precede the data batch they annotate).
type OutputCollector struct {
	schema        *arrow.Schema
	batches       []annotatedBatch
	dataBatchIdx  int // -1 if no data batch yet
	finished      bool
	producerMode  bool
	serverID      string

	// EmitInterceptor, if non-nil, is called on every data batch before it is
	// stored. The returned batch replaces the original. This is used by the VGI
	// framework to auto-apply pushdown filters.
	EmitInterceptor func(batch arrow.RecordBatch) (arrow.RecordBatch, error)

	// ProcessSchema, if non-nil, is used by EmitArrays and EmitMap to build
	// batches instead of the output schema. Use this when the function emits
	// all columns but the wire output should be a projection.
	ProcessSchema *arrow.Schema
}

// annotatedBatch is a batch with optional custom metadata.
type annotatedBatch struct {
	batch arrow.RecordBatch
	meta  *arrow.Metadata // nil if no custom metadata
}

// newOutputCollector creates a new OutputCollector.
func newOutputCollector(schema *arrow.Schema, serverID string, producerMode bool) *OutputCollector {
	return &OutputCollector{
		schema:       schema,
		dataBatchIdx: -1,
		producerMode: producerMode,
		serverID:     serverID,
	}
}

// Emit adds a pre-built data batch. Returns an error if a data batch was already emitted.
// If the batch has a different schema object than the output schema, a new
// batch is created with the output schema to ensure IPC writer compatibility.
// If EmitInterceptor is set, it is called on the batch before storing.
func (o *OutputCollector) Emit(batch arrow.RecordBatch) error {
	if o.dataBatchIdx >= 0 {
		return fmt.Errorf("OutputCollector: only one data batch may be emitted per call")
	}
	if o.EmitInterceptor != nil {
		var err error
		batch, err = o.EmitInterceptor(batch)
		if err != nil {
			return err
		}
	}
	// Re-wrap with the output schema if schemas differ by pointer
	if batch.Schema() != o.schema {
		original := batch
		batch = array.NewRecordBatch(o.schema, batch.Columns(), batch.NumRows())
		original.Release()
	}
	o.dataBatchIdx = len(o.batches)
	o.batches = append(o.batches, annotatedBatch{batch: batch})
	return nil
}

// EmitArrays builds a RecordBatch from arrays using the output schema and emits it.
func (o *OutputCollector) EmitArrays(arrays []arrow.Array, numRows int64) error {
	s := o.schema
	if o.ProcessSchema != nil {
		s = o.ProcessSchema
	}
	batch := array.NewRecordBatch(s, arrays, numRows)
	return o.Emit(batch)
}

// EmitMap builds a 1-row RecordBatch from column name/value pairs using the
// output schema and emits it. Values must be slices matching the schema types.
func (o *OutputCollector) EmitMap(data map[string][]interface{}) error {
	mem := memory.NewGoAllocator()
	schema := o.schema
	if o.ProcessSchema != nil {
		schema = o.ProcessSchema
	}
	numRows := int64(0)
	cols := make([]arrow.Array, schema.NumFields())
	for i := range schema.NumFields() {
		f := schema.Field(i)
		vals := data[f.Name]
		if len(vals) > 0 && int64(len(vals)) > numRows {
			numRows = int64(len(vals))
		}
		arr := buildArrayFromSlice(mem, f.Type, vals)
		cols[i] = arr
	}
	batch := array.NewRecordBatch(schema, cols, numRows)
	for _, c := range cols {
		c.Release()
	}
	return o.Emit(batch)
}

// Finish signals end-of-stream for producer streams.
// Returns an error if called on an exchange stream.
func (o *OutputCollector) Finish() error {
	if !o.producerMode {
		return fmt.Errorf("OutputCollector: finish() is not allowed on exchange streams")
	}
	o.finished = true
	return nil
}

// Finished returns whether Finish() has been called.
func (o *OutputCollector) Finished() bool {
	return o.finished
}

// ClientLog emits a zero-row log batch with the given level and message.
func (o *OutputCollector) ClientLog(level LogLevel, message string, extras ...KV) {
	keys := []string{MetaLogLevel, MetaLogMessage}
	vals := []string{string(level), message}

	if len(extras) > 0 {
		extraMap := make(map[string]string, len(extras))
		for _, kv := range extras {
			extraMap[kv.Key] = kv.Value
		}
		extraJSON, _ := json.Marshal(extraMap)
		keys = append(keys, MetaLogExtra)
		vals = append(vals, string(extraJSON))
	}
	if o.serverID != "" {
		keys = append(keys, MetaServerID)
		vals = append(vals, o.serverID)
	}

	meta := arrow.NewMetadata(keys, vals)
	batch := emptyBatch(o.schema)
	o.batches = append(o.batches, annotatedBatch{batch: batch, meta: &meta})
}

// validate checks that exactly one data batch was emitted.
func (o *OutputCollector) validate() error {
	if o.dataBatchIdx < 0 {
		return &RpcError{Type: "RuntimeError", Message: "No data batch was emitted"}
	}
	return nil
}

// buildArrayFromSlice builds an Arrow array from a slice of interface values.
func buildArrayFromSlice(mem memory.Allocator, dt arrow.DataType, vals []interface{}) arrow.Array {
	switch dt.ID() {
	case arrow.INT64:
		b := array.NewInt64Builder(mem)
		defer b.Release()
		for _, v := range vals {
			switch val := v.(type) {
			case int64:
				b.Append(val)
			case int:
				b.Append(int64(val))
			default:
				b.AppendNull()
			}
		}
		return b.NewArray()
	case arrow.FLOAT64:
		b := array.NewFloat64Builder(mem)
		defer b.Release()
		for _, v := range vals {
			switch val := v.(type) {
			case float64:
				b.Append(val)
			case float32:
				b.Append(float64(val))
			default:
				b.AppendNull()
			}
		}
		return b.NewArray()
	case arrow.STRING:
		b := array.NewStringBuilder(mem)
		defer b.Release()
		for _, v := range vals {
			if s, ok := v.(string); ok {
				b.Append(s)
			} else {
				b.AppendNull()
			}
		}
		return b.NewArray()
	case arrow.BOOL:
		b := array.NewBooleanBuilder(mem)
		defer b.Release()
		for _, v := range vals {
			if val, ok := v.(bool); ok {
				b.Append(val)
			} else {
				b.AppendNull()
			}
		}
		return b.NewArray()
	default:
		// Fallback: build null array
		b := array.NewBuilder(mem, dt)
		defer b.Release()
		for range vals {
			b.AppendNull()
		}
		return b.NewArray()
	}
}

