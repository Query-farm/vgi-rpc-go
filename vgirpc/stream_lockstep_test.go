// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func TestOutputCollectorRejectsSecondDataBatchAsProtocolError(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	out := newOutputCollector(schema, "test-server", true)
	defer out.releaseBatches()

	firstBuilder := array.NewInt64Builder(defaultAllocator())
	firstBuilder.Append(1)
	firstValues := firstBuilder.NewArray()
	firstBuilder.Release()
	first := array.NewRecordBatch(schema, []arrow.Array{firstValues}, 1)
	firstValues.Release()
	if err := out.Emit(first); err != nil {
		t.Fatalf("first emit failed: %v", err)
	}

	secondBuilder := array.NewInt64Builder(defaultAllocator())
	secondBuilder.Append(2)
	secondValues := secondBuilder.NewArray()
	secondBuilder.Release()
	second := array.NewRecordBatch(schema, []arrow.Array{secondValues}, 1)
	secondValues.Release()
	defer second.Release()
	err := out.Emit(second)
	var rpcErr *RpcError
	if !errors.As(err, &rpcErr) || rpcErr.Type != "ProtocolError" {
		t.Fatalf("second emit error = %v, want ProtocolError", err)
	}
	// Ignoring the direct error cannot hide the violation from dispatch.
	err = out.validate()
	if !errors.As(err, &rpcErr) || rpcErr.Type != "ProtocolError" {
		t.Fatalf("collector validation error = %v, want ProtocolError", err)
	}
}
