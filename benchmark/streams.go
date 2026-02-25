// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package benchmark

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/Query-farm/vgi-rpc/vgirpc"
)

// GenerateState produces Count batches with {i, value} where value = i * 10.
type GenerateState struct {
	Count   int
	Current int
}

func (s *GenerateState) Produce(_ context.Context, out *vgirpc.OutputCollector, _ *vgirpc.CallContext) error {
	if s.Current >= s.Count {
		return out.Finish()
	}
	idx := int64(s.Current)
	if err := out.EmitMap(map[string][]interface{}{
		"i":     {idx},
		"value": {idx * 10},
	}); err != nil {
		return err
	}
	s.Current++
	return nil
}

// TransformState scales input values by Factor.
type TransformState struct {
	Factor float64
}

func (s *TransformState) Exchange(_ context.Context, input arrow.RecordBatch, out *vgirpc.OutputCollector, _ *vgirpc.CallContext) error {
	mem := memory.NewGoAllocator()

	// Read values from the "value" column
	valueCol := input.Column(0).(*array.Float64)
	n := valueCol.Len()

	builder := array.NewFloat64Builder(mem)
	defer builder.Release()
	for i := 0; i < n; i++ {
		builder.Append(valueCol.Value(i) * s.Factor)
	}
	arr := builder.NewArray()
	defer arr.Release()

	return out.EmitArrays([]arrow.Array{arr}, int64(n))
}
