// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

func init() {
	// Register all state types for gob serialization (needed for HTTP transport).
	vgirpc.RegisterStateType(&CounterProducerState{})
	vgirpc.RegisterStateType(&EmptyProducerState{})
	vgirpc.RegisterStateType(&SingleProducerState{})
	vgirpc.RegisterStateType(&LargeProducerState{})
	vgirpc.RegisterStateType(&LoggingProducerState{})
	vgirpc.RegisterStateType(&ErrorAfterNState{})
	vgirpc.RegisterStateType(&HeaderProducerState{})
	vgirpc.RegisterStateType(&ScaleExchangeState{})
	vgirpc.RegisterStateType(&AccumulatingExchangeState{})
	vgirpc.RegisterStateType(&LoggingExchangeState{})
	vgirpc.RegisterStateType(&FailOnExchangeNState{})
}

// --- Producer States ---

// CounterProducerState produces count batches with {index, value}.
type CounterProducerState struct {
	Count   int
	Current int
}

func (s *CounterProducerState) Produce(_ context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	if s.Current >= s.Count {
		return out.Finish()
	}
	if err := emitCounterBatch(out, int64(s.Current)); err != nil {
		return err
	}
	s.Current++
	return nil
}

// EmptyProducerState finishes immediately — zero batches.
type EmptyProducerState struct{}

func (s *EmptyProducerState) Produce(_ context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	return out.Finish()
}

// SingleProducerState emits exactly one batch, then finishes.
type SingleProducerState struct {
	Emitted bool
}

func (s *SingleProducerState) Produce(_ context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	if s.Emitted {
		return out.Finish()
	}
	s.Emitted = true
	return emitCounterBatch(out, 0)
}

// LargeProducerState produces batch_count batches of rows_per_batch rows each.
type LargeProducerState struct {
	RowsPerBatch int
	BatchCount   int
	Current      int
}

func (s *LargeProducerState) Produce(_ context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	if s.Current >= s.BatchCount {
		return out.Finish()
	}
	offset := int64(s.Current * s.RowsPerBatch)
	numRows := int64(s.RowsPerBatch)

	mem := memory.NewGoAllocator()
	idxBuilder := array.NewInt64Builder(mem)
	valBuilder := array.NewInt64Builder(mem)
	defer idxBuilder.Release()
	defer valBuilder.Release()

	for i := int64(0); i < numRows; i++ {
		idx := offset + i
		idxBuilder.Append(idx)
		valBuilder.Append(idx * 10)
	}

	idxArr := idxBuilder.NewArray()
	valArr := valBuilder.NewArray()
	defer idxArr.Release()
	defer valArr.Release()

	if err := out.EmitArrays([]arrow.Array{idxArr, valArr}, numRows); err != nil {
		return err
	}
	s.Current++
	return nil
}

// LoggingProducerState produces batches with an INFO log before each.
type LoggingProducerState struct {
	Count   int
	Current int
}

func (s *LoggingProducerState) Produce(_ context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	if s.Current >= s.Count {
		return out.Finish()
	}
	out.ClientLog(vgirpc.LogInfo, fmt.Sprintf("producing batch %d", s.Current))
	if err := emitCounterBatch(out, int64(s.Current)); err != nil {
		return err
	}
	s.Current++
	return nil
}

// ErrorAfterNState raises after emitting emit_before_error batches.
type ErrorAfterNState struct {
	EmitBeforeError int
	Current         int
}

func (s *ErrorAfterNState) Produce(_ context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	if s.Current >= s.EmitBeforeError {
		return &vgirpc.RpcError{Type: "RuntimeError", Message: fmt.Sprintf("intentional error after %d batches", s.EmitBeforeError)}
	}
	if err := emitCounterBatch(out, int64(s.Current)); err != nil {
		return err
	}
	s.Current++
	return nil
}

// HeaderProducerState is used with stream headers — same as CounterProducerState.
type HeaderProducerState struct {
	Count   int
	Current int
}

func (s *HeaderProducerState) Produce(_ context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	if s.Current >= s.Count {
		return out.Finish()
	}
	if err := emitCounterBatch(out, int64(s.Current)); err != nil {
		return err
	}
	s.Current++
	return nil
}

// --- Exchange States ---

// ScaleExchangeState multiplies the value column by factor.
type ScaleExchangeState struct {
	Factor float64
}

func (s *ScaleExchangeState) Exchange(_ context.Context, input arrow.RecordBatch, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	// Read input value column
	valueCol := input.Column(0).(*array.Float64)
	numRows := input.NumRows()

	mem := memory.NewGoAllocator()
	b := array.NewFloat64Builder(mem)
	defer b.Release()
	for i := int64(0); i < numRows; i++ {
		b.Append(valueCol.Value(int(i)) * s.Factor)
	}
	arr := b.NewArray()
	defer arr.Release()

	return out.EmitArrays([]arrow.Array{arr}, numRows)
}

// AccumulatingExchangeState maintains running sum and exchange count.
type AccumulatingExchangeState struct {
	RunningSum    float64
	ExchangeCount int64
}

func (s *AccumulatingExchangeState) Exchange(_ context.Context, input arrow.RecordBatch, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	valueCol := input.Column(0).(*array.Float64)
	numRows := input.NumRows()

	// Sum input values
	var sum float64
	for i := int64(0); i < numRows; i++ {
		sum += valueCol.Value(int(i))
	}
	s.RunningSum += sum
	s.ExchangeCount++

	mem := memory.NewGoAllocator()
	sumBuilder := array.NewFloat64Builder(mem)
	countBuilder := array.NewInt64Builder(mem)
	defer sumBuilder.Release()
	defer countBuilder.Release()

	sumBuilder.Append(s.RunningSum)
	countBuilder.Append(s.ExchangeCount)

	sumArr := sumBuilder.NewArray()
	countArr := countBuilder.NewArray()
	defer sumArr.Release()
	defer countArr.Release()

	return out.EmitArrays([]arrow.Array{sumArr, countArr}, 1)
}

// LoggingExchangeState logs INFO + DEBUG per exchange, then echoes input.
type LoggingExchangeState struct{}

func (s *LoggingExchangeState) Exchange(_ context.Context, input arrow.RecordBatch, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	out.ClientLog(vgirpc.LogInfo, "exchange processing")
	out.ClientLog(vgirpc.LogDebug, "exchange debug")
	return out.Emit(input)
}

// FailOnExchangeNState raises on the Nth exchange (1-indexed).
type FailOnExchangeNState struct {
	FailOn        int
	ExchangeCount int
}

func (s *FailOnExchangeNState) Exchange(_ context.Context, input arrow.RecordBatch, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	s.ExchangeCount++
	if s.ExchangeCount >= s.FailOn {
		return &vgirpc.RpcError{Type: "RuntimeError", Message: fmt.Sprintf("intentional error on exchange %d", s.ExchangeCount)}
	}
	return out.Emit(input)
}

// --- Helper ---

func emitCounterBatch(out *vgirpc.OutputCollector, index int64) error {
	mem := memory.NewGoAllocator()
	idxBuilder := array.NewInt64Builder(mem)
	valBuilder := array.NewInt64Builder(mem)
	defer idxBuilder.Release()
	defer valBuilder.Release()

	idxBuilder.Append(index)
	valBuilder.Append(index * 10)

	idxArr := idxBuilder.NewArray()
	valArr := valBuilder.NewArray()
	defer idxArr.Release()
	defer valArr.Release()

	return out.EmitArrays([]arrow.Array{idxArr, valArr}, 1)
}

// --- Schemas ---

var ScaleInputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "value", Type: arrow.PrimitiveTypes.Float64},
}, nil)

var ScaleOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "value", Type: arrow.PrimitiveTypes.Float64},
}, nil)

var AccumInputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "value", Type: arrow.PrimitiveTypes.Float64},
}, nil)

var AccumOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "running_sum", Type: arrow.PrimitiveTypes.Float64},
	{Name: "exchange_count", Type: arrow.PrimitiveTypes.Int64},
}, nil)
