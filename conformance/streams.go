// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"context"
	"fmt"
	"sync"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func init() {
	// Register all state types for gob serialization (needed for HTTP transport).
	vgirpc.RegisterStateType(&counterProducerState{})
	vgirpc.RegisterStateType(&emptyProducerState{})
	vgirpc.RegisterStateType(&singleProducerState{})
	vgirpc.RegisterStateType(&largeProducerState{})
	vgirpc.RegisterStateType(&loggingProducerState{})
	vgirpc.RegisterStateType(&errorAfterNState{})
	vgirpc.RegisterStateType(&oversizedBatchState{})
	vgirpc.RegisterStateType(&oversizedExchangeState{})
	vgirpc.RegisterStateType(&headerProducerState{})
	vgirpc.RegisterStateType(&scaleExchangeState{})
	vgirpc.RegisterStateType(&accumulatingExchangeState{})
	vgirpc.RegisterStateType(&loggingExchangeState{})
	vgirpc.RegisterStateType(&failOnExchangeNState{})
	vgirpc.RegisterStateType(&dynamicProducerState{})
	vgirpc.RegisterStateType(&zeroColumnExchangeState{})
	vgirpc.RegisterStateType(&cancellableProducerState{})
	vgirpc.RegisterStateType(&cancellableExchangeState{})
	vgirpc.RegisterStateType(&sessionCounterProducerState{})
	vgirpc.RegisterStateType(&sessionCounterExchangeState{})
}

// --- Cancel probe (process-wide counters for cancel conformance tests) ---

var cancelProbeMu sync.Mutex
var cancelProduceCalls int64
var cancelExchangeCalls int64
var cancelOnCancelCalls int64

func resetCancelProbe() {
	cancelProbeMu.Lock()
	defer cancelProbeMu.Unlock()
	cancelProduceCalls = 0
	cancelExchangeCalls = 0
	cancelOnCancelCalls = 0
}

func readCancelProbe() (int64, int64, int64) {
	cancelProbeMu.Lock()
	defer cancelProbeMu.Unlock()
	return cancelProduceCalls, cancelExchangeCalls, cancelOnCancelCalls
}

// cancellableProducerState is an infinite producer that records cancel
// observations for the cancel conformance tests.
type cancellableProducerState struct {
	Current int
}

func (s *cancellableProducerState) Produce(_ context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	cancelProbeMu.Lock()
	cancelProduceCalls++
	cancelProbeMu.Unlock()
	if err := emitCounterBatch(out, int64(s.Current)); err != nil {
		return err
	}
	s.Current++
	return nil
}

func (s *cancellableProducerState) OnCancel(_ context.Context, callCtx *vgirpc.CallContext) error {
	cancelProbeMu.Lock()
	cancelOnCancelCalls++
	cancelProbeMu.Unlock()
	return nil
}

// sessionCounterProducerState emits the sticky-session counter
// ``count`` times. Each Produce call resolves the counter via
// ctx.Session(), increments it by one, and emits a one-row batch with
// the new value. Across HTTP turns ``Current`` rides in the
// continuation token while ctx.Session is rebound by the sticky
// middleware on every request — mirrors Python's
// SessionCounterProducerState in vgi_rpc/conformance/_types.py.
type sessionCounterProducerState struct {
	Count   int
	Current int
}

func (s *sessionCounterProducerState) Produce(_ context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	if s.Current >= s.Count {
		return out.Finish()
	}
	counter, ok := callCtx.Session().(*stickyCounter)
	if !ok {
		return &vgirpc.RpcError{Type: "RuntimeError", Message: "no sticky counter bound to this request"}
	}
	counter.value++
	if err := emitSessionCounterValue(out, counter.value); err != nil {
		return err
	}
	s.Current++
	return nil
}

// sessionCounterExchangeState is an exchange stream that, for every
// turn, sums the input ``by`` column into the bound _StickyCounter and
// emits the post-update value. Mirrors Python's
// SessionCounterExchangeState.
type sessionCounterExchangeState struct{}

func (s *sessionCounterExchangeState) Exchange(_ context.Context, input arrow.RecordBatch, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	counter, ok := callCtx.Session().(*stickyCounter)
	if !ok {
		return &vgirpc.RpcError{Type: "RuntimeError", Message: "no sticky counter bound to this request"}
	}
	col := input.Column(0).(*array.Int64)
	var sum int64
	for i := 0; i < col.Len(); i++ {
		if col.IsValid(i) {
			sum += col.Value(i)
		}
	}
	counter.value += sum
	return emitSessionCounterValue(out, counter.value)
}

// emitSessionCounterValue writes a one-row batch with the supplied
// value into out. Uses EmitArrays so the OutputCollector wraps the
// arrays under its own schema pointer (Arrow IPC requires identity
// match, not just structural equality — see CLAUDE.md).
func emitSessionCounterValue(out *vgirpc.OutputCollector, value int64) error {
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	b.Append(value)
	arr := b.NewArray()
	defer arr.Release()
	return out.EmitArrays([]arrow.Array{arr}, 1)
}

// cancellableExchangeState is an echo exchange that records cancel observations.
type cancellableExchangeState struct{}

func (s *cancellableExchangeState) Exchange(_ context.Context, input arrow.RecordBatch, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	cancelProbeMu.Lock()
	cancelExchangeCalls++
	cancelProbeMu.Unlock()
	return out.Emit(input)
}

func (s *cancellableExchangeState) OnCancel(_ context.Context, callCtx *vgirpc.CallContext) error {
	cancelProbeMu.Lock()
	cancelOnCancelCalls++
	cancelProbeMu.Unlock()
	return nil
}

// --- Producer States ---

// counterProducerState produces count batches with {index, value}.
type counterProducerState struct {
	Count   int
	Current int
}

func (s *counterProducerState) Produce(_ context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	if s.Current >= s.Count {
		return out.Finish()
	}
	if err := emitCounterBatch(out, int64(s.Current)); err != nil {
		return err
	}
	s.Current++
	return nil
}

// emptyProducerState finishes immediately — zero batches.
type emptyProducerState struct{}

func (s *emptyProducerState) Produce(_ context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	return out.Finish()
}

// singleProducerState emits exactly one batch, then finishes.
type singleProducerState struct {
	Emitted bool
}

func (s *singleProducerState) Produce(_ context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	if s.Emitted {
		return out.Finish()
	}
	s.Emitted = true
	return emitCounterBatch(out, 0)
}

// largeProducerState produces batch_count batches of rows_per_batch rows each.
type largeProducerState struct {
	RowsPerBatch int
	BatchCount   int
	Current      int
}

func (s *largeProducerState) Produce(_ context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
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

// oversizedBatchState emits one batch of RowsPerBatch int64 rows then
// finishes. Used by HTTP-only conformance tests to deliberately overshoot
// the configured response cap in a single producer iteration.
type oversizedBatchState struct {
	RowsPerBatch int
	Emitted      bool
}

func (s *oversizedBatchState) Produce(_ context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	if s.Emitted {
		return out.Finish()
	}
	mem := memory.NewGoAllocator()
	idxBuilder := array.NewInt64Builder(mem)
	valBuilder := array.NewInt64Builder(mem)
	defer idxBuilder.Release()
	defer valBuilder.Release()
	for i := int64(0); i < int64(s.RowsPerBatch); i++ {
		idxBuilder.Append(i)
		valBuilder.Append(i * 10)
	}
	idxArr := idxBuilder.NewArray()
	valArr := valBuilder.NewArray()
	defer idxArr.Release()
	defer valArr.Release()
	if err := out.EmitArrays([]arrow.Array{idxArr, valArr}, int64(s.RowsPerBatch)); err != nil {
		return err
	}
	s.Emitted = true
	return nil
}

// oversizedExchangeState emits a fixed-size oversized output batch for
// any input. Companion to oversizedBatchState for the lockstep exchange
// path.
type oversizedExchangeState struct {
	RowsPerBatch int
}

func (s *oversizedExchangeState) Exchange(_ context.Context, _ arrow.RecordBatch, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	mem := memory.NewGoAllocator()
	idxBuilder := array.NewInt64Builder(mem)
	valBuilder := array.NewInt64Builder(mem)
	defer idxBuilder.Release()
	defer valBuilder.Release()
	for i := int64(0); i < int64(s.RowsPerBatch); i++ {
		idxBuilder.Append(i)
		valBuilder.Append(i * 10)
	}
	idxArr := idxBuilder.NewArray()
	valArr := valBuilder.NewArray()
	defer idxArr.Release()
	defer valArr.Release()
	return out.EmitArrays([]arrow.Array{idxArr, valArr}, int64(s.RowsPerBatch))
}

// loggingProducerState produces batches with an INFO log before each.
type loggingProducerState struct {
	Count   int
	Current int
}

func (s *loggingProducerState) Produce(_ context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
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

// errorAfterNState raises after emitting emit_before_error batches.
type errorAfterNState struct {
	EmitBeforeError int
	Current         int
}

func (s *errorAfterNState) Produce(_ context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	if s.Current >= s.EmitBeforeError {
		return &vgirpc.RpcError{Type: "RuntimeError", Message: fmt.Sprintf("intentional error after %d batches", s.EmitBeforeError)}
	}
	if err := emitCounterBatch(out, int64(s.Current)); err != nil {
		return err
	}
	s.Current++
	return nil
}

// headerProducerState is used with stream headers — same as counterProducerState.
type headerProducerState struct {
	Count   int
	Current int
}

func (s *headerProducerState) Produce(_ context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	if s.Current >= s.Count {
		return out.Finish()
	}
	if err := emitCounterBatch(out, int64(s.Current)); err != nil {
		return err
	}
	s.Current++
	return nil
}

// dynamicProducerState produces batches with a schema that varies based on boolean flags.
type dynamicProducerState struct {
	Count          int
	IncludeStrings bool
	IncludeFloats  bool
	Current        int
}

func (s *dynamicProducerState) Produce(_ context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	if s.Current >= s.Count {
		return out.Finish()
	}
	mem := memory.NewGoAllocator()

	idxBuilder := array.NewInt64Builder(mem)
	defer idxBuilder.Release()
	idxBuilder.Append(int64(s.Current))
	idxArr := idxBuilder.NewArray()
	defer idxArr.Release()

	arrays := []arrow.Array{idxArr}

	if s.IncludeStrings {
		lblBuilder := array.NewStringBuilder(mem)
		defer lblBuilder.Release()
		lblBuilder.Append(fmt.Sprintf("row-%d", s.Current))
		lblArr := lblBuilder.NewArray()
		defer lblArr.Release()
		arrays = append(arrays, lblArr)
	}
	if s.IncludeFloats {
		scoreBuilder := array.NewFloat64Builder(mem)
		defer scoreBuilder.Release()
		scoreBuilder.Append(float64(s.Current) * 1.5)
		scoreArr := scoreBuilder.NewArray()
		defer scoreArr.Release()
		arrays = append(arrays, scoreArr)
	}

	if err := out.EmitArrays(arrays, 1); err != nil {
		return err
	}
	s.Current++
	return nil
}

// --- Exchange States ---

// scaleExchangeState multiplies the value column by factor.
type scaleExchangeState struct {
	Factor float64
}

func (s *scaleExchangeState) Exchange(_ context.Context, input arrow.RecordBatch, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
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

// accumulatingExchangeState maintains running sum and exchange count.
type accumulatingExchangeState struct {
	RunningSum    float64
	ExchangeCount int64
}

func (s *accumulatingExchangeState) Exchange(_ context.Context, input arrow.RecordBatch, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
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

// loggingExchangeState logs INFO + DEBUG per exchange, then echoes input.
type loggingExchangeState struct{}

func (s *loggingExchangeState) Exchange(_ context.Context, input arrow.RecordBatch, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	out.ClientLog(vgirpc.LogInfo, "exchange processing")
	out.ClientLog(vgirpc.LogDebug, "exchange debug")
	return out.Emit(input)
}

// failOnExchangeNState raises on the Nth exchange (1-indexed).
type failOnExchangeNState struct {
	FailOn        int
	ExchangeCount int
}

func (s *failOnExchangeNState) Exchange(_ context.Context, input arrow.RecordBatch, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	s.ExchangeCount++
	if s.ExchangeCount >= s.FailOn {
		return &vgirpc.RpcError{Type: "RuntimeError", Message: fmt.Sprintf("intentional error on exchange %d", s.ExchangeCount)}
	}
	return out.Emit(input)
}

// zeroColumnExchangeState accepts zero-column batches and emits zero-column batches.
type zeroColumnExchangeState struct {
	CallCount int
}

func (s *zeroColumnExchangeState) Exchange(_ context.Context, input arrow.RecordBatch, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	s.CallCount++
	return out.EmitArrays([]arrow.Array{}, 0)
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

var scaleInputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "value", Type: arrow.PrimitiveTypes.Float64},
}, nil)

var scaleOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "value", Type: arrow.PrimitiveTypes.Float64},
}, nil)

var accumInputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "value", Type: arrow.PrimitiveTypes.Float64},
}, nil)

var accumOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "running_sum", Type: arrow.PrimitiveTypes.Float64},
	{Name: "exchange_count", Type: arrow.PrimitiveTypes.Int64},
}, nil)
