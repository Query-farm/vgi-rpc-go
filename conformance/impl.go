// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"context"
	"fmt"
	"strconv"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// --- Parameter structs for each method ---

type EchoStringParams struct {
	Value string `vgirpc:"value"`
}
type EchoBytesParams struct {
	Data []byte `vgirpc:"data"`
}
type EchoIntParams struct {
	Value int64 `vgirpc:"value"`
}
type EchoFloatParams struct {
	Value float64 `vgirpc:"value"`
}
type EchoBoolParams struct {
	Value bool `vgirpc:"value"`
}
type VoidNoopParams struct{}
type VoidWithParamParams struct {
	Value int64 `vgirpc:"value"`
}
type EchoEnumParams struct {
	Status Status `vgirpc:"status,enum"`
}
type EchoListParams struct {
	Values []string `vgirpc:"values"`
}
type EchoDictParams struct {
	Mapping map[string]int64 `vgirpc:"mapping"`
}
type EchoNestedListParams struct {
	Matrix [][]int64 `vgirpc:"matrix"`
}
type EchoOptionalStringParams struct {
	Value *string `vgirpc:"value"`
}
type EchoOptionalIntParams struct {
	Value *int64 `vgirpc:"value"`
}
type EchoPointParams struct {
	Point Point `vgirpc:"point,binary"`
}
type EchoAllTypesParams struct {
	Data AllTypes `vgirpc:"data,binary"`
}
type EchoBoundingBoxParams struct {
	Box BoundingBox `vgirpc:"box,binary"`
}
type InspectPointParams struct {
	Point Point `vgirpc:"point,binary"`
}
type EchoInt32Params struct {
	Value int64 `vgirpc:"value,int32"`
}
type EchoFloat32Params struct {
	Value float64 `vgirpc:"value,float32"`
}
type AddFloatsParams struct {
	A float64 `vgirpc:"a"`
	B float64 `vgirpc:"b"`
}
type ConcatenateParams struct {
	Prefix    string `vgirpc:"prefix"`
	Suffix    string `vgirpc:"suffix"`
	Separator string `vgirpc:"separator,default=-"`
}
type WithDefaultsParams struct {
	Required    int64  `vgirpc:"required"`
	OptionalStr string `vgirpc:"optional_str,default=default"`
	OptionalInt int64  `vgirpc:"optional_int,default=42"`
}
type RaiseErrorParams struct {
	Message string `vgirpc:"message"`
}
type EchoWithLogParams struct {
	Value string `vgirpc:"value"`
}

// RegisterMethods registers all conformance methods on the server.
func RegisterMethods(server *vgirpc.Server) {
	// Scalar echo methods
	vgirpc.Unary(server, "echo_string", echoString)
	vgirpc.Unary(server, "echo_bytes", echoBytes)
	vgirpc.Unary(server, "echo_int", echoInt)
	vgirpc.Unary(server, "echo_float", echoFloat)
	vgirpc.Unary(server, "echo_bool", echoBool)

	// Void returns
	vgirpc.UnaryVoid(server, "void_noop", voidNoop)
	vgirpc.UnaryVoid(server, "void_with_param", voidWithParam)

	// Complex type echo
	vgirpc.Unary(server, "echo_enum", echoEnum)
	vgirpc.Unary(server, "echo_list", echoList)
	vgirpc.Unary(server, "echo_dict", echoDict)
	vgirpc.Unary(server, "echo_nested_list", echoNestedList)

	// Optional/nullable
	vgirpc.Unary(server, "echo_optional_string", echoOptionalString)
	vgirpc.Unary(server, "echo_optional_int", echoOptionalInt)

	// Dataclass round-trip
	vgirpc.Unary(server, "echo_point", echoPoint)
	vgirpc.Unary(server, "echo_all_types", echoAllTypes)
	vgirpc.Unary(server, "echo_bounding_box", echoBoundingBox)

	// Dataclass as parameter
	vgirpc.Unary(server, "inspect_point", inspectPoint)

	// Annotated types
	vgirpc.Unary(server, "echo_int32", echoInt32)
	vgirpc.Unary(server, "echo_float32", echoFloat32)

	// Multi-param & defaults
	vgirpc.Unary(server, "add_floats", addFloats)
	vgirpc.Unary(server, "concatenate", concatenate)
	vgirpc.Unary(server, "with_defaults", withDefaults)

	// Error propagation
	vgirpc.Unary(server, "raise_value_error", raiseValueError)
	vgirpc.Unary(server, "raise_runtime_error", raiseRuntimeError)
	vgirpc.Unary(server, "raise_type_error", raiseTypeError)

	// Client-directed logging
	vgirpc.Unary(server, "echo_with_info_log", echoWithInfoLog)
	vgirpc.Unary(server, "echo_with_multi_logs", echoWithMultiLogs)
	vgirpc.Unary(server, "echo_with_log_extras", echoWithLogExtras)

	// Producer streams
	vgirpc.Producer(server, "produce_n", CounterSchema, produceN)
	vgirpc.Producer(server, "produce_empty", CounterSchema, produceEmpty)
	vgirpc.Producer(server, "produce_single", CounterSchema, produceSingle)
	vgirpc.Producer(server, "produce_large_batches", CounterSchema, produceLargeBatches)
	vgirpc.Producer(server, "produce_with_logs", CounterSchema, produceWithLogs)
	vgirpc.Producer(server, "produce_error_mid_stream", CounterSchema, produceErrorMidStream)
	vgirpc.Producer(server, "produce_error_on_init", CounterSchema, produceErrorOnInit)

	// Producer streams with headers
	headerSchema := ConformanceHeader{}.ArrowSchema()
	vgirpc.ProducerWithHeader(server, "produce_with_header", CounterSchema, headerSchema, produceWithHeader)
	vgirpc.ProducerWithHeader(server, "produce_with_header_and_logs", CounterSchema, headerSchema, produceWithHeaderAndLogs)

	// Exchange streams
	vgirpc.Exchange(server, "exchange_scale", ScaleOutputSchema, ScaleInputSchema, exchangeScale)
	vgirpc.Exchange(server, "exchange_accumulate", AccumOutputSchema, AccumInputSchema, exchangeAccumulate)
	vgirpc.Exchange(server, "exchange_with_logs", ScaleOutputSchema, ScaleInputSchema, exchangeWithLogs)
	vgirpc.Exchange(server, "exchange_error_on_nth", ScaleOutputSchema, ScaleInputSchema, exchangeErrorOnNth)
	vgirpc.Exchange(server, "exchange_error_on_init", ScaleOutputSchema, ScaleInputSchema, exchangeErrorOnInit)

	// Exchange streams with headers
	vgirpc.ExchangeWithHeader(server, "exchange_with_header", ScaleOutputSchema, ScaleInputSchema, headerSchema, exchangeWithHeader)
}

// --- Producer stream parameter structs ---

type ProduceNParams struct {
	Count int64 `vgirpc:"count"`
}
type ProduceEmptyParams struct{}
type ProduceSingleParams struct{}
type ProduceLargeBatchesParams struct {
	RowsPerBatch int64 `vgirpc:"rows_per_batch"`
	BatchCount   int64 `vgirpc:"batch_count"`
}
type ProduceWithLogsParams struct {
	Count int64 `vgirpc:"count"`
}
type ProduceErrorMidStreamParams struct {
	EmitBeforeError int64 `vgirpc:"emit_before_error"`
}
type ProduceErrorOnInitParams struct{}
type ProduceWithHeaderParams struct {
	Count int64 `vgirpc:"count"`
}
type ProduceWithHeaderAndLogsParams struct {
	Count int64 `vgirpc:"count"`
}

// --- Exchange stream parameter structs ---

type ExchangeScaleParams struct {
	Factor float64 `vgirpc:"factor"`
}
type ExchangeAccumulateParams struct{}
type ExchangeWithLogsParams struct{}
type ExchangeErrorOnNthParams struct {
	FailOn int64 `vgirpc:"fail_on"`
}
type ExchangeErrorOnInitParams struct{}
type ExchangeWithHeaderParams struct {
	Factor float64 `vgirpc:"factor"`
}

// formatFloat formats a float64 matching Python's default str(float) behavior.
func formatFloat(f float64) string {
	s := strconv.FormatFloat(f, 'f', -1, 64)
	// Ensure at least one decimal place (Python always shows .0 for whole floats)
	if !containsDot(s) {
		s += ".0"
	}
	return s
}

func containsDot(s string) bool {
	for _, c := range s {
		if c == '.' {
			return true
		}
	}
	return false
}

// --- Scalar echo ---

func echoString(_ context.Context, ctx *vgirpc.CallContext, p EchoStringParams) (string, error) {
	return p.Value, nil
}
func echoBytes(_ context.Context, ctx *vgirpc.CallContext, p EchoBytesParams) ([]byte, error) {
	return p.Data, nil
}
func echoInt(_ context.Context, ctx *vgirpc.CallContext, p EchoIntParams) (int64, error) {
	return p.Value, nil
}
func echoFloat(_ context.Context, ctx *vgirpc.CallContext, p EchoFloatParams) (float64, error) {
	return p.Value, nil
}
func echoBool(_ context.Context, ctx *vgirpc.CallContext, p EchoBoolParams) (bool, error) {
	return p.Value, nil
}

// --- Void ---

func voidNoop(_ context.Context, ctx *vgirpc.CallContext, _ VoidNoopParams) error {
	return nil
}
func voidWithParam(_ context.Context, ctx *vgirpc.CallContext, _ VoidWithParamParams) error {
	return nil
}

// --- Complex type echo ---

func echoEnum(_ context.Context, ctx *vgirpc.CallContext, p EchoEnumParams) (Status, error) {
	return p.Status, nil
}
func echoList(_ context.Context, ctx *vgirpc.CallContext, p EchoListParams) ([]string, error) {
	return p.Values, nil
}
func echoDict(_ context.Context, ctx *vgirpc.CallContext, p EchoDictParams) (map[string]int64, error) {
	return p.Mapping, nil
}
func echoNestedList(_ context.Context, ctx *vgirpc.CallContext, p EchoNestedListParams) ([][]int64, error) {
	return p.Matrix, nil
}

// --- Optional/nullable ---

func echoOptionalString(_ context.Context, ctx *vgirpc.CallContext, p EchoOptionalStringParams) (*string, error) {
	return p.Value, nil
}
func echoOptionalInt(_ context.Context, ctx *vgirpc.CallContext, p EchoOptionalIntParams) (*int64, error) {
	return p.Value, nil
}

// --- Dataclass round-trip ---

func echoPoint(_ context.Context, ctx *vgirpc.CallContext, p EchoPointParams) (Point, error) {
	return p.Point, nil
}
func echoAllTypes(_ context.Context, ctx *vgirpc.CallContext, p EchoAllTypesParams) (AllTypes, error) {
	return p.Data, nil
}
func echoBoundingBox(_ context.Context, ctx *vgirpc.CallContext, p EchoBoundingBoxParams) (BoundingBox, error) {
	return p.Box, nil
}

// --- Dataclass as parameter ---

func inspectPoint(_ context.Context, ctx *vgirpc.CallContext, p InspectPointParams) (string, error) {
	return fmt.Sprintf("Point(%g, %g)", p.Point.X, p.Point.Y), nil
}

// --- Annotated types ---

func echoInt32(_ context.Context, ctx *vgirpc.CallContext, p EchoInt32Params) (int64, error) {
	return p.Value, nil
}
func echoFloat32(_ context.Context, ctx *vgirpc.CallContext, p EchoFloat32Params) (float64, error) {
	return p.Value, nil
}

// --- Multi-param & defaults ---

func addFloats(_ context.Context, ctx *vgirpc.CallContext, p AddFloatsParams) (float64, error) {
	return p.A + p.B, nil
}
func concatenate(_ context.Context, ctx *vgirpc.CallContext, p ConcatenateParams) (string, error) {
	return p.Prefix + p.Separator + p.Suffix, nil
}
func withDefaults(_ context.Context, ctx *vgirpc.CallContext, p WithDefaultsParams) (string, error) {
	return fmt.Sprintf("required=%d, optional_str=%s, optional_int=%d",
		p.Required, p.OptionalStr, p.OptionalInt), nil
}

// --- Error propagation ---

func raiseValueError(_ context.Context, ctx *vgirpc.CallContext, p RaiseErrorParams) (string, error) {
	return "", &vgirpc.RpcError{Type: "ValueError", Message: p.Message}
}
func raiseRuntimeError(_ context.Context, ctx *vgirpc.CallContext, p RaiseErrorParams) (string, error) {
	return "", &vgirpc.RpcError{Type: "RuntimeError", Message: p.Message}
}
func raiseTypeError(_ context.Context, ctx *vgirpc.CallContext, p RaiseErrorParams) (string, error) {
	return "", &vgirpc.RpcError{Type: "TypeError", Message: p.Message}
}

// --- Client-directed logging ---

func echoWithInfoLog(_ context.Context, ctx *vgirpc.CallContext, p EchoWithLogParams) (string, error) {
	ctx.ClientLog(vgirpc.LogInfo, fmt.Sprintf("info: %s", p.Value))
	return p.Value, nil
}
func echoWithMultiLogs(_ context.Context, ctx *vgirpc.CallContext, p EchoWithLogParams) (string, error) {
	ctx.ClientLog(vgirpc.LogDebug, fmt.Sprintf("debug: %s", p.Value))
	ctx.ClientLog(vgirpc.LogInfo, fmt.Sprintf("info: %s", p.Value))
	ctx.ClientLog(vgirpc.LogWarn, fmt.Sprintf("warn: %s", p.Value))
	return p.Value, nil
}
func echoWithLogExtras(_ context.Context, ctx *vgirpc.CallContext, p EchoWithLogParams) (string, error) {
	ctx.ClientLog(vgirpc.LogInfo, "echo_with_extras",
		vgirpc.KV{Key: "source", Value: "conformance"},
		vgirpc.KV{Key: "detail", Value: p.Value},
	)
	return p.Value, nil
}

// --- Producer stream handlers ---

func produceN(_ context.Context, ctx *vgirpc.CallContext, p ProduceNParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: CounterSchema,
		State:        &CounterProducerState{Count: int(p.Count)},
	}, nil
}

func produceEmpty(_ context.Context, ctx *vgirpc.CallContext, _ ProduceEmptyParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: CounterSchema,
		State:        &EmptyProducerState{},
	}, nil
}

func produceSingle(_ context.Context, ctx *vgirpc.CallContext, _ ProduceSingleParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: CounterSchema,
		State:        &SingleProducerState{},
	}, nil
}

func produceLargeBatches(_ context.Context, ctx *vgirpc.CallContext, p ProduceLargeBatchesParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: CounterSchema,
		State:        &LargeProducerState{RowsPerBatch: int(p.RowsPerBatch), BatchCount: int(p.BatchCount)},
	}, nil
}

func produceWithLogs(_ context.Context, ctx *vgirpc.CallContext, p ProduceWithLogsParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: CounterSchema,
		State:        &LoggingProducerState{Count: int(p.Count)},
	}, nil
}

func produceErrorMidStream(_ context.Context, ctx *vgirpc.CallContext, p ProduceErrorMidStreamParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: CounterSchema,
		State:        &ErrorAfterNState{EmitBeforeError: int(p.EmitBeforeError)},
	}, nil
}

func produceErrorOnInit(_ context.Context, ctx *vgirpc.CallContext, _ ProduceErrorOnInitParams) (*vgirpc.StreamResult, error) {
	return nil, &vgirpc.RpcError{Type: "RuntimeError", Message: "intentional init error"}
}

func produceWithHeader(_ context.Context, ctx *vgirpc.CallContext, p ProduceWithHeaderParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: CounterSchema,
		State:        &HeaderProducerState{Count: int(p.Count)},
		Header:       ConformanceHeader{TotalExpected: p.Count, Description: fmt.Sprintf("producing %d batches", p.Count)},
	}, nil
}

func produceWithHeaderAndLogs(_ context.Context, ctx *vgirpc.CallContext, p ProduceWithHeaderAndLogsParams) (*vgirpc.StreamResult, error) {
	ctx.ClientLog(vgirpc.LogInfo, "stream init log")
	return &vgirpc.StreamResult{
		OutputSchema: CounterSchema,
		State:        &HeaderProducerState{Count: int(p.Count)},
		Header:       ConformanceHeader{TotalExpected: p.Count, Description: fmt.Sprintf("producing %d with logs", p.Count)},
	}, nil
}

// --- Exchange stream handlers ---

func exchangeScale(_ context.Context, ctx *vgirpc.CallContext, p ExchangeScaleParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: ScaleOutputSchema,
		InputSchema:  ScaleInputSchema,
		State:        &ScaleExchangeState{Factor: p.Factor},
	}, nil
}

func exchangeAccumulate(_ context.Context, ctx *vgirpc.CallContext, _ ExchangeAccumulateParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: AccumOutputSchema,
		InputSchema:  AccumInputSchema,
		State:        &AccumulatingExchangeState{},
	}, nil
}

func exchangeWithLogs(_ context.Context, ctx *vgirpc.CallContext, _ ExchangeWithLogsParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: ScaleOutputSchema,
		InputSchema:  ScaleInputSchema,
		State:        &LoggingExchangeState{},
	}, nil
}

func exchangeErrorOnNth(_ context.Context, ctx *vgirpc.CallContext, p ExchangeErrorOnNthParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: ScaleOutputSchema,
		InputSchema:  ScaleInputSchema,
		State:        &FailOnExchangeNState{FailOn: int(p.FailOn)},
	}, nil
}

func exchangeErrorOnInit(_ context.Context, ctx *vgirpc.CallContext, _ ExchangeErrorOnInitParams) (*vgirpc.StreamResult, error) {
	return nil, &vgirpc.RpcError{Type: "RuntimeError", Message: "intentional exchange init error"}
}

func exchangeWithHeader(_ context.Context, ctx *vgirpc.CallContext, p ExchangeWithHeaderParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: ScaleOutputSchema,
		InputSchema:  ScaleInputSchema,
		State:        &ScaleExchangeState{Factor: p.Factor},
		Header:       ConformanceHeader{TotalExpected: 0, Description: "scale by " + formatFloat(p.Factor)},
	}, nil
}
