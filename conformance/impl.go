// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"context"
	"fmt"
	"strconv"

	"github.com/Query-farm/vgi-rpc/vgirpc"
)

// --- Parameter structs for each method ---

type echoStringParams struct {
	Value string `vgirpc:"value"`
}
type echoBytesParams struct {
	Data []byte `vgirpc:"data"`
}
type echoIntParams struct {
	Value int64 `vgirpc:"value"`
}
type echoFloatParams struct {
	Value float64 `vgirpc:"value"`
}
type echoBoolParams struct {
	Value bool `vgirpc:"value"`
}
type voidNoopParams struct{}
type voidWithParamParams struct {
	Value int64 `vgirpc:"value"`
}
type echoEnumParams struct {
	Status Status `vgirpc:"status,enum"`
}
type echoListParams struct {
	Values []string `vgirpc:"values"`
}
type echoDictParams struct {
	Mapping map[string]int64 `vgirpc:"mapping"`
}
type echoNestedListParams struct {
	Matrix [][]int64 `vgirpc:"matrix"`
}
type echoOptionalStringParams struct {
	Value *string `vgirpc:"value"`
}
type echoOptionalIntParams struct {
	Value *int64 `vgirpc:"value"`
}
type echoPointParams struct {
	Point Point `vgirpc:"point,binary"`
}
type echoAllTypesParams struct {
	Data AllTypes `vgirpc:"data,binary"`
}
type echoBoundingBoxParams struct {
	Box BoundingBox `vgirpc:"box,binary"`
}
type inspectPointParams struct {
	Point Point `vgirpc:"point,binary"`
}
type echoInt32Params struct {
	Value int64 `vgirpc:"value,int32"`
}
type echoFloat32Params struct {
	Value float64 `vgirpc:"value,float32"`
}
type addFloatsParams struct {
	A float64 `vgirpc:"a"`
	B float64 `vgirpc:"b"`
}
type concatenateParams struct {
	Prefix    string `vgirpc:"prefix"`
	Suffix    string `vgirpc:"suffix"`
	Separator string `vgirpc:"separator,default=-"`
}
type withDefaultsParams struct {
	Required    int64  `vgirpc:"required"`
	OptionalStr string `vgirpc:"optional_str,default=default"`
	OptionalInt int64  `vgirpc:"optional_int,default=42"`
}
type raiseErrorParams struct {
	Message string `vgirpc:"message"`
}
type echoWithLogParams struct {
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
	vgirpc.Producer(server, "produce_n", counterSchema, produceN)
	vgirpc.Producer(server, "produce_empty", counterSchema, produceEmpty)
	vgirpc.Producer(server, "produce_single", counterSchema, produceSingle)
	vgirpc.Producer(server, "produce_large_batches", counterSchema, produceLargeBatches)
	vgirpc.Producer(server, "produce_with_logs", counterSchema, produceWithLogs)
	vgirpc.Producer(server, "produce_error_mid_stream", counterSchema, produceErrorMidStream)
	vgirpc.Producer(server, "produce_error_on_init", counterSchema, produceErrorOnInit)

	// Producer streams with headers
	headerSchema := ConformanceHeader{}.ArrowSchema()
	vgirpc.ProducerWithHeader(server, "produce_with_header", counterSchema, headerSchema, produceWithHeader)
	vgirpc.ProducerWithHeader(server, "produce_with_header_and_logs", counterSchema, headerSchema, produceWithHeaderAndLogs)

	// Exchange streams
	vgirpc.Exchange(server, "exchange_scale", scaleOutputSchema, scaleInputSchema, exchangeScale)
	vgirpc.Exchange(server, "exchange_accumulate", accumOutputSchema, accumInputSchema, exchangeAccumulate)
	vgirpc.Exchange(server, "exchange_with_logs", scaleOutputSchema, scaleInputSchema, exchangeWithLogs)
	vgirpc.Exchange(server, "exchange_error_on_nth", scaleOutputSchema, scaleInputSchema, exchangeErrorOnNth)
	vgirpc.Exchange(server, "exchange_error_on_init", scaleOutputSchema, scaleInputSchema, exchangeErrorOnInit)

	// Exchange streams with headers
	vgirpc.ExchangeWithHeader(server, "exchange_with_header", scaleOutputSchema, scaleInputSchema, headerSchema, exchangeWithHeader)

	// Rich header and dynamic schema methods
	richHeaderSchema := RichHeader{}.ArrowSchema()
	vgirpc.ProducerWithHeader(server, "produce_with_rich_header", counterSchema, richHeaderSchema, produceWithRichHeader)
	vgirpc.ExchangeWithHeader(server, "exchange_with_rich_header", scaleOutputSchema, scaleInputSchema, richHeaderSchema, exchangeWithRichHeader)
	vgirpc.DynamicStreamWithHeader(server, "produce_dynamic_schema", richHeaderSchema, produceDynamicSchema)
}

// --- Producer stream parameter structs ---

type produceNParams struct {
	Count int64 `vgirpc:"count"`
}
type produceEmptyParams struct{}
type produceSingleParams struct{}
type produceLargeBatchesParams struct {
	RowsPerBatch int64 `vgirpc:"rows_per_batch"`
	BatchCount   int64 `vgirpc:"batch_count"`
}
type produceWithLogsParams struct {
	Count int64 `vgirpc:"count"`
}
type produceErrorMidStreamParams struct {
	EmitBeforeError int64 `vgirpc:"emit_before_error"`
}
type produceErrorOnInitParams struct{}
type produceWithHeaderParams struct {
	Count int64 `vgirpc:"count"`
}
type produceWithHeaderAndLogsParams struct {
	Count int64 `vgirpc:"count"`
}

// --- Exchange stream parameter structs ---

type exchangeScaleParams struct {
	Factor float64 `vgirpc:"factor"`
}
type exchangeAccumulateParams struct{}
type exchangeWithLogsParams struct{}
type exchangeErrorOnNthParams struct {
	FailOn int64 `vgirpc:"fail_on"`
}
type exchangeErrorOnInitParams struct{}
type exchangeWithHeaderParams struct {
	Factor float64 `vgirpc:"factor"`
}

// --- Rich header and dynamic schema parameter structs ---

type produceWithRichHeaderParams struct {
	Seed  int64 `vgirpc:"seed"`
	Count int64 `vgirpc:"count"`
}
type exchangeWithRichHeaderParams struct {
	Seed   int64   `vgirpc:"seed"`
	Factor float64 `vgirpc:"factor"`
}
type produceDynamicSchemaParams struct {
	Seed           int64 `vgirpc:"seed"`
	Count          int64 `vgirpc:"count"`
	IncludeStrings bool  `vgirpc:"include_strings"`
	IncludeFloats  bool  `vgirpc:"include_floats"`
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

func echoString(_ context.Context, ctx *vgirpc.CallContext, p echoStringParams) (string, error) {
	return p.Value, nil
}
func echoBytes(_ context.Context, ctx *vgirpc.CallContext, p echoBytesParams) ([]byte, error) {
	return p.Data, nil
}
func echoInt(_ context.Context, ctx *vgirpc.CallContext, p echoIntParams) (int64, error) {
	return p.Value, nil
}
func echoFloat(_ context.Context, ctx *vgirpc.CallContext, p echoFloatParams) (float64, error) {
	return p.Value, nil
}
func echoBool(_ context.Context, ctx *vgirpc.CallContext, p echoBoolParams) (bool, error) {
	return p.Value, nil
}

// --- Void ---

func voidNoop(_ context.Context, ctx *vgirpc.CallContext, _ voidNoopParams) error {
	return nil
}
func voidWithParam(_ context.Context, ctx *vgirpc.CallContext, _ voidWithParamParams) error {
	return nil
}

// --- Complex type echo ---

func echoEnum(_ context.Context, ctx *vgirpc.CallContext, p echoEnumParams) (Status, error) {
	return p.Status, nil
}
func echoList(_ context.Context, ctx *vgirpc.CallContext, p echoListParams) ([]string, error) {
	return p.Values, nil
}
func echoDict(_ context.Context, ctx *vgirpc.CallContext, p echoDictParams) (map[string]int64, error) {
	return p.Mapping, nil
}
func echoNestedList(_ context.Context, ctx *vgirpc.CallContext, p echoNestedListParams) ([][]int64, error) {
	return p.Matrix, nil
}

// --- Optional/nullable ---

func echoOptionalString(_ context.Context, ctx *vgirpc.CallContext, p echoOptionalStringParams) (*string, error) {
	return p.Value, nil
}
func echoOptionalInt(_ context.Context, ctx *vgirpc.CallContext, p echoOptionalIntParams) (*int64, error) {
	return p.Value, nil
}

// --- Dataclass round-trip ---

func echoPoint(_ context.Context, ctx *vgirpc.CallContext, p echoPointParams) (Point, error) {
	return p.Point, nil
}
func echoAllTypes(_ context.Context, ctx *vgirpc.CallContext, p echoAllTypesParams) (AllTypes, error) {
	return p.Data, nil
}
func echoBoundingBox(_ context.Context, ctx *vgirpc.CallContext, p echoBoundingBoxParams) (BoundingBox, error) {
	return p.Box, nil
}

// --- Dataclass as parameter ---

func inspectPoint(_ context.Context, ctx *vgirpc.CallContext, p inspectPointParams) (string, error) {
	return fmt.Sprintf("Point(%s, %s)", formatFloat(p.Point.X), formatFloat(p.Point.Y)), nil
}

// --- Annotated types ---

func echoInt32(_ context.Context, ctx *vgirpc.CallContext, p echoInt32Params) (int64, error) {
	return p.Value, nil
}
func echoFloat32(_ context.Context, ctx *vgirpc.CallContext, p echoFloat32Params) (float64, error) {
	return p.Value, nil
}

// --- Multi-param & defaults ---

func addFloats(_ context.Context, ctx *vgirpc.CallContext, p addFloatsParams) (float64, error) {
	return p.A + p.B, nil
}
func concatenate(_ context.Context, ctx *vgirpc.CallContext, p concatenateParams) (string, error) {
	return p.Prefix + p.Separator + p.Suffix, nil
}
func withDefaults(_ context.Context, ctx *vgirpc.CallContext, p withDefaultsParams) (string, error) {
	return fmt.Sprintf("required=%d, optional_str=%s, optional_int=%d",
		p.Required, p.OptionalStr, p.OptionalInt), nil
}

// --- Error propagation ---

func raiseValueError(_ context.Context, ctx *vgirpc.CallContext, p raiseErrorParams) (string, error) {
	return "", &vgirpc.RpcError{Type: "ValueError", Message: p.Message}
}
func raiseRuntimeError(_ context.Context, ctx *vgirpc.CallContext, p raiseErrorParams) (string, error) {
	return "", &vgirpc.RpcError{Type: "RuntimeError", Message: p.Message}
}
func raiseTypeError(_ context.Context, ctx *vgirpc.CallContext, p raiseErrorParams) (string, error) {
	return "", &vgirpc.RpcError{Type: "TypeError", Message: p.Message}
}

// --- Client-directed logging ---

func echoWithInfoLog(_ context.Context, ctx *vgirpc.CallContext, p echoWithLogParams) (string, error) {
	ctx.ClientLog(vgirpc.LogInfo, fmt.Sprintf("info: %s", p.Value))
	return p.Value, nil
}
func echoWithMultiLogs(_ context.Context, ctx *vgirpc.CallContext, p echoWithLogParams) (string, error) {
	ctx.ClientLog(vgirpc.LogDebug, fmt.Sprintf("debug: %s", p.Value))
	ctx.ClientLog(vgirpc.LogInfo, fmt.Sprintf("info: %s", p.Value))
	ctx.ClientLog(vgirpc.LogWarn, fmt.Sprintf("warn: %s", p.Value))
	return p.Value, nil
}
func echoWithLogExtras(_ context.Context, ctx *vgirpc.CallContext, p echoWithLogParams) (string, error) {
	ctx.ClientLog(vgirpc.LogInfo, "echo_with_extras",
		vgirpc.KV{Key: "source", Value: "conformance"},
		vgirpc.KV{Key: "detail", Value: p.Value},
	)
	return p.Value, nil
}

// --- Producer stream handlers ---

func produceN(_ context.Context, ctx *vgirpc.CallContext, p produceNParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: counterSchema,
		State:        &counterProducerState{Count: int(p.Count)},
	}, nil
}

func produceEmpty(_ context.Context, ctx *vgirpc.CallContext, _ produceEmptyParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: counterSchema,
		State:        &emptyProducerState{},
	}, nil
}

func produceSingle(_ context.Context, ctx *vgirpc.CallContext, _ produceSingleParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: counterSchema,
		State:        &singleProducerState{},
	}, nil
}

func produceLargeBatches(_ context.Context, ctx *vgirpc.CallContext, p produceLargeBatchesParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: counterSchema,
		State:        &largeProducerState{RowsPerBatch: int(p.RowsPerBatch), BatchCount: int(p.BatchCount)},
	}, nil
}

func produceWithLogs(_ context.Context, ctx *vgirpc.CallContext, p produceWithLogsParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: counterSchema,
		State:        &loggingProducerState{Count: int(p.Count)},
	}, nil
}

func produceErrorMidStream(_ context.Context, ctx *vgirpc.CallContext, p produceErrorMidStreamParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: counterSchema,
		State:        &errorAfterNState{EmitBeforeError: int(p.EmitBeforeError)},
	}, nil
}

func produceErrorOnInit(_ context.Context, ctx *vgirpc.CallContext, _ produceErrorOnInitParams) (*vgirpc.StreamResult, error) {
	return nil, &vgirpc.RpcError{Type: "RuntimeError", Message: "intentional init error"}
}

func produceWithHeader(_ context.Context, ctx *vgirpc.CallContext, p produceWithHeaderParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: counterSchema,
		State:        &headerProducerState{Count: int(p.Count)},
		Header:       ConformanceHeader{TotalExpected: p.Count, Description: fmt.Sprintf("producing %d batches", p.Count)},
	}, nil
}

func produceWithHeaderAndLogs(_ context.Context, ctx *vgirpc.CallContext, p produceWithHeaderAndLogsParams) (*vgirpc.StreamResult, error) {
	ctx.ClientLog(vgirpc.LogInfo, "stream init log")
	return &vgirpc.StreamResult{
		OutputSchema: counterSchema,
		State:        &headerProducerState{Count: int(p.Count)},
		Header:       ConformanceHeader{TotalExpected: p.Count, Description: fmt.Sprintf("producing %d with logs", p.Count)},
	}, nil
}

// --- Exchange stream handlers ---

func exchangeScale(_ context.Context, ctx *vgirpc.CallContext, p exchangeScaleParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: scaleOutputSchema,
		InputSchema:  scaleInputSchema,
		State:        &scaleExchangeState{Factor: p.Factor},
	}, nil
}

func exchangeAccumulate(_ context.Context, ctx *vgirpc.CallContext, _ exchangeAccumulateParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: accumOutputSchema,
		InputSchema:  accumInputSchema,
		State:        &accumulatingExchangeState{},
	}, nil
}

func exchangeWithLogs(_ context.Context, ctx *vgirpc.CallContext, _ exchangeWithLogsParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: scaleOutputSchema,
		InputSchema:  scaleInputSchema,
		State:        &loggingExchangeState{},
	}, nil
}

func exchangeErrorOnNth(_ context.Context, ctx *vgirpc.CallContext, p exchangeErrorOnNthParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: scaleOutputSchema,
		InputSchema:  scaleInputSchema,
		State:        &failOnExchangeNState{FailOn: int(p.FailOn)},
	}, nil
}

func exchangeErrorOnInit(_ context.Context, ctx *vgirpc.CallContext, _ exchangeErrorOnInitParams) (*vgirpc.StreamResult, error) {
	return nil, &vgirpc.RpcError{Type: "RuntimeError", Message: "intentional exchange init error"}
}

func exchangeWithHeader(_ context.Context, ctx *vgirpc.CallContext, p exchangeWithHeaderParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: scaleOutputSchema,
		InputSchema:  scaleInputSchema,
		State:        &scaleExchangeState{Factor: p.Factor},
		Header:       ConformanceHeader{TotalExpected: 0, Description: "scale by " + formatFloat(p.Factor)},
	}, nil
}

// --- Rich header and dynamic schema handlers ---

func produceWithRichHeader(_ context.Context, ctx *vgirpc.CallContext, p produceWithRichHeaderParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: counterSchema,
		State:        &headerProducerState{Count: int(p.Count)},
		Header:       buildRichHeader(int(p.Seed)),
	}, nil
}

func exchangeWithRichHeader(_ context.Context, ctx *vgirpc.CallContext, p exchangeWithRichHeaderParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: scaleOutputSchema,
		InputSchema:  scaleInputSchema,
		State:        &scaleExchangeState{Factor: p.Factor},
		Header:       buildRichHeader(int(p.Seed)),
	}, nil
}

func produceDynamicSchema(_ context.Context, ctx *vgirpc.CallContext, p produceDynamicSchemaParams) (*vgirpc.StreamResult, error) {
	return &vgirpc.StreamResult{
		OutputSchema: buildDynamicSchema(p.IncludeStrings, p.IncludeFloats),
		State: &dynamicProducerState{
			Count:          int(p.Count),
			IncludeStrings: p.IncludeStrings,
			IncludeFloats:  p.IncludeFloats,
		},
		Header: buildRichHeader(int(p.Seed)),
	}, nil
}
