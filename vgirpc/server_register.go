// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
)

func Unary[P any, R any](s *Server, name string, handler func(context.Context, *CallContext, P) (R, error)) {
	var p P
	var r R
	paramsType := reflect.TypeOf(p)
	resultType := reflect.TypeOf(r)

	paramsSchema, err := structToSchema(paramsType)
	if err != nil {
		panic(fmt.Sprintf("vgirpc: registering %q: invalid params type %T: %v", name, p, err))
	}

	resultSchema, err := resultSchema(resultType)
	if err != nil {
		panic(fmt.Sprintf("vgirpc: registering %q: invalid result type %T: %v", name, r, err))
	}

	s.methods[name] = &methodInfo{
		Name:          name,
		Type:          MethodUnary,
		ParamsType:    paramsType,
		ResultType:    resultType,
		ParamsSchema:  paramsSchema,
		ResultSchema:  resultSchema,
		Handler:       reflect.ValueOf(handler),
		ParamDefaults: extractDefaults(paramsType),
	}
}

// UnaryVoid registers a unary RPC method that returns no value.
func UnaryVoid[P any](s *Server, name string, handler func(context.Context, *CallContext, P) error) {
	var p P
	paramsType := reflect.TypeOf(p)

	paramsSchema, err := structToSchema(paramsType)
	if err != nil {
		panic(fmt.Sprintf("vgirpc: registering %q: invalid params type %T: %v", name, p, err))
	}

	resultSchema := arrow.NewSchema(nil, nil) // empty schema for void

	s.methods[name] = &methodInfo{
		Name:          name,
		Type:          MethodUnary,
		ParamsType:    paramsType,
		ResultType:    nil,
		ParamsSchema:  paramsSchema,
		ResultSchema:  resultSchema,
		Handler:       reflect.ValueOf(handler),
		ParamDefaults: extractDefaults(paramsType),
	}
}

// Producer registers a producer stream method.
// The handler returns a StreamResult containing the ProducerState.
func Producer[P any](s *Server, name string, outputSchema *arrow.Schema,
	handler func(context.Context, *CallContext, P) (*StreamResult, error)) {
	if outputSchema == nil {
		panic(fmt.Sprintf("vgirpc: registering %q: outputSchema must not be nil", name))
	}
	var p P
	paramsType := reflect.TypeOf(p)
	paramsSchema, err := structToSchema(paramsType)
	if err != nil {
		panic(fmt.Sprintf("vgirpc: registering %q: invalid params type %T: %v", name, p, err))
	}

	s.methods[name] = &methodInfo{
		Name:          name,
		Type:          MethodProducer,
		ParamsType:    paramsType,
		ParamsSchema:  paramsSchema,
		ResultSchema:  arrow.NewSchema(nil, nil), // empty for streams
		Handler:       reflect.ValueOf(handler),
		ParamDefaults: extractDefaults(paramsType),
		OutputSchema:  outputSchema,
	}
}

// ProducerWithHeader registers a producer stream method that returns a header.
func ProducerWithHeader[P any](s *Server, name string, outputSchema *arrow.Schema,
	headerSchema *arrow.Schema, handler func(context.Context, *CallContext, P) (*StreamResult, error)) {
	if outputSchema == nil {
		panic(fmt.Sprintf("vgirpc: registering %q: outputSchema must not be nil", name))
	}
	var p P
	paramsType := reflect.TypeOf(p)
	paramsSchema, err := structToSchema(paramsType)
	if err != nil {
		panic(fmt.Sprintf("vgirpc: registering %q: invalid params type %T: %v", name, p, err))
	}

	s.methods[name] = &methodInfo{
		Name:          name,
		Type:          MethodProducer,
		ParamsType:    paramsType,
		ParamsSchema:  paramsSchema,
		ResultSchema:  arrow.NewSchema(nil, nil),
		Handler:       reflect.ValueOf(handler),
		ParamDefaults: extractDefaults(paramsType),
		OutputSchema:  outputSchema,
		HasHeader:     true,
		HeaderSchema:  headerSchema,
	}
}

// Exchange registers an exchange stream method.
func Exchange[P any](s *Server, name string, outputSchema, inputSchema *arrow.Schema,
	handler func(context.Context, *CallContext, P) (*StreamResult, error)) {
	if outputSchema == nil {
		panic(fmt.Sprintf("vgirpc: registering %q: outputSchema must not be nil", name))
	}
	if inputSchema == nil {
		panic(fmt.Sprintf("vgirpc: registering %q: inputSchema must not be nil", name))
	}
	var p P
	paramsType := reflect.TypeOf(p)
	paramsSchema, err := structToSchema(paramsType)
	if err != nil {
		panic(fmt.Sprintf("vgirpc: registering %q: invalid params type %T: %v", name, p, err))
	}

	s.methods[name] = &methodInfo{
		Name:          name,
		Type:          MethodExchange,
		ParamsType:    paramsType,
		ParamsSchema:  paramsSchema,
		ResultSchema:  arrow.NewSchema(nil, nil),
		Handler:       reflect.ValueOf(handler),
		ParamDefaults: extractDefaults(paramsType),
		OutputSchema:  outputSchema,
		InputSchema:   inputSchema,
	}
}

// ExchangeWithHeader registers an exchange stream method that returns a header.
func ExchangeWithHeader[P any](s *Server, name string, outputSchema, inputSchema *arrow.Schema,
	headerSchema *arrow.Schema, handler func(context.Context, *CallContext, P) (*StreamResult, error)) {
	if outputSchema == nil {
		panic(fmt.Sprintf("vgirpc: registering %q: outputSchema must not be nil", name))
	}
	if inputSchema == nil {
		panic(fmt.Sprintf("vgirpc: registering %q: inputSchema must not be nil", name))
	}
	var p P
	paramsType := reflect.TypeOf(p)
	paramsSchema, err := structToSchema(paramsType)
	if err != nil {
		panic(fmt.Sprintf("vgirpc: registering %q: invalid params type %T: %v", name, p, err))
	}

	s.methods[name] = &methodInfo{
		Name:          name,
		Type:          MethodExchange,
		ParamsType:    paramsType,
		ParamsSchema:  paramsSchema,
		ResultSchema:  arrow.NewSchema(nil, nil),
		Handler:       reflect.ValueOf(handler),
		ParamDefaults: extractDefaults(paramsType),
		OutputSchema:  outputSchema,
		InputSchema:   inputSchema,
		HasHeader:     true,
		HeaderSchema:  headerSchema,
	}
}

// DynamicStreamWithHeader registers a stream method where the state type
// (ProducerState or ExchangeState) is determined at runtime based on the
// StreamResult returned by the handler. The handler must return a StreamResult
// whose State field implements either ProducerState or ExchangeState.
// OutputSchema and InputSchema are taken from the StreamResult at runtime.
func DynamicStreamWithHeader[P any](s *Server, name string,
	headerSchema *arrow.Schema, handler func(context.Context, *CallContext, P) (*StreamResult, error)) {
	var p P
	paramsType := reflect.TypeOf(p)
	paramsSchema, err := structToSchema(paramsType)
	if err != nil {
		panic(fmt.Sprintf("vgirpc: registering %q: invalid params type %T: %v", name, p, err))
	}

	s.methods[name] = &methodInfo{
		Name:          name,
		Type:          MethodDynamic,
		ParamsType:    paramsType,
		ParamsSchema:  paramsSchema,
		ResultSchema:  arrow.NewSchema(nil, nil),
		Handler:       reflect.ValueOf(handler),
		ParamDefaults: extractDefaults(paramsType),
		HasHeader:     true,
		HeaderSchema:  headerSchema,
	}
}

// RunStdio runs the server loop reading from stdin and writing to stdout.
// If stdin or stdout is connected to a terminal, a warning is printed to
// stderr (matching the Python vgi-rpc behaviour).
