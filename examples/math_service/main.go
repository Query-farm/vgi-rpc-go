// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Query-farm/vgi-rpc/vgirpc"
)

func init() {
	// Register state types for gob serialization (needed for HTTP transport).
	vgirpc.RegisterStateType(&CountdownState{})
	vgirpc.RegisterStateType(&RunningSumState{})
}

// --- Parameter structs ---

type AddParams struct {
	A float64 `vgirpc:"a"`
	B float64 `vgirpc:"b"`
}

type MultiplyParams struct {
	A float64 `vgirpc:"a"`
	B float64 `vgirpc:"b"`
}

type CountdownParams struct {
	Start int64 `vgirpc:"start"`
}

type RunningSumParams struct {
	Initial float64 `vgirpc:"initial,default=0"`
}

// --- Schemas ---

var CountdownOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "value", Type: arrow.PrimitiveTypes.Int64},
}, nil)

var RunningSumInputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "value", Type: arrow.PrimitiveTypes.Float64},
}, nil)

var RunningSumOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "sum", Type: arrow.PrimitiveTypes.Float64},
}, nil)

// --- Producer state: countdown ---

type CountdownState struct {
	Current int64
}

func (s *CountdownState) Produce(_ context.Context, out *vgirpc.OutputCollector, _ *vgirpc.CallContext) error {
	if s.Current < 0 {
		return out.Finish()
	}
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	b.Append(s.Current)
	arr := b.NewArray()
	defer arr.Release()

	if err := out.EmitArrays([]arrow.Array{arr}, 1); err != nil {
		return err
	}
	s.Current--
	return nil
}

// --- Exchange state: running_sum ---

type RunningSumState struct {
	Sum float64
}

func (s *RunningSumState) Exchange(_ context.Context, input arrow.RecordBatch, out *vgirpc.OutputCollector, _ *vgirpc.CallContext) error {
	valueCol := input.Column(0).(*array.Float64)
	for i := int64(0); i < input.NumRows(); i++ {
		s.Sum += valueCol.Value(int(i))
	}

	mem := memory.NewGoAllocator()
	b := array.NewFloat64Builder(mem)
	defer b.Release()
	b.Append(s.Sum)
	arr := b.NewArray()
	defer arr.Release()

	return out.EmitArrays([]arrow.Array{arr}, 1)
}

// --- Main ---

func main() {
	server := vgirpc.NewServer()

	// Unary: add two numbers
	vgirpc.Unary(server, "add", func(_ context.Context, _ *vgirpc.CallContext, p AddParams) (float64, error) {
		return p.A + p.B, nil
	})

	// Unary: multiply two numbers
	vgirpc.Unary(server, "multiply", func(_ context.Context, _ *vgirpc.CallContext, p MultiplyParams) (float64, error) {
		return p.A * p.B, nil
	})

	// Producer: countdown from start to 0
	vgirpc.Producer(server, "countdown", CountdownOutputSchema,
		func(_ context.Context, _ *vgirpc.CallContext, p CountdownParams) (*vgirpc.StreamResult, error) {
			return &vgirpc.StreamResult{
				OutputSchema: CountdownOutputSchema,
				State:        &CountdownState{Current: p.Start},
			}, nil
		})

	// Exchange: running sum of input values
	vgirpc.Exchange(server, "running_sum", RunningSumOutputSchema, RunningSumInputSchema,
		func(_ context.Context, _ *vgirpc.CallContext, p RunningSumParams) (*vgirpc.StreamResult, error) {
			return &vgirpc.StreamResult{
				OutputSchema: RunningSumOutputSchema,
				State:        &RunningSumState{Sum: p.Initial},
			}, nil
		})

	if len(os.Args) > 1 && os.Args[1] == "--http" {
		httpServer := vgirpc.NewHttpServer(server)

		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to listen: %v\n", err)
			os.Exit(1)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		fmt.Printf("PORT:%d\n", port)
		os.Stdout.Sync()

		if err := http.Serve(listener, httpServer); err != nil {
			fmt.Fprintf(os.Stderr, "http serve error: %v\n", err)
			os.Exit(1)
		}
	} else {
		server.RunStdio()
	}
}
