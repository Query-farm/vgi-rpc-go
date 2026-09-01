// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"net"
	"reflect"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type tcpTestProducer struct{ emitted bool }

func (producer *tcpTestProducer) Produce(_ context.Context, out *OutputCollector, _ *CallContext) error {
	if producer.emitted {
		return out.Finish()
	}
	producer.emitted = true
	builder := array.NewInt32Builder(memory.DefaultAllocator)
	builder.Append(7)
	values := builder.NewInt32Array()
	builder.Release()
	defer values.Release()
	return out.EmitArrays([]arrow.Array{values}, 1)
}

type tcpTestExchange struct{}

func (*tcpTestExchange) Exchange(_ context.Context, input arrow.RecordBatch, out *OutputCollector, _ *CallContext) error {
	input.Retain()
	return out.Emit(input)
}

func TestTcpClientUnaryConnectionReuse(t *testing.T) {
	server := NewServer()
	Unary(server, "greet", func(context.Context, *CallContext, struct{}) (string, error) {
		return "hello", nil
	})
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	go server.serveTcpConn(context.Background(), serverConn)
	client := newTcpClientFromConn(clientConn, tcpClientConfig{
		maxRequest:  defaultClientMaxRequestBytes,
		maxResponse: defaultClientMaxDecodedResponseBytes,
	})
	defer client.Close()
	params := emptyBatch(arrow.NewSchema(nil, nil))
	defer params.Release()
	resultSchema, err := resultSchema(reflect.TypeOf(""))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		result, callErr := client.CallUnary(context.Background(), "greet", params, resultSchema)
		if callErr != nil {
			t.Fatal(callErr)
		}
		if got := result.Batch.Column(0).(*array.String).Value(0); got != "hello" {
			t.Fatalf("result = %q, want hello", got)
		}
		result.Release()
	}
}

func TestTcpClientSerializesConcurrentCalls(t *testing.T) {
	server := NewServer()
	Unary(server, "echo", func(_ context.Context, _ *CallContext, params struct {
		Value string `vgirpc:"value"`
	}) (string, error) {
		return params.Value, nil
	})
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	go server.serveTcpConn(context.Background(), serverConn)
	client := newTcpClientFromConn(clientConn, tcpClientConfig{
		maxRequest:  defaultClientMaxRequestBytes,
		maxResponse: defaultClientMaxDecodedResponseBytes,
	})
	defer client.Close()
	resultSchema, err := resultSchema(reflect.TypeOf(""))
	if err != nil {
		t.Fatal(err)
	}
	const calls = 16
	var wait sync.WaitGroup
	errors := make(chan error, calls)
	for index := range calls {
		wait.Add(1)
		go func() {
			defer wait.Done()
			builder := array.NewStringBuilder(defaultAllocator())
			builder.Append(string(rune('a' + index)))
			values := builder.NewArray()
			builder.Release()
			params := array.NewRecordBatch(
				arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String}}, nil),
				[]arrow.Array{values}, 1)
			values.Release()
			defer params.Release()
			result, callErr := client.CallUnary(context.Background(), "echo", params, resultSchema)
			if callErr == nil {
				result.Release()
			}
			errors <- callErr
		}()
	}
	wait.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestTcpClientProducerAndExchangeReuseConnection(t *testing.T) {
	valueSchema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
	server := NewServer()
	Producer(server, "numbers", valueSchema,
		func(context.Context, *CallContext, struct{}) (*StreamResult, error) {
			return &StreamResult{OutputSchema: valueSchema, State: &tcpTestProducer{}}, nil
		})
	Exchange(server, "echo", valueSchema, valueSchema,
		func(context.Context, *CallContext, struct{}) (*StreamResult, error) {
			return &StreamResult{OutputSchema: valueSchema, InputSchema: valueSchema, State: &tcpTestExchange{}}, nil
		})
	Unary(server, "after", func(context.Context, *CallContext, struct{}) (string, error) {
		return "still-synchronized", nil
	})
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	go server.serveTcpConn(context.Background(), serverConn)
	client := newTcpClientFromConn(clientConn, tcpClientConfig{
		maxRequest:  defaultClientMaxRequestBytes,
		maxResponse: defaultClientMaxDecodedResponseBytes,
	})
	defer client.Close()
	params := emptyBatch(arrow.NewSchema(nil, nil))
	defer params.Release()

	producer, err := client.OpenProducer(context.Background(), "numbers", params,
		ClientStreamSchema{Output: valueSchema})
	if err != nil {
		t.Fatal(err)
	}
	batch, ok, err := producer.Next(context.Background())
	if err != nil || !ok || batch.Batch.Column(0).(*array.Int32).Value(0) != 7 {
		t.Fatalf("producer first turn: batch=%v ok=%v err=%v", batch, ok, err)
	}
	batch.Release()
	if batch, ok, err = producer.Next(context.Background()); err != nil || ok || batch != nil {
		t.Fatalf("producer end: batch=%v ok=%v err=%v", batch, ok, err)
	}
	if err := producer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	exchange, err := client.OpenExchange(context.Background(), "echo", params,
		ClientStreamSchema{Input: valueSchema, Output: valueSchema})
	if err != nil {
		t.Fatal(err)
	}
	builder := array.NewInt32Builder(memory.DefaultAllocator)
	builder.Append(11)
	values := builder.NewInt32Array()
	builder.Release()
	input := array.NewRecordBatch(valueSchema, []arrow.Array{values}, 1)
	values.Release()
	result, err := exchange.Exchange(context.Background(), input)
	input.Release()
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Batch.Column(0).(*array.Int32).Value(0); got != 11 {
		t.Fatalf("exchange result = %d, want 11", got)
	}
	result.Release()
	if err := exchange.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	resultSchema, err := resultSchema(reflect.TypeOf(""))
	if err != nil {
		t.Fatal(err)
	}
	after, err := client.CallUnary(context.Background(), "after", params, resultSchema)
	if err != nil {
		t.Fatal(err)
	}
	if got := after.Batch.Column(0).(*array.String).Value(0); got != "still-synchronized" {
		t.Fatalf("post-stream unary = %q", got)
	}
	after.Release()
}

func TestTcpClientOptionsRejectUnsafeProxyAndLimits(t *testing.T) {
	if _, err := NewTcpClient(context.Background(), "example.invalid", 9400,
		WithTcpClientProxy("socks5h://user@127.0.0.1:1080")); err == nil {
		t.Fatal("credential-bearing proxy was accepted")
	}
	if _, err := NewTcpClient(context.Background(), "example.invalid", 9400,
		WithTcpClientLimits(0, 1)); err == nil {
		t.Fatal("zero request limit was accepted")
	}
}

func TestTcpClientUnaryEnforcesResponseLimit(t *testing.T) {
	server := NewServer()
	Unary(server, "greet", func(context.Context, *CallContext, struct{}) (string, error) {
		return "hello", nil
	})
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	go server.serveTcpConn(context.Background(), serverConn)
	client := newTcpClientFromConn(clientConn, tcpClientConfig{
		maxRequest:  defaultClientMaxRequestBytes,
		maxResponse: 1,
	})
	defer client.Close()
	params := emptyBatch(arrow.NewSchema(nil, nil))
	defer params.Release()
	resultSchema, err := resultSchema(reflect.TypeOf(""))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CallUnary(context.Background(), "greet", params, resultSchema); err == nil {
		t.Fatal("oversized raw unary response was accepted")
	}
}
