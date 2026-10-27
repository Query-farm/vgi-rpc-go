// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type wrappedRequestTestParams struct {
	Value string `vgirpc:"value"`
}

type wrappedRequestLimitedTestParams struct {
	Value string `vgirpc:"value"`
}

func (wrappedRequestLimitedTestParams) VgiRpcMaxWrappedRequestBytes() int { return 16 }

type wrappedRequestPointerLimitedTestParams struct {
	Value string `vgirpc:"value"`
}

func (*wrappedRequestPointerLimitedTestParams) VgiRpcMaxWrappedRequestBytes() int { return 16 }

// These tests exercise the private decoder and size-hook ordering. Public RPC
// behavior is also exercised by VGI's shared projection conformance runner.
func wrappedRequestTestStream(t *testing.T, count int) []byte {
	t.Helper()
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	builder.Append("kept")
	values := builder.NewArray()
	builder.Release()
	defer values.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String}}, nil)
	row := array.NewRecordBatch(schema, []arrow.Array{values}, 1)
	defer row.Release()
	var buffer bytes.Buffer
	writer := ipc.NewWriter(&buffer, ipc.WithSchema(schema))
	for range count {
		if err := writer.Write(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func wrappedRequestTestEnvelope(raw []byte) arrow.RecordBatch {
	builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	builder.Append(raw)
	value := builder.NewArray()
	builder.Release()
	defer value.Release()
	return array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "request", Type: arrow.BinaryTypes.Binary}}, nil), []arrow.Array{value}, 1)
}

func TestWrappedRequestCompleteFraming(t *testing.T) {
	complete := wrappedRequestTestStream(t, 1)
	cleanEOF := complete[:len(complete)-8]
	appendBytes := func(suffix []byte) []byte { return append(append([]byte(nil), complete...), suffix...) }
	for _, tc := range []struct {
		name  string
		bytes []byte
		valid bool
	}{
		{"modern_eos", complete, true},
		{"clean_physical_eof", cleanEOF, true},
		{"legacy_eos", append(append([]byte(nil), cleanEOF...), 0, 0, 0, 0), true},
		{"no_batch", wrappedRequestTestStream(t, 0), false},
		{"two_batches", wrappedRequestTestStream(t, 2), false},
		{"trailing_garbage", appendBytes([]byte("ignored before the fix")), false},
		{"repeated_eos", appendBytes([]byte{255, 255, 255, 255, 0, 0, 0, 0}), false},
		{"second_stream", appendBytes(complete), false},
		{"truncated_eos", complete[:len(complete)-4], false},
		{"partial_message_header", append(append([]byte(nil), cleanEOF...), 255, 255), false},
		{"truncated_batch", complete[:len(complete)-17], false},
		{"empty", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outer := wrappedRequestTestEnvelope(tc.bytes)
			defer outer.Release()
			value, err := deserializeParams(outer, reflect.TypeFor[wrappedRequestTestParams]())
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v, error=%v", tc.valid, err)
			}
			if tc.valid && value.Interface().(wrappedRequestTestParams).Value != "kept" {
				t.Fatal("decoded value changed")
			}
		})
	}
}

func TestWrappedRequestRawLimitBeforeDecode(t *testing.T) {
	// Malformed IPC is deliberately larger than the declared limit: the size
	// error must win before Arrow attempts to parse its schema or allocate data.
	outer := wrappedRequestTestEnvelope(bytes.Repeat([]byte{255}, 17))
	defer outer.Release()
	for _, target := range []reflect.Type{
		reflect.TypeFor[wrappedRequestLimitedTestParams](),
		reflect.TypeFor[wrappedRequestPointerLimitedTestParams](),
	} {
		if _, err := deserializeParams(outer, target); err == nil || !strings.Contains(err.Error(), "exceeds limit of 16 bytes") {
			t.Fatalf("%v did not enforce raw byte limit before decode: %v", target, err)
		}
	}
	complete := wrappedRequestTestStream(t, 1)
	for _, limit := range []int{0, len(complete)} {
		row, err := readWrappedRequest(complete, limit)
		if err != nil {
			t.Fatal(err)
		}
		row.Release()
	}
	for _, limit := range []int{-1, len(complete) - 1} {
		if row, err := readWrappedRequest(complete, limit); err == nil {
			row.Release()
			t.Fatalf("limit %d accepted request size %d", limit, len(complete))
		}
	}
}
