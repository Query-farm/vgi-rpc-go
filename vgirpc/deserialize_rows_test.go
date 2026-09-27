// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package vgirpc

import (
	"bytes"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"reflect"
	"testing"
)

func TestDeserializeParamsRequiresExactlyOneRow(t *testing.T) {
	type params struct {
		Value string `vgirpc:"value"`
	}
	for _, n := range []int{0, 1, 2} {
		builder := array.NewStringBuilder(memory.DefaultAllocator)
		for i := 0; i < n; i++ {
			builder.Append("x")
		}
		values := builder.NewArray()
		builder.Release()
		schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String}}, nil)
		record := array.NewRecordBatch(schema, []arrow.Array{values}, int64(n))
		values.Release()
		_, e := deserializeParams(record, reflect.TypeOf(params{}))
		if (e == nil) != (n == 1) {
			t.Fatalf("direct rows=%d error=%v", n, e)
		}
		var buffer bytes.Buffer
		writer := ipc.NewWriter(&buffer, ipc.WithSchema(schema))
		if e := writer.Write(record); e != nil {
			t.Fatal(e)
		}
		if e := writer.Close(); e != nil {
			t.Fatal(e)
		}
		record.Release()
		binary := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
		binary.Append(buffer.Bytes())
		data := binary.NewArray()
		binary.Release()
		outer := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "request", Type: arrow.BinaryTypes.Binary}}, nil), []arrow.Array{data}, 1)
		data.Release()
		_, e = deserializeParams(outer, reflect.TypeOf(params{}))
		outer.Release()
		if (e == nil) != (n == 1) {
			t.Fatalf("wrapped rows=%d error=%v", n, e)
		}
	}
}

func TestDeserializeParamsPreservesEmptyMethod(t *testing.T) {
	record := array.NewRecordBatch(arrow.NewSchema(nil, nil), nil, 0)
	defer record.Release()
	if _, err := deserializeParams(record, reflect.TypeOf(struct{}{})); err != nil {
		t.Fatal("canonical empty request rejected", err)
	}
	if _, err := deserializeParams(record, reflect.TypeOf(struct {
		Value string `vgirpc:"value"`
	}{})); err == nil {
		t.Fatal("nonempty parameter target accepted empty request")
	}
}
