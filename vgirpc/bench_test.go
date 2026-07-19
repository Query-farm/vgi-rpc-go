// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Allocation and throughput benchmarks for the per-call hot paths.
//
// These are Benchmark-only — correctness validation remains the Python
// conformance suite (see CLAUDE.md). Run with:
//
//	go test -bench . -benchmem ./vgirpc/

package vgirpc

import (
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// --- fixtures ---------------------------------------------------------------

// benchScalarParams is the common shape: a handful of scalar columns.
type benchScalarParams struct {
	A float64 `vgirpc:"a"`
	B float64 `vgirpc:"b"`
	N string  `vgirpc:"name"`
}

// benchWideParams exercises the per-field reflection walk more heavily.
type benchWideParams struct {
	F1  string  `vgirpc:"f1"`
	F2  int64   `vgirpc:"f2"`
	F3  float64 `vgirpc:"f3"`
	F4  bool    `vgirpc:"f4"`
	F5  string  `vgirpc:"f5"`
	F6  int64   `vgirpc:"f6"`
	F7  float64 `vgirpc:"f7"`
	F8  bool    `vgirpc:"f8"`
	F9  string  `vgirpc:"f9"`
	F10 int64   `vgirpc:"f10"`
	F11 float64 `vgirpc:"f11"`
	F12 bool    `vgirpc:"f12"`
}

// benchCollectionParams exercises the map/list builder paths.
type benchCollectionParams struct {
	Mapping map[string]int64 `vgirpc:"mapping"`
	Tags    []int64          `vgirpc:"tags"`
}

// buildParamsBatch serializes a tagged struct into the 1-row record batch a
// client would send, mirroring what the transports hand to deserializeParams.
func buildParamsBatch(tb testing.TB, v any) arrow.RecordBatch {
	tb.Helper()
	rv := reflect.ValueOf(v)
	rt := rv.Type()

	schema, err := structToSchema(rt)
	if err != nil {
		tb.Fatalf("structToSchema: %v", err)
	}

	mem := defaultAllocator()
	cols := make([]arrow.Array, 0, schema.NumFields())
	fieldIdx := 0
	for i := range rt.NumField() {
		tag := rt.Field(i).Tag.Get("vgirpc")
		if tag == "" || tag == "-" {
			continue
		}
		arr, err := buildArray(mem, schema.Field(fieldIdx).Type, rv.Field(i).Interface())
		if err != nil {
			tb.Fatalf("buildArray field %d: %v", i, err)
		}
		cols = append(cols, arr)
		fieldIdx++
	}

	batch := array.NewRecordBatch(schema, cols, 1)
	for _, c := range cols {
		c.Release()
	}
	return batch
}

// --- deserialize (every inbound call) --------------------------------------

func BenchmarkDeserializeParamsScalar(b *testing.B) {
	batch := buildParamsBatch(b, benchScalarParams{A: 1.5, B: 2.5, N: "world"})
	defer batch.Release()
	rt := reflect.TypeOf(benchScalarParams{})

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := deserializeParams(batch, rt); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeserializeParamsWide(b *testing.B) {
	batch := buildParamsBatch(b, benchWideParams{F1: "a", F5: "b", F9: "c"})
	defer batch.Release()
	rt := reflect.TypeOf(benchWideParams{})

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := deserializeParams(batch, rt); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeserializeParamsCollections(b *testing.B) {
	batch := buildParamsBatch(b, benchCollectionParams{
		Mapping: map[string]int64{"a": 1, "b": 2, "c": 3},
		Tags:    []int64{1, 2, 3, 4, 5, 6, 7, 8},
	})
	defer batch.Release()
	rt := reflect.TypeOf(benchCollectionParams{})

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := deserializeParams(batch, rt); err != nil {
			b.Fatal(err)
		}
	}
}

// --- serialize (every outbound result) -------------------------------------

func BenchmarkSerializeResultFloat(b *testing.B) {
	schema, err := resultSchema(reflect.TypeOf(float64(0)))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		batch, err := serializeResult(schema, 42.5)
		if err != nil {
			b.Fatal(err)
		}
		batch.Release()
	}
}

func BenchmarkSerializeResultString(b *testing.B) {
	schema, err := resultSchema(reflect.TypeOf(""))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		batch, err := serializeResult(schema, "Hello, world!")
		if err != nil {
			b.Fatal(err)
		}
		batch.Release()
	}
}

// serializeVgirpcStruct is the path that rebuilds the schema per call.
func BenchmarkSerializeVgirpcStruct(b *testing.B) {
	v := benchScalarParams{A: 1.5, B: 2.5, N: "world"}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := serializeVgirpcStruct(v); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSerializeVgirpcStructWide(b *testing.B) {
	v := benchWideParams{F1: "a", F5: "b", F9: "c"}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := serializeVgirpcStruct(v); err != nil {
			b.Fatal(err)
		}
	}
}

// --- component costs --------------------------------------------------------

func BenchmarkStructToSchema(b *testing.B) {
	rt := reflect.TypeOf(benchWideParams{})
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := structToSchema(rt); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseTag(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = parseTag("name,default=foo")
	}
}

func BenchmarkBuildArrayInt64(b *testing.B) {
	mem := defaultAllocator()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		arr, err := buildArray(mem, arrow.PrimitiveTypes.Int64, int64(42))
		if err != nil {
			b.Fatal(err)
		}
		arr.Release()
	}
}

func BenchmarkBuildArrayMap(b *testing.B) {
	mem := defaultAllocator()
	mt := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64)
	m := map[string]int64{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6, "g": 7, "h": 8}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		arr, err := buildArray(mem, mt, m)
		if err != nil {
			b.Fatal(err)
		}
		arr.Release()
	}
}
