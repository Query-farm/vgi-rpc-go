// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"reflect"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
)

// Per-type reflection memoization.
//
// Deriving a struct's Arrow schema and its parsed `vgirpc` tags is a pure
// function of the reflect.Type, but the per-call paths (deserializeParams on
// every inbound request, serializeVgirpcStruct on every outbound struct
// result) used to redo the whole walk — reading struct tags, splitting them
// on commas, and rebuilding arrow.DataType values — for every single call.
//
// structDesc caches that work once per type. The cached *arrow.Schema is
// shared by every caller; arrow.Schema is immutable, and sharing the pointer
// additionally lets the IPC writer's schema-identity fast path hit (see the
// schema-pointer note in CLAUDE.md).

// fieldDesc is one tagged struct field, resolved once.
type fieldDesc struct {
	Index int          // index into the Go struct's fields
	Type  reflect.Type // the Go field type
	Info  tagInfo      // parsed `vgirpc` tag
}

// structDesc is the memoized description of a tagged struct type.
type structDesc struct {
	Schema *arrow.Schema
	Fields []fieldDesc // tagged fields only, in schema column order
	Err    error       // set if the type could not be described
}

var structDescCache sync.Map // reflect.Type -> *structDesc

// describeStruct returns the memoized description of t, computing it on first
// use. t must be a struct type (pointers are dereferenced by the caller).
func describeStruct(t reflect.Type) *structDesc {
	if cached, ok := structDescCache.Load(t); ok {
		return cached.(*structDesc)
	}
	desc := buildStructDesc(t)
	actual, _ := structDescCache.LoadOrStore(t, desc)
	return actual.(*structDesc)
}

// buildStructDesc does the uncached reflection walk. The walk itself lives in
// structFieldsOf (types_schema.go), which a `struct`-tagged field's inline
// child schema reuses at greater depth.
func buildStructDesc(t reflect.Type) *structDesc {
	fields, descs, err := structFieldsOf(t, 0)
	if err != nil {
		return &structDesc{Err: err}
	}
	return &structDesc{Schema: arrow.NewSchema(fields, nil), Fields: descs}
}

// resolveColumn finds the batch column index for the field at ordinal
// position `ord` named `name`. Batches almost always arrive with columns in
// schema order, so try that position first and fall back to a scan. Both
// paths are allocation-free, which turns the old per-field linear scan
// (O(fields × columns) per request) into O(fields) in the common case.
//
// The fallback scan takes the first name match, matching the previous
// behaviour. The fast path can only disagree with first-match if the batch
// carries two columns with the same name, which is malformed for a params
// batch — a duplicate name makes the field-to-column mapping ambiguous
// either way.
func resolveColumn(batch arrow.RecordBatch, ord int, name string) int {
	n := int(batch.NumCols())
	if ord < n && batch.ColumnName(ord) == name {
		return ord
	}
	for ci := range n {
		if batch.ColumnName(ci) == name {
			return ci
		}
	}
	return -1
}
