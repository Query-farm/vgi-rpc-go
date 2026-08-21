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
)

// readOneBatch reads the single record batch out of an Arrow IPC stream.
func readOneBatch(t *testing.T, data []byte) arrow.RecordBatch {
	t.Helper()
	r, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("ipc.NewReader: %v", err)
	}
	defer r.Release()
	if !r.Next() {
		t.Fatal("no record batch in IPC stream")
	}
	batch := r.RecordBatch()
	batch.Retain()
	return batch
}

// The VGI protocol declares BindRequest.copy_from / .copy_to as INLINE Arrow
// structs, not as IPC bytes in a binary column. These types mirror that shape
// so the `struct` tag is exercised against the schema a real client sends.

type structTagCopyFrom struct {
	Format         string `vgirpc:"format"`
	FilePath       string `vgirpc:"file_path"`
	ExpectedSchema []byte `vgirpc:"expected_schema"`
}

type structTagCopyTo struct {
	Format   string `vgirpc:"format"`
	FilePath string `vgirpc:"file_path"`
}

type structTagBind struct {
	FunctionName string             `vgirpc:"function_name"`
	CopyFrom     *structTagCopyFrom `vgirpc:"copy_from,struct"`
	CopyTo       *structTagCopyTo   `vgirpc:"copy_to,struct"`
	SchemaName   *string            `vgirpc:"schema_name"`
}

// TestStructTagSchemaDerivation pins the derived schema field-for-field:
// a struct column whose children carry the nullability the tags imply, and a
// field-level nullability driven by the pointer, not by the children.
func TestStructTagSchemaDerivation(t *testing.T) {
	got, err := SchemaForStruct(reflect.TypeOf(structTagBind{}))
	if err != nil {
		t.Fatalf("SchemaForStruct: %v", err)
	}

	want := arrow.NewSchema([]arrow.Field{
		{Name: "function_name", Type: arrow.BinaryTypes.String},
		{Name: "copy_from", Nullable: true, Type: arrow.StructOf(
			arrow.Field{Name: "format", Type: arrow.BinaryTypes.String},
			arrow.Field{Name: "file_path", Type: arrow.BinaryTypes.String},
			arrow.Field{Name: "expected_schema", Type: arrow.BinaryTypes.Binary},
		)},
		{Name: "copy_to", Nullable: true, Type: arrow.StructOf(
			arrow.Field{Name: "format", Type: arrow.BinaryTypes.String},
			arrow.Field{Name: "file_path", Type: arrow.BinaryTypes.String},
		)},
		{Name: "schema_name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	if !got.Equal(want) {
		t.Fatalf("derived schema mismatch\n got: %s\nwant: %s", got, want)
	}
}

// A non-pointer struct field derives the same struct type but a NON-nullable
// column, so nullability of the field itself is expressible either way.
func TestStructTagNonPointerFieldIsNotNullable(t *testing.T) {
	type inline struct {
		Ctx structTagCopyTo `vgirpc:"ctx,struct"`
	}
	got, err := SchemaForStruct(reflect.TypeOf(inline{}))
	if err != nil {
		t.Fatalf("SchemaForStruct: %v", err)
	}
	f := got.Field(0)
	if f.Nullable {
		t.Fatalf("non-pointer struct field derived as nullable: %s", got)
	}
	if f.Type.ID() != arrow.STRUCT {
		t.Fatalf("expected a struct column, got %s", f.Type)
	}
}

// roundTrip serializes value through the same path a parameter batch takes
// (struct -> record batch -> IPC -> record batch) and reads it back into a
// fresh value of the same type.
func roundTrip(t *testing.T, value any) reflect.Value {
	t.Helper()
	rt := reflect.TypeOf(value)

	ipcBytes, err := serializeVgirpcStruct(value)
	if err != nil {
		t.Fatalf("serializeVgirpcStruct: %v", err)
	}
	batch := readOneBatch(t, ipcBytes)
	defer batch.Release()

	// deserializeParams validates with Schema.Equal, so a derivation that did
	// not match what was written would fail here rather than silently decode.
	out, err := deserializeParams(batch, rt)
	if err != nil {
		t.Fatalf("deserializeParams: %v", err)
	}
	return out
}

// TestStructTagRoundTripPopulated is the COPY case: both struct columns carry
// values, including a binary child.
func TestStructTagRoundTripPopulated(t *testing.T) {
	schemaName := "main"
	in := structTagBind{
		FunctionName: "read_lines",
		CopyFrom: &structTagCopyFrom{
			Format:         "example.lines",
			FilePath:       "/tmp/in.txt",
			ExpectedSchema: []byte{0xde, 0xad, 0xbe, 0xef},
		},
		CopyTo: &structTagCopyTo{
			Format:   "example.lines",
			FilePath: "/tmp/out.txt",
		},
		SchemaName: &schemaName,
	}

	got := roundTrip(t, in).Interface().(structTagBind)

	if got.CopyFrom == nil {
		t.Fatal("copy_from decoded as nil")
	}
	if got.CopyFrom.Format != in.CopyFrom.Format ||
		got.CopyFrom.FilePath != in.CopyFrom.FilePath ||
		string(got.CopyFrom.ExpectedSchema) != string(in.CopyFrom.ExpectedSchema) {
		t.Fatalf("copy_from round-trip mismatch: %+v", *got.CopyFrom)
	}
	if got.CopyTo == nil {
		t.Fatal("copy_to decoded as nil")
	}
	if got.CopyTo.Format != in.CopyTo.Format || got.CopyTo.FilePath != in.CopyTo.FilePath {
		t.Fatalf("copy_to round-trip mismatch: %+v", *got.CopyTo)
	}
	if got.SchemaName == nil || *got.SchemaName != schemaName {
		t.Fatalf("schema_name round-trip mismatch: %v", got.SchemaName)
	}
}

// TestStructTagRoundTripNull is the COMMON case: an ordinary (non-COPY) bind
// sends a valid batch whose copy_from/copy_to cells are null. That must
// decode to a nil pointer — user code tests `req.CopyFrom != nil` to tell
// "absent" from "present", so a zero-valued struct would be indistinguishable
// from a COPY bind with empty strings.
func TestStructTagRoundTripNull(t *testing.T) {
	in := structTagBind{FunctionName: "sequence"}

	got := roundTrip(t, in).Interface().(structTagBind)

	if got.FunctionName != "sequence" {
		t.Fatalf("function_name round-trip mismatch: %q", got.FunctionName)
	}
	if got.CopyFrom != nil {
		t.Fatalf("null copy_from decoded as non-nil: %+v", *got.CopyFrom)
	}
	if got.CopyTo != nil {
		t.Fatalf("null copy_to decoded as non-nil: %+v", *got.CopyTo)
	}
	if got.SchemaName != nil {
		t.Fatalf("null schema_name decoded as non-nil: %q", *got.SchemaName)
	}
}

// TestStructTagNullColumnIsArrowNull checks the wire form directly, not just
// what Go decodes: the column must be a struct column with a null slot, which
// is what a peer validating the parameter contract expects to receive.
func TestStructTagNullColumnIsArrowNull(t *testing.T) {
	ipcBytes, err := serializeVgirpcStruct(structTagBind{FunctionName: "sequence"})
	if err != nil {
		t.Fatalf("serializeVgirpcStruct: %v", err)
	}
	batch := readOneBatch(t, ipcBytes)
	defer batch.Release()

	col := batch.Column(1)
	if _, ok := col.(*array.Struct); !ok {
		t.Fatalf("copy_from column is %T, want *array.Struct", col)
	}
	if !col.IsNull(0) {
		t.Fatal("copy_from column is not null on a non-COPY bind")
	}
}

// Nested structs are supported to maxStructNestDepth levels; this proves one
// level of nesting inside a struct field derives and round-trips.
type structTagInner struct {
	Label string `vgirpc:"label"`
	Count int64  `vgirpc:"count"`
}

type structTagOuter struct {
	Name  string          `vgirpc:"name"`
	Inner *structTagInner `vgirpc:"inner,struct"`
}

type structTagNested struct {
	Top *structTagOuter `vgirpc:"top,struct"`
}

func TestStructTagNestedStruct(t *testing.T) {
	got, err := SchemaForStruct(reflect.TypeOf(structTagNested{}))
	if err != nil {
		t.Fatalf("SchemaForStruct: %v", err)
	}
	want := arrow.NewSchema([]arrow.Field{
		{Name: "top", Nullable: true, Type: arrow.StructOf(
			arrow.Field{Name: "name", Type: arrow.BinaryTypes.String},
			arrow.Field{Name: "inner", Nullable: true, Type: arrow.StructOf(
				arrow.Field{Name: "label", Type: arrow.BinaryTypes.String},
				arrow.Field{Name: "count", Type: arrow.PrimitiveTypes.Int64},
			)},
		)},
	}, nil)
	if !got.Equal(want) {
		t.Fatalf("nested schema mismatch\n got: %s\nwant: %s", got, want)
	}

	in := structTagNested{Top: &structTagOuter{
		Name:  "outer",
		Inner: &structTagInner{Label: "inner", Count: 7},
	}}
	out := roundTrip(t, in).Interface().(structTagNested)
	if out.Top == nil || out.Top.Inner == nil {
		t.Fatalf("nested struct decoded as nil: %+v", out)
	}
	if out.Top.Name != "outer" || out.Top.Inner.Label != "inner" || out.Top.Inner.Count != 7 {
		t.Fatalf("nested round-trip mismatch: %+v / %+v", *out.Top, *out.Top.Inner)
	}

	// An inner null must stay nil while the outer struct is present.
	partial := structTagNested{Top: &structTagOuter{Name: "outer"}}
	outPartial := roundTrip(t, partial).Interface().(structTagNested)
	if outPartial.Top == nil {
		t.Fatal("outer struct decoded as nil")
	}
	if outPartial.Top.Inner != nil {
		t.Fatalf("null inner struct decoded as non-nil: %+v", *outPartial.Top.Inner)
	}
}

// A self-referential type cannot be described (the per-type memo cannot break
// the cycle), so it must fail with a clear error rather than overflow the
// stack.
type structTagRecursive struct {
	Next *structTagRecursive `vgirpc:"next,struct"`
}

func TestStructTagRecursionIsBounded(t *testing.T) {
	_, err := SchemaForStruct(reflect.TypeOf(structTagRecursive{}))
	if err == nil {
		t.Fatal("expected an error for a self-referential struct field")
	}
	if !strings.Contains(err.Error(), "nesting exceeds") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// The `struct` tag is only meaningful on a struct-typed field.
func TestStructTagRejectsNonStructField(t *testing.T) {
	type bad struct {
		X string `vgirpc:"x,struct"`
	}
	_, err := SchemaForStruct(reflect.TypeOf(bad{}))
	if err == nil {
		t.Fatal("expected an error for a `struct` tag on a string field")
	}
	if !strings.Contains(err.Error(), "requires a struct") {
		t.Fatalf("unexpected error: %v", err)
	}
}
