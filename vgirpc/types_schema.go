// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

// ArrowSerializable is the interface for Go types that can be serialized
// to/from Arrow IPC streams. At the method parameter/result level, these are
// serialized as binary (IPC stream bytes). When nested inside another
// ArrowSerializable type, they become Arrow struct columns.
type ArrowSerializable interface {
	ArrowSchema() *arrow.Schema
}

// arrowSerializableType is used to check interface implementation at reflect time.
var arrowSerializableType = reflect.TypeOf((*ArrowSerializable)(nil)).Elem()

// AnnotatedReturn is an optional interface a unary method's return type can
// implement to declare the Arrow data type its result column should carry.
// Use it for wide types (decimal, date32, large_string, etc.) where the Go
// type alone is not enough to choose the right Arrow primitive.
type AnnotatedReturn interface {
	VgirpcArrowResult() arrow.DataType
}

var annotatedReturnType = reflect.TypeOf((*AnnotatedReturn)(nil)).Elem()

// tagInfo holds parsed information from a `vgirpc` struct tag.
type tagInfo struct {
	Name        string
	Default     *string // nil if no default
	ArrowType   string  // explicit type override: "int32", "float32", "enum", "binary", "struct"
	ElemType    string  // explicit slice element or map value override; see parseTag
	Nullable    bool    // force nullable for non-pointer primitive fields
	NonNullable bool    // force non-nullable even when the Go field is a pointer
}

// parseTag parses a `vgirpc` struct tag.
//
// Forms: "name", "name,default=foo", "name,enum", "name,int32", "name,nullable",
// "name,nonnullable",
// "name,struct", "name,elem=large_binary".
//
// `elem=` overrides a slice field's element type or a map field's value type,
// where a bare type option overrides the field's own type. The two are
// distinct because a Go
// `[][]byte` is a list whose items are binary, and there is no other way to
// say "list of large_binary": a bare `large_binary` on that field would
// describe the field itself, which is not a list at all. The protocol needs
// exactly this for InitRequest.join_keys / .split_tokens and
// TableFunctionPlanRequest.join_keys — a set of join keys or a batch of split
// tokens can exceed the 2 GiB that 32-bit offsets address.
func parseTag(tag string) tagInfo {
	parts := strings.Split(tag, ",")
	info := tagInfo{Name: parts[0]}
	for _, part := range parts[1:] {
		if strings.HasPrefix(part, "default=") {
			val := strings.TrimPrefix(part, "default=")
			info.Default = &val
		} else if strings.HasPrefix(part, "elem=") {
			info.ElemType = strings.TrimPrefix(part, "elem=")
		} else if part == "nullable" {
			info.Nullable = true
		} else if part == "nonnullable" {
			info.NonNullable = true
		} else {
			info.ArrowType = part
		}
	}
	return info
}

// maxStructNestDepth bounds the recursive derivation of `struct`-tagged
// fields. Nothing on the VGI wire nests structs more than two deep, and the
// bound is what makes a self-referential Go type (type T struct{ Next *T
// `vgirpc:"next,struct"` }) a clean error instead of a stack overflow — the
// per-type memo cannot break the cycle, because the entry is only stored once
// the walk that would consult it has finished.
const maxStructNestDepth = 8

// goTypeToArrowType maps a Go reflect.Type to an Arrow DataType.
// The tag provides additional type hints (e.g., "enum", "int32", "binary",
// "struct").
func goTypeToArrowType(t reflect.Type, tag tagInfo) (arrow.DataType, bool, error) {
	return goTypeToArrowTypeAt(t, tag, 0)
}

// goTypeToArrowTypeAt is goTypeToArrowType carrying the struct-nesting depth
// of the field being described; see maxStructNestDepth.
func goTypeToArrowTypeAt(t reflect.Type, tag tagInfo, depth int) (arrow.DataType, bool, error) {
	nullable := tag.Nullable

	// Handle pointer types (optional/nullable)
	if t.Kind() == reflect.Ptr {
		nullable = true
		t = t.Elem()
	}
	if tag.NonNullable {
		nullable = false
	}

	// Check for explicit tag overrides
	switch tag.ArrowType {
	case "int8":
		return arrow.PrimitiveTypes.Int8, nullable, nil
	case "int16":
		return arrow.PrimitiveTypes.Int16, nullable, nil
	case "int32":
		return arrow.PrimitiveTypes.Int32, nullable, nil
	case "uint8":
		return arrow.PrimitiveTypes.Uint8, nullable, nil
	case "uint16":
		return arrow.PrimitiveTypes.Uint16, nullable, nil
	case "uint32":
		return arrow.PrimitiveTypes.Uint32, nullable, nil
	case "uint64":
		return arrow.PrimitiveTypes.Uint64, nullable, nil
	case "float32":
		return arrow.PrimitiveTypes.Float32, nullable, nil
	case "enum", "dict_string":
		return &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int16,
			ValueType: arrow.BinaryTypes.String,
		}, nullable, nil
	case "binary":
		return arrow.BinaryTypes.Binary, nullable, nil
	case "struct":
		// An INLINE Arrow struct column, derived from the Go struct's own
		// vgirpc-tagged fields by the same rules used here — so a child
		// declared `vgirpc:"file_path"` comes out `utf8 not null` and one
		// declared `*[]byte` comes out `binary` nullable.
		//
		// This is deliberately distinct from ArrowSerializable, which means
		// "carry me as IPC stream bytes in a binary column". Some protocol
		// fields are declared inline (BindRequest.copy_from / .copy_to), and
		// a peer that validates its parameter contract with Schema.Equal
		// rejects binary where the protocol says struct.
		dt, err := structArrowType(t, depth)
		if err != nil {
			return nil, false, err
		}
		return dt, nullable, nil
	case "large_string":
		return arrow.BinaryTypes.LargeString, nullable, nil
	case "large_binary":
		return arrow.BinaryTypes.LargeBinary, nullable, nil
	case "date":
		return arrow.FixedWidthTypes.Date32, nullable, nil
	case "timestamp":
		return &arrow.TimestampType{Unit: arrow.Microsecond}, nullable, nil
	case "timestamp_utc":
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, nullable, nil
	case "time":
		return arrow.FixedWidthTypes.Time64us, nullable, nil
	case "duration":
		return arrow.FixedWidthTypes.Duration_us, nullable, nil
	case "decimal":
		// decimal128(20, 4) — matches the conformance protocol.
		return &arrow.Decimal128Type{Precision: 20, Scale: 4}, nullable, nil
	}
	if strings.HasPrefix(tag.ArrowType, "fixed_binary[") && strings.HasSuffix(tag.ArrowType, "]") {
		inner := tag.ArrowType[len("fixed_binary[") : len(tag.ArrowType)-1]
		width, err := strconv.Atoi(inner)
		if err != nil || width <= 0 {
			return nil, false, fmt.Errorf("invalid fixed_binary tag %q", tag.ArrowType)
		}
		return &arrow.FixedSizeBinaryType{ByteWidth: width}, nullable, nil
	}

	// Check if the type implements ArrowSerializable
	if t.Implements(arrowSerializableType) || reflect.PointerTo(t).Implements(arrowSerializableType) {
		// At method param level, this becomes binary (IPC stream)
		return arrow.BinaryTypes.Binary, nullable, nil
	}

	switch t.Kind() {
	case reflect.String:
		return arrow.BinaryTypes.String, nullable, nil
	case reflect.Int64, reflect.Int:
		return arrow.PrimitiveTypes.Int64, nullable, nil
	case reflect.Int32:
		return arrow.PrimitiveTypes.Int32, nullable, nil
	case reflect.Int16:
		return arrow.PrimitiveTypes.Int16, nullable, nil
	case reflect.Int8:
		return arrow.PrimitiveTypes.Int8, nullable, nil
	case reflect.Uint64, reflect.Uint:
		return arrow.PrimitiveTypes.Uint64, nullable, nil
	case reflect.Uint32:
		return arrow.PrimitiveTypes.Uint32, nullable, nil
	case reflect.Uint16:
		return arrow.PrimitiveTypes.Uint16, nullable, nil
	case reflect.Uint8:
		return arrow.PrimitiveTypes.Uint8, nullable, nil
	case reflect.Float64:
		return arrow.PrimitiveTypes.Float64, nullable, nil
	case reflect.Float32:
		return arrow.PrimitiveTypes.Float32, nullable, nil
	case reflect.Bool:
		return &arrow.BooleanType{}, nullable, nil
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return arrow.BinaryTypes.Binary, nullable, nil
		}
		// List type. `elem=` carries a type override down to the item; nothing
		// else from this field's tag applies to the element (a `default=` or
		// `nullable` describes the column, not its items).
		elemType, _, err := goTypeToArrowTypeAt(t.Elem(), tagInfo{ArrowType: tag.ElemType}, depth)
		if err != nil {
			return nil, false, fmt.Errorf("list element: %w", err)
		}
		return arrow.ListOf(elemType), nullable, nil
	case reflect.Map:
		keyType, _, err := goTypeToArrowTypeAt(t.Key(), tagInfo{}, depth)
		if err != nil {
			return nil, false, fmt.Errorf("map key: %w", err)
		}
		valType, _, err := goTypeToArrowTypeAt(t.Elem(), tagInfo{ArrowType: tag.ElemType}, depth)
		if err != nil {
			return nil, false, fmt.Errorf("map value: %w", err)
		}
		return arrow.MapOf(keyType, valType), nullable, nil
	default:
		return nil, false, fmt.Errorf("unsupported Go type: %v (kind: %v)", t, t.Kind())
	}
}

// structFieldsOf derives the Arrow fields of a vgirpc-tagged Go struct, and
// the parallel fieldDesc list that maps each column back to its Go field.
//
// It is the single derivation used by BOTH the top-level parameter/result
// schema (depth 0, via buildStructDesc) and a `struct`-tagged field's inline
// child schema (depth n, via structArrowType), so an inline struct is
// described by exactly the same tag rules as the record that carries it.
func structFieldsOf(t reflect.Type, depth int) ([]arrow.Field, []fieldDesc, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf("expected struct type, got %v", t.Kind())
	}

	n := t.NumField()
	fields := make([]arrow.Field, 0, n)
	descs := make([]fieldDesc, 0, n)

	for i := range n {
		f := t.Field(i)
		tag := f.Tag.Get("vgirpc")
		if tag == "" || tag == "-" {
			continue
		}
		info := parseTag(tag)

		arrowType, nullable, err := goTypeToArrowTypeAt(f.Type, info, depth)
		if err != nil {
			return nil, nil, fmt.Errorf("field %s: %w", f.Name, err)
		}
		fields = append(fields, arrow.Field{
			Name:     info.Name,
			Type:     arrowType,
			Nullable: nullable,
		})
		descs = append(descs, fieldDesc{Index: i, Type: f.Type, Info: info})
	}

	return fields, descs, nil
}

// structArrowType derives the arrow.StructType of a `struct`-tagged field.
// t is the field's type with any pointer already stripped by the caller, so a
// *T field and a T field derive the same struct type and differ only in the
// field's own nullability.
func structArrowType(t reflect.Type, depth int) (arrow.DataType, error) {
	if depth >= maxStructNestDepth {
		return nil, fmt.Errorf("struct field nesting exceeds %d levels at %v (recursive type?)", maxStructNestDepth, t)
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf(`vgirpc "struct" tag requires a struct (or *struct) field, got %v`, t)
	}
	fields, _, err := structFieldsOf(t, depth+1)
	if err != nil {
		return nil, fmt.Errorf("struct %v: %w", t, err)
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf(`struct %v has no vgirpc-tagged fields; a "struct" field needs at least one`, t)
	}
	return arrow.StructOf(fields...), nil
}

// tagName is the name part of a struct tag value — everything before the
// first comma, since both the `vgirpc` and `arrow` tags spell options after
// the name ("function_type,enum"). Returns "" for an absent or "-" tag, which
// never matches a real field name.
func tagName(tag string) string {
	if tag == "" || tag == "-" {
		return ""
	}
	if i := strings.IndexByte(tag, ','); i >= 0 {
		return tag[:i]
	}
	return tag
}

// goFieldForArrowName finds the index of the Go struct field that carries the
// Arrow struct child named `name`, or -1.
//
// The `arrow` tag wins, matching the historical lookup this replaces; the
// `vgirpc` tag is the fallback, so a struct described purely by vgirpc tags
// (what a `struct`-tagged field derives from) also round-trips. Matching is on
// the tag's NAME part, so an option-carrying tag such as `arrow:"phase,enum"`
// resolves too — the old exact-string compare silently missed those.
func goFieldForArrowName(rt reflect.Type, name string) int {
	for i := range rt.NumField() {
		if tagName(rt.Field(i).Tag.Get("arrow")) == name {
			return i
		}
	}
	for i := range rt.NumField() {
		if tagName(rt.Field(i).Tag.Get("vgirpc")) == name {
			return i
		}
	}
	return -1
}

// SchemaForStruct is [structToSchema] for callers outside this package: the
// Arrow schema this library will derive from a struct's vgirpc tags.
//
// Exported so a peer SDK can CHECK that derivation against the schema its
// protocol codegen emits. Those two must agree — the tags are the only
// description of the wire shape on the Go side, and codegen is the only one
// everywhere else — but nothing compared them, so a struct could drift from the
// protocol silently and be discovered by a client rejecting the response. That
// is exactly what happened in the Java SDK, where two fields were declared
// nullable against a non-null protocol and the client refused the whole
// response as an "out-of-date Apache Arrow schema".
//
// Reflection-only and memoized, so a test may call it freely.
func SchemaForStruct(t reflect.Type) (*arrow.Schema, error) {
	return structToSchema(t)
}

// SchemaForResult is [resultSchema] for callers outside this package: the Arrow
// schema of a method's RETURN type, which is wrapped in a single `result`
// column. See [SchemaForStruct] for why this is exported.
func SchemaForResult(t reflect.Type) (*arrow.Schema, error) {
	return resultSchema(t)
}

// structToSchema builds an Arrow schema from a Go struct type using vgirpc tags.
// The reflection walk is memoized per type; see types_cache.go. The returned
// schema is shared across callers — arrow.Schema is immutable, so this is
// safe and lets the IPC writer's schema-identity check hit.
func structToSchema(t reflect.Type) (*arrow.Schema, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	desc := describeStruct(t)
	if desc.Err != nil {
		return nil, desc.Err
	}
	return desc.Schema, nil
}

// resultSchema builds an Arrow schema for a return type.
// All results are serialized as a single "result" column.
func resultSchema(t reflect.Type) (*arrow.Schema, error) {
	if t == nil {
		return arrow.NewSchema(nil, nil), nil
	}

	// AnnotatedReturn lets a return type declare its own Arrow column type.
	// Take precedence over the default inference so wide types (decimal,
	// date, large_string, etc.) emit the right column.
	if t.Implements(annotatedReturnType) || reflect.PointerTo(t).Implements(annotatedReturnType) {
		zero := reflect.New(t).Elem().Interface().(AnnotatedReturn)
		return arrow.NewSchema([]arrow.Field{
			{Name: "result", Type: zero.VgirpcArrowResult(), Nullable: false},
		}, nil), nil
	}

	// Check if it implements ArrowSerializable — result is binary
	if t.Implements(arrowSerializableType) || reflect.PointerTo(t).Implements(arrowSerializableType) {
		return arrow.NewSchema([]arrow.Field{
			{Name: "result", Type: arrow.BinaryTypes.Binary, Nullable: t.Kind() == reflect.Ptr},
		}, nil), nil
	}

	// Struct types with vgirpc tags are serialized as IPC bytes in a binary "result" column.
	derefT := t
	if derefT.Kind() == reflect.Ptr {
		derefT = derefT.Elem()
	}
	if derefT.Kind() == reflect.Struct {
		return arrow.NewSchema([]arrow.Field{
			{Name: "result", Type: arrow.BinaryTypes.Binary, Nullable: t.Kind() == reflect.Ptr},
		}, nil), nil
	}

	arrowType, nullable, err := goTypeToArrowType(t, tagInfo{})
	if err != nil {
		return nil, fmt.Errorf("result type: %w", err)
	}
	return arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrowType, Nullable: nullable},
	}, nil), nil
}
