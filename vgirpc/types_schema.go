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
	Name      string
	Default   *string // nil if no default
	ArrowType string  // explicit type override: "int32", "float32", "enum", "binary"
	Nullable  bool    // force nullable for non-pointer primitive fields
}

// parseTag parses a vgirpc struct tag like "name", "name,default=foo", "name,enum", "name,int32", "name,nullable".
func parseTag(tag string) tagInfo {
	parts := strings.Split(tag, ",")
	info := tagInfo{Name: parts[0]}
	for _, part := range parts[1:] {
		if strings.HasPrefix(part, "default=") {
			val := strings.TrimPrefix(part, "default=")
			info.Default = &val
		} else if part == "nullable" {
			info.Nullable = true
		} else {
			info.ArrowType = part
		}
	}
	return info
}

// goTypeToArrowType maps a Go reflect.Type to an Arrow DataType.
// The tag provides additional type hints (e.g., "enum", "int32", "binary").
func goTypeToArrowType(t reflect.Type, tag tagInfo) (arrow.DataType, bool, error) {
	nullable := tag.Nullable

	// Handle pointer types (optional/nullable)
	if t.Kind() == reflect.Ptr {
		nullable = true
		t = t.Elem()
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
		// List type
		elemType, _, err := goTypeToArrowType(t.Elem(), tagInfo{})
		if err != nil {
			return nil, false, fmt.Errorf("list element: %w", err)
		}
		return arrow.ListOf(elemType), nullable, nil
	case reflect.Map:
		keyType, _, err := goTypeToArrowType(t.Key(), tagInfo{})
		if err != nil {
			return nil, false, fmt.Errorf("map key: %w", err)
		}
		valType, _, err := goTypeToArrowType(t.Elem(), tagInfo{})
		if err != nil {
			return nil, false, fmt.Errorf("map value: %w", err)
		}
		return arrow.MapOf(keyType, valType), nullable, nil
	default:
		return nil, false, fmt.Errorf("unsupported Go type: %v (kind: %v)", t, t.Kind())
	}
}

// structToSchema builds an Arrow schema from a Go struct type using vgirpc tags.
func structToSchema(t reflect.Type) (*arrow.Schema, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct type, got %v", t.Kind())
	}
	var fields []arrow.Field
	for i := range t.NumField() {
		f := t.Field(i)
		tag := f.Tag.Get("vgirpc")
		if tag == "" || tag == "-" {
			continue
		}
		info := parseTag(tag)

		arrowType, nullable, err := goTypeToArrowType(f.Type, info)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", f.Name, err)
		}
		fields = append(fields, arrow.Field{
			Name:     info.Name,
			Type:     arrowType,
			Nullable: nullable,
		})
	}
	return arrow.NewSchema(fields, nil), nil
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
			{Name: "result", Type: arrow.BinaryTypes.Binary, Nullable: false},
		}, nil), nil
	}

	// Struct types with vgirpc tags are serialized as IPC bytes in a binary "result" column.
	derefT := t
	if derefT.Kind() == reflect.Ptr {
		derefT = derefT.Elem()
	}
	if derefT.Kind() == reflect.Struct {
		return arrow.NewSchema([]arrow.Field{
			{Name: "result", Type: arrow.BinaryTypes.Binary, Nullable: false},
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
