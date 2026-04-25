// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// serializeResult builds a 1-row record batch with a single "result" column.
func serializeResult(schema *arrow.Schema, value any) (arrow.RecordBatch, error) {
	mem := defaultAllocator()

	if schema.NumFields() == 0 {
		return array.NewRecordBatch(schema, nil, 0), nil
	}

	field := schema.Field(0)

	// Handle struct values that need IPC serialization into a binary "result" column.
	// ArrowSerializable structs are handled by buildArray, but plain structs with
	// vgirpc tags need to be serialized here.
	if field.Type.ID() == arrow.BINARY {
		rv := reflect.ValueOf(value)
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}
		if rv.Kind() == reflect.Struct {
			if _, ok := value.(ArrowSerializable); !ok {
				data, err := serializeVgirpcStruct(value)
				if err != nil {
					return nil, fmt.Errorf("serialize struct result: %w", err)
				}
				b := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
				defer b.Release()
				b.Append(data)
				arr := b.NewArray()
				defer arr.Release()
				return array.NewRecordBatch(schema, []arrow.Array{arr}, 1), nil
			}
		}
	}

	arr, err := buildArray(mem, field.Type, value)
	if err != nil {
		return nil, fmt.Errorf("serialize result: %w", err)
	}
	defer arr.Release()

	return array.NewRecordBatch(schema, []arrow.Array{arr}, 1), nil
}

// serializeVgirpcStruct serializes a Go struct with vgirpc tags to Arrow IPC bytes.
func serializeVgirpcStruct(value any) ([]byte, error) {
	rv := reflect.ValueOf(value)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	rt := rv.Type()

	schema, err := structToSchema(rt)
	if err != nil {
		return nil, err
	}

	mem := defaultAllocator()
	cols := make([]arrow.Array, schema.NumFields())

	fieldIdx := 0
	for i := range rt.NumField() {
		f := rt.Field(i)
		tag := f.Tag.Get("vgirpc")
		if tag == "" || tag == "-" {
			continue
		}

		fieldVal := rv.Field(i).Interface()
		arr, err := buildArray(mem, schema.Field(fieldIdx).Type, fieldVal)
		if err != nil {
			for _, c := range cols[:fieldIdx] {
				if c != nil {
					c.Release()
				}
			}
			return nil, fmt.Errorf("field %s: %w", f.Name, err)
		}
		cols[fieldIdx] = arr
		fieldIdx++
	}

	batch := array.NewRecordBatch(schema, cols, 1)
	for _, c := range cols {
		c.Release()
	}
	defer batch.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := w.Write(batch); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// buildArray creates a 1-element Arrow array from a Go value.
func buildArray(mem memory.Allocator, dt arrow.DataType, value any) (arrow.Array, error) {
	if value == nil {
		return buildNullArray(mem, dt), nil
	}

	// Handle interface values — extract the concrete value
	rv := reflect.ValueOf(value)
	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return buildNullArray(mem, dt), nil
		}
		value = rv.Elem().Interface()
		rv = reflect.ValueOf(value)
	}

	switch dt.ID() {
	case arrow.STRING:
		b := array.NewStringBuilder(mem)
		defer b.Release()
		b.Append(fmt.Sprintf("%v", value))
		return b.NewArray(), nil

	case arrow.INT64:
		b := array.NewInt64Builder(mem)
		defer b.Release()
		v, err := toInt64(value)
		if err != nil {
			return nil, err
		}
		b.Append(v)
		return b.NewArray(), nil

	case arrow.INT32:
		b := array.NewInt32Builder(mem)
		defer b.Release()
		v, err := toInt64(value)
		if err != nil {
			return nil, err
		}
		b.Append(int32(v))
		return b.NewArray(), nil

	case arrow.FLOAT64:
		b := array.NewFloat64Builder(mem)
		defer b.Release()
		v, err := toFloat64(value)
		if err != nil {
			return nil, err
		}
		b.Append(v)
		return b.NewArray(), nil

	case arrow.FLOAT32:
		b := array.NewFloat32Builder(mem)
		defer b.Release()
		v, err := toFloat64(value)
		if err != nil {
			return nil, err
		}
		b.Append(float32(v))
		return b.NewArray(), nil

	case arrow.BOOL:
		b := array.NewBooleanBuilder(mem)
		defer b.Release()
		v, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("expected bool for BOOL field, got %T", value)
		}
		b.Append(v)
		return b.NewArray(), nil

	case arrow.BINARY:
		b := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		defer b.Release()
		// Handle ArrowSerializable types
		if as, ok := value.(ArrowSerializable); ok {
			data, err := serializeArrowSerializable(as)
			if err != nil {
				return nil, err
			}
			b.Append(data)
		} else {
			v, ok := value.([]byte)
			if !ok {
				return nil, fmt.Errorf("expected []byte for BINARY field, got %T", value)
			}
			b.Append(v)
		}
		return b.NewArray(), nil

	case arrow.LIST:
		return buildListArray(mem, dt.(*arrow.ListType), rv)

	case arrow.MAP:
		return buildMapArray(mem, dt.(*arrow.MapType), rv)

	case arrow.DICTIONARY:
		// Dictionary-encoded enum (string value)
		dictType := dt.(*arrow.DictionaryType)
		b := array.NewDictionaryBuilder(mem, dictType)
		defer b.Release()
		sb := b.(*array.BinaryDictionaryBuilder)
		sb.AppendString(fmt.Sprintf("%v", value))
		return b.NewArray(), nil

	case arrow.STRUCT:
		return buildStructArray(mem, dt.(*arrow.StructType), rv)

	default:
		return nil, fmt.Errorf("unsupported Arrow type for serialization: %v", dt)
	}
}

func buildNullArray(mem memory.Allocator, dt arrow.DataType) arrow.Array {
	b := array.NewBuilder(mem, dt)
	defer b.Release()
	b.AppendNull()
	return b.NewArray()
}

func buildListArray(mem memory.Allocator, lt *arrow.ListType, rv reflect.Value) (arrow.Array, error) {
	lb := array.NewListBuilder(mem, lt.Elem())
	defer lb.Release()

	lb.Append(true) // start list element
	vb := lb.ValueBuilder()
	for i := range rv.Len() {
		elemVal := rv.Index(i).Interface()
		if err := appendToBuilder(vb, lt.Elem(), elemVal); err != nil {
			return nil, fmt.Errorf("list element [%d]: %w", i, err)
		}
	}
	return lb.NewArray(), nil
}

func buildMapArray(mem memory.Allocator, mt *arrow.MapType, rv reflect.Value) (arrow.Array, error) {
	mb := array.NewMapBuilder(mem, mt.KeyType(), mt.ItemType(), false)
	defer mb.Release()

	// Sort keys for deterministic output
	keys := rv.MapKeys()
	sort.Slice(keys, func(i, j int) bool {
		return fmt.Sprintf("%v", keys[i].Interface()) < fmt.Sprintf("%v", keys[j].Interface())
	})

	mb.Append(true) // start map element
	kb := mb.KeyBuilder()
	ib := mb.ItemBuilder()
	for _, k := range keys {
		v := rv.MapIndex(k)
		if err := appendToBuilder(kb, mt.KeyType(), k.Interface()); err != nil {
			return nil, fmt.Errorf("map key: %w", err)
		}
		if err := appendToBuilder(ib, mt.ItemType(), v.Interface()); err != nil {
			return nil, fmt.Errorf("map value: %w", err)
		}
	}
	return mb.NewArray(), nil
}

func buildStructArray(mem memory.Allocator, st *arrow.StructType, rv reflect.Value) (arrow.Array, error) {
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	rt := rv.Type()

	sb := array.NewStructBuilder(mem, st)
	defer sb.Release()
	sb.Append(true)
	for i := range st.NumFields() {
		fb := sb.FieldBuilder(i)
		if err := appendToBuilder(fb, st.Field(i).Type, getFieldValue(rv, rt, st.Field(i).Name)); err != nil {
			return nil, fmt.Errorf("struct field %s: %w", st.Field(i).Name, err)
		}
	}
	return sb.NewArray(), nil
}

// getFieldValue finds a Go struct field value by arrow tag name.
func getFieldValue(rv reflect.Value, rt reflect.Type, arrowName string) any {
	for i := range rt.NumField() {
		if rt.Field(i).Tag.Get("arrow") == arrowName {
			return rv.Field(i).Interface()
		}
	}
	return nil
}

// appendToBuilder appends a single value to an Arrow array builder.
func appendToBuilder(b array.Builder, dt arrow.DataType, value any) error {
	if value == nil {
		b.AppendNull()
		return nil
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			b.AppendNull()
			return nil
		}
		value = rv.Elem().Interface()
	}

	switch dt.ID() {
	case arrow.STRING:
		b.(*array.StringBuilder).Append(fmt.Sprintf("%v", value))
	case arrow.INT64:
		v, err := toInt64(value)
		if err != nil {
			return err
		}
		b.(*array.Int64Builder).Append(v)
	case arrow.INT32:
		v, err := toInt64(value)
		if err != nil {
			return err
		}
		b.(*array.Int32Builder).Append(int32(v))
	case arrow.FLOAT64:
		v, err := toFloat64(value)
		if err != nil {
			return err
		}
		b.(*array.Float64Builder).Append(v)
	case arrow.FLOAT32:
		v, err := toFloat64(value)
		if err != nil {
			return err
		}
		b.(*array.Float32Builder).Append(float32(v))
	case arrow.BOOL:
		b.(*array.BooleanBuilder).Append(value.(bool))
	case arrow.BINARY:
		if as, ok := value.(ArrowSerializable); ok {
			data, err := serializeArrowSerializable(as)
			if err != nil {
				return err
			}
			b.(*array.BinaryBuilder).Append(data)
		} else {
			b.(*array.BinaryBuilder).Append(value.([]byte))
		}
	case arrow.LIST:
		lb := b.(*array.ListBuilder)
		lb.Append(true)
		vb := lb.ValueBuilder()
		elemRV := reflect.ValueOf(value)
		for i := range elemRV.Len() {
			if err := appendToBuilder(vb, dt.(*arrow.ListType).Elem(), elemRV.Index(i).Interface()); err != nil {
				return err
			}
		}
	case arrow.MAP:
		mb := b.(*array.MapBuilder)
		mb.Append(true)
		kb := mb.KeyBuilder()
		ib := mb.ItemBuilder()
		mapRV := reflect.ValueOf(value)
		mapKeys := mapRV.MapKeys()
		sort.Slice(mapKeys, func(i, j int) bool {
			return fmt.Sprintf("%v", mapKeys[i].Interface()) < fmt.Sprintf("%v", mapKeys[j].Interface())
		})
		for _, k := range mapKeys {
			v := mapRV.MapIndex(k)
			if err := appendToBuilder(kb, dt.(*arrow.MapType).KeyType(), k.Interface()); err != nil {
				return err
			}
			if err := appendToBuilder(ib, dt.(*arrow.MapType).ItemType(), v.Interface()); err != nil {
				return err
			}
		}
	case arrow.DICTIONARY:
		dictBuilder := b.(*array.BinaryDictionaryBuilder)
		dictBuilder.AppendString(fmt.Sprintf("%v", value))
	case arrow.STRUCT:
		sb := b.(*array.StructBuilder)
		sb.Append(true)
		structType := dt.(*arrow.StructType)
		rv := reflect.ValueOf(value)
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}
		rt := rv.Type()
		for ci := range structType.NumFields() {
			sf := structType.Field(ci)
			fb := sb.FieldBuilder(ci)
			// Find matching Go field by arrow tag
			found := false
			for fi := range rt.NumField() {
				if rt.Field(fi).Tag.Get("arrow") == sf.Name {
					if err := appendToBuilder(fb, sf.Type, rv.Field(fi).Interface()); err != nil {
						return fmt.Errorf("struct field %s: %w", sf.Name, err)
					}
					found = true
					break
				}
			}
			if !found {
				fb.AppendNull()
			}
		}
	default:
		return fmt.Errorf("unsupported type in appendToBuilder: %v", dt)
	}
	return nil
}

// serializeArrowSerializable converts an ArrowSerializable value to IPC stream bytes.
func serializeArrowSerializable(as ArrowSerializable) ([]byte, error) {
	schema := as.ArrowSchema()
	mem := defaultAllocator()

	// Use reflection to build a single-row batch from the struct
	rv := reflect.ValueOf(as)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	rt := rv.Type()

	cols := make([]arrow.Array, schema.NumFields())
	for i := range schema.NumFields() {
		f := schema.Field(i)
		// Find the Go struct field with matching arrow tag name
		goField, val, err := findArrowField(rt, rv, f.Name)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", f.Name, err)
		}
		_ = goField
		arr, err := buildArray(mem, f.Type, val)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", f.Name, err)
		}
		cols[i] = arr
		defer cols[i].Release()
	}

	batch := array.NewRecordBatch(schema, cols, 1)
	defer batch.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := w.Write(batch); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// findArrowField finds a struct field with a matching "arrow" or "vgirpc" tag name.
func findArrowField(rt reflect.Type, rv reflect.Value, arrowName string) (reflect.StructField, any, error) {
	// Check "arrow" tags first
	for i := range rt.NumField() {
		f := rt.Field(i)
		tag := f.Tag.Get("arrow")
		if tag == arrowName {
			return f, rv.Field(i).Interface(), nil
		}
	}
	// Fall back to "vgirpc" tags (for types that use vgirpc tags but implement ArrowSerializable)
	for i := range rt.NumField() {
		f := rt.Field(i)
		tag := f.Tag.Get("vgirpc")
		if tag == "" || tag == "-" {
			continue
		}
		info := parseTag(tag)
		if info.Name == arrowName {
			return f, rv.Field(i).Interface(), nil
		}
	}
	return reflect.StructField{}, nil, fmt.Errorf("no field with arrow tag %q", arrowName)
}

// deserializeArrowSerializable reads IPC stream bytes into an ArrowSerializable Go struct.
func deserializeArrowSerializable(targetType reflect.Type, data []byte) (reflect.Value, error) {
	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return reflect.Value{}, fmt.Errorf("reading ArrowSerializable IPC: %w", err)
	}
	defer reader.Release()

	if !reader.Next() {
		return reflect.Value{}, fmt.Errorf("no batch in ArrowSerializable IPC stream")
	}
	batch := reader.RecordBatch()

	result := reflect.New(targetType).Elem()
	for i := range targetType.NumField() {
		f := targetType.Field(i)
		tag := f.Tag.Get("arrow")
		if tag == "" {
			continue
		}

		colIdx := -1
		for ci := range batch.NumCols() {
			if batch.ColumnName(int(ci)) == tag {
				colIdx = int(ci)
				break
			}
		}
		if colIdx == -1 {
			continue
		}

		col := batch.Column(colIdx)
		if col.IsNull(0) {
			continue
		}
		if err := setFieldFromArrow(result.Field(i), f.Type, col, 0, tagInfo{}); err != nil {
			return reflect.Value{}, fmt.Errorf("ArrowSerializable field %s: %w", tag, err)
		}
	}
	return result, nil
}

// Numeric conversion helpers
