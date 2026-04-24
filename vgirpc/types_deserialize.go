// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"fmt"
	"log/slog"
	"reflect"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// deserializeParams reads row 0 from a record batch into a Go struct.
func deserializeParams(batch arrow.RecordBatch, target reflect.Type) (reflect.Value, error) {
	if target.Kind() == reflect.Ptr {
		target = target.Elem()
	}

	// Handle wrapped request: if the batch has a single "request" column of type
	// binary, the actual parameters are IPC-serialized inside it. Unwrap the
	// inner batch and use it for field mapping.
	if batch.NumCols() == 1 && batch.ColumnName(0) == "request" && batch.Column(0).DataType().ID() == arrow.BINARY {
		slog.Debug("deserializeParams: detected wrapped request column", "target", target.Name())
		if binCol, ok := batch.Column(0).(*array.Binary); ok && binCol.Len() > 0 && !binCol.IsNull(0) {
			data := binCol.Value(0)
			slog.Debug("deserializeParams: unwrapping request", "dataLen", len(data))
			if len(data) > 0 {
				innerReader, err := ipc.NewReader(bytes.NewReader(data))
				if err != nil {
					slog.Debug("deserializeParams: unwrap error", "err", err)
					return reflect.Value{}, fmt.Errorf("unwrapping request IPC: %w", err)
				}
				defer innerReader.Release()
				if innerReader.Next() {
					innerBatch := innerReader.RecordBatch()
					innerBatch.Retain()
					defer innerBatch.Release()
					slog.Debug("deserializeParams: unwrapped inner batch", "numCols", innerBatch.NumCols(), "numRows", innerBatch.NumRows())
					return deserializeParams(innerBatch, target)
				}
				slog.Debug("deserializeParams: no batch in IPC stream")
			}
		}
	}

	result := reflect.New(target).Elem()

	for i := range target.NumField() {
		f := target.Field(i)
		tag := f.Tag.Get("vgirpc")
		if tag == "" || tag == "-" {
			continue
		}
		info := parseTag(tag)

		// Find column by name
		colIdx := -1
		for ci := range batch.NumCols() {
			if batch.ColumnName(int(ci)) == info.Name {
				colIdx = int(ci)
				break
			}
		}
		if colIdx == -1 {
			// Column not present — use default if available
			if info.Default != nil {
				if err := setFieldFromString(result.Field(i), f.Type, *info.Default); err != nil {
					return reflect.Value{}, fmt.Errorf("default for %s: %w", info.Name, err)
				}
			}
			continue
		}

		col := batch.Column(colIdx)
		if col.IsNull(0) {
			// Value is null — apply default if available, otherwise leave as zero
			if info.Default != nil {
				if err := setFieldFromString(result.Field(i), f.Type, *info.Default); err != nil {
					return reflect.Value{}, fmt.Errorf("default for %s: %w", info.Name, err)
				}
			}
			continue
		}

		if err := setFieldFromArrow(result.Field(i), f.Type, col, 0, info); err != nil {
			return reflect.Value{}, fmt.Errorf("field %s: %w", info.Name, err)
		}
	}
	return result, nil
}

// setFieldFromArrow sets a struct field value from an Arrow array at index i.
func setFieldFromArrow(field reflect.Value, fieldType reflect.Type, col arrow.Array, idx int, info tagInfo) error {
	isPtr := fieldType.Kind() == reflect.Ptr
	if isPtr {
		fieldType = fieldType.Elem()
	}

	// Handle ArrowSerializable types — could be binary (IPC stream at param level)
	// or struct (nested inside another ArrowSerializable)
	if fieldType.Implements(arrowSerializableType) || reflect.PointerTo(fieldType).Implements(arrowSerializableType) {
		switch c := col.(type) {
		case *array.Binary:
			// At method param level: binary column containing IPC stream bytes
			data := c.Value(idx)
			val, err := deserializeArrowSerializable(fieldType, data)
			if err != nil {
				return err
			}
			if isPtr {
				ptr := reflect.New(fieldType)
				ptr.Elem().Set(val)
				field.Set(ptr)
			} else {
				field.Set(val)
			}
			return nil
		case *array.Struct:
			// Nested inside another ArrowSerializable: struct column
			return setStructField(field, fieldType, isPtr, c, idx)
		default:
			return fmt.Errorf("expected Binary or Struct array for ArrowSerializable, got %T", col)
		}
	}

	// Handle enum tag
	if info.ArrowType == "enum" {
		var strVal string
		switch c := col.(type) {
		case *array.Dictionary:
			dict := c.Dictionary().(*array.String)
			strVal = dict.Value(c.GetValueIndex(idx))
		case *array.String:
			strVal = c.Value(idx)
		default:
			return fmt.Errorf("expected Dictionary or String for enum, got %T", col)
		}
		if isPtr {
			ptr := reflect.New(fieldType)
			ptr.Elem().SetString(strVal)
			field.Set(ptr)
		} else {
			field.SetString(strVal)
		}
		return nil
	}

	switch c := col.(type) {
	case *array.String:
		setStringField(field, fieldType, isPtr, c.Value(idx))
	case *array.Int64:
		setIntField(field, fieldType, isPtr, c.Value(idx))
	case *array.Int32:
		setIntField(field, fieldType, isPtr, int64(c.Value(idx)))
	case *array.Float64:
		setFloatField(field, fieldType, isPtr, c.Value(idx))
	case *array.Float32:
		setFloatField(field, fieldType, isPtr, float64(c.Value(idx)))
	case *array.Boolean:
		setBoolField(field, fieldType, isPtr, c.Value(idx))
	case *array.Binary:
		if isPtr {
			v := c.Value(idx)
			ptr := reflect.New(fieldType)
			ptr.Elem().SetBytes(v)
			field.Set(ptr)
		} else {
			field.SetBytes(c.Value(idx))
		}
	case *array.LargeBinary:
		if isPtr {
			v := c.Value(idx)
			ptr := reflect.New(fieldType)
			ptr.Elem().SetBytes(v)
			field.Set(ptr)
		} else {
			field.SetBytes(c.Value(idx))
		}
	case *array.List:
		return setListField(field, fieldType, isPtr, c, idx, info)
	case *array.Map:
		return setMapField(field, fieldType, isPtr, c, idx)
	case *array.Dictionary:
		// Dictionary-encoded string (enum)
		dict := c.Dictionary().(*array.String)
		strVal := dict.Value(c.GetValueIndex(idx))
		setStringField(field, fieldType, isPtr, strVal)
	case *array.Struct:
		return setStructField(field, fieldType, isPtr, c, idx)
	default:
		return fmt.Errorf("unsupported Arrow array type: %T", col)
	}
	return nil
}

func setStringField(field reflect.Value, fieldType reflect.Type, isPtr bool, val string) {
	if isPtr {
		ptr := reflect.New(fieldType)
		ptr.Elem().SetString(val)
		field.Set(ptr)
	} else {
		field.SetString(val)
	}
}

func setIntField(field reflect.Value, fieldType reflect.Type, isPtr bool, val int64) {
	if isPtr {
		ptr := reflect.New(fieldType)
		ptr.Elem().SetInt(val)
		field.Set(ptr)
	} else {
		field.SetInt(val)
	}
}

func setFloatField(field reflect.Value, fieldType reflect.Type, isPtr bool, val float64) {
	if isPtr {
		ptr := reflect.New(fieldType)
		ptr.Elem().SetFloat(val)
		field.Set(ptr)
	} else {
		field.SetFloat(val)
	}
}

func setBoolField(field reflect.Value, fieldType reflect.Type, isPtr bool, val bool) {
	if isPtr {
		ptr := reflect.New(fieldType)
		ptr.Elem().SetBool(val)
		field.Set(ptr)
	} else {
		field.SetBool(val)
	}
}

func setListField(field reflect.Value, fieldType reflect.Type, isPtr bool, listArr *array.List, idx int, info tagInfo) error {
	// Note: fieldType is already dereferenced by the caller (setFieldFromArrow)
	start, end := listArr.ValueOffsets(idx)
	values := listArr.ListValues()
	length := int(end - start)

	slice := reflect.MakeSlice(fieldType, length, length)
	for j := 0; j < length; j++ {
		elemField := slice.Index(j)
		if err := setFieldFromArrow(elemField, fieldType.Elem(), values, int(start)+j, tagInfo{}); err != nil {
			return fmt.Errorf("list element [%d]: %w", j, err)
		}
	}

	if isPtr {
		ptr := reflect.New(fieldType)
		ptr.Elem().Set(slice)
		field.Set(ptr)
	} else {
		field.Set(slice)
	}
	return nil
}

func setStructField(field reflect.Value, fieldType reflect.Type, isPtr bool, structArr *array.Struct, idx int) error {
	// Note: fieldType is already dereferenced by the caller (setFieldFromArrow)
	result := reflect.New(fieldType).Elem()
	structType := structArr.DataType().(*arrow.StructType)

	for fi := range fieldType.NumField() {
		goField := fieldType.Field(fi)
		arrowTag := goField.Tag.Get("arrow")
		if arrowTag == "" {
			continue
		}

		// Find the child array by arrow tag name
		childIdx := -1
		for ci := range structType.NumFields() {
			if structType.Field(ci).Name == arrowTag {
				childIdx = ci
				break
			}
		}
		if childIdx == -1 {
			continue
		}

		childArr := structArr.Field(childIdx)
		if childArr.IsNull(idx) {
			continue
		}
		if err := setFieldFromArrow(result.Field(fi), goField.Type, childArr, idx, tagInfo{}); err != nil {
			return fmt.Errorf("struct field %s: %w", arrowTag, err)
		}
	}

	if isPtr {
		ptr := reflect.New(fieldType)
		ptr.Elem().Set(result)
		field.Set(ptr)
	} else {
		field.Set(result)
	}
	return nil
}

func setMapField(field reflect.Value, fieldType reflect.Type, isPtr bool, mapArr *array.Map, idx int) error {
	start, end := mapArr.ValueOffsets(idx)
	keys := mapArr.Keys()
	items := mapArr.Items()
	length := int(end - start)

	if isPtr {
		fieldType = fieldType.Elem()
	}

	m := reflect.MakeMapWithSize(fieldType, length)
	for j := 0; j < length; j++ {
		k := reflect.New(fieldType.Key()).Elem()
		v := reflect.New(fieldType.Elem()).Elem()
		if err := setFieldFromArrow(k, fieldType.Key(), keys, int(start)+j, tagInfo{}); err != nil {
			return fmt.Errorf("map key [%d]: %w", j, err)
		}
		if err := setFieldFromArrow(v, fieldType.Elem(), items, int(start)+j, tagInfo{}); err != nil {
			return fmt.Errorf("map value [%d]: %w", j, err)
		}
		m.SetMapIndex(k, v)
	}

	if isPtr {
		ptr := reflect.New(fieldType)
		ptr.Elem().Set(m)
		field.Set(ptr)
	} else {
		field.Set(m)
	}
	return nil
}

// setFieldFromString sets a struct field from a string default value.
func setFieldFromString(field reflect.Value, fieldType reflect.Type, s string) error {
	if fieldType.Kind() == reflect.Ptr {
		fieldType = fieldType.Elem()
	}
	switch fieldType.Kind() {
	case reflect.String:
		field.SetString(s)
	case reflect.Int64, reflect.Int:
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("parsing int default %q: %w", s, err)
		}
		field.SetInt(v)
	case reflect.Float64:
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return fmt.Errorf("parsing float default %q: %w", s, err)
		}
		field.SetFloat(v)
	case reflect.Bool:
		v, err := strconv.ParseBool(s)
		if err != nil {
			return fmt.Errorf("parsing bool default %q: %w", s, err)
		}
		field.SetBool(v)
	default:
		return fmt.Errorf("default value parsing not supported for %v", fieldType.Kind())
	}
	return nil
}
