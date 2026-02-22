// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

// tagInfo holds parsed information from a `vgirpc` struct tag.
type tagInfo struct {
	Name      string
	Default   *string // nil if no default
	ArrowType string  // explicit type override: "int32", "float32", "enum", "binary"
}

// parseTag parses a vgirpc struct tag like "name", "name,default=foo", "name,enum", "name,int32".
func parseTag(tag string) tagInfo {
	parts := strings.Split(tag, ",")
	info := tagInfo{Name: parts[0]}
	for _, part := range parts[1:] {
		if strings.HasPrefix(part, "default=") {
			val := strings.TrimPrefix(part, "default=")
			info.Default = &val
		} else {
			info.ArrowType = part
		}
	}
	return info
}

// goTypeToArrowType maps a Go reflect.Type to an Arrow DataType.
// The tag provides additional type hints (e.g., "enum", "int32", "binary").
func goTypeToArrowType(t reflect.Type, tag tagInfo) (arrow.DataType, bool, error) {
	nullable := false

	// Handle pointer types (optional/nullable)
	if t.Kind() == reflect.Ptr {
		nullable = true
		t = t.Elem()
	}

	// Check for explicit tag overrides
	switch tag.ArrowType {
	case "int32":
		return arrow.PrimitiveTypes.Int32, nullable, nil
	case "float32":
		return arrow.PrimitiveTypes.Float32, nullable, nil
	case "enum":
		return &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int16,
			ValueType: arrow.BinaryTypes.String,
		}, nullable, nil
	case "binary":
		return arrow.BinaryTypes.Binary, nullable, nil
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
func resultSchema(t reflect.Type) (*arrow.Schema, error) {
	if t == nil {
		return arrow.NewSchema(nil, nil), nil
	}

	// Check if it implements ArrowSerializable — result is binary
	if t.Implements(arrowSerializableType) || reflect.PointerTo(t).Implements(arrowSerializableType) {
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

// deserializeParams reads row 0 from a record batch into a Go struct.
func deserializeParams(batch arrow.RecordBatch, target reflect.Type) (reflect.Value, error) {
	if target.Kind() == reflect.Ptr {
		target = target.Elem()
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
		field.SetBytes(c.Value(idx))
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
	start, end := listArr.ValueOffsets(idx)
	values := listArr.ListValues()
	length := int(end - start)

	if isPtr {
		fieldType = fieldType.Elem()
	}

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

// serializeResult builds a 1-row record batch with a single "result" column.
func serializeResult(schema *arrow.Schema, value any) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()

	if schema.NumFields() == 0 {
		return array.NewRecordBatch(schema, nil, 0), nil
	}

	field := schema.Field(0)
	arr, err := buildArray(mem, field.Type, value)
	if err != nil {
		return nil, fmt.Errorf("serialize result: %w", err)
	}
	defer arr.Release()

	return array.NewRecordBatch(schema, []arrow.Array{arr}, 1), nil
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
		b.Append(value.(bool))
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
			b.Append(value.([]byte))
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
	mem := memory.NewGoAllocator()

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

// findArrowField finds a struct field with a matching "arrow" tag name.
func findArrowField(rt reflect.Type, rv reflect.Value, arrowName string) (reflect.StructField, any, error) {
	for i := range rt.NumField() {
		f := rt.Field(i)
		tag := f.Tag.Get("arrow")
		if tag == arrowName {
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

func toInt64(v any) (int64, error) {
	switch val := v.(type) {
	case int64:
		return val, nil
	case int:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case int16:
		return int64(val), nil
	case int8:
		return int64(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

func toFloat64(v any) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int:
		return float64(val), nil
	case int64:
		return float64(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}
