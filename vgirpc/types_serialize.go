// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// epochUTC is the Unix epoch interpreted as UTC; used to derive Arrow
// date32 / time64 / timestamp values from Go time.Time.
var epochUTC = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

// asTime converts value to a time.Time, accepting either a plain time.Time
// or a named type whose underlying type is time.Time (so handlers can
// declare a typed alias that implements AnnotatedReturn).
func asTime(value any) (time.Time, bool) {
	if t, ok := value.(time.Time); ok {
		return t, true
	}
	rv := reflect.ValueOf(value)
	if rv.IsValid() && rv.Type().ConvertibleTo(timeType) {
		return rv.Convert(timeType).Interface().(time.Time), true
	}
	return time.Time{}, false
}

// asDuration is the time.Duration analogue of asTime.
func asDuration(value any) (time.Duration, bool) {
	if d, ok := value.(time.Duration); ok {
		return d, true
	}
	rv := reflect.ValueOf(value)
	if rv.IsValid() && rv.Type().ConvertibleTo(durationType) {
		return rv.Convert(durationType).Interface().(time.Duration), true
	}
	return 0, false
}

// asString accepts a plain string, a named string type, or anything with a
// String() method.
func asString(value any) (string, bool) {
	if s, ok := value.(string); ok {
		return s, true
	}
	rv := reflect.ValueOf(value)
	if rv.IsValid() && rv.Kind() == reflect.String {
		return rv.String(), true
	}
	if s, ok := value.(fmt.Stringer); ok {
		return s.String(), true
	}
	return "", false
}

// asBytes accepts a []byte or a named slice-of-byte type.
func asBytes(value any) ([]byte, bool) {
	if b, ok := value.([]byte); ok {
		return b, true
	}
	rv := reflect.ValueOf(value)
	if rv.IsValid() && rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
		return rv.Bytes(), true
	}
	return nil, false
}

// daysSinceEpoch returns the number of full UTC days between t and the
// Unix epoch — the Arrow date32 wire encoding.
func daysSinceEpoch(t time.Time) int32 {
	return int32(t.UTC().Sub(epochUTC) / (24 * time.Hour))
}

// microsSinceMidnight returns the wall-clock microsecond offset of t
// within its date — the Arrow time64[us] wire encoding.
func microsSinceMidnight(t time.Time) int64 {
	t = t.UTC()
	h, m, s := t.Clock()
	return int64(h)*3_600_000_000 + int64(m)*60_000_000 + int64(s)*1_000_000 + int64(t.Nanosecond()/1000)
}

// decimalFromValue accepts a string-shaped Decimal value and converts it to
// a decimal128.Num at the given scale.
func decimalFromValue(value any, dt *arrow.Decimal128Type) (decimal128.Num, error) {
	s, ok := asString(value)
	if !ok {
		return decimal128.Num{}, fmt.Errorf("expected string for decimal, got %T", value)
	}
	n, err := decimal128.FromString(s, dt.Precision, dt.Scale)
	if err != nil {
		return decimal128.Num{}, fmt.Errorf("decimal128 parse %q: %w", s, err)
	}
	return n, nil
}

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

	case arrow.INT16:
		b := array.NewInt16Builder(mem)
		defer b.Release()
		v, err := toInt64(value)
		if err != nil {
			return nil, err
		}
		b.Append(int16(v))
		return b.NewArray(), nil

	case arrow.INT8:
		b := array.NewInt8Builder(mem)
		defer b.Release()
		v, err := toInt64(value)
		if err != nil {
			return nil, err
		}
		b.Append(int8(v))
		return b.NewArray(), nil

	case arrow.UINT64:
		b := array.NewUint64Builder(mem)
		defer b.Release()
		v, err := toUint64(value)
		if err != nil {
			return nil, err
		}
		b.Append(v)
		return b.NewArray(), nil

	case arrow.UINT32:
		b := array.NewUint32Builder(mem)
		defer b.Release()
		v, err := toUint64(value)
		if err != nil {
			return nil, err
		}
		b.Append(uint32(v))
		return b.NewArray(), nil

	case arrow.UINT16:
		b := array.NewUint16Builder(mem)
		defer b.Release()
		v, err := toUint64(value)
		if err != nil {
			return nil, err
		}
		b.Append(uint16(v))
		return b.NewArray(), nil

	case arrow.UINT8:
		b := array.NewUint8Builder(mem)
		defer b.Release()
		v, err := toUint64(value)
		if err != nil {
			return nil, err
		}
		b.Append(uint8(v))
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

	case arrow.LARGE_BINARY:
		b := array.NewBinaryBuilder(mem, arrow.BinaryTypes.LargeBinary)
		defer b.Release()
		v, ok := asBytes(value)
		if !ok {
			return nil, fmt.Errorf("expected []byte for LARGE_BINARY field, got %T", value)
		}
		b.Append(v)
		return b.NewArray(), nil

	case arrow.LARGE_STRING:
		b := array.NewLargeStringBuilder(mem)
		defer b.Release()
		s, ok := asString(value)
		if !ok {
			s = fmt.Sprintf("%v", value)
		}
		b.Append(s)
		return b.NewArray(), nil

	case arrow.FIXED_SIZE_BINARY:
		fsb := dt.(*arrow.FixedSizeBinaryType)
		b := array.NewFixedSizeBinaryBuilder(mem, fsb)
		defer b.Release()
		v, ok := asBytes(value)
		if !ok {
			return nil, fmt.Errorf("expected []byte for FIXED_SIZE_BINARY, got %T", value)
		}
		if len(v) != fsb.ByteWidth {
			return nil, fmt.Errorf("fixed_size_binary(%d) value has wrong length %d", fsb.ByteWidth, len(v))
		}
		b.Append(v)
		return b.NewArray(), nil

	case arrow.DATE32:
		b := array.NewDate32Builder(mem)
		defer b.Release()
		t, ok := asTime(value)
		if !ok {
			return nil, fmt.Errorf("expected time.Time for DATE32, got %T", value)
		}
		b.Append(arrow.Date32(daysSinceEpoch(t)))
		return b.NewArray(), nil

	case arrow.TIMESTAMP:
		ts := dt.(*arrow.TimestampType)
		b := array.NewTimestampBuilder(mem, ts)
		defer b.Release()
		t, ok := asTime(value)
		if !ok {
			return nil, fmt.Errorf("expected time.Time for TIMESTAMP, got %T", value)
		}
		b.Append(arrow.Timestamp(t.UTC().UnixMicro()))
		return b.NewArray(), nil

	case arrow.TIME64:
		b := array.NewTime64Builder(mem, &arrow.Time64Type{Unit: arrow.Microsecond})
		defer b.Release()
		t, ok := asTime(value)
		if !ok {
			return nil, fmt.Errorf("expected time.Time for TIME64, got %T", value)
		}
		b.Append(arrow.Time64(microsSinceMidnight(t)))
		return b.NewArray(), nil

	case arrow.DURATION:
		b := array.NewDurationBuilder(mem, &arrow.DurationType{Unit: arrow.Microsecond})
		defer b.Release()
		d, ok := asDuration(value)
		if !ok {
			return nil, fmt.Errorf("expected time.Duration for DURATION, got %T", value)
		}
		b.Append(arrow.Duration(d.Microseconds()))
		return b.NewArray(), nil

	case arrow.DECIMAL128:
		dec := dt.(*arrow.Decimal128Type)
		b := array.NewDecimal128Builder(mem, dec)
		defer b.Release()
		n, err := decimalFromValue(value, dec)
		if err != nil {
			return nil, err
		}
		b.Append(n)
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
	case arrow.INT16:
		v, err := toInt64(value)
		if err != nil {
			return err
		}
		b.(*array.Int16Builder).Append(int16(v))
	case arrow.INT8:
		v, err := toInt64(value)
		if err != nil {
			return err
		}
		b.(*array.Int8Builder).Append(int8(v))
	case arrow.UINT64:
		v, err := toUint64(value)
		if err != nil {
			return err
		}
		b.(*array.Uint64Builder).Append(v)
	case arrow.UINT32:
		v, err := toUint64(value)
		if err != nil {
			return err
		}
		b.(*array.Uint32Builder).Append(uint32(v))
	case arrow.UINT16:
		v, err := toUint64(value)
		if err != nil {
			return err
		}
		b.(*array.Uint16Builder).Append(uint16(v))
	case arrow.UINT8:
		v, err := toUint64(value)
		if err != nil {
			return err
		}
		b.(*array.Uint8Builder).Append(uint8(v))
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
	case arrow.LARGE_BINARY:
		v, ok := asBytes(value)
		if !ok {
			return fmt.Errorf("expected []byte for LARGE_BINARY, got %T", value)
		}
		b.(*array.BinaryBuilder).Append(v)
	case arrow.LARGE_STRING:
		s, ok := asString(value)
		if !ok {
			s = fmt.Sprintf("%v", value)
		}
		b.(*array.LargeStringBuilder).Append(s)
	case arrow.FIXED_SIZE_BINARY:
		v, ok := asBytes(value)
		if !ok {
			return fmt.Errorf("expected []byte for FIXED_SIZE_BINARY, got %T", value)
		}
		b.(*array.FixedSizeBinaryBuilder).Append(v)
	case arrow.DATE32:
		t, ok := asTime(value)
		if !ok {
			return fmt.Errorf("expected time.Time for DATE32, got %T", value)
		}
		b.(*array.Date32Builder).Append(arrow.Date32(daysSinceEpoch(t)))
	case arrow.TIMESTAMP:
		t, ok := asTime(value)
		if !ok {
			return fmt.Errorf("expected time.Time for TIMESTAMP, got %T", value)
		}
		b.(*array.TimestampBuilder).Append(arrow.Timestamp(t.UTC().UnixMicro()))
	case arrow.TIME64:
		t, ok := asTime(value)
		if !ok {
			return fmt.Errorf("expected time.Time for TIME64, got %T", value)
		}
		b.(*array.Time64Builder).Append(arrow.Time64(microsSinceMidnight(t)))
	case arrow.DURATION:
		d, ok := asDuration(value)
		if !ok {
			return fmt.Errorf("expected time.Duration for DURATION, got %T", value)
		}
		b.(*array.DurationBuilder).Append(arrow.Duration(d.Microseconds()))
	case arrow.DECIMAL128:
		dec := dt.(*arrow.Decimal128Type)
		n, err := decimalFromValue(value, dec)
		if err != nil {
			return err
		}
		b.(*array.Decimal128Builder).Append(n)
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
