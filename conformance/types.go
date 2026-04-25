// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// --- Annotated result types for echo handlers that return wide Arrow types.
// Each type implements vgirpc.AnnotatedReturn so the registered method's
// result column carries the right Arrow data type.

// DateResult is a date32-typed return value.
type DateResult time.Time

func (DateResult) VgirpcArrowResult() arrow.DataType { return arrow.FixedWidthTypes.Date32 }

// TimestampResult is a naive microsecond timestamp.
type TimestampResult time.Time

func (TimestampResult) VgirpcArrowResult() arrow.DataType {
	return &arrow.TimestampType{Unit: arrow.Microsecond}
}

// TimestampUTCResult is a UTC-tagged microsecond timestamp.
type TimestampUTCResult time.Time

func (TimestampUTCResult) VgirpcArrowResult() arrow.DataType {
	return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
}

// TimeResult is a microsecond time-of-day value.
type TimeResult time.Time

func (TimeResult) VgirpcArrowResult() arrow.DataType { return arrow.FixedWidthTypes.Time64us }

// DurationResult is a microsecond-resolution duration.
type DurationResult time.Duration

func (DurationResult) VgirpcArrowResult() arrow.DataType {
	return arrow.FixedWidthTypes.Duration_us
}

// DecimalResult carries a decimal128(20, 4) value as its string form.
type DecimalResult string

func (DecimalResult) VgirpcArrowResult() arrow.DataType {
	return &arrow.Decimal128Type{Precision: 20, Scale: 4}
}

// LargeStringResult round-trips as a large_utf8 column.
type LargeStringResult string

func (LargeStringResult) VgirpcArrowResult() arrow.DataType {
	return arrow.BinaryTypes.LargeString
}

// LargeBinaryResult round-trips as a large_binary column.
type LargeBinaryResult []byte

func (LargeBinaryResult) VgirpcArrowResult() arrow.DataType {
	return arrow.BinaryTypes.LargeBinary
}

// FixedBinary8Result round-trips as fixed_size_binary(8).
type FixedBinary8Result []byte

func (FixedBinary8Result) VgirpcArrowResult() arrow.DataType {
	return &arrow.FixedSizeBinaryType{ByteWidth: 8}
}

// DictEncodedStringResult round-trips as dictionary<int16, utf8>.
type DictEncodedStringResult string

func (DictEncodedStringResult) VgirpcArrowResult() arrow.DataType {
	return &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int16,
		ValueType: arrow.BinaryTypes.String,
	}
}

// --- Wide-type dataclasses mirroring the Python conformance protocol.

// WideTypes exercises the full breadth of Arrow primitive widths.
type WideTypes struct {
	Int8Field         int64         `arrow:"int8_field"`
	Int16Field        int64         `arrow:"int16_field"`
	Int32Field        int64         `arrow:"int32_field"`
	Uint8Field        uint64        `arrow:"uint8_field"`
	Uint16Field       uint64        `arrow:"uint16_field"`
	Uint32Field       uint64        `arrow:"uint32_field"`
	Uint64Field       uint64        `arrow:"uint64_field"`
	Float32Field      float64       `arrow:"float32_field"`
	DateField         time.Time     `arrow:"date_field"`
	TimestampField    time.Time     `arrow:"timestamp_field"`
	TimestampUtcField time.Time     `arrow:"timestamp_utc_field"`
	TimeField         time.Time     `arrow:"time_field"`
	DurationField     time.Duration `arrow:"duration_field"`
	DecimalField      string        `arrow:"decimal_field"`
	LargeStringField  string        `arrow:"large_string_field"`
	LargeBinaryField  []byte        `arrow:"large_binary_field"`
	FixedBinaryField  []byte        `arrow:"fixed_binary_field"`
}

func (WideTypes) ArrowSchema() *arrow.Schema {
	dec := &arrow.Decimal128Type{Precision: 20, Scale: 4}
	return arrow.NewSchema([]arrow.Field{
		{Name: "int8_field", Type: arrow.PrimitiveTypes.Int8},
		{Name: "int16_field", Type: arrow.PrimitiveTypes.Int16},
		{Name: "int32_field", Type: arrow.PrimitiveTypes.Int32},
		{Name: "uint8_field", Type: arrow.PrimitiveTypes.Uint8},
		{Name: "uint16_field", Type: arrow.PrimitiveTypes.Uint16},
		{Name: "uint32_field", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "uint64_field", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "float32_field", Type: arrow.PrimitiveTypes.Float32},
		{Name: "date_field", Type: arrow.FixedWidthTypes.Date32},
		{Name: "timestamp_field", Type: &arrow.TimestampType{Unit: arrow.Microsecond}},
		{Name: "timestamp_utc_field", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{Name: "time_field", Type: arrow.FixedWidthTypes.Time64us},
		{Name: "duration_field", Type: arrow.FixedWidthTypes.Duration_us},
		{Name: "decimal_field", Type: dec},
		{Name: "large_string_field", Type: arrow.BinaryTypes.LargeString},
		{Name: "large_binary_field", Type: arrow.BinaryTypes.LargeBinary},
		{Name: "fixed_binary_field", Type: &arrow.FixedSizeBinaryType{ByteWidth: 8}},
	}, nil)
}

// ContainerWideTypes exercises wide types nested in list/dict/optional positions.
type ContainerWideTypes struct {
	ListDecimal       []string          `arrow:"list_decimal"`
	ListDate          []time.Time       `arrow:"list_date"`
	ListTimestamp     []time.Time       `arrow:"list_timestamp"`
	OptionalDate      *time.Time        `arrow:"optional_date"`
	OptionalDecimal   *string           `arrow:"optional_decimal"`
	OptionalTimestamp *time.Time        `arrow:"optional_timestamp"`
	DictStrDecimal    map[string]string `arrow:"dict_str_decimal"`
	FrozensetInt      []int64           `arrow:"frozenset_int"`
	ListOptionalInt   []*int64          `arrow:"list_optional_int"`
}

func (ContainerWideTypes) ArrowSchema() *arrow.Schema {
	dec := &arrow.Decimal128Type{Precision: 20, Scale: 4}
	tsNaive := &arrow.TimestampType{Unit: arrow.Microsecond}
	return arrow.NewSchema([]arrow.Field{
		{Name: "list_decimal", Type: arrow.ListOf(dec)},
		{Name: "list_date", Type: arrow.ListOf(arrow.FixedWidthTypes.Date32)},
		{Name: "list_timestamp", Type: arrow.ListOf(tsNaive)},
		{Name: "optional_date", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "optional_decimal", Type: dec, Nullable: true},
		{Name: "optional_timestamp", Type: tsNaive, Nullable: true},
		{Name: "dict_str_decimal", Type: arrow.MapOf(arrow.BinaryTypes.String, dec)},
		{Name: "frozenset_int", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
		{Name: "list_optional_int", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
	}, nil)
}

// DeepNested exercises multi-level nesting and dictionary-encoded strings.
type DeepNested struct {
	ListOfListsDecimal []([]string) `arrow:"list_of_lists_decimal"`
	OptionalListDate   *[]time.Time `arrow:"optional_list_date"`
	DictEncodedString  string       `arrow:"dict_encoded_string"`
	ListOfDictEncoded  []string     `arrow:"list_of_dict_encoded"`
}

func (DeepNested) ArrowSchema() *arrow.Schema {
	dec := &arrow.Decimal128Type{Precision: 20, Scale: 4}
	dict := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int16,
		ValueType: arrow.BinaryTypes.String,
	}
	return arrow.NewSchema([]arrow.Field{
		{Name: "list_of_lists_decimal", Type: arrow.ListOf(arrow.ListOf(dec))},
		{Name: "optional_list_date", Type: arrow.ListOf(arrow.FixedWidthTypes.Date32), Nullable: true},
		{Name: "dict_encoded_string", Type: dict},
		{Name: "list_of_dict_encoded", Type: arrow.ListOf(dict)},
	}, nil)
}

// EmbeddedArrow carries an Arrow RecordBatch and Schema as nested IPC bytes.
type EmbeddedArrow struct {
	Batch  []byte `arrow:"batch"`
	Schema []byte `arrow:"schema"`
}

func (EmbeddedArrow) ArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "batch", Type: arrow.BinaryTypes.Binary},
		{Name: "schema", Type: arrow.BinaryTypes.Binary},
	}, nil)
}

// Status is a string-backed enum matching the Python Status enum.
type Status string

// Point is a simple 2D point.
type Point struct {
	X float64 `arrow:"x"`
	Y float64 `arrow:"y"`
}

func (p Point) ArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Float64},
		{Name: "y", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
}

// BoundingBox contains two nested Points and a label.
type BoundingBox struct {
	TopLeft     Point  `arrow:"top_left"`
	BottomRight Point  `arrow:"bottom_right"`
	Label       string `arrow:"label"`
}

func (b BoundingBox) ArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "top_left", Type: arrow.StructOf(
			arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64},
			arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64},
		)},
		{Name: "bottom_right", Type: arrow.StructOf(
			arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64},
			arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64},
		)},
		{Name: "label", Type: arrow.BinaryTypes.String},
	}, nil)
}

// AllTypes demonstrates comprehensive type coverage.
type AllTypes struct {
	StrField       string            `arrow:"str_field"`
	BytesField     []byte            `arrow:"bytes_field"`
	IntField       int64             `arrow:"int_field"`
	FloatField     float64           `arrow:"float_field"`
	BoolField      bool              `arrow:"bool_field"`
	ListOfInt      []int64           `arrow:"list_of_int"`
	ListOfStr      []string          `arrow:"list_of_str"`
	DictField      map[string]int64  `arrow:"dict_field"`
	EnumField      Status            `arrow:"enum_field"`
	NestedPoint    Point             `arrow:"nested_point"`
	OptionalStr    *string           `arrow:"optional_str"`
	OptionalInt    *int64            `arrow:"optional_int"`
	OptionalNested *Point            `arrow:"optional_nested"`
	ListOfNested   []Point           `arrow:"list_of_nested"`
	AnnotatedInt32 int32             `arrow:"annotated_int32"`
	AnnotatedFloat float32           `arrow:"annotated_float32"`
	NestedList     [][]int64         `arrow:"nested_list"`
	DictStrStr     map[string]string `arrow:"dict_str_str"`
}

func (a AllTypes) ArrowSchema() *arrow.Schema {
	pointStruct := arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64},
	)
	return arrow.NewSchema([]arrow.Field{
		{Name: "str_field", Type: arrow.BinaryTypes.String},
		{Name: "bytes_field", Type: arrow.BinaryTypes.Binary},
		{Name: "int_field", Type: arrow.PrimitiveTypes.Int64},
		{Name: "float_field", Type: arrow.PrimitiveTypes.Float64},
		{Name: "bool_field", Type: &arrow.BooleanType{}},
		{Name: "list_of_int", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
		{Name: "list_of_str", Type: arrow.ListOf(arrow.BinaryTypes.String)},
		{Name: "dict_field", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64)},
		{Name: "enum_field", Type: &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int16,
			ValueType: arrow.BinaryTypes.String,
		}},
		{Name: "nested_point", Type: pointStruct},
		{Name: "optional_str", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "optional_int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "optional_nested", Type: pointStruct, Nullable: true},
		{Name: "list_of_nested", Type: arrow.ListOf(pointStruct)},
		{Name: "annotated_int32", Type: arrow.PrimitiveTypes.Int32},
		{Name: "annotated_float32", Type: arrow.PrimitiveTypes.Float32},
		{Name: "nested_list", Type: arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64))},
		{Name: "dict_str_str", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)},
	}, nil)
}

// ConformanceHeader is the header type for conformance stream methods.
type ConformanceHeader struct {
	TotalExpected int64  `arrow:"total_expected"`
	Description   string `arrow:"description"`
}

func (h ConformanceHeader) ArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "total_expected", Type: arrow.PrimitiveTypes.Int64},
		{Name: "description", Type: arrow.BinaryTypes.String},
	}, nil)
}

// RichHeader is a complex header type with all supported field types.
// It mirrors AllTypes but is used as a stream header (ArrowSerializable).
type RichHeader struct {
	StrField       string            `arrow:"str_field"`
	BytesField     []byte            `arrow:"bytes_field"`
	IntField       int64             `arrow:"int_field"`
	FloatField     float64           `arrow:"float_field"`
	BoolField      bool              `arrow:"bool_field"`
	ListOfInt      []int64           `arrow:"list_of_int"`
	ListOfStr      []string          `arrow:"list_of_str"`
	DictField      map[string]int64  `arrow:"dict_field"`
	EnumField      Status            `arrow:"enum_field"`
	NestedPoint    Point             `arrow:"nested_point"`
	OptionalStr    *string           `arrow:"optional_str"`
	OptionalInt    *int64            `arrow:"optional_int"`
	OptionalNested *Point            `arrow:"optional_nested"`
	ListOfNested   []Point           `arrow:"list_of_nested"`
	NestedList     [][]int64         `arrow:"nested_list"`
	AnnotatedInt32 int32             `arrow:"annotated_int32"`
	AnnotatedFloat float32           `arrow:"annotated_float32"`
	DictStrStr     map[string]string `arrow:"dict_str_str"`
}

func (r RichHeader) ArrowSchema() *arrow.Schema {
	pointStruct := arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64},
	)
	return arrow.NewSchema([]arrow.Field{
		{Name: "str_field", Type: arrow.BinaryTypes.String},
		{Name: "bytes_field", Type: arrow.BinaryTypes.Binary},
		{Name: "int_field", Type: arrow.PrimitiveTypes.Int64},
		{Name: "float_field", Type: arrow.PrimitiveTypes.Float64},
		{Name: "bool_field", Type: &arrow.BooleanType{}},
		{Name: "list_of_int", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
		{Name: "list_of_str", Type: arrow.ListOf(arrow.BinaryTypes.String)},
		{Name: "dict_field", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64)},
		{Name: "enum_field", Type: &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int16,
			ValueType: arrow.BinaryTypes.String,
		}},
		{Name: "nested_point", Type: pointStruct},
		{Name: "optional_str", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "optional_int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "optional_nested", Type: pointStruct, Nullable: true},
		{Name: "list_of_nested", Type: arrow.ListOf(pointStruct)},
		{Name: "nested_list", Type: arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64))},
		{Name: "annotated_int32", Type: arrow.PrimitiveTypes.Int32},
		{Name: "annotated_float32", Type: arrow.PrimitiveTypes.Float32},
		{Name: "dict_str_str", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)},
	}, nil)
}

var statusCycle = []Status{"pending", "active", "closed"}

func buildRichHeader(seed int) RichHeader {
	var optionalStr *string
	if seed%2 == 0 {
		s := fmt.Sprintf("opt-%d", seed)
		optionalStr = &s
	}
	var optionalInt *int64
	if seed%2 == 1 {
		v := int64(seed * 3)
		optionalInt = &v
	}
	var optionalNested *Point
	if seed%3 == 0 {
		p := Point{X: float64(seed), Y: 0.0}
		optionalNested = &p
	}
	return RichHeader{
		StrField:       fmt.Sprintf("seed-%d", seed),
		BytesField:     []byte{byte(seed % 256), byte((seed + 1) % 256), byte((seed + 2) % 256)},
		IntField:       int64(seed * 7),
		FloatField:     float64(seed) * 1.5,
		BoolField:      seed%2 == 0,
		ListOfInt:      []int64{int64(seed), int64(seed + 1), int64(seed + 2)},
		ListOfStr:      []string{fmt.Sprintf("item-%d", seed), fmt.Sprintf("item-%d", seed+1)},
		DictField:      map[string]int64{"a": int64(seed), "b": int64(seed + 1)},
		EnumField:      statusCycle[seed%3],
		NestedPoint:    Point{X: float64(seed), Y: float64(seed * 2)},
		OptionalStr:    optionalStr,
		OptionalInt:    optionalInt,
		OptionalNested: optionalNested,
		ListOfNested:   []Point{{X: float64(seed), Y: float64(seed + 1)}},
		NestedList:     [][]int64{{int64(seed), int64(seed + 1)}, {int64(seed + 2)}},
		AnnotatedInt32: int32(seed % 1000),
		AnnotatedFloat: float32(seed) / 3.0,
		DictStrStr:     map[string]string{"key": fmt.Sprintf("val-%d", seed)},
	}
}

func buildDynamicSchema(includeStrings, includeFloats bool) *arrow.Schema {
	fields := []arrow.Field{{Name: "index", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}
	if includeStrings {
		fields = append(fields, arrow.Field{Name: "label", Type: arrow.BinaryTypes.String, Nullable: true})
	}
	if includeFloats {
		fields = append(fields, arrow.Field{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true})
	}
	return arrow.NewSchema(fields, nil)
}

// counterSchema is the Arrow schema for producer stream counter batches.
var counterSchema = arrow.NewSchema([]arrow.Field{
	{Name: "index", Type: arrow.PrimitiveTypes.Int64},
	{Name: "value", Type: arrow.PrimitiveTypes.Int64},
}, nil)
