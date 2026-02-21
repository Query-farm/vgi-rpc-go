package conformance

import (
	"github.com/apache/arrow-go/v18/arrow"
)

// Status is a string-backed enum matching the Python Status enum.
type Status string

const (
	StatusPending Status = "PENDING"
	StatusActive  Status = "ACTIVE"
	StatusClosed  Status = "CLOSED"
)

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
	StrField        string            `arrow:"str_field"`
	BytesField      []byte            `arrow:"bytes_field"`
	IntField        int64             `arrow:"int_field"`
	FloatField      float64           `arrow:"float_field"`
	BoolField       bool              `arrow:"bool_field"`
	ListOfInt       []int64           `arrow:"list_of_int"`
	ListOfStr       []string          `arrow:"list_of_str"`
	DictField       map[string]int64  `arrow:"dict_field"`
	EnumField       Status            `arrow:"enum_field"`
	NestedPoint     Point             `arrow:"nested_point"`
	OptionalStr     *string           `arrow:"optional_str"`
	OptionalInt     *int64            `arrow:"optional_int"`
	OptionalNested  *Point            `arrow:"optional_nested"`
	ListOfNested    []Point           `arrow:"list_of_nested"`
	AnnotatedInt32  int32             `arrow:"annotated_int32"`
	AnnotatedFloat  float32           `arrow:"annotated_float32"`
	NestedList      [][]int64         `arrow:"nested_list"`
	DictStrStr      map[string]string `arrow:"dict_str_str"`
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

// Counter schema for producer streams.
var CounterSchema = arrow.NewSchema([]arrow.Field{
	{Name: "index", Type: arrow.PrimitiveTypes.Int64},
	{Name: "value", Type: arrow.PrimitiveTypes.Int64},
}, nil)
