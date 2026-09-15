package vgirpc

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func TestTypeTokenSpellings(t *testing.T) {
	cases := []struct {
		dt   arrow.DataType
		want string
	}{
		{arrow.Null, "null"},
		{arrow.FixedWidthTypes.Boolean, "bool"},
		{arrow.PrimitiveTypes.Int8, "int8"},
		{arrow.PrimitiveTypes.Int64, "int64"},
		{arrow.PrimitiveTypes.Uint32, "uint32"},
		{arrow.PrimitiveTypes.Float64, "float64"},
		{arrow.BinaryTypes.String, "utf8"},
		{arrow.BinaryTypes.LargeString, "large_utf8"},
		{arrow.BinaryTypes.Binary, "binary"},
		{arrow.FixedWidthTypes.Date32, "date32"},
		{&arrow.FixedSizeBinaryType{ByteWidth: 16}, "fixed_size_binary(16)"},
		{&arrow.Decimal128Type{Precision: 38, Scale: 9}, "decimal128(38,9)"},
		{&arrow.Decimal256Type{Precision: 38, Scale: 9}, "decimal256(38,9)"},
		{&arrow.TimestampType{Unit: arrow.Microsecond}, "timestamp(us)"},
		{&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, "timestamp(us,tz=UTC)"},
		{&arrow.Time32Type{Unit: arrow.Millisecond}, "time32(ms)"},
		{&arrow.Time64Type{Unit: arrow.Microsecond}, "time64(us)"},
		{&arrow.DurationType{Unit: arrow.Nanosecond}, "duration(ns)"},
		{arrow.ListOf(arrow.PrimitiveTypes.Int64), "list<item?:int64>"},
		{arrow.LargeListOf(arrow.PrimitiveTypes.Int64), "large_list<item?:int64>"},
		{arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Int64), "fixed_size_list(4)<item?:int64>"},
		{arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64), "map<key:utf8,value?:int64>"},
		{arrow.StructOf(
			arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
		), "struct<a:int32,b?:utf8>"},
		{&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}, "dictionary<index:int8,value:utf8>"},
		{arrow.StructOf(), "struct<>"},
	}
	for _, c := range cases {
		got, err := TypeToken(c.dt)
		if err != nil {
			t.Fatalf("TypeToken(%v): %v", c.dt, err)
		}
		if got != c.want {
			t.Errorf("TypeToken(%v) = %q, want %q", c.dt, got, c.want)
		}
	}
}

func TestListChildNameIsNormalised(t *testing.T) {
	// Arrow's own equality ignores it, so the token must too -- otherwise two
	// ports that default differently hash the same protocol differently.
	named := arrow.ListOfField(arrow.Field{Name: "element", Type: arrow.PrimitiveTypes.Int64, Nullable: true})
	got, err := TypeToken(named)
	if err != nil {
		t.Fatal(err)
	}
	if got != "list<item?:int64>" {
		t.Errorf("got %q, want the normalised name", got)
	}
}

func TestChildNullabilityIsKept(t *testing.T) {
	nonNull := arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Int64, Nullable: false})
	got, _ := TypeToken(nonNull)
	if got != "list<item:int64>" {
		t.Errorf("got %q, want the non-nullable spelling", got)
	}
}
