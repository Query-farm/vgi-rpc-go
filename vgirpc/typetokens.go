// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgirpc

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

// Canonical text tokens for Arrow types, for the protocol hash preimage.
//
// The protocol hash is taken over what Arrow *decodes to*, not over what an
// encoder emits: each language's Arrow implementation may legitimately produce
// different bytes for the same logical schema, so a hash over serialized IPC is
// not a cross-language contract. The preimage is canonical JSON (RFC 8785) of
// the decoded description, and these tokens are how a type appears inside it.
//
// JSON solves framing, escaping and key ordering. It does not solve spelling --
// two ports can agree on every JCS rule and still disagree on whether a
// microsecond timestamp is "timestamp[us]" or "timestamp(us)", which is a
// silent hash divergence. So the vocabulary is enumerated exhaustively and
// TypeToken is total over Arrow's type universe: an unrecognised type returns
// an error rather than falling back to DataType.String(), whose output is an
// Arrow implementation detail that differs between ports and across releases.
//
// Grammar: a token is lowercase ASCII. Parameters go in parentheses, children
// in angle brackets. A child is "name:token" when the child field is
// non-nullable and "name?:token" when it is nullable -- child nullability is
// part of the type in Arrow, and two schemas differing only there are different
// schemas. Decimal precision and scale are folded into the token
// ("decimal128(38,9)") so the preimage contains no JSON numbers and RFC 8785's
// hardest rule, number canonicalisation, never applies. Keep it that way.
//
// What is normalised: Arrow's own type equality ignores the *name* of a list's
// child field and of a map's key/value fields -- Go names the list child "item",
// some Parquet producers name it "element". Those names are normalised, because
// keeping them would give two ports different hashes for a protocol Arrow itself
// calls identical. Everything Arrow does treat as part of the type is kept:
// child nullability, struct field names, union child names and type codes,
// dictionary index/value types and orderedness, and map keysSorted.

// UnsupportedArrowTypeError reports an Arrow type with no canonical token.
//
// Returned rather than falling back to DataType.String(): a port that silently
// spelled an unknown type its own way would produce a protocol hash that
// disagrees with every other port, and the disagreement would surface as an
// unexplained mismatch at a client rather than as an error here.
type UnsupportedArrowTypeError struct {
	Type arrow.DataType
}

func (e *UnsupportedArrowTypeError) Error() string {
	return fmt.Sprintf(
		"arrow type %v has no canonical token; add one to typetokens.go and to every "+
			"other port at the same time -- a one-sided addition changes only this port's protocol hash",
		e.Type,
	)
}

// anonChild spells a child whose name Arrow does not consider part of the type.
//
// A list's child is named "item" by Go, "element" by some Parquet producers, and
// whatever the caller passed by anyone constructing the type by hand -- and
// Arrow's own type equality ignores all of it. Normalising to a fixed name is
// what keeps two ports that default differently from hashing the same protocol
// differently. Nullability *is* part of the type, so it is kept.
func anonChild(f arrow.Field, name string) (string, error) {
	tok, err := TypeToken(f.Type)
	if err != nil {
		return "", err
	}
	if f.Nullable {
		return name + "?:" + tok, nil
	}
	return name + ":" + tok, nil
}

// child spells a child field whose name is part of the type.
func child(f arrow.Field) (string, error) {
	return anonChild(f, f.Name)
}

// TypeToken returns the canonical token for dt.
func TypeToken(dt arrow.DataType) (string, error) {
	switch t := dt.(type) {
	case *arrow.NullType:
		return "null", nil
	case *arrow.BooleanType:
		return "bool", nil
	case *arrow.Int8Type:
		return "int8", nil
	case *arrow.Int16Type:
		return "int16", nil
	case *arrow.Int32Type:
		return "int32", nil
	case *arrow.Int64Type:
		return "int64", nil
	case *arrow.Uint8Type:
		return "uint8", nil
	case *arrow.Uint16Type:
		return "uint16", nil
	case *arrow.Uint32Type:
		return "uint32", nil
	case *arrow.Uint64Type:
		return "uint64", nil
	case *arrow.Float16Type:
		return "float16", nil
	case *arrow.Float32Type:
		return "float32", nil
	case *arrow.Float64Type:
		return "float64", nil
	case *arrow.StringType:
		return "utf8", nil
	case *arrow.LargeStringType:
		return "large_utf8", nil
	case *arrow.StringViewType:
		return "utf8_view", nil
	case *arrow.BinaryType:
		return "binary", nil
	case *arrow.LargeBinaryType:
		return "large_binary", nil
	case *arrow.BinaryViewType:
		return "binary_view", nil
	case *arrow.FixedSizeBinaryType:
		return fmt.Sprintf("fixed_size_binary(%d)", t.ByteWidth), nil
	case *arrow.Date32Type:
		return "date32", nil
	case *arrow.Date64Type:
		return "date64", nil
	case *arrow.MonthIntervalType:
		return "interval_months", nil
	case *arrow.DayTimeIntervalType:
		return "interval_day_time", nil
	case *arrow.MonthDayNanoIntervalType:
		return "interval_month_day_nano", nil
	case *arrow.Decimal128Type:
		return fmt.Sprintf("decimal128(%d,%d)", t.Precision, t.Scale), nil
	case *arrow.Decimal256Type:
		return fmt.Sprintf("decimal256(%d,%d)", t.Precision, t.Scale), nil
	case *arrow.Time32Type:
		return fmt.Sprintf("time32(%s)", timeUnitToken(t.Unit)), nil
	case *arrow.Time64Type:
		return fmt.Sprintf("time64(%s)", timeUnitToken(t.Unit)), nil
	case *arrow.TimestampType:
		// The zone is carried verbatim: "UTC" and "+00:00" are distinct Arrow
		// types and must not collapse to one token.
		if t.TimeZone == "" {
			return fmt.Sprintf("timestamp(%s)", timeUnitToken(t.Unit)), nil
		}
		return fmt.Sprintf("timestamp(%s,tz=%s)", timeUnitToken(t.Unit), t.TimeZone), nil
	case *arrow.DurationType:
		return fmt.Sprintf("duration(%s)", timeUnitToken(t.Unit)), nil

	case *arrow.FixedSizeListType:
		c, err := anonChild(t.ElemField(), "item")
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("fixed_size_list(%d)<%s>", t.Len(), c), nil
	case *arrow.LargeListType:
		c, err := anonChild(t.ElemField(), "item")
		if err != nil {
			return "", err
		}
		return "large_list<" + c + ">", nil
	case *arrow.ListType:
		c, err := anonChild(t.ElemField(), "item")
		if err != nil {
			return "", err
		}
		return "list<" + c + ">", nil
	case *arrow.ListViewType:
		c, err := anonChild(t.ElemField(), "item")
		if err != nil {
			return "", err
		}
		return "list_view<" + c + ">", nil
	case *arrow.LargeListViewType:
		c, err := anonChild(t.ElemField(), "item")
		if err != nil {
			return "", err
		}
		return "large_list_view<" + c + ">", nil

	case *arrow.MapType:
		// keysSorted is part of the type in Arrow, so it is part of the token.
		k, err := anonChild(t.KeyField(), "key")
		if err != nil {
			return "", err
		}
		v, err := anonChild(t.ItemField(), "value")
		if err != nil {
			return "", err
		}
		tok := "map<" + k + "," + v + ">"
		if t.KeysSorted {
			tok += ",keys_sorted"
		}
		return tok, nil

	case *arrow.StructType:
		parts := make([]string, 0, t.NumFields())
		for i := 0; i < t.NumFields(); i++ {
			c, err := child(t.Field(i))
			if err != nil {
				return "", err
			}
			parts = append(parts, c)
		}
		return "struct<" + strings.Join(parts, ",") + ">", nil

	case *arrow.DenseUnionType:
		return unionToken("dense_union", t.Fields(), t.TypeCodes())
	case *arrow.SparseUnionType:
		return unionToken("sparse_union", t.Fields(), t.TypeCodes())

	case *arrow.DictionaryType:
		idx, err := TypeToken(t.IndexType)
		if err != nil {
			return "", err
		}
		val, err := TypeToken(t.ValueType)
		if err != nil {
			return "", err
		}
		tok := "dictionary<index:" + idx + ",value:" + val + ">"
		if t.Ordered {
			tok += ",ordered"
		}
		return tok, nil

	case *arrow.RunEndEncodedType:
		ends, err := TypeToken(t.RunEnds())
		if err != nil {
			return "", err
		}
		vals, err := TypeToken(t.Encoded())
		if err != nil {
			return "", err
		}
		return "run_end_encoded<run_ends:" + ends + ",values:" + vals + ">", nil
	}

	// Extension types: the extension name plus its storage, so a reader without
	// the extension registered still sees a type it can compare.
	if ext, ok := dt.(arrow.ExtensionType); ok {
		storage, err := TypeToken(ext.StorageType())
		if err != nil {
			return "", err
		}
		return "extension(" + ext.ExtensionName() + ")<" + storage + ">", nil
	}

	return "", &UnsupportedArrowTypeError{Type: dt}
}

// unionToken spells a union, whose type codes need not be 0..n-1 and so are
// written out rather than implied by position.
func unionToken(kind string, fields []arrow.Field, codes []arrow.UnionTypeCode) (string, error) {
	parts := make([]string, 0, len(fields))
	for i, f := range fields {
		c, err := child(f)
		if err != nil {
			return "", err
		}
		parts = append(parts, fmt.Sprintf("%d=%s", codes[i], c))
	}
	return kind + "<" + strings.Join(parts, ",") + ">", nil
}

// timeUnitToken returns Arrow's own spelling of a time unit.
func timeUnitToken(u arrow.TimeUnit) string {
	switch u {
	case arrow.Second:
		return "s"
	case arrow.Millisecond:
		return "ms"
	case arrow.Microsecond:
		return "us"
	case arrow.Nanosecond:
		return "ns"
	}
	return "unknown"
}

// FieldToken describes one top-level schema field for the hash preimage.
//
// Strings and booleans only, so the preimage carries no JSON numbers.
type FieldToken struct {
	Name     string `json:"name"`
	Nullable bool   `json:"nullable"`
	Type     string `json:"type"`
}

// SchemaTokens describes a schema's fields in declaration order, which is
// significant.
func SchemaTokens(schema *arrow.Schema) ([]FieldToken, error) {
	if schema == nil {
		return []FieldToken{}, nil
	}
	out := make([]FieldToken, 0, len(schema.Fields()))
	for _, f := range schema.Fields() {
		tok, err := TypeToken(f.Type)
		if err != nil {
			return nil, err
		}
		out = append(out, FieldToken{Name: f.Name, Nullable: f.Nullable, Type: tok})
	}
	return out, nil
}
