// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"encoding/json"
	"log"
	"sort"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Describe schema field definitions matching Python introspect._DESCRIBE_FIELDS.
var describeSchema = arrow.NewSchema([]arrow.Field{
	{Name: "name", Type: arrow.BinaryTypes.String},
	{Name: "method_type", Type: arrow.BinaryTypes.String},
	{Name: "doc", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "has_return", Type: &arrow.BooleanType{}},
	{Name: "params_schema_ipc", Type: arrow.BinaryTypes.Binary},
	{Name: "result_schema_ipc", Type: arrow.BinaryTypes.Binary},
	{Name: "param_types_json", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "param_defaults_json", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "has_header", Type: &arrow.BooleanType{}},
	{Name: "header_schema_ipc", Type: arrow.BinaryTypes.Binary, Nullable: true},
}, nil)

// Describe metadata keys.
const (
	MetaProtocolName    = "vgi_rpc.protocol_name"
	MetaDescribeVersion = "vgi_rpc.describe_version"
	DescribeVersion     = "2"
)

// serializeSchema serializes an Arrow schema to IPC format bytes.
func serializeSchema(schema *arrow.Schema) []byte {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	w.Close()
	return buf.Bytes()
}

// buildDescribeBatch builds the __describe__ response batch and metadata.
func (s *Server) buildDescribeBatch() (arrow.RecordBatch, arrow.Metadata) {
	mem := memory.NewGoAllocator()

	// Collect sorted method names
	names := s.availableMethods()
	sort.Strings(names)

	n := len(names)

	nameBuilder := array.NewStringBuilder(mem)
	defer nameBuilder.Release()

	methodTypeBuilder := array.NewStringBuilder(mem)
	defer methodTypeBuilder.Release()

	docBuilder := array.NewStringBuilder(mem)
	defer docBuilder.Release()

	hasReturnBuilder := array.NewBooleanBuilder(mem)
	defer hasReturnBuilder.Release()

	paramsSchemaBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer paramsSchemaBuilder.Release()

	resultSchemaBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer resultSchemaBuilder.Release()

	paramTypesBuilder := array.NewStringBuilder(mem)
	defer paramTypesBuilder.Release()

	paramDefaultsBuilder := array.NewStringBuilder(mem)
	defer paramDefaultsBuilder.Release()

	hasHeaderBuilder := array.NewBooleanBuilder(mem)
	defer hasHeaderBuilder.Release()

	headerSchemaBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer headerSchemaBuilder.Release()

	for _, name := range names {
		info := s.methods[name]

		nameBuilder.Append(name)

		switch info.Type {
		case MethodUnary:
			methodTypeBuilder.Append("unary")
		case MethodProducer, MethodExchange:
			methodTypeBuilder.Append("stream")
		}

		// Doc (nullable)
		docBuilder.AppendNull()

		// has_return — true for valued unary methods, false for void/stream
		hasReturnBuilder.Append(info.Type == MethodUnary && info.ResultType != nil)

		// params_schema_ipc
		paramsSchemaBuilder.Append(serializeSchema(info.ParamsSchema))

		// result_schema_ipc — for streams, serialize the output schema
		if info.OutputSchema != nil {
			resultSchemaBuilder.Append(serializeSchema(info.OutputSchema))
		} else {
			resultSchemaBuilder.Append(serializeSchema(info.ResultSchema))
		}

		// param_types_json
		if info.ParamsSchema.NumFields() > 0 {
			paramTypes := make(map[string]string)
			for i := range info.ParamsSchema.NumFields() {
				f := info.ParamsSchema.Field(i)
				paramTypes[f.Name] = arrowTypeToString(f.Type)
			}
			ptJSON, err := json.Marshal(paramTypes)
			if err != nil {
				log.Printf("vgirpc: failed to marshal param types JSON: %v", err)
				paramTypesBuilder.AppendNull()
			} else {
				paramTypesBuilder.Append(string(ptJSON))
			}
		} else {
			paramTypesBuilder.AppendNull()
		}

		// param_defaults_json — values must be native JSON types, not all strings
		if len(info.ParamDefaults) > 0 {
			typed := make(map[string]any, len(info.ParamDefaults))
			for k, v := range info.ParamDefaults {
				typed[k] = coerceDefaultValue(v, info.ParamsSchema, k)
			}
			pdJSON, err := json.Marshal(typed)
			if err != nil {
				log.Printf("vgirpc: failed to marshal param defaults JSON: %v", err)
				paramDefaultsBuilder.AppendNull()
			} else {
				paramDefaultsBuilder.Append(string(pdJSON))
			}
		} else {
			paramDefaultsBuilder.AppendNull()
		}

		// has_header
		hasHeaderBuilder.Append(info.HasHeader)

		// header_schema_ipc
		if info.HasHeader && info.HeaderSchema != nil {
			headerSchemaBuilder.Append(serializeSchema(info.HeaderSchema))
		} else {
			headerSchemaBuilder.AppendNull()
		}
	}

	cols := make([]arrow.Array, 10)
	cols[0] = nameBuilder.NewArray()
	cols[1] = methodTypeBuilder.NewArray()
	cols[2] = docBuilder.NewArray()
	cols[3] = hasReturnBuilder.NewArray()
	cols[4] = paramsSchemaBuilder.NewArray()
	cols[5] = resultSchemaBuilder.NewArray()
	cols[6] = paramTypesBuilder.NewArray()
	cols[7] = paramDefaultsBuilder.NewArray()
	cols[8] = hasHeaderBuilder.NewArray()
	cols[9] = headerSchemaBuilder.NewArray()
	for _, c := range cols {
		defer c.Release()
	}

	batch := array.NewRecordBatch(describeSchema, cols, int64(n))

	// Build custom metadata
	keys := []string{
		MetaProtocolName,
		MetaRequestVersion,
		MetaDescribeVersion,
	}
	vals := []string{
		"GoRpcServer",
		ProtocolVersion,
		DescribeVersion,
	}
	if s.serverID != "" {
		keys = append(keys, MetaServerID)
		vals = append(vals, s.serverID)
	}

	meta := arrow.NewMetadata(keys, vals)
	return batch, meta
}

// coerceDefaultValue converts a string default to its proper JSON type
// based on the Arrow schema field type.
func coerceDefaultValue(val string, schema *arrow.Schema, fieldName string) any {
	indices := schema.FieldIndices(fieldName)
	if len(indices) == 0 {
		return val
	}
	f := schema.Field(indices[0])
	switch f.Type.ID() {
	case arrow.INT64, arrow.INT32:
		if v, err := strconv.ParseInt(val, 10, 64); err == nil {
			return v
		}
	case arrow.FLOAT64, arrow.FLOAT32:
		if v, err := strconv.ParseFloat(val, 64); err == nil {
			return v
		}
	case arrow.BOOL:
		if v, err := strconv.ParseBool(val); err == nil {
			return v
		}
	}
	return val
}

// arrowTypeToString returns a human-readable type name for an Arrow type.
func arrowTypeToString(dt arrow.DataType) string {
	switch dt.ID() {
	case arrow.STRING:
		return "string"
	case arrow.INT64:
		return "int"
	case arrow.INT32:
		return "int32"
	case arrow.FLOAT64:
		return "float"
	case arrow.FLOAT32:
		return "float32"
	case arrow.BOOL:
		return "bool"
	case arrow.BINARY:
		return "bytes"
	case arrow.LIST:
		lt := dt.(*arrow.ListType)
		return "list[" + arrowTypeToString(lt.Elem()) + "]"
	case arrow.MAP:
		mt := dt.(*arrow.MapType)
		return "dict[" + arrowTypeToString(mt.KeyType()) + ", " + arrowTypeToString(mt.ItemType()) + "]"
	case arrow.DICTIONARY:
		return "enum"
	default:
		return dt.String()
	}
}
