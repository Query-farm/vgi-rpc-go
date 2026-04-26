// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// describeSchema mirrors Python's slim _DESCRIBE_FIELDS for DESCRIBE_VERSION 4.
// Python-flavoured fields (doc, param_types_json, param_defaults_json,
// param_docs_json) are not on the wire — the Arrow schema IPC bytes are the
// authoritative type information; everything else is source-level metadata
// that callers should consult the Protocol class for.
var describeSchema = arrow.NewSchema([]arrow.Field{
	{Name: "name", Type: arrow.BinaryTypes.String},
	{Name: "method_type", Type: arrow.BinaryTypes.String},
	{Name: "has_return", Type: &arrow.BooleanType{}},
	{Name: "params_schema_ipc", Type: arrow.BinaryTypes.Binary},
	{Name: "result_schema_ipc", Type: arrow.BinaryTypes.Binary},
	{Name: "has_header", Type: &arrow.BooleanType{}},
	{Name: "header_schema_ipc", Type: arrow.BinaryTypes.Binary, Nullable: true},
	{Name: "is_exchange", Type: &arrow.BooleanType{}, Nullable: true},
}, nil)

// Describe metadata keys used in __describe__ introspection responses.
const (
	// MetaProtocolName identifies the server implementation in describe responses.
	MetaProtocolName = "vgi_rpc.protocol_name"
	// MetaDescribeVersion carries the describe schema version for forwards
	// compatibility.
	MetaDescribeVersion = "vgi_rpc.describe_version"
	// MetaProtocolHash carries a SHA-256 hex digest of the canonical describe
	// payload — stable across processes/builds that expose the same Protocol.
	MetaProtocolHash = "vgi_rpc.protocol_hash"
	// DescribeVersion is the current describe schema version string.
	DescribeVersion = "4"
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
	mem := defaultAllocator()

	// Collect sorted method names
	names := s.availableMethods()
	sort.Strings(names)

	n := len(names)

	nameBuilder := array.NewStringBuilder(mem)
	defer nameBuilder.Release()

	methodTypeBuilder := array.NewStringBuilder(mem)
	defer methodTypeBuilder.Release()

	hasReturnBuilder := array.NewBooleanBuilder(mem)
	defer hasReturnBuilder.Release()

	paramsSchemaBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer paramsSchemaBuilder.Release()

	resultSchemaBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer resultSchemaBuilder.Release()

	hasHeaderBuilder := array.NewBooleanBuilder(mem)
	defer hasHeaderBuilder.Release()

	headerSchemaBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer headerSchemaBuilder.Release()

	isExchangeBuilder := array.NewBooleanBuilder(mem)
	defer isExchangeBuilder.Release()

	// Per-row metadata snapshots are also fed to the hash, in the same order.
	methodTypeStrs := make([]string, 0, n)
	hasReturns := make([]bool, 0, n)
	hasHeaders := make([]bool, 0, n)
	isExchanges := make([]int8, 0, n) // -1 == null, 0 == false, 1 == true
	paramsSchemaIPC := make([][]byte, 0, n)
	resultSchemaIPC := make([][]byte, 0, n)
	headerSchemaIPC := make([][]byte, 0, n)

	for _, name := range names {
		info := s.methods[name]

		nameBuilder.Append(name)

		var methodTypeStr string
		switch info.Type {
		case MethodUnary:
			methodTypeStr = "unary"
		case MethodProducer, MethodExchange, MethodDynamic:
			methodTypeStr = "stream"
		}
		methodTypeBuilder.Append(methodTypeStr)
		methodTypeStrs = append(methodTypeStrs, methodTypeStr)

		hasReturn := info.Type == MethodUnary && info.ResultType != nil
		hasReturnBuilder.Append(hasReturn)
		hasReturns = append(hasReturns, hasReturn)

		paramsBytes := serializeSchema(info.ParamsSchema)
		paramsSchemaBuilder.Append(paramsBytes)
		paramsSchemaIPC = append(paramsSchemaIPC, paramsBytes)

		var resultBytes []byte
		if info.OutputSchema != nil {
			resultBytes = serializeSchema(info.OutputSchema)
		} else {
			resultBytes = serializeSchema(info.ResultSchema)
		}
		resultSchemaBuilder.Append(resultBytes)
		resultSchemaIPC = append(resultSchemaIPC, resultBytes)

		hasHeaderBuilder.Append(info.HasHeader)
		hasHeaders = append(hasHeaders, info.HasHeader)

		var headerBytes []byte
		if info.HasHeader && info.HeaderSchema != nil {
			headerBytes = serializeSchema(info.HeaderSchema)
			headerSchemaBuilder.Append(headerBytes)
		} else {
			headerSchemaBuilder.AppendNull()
		}
		headerSchemaIPC = append(headerSchemaIPC, headerBytes)

		var isExchangeFlag int8 = -1
		switch info.Type {
		case MethodExchange:
			isExchangeBuilder.Append(true)
			isExchangeFlag = 1
		case MethodProducer:
			isExchangeBuilder.Append(false)
			isExchangeFlag = 0
		default:
			isExchangeBuilder.AppendNull()
		}
		isExchanges = append(isExchanges, isExchangeFlag)
	}

	cols := make([]arrow.Array, 8)
	cols[0] = nameBuilder.NewArray()
	cols[1] = methodTypeBuilder.NewArray()
	cols[2] = hasReturnBuilder.NewArray()
	cols[3] = paramsSchemaBuilder.NewArray()
	cols[4] = resultSchemaBuilder.NewArray()
	cols[5] = hasHeaderBuilder.NewArray()
	cols[6] = headerSchemaBuilder.NewArray()
	cols[7] = isExchangeBuilder.NewArray()
	for _, c := range cols {
		defer c.Release()
	}

	batch := array.NewRecordBatch(describeSchema, cols, int64(n))

	protocolName := s.serviceName
	if protocolName == "" {
		protocolName = "GoRpcServer"
	}

	hash := computeProtocolHash(
		protocolName, names, methodTypeStrs, hasReturns, hasHeaders, isExchanges,
		paramsSchemaIPC, resultSchemaIPC, headerSchemaIPC,
	)

	keys := []string{
		MetaProtocolName,
		MetaRequestVersion,
		MetaDescribeVersion,
		MetaProtocolHash,
	}
	vals := []string{
		protocolName,
		ProtocolVersion,
		DescribeVersion,
		hash,
	}
	if s.serverID != "" {
		keys = append(keys, MetaServerID)
		vals = append(vals, s.serverID)
	}

	meta := arrow.NewMetadata(keys, vals)
	return batch, meta
}

// computeProtocolHash returns the SHA-256 hex digest of the canonical
// describe payload, byte-identical to the Python compute_protocol_hash
// implementation in vgi_rpc/introspect.py.
//
// Inputs are pre-extracted from the per-method describe rows in sorted-name
// order so callers don't need a second pass over the Arrow batch.
func computeProtocolHash(
	protocolName string,
	names, methodTypes []string,
	hasReturns, hasHeaders []bool,
	isExchanges []int8,
	paramsIPC, resultIPC, headerIPC [][]byte,
) string {
	h := sha256.New()
	h.Write([]byte("vgi_rpc.describe.v"))
	h.Write([]byte(DescribeVersion))
	h.Write([]byte("|"))
	h.Write([]byte(ProtocolVersion))
	h.Write([]byte("|"))
	h.Write([]byte(protocolName))
	h.Write([]byte("|"))
	for i := range names {
		h.Write([]byte{0x1f})
		h.Write([]byte(names[i]))
		h.Write([]byte{0x1e})
		h.Write([]byte(methodTypes[i]))
		h.Write([]byte{0x1e})
		if hasReturns[i] {
			h.Write([]byte("1"))
		} else {
			h.Write([]byte("0"))
		}
		h.Write([]byte{0x1e})
		if hasHeaders[i] {
			h.Write([]byte("1"))
		} else {
			h.Write([]byte("0"))
		}
		h.Write([]byte{0x1e})
		switch isExchanges[i] {
		case 1:
			h.Write([]byte("1"))
		case 0:
			h.Write([]byte("0"))
		default:
			h.Write([]byte("-"))
		}
		h.Write([]byte{0x1e})
		h.Write(paramsIPC[i])
		h.Write([]byte{0x1e})
		h.Write(resultIPC[i])
		h.Write([]byte{0x1e})
		if len(headerIPC[i]) > 0 {
			h.Write(headerIPC[i])
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}
