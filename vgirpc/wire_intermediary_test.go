// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Tests for the public intermediary wire helpers, translated from the
// Python reference's tests/test_wire.py.

package vgirpc

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

var wireTestSchema = arrow.NewSchema([]arrow.Field{
	{Name: "request", Type: arrow.BinaryTypes.Binary},
}, nil)

// makeBinaryParamsBatch builds a one-row {request: binary} params batch.
func makeBinaryParamsBatch(t *testing.T, payload []byte) arrow.RecordBatch {
	t.Helper()
	b := array.NewBinaryBuilder(defaultAllocator(), arrow.BinaryTypes.Binary)
	defer b.Release()
	b.Append(payload)
	arr := b.NewArray()
	defer arr.Release()
	return array.NewRecordBatch(wireTestSchema, []arrow.Array{arr}, 1)
}

// makeStream writes a single zero-row batch under schema, optionally
// carrying a stream-state token in its custom_metadata.
func makeStream(t *testing.T, schema *arrow.Schema, token string) []byte {
	t.Helper()
	batch := emptyBatch(schema)
	defer batch.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if token != "" {
		meta := arrow.NewMetadata([]string{MetaStreamState}, []string{token})
		withMeta := array.NewRecordBatchWithMetadata(schema, batch.Columns(), 0, meta)
		defer withMeta.Release()
		if err := w.Write(withMeta); err != nil {
			t.Fatal(err)
		}
	} else if err := w.Write(batch); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestWriteReadRequestRoundTrip(t *testing.T) {
	params := makeBinaryParamsBatch(t, []byte("payload-bytes"))
	defer params.Release()

	var buf bytes.Buffer
	if err := WriteRequest(&buf, "bind", params, ""); err != nil {
		t.Fatal(err)
	}

	req, err := ReadRequest(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer req.Batch.Release()

	if req.Method != "bind" {
		t.Fatalf("expected method bind, got %q", req.Method)
	}
	got := req.Batch.Column(0).(*array.Binary).Value(0)
	if !bytes.Equal(got, []byte("payload-bytes")) {
		t.Fatalf("expected payload-bytes, got %q", got)
	}
}

func TestWriteRequestPreservesProtocolVersion(t *testing.T) {
	params := makeBinaryParamsBatch(t, []byte("x"))
	defer params.Release()

	var buf bytes.Buffer
	if err := WriteRequest(&buf, "init", params, "2.3"); err != nil {
		t.Fatal(err)
	}

	req, err := ReadRequest(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer req.Batch.Release()

	if got := req.Metadata[MetaProtocolVersion]; got != "2.3" {
		t.Fatalf("expected protocol_version 2.3, got %q", got)
	}
}

func TestWriteRequestOmitsProtocolVersionWhenEmpty(t *testing.T) {
	params := makeBinaryParamsBatch(t, []byte("x"))
	defer params.Release()

	var buf bytes.Buffer
	if err := WriteRequest(&buf, "init", params, ""); err != nil {
		t.Fatal(err)
	}

	req, err := ReadRequest(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer req.Batch.Release()

	if _, present := req.Metadata[MetaProtocolVersion]; present {
		t.Fatal("expected no protocol_version key on the request batch")
	}
}

func TestWriteErrorResponseEncodesError(t *testing.T) {
	var buf bytes.Buffer
	rpcErr := &RpcError{Type: "PermissionError", Message: "denied: nope"}
	if err := WriteErrorResponse(&buf, nil, rpcErr, "", ""); err != nil {
		t.Fatal(err)
	}

	reader, err := ipc.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatal("expected one batch in the error stream")
	}
	rb, ok := reader.RecordBatch().(arrow.RecordBatchWithMetadata)
	if !ok {
		t.Fatal("expected batch custom metadata")
	}
	if level, _ := rb.Metadata().GetValue(MetaLogLevel); level != string(LogException) {
		t.Fatalf("expected EXCEPTION log level, got %q", level)
	}
	if msg, _ := rb.Metadata().GetValue(MetaLogMessage); !bytes.Contains([]byte(msg), []byte("denied: nope")) {
		t.Fatalf("expected error message in log_message, got %q", msg)
	}
}

func TestWriteErrorResponseDefaultsToEmptySchema(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteErrorResponse(&buf, nil, &RpcError{Type: "ValueError", Message: "boom"}, "", ""); err != nil {
		t.Fatal(err)
	}
	reader, err := ipc.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	if got := reader.Schema().NumFields(); got != 0 {
		t.Fatalf("expected an empty schema, got %d fields", got)
	}
}

func TestFindStateTokenInSingleStreamRequest(t *testing.T) {
	body := makeStream(t, arrow.NewSchema(nil, nil), "TOK")
	if got := FindStateToken(body); !bytes.Equal(got, []byte("TOK")) {
		t.Fatalf("expected TOK, got %q", got)
	}
}

func TestFindStateTokenWalksConcatenatedResponseStreams(t *testing.T) {
	// A producer response is header-stream ++ data-stream; the token is in
	// the latter.
	headerSchema := arrow.NewSchema([]arrow.Field{
		{Name: "execution_id", Type: arrow.BinaryTypes.String},
	}, nil)
	dataSchema := arrow.NewSchema([]arrow.Field{
		{Name: "v", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	header := makeStream(t, headerSchema, "") // no token
	data := makeStream(t, dataSchema, "TOK2")
	if got := FindStateToken(append(header, data...)); !bytes.Equal(got, []byte("TOK2")) {
		t.Fatalf("expected TOK2, got %q", got)
	}
}

func TestFindStateTokenAbsentOrUnparseable(t *testing.T) {
	if got := FindStateToken(nil); got != nil {
		t.Fatalf("expected nil for empty body, got %q", got)
	}
	if got := FindStateToken(makeStream(t, arrow.NewSchema(nil, nil), "")); got != nil {
		t.Fatalf("expected nil for tokenless stream, got %q", got)
	}
	if got := FindStateToken([]byte("not-an-ipc-stream")); got != nil {
		t.Fatalf("expected nil for junk bytes, got %q", got)
	}
}

// makeSplitStateStream frames an /init response from a server that splits
// its stream state: one zero-row sentinel carrying both the cursor and the
// call token, as the Python reference emits.
func makeSplitStateStream(t *testing.T, schema *arrow.Schema, token, callToken string) []byte {
	t.Helper()
	batch := emptyBatch(schema)
	defer batch.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	meta := arrow.NewMetadata(
		[]string{MetaStreamState, MetaCallState},
		[]string{token, callToken},
	)
	withMeta := array.NewRecordBatchWithMetadata(schema, batch.Columns(), 0, meta)
	defer withMeta.Release()
	if err := w.Write(withMeta); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestFindStreamTokensRecoversBothHalves(t *testing.T) {
	body := makeSplitStateStream(t, arrow.NewSchema(nil, nil), "CURSOR", "CALL")
	state, callState := FindStreamTokens(body)
	if !bytes.Equal(state, []byte("CURSOR")) {
		t.Fatalf("expected cursor CURSOR, got %q", state)
	}
	if !bytes.Equal(callState, []byte("CALL")) {
		t.Fatalf("expected call token CALL, got %q", callState)
	}
	// The single-token accessors read the same walk.
	if got := FindStateToken(body); !bytes.Equal(got, []byte("CURSOR")) {
		t.Fatalf("FindStateToken: expected CURSOR, got %q", got)
	}
	if got := FindCallStateToken(body); !bytes.Equal(got, []byte("CALL")) {
		t.Fatalf("FindCallStateToken: expected CALL, got %q", got)
	}
}

func TestFindStreamTokensWalksConcatenatedStreams(t *testing.T) {
	headerSchema := arrow.NewSchema([]arrow.Field{
		{Name: "execution_id", Type: arrow.BinaryTypes.String},
	}, nil)
	header := makeStream(t, headerSchema, "") // no tokens
	data := makeSplitStateStream(t, arrow.NewSchema(nil, nil), "CURSOR2", "CALL2")
	state, callState := FindStreamTokens(append(header, data...))
	if !bytes.Equal(state, []byte("CURSOR2")) {
		t.Fatalf("expected cursor CURSOR2, got %q", state)
	}
	if !bytes.Equal(callState, []byte("CALL2")) {
		t.Fatalf("expected call token CALL2, got %q", callState)
	}
}

func TestFindStreamTokensCallStateAbsent(t *testing.T) {
	// This port's own server does not split yet, so a body it produced
	// yields no call token. The helper must report that as absence rather
	// than fail, since it also parses bodies from non-vgi peers.
	body := makeStream(t, arrow.NewSchema(nil, nil), "TOK")
	state, callState := FindStreamTokens(body)
	if !bytes.Equal(state, []byte("TOK")) {
		t.Fatalf("expected TOK, got %q", state)
	}
	if callState != nil {
		t.Fatalf("expected no call token, got %q", callState)
	}
	if got := FindCallStateToken([]byte("not-an-ipc-stream")); got != nil {
		t.Fatalf("expected nil for junk bytes, got %q", got)
	}
}

func TestFindProtocolVersion(t *testing.T) {
	params := makeBinaryParamsBatch(t, []byte("x"))
	defer params.Release()

	var stamped bytes.Buffer
	if err := WriteRequest(&stamped, "bind", params, "3.1"); err != nil {
		t.Fatal(err)
	}
	if got := FindProtocolVersion(stamped.Bytes()); got != "3.1" {
		t.Fatalf("expected 3.1, got %q", got)
	}

	var bare bytes.Buffer
	if err := WriteRequest(&bare, "bind", params, ""); err != nil {
		t.Fatal(err)
	}
	if got := FindProtocolVersion(bare.Bytes()); got != "" {
		t.Fatalf("expected empty version, got %q", got)
	}

	if got := FindProtocolVersion([]byte("junk")); got != "" {
		t.Fatalf("expected empty version for junk, got %q", got)
	}
}

func TestUnaryResultRoundTrip(t *testing.T) {
	envelope := arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.BinaryTypes.Binary},
	}, nil)

	var buf bytes.Buffer
	if err := WriteUnaryResult(&buf, envelope, []byte("serialized-response")); err != nil {
		t.Fatal(err)
	}

	schema, result, ok := ReadUnaryResult(buf.Bytes())
	if !ok {
		t.Fatal("expected ok for a unary result stream")
	}
	if schema.NumFields() != 1 || schema.Field(0).Name != "result" {
		t.Fatalf("expected [result] envelope schema, got %s", schema)
	}
	if !bytes.Equal(result, []byte("serialized-response")) {
		t.Fatalf("expected serialized-response, got %q", result)
	}
}

func TestReadUnaryResultSkipsLogBatches(t *testing.T) {
	envelope := arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.BinaryTypes.Binary},
	}, nil)

	b := array.NewBinaryBuilder(defaultAllocator(), arrow.BinaryTypes.Binary)
	defer b.Release()
	b.Append([]byte("after-logs"))
	arr := b.NewArray()
	defer arr.Release()
	batch := array.NewRecordBatch(envelope, []arrow.Array{arr}, 1)
	defer batch.Release()

	var buf bytes.Buffer
	logs := []LogMessage{{Level: LogInfo, Message: "processing"}}
	if err := WriteUnaryResponse(&buf, envelope, logs, batch, "", ""); err != nil {
		t.Fatal(err)
	}

	_, result, ok := ReadUnaryResult(buf.Bytes())
	if !ok {
		t.Fatal("expected ok for a log-prefixed unary result stream")
	}
	if !bytes.Equal(result, []byte("after-logs")) {
		t.Fatalf("expected after-logs, got %q", result)
	}
}

func TestReadUnaryResultNotOkForNonResultStream(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	ib := array.NewInt64Builder(defaultAllocator())
	defer ib.Release()
	ib.Append(1)
	arr := ib.NewArray()
	defer arr.Release()
	batch := array.NewRecordBatch(schema, []arrow.Array{arr}, 1)
	defer batch.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := w.Write(batch); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if _, _, ok := ReadUnaryResult(buf.Bytes()); ok {
		t.Fatal("expected not-ok for a data batch without a result column")
	}
}

func TestReadUnaryResultNotOkForErrorStream(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteErrorResponse(&buf, nil, &RpcError{Type: "ValueError", Message: "boom"}, "", ""); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := ReadUnaryResult(buf.Bytes()); ok {
		t.Fatal("expected not-ok for an error stream")
	}
}
