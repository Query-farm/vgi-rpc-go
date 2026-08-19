// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type pythonClientWorker struct {
	cmd  *exec.Cmd
	port int
}

func startPythonClientWorker(t *testing.T) *pythonClientWorker {
	t.Helper()
	python := os.Getenv("VGI_RPC_PYTHON")
	if python == "" {
		t.Skip("set VGI_RPC_PYTHON to run the Python native-client conformance worker")
	}
	cmd := exec.Command(python, "-m", "vgi_rpc.conformance.client_worker", "--http", "0")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start Python client worker: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatalf("read Python worker readiness: %v", err)
	}
	rawPort, ok := strings.CutPrefix(strings.TrimSpace(line), "PORT:")
	if !ok {
		t.Fatalf("expected PORT:<n>, got %q", line)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		t.Fatalf("parse Python worker port: %v", err)
	}
	address := fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, dialErr := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Python worker did not accept connections on %s: %v", address, dialErr)
		}
		time.Sleep(25 * time.Millisecond)
	}
	return &pythonClientWorker{cmd: cmd, port: port}
}

func typedExchangeSchema() *arrow.Schema {
	tags := arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.BinaryTypes.String, Nullable: true})
	scores := arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Int32, Nullable: true})
	nested := arrow.StructOf(
		arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "scores", Type: scores, Nullable: true},
	)
	return arrow.NewSchema([]arrow.Field{
		{Name: "nullable_float", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "tags", Type: tags, Nullable: true},
		{Name: "category", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int16, ValueType: arrow.BinaryTypes.String}, Nullable: true},
		{Name: "event_time", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 18, Scale: 4}, Nullable: true},
		{Name: "nested", Type: nested, Nullable: true},
	}, nil)
}

func allNullClientBatch(schema *arrow.Schema) arrow.RecordBatch {
	columns := make([]arrow.Array, schema.NumFields())
	for i, field := range schema.Fields() {
		columns[i] = array.MakeArrayOfNull(memory.DefaultAllocator, field.Type, 1)
	}
	batch := array.NewRecordBatch(schema, columns, 1)
	for _, column := range columns {
		column.Release()
	}
	return batch
}

func emptyClientBatch(schema *arrow.Schema) arrow.RecordBatch {
	columns := make([]arrow.Array, schema.NumFields())
	for i, field := range schema.Fields() {
		columns[i] = makeEmptyArray(memory.DefaultAllocator, field.Type)
	}
	batch := array.NewRecordBatch(schema, columns, 0)
	for _, column := range columns {
		column.Release()
	}
	return batch
}

func populatedClientBatch(t *testing.T, schema *arrow.Schema) arrow.RecordBatch {
	t.Helper()
	mem := memory.DefaultAllocator

	floatBuilder := array.NewFloat64Builder(mem)
	floatBuilder.Append(42.5)
	nullableFloat := floatBuilder.NewFloat64Array()
	floatBuilder.Release()

	tagsBuilder := array.NewListBuilderWithField(mem, arrow.Field{Name: "item", Type: arrow.BinaryTypes.String, Nullable: true})
	tagsBuilder.Append(true)
	tagValues := tagsBuilder.ValueBuilder().(*array.StringBuilder)
	tagValues.Append("red")
	tagValues.AppendNull()
	tagValues.Append("blue")
	tags := tagsBuilder.NewListArray()
	tagsBuilder.Release()

	dictionaryBuilder := array.NewDictionaryBuilder(mem, schema.Field(2).Type.(*arrow.DictionaryType))
	if err := dictionaryBuilder.(*array.BinaryDictionaryBuilder).AppendString("alpha"); err != nil {
		t.Fatalf("append dictionary value: %v", err)
	}
	category := dictionaryBuilder.NewDictionaryArray()
	dictionaryBuilder.Release()

	timestampBuilder := array.NewTimestampBuilder(mem, schema.Field(3).Type.(*arrow.TimestampType))
	timestampBuilder.Append(arrow.Timestamp(1_725_000_123_456_789))
	eventTime := timestampBuilder.NewTimestampArray()
	timestampBuilder.Release()

	decimalBuilder := array.NewDecimal128Builder(mem, schema.Field(4).Type.(*arrow.Decimal128Type))
	decimalBuilder.Append(decimal128.FromI64(1_234_567))
	amount := decimalBuilder.NewDecimal128Array()
	decimalBuilder.Release()

	nestedBuilder := array.NewStructBuilder(mem, schema.Field(5).Type.(*arrow.StructType))
	nestedBuilder.Append(true)
	nestedBuilder.FieldBuilder(0).(*array.StringBuilder).Append("node-a")
	scores := nestedBuilder.FieldBuilder(1).(*array.ListBuilder)
	scores.Append(true)
	scoreValues := scores.ValueBuilder().(*array.Int32Builder)
	scoreValues.Append(7)
	scoreValues.AppendNull()
	scoreValues.Append(11)
	nested := nestedBuilder.NewStructArray()
	nestedBuilder.Release()

	columns := []arrow.Array{nullableFloat, tags, category, eventTime, amount, nested}
	batch := array.NewRecordBatch(schema, columns, 1)
	for _, column := range columns {
		column.Release()
	}
	return batch
}

func assertClientEcho(t *testing.T, session *HttpClientStream, expected arrow.RecordBatch) {
	t.Helper()
	actual, err := session.Exchange(context.Background(), expected)
	if err != nil {
		t.Fatalf("exchange typed batch: %v", err)
	}
	defer actual.Release()
	if !actual.Batch.Schema().Equal(expected.Schema()) {
		t.Fatalf("echo schema differs:\nwant %s\n got %s", expected.Schema(), actual.Batch.Schema())
	}
	if actual.Batch.NumRows() != expected.NumRows() {
		t.Fatalf("echo rows = %d, want %d", actual.Batch.NumRows(), expected.NumRows())
	}
	for i := range int(expected.NumCols()) {
		if !array.Equal(actual.Batch.Column(i), expected.Column(i)) {
			t.Fatalf("echoed column %d differs", i)
		}
	}
}

func TestPythonNativeClientTypedExchange(t *testing.T) {
	worker := startPythonClientWorker(t)
	client, err := NewHttpClient(fmt.Sprintf("http://127.0.0.1:%d", worker.port))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	emptySchema := arrow.NewSchema(nil, nil)
	params := emptyBatch(emptySchema)
	defer params.Release()
	schema := typedExchangeSchema()
	streamSchema := ClientStreamSchema{Input: schema, Output: schema}
	session, err := client.OpenExchange(context.Background(), "typed_exchange", params, streamSchema)
	if err != nil {
		t.Fatalf("open typed exchange: %v", err)
	}
	defer session.Close()

	allNull := allNullClientBatch(schema)
	defer allNull.Release()
	assertClientEcho(t, session, allNull)
	empty := emptyClientBatch(schema)
	defer empty.Release()
	assertClientEcho(t, session, empty)
	populated := populatedClientBatch(t, schema)
	defer populated.Release()
	assertClientEcho(t, session, populated)

	// A client declaration that is self-consistent but differs from the
	// worker's contract reaches the strict pre-cast boundary and gets a plain
	// HTTP 400. The response must be drained so this same HttpClient can open a
	// fresh session and successfully continue on a reused connection.
	wrongSchema := arrow.NewSchema([]arrow.Field{{Name: "nullable_float", Type: arrow.PrimitiveTypes.Float64, Nullable: true}}, nil)
	wrongSession, err := client.OpenExchange(context.Background(), "typed_exchange", params, ClientStreamSchema{
		Input:  wrongSchema,
		Output: schema,
	})
	if err != nil {
		t.Fatalf("open intentionally wrong exchange: %v", err)
	}
	wrong := emptyClientBatch(wrongSchema)
	_, err = wrongSession.Exchange(context.Background(), wrong)
	wrong.Release()
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusBadRequest || !strings.Contains(statusErr.Detail, "schema") {
		t.Fatalf("wrong schema error = %T %v, want typed plain HTTP 400 schema error", err, err)
	}
	wrongSession.Close()

	recovered, err := client.OpenExchange(context.Background(), "typed_exchange", params, streamSchema)
	if err != nil {
		t.Fatalf("open recovery exchange: %v", err)
	}
	defer recovered.Close()
	assertClientEcho(t, recovered, empty)
}
