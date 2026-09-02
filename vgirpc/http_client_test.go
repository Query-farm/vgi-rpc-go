// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type clientLifecycleProducer struct {
	emitted bool
}

func newResponseBudgetTestServer(handler http.HandlerFunc) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(acceptMaxResponseBytesSupportHeader, "true")
		if r.Method == http.MethodOptions && strings.HasSuffix(r.URL.Path, "/health") {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		handler(w, r)
	}))
}

func (s *clientLifecycleProducer) Produce(_ context.Context, out *OutputCollector, _ *CallContext) error {
	if s.emitted {
		return out.Finish()
	}
	s.emitted = true
	builder := array.NewInt32Builder(memory.DefaultAllocator)
	builder.Append(7)
	values := builder.NewInt32Array()
	builder.Release()
	defer values.Release()
	return out.EmitArrays([]arrow.Array{values}, 1)
}

func TestHttpClientUnaryAndProducerLifecycle(t *testing.T) {
	producerSchema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
	server := NewServer()
	Unary(server, "greet", func(context.Context, *CallContext, struct{}) (string, error) {
		return "hello", nil
	})
	Producer(server, "numbers", producerSchema, func(context.Context, *CallContext, struct{}) (*StreamResult, error) {
		return &StreamResult{OutputSchema: producerSchema, State: &clientLifecycleProducer{}}, nil
	})
	httpServer := httptest.NewServer(NewHttpServer(server))
	defer httpServer.Close()
	client, err := NewHttpClient(httpServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	params := emptyBatch(arrow.NewSchema(nil, nil))
	defer params.Release()

	unarySchema, err := resultSchema(reflect.TypeOf(""))
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.CallUnary(context.Background(), "greet", params, unarySchema)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Batch.Column(0).(*array.String).Value(0); got != "hello" {
		t.Fatalf("unary result = %q, want hello", got)
	}
	result.Release()

	stream, err := client.OpenProducer(context.Background(), "numbers", params, ClientStreamSchema{Output: producerSchema})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	batch, ok, err := stream.Next(context.Background())
	if err != nil || !ok {
		t.Fatalf("producer first batch: ok=%v err=%v", ok, err)
	}
	if got := batch.Batch.Column(0).(*array.Int32).Value(0); got != 7 {
		t.Fatalf("producer result = %d, want 7", got)
	}
	batch.Release()
	if batch, ok, err := stream.Next(context.Background()); err != nil || ok || batch != nil {
		t.Fatalf("producer end: batch=%v ok=%v err=%v", batch, ok, err)
	}
}

func TestHttpClientRejectsMultipleProducerBatchesPerTurn(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
	builder := array.NewInt32Builder(memory.DefaultAllocator)
	builder.AppendValues([]int32{1, 2}, nil)
	values := builder.NewInt32Array()
	builder.Release()
	firstValues := array.NewSlice(values, 0, 1)
	secondValues := array.NewSlice(values, 1, 2)
	first := array.NewRecordBatch(schema, []arrow.Array{firstValues}, 1)
	second := array.NewRecordBatch(schema, []arrow.Array{secondValues}, 1)
	firstValues.Release()
	secondValues.Release()
	values.Release()
	var body bytes.Buffer
	writer := ipc.NewWriter(&body, ipc.WithSchema(schema))
	if err := writer.Write(first); err != nil {
		t.Fatal(err)
	}
	if err := writer.Write(second); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	first.Release()
	second.Release()

	server := newResponseBudgetTestServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", arrowContentType)
		_, _ = w.Write(body.Bytes())
	}))
	defer server.Close()
	client, err := NewHttpClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	params := emptyBatch(arrow.NewSchema(nil, nil))
	defer params.Release()
	_, err = client.OpenProducer(context.Background(), "numbers", params, ClientStreamSchema{Output: schema})
	var rpcErr *RpcError
	if !errors.As(err, &rpcErr) || rpcErr.Type != "ProtocolError" {
		t.Fatalf("OpenProducer error = %v, want ProtocolError", err)
	}
}

func TestHttpClientParses200ExceptionEnvelope(t *testing.T) {
	server := NewServer()
	Unary(server, "fail", func(context.Context, *CallContext, struct{}) (string, error) {
		return "", &RpcError{Type: "ValueError", Message: "deliberate failure"}
	})
	httpServer := httptest.NewServer(NewHttpServer(server))
	defer httpServer.Close()

	client, err := NewHttpClient(httpServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	params := emptyBatch(arrow.NewSchema(nil, nil))
	defer params.Release()
	_, err = client.CallUnary(context.Background(), "fail", params, nil)
	var rpcErr *RpcError
	if !errors.As(err, &rpcErr) {
		t.Fatalf("error = %T %v, want RpcError", err, err)
	}
	if rpcErr.Type != "ValueError" || !strings.Contains(rpcErr.Message, "deliberate failure") {
		t.Fatalf("unexpected RPC error: %#v", rpcErr)
	}
}

func TestHttpClientRejectsRPCErrorHeaderWithoutException(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
	builder := array.NewInt32Builder(memory.DefaultAllocator)
	builder.Append(7)
	values := builder.NewInt32Array()
	builder.Release()
	batch := array.NewRecordBatch(schema, []arrow.Array{values}, 1)
	values.Release()
	body, err := encodeClientBatch(batch, nil, 1<<20)
	batch.Release()
	if err != nil {
		t.Fatal(err)
	}
	server := newResponseBudgetTestServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", arrowContentType)
		w.Header().Set(rpcErrorHeader, "true")
		_, _ = w.Write(body)
	}))
	defer server.Close()
	client, err := NewHttpClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	params := emptyBatch(arrow.NewSchema(nil, nil))
	defer params.Release()
	_, err = client.CallUnary(context.Background(), "method", params, schema)
	if err == nil || !strings.Contains(err.Error(), "declared an RPC error") {
		t.Fatalf("error = %v, want missing exception-envelope rejection", err)
	}
}

func TestHttpClientPoisonsAmbiguousExchange(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	initBatch := emptyBatch(schema)
	initBody, err := encodeClientBatch(initBatch, map[string]string{
		MetaStreamState: "cursor-one",
		MetaCallState:   "call-one",
	}, 1<<20)
	initBatch.Release()
	if err != nil {
		t.Fatal(err)
	}
	var exchanges atomic.Int64
	server := newResponseBudgetTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", arrowContentType)
		if strings.HasSuffix(r.URL.Path, "/init") {
			_, _ = w.Write(initBody)
			return
		}
		exchanges.Add(1)
		// The request may have reached and mutated the worker, but the response
		// is truncated. Reusing cursor-one would duplicate that exchange.
		_, _ = w.Write([]byte{0xff, 0x00, 0x01})
	}))
	defer server.Close()
	client, err := NewHttpClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	params := emptyBatch(arrow.NewSchema(nil, nil))
	defer params.Release()
	session, err := client.OpenExchange(context.Background(), "echo", params, ClientStreamSchema{Input: schema, Output: schema})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	input := emptyBatch(schema)
	defer input.Release()
	if _, err := session.Exchange(context.Background(), input); err == nil {
		t.Fatal("ambiguous exchange unexpectedly succeeded")
	}
	if _, err := session.Exchange(context.Background(), input); err == nil || !strings.Contains(err.Error(), "no continuation token") {
		t.Fatalf("second exchange error = %v, want poisoned-session rejection", err)
	}
	if got := exchanges.Load(); got != 1 {
		t.Fatalf("exchange HTTP requests = %d, want 1", got)
	}
}

func TestHttpClientInitialMetadataPreservedAndReservedOverwritten(t *testing.T) {
	client, err := NewHttpClient("http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	schema := arrow.NewSchema(nil, nil)
	base := emptyBatch(schema)
	metadata := arrow.NewMetadata(
		[]string{"user-key", MetaMethod, MetaRequestVersion, MetaStreamState, MetaCancel},
		[]string{"user-value", "wrong", "wrong", "injected-cursor", "1"},
	)
	annotated := array.NewRecordBatchWithMetadata(schema, base.Columns(), 0, metadata)
	base.Release()
	body, err := client.initialBody("right", annotated)
	annotated.Release()
	if err != nil {
		t.Fatal(err)
	}
	request, err := ReadRequest(bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer request.Batch.Release()
	if request.Method != "right" || request.Version != ProtocolVersion {
		t.Fatalf("reserved metadata not overwritten: %#v", request.Metadata)
	}
	if request.Metadata["user-key"] != "user-value" {
		t.Fatalf("caller metadata missing: %#v", request.Metadata)
	}
	if request.Metadata[MetaStreamState] != "" || request.Metadata[MetaCancel] != "" {
		t.Fatalf("caller injected transport control metadata: %#v", request.Metadata)
	}
}

func TestHttpClientRejectsExchangeInputSchemaMetadataDriftBeforeDispatch(t *testing.T) {
	fields := []arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}
	expectedMetadata := arrow.NewMetadata([]string{"contract"}, []string{"expected"})
	driftedMetadata := arrow.NewMetadata([]string{"contract"}, []string{"drifted"})
	expected := arrow.NewSchema(fields, &expectedMetadata)
	drifted := arrow.NewSchema(fields, &driftedMetadata)
	if !expected.Equal(drifted) || expected.Metadata().Equal(drifted.Metadata()) {
		t.Fatal("test requires Arrow Schema.Equal to ignore only schema-level metadata")
	}
	initBatch := emptyBatch(expected)
	initBody, err := encodeClientBatch(initBatch, map[string]string{
		MetaStreamState: "cursor-one",
		MetaCallState:   "call-one",
	}, 1<<20)
	initBatch.Release()
	if err != nil {
		t.Fatal(err)
	}
	var exchanges atomic.Int64
	server := newResponseBudgetTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", arrowContentType)
		if strings.HasSuffix(r.URL.Path, "/init") {
			_, _ = w.Write(initBody)
			return
		}
		exchanges.Add(1)
	}))
	defer server.Close()
	client, err := NewHttpClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	params := emptyBatch(arrow.NewSchema(nil, nil))
	defer params.Release()
	session, err := client.OpenExchange(context.Background(), "echo", params, ClientStreamSchema{Input: expected, Output: expected})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	input := emptyBatch(drifted)
	defer input.Release()
	if _, err := session.Exchange(context.Background(), input); err == nil || !strings.Contains(err.Error(), "input schema mismatch") {
		t.Fatalf("metadata-drift exchange error = %v, want schema mismatch", err)
	}
	if got := exchanges.Load(); got != 0 {
		t.Fatalf("metadata-drift input dispatched %d HTTP requests, want 0", got)
	}
}

func TestHttpClientRejectsResponseSchemaMetadataDrift(t *testing.T) {
	fields := []arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}
	expectedMetadata := arrow.NewMetadata([]string{"contract"}, []string{"expected"})
	driftedMetadata := arrow.NewMetadata([]string{"contract"}, []string{"drifted"})
	expected := arrow.NewSchema(fields, &expectedMetadata)
	drifted := arrow.NewSchema(fields, &driftedMetadata)
	builder := array.NewInt32Builder(memory.DefaultAllocator)
	builder.Append(7)
	values := builder.NewInt32Array()
	builder.Release()
	batch := array.NewRecordBatch(drifted, []arrow.Array{values}, 1)
	values.Release()
	body, err := encodeClientBatch(batch, nil, 1<<20)
	batch.Release()
	if err != nil {
		t.Fatal(err)
	}
	server := newResponseBudgetTestServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", arrowContentType)
		_, _ = w.Write(body)
	}))
	defer server.Close()
	client, err := NewHttpClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	params := emptyBatch(arrow.NewSchema(nil, nil))
	defer params.Release()
	if _, err := client.CallUnary(context.Background(), "method", params, expected); err == nil || !strings.Contains(err.Error(), "response schema mismatch") {
		t.Fatalf("metadata-drift response error = %v, want schema mismatch", err)
	}
}

func TestHttpClientRejectsUnknownResponseEncoding(t *testing.T) {
	server := newResponseBudgetTestServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(contentEncodingHeader, "br")
		_, _ = w.Write([]byte("encoded"))
	}))
	defer server.Close()
	client, err := NewHttpClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	params := emptyBatch(arrow.NewSchema(nil, nil))
	defer params.Release()
	_, err = client.CallUnary(context.Background(), "method", params, nil)
	if err == nil || !strings.Contains(err.Error(), "unsupported HTTP response Content-Encoding") {
		t.Fatalf("error = %v, want unsupported encoding", err)
	}
}

func TestHttpClientCapsIPCSerializationBeforeDispatch(t *testing.T) {
	var requests atomic.Int64
	server := newResponseBudgetTestServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	defer server.Close()
	client, err := NewHttpClient(server.URL, WithClientRequestLimit(64))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	params := emptyBatch(arrow.NewSchema(nil, nil))
	defer params.Release()
	_, err = client.CallUnary(context.Background(), "method", params, nil)
	if err == nil || !strings.Contains(err.Error(), "request IPC exceeds client limit") {
		t.Fatalf("error = %v, want serialization cap", err)
	}
	if requests.Load() != 0 {
		t.Fatalf("server received %d requests after local serialization rejection", requests.Load())
	}
}

func TestHttpClientCapsKnownLengthResponseAndRecovers(t *testing.T) {
	valid := validEmptyClientResponse(t)
	server := newResponseBudgetTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "oversized") {
			body := bytes.Repeat([]byte("x"), 70_000)
			w.Header().Set("Content-Length", "70000")
			_, _ = w.Write(body)
			return
		}
		w.Header().Set("Content-Type", arrowContentType)
		_, _ = w.Write(valid)
	}))
	defer server.Close()
	client, err := NewHttpClient(server.URL, WithClientResponseLimits(64<<10, 128<<10))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	assertClientResponseCapAndRecovery(t, client, "oversized", "encoded HTTP response exceeds client limit")
}

func TestHttpClientCapsChunkedResponseAndRecovers(t *testing.T) {
	valid := validEmptyClientResponse(t)
	server := newResponseBudgetTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "oversized") {
			flusher, ok := w.(http.Flusher)
			if !ok {
				t.Error("test server response writer cannot flush")
				return
			}
			for range 8 {
				_, _ = w.Write(bytes.Repeat([]byte("x"), 16<<10))
				flusher.Flush()
			}
			return
		}
		w.Header().Set("Content-Type", arrowContentType)
		_, _ = w.Write(valid)
	}))
	defer server.Close()
	client, err := NewHttpClient(server.URL, WithClientResponseLimits(64<<10, 128<<10))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	assertClientResponseCapAndRecovery(t, client, "oversized", "encoded HTTP response exceeds client limit")
}

func TestHttpClientCapsDecodedResponseAndRecovers(t *testing.T) {
	valid := validEmptyClientResponse(t)
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	if _, err := zw.Write(bytes.Repeat([]byte("expanded"), 10_000)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	server := newResponseBudgetTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "oversized") {
			w.Header().Set(contentEncodingHeader, "gzip")
			_, _ = w.Write(compressed.Bytes())
			return
		}
		w.Header().Set("Content-Type", arrowContentType)
		_, _ = w.Write(valid)
	}))
	defer server.Close()
	client, err := NewHttpClient(server.URL, WithClientResponseLimits(128<<10, 64<<10))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	assertClientResponseCapAndRecovery(t, client, "oversized", "decode HTTP response")
}

func TestHttpClientAcceptedLimitAndLocalCeilingsAreOrderIndependent(t *testing.T) {
	const accepted = int64(1 << 30)
	for _, tc := range []struct {
		name    string
		options []HttpClientOption
	}{
		{
			name: "accepted then transport limits",
			options: []HttpClientOption{
				WithClientAcceptedMaxResponseBytes(accepted),
				WithClientResponseLimits(512<<20, 768<<20),
			},
		},
		{
			name: "transport limits then accepted",
			options: []HttpClientOption{
				WithClientResponseLimits(512<<20, 768<<20),
				WithClientAcceptedMaxResponseBytes(accepted),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := NewHttpClient("http://example.test", tc.options...)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			if client.acceptedMaxResponse != 512<<20 || client.maxEncoded != 512<<20 || client.maxDecoded != 768<<20 {
				t.Fatalf("accepted=%d encoded=%d decoded=%d, want 512MiB/512MiB/768MiB",
					client.acceptedMaxResponse, client.maxEncoded, client.maxDecoded)
			}
		})
	}
	if _, err := NewHttpClient("http://example.test", WithClientResponseLimits(1024, 1024)); err == nil {
		t.Fatal("response limits below the wire minimum produced an untruthful accepted advertisement")
	}
}

func TestHttpClientAcceptedOneGiBRaisesDefaultLocalCeilings(t *testing.T) {
	const accepted = int64(1 << 30)
	var advertised string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		advertised = r.Header.Get(acceptMaxResponseBytesHeader)
		w.Header().Set(acceptMaxResponseBytesSupportHeader, "true")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := NewHttpClient(server.URL, WithClientAcceptedMaxResponseBytes(accepted))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if client.acceptedMaxResponse != accepted || client.maxEncoded != accepted || client.maxDecoded != accepted {
		t.Fatalf("accepted=%d encoded=%d decoded=%d, want all %d",
			client.acceptedMaxResponse, client.maxEncoded, client.maxDecoded, accepted)
	}
	if _, err := client.DiscoverCapabilities(context.Background()); err != nil {
		t.Fatal(err)
	}
	if advertised != "1073741824" {
		t.Fatalf("advertised accepted response limit = %q, want 1073741824", advertised)
	}
}

func TestHttpClientRequiresAndCachesResponseBudgetDiscovery(t *testing.T) {
	var options atomic.Int64
	var posts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(acceptMaxResponseBytesSupportHeader, "true")
		if r.Method == http.MethodOptions {
			options.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		posts.Add(1)
		w.Header().Set("Content-Type", arrowContentType)
		_, _ = w.Write(validEmptyClientResponse(t))
	}))
	defer server.Close()
	client, err := NewHttpClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	params := emptyBatch(arrow.NewSchema(nil, nil))
	defer params.Release()
	for range 2 {
		result, err := client.CallUnary(context.Background(), "method", params, nil)
		if err != nil {
			t.Fatal(err)
		}
		result.Release()
	}
	if got := options.Load(); got != 1 {
		t.Fatalf("OPTIONS requests = %d, want one cached discovery", got)
	}
	if got := posts.Load(); got != 2 {
		t.Fatalf("POST requests = %d, want two RPC dispatches", got)
	}
}

func TestHttpClientRejectsUnsupportedResponseBudgetBeforeDispatch(t *testing.T) {
	var posts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		posts.Add(1)
	}))
	defer server.Close()
	client, err := NewHttpClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	params := emptyBatch(arrow.NewSchema(nil, nil))
	defer params.Release()
	if _, err := client.CallUnary(context.Background(), "method", params, nil); err == nil ||
		!strings.Contains(err.Error(), acceptMaxResponseBytesSupportHeader) {
		t.Fatalf("error = %v, want mandatory discovery failure", err)
	}
	if got := posts.Load(); got != 0 {
		t.Fatalf("server received %d POST requests after failed discovery", got)
	}
}

func TestHttpClientEnforcesAdvertisedAcceptedLimitBeforeAllocation(t *testing.T) {
	server := newResponseBudgetTestServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "70000")
		_, _ = w.Write(bytes.Repeat([]byte("x"), 70_000))
	}))
	defer server.Close()
	client, err := NewHttpClient(server.URL,
		WithClientResponseLimits(128<<10, 128<<10),
		WithClientAcceptedMaxResponseBytes(64<<10))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	params := emptyBatch(arrow.NewSchema(nil, nil))
	defer params.Release()
	if _, err := client.CallUnary(context.Background(), "method", params, nil); err == nil ||
		!strings.Contains(err.Error(), "65536") {
		t.Fatalf("error = %v, want accepted response cap", err)
	}
}

func TestHttpClientRequiresSupportOnEveryResponseAndPreservesAdvertisedLimit(t *testing.T) {
	for _, omitRPCSupport := range []bool{false, true} {
		t.Run(fmt.Sprintf("omit_support_%t", omitRPCSupport), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodOptions {
					w.Header().Set(acceptMaxResponseBytesSupportHeader, "true")
					w.Header().Set(maxResponseBytesHeader, "65536")
					w.WriteHeader(http.StatusNoContent)
					return
				}
				if !omitRPCSupport {
					w.Header().Set(acceptMaxResponseBytesSupportHeader, "true")
				}
				w.Header().Set("Content-Length", "70000")
				_, _ = w.Write(bytes.Repeat([]byte("x"), 70_000))
			}))
			defer server.Close()
			client, err := NewHttpClient(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			params := emptyBatch(arrow.NewSchema(nil, nil))
			defer params.Release()
			_, err = client.CallUnary(context.Background(), "method", params, nil)
			if err == nil {
				t.Fatal("oversized or unsupported response was accepted")
			}
			if omitRPCSupport && !strings.Contains(err.Error(), "every RPC response") {
				t.Fatalf("error = %v, want per-response support failure", err)
			}
			if !omitRPCSupport && !strings.Contains(err.Error(), "65536") {
				t.Fatalf("error = %v, want advertised response cap", err)
			}
		})
	}
}

func TestHttpClientStreamCloseIsLocalAndCancelIsExplicit(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	initBatch := emptyBatch(schema)
	initBody, err := encodeClientBatch(initBatch, map[string]string{
		MetaStreamState: "cursor-one",
		MetaCallState:   "call-one",
	}, 1<<20)
	initBatch.Release()
	if err != nil {
		t.Fatal(err)
	}
	var cancelBody bytes.Buffer
	cancelWriter := ipc.NewWriter(&cancelBody, ipc.WithSchema(schema))
	if err := cancelWriter.Close(); err != nil {
		t.Fatal(err)
	}
	var initRequests atomic.Int64
	var cancelRequests atomic.Int64
	server := newResponseBudgetTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", arrowContentType)
		if strings.HasSuffix(r.URL.Path, "/init") {
			initRequests.Add(1)
			_, _ = w.Write(initBody)
			return
		}
		reader, readErr := ipc.NewReader(r.Body)
		if readErr != nil {
			t.Errorf("read cancellation request: %v", readErr)
			return
		}
		defer reader.Release()
		if !reader.Next() {
			t.Errorf("read cancellation batch: %v", reader.Err())
			return
		}
		metadata := recordMetadata(reader.RecordBatch())
		if metadata[MetaCancel] != "1" {
			t.Errorf("cancel metadata = %#v", metadata)
		}
		cancelRequests.Add(1)
		_, _ = w.Write(cancelBody.Bytes())
	}))
	defer server.Close()
	client, err := NewHttpClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	params := emptyBatch(arrow.NewSchema(nil, nil))
	defer params.Release()

	local, err := client.OpenExchange(context.Background(), "echo", params, ClientStreamSchema{Input: schema, Output: schema})
	if err != nil {
		t.Fatal(err)
	}
	local.Close()
	local.Close()
	if got := cancelRequests.Load(); got != 0 {
		t.Fatalf("local Close sent %d requests, want 0", got)
	}

	remote, err := client.OpenExchange(context.Background(), "echo", params, ClientStreamSchema{Input: schema, Output: schema})
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()
	if err := remote.Cancel(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := remote.Cancel(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := initRequests.Load(); got != 2 {
		t.Fatalf("init requests = %d, want 2", got)
	}
	if got := cancelRequests.Load(); got != 1 {
		t.Fatalf("cancel requests = %d, want 1", got)
	}
}

func validEmptyClientResponse(t *testing.T) []byte {
	t.Helper()
	batch := emptyBatch(arrow.NewSchema(nil, nil))
	defer batch.Release()
	body, err := encodeClientBatch(batch, nil, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func assertClientResponseCapAndRecovery(t *testing.T, client *HttpClient, method, wantError string) {
	t.Helper()
	params := emptyBatch(arrow.NewSchema(nil, nil))
	defer params.Release()
	if _, err := client.CallUnary(context.Background(), method, params, nil); err == nil || !strings.Contains(err.Error(), wantError) {
		t.Fatalf("oversized response error = %v, want %q", err, wantError)
	}
	result, err := client.CallUnary(context.Background(), "valid", params, arrow.NewSchema(nil, nil))
	if err != nil {
		t.Fatalf("same-client recovery failed: %v", err)
	}
	result.Release()
}
