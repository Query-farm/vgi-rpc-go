// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Regression test for the HTTP exchange turn's input metadata.
//
// An exchange continuation POST carries the framework's own transport keys in
// the request batch's custom metadata — the sealed stream-state cursor, the
// call-state token, and the cancel flag. Those are transport plumbing: the pipe
// transports keep that state in the CONNECTION and never put it on a batch, so
// server_stream.go's InputMetadata only ever holds application metadata. Over
// HTTP the server previously handed the request metadata to the handler
// verbatim, so identical worker code saw clean user metadata over subprocess
// and framework internals over HTTP — and application code was handed a
// credential-shaped value (the AEAD-sealed cursor) it may log or persist.
//
// User metadata must still arrive: the conditional-revalidation validators
// (if_none_match / if_modified_since) ride exactly here.
//
// Mirrors the producer-side guarantee in
// http_producer_continuation_meta_test.go.

package vgirpc

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type frameworkMetaParams struct {
	Factor float64 `vgirpc:"factor"`
}

// frameworkMetaExchangeState reports, via its emit metadata, both the user key
// it saw and any framework transport key that leaked into InputMetadata.
type frameworkMetaExchangeState struct{}

func (s *frameworkMetaExchangeState) Exchange(_ context.Context, _ arrow.RecordBatch, out *OutputCollector, callCtx *CallContext) error {
	meta := map[string]string{}
	if v, ok := callCtx.InputMetadata.GetValue("if_none_match"); ok {
		meta["echo_if_none_match"] = v
	}
	var leaked []string
	for _, k := range []string{MetaStreamState, MetaCallState, MetaCancel} {
		if _, ok := callCtx.InputMetadata.GetValue(k); ok {
			leaked = append(leaked, k)
		}
	}
	meta["leaked_framework_keys"] = strings.Join(leaked, ";")

	b := array.NewFloat64Builder(memory.NewGoAllocator())
	defer b.Release()
	arr := b.NewArray()
	defer arr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Float64}}, nil)
	batch := array.NewRecordBatch(schema, []arrow.Array{arr}, 0)
	// EmitWithMetadata takes ownership of batch.
	return out.EmitWithMetadata(batch, meta)
}

// TestHTTPExchangeStripsFrameworkInputMetadata proves an exchange handler sees
// the request's USER metadata and none of the framework's transport keys.
func TestHTTPExchangeStripsFrameworkInputMetadata(t *testing.T) {
	RegisterStateType(&frameworkMetaExchangeState{})

	valueSchema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Float64}}, nil)

	srv := NewServer()
	Exchange(srv, "framework_meta", valueSchema, valueSchema,
		func(_ context.Context, _ *CallContext, _ frameworkMetaParams) (*StreamResult, error) {
			return &StreamResult{
				OutputSchema: valueSchema,
				InputSchema:  valueSchema,
				State:        &frameworkMetaExchangeState{},
			}, nil
		})

	h := NewHttpServer(srv)
	h.InitPages()
	ts := httptest.NewServer(h)
	defer ts.Close()

	mem := memory.NewGoAllocator()

	// --- /init: obtain the continuation tokens ---
	pb := array.NewFloat64Builder(mem)
	pb.Append(2.0)
	paramsArr := pb.NewArray()
	pb.Release()
	paramsSchema := arrow.NewSchema([]arrow.Field{{Name: "factor", Type: arrow.PrimitiveTypes.Float64}}, nil)
	paramsBatch := array.NewRecordBatch(paramsSchema, []arrow.Array{paramsArr}, 1)

	var initBody bytes.Buffer
	if err := WriteRequest(&initBody, "framework_meta", paramsBatch, ""); err != nil {
		t.Fatal(err)
	}
	paramsBatch.Release()
	paramsArr.Release()

	initResp, err := http.Post(ts.URL+"/framework_meta/init", "application/vnd.apache.arrow.stream", &initBody)
	if err != nil {
		t.Fatal(err)
	}
	initBytes, err := io.ReadAll(initResp.Body)
	_ = initResp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if initResp.StatusCode != http.StatusOK {
		t.Fatalf("/init: expected 200, got %d: %s", initResp.StatusCode, initBytes)
	}
	token, callToken := FindStreamTokens(initBytes)
	if token == nil || callToken == nil {
		t.Fatal("/init response missing stream tokens")
	}

	// --- /exchange: a conformant client echoes BOTH framework tokens
	// alongside its own application metadata. ---
	vb := array.NewFloat64Builder(mem)
	vb.Append(21.0)
	valueArr := vb.NewArray()
	vb.Release()
	inputBatch := array.NewRecordBatch(valueSchema, []arrow.Array{valueArr}, 1)
	reqMeta := arrow.NewMetadata(
		[]string{MetaStreamState, MetaCallState, "if_none_match"},
		[]string{string(token), string(callToken), "etag-abc"})
	inputWithMeta := array.NewRecordBatchWithMetadata(
		valueSchema, inputBatch.Columns(), inputBatch.NumRows(), reqMeta)

	var exBody bytes.Buffer
	writer := ipc.NewWriter(&exBody, ipc.WithSchema(valueSchema))
	if err := writer.Write(inputWithMeta); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	inputWithMeta.Release()
	inputBatch.Release()
	valueArr.Release()

	exResp, err := http.Post(ts.URL+"/framework_meta/exchange", "application/vnd.apache.arrow.stream", &exBody)
	if err != nil {
		t.Fatal(err)
	}
	exBytes, err := io.ReadAll(exResp.Body)
	_ = exResp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if exResp.StatusCode != http.StatusOK {
		t.Fatalf("/exchange: expected 200, got %d: %s", exResp.StatusCode, exBytes)
	}

	reader, err := ipc.NewReader(bytes.NewReader(exBytes))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	var dataMeta *arrow.Metadata
	for reader.Next() {
		rb := reader.RecordBatch()
		bwm, ok := rb.(arrow.RecordBatchWithMetadata)
		if !ok {
			continue
		}
		meta := bwm.Metadata()
		if _, isLog := meta.GetValue(MetaLogLevel); isLog {
			continue
		}
		if _, isReport := meta.GetValue("leaked_framework_keys"); !isReport {
			continue
		}
		m := meta
		dataMeta = &m
	}
	if dataMeta == nil {
		t.Fatal("/exchange response had no handler report batch")
	}

	// The user key must survive the strip — conditional revalidation rides here.
	if v, ok := dataMeta.GetValue("echo_if_none_match"); !ok || v != "etag-abc" {
		t.Fatalf("exchange handler lost the request's USER metadata: echo_if_none_match=%q present=%v", v, ok)
	}
	// ...and no framework transport key may reach the handler.
	if v, _ := dataMeta.GetValue("leaked_framework_keys"); v != "" {
		t.Fatalf("framework transport key(s) leaked into exchange InputMetadata: %s "+
			"(the pipe transports keep this state in the connection, never on a batch; "+
			"the stream-state value is a sealed cursor token)", v)
	}
}
