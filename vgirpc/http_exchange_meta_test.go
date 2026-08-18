// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Regression tests for the HTTP exchange turn:
//
//  1. The continuation token must ride the data batch even when the emit
//     carried per-emit metadata (vgi.cache.*, vgi_batch_index, …) or produced
//     zero rows — previously the flush wrote EITHER the emit metadata OR the
//     token, so a metadata-carrying multi-chunk exchange lost its token after
//     the first turn ("returned end-of-stream mid-exchange"). Mirrors
//     Python's merge_data_metadata, which merges onto the data batch
//     regardless of row count.
//  2. The request batch's custom metadata must surface as
//     CallContext.InputMetadata, matching the pipe transports
//     (conditional-revalidation validators like if_none_match ride there).

package vgirpc

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type metaTokenParams struct {
	Factor float64 `vgirpc:"factor"`
}

// metaTokenExchangeState replies to every input with a ZERO-row data batch
// carrying per-emit metadata — the shape of a conditional-revalidation
// "not modified" reply — and echoes the request's if_none_match validator so
// the test can prove InputMetadata reached the handler.
type metaTokenExchangeState struct{}

func (s *metaTokenExchangeState) Exchange(_ context.Context, _ arrow.RecordBatch, out *OutputCollector, callCtx *CallContext) error {
	meta := map[string]string{"vgi.cache.status": "not_modified"}
	if v, ok := callCtx.InputMetadata.GetValue("if_none_match"); ok {
		meta["echo_if_none_match"] = v
	}
	b := array.NewFloat64Builder(memory.NewGoAllocator())
	defer b.Release()
	arr := b.NewArray()
	defer arr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Float64}}, nil)
	batch := array.NewRecordBatch(schema, []arrow.Array{arr}, 0)
	// EmitWithMetadata takes ownership of batch (and may release it immediately
	// while re-wrapping it onto the registered output schema).
	return out.EmitWithMetadata(batch, meta)
}

func TestHTTPExchangeKeepsTokenWithEmitMetadata(t *testing.T) {
	RegisterStateType(&metaTokenExchangeState{})

	valueSchema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Float64}}, nil)

	srv := NewServer()
	Exchange(srv, "meta_token", valueSchema, valueSchema,
		func(_ context.Context, _ *CallContext, _ metaTokenParams) (*StreamResult, error) {
			return &StreamResult{
				OutputSchema: valueSchema,
				InputSchema:  valueSchema,
				State:        &metaTokenExchangeState{},
			}, nil
		})

	h := NewHttpServer(srv)
	h.InitPages()
	ts := httptest.NewServer(h)
	defer ts.Close()

	mem := memory.NewGoAllocator()

	// --- /init: obtain the continuation token ---
	pb := array.NewFloat64Builder(mem)
	pb.Append(2.0)
	paramsArr := pb.NewArray()
	pb.Release()
	paramsSchema := arrow.NewSchema([]arrow.Field{{Name: "factor", Type: arrow.PrimitiveTypes.Float64}}, nil)
	paramsBatch := array.NewRecordBatch(paramsSchema, []arrow.Array{paramsArr}, 1)

	var initBody bytes.Buffer
	if err := WriteRequest(&initBody, "meta_token", paramsBatch, ""); err != nil {
		t.Fatal(err)
	}
	paramsBatch.Release()
	paramsArr.Release()

	initResp, err := http.Post(ts.URL+"/meta_token/init", "application/vnd.apache.arrow.stream", &initBody)
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
	// A conformant client echoes BOTH tokens on every continuation: the
	// cursor it was just handed and the call token /init minted once. See
	// docs/WIRE_PROTOCOL.md in the reference repo.
	token, callToken := FindStreamTokens(initBytes)
	if token == nil {
		t.Fatal("/init response carried no state token")
	}
	if callToken == nil {
		t.Fatal("/init response carried no call token")
	}

	// --- /exchange: input batch whose custom metadata carries the token and
	// a conditional-request validator ---
	vb := array.NewFloat64Builder(mem)
	vb.Append(21.0)
	valueArr := vb.NewArray()
	vb.Release()
	inputBatch := array.NewRecordBatch(valueSchema, []arrow.Array{valueArr}, 1)
	reqMeta := arrow.NewMetadata(
		[]string{MetaStreamState, MetaCallState, "if_none_match"},
		[]string{string(token), string(callToken), "etag-123"})
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

	exResp, err := http.Post(ts.URL+"/meta_token/exchange", "application/vnd.apache.arrow.stream", &exBody)
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

	// The single data batch must carry the per-emit metadata AND the token.
	reader, err := ipc.NewReader(bytes.NewReader(exBytes))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	var dataMeta *arrow.Metadata
	var dataRows int64 = -1
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
		m := meta
		dataMeta = &m
		dataRows = rb.NumRows()
	}
	if dataMeta == nil {
		t.Fatal("/exchange response had no metadata-carrying data batch")
	}
	if dataRows != 0 {
		t.Fatalf("expected the 0-row data batch, got %d rows", dataRows)
	}
	if v, ok := dataMeta.GetValue("vgi.cache.status"); !ok || v != "not_modified" {
		t.Fatalf("per-emit metadata lost: vgi.cache.status=%q (present=%v)", v, ok)
	}
	if v, ok := dataMeta.GetValue("echo_if_none_match"); !ok || v != "etag-123" {
		t.Fatalf("CallContext.InputMetadata missing on HTTP exchange: echo_if_none_match=%q (present=%v)", v, ok)
	}
	if _, ok := dataMeta.GetValue(MetaStreamState); !ok {
		t.Fatal("continuation token lost when the data batch carries emit metadata")
	}
}
