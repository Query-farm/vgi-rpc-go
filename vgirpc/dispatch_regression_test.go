// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type regressionParams struct {
	Value int64 `vgirpc:"value"`
}

type regressionResult struct {
	Value int64 `vgirpc:"value"`
}

var regressionSchema = arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)

func regressionBatch(t *testing.T, value int64) arrow.RecordBatch {
	t.Helper()
	b := array.NewInt64Builder(memory.NewGoAllocator())
	b.Append(value)
	col := b.NewArray()
	b.Release()
	rec := array.NewRecordBatch(regressionSchema, []arrow.Array{col}, 1)
	col.Release()
	return rec
}

func regressionIPC(t *testing.T, batch arrow.RecordBatch, meta arrow.Metadata) []byte {
	t.Helper()
	toWrite := batch
	if meta.Len() > 0 {
		wrapped := array.NewRecordBatchWithMetadata(batch.Schema(), batch.Columns(), batch.NumRows(), meta)
		defer wrapped.Release()
		toWrite = wrapped
	}
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(batch.Schema()))
	if err := w.Write(toWrite); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func regressionRequest(t *testing.T, method string, batch arrow.RecordBatch) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := WriteRequest(&buf, method, batch, ""); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func requireRuntimeError(t *testing.T, body []byte, contains string) {
	t.Helper()
	r, err := ipc.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("open error response: %v", err)
	}
	defer r.Release()
	for r.Next() {
		rb, ok := r.RecordBatch().(arrow.RecordBatchWithMetadata)
		if !ok {
			continue
		}
		md := rb.Metadata()
		level, _ := md.GetValue(MetaLogLevel)
		if level != string(LogException) {
			continue
		}
		extra, _ := md.GetValue(MetaLogExtra)
		var decoded errorExtra
		if err := json.Unmarshal([]byte(extra), &decoded); err != nil {
			t.Fatalf("decode error metadata: %v", err)
		}
		if decoded.ExceptionType != "RuntimeError" {
			t.Fatalf("expected RuntimeError, got %q", decoded.ExceptionType)
		}
		if !strings.Contains(decoded.ExceptionMessage, contains) {
			t.Fatalf("expected error containing %q, got %q", contains, decoded.ExceptionMessage)
		}
		return
	}
	if err := r.Err(); err != nil {
		t.Fatalf("read error response: %v", err)
	}
	t.Fatal("response contained no EXCEPTION batch")
}

type regressionHook struct {
	mu      sync.Mutex
	ends    int
	lastErr error
}

func (h *regressionHook) OnDispatchStart(ctx context.Context, _ DispatchInfo) (context.Context, HookToken) {
	return ctx, nil
}

func (h *regressionHook) OnDispatchEnd(_ context.Context, _ HookToken, _ DispatchInfo, _ *CallStatistics, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ends++
	h.lastErr = err
}

func (h *regressionHook) requireRuntimeEnd(t *testing.T) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ends != 1 {
		t.Fatalf("expected one dispatch-end call, got %d", h.ends)
	}
	if h.lastErr == nil || !strings.Contains(h.lastErr.Error(), "RuntimeError") {
		t.Fatalf("expected RuntimeError at dispatch end, got %v", h.lastErr)
	}
}

func TestRawUnaryPanicBecomesRuntimeErrorAndEndsHook(t *testing.T) {
	s := NewServer()
	hook := &regressionHook{}
	s.SetDispatchHook(hook)
	Unary(s, "panic_unary", func(context.Context, *CallContext, regressionParams) (regressionResult, error) {
		panic("unary boom")
	})

	params := regressionBatch(t, 1)
	defer params.Release()
	var response bytes.Buffer
	if err := s.serveOne(context.Background(), bytes.NewReader(regressionRequest(t, "panic_unary", params)), &response, &shmConnState{}); err != nil {
		t.Fatal(err)
	}
	requireRuntimeError(t, response.Bytes(), "unary boom")
	hook.requireRuntimeEnd(t)

	// The same worker remains reusable after the panic.
	Unary(s, "healthy_unary", func(_ context.Context, _ *CallContext, p regressionParams) (regressionResult, error) {
		return regressionResult(p), nil
	})
	secondHook := &regressionHook{}
	s.SetDispatchHook(secondHook)
	response.Reset()
	if err := s.serveOne(context.Background(), bytes.NewReader(regressionRequest(t, "healthy_unary", params)), &response, &shmConnState{}); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := ReadUnaryResult(response.Bytes()); !ok {
		t.Fatal("worker did not serve a valid unary response after handler panic")
	}
}

func TestRawStreamInitPanicBecomesRuntimeErrorAndEndsHook(t *testing.T) {
	s := NewServer()
	hook := &regressionHook{}
	s.SetDispatchHook(hook)
	Producer(s, "panic_stream", regressionSchema,
		func(context.Context, *CallContext, regressionParams) (*StreamResult, error) {
			panic("stream init boom")
		})

	params := regressionBatch(t, 1)
	defer params.Release()
	input := append([]byte(nil), regressionRequest(t, "panic_stream", params)...)
	// Init errors drain the following client-input IPC stream.
	empty := array.NewRecordBatch(arrow.NewSchema(nil, nil), nil, 0)
	input = append(input, regressionIPC(t, empty, arrow.Metadata{})...)
	empty.Release()

	var response bytes.Buffer
	if err := s.serveOne(context.Background(), bytes.NewReader(input), &response, &shmConnState{}); err != nil {
		t.Fatal(err)
	}
	requireRuntimeError(t, response.Bytes(), "stream init boom")
	hook.requireRuntimeEnd(t)
}

func TestHTTPStreamInitPanicBecomesRuntimeErrorAndEndsHook(t *testing.T) {
	s := NewServer()
	hook := &regressionHook{}
	s.SetDispatchHook(hook)
	Producer(s, "panic_http_stream", regressionSchema,
		func(context.Context, *CallContext, regressionParams) (*StreamResult, error) {
			panic("http stream init boom")
		})
	h := NewHttpServer(s)
	h.InitPages()

	params := regressionBatch(t, 1)
	defer params.Release()
	req := httptest.NewRequest(http.MethodPost, "/panic_http_stream/init", bytes.NewReader(regressionRequest(t, "panic_http_stream", params)))
	req.Header.Set("Content-Type", arrowContentType)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Header().Get(rpcErrorHeader) != "true" {
		t.Fatalf("expected %s=true, got headers %v", rpcErrorHeader, w.Header())
	}
	requireRuntimeError(t, w.Body.Bytes(), "http stream init boom")
	hook.requireRuntimeEnd(t)
}

type finishProducerState struct{}

func (*finishProducerState) Produce(_ context.Context, out *OutputCollector, _ *CallContext) error {
	return out.Finish()
}

func TestHTTPStreamInitResolvesExternalParams(t *testing.T) {
	RegisterStateType(&finishProducerState{})
	params := regressionBatch(t, 37)
	externalBody := regressionIPC(t, params, arrow.Metadata{})
	params.Release()
	fetch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(externalBody)
	}))
	defer fetch.Close()

	var got int64
	s := NewServer()
	s.SetExternalLocation(&ExternalLocationConfig{URLValidator: nil, HTTPClient: fetch.Client()})
	Producer(s, "external_init", regressionSchema,
		func(_ context.Context, _ *CallContext, p regressionParams) (*StreamResult, error) {
			got = p.Value
			return &StreamResult{OutputSchema: regressionSchema, State: &finishProducerState{}}, nil
		})
	h := NewHttpServer(s)
	h.InitPages()

	pointer, locationMeta := MakeExternalLocationBatch(regressionSchema, fetch.URL)
	defer pointer.Release()
	meta := arrow.NewMetadata(
		[]string{MetaMethod, MetaRequestVersion, MetaLocation},
		[]string{"external_init", ProtocolVersion, locationMeta.Values()[0]},
	)
	req := httptest.NewRequest(http.MethodPost, "/external_init/init", bytes.NewReader(regressionIPC(t, pointer, meta)))
	req.Header.Set("Content-Type", arrowContentType)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if got != 37 {
		t.Fatalf("stream init received pointer rows instead of fetched params: got %d", got)
	}
}

type echoExternalExchangeState struct{}

func (*echoExternalExchangeState) Exchange(_ context.Context, input arrow.RecordBatch, out *OutputCollector, _ *CallContext) error {
	value := input.Column(0).(*array.Int64).Value(0)
	return out.EmitMap(map[string][]interface{}{"value": {value}})
}

func TestHTTPStreamExchangeResolvesExternalInput(t *testing.T) {
	RegisterStateType(&echoExternalExchangeState{})
	s := NewServer()
	Exchange(s, "external_exchange", regressionSchema, regressionSchema,
		func(context.Context, *CallContext, regressionParams) (*StreamResult, error) {
			return &StreamResult{OutputSchema: regressionSchema, InputSchema: regressionSchema, State: &echoExternalExchangeState{}}, nil
		})
	h := NewHttpServer(s)
	h.InitPages()

	params := regressionBatch(t, 1)
	initReq := httptest.NewRequest(http.MethodPost, "/external_exchange/init", bytes.NewReader(regressionRequest(t, "external_exchange", params)))
	params.Release()
	initReq.Header.Set("Content-Type", arrowContentType)
	initW := httptest.NewRecorder()
	h.ServeHTTP(initW, initReq)
	token, callToken := FindStreamTokens(initW.Body.Bytes())
	if token == nil || callToken == nil {
		t.Fatalf("init response missing stream tokens")
	}

	input := regressionBatch(t, 42)
	externalBody := regressionIPC(t, input, arrow.Metadata{})
	input.Release()
	fetch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(externalBody)
	}))
	defer fetch.Close()
	s.SetExternalLocation(&ExternalLocationConfig{URLValidator: nil, HTTPClient: fetch.Client()})

	pointer, _ := MakeExternalLocationBatch(regressionSchema, fetch.URL)
	defer pointer.Release()
	meta := arrow.NewMetadata(
		[]string{MetaStreamState, MetaCallState, MetaLocation},
		[]string{string(token), string(callToken), fetch.URL},
	)
	exReq := httptest.NewRequest(http.MethodPost, "/external_exchange/exchange", bytes.NewReader(regressionIPC(t, pointer, meta)))
	exReq.Header.Set("Content-Type", arrowContentType)
	exW := httptest.NewRecorder()
	h.ServeHTTP(exW, exReq)
	if exW.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", exW.Code, exW.Body.String())
	}
	r, err := ipc.NewReader(bytes.NewReader(exW.Body.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	if !r.Next() {
		t.Fatalf("exchange response had no data batch: %v", r.Err())
	}
	if got := r.RecordBatch().Column(0).(*array.Int64).Value(0); got != 42 {
		t.Fatalf("exchange received pointer rows instead of fetched input: got %d", got)
	}
}

type countingExchangeState struct{ calls int }

func (s *countingExchangeState) Exchange(context.Context, arrow.RecordBatch, *OutputCollector, *CallContext) error {
	s.calls++
	return nil
}

func TestRawExchangeExternalResolveFailureStopsBeforeDispatch(t *testing.T) {
	state := &countingExchangeState{}
	s := NewServer()
	s.SetExternalLocation(&ExternalLocationConfig{
		URLValidator: func(string) error { return fmt.Errorf("blocked URL") },
	})
	Exchange(s, "external_failure", regressionSchema, regressionSchema,
		func(context.Context, *CallContext, regressionParams) (*StreamResult, error) {
			return &StreamResult{OutputSchema: regressionSchema, InputSchema: regressionSchema, State: state}, nil
		})

	params := regressionBatch(t, 1)
	input := append([]byte(nil), regressionRequest(t, "external_failure", params)...)
	params.Release()
	pointer, _ := MakeExternalLocationBatch(regressionSchema, "http://blocked.invalid/data")
	meta := arrow.NewMetadata([]string{MetaLocation}, []string{"http://blocked.invalid/data"})
	input = append(input, regressionIPC(t, pointer, meta)...)
	pointer.Release()

	var response bytes.Buffer
	if err := s.serveOne(context.Background(), bytes.NewReader(input), &response, &shmConnState{}); err != nil {
		t.Fatal(err)
	}
	if state.calls != 0 {
		t.Fatalf("exchange handler ran %d times after external resolve failed", state.calls)
	}
	if !strings.Contains(response.String(), "external input resolve failed") {
		// Arrow metadata is encoded verbatim in the response stream, making this
		// a useful assertion without coupling the test to the error schema.
		t.Fatalf("response did not contain external resolve error")
	}
}

type recordingStorage struct{ uploads int }

func (s *recordingStorage) Upload([]byte, *arrow.Schema, string) (string, error) {
	s.uploads++
	return "https://storage.invalid/result", nil
}

func TestUnaryExternalizedResultOwnership(t *testing.T) {
	for _, transport := range []string{"raw", "http"} {
		t.Run(transport, func(t *testing.T) {
			storage := &recordingStorage{}
			s := NewServer()
			s.SetExternalLocation(&ExternalLocationConfig{Storage: storage, ExternalizeThresholdBytes: 1})
			Unary(s, "external_result", func(_ context.Context, _ *CallContext, p regressionParams) (regressionResult, error) {
				return regressionResult(p), nil
			})
			params := regressionBatch(t, 9)
			body := regressionRequest(t, "external_result", params)
			params.Release()

			if transport == "raw" {
				var response bytes.Buffer
				if err := s.serveOne(context.Background(), bytes.NewReader(body), &response, &shmConnState{}); err != nil {
					t.Fatal(err)
				}
			} else {
				h := NewHttpServer(s)
				h.InitPages()
				req := httptest.NewRequest(http.MethodPost, "/external_result", bytes.NewReader(body))
				req.Header.Set("Content-Type", arrowContentType)
				w := httptest.NewRecorder()
				h.ServeHTTP(w, req)
				if w.Code != http.StatusOK {
					t.Fatalf("expected 200, got %d", w.Code)
				}
			}
			if storage.uploads != 1 {
				t.Fatalf("expected one externalized result, got %d", storage.uploads)
			}
		})
	}
}
