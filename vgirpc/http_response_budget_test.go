// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestParsePositiveSafeDecimal(t *testing.T) {
	for _, good := range []string{"1", "256", "9007199254740991"} {
		if _, err := parsePositiveSafeDecimal(good); err != nil {
			t.Errorf("parsePositiveSafeDecimal(%q): %v", good, err)
		}
	}
	for _, bad := range []string{"", "0", "01", "+1", " 1", "1 ", "1.0", "9007199254740992"} {
		if _, err := parsePositiveSafeDecimal(bad); err == nil {
			t.Errorf("parsePositiveSafeDecimal(%q) unexpectedly succeeded", bad)
		}
	}
}

func TestParseResponseBudgetDecimalEnforcesMinimum(t *testing.T) {
	for _, good := range []string{"65536", "9007199254740991"} {
		if _, err := parseResponseBudgetDecimal(good); err != nil {
			t.Errorf("parseResponseBudgetDecimal(%q): %v", good, err)
		}
	}
	for _, bad := range []string{"1", "65535"} {
		if _, err := parseResponseBudgetDecimal(bad); err == nil {
			t.Errorf("parseResponseBudgetDecimal(%q) unexpectedly succeeded", bad)
		}
	}
}

func TestResponseBudgetUsesEffectiveMinimumAndClampsPreference(t *testing.T) {
	h := NewHttpServer(NewServer())
	h.SetMaxRequestBytes(900)
	h.SetHostingMaxRequestBytes(700)
	h.SetMaxResponseBytes(80_000)
	h.SetHostingMaxResponseBytes(70_000)
	h.SetPreferredResponseBytes(75_000)
	headers := make(http.Header)
	headers.Set(acceptMaxResponseBytesHeader, "65536")
	budget, err := h.responseBudget(headers)
	if err != nil {
		t.Fatal(err)
	}
	if budget.Limit != 65_536 || budget.Preferred != 65_536 {
		t.Fatalf("budget = %+v, want limit/preferred 65536", budget)
	}
	if got := h.effectiveRequestLimit(); got != 700 {
		t.Fatalf("request limit = %d, want 700", got)
	}
}

func TestCapabilitiesAdvertiseResponseAcceptanceAndEffectiveLimits(t *testing.T) {
	h := NewHttpServer(NewServer())
	h.SetMaxRequestBytes(900)
	h.SetHostingMaxRequestBytes(700)
	h.SetMaxResponseBytes(80_000)
	h.SetHostingMaxResponseBytes(70_000)
	h.SetCorsOrigins("*")
	req := httptest.NewRequest(http.MethodOptions, "/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if got := w.Header().Get(maxRequestBytesHeader); got != "700" {
		t.Fatalf("%s = %q, want 700", maxRequestBytesHeader, got)
	}
	if got := w.Header().Get(maxResponseBytesHeader); got != "70000" {
		t.Fatalf("%s = %q, want 70000", maxResponseBytesHeader, got)
	}
	if got := w.Header().Get(acceptMaxResponseBytesSupportHeader); got != "true" {
		t.Fatalf("%s = %q, want true", acceptMaxResponseBytesSupportHeader, got)
	}
	if got := w.Header().Get("Access-Control-Allow-Headers"); !strings.Contains(got, acceptMaxResponseBytesHeader) {
		t.Fatalf("allow headers %q omit %s", got, acceptMaxResponseBytesHeader)
	}
	if got := w.Header().Get("Access-Control-Expose-Headers"); !strings.Contains(got, acceptMaxResponseBytesSupportHeader) {
		t.Fatalf("expose headers %q omit %s", got, acceptMaxResponseBytesSupportHeader)
	}
}

func TestOptionsValidatesAcceptedResponseLimit(t *testing.T) {
	h := NewHttpServer(NewServer())

	for _, tc := range []struct {
		name   string
		values []string
		status int
	}{
		{name: "absent", status: http.StatusNoContent},
		{name: "valid minimum", values: []string{"65536"}, status: http.StatusNoContent},
		{name: "leading zero", values: []string{"065536"}, status: http.StatusBadRequest},
		{name: "comma coalesced", values: []string{"65536, 70000"}, status: http.StatusBadRequest},
		{name: "non ascii", values: []string{"٦٥٥٣٦"}, status: http.StatusBadRequest},
		{name: "below minimum", values: []string{"65535"}, status: http.StatusBadRequest},
		{name: "duplicate", values: []string{"65536", "70000"}, status: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodOptions, "/health", nil)
			for _, value := range tc.values {
				req.Header.Add(acceptMaxResponseBytesHeader, value)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != tc.status {
				t.Fatalf("status=%d body=%q, want %d", w.Code, w.Body.String(), tc.status)
			}
			if got := w.Header().Get(acceptMaxResponseBytesSupportHeader); got != "true" {
				t.Fatalf("%s=%q, want true", acceptMaxResponseBytesSupportHeader, got)
			}
			if tc.status == http.StatusBadRequest {
				if got := w.Header().Get(rpcErrorHeader); got != "true" {
					t.Fatalf("%s=%q, want true", rpcErrorHeader, got)
				}
				if got := w.Header().Get("Content-Type"); got != arrowContentType {
					t.Fatalf("Content-Type=%q, want %q", got, arrowContentType)
				}
				if !bytes.Contains(w.Body.Bytes(), []byte("ValueError")) {
					t.Fatalf("Arrow error body does not contain ValueError metadata")
				}
			}
		})
	}
}

type responseCapExchangeState struct{}

func (*responseCapExchangeState) Exchange(context.Context, arrow.RecordBatch, *OutputCollector, *CallContext) error {
	return nil
}

func TestFinalResponseCapUsesNegotiatedClientLimitForEveryResponseShape(t *testing.T) {
	RegisterStateType(&responseCapExchangeState{})
	large := strings.Repeat("x", 70<<10)
	s := NewServer()
	UnaryVoid(s, "large_void", func(_ context.Context, call *CallContext, _ struct{}) error {
		call.ClientLog(LogError, large)
		return nil
	})
	UnaryVoid(s, "large_error", func(context.Context, *CallContext, struct{}) error {
		return &RpcError{Type: "RuntimeError", Message: large}
	})
	Exchange(s, "large_exchange_init", regressionSchema, regressionSchema,
		func(_ context.Context, call *CallContext, _ regressionParams) (*StreamResult, error) {
			call.ClientLog(LogError, large)
			return &StreamResult{
				OutputSchema: regressionSchema,
				InputSchema:  regressionSchema,
				State:        &responseCapExchangeState{},
			}, nil
		})
	h := NewHttpServer(s)

	for _, tc := range []struct {
		name   string
		path   string
		params func(*testing.T) arrow.RecordBatch
	}{
		{name: "void", path: "/large_void", params: func(*testing.T) arrow.RecordBatch { return emptyBatch(arrowEmptySchema) }},
		{name: "error", path: "/large_error", params: func(*testing.T) arrow.RecordBatch { return emptyBatch(arrowEmptySchema) }},
		{name: "exchange init", path: "/large_exchange_init/init", params: func(t *testing.T) arrow.RecordBatch { return regressionBatch(t, 1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := tc.params(t)
			body := regressionRequest(t, strings.TrimSuffix(strings.TrimPrefix(tc.path, "/"), "/init"), params)
			params.Release()
			req := httptest.NewRequest(http.MethodPost, tc.path, bytes.NewReader(body))
			req.Header.Set("Content-Type", arrowContentType)
			req.Header.Set(acceptMaxResponseBytesHeader, "65536")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != http.StatusOK || w.Header().Get(rpcErrorHeader) != "true" {
				t.Fatalf("status=%d rpc-error=%q body=%q, want bounded in-band error", w.Code, w.Header().Get(rpcErrorHeader), w.Body.String())
			}
			if w.Body.Len() > 65_536 {
				t.Fatalf("replacement error is %d bytes, exceeds client limit", w.Body.Len())
			}
			if !bytes.Contains(w.Body.Bytes(), []byte("ResponseTooLargeError")) {
				t.Fatal("response was not replaced with ResponseTooLargeError")
			}
			if token, callToken := FindStreamTokens(w.Body.Bytes()); token != nil || callToken != nil {
				t.Fatalf("oversize response leaked cursor=%q call-token=%q", token, callToken)
			}
		})
	}
}

func TestResponseCapWriterRetainsOnlyLimitPlusOne(t *testing.T) {
	server := NewHttpServer(NewServer())
	recorder := httptest.NewRecorder()
	budget := &httpResponseBudget{Limit: 65_536}
	writer := newResponseCapWriter(recorder, budget, server, "/large")
	payload := bytes.Repeat([]byte("x"), 8<<20)
	n, err := writer.Write(payload)
	if err != nil || n != len(payload) {
		t.Fatalf("Write = (%d, %v), want (%d, nil)", n, err, len(payload))
	}
	if writer.body.Len() != int(budget.Limit+1) {
		t.Fatalf("retained body = %d bytes, want %d", writer.body.Len(), budget.Limit+1)
	}
	if writer.bodyBytes != int64(len(payload)) {
		t.Fatalf("observed body = %d bytes, want %d", writer.bodyBytes, len(payload))
	}
	writer.finish()
	if recorder.Code != http.StatusOK || recorder.Header().Get(rpcErrorHeader) != "true" {
		t.Fatalf("status=%d rpc-error=%q, want bounded structured error", recorder.Code, recorder.Header().Get(rpcErrorHeader))
	}
	if !bytes.Contains(recorder.Body.Bytes(), []byte("ResponseTooLargeError")) {
		t.Fatal("replacement body does not contain ResponseTooLargeError")
	}
}

func TestMalformedAcceptedResponseLimitRejectedBeforeDispatch(t *testing.T) {
	called := false
	s := NewServer()
	UnaryVoid(s, "noop", func(context.Context, *CallContext, struct{}) error {
		called = true
		return nil
	})
	h := NewHttpServer(s)
	params := emptyBatch(arrowEmptySchema)
	defer params.Release()
	req := httptest.NewRequest(http.MethodPost, "/noop", bytes.NewReader(regressionRequest(t, "noop", params)))
	req.Header.Set("Content-Type", arrowContentType)
	req.Header.Set(acceptMaxResponseBytesHeader, "01")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest || called {
		t.Fatalf("status=%d called=%v, want 400 and no dispatch", w.Code, called)
	}
}

func TestAuthenticationPrecedesResponseBudgetAndBodyRejection(t *testing.T) {
	s := NewServer()
	UnaryVoid(s, "noop", func(context.Context, *CallContext, struct{}) error { return nil })
	h := NewHttpServer(s)
	h.SetMaxRequestBytes(64 << 10)
	h.SetAuthenticate(func(*http.Request) (*AuthContext, error) {
		return nil, NewAuthFailure(AuthReasonMissingCredential, "credential required")
	})
	req := httptest.NewRequest(http.MethodPost, "/noop", strings.NewReader(strings.Repeat("x", 70<<10)))
	req.Header.Set("Content-Type", arrowContentType)
	req.Header.Set(acceptMaxResponseBytesHeader, "1")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%q, want authentication rejection before budget/body validation", w.Code, w.Body.String())
	}
}

type strictOversizeProducer struct{}

func (*strictOversizeProducer) Produce(_ context.Context, out *OutputCollector, call *CallContext) error {
	if out.ResponseLimitBytes() != 65_536 || out.PreferredResponseBytes() != 65_536 ||
		call.ResponseLimitBytes != 65_536 || call.PreferredResponseBytes != 65_536 {
		return &RpcError{Type: "RuntimeError", Message: "response budget was not propagated"}
	}
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	for i := 0; i < 20_000; i++ {
		b.Append(int64(i))
	}
	values := b.NewInt64Array()
	defer values.Release()
	return out.EmitArrays([]arrow.Array{values}, int64(values.Len()))
}

type sealedBudgetProducer struct{ Turn int }

func (p *sealedBudgetProducer) Produce(_ context.Context, out *OutputCollector, _ *CallContext) error {
	rows := 1
	if p.Turn > 0 {
		rows = 20_000
	}
	p.Turn++
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	for i := 0; i < rows; i++ {
		b.Append(int64(i))
	}
	values := b.NewInt64Array()
	defer values.Release()
	return out.EmitArrays([]arrow.Array{values}, int64(values.Len()))
}

func TestProducerContinuationCannotRaiseInitialResponseLimit(t *testing.T) {
	RegisterStateType(&sealedBudgetProducer{})
	s := NewServer()
	Producer(s, "sealed_budget", regressionSchema,
		func(context.Context, *CallContext, regressionParams) (*StreamResult, error) {
			return &StreamResult{OutputSchema: regressionSchema, State: &sealedBudgetProducer{}}, nil
		})
	h := NewHttpServer(s)
	h.SetCallStateCacheEntries(0)
	params := regressionBatch(t, 1)
	initReq := httptest.NewRequest(http.MethodPost, "/sealed_budget/init",
		bytes.NewReader(regressionRequest(t, "sealed_budget", params)))
	params.Release()
	initReq.Header.Set("Content-Type", arrowContentType)
	initReq.Header.Set(acceptMaxResponseBytesHeader, "65536")
	initW := httptest.NewRecorder()
	h.ServeHTTP(initW, initReq)
	token, callToken := FindStreamTokens(initW.Body.Bytes())
	if token == nil || callToken == nil {
		t.Fatalf("init omitted continuation tokens: status=%d body=%q", initW.Code, initW.Body.String())
	}

	tick := array.NewRecordBatch(arrow.NewSchema(nil, nil), nil, 0)
	meta := arrow.NewMetadata(
		[]string{MetaStreamState, MetaCallState},
		[]string{string(token), string(callToken)},
	)
	body := regressionIPC(t, tick, meta)
	tick.Release()
	contReq := httptest.NewRequest(http.MethodPost, "/sealed_budget/exchange", bytes.NewReader(body))
	contReq.Header.Set("Content-Type", arrowContentType)
	contReq.Header.Set(acceptMaxResponseBytesHeader, "131072")
	contW := httptest.NewRecorder()
	h.ServeHTTP(contW, contReq)
	if contW.Code != http.StatusOK || contW.Header().Get(rpcErrorHeader) != "true" {
		t.Fatalf("continuation status=%d rpc-error=%q, want bounded in-band error", contW.Code, contW.Header().Get(rpcErrorHeader))
	}
	if next, nextCall := FindStreamTokens(contW.Body.Bytes()); next != nil || nextCall != nil {
		t.Fatalf("oversized continuation leaked cursor=%q call-token=%q", next, nextCall)
	}
}

func TestProducerOversizePublishesNoCursor(t *testing.T) {
	RegisterStateType(&strictOversizeProducer{})
	s := NewServer()
	Producer(s, "strict_producer", regressionSchema,
		func(context.Context, *CallContext, regressionParams) (*StreamResult, error) {
			return &StreamResult{OutputSchema: regressionSchema, State: &strictOversizeProducer{}}, nil
		})
	h := NewHttpServer(s)
	h.SetPreferredResponseBytes(65_536)
	params := regressionBatch(t, 1)
	defer params.Release()
	req := httptest.NewRequest(http.MethodPost, "/strict_producer/init", bytes.NewReader(regressionRequest(t, "strict_producer", params)))
	req.Header.Set("Content-Type", arrowContentType)
	req.Header.Set(acceptMaxResponseBytesHeader, "65536")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK || w.Header().Get(rpcErrorHeader) != "true" {
		t.Fatalf("status=%d rpc-error=%q body=%q, want in-band strict error", w.Code, w.Header().Get(rpcErrorHeader), w.Body.String())
	}
	if token, callToken := FindStreamTokens(w.Body.Bytes()); token != nil || callToken != nil {
		t.Fatalf("oversize response leaked cursor=%q call-token=%q", token, callToken)
	}
}

func TestProducerExternalizationRescuesStrictTurn(t *testing.T) {
	RegisterStateType(&strictOversizeProducer{})
	storage := newMockStorage()
	s := NewServer()
	s.SetExternalLocation(&ExternalLocationConfig{Storage: storage, ExternalizeThresholdBytes: 1 << 30})
	Producer(s, "rescued_producer", regressionSchema,
		func(context.Context, *CallContext, regressionParams) (*StreamResult, error) {
			return &StreamResult{OutputSchema: regressionSchema, State: &strictOversizeProducer{}}, nil
		})
	h := NewHttpServer(s)
	h.SetPreferredResponseBytes(65_536)
	params := regressionBatch(t, 1)
	defer params.Release()
	req := httptest.NewRequest(http.MethodPost, "/rescued_producer/init", bytes.NewReader(regressionRequest(t, "rescued_producer", params)))
	req.Header.Set("Content-Type", arrowContentType)
	req.Header.Set(acceptMaxResponseBytesHeader, "65536")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK || w.Header().Get(rpcErrorHeader) != "" {
		t.Fatalf("status=%d rpc-error=%q body=%q", w.Code, w.Header().Get(rpcErrorHeader), w.Body.String())
	}
	if storage.counter != 1 {
		t.Fatalf("uploads=%d, want one forced producer rescue", storage.counter)
	}
	if token, callToken := FindStreamTokens(w.Body.Bytes()); token == nil || callToken == nil {
		t.Fatalf("rescued active producer omitted continuation tokens")
	}
}

func TestNativeClientAdvertisesAcceptedResponseLimit(t *testing.T) {
	seen := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Get(acceptMaxResponseBytesHeader)
		w.Header().Set(acceptMaxResponseBytesSupportHeader, "true")
		w.Header().Set(maxResponseBytesHeader, "123456")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := NewHttpClient(server.URL, WithClientAcceptedMaxResponseBytes(65_536))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	caps, err := client.DiscoverCapabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if seen != "65536" {
		t.Fatalf("accepted response header = %q, want 65536", seen)
	}
	if !caps.AcceptMaxResponseBytesSupport || caps.MaxResponseBytes != 123456 {
		t.Fatalf("capabilities = %+v", caps)
	}
}

func TestCapabilityParserRejectsUnsafeOrLooseDecimals(t *testing.T) {
	for _, value := range []string{"0", "01", "+1", "9007199254740992"} {
		headers := make(http.Header)
		headers.Set(maxResponseBytesHeader, value)
		if _, err := ParseHTTPServerCapabilities(headers); err == nil {
			t.Errorf("capability decimal %q unexpectedly accepted", value)
		}
	}
}

func TestCapabilityParserRequiresExactlyOneTrueSupportHeader(t *testing.T) {
	for name, values := range map[string][]string{
		"false":     {"false"},
		"uppercase": {"TRUE"},
		"duplicate": {"true", "true"},
	} {
		t.Run(name, func(t *testing.T) {
			headers := make(http.Header)
			for _, value := range values {
				headers.Add(acceptMaxResponseBytesSupportHeader, value)
			}
			if _, err := ParseHTTPServerCapabilities(headers); err == nil {
				t.Fatal("malformed response-budget support capability was accepted")
			}
		})
	}
}

func TestUnaryExternalizationRescuesClientResponseLimit(t *testing.T) {
	storage := newMockStorage()
	s := NewServer()
	s.SetExternalLocation(&ExternalLocationConfig{
		Storage:                   storage,
		ExternalizeThresholdBytes: 1 << 30,
	})
	var seenLimit, seenPreferred int64
	Unary(s, "large_result", func(_ context.Context, call *CallContext, _ struct{}) ([]byte, error) {
		seenLimit = call.ResponseLimitBytes
		seenPreferred = call.PreferredResponseBytes
		return bytes.Repeat([]byte("x"), 64<<10), nil
	})
	h := NewHttpServer(s)
	h.SetMaxResponseBytes(128 << 10)
	h.SetHostingMaxResponseBytes(96 << 10)
	h.SetPreferredResponseBytes(80 << 10)
	params := emptyBatch(arrowEmptySchema)
	defer params.Release()
	req := httptest.NewRequest(http.MethodPost, "/large_result", bytes.NewReader(regressionRequest(t, "large_result", params)))
	req.Header.Set("Content-Type", arrowContentType)
	req.Header.Set(acceptMaxResponseBytesHeader, "65536")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", w.Code, w.Body.String())
	}
	if w.Body.Len() > 65536 {
		t.Fatalf("rescued body size=%d exceeds accepted max", w.Body.Len())
	}
	if storage.counter != 1 {
		t.Fatalf("uploads=%d, want one forced rescue", storage.counter)
	}
	if seenLimit != 65536 || seenPreferred != 65536 {
		t.Fatalf("handler budget=(%d,%d), want (65536,65536)", seenLimit, seenPreferred)
	}
}

// arrowEmptySchema avoids rebuilding a logically identical schema in tests.
var arrowEmptySchema = arrow.NewSchema(nil, nil)
