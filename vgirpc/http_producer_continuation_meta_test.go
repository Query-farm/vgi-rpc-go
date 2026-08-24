// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Regression test for the producer continuation turn over HTTP.
//
// On the pipe transports every producer turn is a tick batch whose custom
// metadata reaches the worker, and DuckDB uses that to push *updated* dynamic
// filters (vgi_pushdown_filters — Top-N boundary tightening, join-key IN sets)
// between ticks. Over HTTP a turn is a continuation POST, so the server has to
// forward that request's metadata as the turn's first-tick metadata or the
// updates are silently dropped — the server previously passed an empty
// arrow.Metadata{} here, so an HTTP worker saw the filter from /init and never
// any refinement.
//
// The framework's own transport keys (stream-state cursor, call-state, cancel)
// must NOT be visible to user code: the pipe transports never put them on a
// tick, and the stream-state value is a sealed cursor token.

package vgirpc

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// tickMetaRecorder collects the InputMetadata seen by each Produce call.
// The producer state round-trips through the (serialized) cursor token, so the
// continuation runs on a fresh state value — the recorder has to be package
// level rather than a field on the state.
type tickMetaRecorder struct {
	mu    sync.Mutex
	ticks []arrow.Metadata
}

func (r *tickMetaRecorder) record(meta arrow.Metadata) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ticks = append(r.ticks, meta)
}

func (r *tickMetaRecorder) snapshot() []arrow.Metadata {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]arrow.Metadata(nil), r.ticks...)
}

var tickMetaProbe = &tickMetaRecorder{}

// tickMetaProducer records its per-tick InputMetadata and emits one row per
// Produce call without ever finishing, so global lock-step dispatch makes
// every turn end with a continuation token.
type tickMetaProducer struct{ Seq int64 }

func (p *tickMetaProducer) Produce(_ context.Context, out *OutputCollector, callCtx *CallContext) error {
	tickMetaProbe.record(callCtx.InputMetadata)
	p.Seq++
	return out.EmitMap(map[string][]interface{}{"value": {p.Seq}})
}

// TestHTTPProducerContinuationCarriesRequestMetadata proves a continuation
// request's custom metadata reaches the produce callback of that HTTP turn,
// and that the framework's transport keys are stripped before it does.
func TestHTTPProducerContinuationCarriesRequestMetadata(t *testing.T) {
	RegisterStateType(&tickMetaProducer{})
	tickMetaProbe = &tickMetaRecorder{}

	s := NewServer()
	Producer(s, "tick_meta", regressionSchema,
		func(context.Context, *CallContext, regressionParams) (*StreamResult, error) {
			return &StreamResult{OutputSchema: regressionSchema, State: &tickMetaProducer{}}, nil
		})
	h := NewHttpServer(s)
	h.InitPages()

	params := regressionBatch(t, 1)
	initReq := httptest.NewRequest(http.MethodPost, "/tick_meta/init",
		bytes.NewReader(regressionRequest(t, "tick_meta", params)))
	params.Release()
	initReq.Header.Set("Content-Type", arrowContentType)
	initW := httptest.NewRecorder()
	h.ServeHTTP(initW, initReq)
	if initW.Code != http.StatusOK {
		t.Fatalf("init: expected 200, got %d: %s", initW.Code, initW.Body.String())
	}
	token, callToken := FindStreamTokens(initW.Body.Bytes())
	if token == nil || callToken == nil {
		t.Fatalf("init response missing stream tokens after one lock-step transition")
	}

	// The continuation turn: a tick batch carrying a *tightened* filter
	// alongside the framework's transport keys.
	const tightenedFilter = "value < 500"
	tick := array.NewRecordBatch(arrow.NewSchema(nil, nil), nil, 0)
	meta := arrow.NewMetadata(
		[]string{MetaStreamState, MetaCallState, "vgi_pushdown_filters", "vgi_batch_index"},
		[]string{string(token), string(callToken), tightenedFilter, "1"},
	)
	body := regressionIPC(t, tick, meta)
	tick.Release()

	contReq := httptest.NewRequest(http.MethodPost, "/tick_meta/exchange", bytes.NewReader(body))
	contReq.Header.Set("Content-Type", arrowContentType)
	contW := httptest.NewRecorder()
	h.ServeHTTP(contW, contReq)
	if contW.Code != http.StatusOK {
		t.Fatalf("continuation: expected 200, got %d: %s", contW.Code, contW.Body.String())
	}

	ticks := tickMetaProbe.snapshot()
	if len(ticks) != 2 {
		t.Fatalf("expected 2 produce calls (init turn + continuation turn), got %d", len(ticks))
	}

	cont := ticks[1]
	got, ok := cont.GetValue("vgi_pushdown_filters")
	if !ok || got != tightenedFilter {
		t.Fatalf("continuation produce call did not see the request's tick metadata: "+
			"vgi_pushdown_filters=%q present=%v (keys=%v)", got, ok, cont.Keys())
	}
	if got, ok := cont.GetValue("vgi_batch_index"); !ok || got != "1" {
		t.Fatalf("continuation produce call lost non-framework metadata: vgi_batch_index=%q present=%v", got, ok)
	}
	for _, k := range []string{MetaStreamState, MetaCallState, MetaCancel} {
		if v, present := cont.GetValue(k); present {
			t.Fatalf("framework transport key %q leaked into user-visible tick metadata (value %q)", k, v)
		}
	}
}
