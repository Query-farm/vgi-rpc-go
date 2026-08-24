// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

//go:build leakcheck

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
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type emitThenPanicProducer struct{}

func (*emitThenPanicProducer) Produce(_ context.Context, out *OutputCollector, _ *CallContext) error {
	if err := out.EmitMap(map[string][]interface{}{"value": {int64(1)}}); err != nil {
		return err
	}
	panic("after emit")
}

type allocationSamplingExchange struct {
	samples []int
}

func (s *allocationSamplingExchange) Exchange(_ context.Context, input arrow.RecordBatch, out *OutputCollector, _ *CallContext) error {
	s.samples = append(s.samples, leakCheckAllocator().CurrentAlloc())
	value := input.Column(0).(*array.Int64).Value(0)
	return out.EmitMap(map[string][]interface{}{"value": {value}})
}

func assertFlatAllocations(t *testing.T, samples []int) {
	t.Helper()
	if len(samples) < 8 {
		t.Fatalf("expected multiple stream turns, got %d", len(samples))
	}
	minAlloc, maxAlloc := samples[0], samples[0]
	for _, sample := range samples[1:] {
		if sample < minAlloc {
			minAlloc = sample
		}
		if sample > maxAlloc {
			maxAlloc = sample
		}
	}
	if maxAlloc != minAlloc {
		t.Fatalf("per-turn Arrow allocations accumulated across stream: min=%d max=%d samples=%v", minAlloc, maxAlloc, samples)
	}
}

func TestRawStreamReleasesCastBatchEachTurn(t *testing.T) {
	inputSchema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	wireSchema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
	state := &allocationSamplingExchange{}
	s := NewServer()
	Exchange(s, "cast_lifetime", regressionSchema, inputSchema,
		func(context.Context, *CallContext, regressionParams) (*StreamResult, error) {
			return &StreamResult{OutputSchema: regressionSchema, InputSchema: inputSchema, State: state}, nil
		})

	params := regressionBatch(t, 1)
	request := append([]byte(nil), regressionRequest(t, "cast_lifetime", params)...)
	params.Release()
	var turns bytes.Buffer
	w := ipc.NewWriter(&turns, ipc.WithSchema(wireSchema))
	for i := 0; i < 12; i++ {
		b := array.NewInt32Builder(memory.NewGoAllocator())
		b.Append(int32(i))
		col := b.NewArray()
		b.Release()
		rec := array.NewRecordBatch(wireSchema, []arrow.Array{col}, 1)
		col.Release()
		if err := w.Write(rec); err != nil {
			t.Fatal(err)
		}
		rec.Release()
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	request = append(request, turns.Bytes()...)

	var response bytes.Buffer
	if err := s.serveOne(context.Background(), bytes.NewReader(request), &response, &shmConnState{}); err != nil {
		t.Fatal(err)
	}
	assertFlatAllocations(t, state.samples)
}

func TestRawStreamReleasesResolvedExternalBatchEachTurn(t *testing.T) {
	data := regressionBatch(t, 7)
	externalBody := regressionIPC(t, data, arrow.Metadata{})
	data.Release()
	fetch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(externalBody)
	}))
	defer fetch.Close()

	state := &allocationSamplingExchange{}
	s := NewServer()
	s.SetExternalLocation(&ExternalLocationConfig{URLValidator: nil, HTTPClient: fetch.Client()})
	Exchange(s, "external_lifetime", regressionSchema, regressionSchema,
		func(context.Context, *CallContext, regressionParams) (*StreamResult, error) {
			return &StreamResult{OutputSchema: regressionSchema, InputSchema: regressionSchema, State: state}, nil
		})

	params := regressionBatch(t, 1)
	request := append([]byte(nil), regressionRequest(t, "external_lifetime", params)...)
	params.Release()
	pointer, _ := MakeExternalLocationBatch(regressionSchema, fetch.URL)
	defer pointer.Release()
	var turns bytes.Buffer
	w := ipc.NewWriter(&turns, ipc.WithSchema(regressionSchema))
	location := arrow.NewMetadata([]string{MetaLocation}, []string{fetch.URL})
	for i := 0; i < 12; i++ {
		wrapped := array.NewRecordBatchWithMetadata(pointer.Schema(), pointer.Columns(), 0, location)
		if err := w.Write(wrapped); err != nil {
			t.Fatal(err)
		}
		wrapped.Release()
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	request = append(request, turns.Bytes()...)

	var response bytes.Buffer
	if err := s.serveOne(context.Background(), bytes.NewReader(request), &response, &shmConnState{}); err != nil {
		t.Fatal(err)
	}
	assertFlatAllocations(t, state.samples)
}

func TestHTTPProducerPanicReleasesCollectedBatch(t *testing.T) {
	h := NewHttpServer(NewServer())
	info := &methodInfo{Name: "panic_after_emit", OutputSchema: regressionSchema}
	var body bytes.Buffer
	w := ipc.NewWriter(&body, ipc.WithSchema(regressionSchema))
	before := leakCheckAllocator().CurrentAlloc()
	finished, err := h.runProduceTurn(
		context.Background(), w, regressionSchema, &emitThenPanicProducer{}, info,
		&CallStatistics{}, Anonymous(), nil, nil, nil, arrow.Metadata{},
	)
	if finished {
		t.Fatal("panicking producer reported finished")
	}
	if err == nil || !strings.Contains(err.Error(), "after emit") {
		t.Fatalf("expected recovered producer panic, got %v", err)
	}
	if closeErr := w.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if after := leakCheckAllocator().CurrentAlloc(); after != before {
		t.Fatalf("panic path retained collected Arrow batch: before=%d after=%d", before, after)
	}
}
