// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
)

type hookCtxKey string

// recordingHook appends "start:<name>" / "end:<name>" to a shared log, stamps
// its name into the context it returns, and checks that OnDispatchEnd gets
// back the context and token it handed out.
type recordingHook struct {
	t          *testing.T
	name       string
	mu         *sync.Mutex
	log        *[]string
	panicStart bool
	panicEnd   bool
	sawCtx     []string // context values visible at OnDispatchStart
}

func (h *recordingHook) OnDispatchStart(ctx context.Context, _ DispatchInfo) (context.Context, HookToken) {
	h.mu.Lock()
	*h.log = append(*h.log, "start:"+h.name)
	h.mu.Unlock()
	for _, k := range []string{"a", "b", "c"} {
		if v, ok := ctx.Value(hookCtxKey(k)).(string); ok {
			h.sawCtx = append(h.sawCtx, v)
		}
	}
	if h.panicStart {
		panic("start " + h.name)
	}
	return context.WithValue(ctx, hookCtxKey(h.name), h.name), "token-" + h.name
}

func (h *recordingHook) OnDispatchEnd(ctx context.Context, token HookToken, _ DispatchInfo, _ *CallStatistics, _ error) {
	h.mu.Lock()
	*h.log = append(*h.log, "end:"+h.name)
	h.mu.Unlock()
	if got := ctx.Value(hookCtxKey(h.name)); got != h.name {
		h.t.Errorf("hook %s: OnDispatchEnd context lacks the value its own start set (got %v)", h.name, got)
	}
	if token != "token-"+h.name {
		h.t.Errorf("hook %s: got token %v, want its own", h.name, token)
	}
	if h.panicEnd {
		panic("end " + h.name)
	}
}

func newRecordingHooks(t *testing.T, names ...string) ([]*recordingHook, *[]string) {
	var mu sync.Mutex
	log := &[]string{}
	hooks := make([]*recordingHook, len(names))
	for i, n := range names {
		hooks[i] = &recordingHook{t: t, name: n, mu: &mu, log: log}
	}
	return hooks, log
}

func TestMultiDispatchHookOrderAndContext(t *testing.T) {
	hs, log := newRecordingHooks(t, "a", "b", "c")
	m := MultiDispatchHook(hs[0], hs[1], hs[2])

	ctx, tok := m.OnDispatchStart(context.Background(), DispatchInfo{})
	for _, k := range []string{"a", "b", "c"} {
		if ctx.Value(hookCtxKey(k)) != k {
			t.Errorf("returned context lacks hook %s's value", k)
		}
	}
	m.OnDispatchEnd(ctx, tok, DispatchInfo{}, &CallStatistics{}, nil)

	want := []string{"start:a", "start:b", "start:c", "end:c", "end:b", "end:a"}
	if !reflect.DeepEqual(*log, want) {
		t.Fatalf("call order %v, want %v", *log, want)
	}
	// Each hook sees the contexts of the hooks before it, not after.
	if !reflect.DeepEqual(hs[2].sawCtx, []string{"a", "b"}) || len(hs[0].sawCtx) != 0 {
		t.Fatalf("context chaining wrong: a saw %v, c saw %v", hs[0].sawCtx, hs[2].sawCtx)
	}
}

// A panicking hook must not take its neighbours down, and a hook whose start
// panicked gets no end -- the server's rule for a lone hook.
func TestMultiDispatchHookIsolatesPanics(t *testing.T) {
	hs, log := newRecordingHooks(t, "a", "b", "c")
	hs[1].panicStart = true
	hs[2].panicEnd = true
	m := MultiDispatchHook(hs[0], hs[1], hs[2])

	ctx, tok := m.OnDispatchStart(context.Background(), DispatchInfo{})
	m.OnDispatchEnd(ctx, tok, DispatchInfo{}, &CallStatistics{}, errors.New("boom"))

	want := []string{"start:a", "start:b", "start:c", "end:c", "end:a"}
	if !reflect.DeepEqual(*log, want) {
		t.Fatalf("call order %v, want %v", *log, want)
	}
}

func TestMultiDispatchHookNormalizes(t *testing.T) {
	if MultiDispatchHook() != nil || MultiDispatchHook(nil, nil) != nil {
		t.Fatal("no hooks must yield nil, so the server skips hook work entirely")
	}
	hs, _ := newRecordingHooks(t, "a", "b", "c")
	if got := MultiDispatchHook(nil, hs[0]); got != DispatchHook(hs[0]) {
		t.Fatalf("a single hook must be returned unwrapped, got %T", got)
	}
	nested := MultiDispatchHook(MultiDispatchHook(hs[0], hs[1]), hs[2])
	m, ok := nested.(*multiDispatchHook)
	if !ok || len(m.hooks) != 3 {
		t.Fatalf("nested composites must flatten to three hooks, got %#v", nested)
	}
}

// AddDispatchHook composes rather than replaces, and the access log's deferred
// egress emission (which runs after the composite's OnDispatchEnd returns)
// still produces its record when it shares the server with another hook.
func TestAddDispatchHookKeepsAccessLog(t *testing.T) {
	var buf bytes.Buffer
	accessLog := NewAccessLogHook(&buf, "")
	h := newEgressTestServer(t, accessLog)
	hs, log := newRecordingHooks(t, "other")
	h.server.AddDispatchHook(hs[0])

	body := encodeRequestBodyFor(t, "EgressService", "big", bigParams{N: 1000})
	req := httptest.NewRequest(http.MethodPost, "/EgressService/big", bytes.NewReader(body))
	req.Header.Set("Content-Type", arrowContentType)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}

	if want := []string{"start:other", "end:other"}; !reflect.DeepEqual(*log, want) {
		t.Fatalf("added hook calls %v, want %v", *log, want)
	}
	records := decodeRecords(t, &buf)
	if len(records) != 1 {
		t.Fatalf("expected one access-log record, got %d", len(records))
	}
	if rb, ok := records[0]["response_bytes"].(float64); !ok || int(rb) != rec.Body.Len() {
		t.Fatalf("response_bytes=%v, want %d", records[0]["response_bytes"], rec.Body.Len())
	}
}
