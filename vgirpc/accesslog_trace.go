// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"log/slog"
	"sync/atomic"
)

// TraceContextFunc reports the W3C trace and span IDs of whatever span is
// current in ctx, as lowercase hex — 32 characters and 16 characters
// respectively — or two empty strings when no valid span is current.
//
// It must never panic and must never block: it runs once per access-log
// record, on the dispatch path.
type TraceContextFunc func(ctx context.Context) (traceID, spanID string)

// traceContextProvider is nil until an application installs one. Core carries
// no OpenTelemetry dependency, so the accessor is injected rather than
// imported: see [SetTraceContextProvider].
var traceContextProvider atomic.Pointer[TraceContextFunc]

// SetTraceContextProvider installs the accessor the access log uses to read
// trace correlation IDs. Pass nil to remove it.
//
// [DispatchInfo.RequestID] only joins records within one service, so without
// this a log line and the span describing the same call cannot be matched —
// in a framework that already ships OTel instrumentation. The accessor reads
// from whatever span is current in the dispatch context rather than from
// anything this framework threads through, so a record correlates with an
// application-opened span as readily as one opened by vgirpc/otel.
//
// Core does not import OpenTelemetry, so the wiring is one line in the
// application:
//
//	vgirpc.SetTraceContextProvider(vgiotel.TraceContext)
//
// Safe to call from any goroutine, but intended for process startup: the
// value is read on every dispatch.
func SetTraceContextProvider(fn TraceContextFunc) {
	if fn == nil {
		traceContextProvider.Store(nil)
		return
	}
	traceContextProvider.Store(&fn)
}

// currentTraceContext returns the (trace_id, span_id) pair for the span
// current in ctx, or two empty strings.
//
// Both or neither: a record carrying only one of the pair is useless for
// correlation and fails the access-log schema, so a provider that returns a
// malformed value is treated as having returned nothing. An observability
// failure must not surface as a request failure either, hence the recover.
func currentTraceContext(ctx context.Context) (traceID, spanID string) {
	p := traceContextProvider.Load()
	if p == nil {
		return "", ""
	}
	defer func() {
		if rv := recover(); rv != nil {
			slog.Debug("access log: trace context provider panicked", "err", rv)
			traceID, spanID = "", ""
		}
	}()
	traceID, spanID = (*p)(ctx)
	if !isLowerHex(traceID, 32) || !isLowerHex(spanID, 16) {
		return "", ""
	}
	return traceID, spanID
}

// isLowerHex reports whether s is exactly n lowercase hex characters. The
// access-log schema enforces the same patterns cross-language, so a port
// emitting a dashed UUID fails validation — checking here turns that into a
// dropped field rather than an invalid record.
func isLowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
