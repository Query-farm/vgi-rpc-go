// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Package vgiotel provides OpenTelemetry instrumentation for vgi-rpc servers.
// It implements the [vgirpc.DispatchHook] interface to add distributed tracing
// and metrics to RPC dispatch.
//
// Usage:
//
//	server := vgirpc.NewServer()
//	// ... register methods ...
//	vgiotel.InstrumentServer(server, vgiotel.DefaultConfig())
package vgiotel

import (
	"context"
	"fmt"
	"time"

	"github.com/Query-farm/vgi-rpc/vgirpc"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "vgi_rpc"

// OtelConfig configures OpenTelemetry instrumentation for a vgi-rpc server.
type OtelConfig struct {
	// TracerProvider supplies the tracer. Defaults to otel.GetTracerProvider().
	TracerProvider trace.TracerProvider
	// MeterProvider supplies the meter. Defaults to otel.GetMeterProvider().
	MeterProvider metric.MeterProvider
	// Propagator extracts trace context from transport metadata.
	// Defaults to otel.GetTextMapPropagator().
	Propagator propagation.TextMapPropagator
	// EnableTracing enables span creation. Default true.
	EnableTracing bool
	// EnableMetrics enables counter and histogram recording. Default true.
	EnableMetrics bool
	// RecordExceptions calls RecordError on the span for failed dispatches.
	// Default true.
	RecordExceptions bool
	// ServiceName is the rpc.service attribute value.
	// Defaults to Server.ServiceName() or "GoRpcServer".
	ServiceName string
	// CustomAttributes are added to every span.
	CustomAttributes []attribute.KeyValue
}

// DefaultConfig returns an OtelConfig with sensible defaults.
// TracerProvider, MeterProvider, and Propagator are resolved from the
// global OTel SDK at instrumentation time.
func DefaultConfig() OtelConfig {
	return OtelConfig{
		EnableTracing:    true,
		EnableMetrics:    true,
		RecordExceptions: true,
	}
}

// InstrumentServer attaches OpenTelemetry instrumentation to a vgi-rpc server.
// The hook is installed via [vgirpc.Server.SetDispatchHook].
func InstrumentServer(server *vgirpc.Server, cfg OtelConfig) {
	if cfg.TracerProvider == nil {
		cfg.TracerProvider = otel.GetTracerProvider()
	}
	if cfg.MeterProvider == nil {
		cfg.MeterProvider = otel.GetMeterProvider()
	}
	if cfg.Propagator == nil {
		cfg.Propagator = otel.GetTextMapPropagator()
	}
	if cfg.ServiceName == "" {
		if sn := server.ServiceName(); sn != "" {
			cfg.ServiceName = sn
		} else {
			cfg.ServiceName = "GoRpcServer"
		}
	}

	hook := &otelHook{
		cfg:    cfg,
		tracer: cfg.TracerProvider.Tracer(instrumentationName),
	}

	if cfg.EnableMetrics {
		meter := cfg.MeterProvider.Meter(instrumentationName)
		hook.requestCounter, _ = meter.Int64Counter("rpc.server.requests",
			metric.WithUnit("{request}"),
			metric.WithDescription("Number of RPC requests"),
		)
		hook.durationHistogram, _ = meter.Float64Histogram("rpc.server.duration",
			metric.WithUnit("s"),
			metric.WithDescription("Duration of RPC requests"),
		)
	}

	server.SetDispatchHook(hook)
}

// otelHook implements vgirpc.DispatchHook with OpenTelemetry tracing and metrics.
type otelHook struct {
	cfg                OtelConfig
	tracer             trace.Tracer
	requestCounter     metric.Int64Counter
	durationHistogram  metric.Float64Histogram
}

// spanToken is the HookToken returned by OnDispatchStart.
type spanToken struct {
	span      trace.Span
	startTime time.Time
}

// OnDispatchStart extracts parent trace context and starts a server span.
func (h *otelHook) OnDispatchStart(ctx context.Context, info vgirpc.DispatchInfo) (context.Context, vgirpc.HookToken) {
	// Extract parent trace context from transport metadata (traceparent/tracestate)
	if h.cfg.Propagator != nil && info.TransportMetadata != nil {
		carrier := propagation.MapCarrier(info.TransportMetadata)
		ctx = h.cfg.Propagator.Extract(ctx, carrier)
	}

	if !h.cfg.EnableTracing {
		return ctx, &spanToken{startTime: time.Now()}
	}

	spanName := fmt.Sprintf("vgi_rpc/%s", info.Method)

	attrs := []attribute.KeyValue{
		attribute.String("rpc.system", "vgi_rpc"),
		attribute.String("rpc.service", h.cfg.ServiceName),
		attribute.String("rpc.method", info.Method),
		attribute.String("rpc.vgi_rpc.method_type", info.MethodType),
		attribute.String("rpc.vgi_rpc.server_id", info.ServerID),
	}
	attrs = append(attrs, h.cfg.CustomAttributes...)

	// Add transport metadata attributes (HTTP only)
	if v, ok := info.TransportMetadata["remote_addr"]; ok && v != "" {
		attrs = append(attrs, attribute.String("net.peer.ip", v))
	}
	if v, ok := info.TransportMetadata["user_agent"]; ok && v != "" {
		attrs = append(attrs, attribute.String("user_agent.original", v))
	}

	ctx, span := h.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(attrs...),
	)

	return ctx, &spanToken{span: span, startTime: time.Now()}
}

// OnDispatchEnd records span attributes, metrics, and ends the span.
func (h *otelHook) OnDispatchEnd(ctx context.Context, token vgirpc.HookToken, info vgirpc.DispatchInfo, stats *vgirpc.CallStatistics, err error) {
	st, ok := token.(*spanToken)
	if !ok {
		return
	}

	duration := time.Since(st.startTime)

	status := "ok"
	if err != nil {
		status = "error"
	}

	// Record metrics
	if h.cfg.EnableMetrics {
		metricAttrs := metric.WithAttributes(
			attribute.String("rpc.system", "vgi_rpc"),
			attribute.String("rpc.service", h.cfg.ServiceName),
			attribute.String("rpc.method", info.Method),
			attribute.String("rpc.vgi_rpc.method_type", info.MethodType),
			attribute.String("status", status),
		)
		if h.requestCounter != nil {
			h.requestCounter.Add(ctx, 1, metricAttrs)
		}
		if h.durationHistogram != nil {
			h.durationHistogram.Record(ctx, duration.Seconds(), metricAttrs)
		}
	}

	// Record span attributes and status
	if st.span != nil && st.span.IsRecording() {
		if stats != nil {
			st.span.SetAttributes(
				attribute.Int64("rpc.vgi_rpc.input_batches", stats.InputBatches),
				attribute.Int64("rpc.vgi_rpc.output_batches", stats.OutputBatches),
				attribute.Int64("rpc.vgi_rpc.input_rows", stats.InputRows),
				attribute.Int64("rpc.vgi_rpc.output_rows", stats.OutputRows),
				attribute.Int64("rpc.vgi_rpc.input_bytes", stats.InputBytes),
				attribute.Int64("rpc.vgi_rpc.output_bytes", stats.OutputBytes),
			)
		}

		if err != nil {
			st.span.SetStatus(codes.Error, err.Error())
			if h.cfg.RecordExceptions {
				st.span.RecordError(err)
			}
			// Set error type attribute
			errType := fmt.Sprintf("%T", err)
			if rpcErr, ok := err.(*vgirpc.RpcError); ok {
				errType = rpcErr.Type
			}
			st.span.SetAttributes(attribute.String("rpc.vgi_rpc.error_type", errType))
		} else {
			st.span.SetStatus(codes.Ok, "")
		}

		st.span.End()
	}
}
