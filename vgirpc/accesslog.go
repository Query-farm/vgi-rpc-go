// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// AccessLogHook is a [DispatchHook] that emits one JSON record per RPC call
// to an io.Writer, conforming to the cross-language vgi-rpc access-log
// specification (see “docs/access-log-spec.md“ and
// “vgi_rpc/access_log.schema.json“ in the Python reference repo).
//
// Records are written as JSON-Lines (NDJSON), UTF-8 encoded, with one
// record per RPC call. The hook is safe for concurrent use across
// goroutines; writes are serialized through an internal mutex.
//
// Use [NewAccessLogHook] to construct one and pass it to
// [Server.SetDispatchHook] (or wire it in alongside another hook via
// a multiplexing wrapper). The optional behaviours — [AccessLogHook.SetSampleRate]
// and [AccessLogHook.SetAsync] — are configured before the server starts
// serving, and [AccessLogHook.Close] shuts down the async writer.
type AccessLogHook struct {
	mu            sync.Mutex
	w             io.Writer
	serverVersion string
	debug         atomic.Bool
	sampler       atomic.Pointer[accessLogSampler]
	async         atomic.Pointer[asyncEmitter]
}

// NewAccessLogHook returns an [AccessLogHook] that writes records to w.
// serverVersion is reported in the optional “server_version“ field;
// pass an empty string to omit it.
//
// By default the hook emits records at the equivalent of "INFO" level:
// “request_data“ is replaced with “original_request_bytes“ plus
// “truncated: "payload_omitted"“ because the full base64-encoded payload
// typically dominates the record (8+ KiB per call) and most audit consumers
// care about who/what/when rather than the raw bytes. Call
// [AccessLogHook.SetDebug] to re-enable the full payload for replay/audit
// workloads.
func NewAccessLogHook(w io.Writer, serverVersion string) *AccessLogHook {
	return &AccessLogHook{w: w, serverVersion: serverVersion}
}

// SetDebug toggles emission of the full base64-encoded request payload
// in the “request_data“ field. When true, the record carries the
// payload (suitable for replay/audit). When false (default), the
// payload is omitted and the record is marked “truncated: "payload_omitted"“
// with “original_request_bytes“ set so the access-log schema's
// "unary requires request_data unless truncated" invariant still holds.
//
// Mirrors Python's “_access_logger.isEnabledFor(logging.DEBUG)“
// gating introduced in vgi-rpc e7ee750.
func (h *AccessLogHook) SetDebug(debug bool) {
	h.debug.Store(debug)
}

// SetSampleRate keeps only the given fraction of successful calls, 0.0–1.0;
// 1.0 (the default) keeps everything. Returns an error for a rate outside
// that range, which callers are expected to surface at startup rather than
// swallow: a rate of 100 meaning "100%" would otherwise silently log
// everything.
//
// Errors are never sampled, the decision is deterministic per call rather
// than per record, and every kept record carries “sample_rate“. See
// [accessLogSampler] for why each of those is load-bearing.
func (h *AccessLogHook) SetSampleRate(rate float64) error {
	// Validate before the 1.0 shortcut, so a rate of 100 meaning "100%" is
	// rejected rather than rounded into "log everything".
	sampler, err := newAccessLogSampler(rate)
	if err != nil {
		return err
	}
	if rate >= 1.0 {
		h.sampler.Store(nil)
		return nil
	}
	h.sampler.Store(sampler)
	return nil
}

// SetAsync moves record writes to a background goroutine so disk latency
// stays out of the dispatch path, buffering up to queueSize records. Pass 0
// for the default (10000).
//
// Opt-in on purpose: it trades the guarantee that a record on disk means the
// call completed, which is the wrong trade for audit and the right one for
// high throughput. The queue never blocks — full means drop, and the next
// record through carries “dropped_records“. Call [AccessLogHook.Close] at
// shutdown to drain it.
func (h *AccessLogHook) SetAsync(queueSize int) error {
	if queueSize <= 0 {
		queueSize = defaultAccessLogQueueSize
	}
	emitter, err := newAsyncEmitter(queueSize, h.writeRecord)
	if err != nil {
		return err
	}
	if prev := h.async.Swap(emitter); prev != nil {
		prev.close()
	}
	return nil
}

// Close drains and stops the async writer, if one was started. Safe to call
// on a hook that never enabled it, and safe to call more than once.
func (h *AccessLogHook) Close() error {
	if prev := h.async.Swap(nil); prev != nil {
		prev.close()
	}
	return nil
}

type accessLogToken struct {
	start time.Time
}

// OnDispatchStart records the call start time.
func (h *AccessLogHook) OnDispatchStart(ctx context.Context, _ DispatchInfo) (context.Context, HookToken) {
	return ctx, &accessLogToken{start: time.Now()}
}

// OnDispatchEnd assembles a record from the dispatch info, statistics, and
// error and writes it to the underlying io.Writer as one JSON line — or, on
// an HTTP transport, hands it to the request's [egressRecorder] so it can be
// stamped with the response's on-wire size and emitted after compression.
func (h *AccessLogHook) OnDispatchEnd(ctx context.Context, token HookToken, info DispatchInfo, stats *CallStatistics, err error) {
	tok, _ := token.(*accessLogToken)
	var durationMs float64
	if tok != nil {
		durationMs = roundTo2Decimals(float64(time.Since(tok.start).Microseconds()) / 1000.0)
	}

	status := "ok"
	errType := ""
	errMsg := ""
	if err != nil {
		status = "error"
		errType = "Error"
		errMsg = err.Error()
		if rpcErr, ok := err.(*RpcError); ok {
			errType = rpcErr.Type
			errMsg = rpcErr.Message
		}
	}

	now := time.Now().UTC()
	record := map[string]any{
		"timestamp":     now.Format("2006-01-02T15:04:05.000Z"),
		"level":         "INFO",
		"logger":        "vgi_rpc.access",
		"message":       info.Protocol + "." + info.Method + " " + status,
		"server_id":     info.ServerID,
		"protocol":      info.Protocol,
		"protocol_hash": info.ProtocolHash,
		"method":        info.Method,
		"method_type":   info.MethodType,
		"principal":     authPrincipal(info.Auth),
		"auth_domain":   authDomain(info.Auth),
		"authenticated": authAuthenticated(info.Auth),
		"remote_addr":   info.RemoteAddr,
		"duration_ms":   durationMs,
		"status":        status,
		"error_type":    errType,
	}

	rec := egressRecorderFrom(ctx)

	if errMsg != "" {
		record["error_message"] = errMsg
	}
	if h.serverVersion != "" {
		record["server_version"] = h.serverVersion
	}
	// The request batch's own id wins — it is what the response batches echo,
	// so it is the value a client already holds. The transport's id is the
	// fallback, and on HTTP it is always present because the server mints one
	// when the caller sends no X-Request-ID.
	if info.RequestID != "" {
		record["request_id"] = info.RequestID
	} else if rec != nil && rec.requestID != "" {
		record["request_id"] = rec.requestID
	}
	// Trace correlation. request_id only joins records within this service;
	// these join them to the surrounding distributed trace. Emitted both or
	// neither — see currentTraceContext.
	if traceID, spanID := currentTraceContext(ctx); traceID != "" {
		record["trace_id"] = traceID
		record["span_id"] = spanID
	}
	if info.HTTPStatus > 0 {
		record["http_status"] = info.HTTPStatus
	}
	if len(info.RequestData) > 0 {
		// Gate full base64 payload on DEBUG mode. At INFO this field is
		// by far the heaviest in the record; audit consumers rarely need
		// the bytes.
		encoded := base64.StdEncoding.EncodeToString(info.RequestData)
		if h.debug.Load() {
			record["request_data"] = encoded
		} else {
			// "payload_omitted", not true: nothing was lost to a size cap
			// here, the emitter simply is not logging payloads at this
			// level. Sharing one marker made it fire on essentially every
			// record and stop meaning anything to a consumer scanning for
			// real data loss.
			record["original_request_bytes"] = len(encoded)
			record["truncated"] = "payload_omitted"
		}
	}
	if info.MethodType == DispatchMethodStream {
		// Schema requires stream_id (32 lowercase hex chars) on every stream
		// record. When the dispatch path has not yet plumbed a stable
		// per-stream identifier, fall back to a per-dispatch UUID so the
		// record is at least schema-valid. This satisfies the JSON-schema
		// gate but NOT the spec's "stable across continuations" semantic;
		// see docs/porting-guide.md for the conformance gap.
		streamID := info.StreamID
		if streamID == "" {
			streamID = RandomStreamID()
		}
		record["stream_id"] = streamID
	}
	if info.Cancelled {
		record["cancelled"] = true
	}
	if claims := authClaims(info.Auth); len(claims) > 0 {
		// Redacted by key before the record exists. An access log outlives
		// the token by years and is shipped to systems chosen for
		// searchability, so email/phone/*_token reaching it verbatim is a
		// data-retention problem, not a debugging feature. A redactor that
		// panics fails closed and drops the claims entirely.
		if redacted := applyClaimRedaction(claims); len(redacted) > 0 {
			record["claims"] = redacted
		}
	}
	// Egress accounting. The call statistics below measure logical Arrow
	// buffers — what the worker processed. These measure what actually
	// crossed the network, a different number in both directions:
	// compression shrinks the body, and externalised payloads leave it
	// entirely. response_bytes is stamped by the recorder at flush time,
	// because compression has not run yet.
	if rec != nil {
		record["request_bytes"] = rec.requestBytes
		if externalized := rec.externalized.Load(); externalized > 0 {
			record["externalized_bytes"] = externalized
		}
	}
	if stats != nil && (stats.InputBatches+stats.OutputBatches+stats.InputRows+stats.OutputRows+stats.InputBytes+stats.OutputBytes) != 0 {
		record["input_batches"] = stats.InputBatches
		record["output_batches"] = stats.OutputBatches
		record["input_rows"] = stats.InputRows
		record["output_rows"] = stats.OutputRows
		record["input_bytes"] = stats.InputBytes
		record["output_bytes"] = stats.OutputBytes
	}

	if rec != nil {
		rec.queue(h, record)
		return
	}
	h.emit(record)
}

// emit applies the sampling decision and routes the record to the async
// writer or straight to the file.
func (h *AccessLogHook) emit(record map[string]any) {
	if sampler := h.sampler.Load(); sampler != nil && !sampler.keep(record) {
		return
	}
	if async := h.async.Load(); async != nil {
		async.enqueue(record)
		return
	}
	h.writeRecord(record)
}

// writeRecord serializes one record and appends it as a single line.
func (h *AccessLogHook) writeRecord(record map[string]any) {
	line, marshalErr := json.Marshal(record)
	if marshalErr != nil {
		// Best-effort: drop the record rather than panic in observability code.
		return
	}
	line = append(line, '\n')

	h.mu.Lock()
	defer h.mu.Unlock()
	_, _ = h.w.Write(line)
}

// SerializeRequestBatch produces a self-contained Arrow IPC stream
// (one schema message + one record batch message) suitable for the
// access-log “request_data“ field.
//
// The returned bytes round-trip through any Arrow library's IPC stream
// reader to a logically-equal RecordBatch, satisfying the access-log
// spec's round-trip-equivalence requirement.
func SerializeRequestBatch(batch arrow.RecordBatch) ([]byte, error) {
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(batch.Schema()))
	if err := writer.Write(batch); err != nil {
		writer.Close()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// roundTo2Decimals rounds f to two decimal places. Defined here so the
// access-log emitter does not pull in math just for one call site.
func roundTo2Decimals(f float64) float64 {
	if f >= 0 {
		return float64(int64(f*100+0.5)) / 100.0
	}
	return float64(int64(f*100-0.5)) / 100.0
}

// RandomStreamID returns 32 lowercase hex characters from crypto/rand.
// Use this to mint a stream_id at the start of a stream call; reuse
// the same value across the init and every continuation record to
// satisfy the spec's "stable across continuations" semantic.
func RandomStreamID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand.Read is documented as never returning an error in
		// practice; fall back to a deterministic non-empty value rather
		// than panic in observability code.
		return "00000000000000000000000000000000"
	}
	return hex.EncodeToString(b[:])
}

func authPrincipal(a *AuthContext) string {
	if a == nil {
		return ""
	}
	return a.Principal
}

func authDomain(a *AuthContext) string {
	if a == nil {
		return ""
	}
	return a.Domain
}

func authAuthenticated(a *AuthContext) bool {
	if a == nil {
		return false
	}
	return a.Authenticated
}

func authClaims(a *AuthContext) map[string]any {
	if a == nil {
		return nil
	}
	return a.Claims
}
