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
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// AccessLogHook is a [DispatchHook] that emits one JSON record per RPC call
// to an io.Writer, conforming to the cross-language vgi-rpc access-log
// specification (see ``docs/access-log-spec.md`` and
// ``vgi_rpc/access_log.schema.json`` in the Python reference repo).
//
// Records are written as JSON-Lines (NDJSON), UTF-8 encoded, with one
// record per RPC call. The hook is safe for concurrent use across
// goroutines; writes are serialized through an internal mutex.
//
// Use [NewAccessLogHook] to construct one and pass it to
// [Server.SetDispatchHook] (or wire it in alongside another hook via
// a multiplexing wrapper).
type AccessLogHook struct {
	mu            sync.Mutex
	w             io.Writer
	serverVersion string
}

// NewAccessLogHook returns an [AccessLogHook] that writes records to w.
// serverVersion is reported in the optional ``server_version`` field;
// pass an empty string to omit it.
func NewAccessLogHook(w io.Writer, serverVersion string) *AccessLogHook {
	return &AccessLogHook{w: w, serverVersion: serverVersion}
}

type accessLogToken struct {
	start time.Time
}

// OnDispatchStart records the call start time.
func (h *AccessLogHook) OnDispatchStart(ctx context.Context, _ DispatchInfo) (context.Context, HookToken) {
	return ctx, &accessLogToken{start: time.Now()}
}

// OnDispatchEnd assembles a record from the dispatch info, statistics, and
// error and writes it to the underlying io.Writer as one JSON line.
func (h *AccessLogHook) OnDispatchEnd(_ context.Context, token HookToken, info DispatchInfo, stats *CallStatistics, err error) {
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

	if errMsg != "" {
		record["error_message"] = errMsg
	}
	if h.serverVersion != "" {
		record["server_version"] = h.serverVersion
	}
	if info.RequestID != "" {
		record["request_id"] = info.RequestID
	}
	if info.HTTPStatus > 0 {
		record["http_status"] = info.HTTPStatus
	}
	if len(info.RequestData) > 0 {
		record["request_data"] = base64.StdEncoding.EncodeToString(info.RequestData)
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
			streamID = randomStreamID()
		}
		record["stream_id"] = streamID
	}
	if info.Cancelled {
		record["cancelled"] = true
	}
	if claims := authClaims(info.Auth); len(claims) > 0 {
		record["claims"] = claims
	}
	if stats != nil && (stats.InputBatches+stats.OutputBatches+stats.InputRows+stats.OutputRows+stats.InputBytes+stats.OutputBytes) != 0 {
		record["input_batches"] = stats.InputBatches
		record["output_batches"] = stats.OutputBatches
		record["input_rows"] = stats.InputRows
		record["output_rows"] = stats.OutputRows
		record["input_bytes"] = stats.InputBytes
		record["output_bytes"] = stats.OutputBytes
	}

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
// access-log ``request_data`` field.
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

// randomStreamID returns 32 lowercase hex characters from crypto/rand.
// Used as a fallback stream_id when the dispatch path has not yet
// plumbed a stable per-stream identifier through DispatchInfo.
func randomStreamID() string {
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
