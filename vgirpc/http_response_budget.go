// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	"github.com/apache/arrow-go/v18/arrow"
)

const maxSafeDecimal = int64(1<<53 - 1)
const minResponseBudgetBytes = int64(64 << 10)

type httpResponseBudget struct {
	Limit     int64
	Preferred int64
}

type httpResponseBudgetKey struct{}

func withHTTPResponseBudget(ctx context.Context, budget httpResponseBudget) context.Context {
	if state, ok := ctx.Value(httpResponseBudgetKey{}).(*httpResponseBudget); ok {
		*state = budget
		return ctx
	}
	return context.WithValue(ctx, httpResponseBudgetKey{}, &budget)
}

func responseBudgetFromContext(ctx context.Context) httpResponseBudget {
	if budget, ok := ctx.Value(httpResponseBudgetKey{}).(*httpResponseBudget); ok {
		return *budget
	}
	return httpResponseBudget{}
}

func responseBudgetStateFromContext(ctx context.Context) *httpResponseBudget {
	budget, _ := ctx.Value(httpResponseBudgetKey{}).(*httpResponseBudget)
	return budget
}

func minPositive(values ...int64) int64 {
	var result int64
	for _, value := range values {
		if value > 0 && (result == 0 || value < result) {
			result = value
		}
	}
	return result
}

func parsePositiveSafeDecimal(value string) (int64, error) {
	if value == "" || value[0] < '1' || value[0] > '9' {
		return 0, fmt.Errorf("must be a positive decimal integer")
	}
	var result int64
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("must be a positive decimal integer")
		}
		digit := int64(c - '0')
		if result > (maxSafeDecimal-digit)/10 {
			return 0, fmt.Errorf("must not exceed %d", maxSafeDecimal)
		}
		result = result*10 + digit
	}
	return result, nil
}

func parseResponseBudgetDecimal(value string) (int64, error) {
	result, err := parsePositiveSafeDecimal(value)
	if err != nil {
		return 0, err
	}
	if result < minResponseBudgetBytes {
		return 0, fmt.Errorf("must be at least %d", minResponseBudgetBytes)
	}
	return result, nil
}

// applyResponseBudget parses the client limit only after the caller has been
// authenticated. Every RPC handler calls this before inspecting content type
// or reading the body; OPTIONS /health remains the auth-exempt discovery path.
func (h *HttpServer) applyResponseBudget(w http.ResponseWriter, r *http.Request, schema *arrow.Schema) (*http.Request, bool) {
	budget, err := h.responseBudget(r.Header)
	if err != nil {
		h.writeHttpError(w, http.StatusBadRequest, &RpcError{Type: "ValueError", Message: err.Error()}, schema)
		return r, false
	}
	return r.WithContext(withHTTPResponseBudget(r.Context(), budget)), true
}

func (h *HttpServer) effectiveRequestLimit() int64 {
	return minPositive(h.maxRequestBytes, h.hostingMaxRequestBytes)
}

func (h *HttpServer) responseBudget(headers http.Header) (httpResponseBudget, error) {
	var accepted int64
	values, present := headers[http.CanonicalHeaderKey(acceptMaxResponseBytesHeader)]
	if present {
		if len(values) != 1 {
			return httpResponseBudget{}, fmt.Errorf("%s must occur exactly once", acceptMaxResponseBytesHeader)
		}
		var err error
		accepted, err = parseResponseBudgetDecimal(values[0])
		if err != nil {
			return httpResponseBudget{}, fmt.Errorf("invalid %s: %w", acceptMaxResponseBytesHeader, err)
		}
	}
	limit := minPositive(h.maxResponseBytes, h.hostingMaxResponseBytes, accepted)
	preferred := h.preferredResponseBytes
	if preferred > 0 && limit > 0 && preferred > limit {
		preferred = limit
	}
	return httpResponseBudget{Limit: limit, Preferred: preferred}, nil
}

// responseCapWriter is the final safety net for every RPC response shape,
// including describe, void/error envelopes, stream headers, and producer
// turns. It buffers status/body until dispatch completes. An oversized body
// is discarded in full, so no continuation cursor can escape.
type responseCapWriter struct {
	http.ResponseWriter
	server      *HttpServer
	method      string
	budget      *httpResponseBudget
	status      int
	body        bytes.Buffer
	bodyBytes   int64
	wroteHeader bool
}

func newResponseCapWriter(w http.ResponseWriter, budget *httpResponseBudget, server *HttpServer, method string) *responseCapWriter {
	return &responseCapWriter{ResponseWriter: w, server: server, method: method, budget: budget}
}

func (w *responseCapWriter) WriteHeader(status int) {
	if !w.wroteHeader {
		w.status = status
		w.wroteHeader = true
	}
}

func (w *responseCapWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	w.bodyBytes += int64(len(p))
	limit := int64(0)
	if w.budget != nil {
		limit = w.budget.Limit
	}
	if limit <= 0 {
		return w.body.Write(p)
	}
	// Retain only enough to distinguish an exact-limit response from an
	// overshoot. The handler still observes a successful full write, while the
	// structured replacement reports bodyBytes and no cursor can escape.
	remaining := limit + 1 - int64(w.body.Len())
	if remaining > 0 {
		keep := int64(len(p))
		if keep > remaining {
			keep = remaining
		}
		_, _ = w.body.Write(p[:int(keep)])
	}
	return len(p), nil
}

func (w *responseCapWriter) finish() {
	status := w.status
	if status == 0 {
		status = http.StatusOK
	}
	limit := int64(0)
	if w.budget != nil {
		limit = w.budget.Limit
	}
	if limit > 0 && w.bodyBytes > limit {
		var body bytes.Buffer
		err := newResponseCapError(w.method, w.bodyBytes, limit)
		w.server.logIPCWriteErr("response-cap-error", w.method,
			writeErrorResponse(&body, arrow.NewSchema(nil, nil), err,
				w.server.server.serverID, "", w.server.server.debugErrors))
		w.Header().Set(rpcErrorHeader, "true")
		w.Header().Del("Content-Encoding")
		w.Header().Del(customContentEncodingHeader)
		w.Header().Set("Content-Type", arrowContentType)
		w.Header().Set("Content-Length", fmt.Sprintf("%d", body.Len()))
		w.ResponseWriter.WriteHeader(http.StatusOK)
		_, _ = w.ResponseWriter.Write(body.Bytes())
		return
	}
	w.ResponseWriter.WriteHeader(status)
	if w.body.Len() > 0 {
		_, _ = w.ResponseWriter.Write(w.body.Bytes())
	}
}
