// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/klauspost/compress/zstd"
)

func buildHTTPTransportMeta(ipcMeta map[string]string, r *http.Request) map[string]string {
	meta := make(map[string]string, len(ipcMeta)+4)
	for k, v := range ipcMeta {
		meta[k] = v
	}
	if tp := r.Header.Get("Traceparent"); tp != "" {
		meta[MetaTraceparent] = tp
	}
	if ts := r.Header.Get("Tracestate"); ts != "" {
		meta[MetaTracestate] = ts
	}
	meta["remote_addr"] = r.RemoteAddr
	meta["user_agent"] = r.UserAgent()
	return meta
}

// buildHTTPCookies extracts the incoming request cookies into a map.
// Returns nil if the request has no cookies.
func buildHTTPCookies(r *http.Request) map[string]string {
	c := r.Cookies()
	if len(c) == 0 {
		return nil
	}
	out := make(map[string]string, len(c))
	for _, ck := range c {
		out[ck.Name] = ck.Value
	}
	return out
}

// applyResponseCookies writes queued CookieSpec entries as Set-Cookie
// headers on the response.  Must be called before w.WriteHeader.
func applyResponseCookies(w http.ResponseWriter, cookies []CookieSpec) {
	for _, c := range cookies {
		hc := &http.Cookie{
			Name:        c.Name,
			Path:        c.Path,
			Domain:      c.Domain,
			Secure:      c.Secure,
			HttpOnly:    c.HttpOnly,
			SameSite:    c.SameSite,
			Partitioned: c.Partitioned,
		}
		if c.Delete {
			hc.Value = ""
			hc.MaxAge = -1
		} else {
			hc.Value = c.Value
			hc.MaxAge = c.MaxAge
			if !c.Expires.IsZero() {
				hc.Expires = c.Expires
			}
		}
		http.SetCookie(w, hc)
	}
}

// handleDescribe handles the __describe__ introspection endpoint.
func (h *HttpServer) handleDescribe(w http.ResponseWriter, r *http.Request) {
	body, err := h.readHTTPBody(r)
	if err != nil {
		h.writeHttpError(w, http.StatusBadRequest, err, nil)
		return
	}

	req, err := ReadRequest(bytes.NewReader(body))
	if err != nil {
		h.writeHttpError(w, http.StatusBadRequest, err, nil)
		return
	}
	defer req.Batch.Release()

	batch, meta := h.server.buildDescribeBatch()
	defer batch.Release()

	batchWithMeta := array.NewRecordBatchWithMetadata(
		describeSchema, batch.Columns(), batch.NumRows(), meta)
	defer batchWithMeta.Release()

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(describeSchema))
	h.logIPCWriteErr("describe-batch", "describe", writer.Write(batchWithMeta))
	h.logIPCWriteErr("close", "describe", writer.Close())

	h.writeArrow(w, http.StatusOK, buf.Bytes())
}

// --- Helpers ---

// readHTTPBody reads the request body, decompressing it if the Content-Encoding
// header indicates zstd compression. Both the raw and decompressed body are
// limited to h.maxBodySize bytes (0 = unlimited).
func (h *HttpServer) readHTTPBody(r *http.Request) ([]byte, error) {
	limit := h.maxBodySize

	var body []byte
	var err error
	if limit > 0 {
		body, err = io.ReadAll(io.LimitReader(r.Body, limit+1))
	} else {
		body, err = io.ReadAll(r.Body)
	}
	if err != nil {
		return nil, err
	}
	if limit > 0 && int64(len(body)) > limit {
		return nil, &RpcError{Type: "ValueError", Message: fmt.Sprintf("Request body exceeds maximum size of %d bytes", limit)}
	}

	if r.Header.Get("Content-Encoding") == "zstd" {
		reader, err := zstd.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("zstd decompression init: %w", err)
		}
		defer reader.Close()

		if limit > 0 {
			body, err = io.ReadAll(io.LimitReader(reader, limit+1))
		} else {
			body, err = io.ReadAll(reader)
		}
		if err != nil {
			return nil, fmt.Errorf("zstd decompression: %w", err)
		}
		if limit > 0 && int64(len(body)) > limit {
			return nil, &RpcError{Type: "ValueError", Message: fmt.Sprintf("Decompressed body exceeds maximum size of %d bytes", limit)}
		}
	}
	return body, nil
}

func (h *HttpServer) writeHttpError(w http.ResponseWriter, statusCode int, err error, schema *arrow.Schema) {
	if schema == nil {
		schema = arrow.NewSchema(nil, nil)
	}
	var buf bytes.Buffer
	h.logIPCWriteErr("error-response", "", writeErrorResponse(&buf, schema, err, h.server.serverID, "", h.server.debugErrors))
	h.writeArrow(w, statusCode, buf.Bytes())
}

func (h *HttpServer) writeArrow(w http.ResponseWriter, statusCode int, data []byte) {
	w.Header().Set("Content-Type", arrowContentType)
	if statusCode == http.StatusInternalServerError {
		w.Header().Set(rpcErrorHeader, "true")
		statusCode = http.StatusOK
	}
	w.WriteHeader(statusCode)
	if _, err := w.Write(data); err != nil {
		// Client disconnect mid-response is expected — log at debug level.
		slog.Debug("http: response write failed", "err", err)
	}
}

// logIPCWriteErr reports a non-nil error encountered while serializing an
// Arrow IPC stream into a response buffer. These writes go to a bytes.Buffer
// (which cannot itself fail), so any error indicates a serialization bug
// such as a schema/batch mismatch — silent failures here produce a truncated
// response that the client will not understand. Logs at error level.
func (h *HttpServer) logIPCWriteErr(op, method string, err error) {
	if err == nil {
		return
	}
	slog.Error("ipc write failed", "op", op, "method", method, "err", err)
}
