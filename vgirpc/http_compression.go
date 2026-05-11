// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/klauspost/compress/zstd"
)

// SetCompressionLevel enables zstd compression of response bodies at the
// given level (1–11). When enabled, responses are compressed if the client
// sends an Accept-Encoding header containing "zstd". Pass 0 to disable
// response compression (the default).
func (h *HttpServer) SetCompressionLevel(level int) error {
	if level <= 0 {
		h.zstdEncoderLevel = 0
		return nil
	}
	// Validate the level early by attempting a probe encoder. The probe
	// is discarded; per-request encoders are constructed on demand so
	// each goroutine gets a fresh writer (klauspost's Encoder is not
	// goroutine-safe and reuse-via-Reset across goroutines is awkward).
	probe, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevel(level)))
	if err != nil {
		return fmt.Errorf("vgirpc: failed to create zstd encoder: %w", err)
	}
	_ = probe.Close()
	h.zstdEncoderLevel = level
	return nil
}

// SetRehydrateFunc sets a callback that reconstructs non-serializable fields
// on deserialized stream state. Called after unpacking a state token in

// when the response has an Arrow content type.
type compressResponseWriter struct {
	http.ResponseWriter
	encoderLevel int
	buf          bytes.Buffer
	statusCode   int
}

func (cw *compressResponseWriter) WriteHeader(code int) {
	cw.statusCode = code
}

func (cw *compressResponseWriter) Write(data []byte) (int, error) {
	return cw.buf.Write(data)
}

// finish flushes the buffered response. For Arrow IPC bodies that the
// client accepts zstd-compressed, the buffer is streamed directly into a
// per-request zstd writer attached to the underlying ResponseWriter —
// avoiding the extra `len(compressed)` allocation that EncodeAll would
// pay for a "buffer → compressed buffer → write" pipeline.
//
// Mirrors the streaming compression refactor on the Python side
// (vgi-rpc 4cfbcbe), which dropped per-thread peak from ~2× body size
// to ~1× body size by removing the intermediate compressed bytes copy.
func (cw *compressResponseWriter) finish() {
	if cw.statusCode == 0 {
		cw.statusCode = http.StatusOK
	}
	if cw.ResponseWriter.Header().Get("Content-Type") == arrowContentType && cw.buf.Len() > 0 {
		cw.ResponseWriter.Header().Set("Content-Encoding", "zstd")
		cw.ResponseWriter.WriteHeader(cw.statusCode)
		writer, err := zstd.NewWriter(cw.ResponseWriter, zstd.WithEncoderLevel(zstd.EncoderLevel(cw.encoderLevel)))
		if err != nil {
			slog.Debug("http: zstd writer init failed", "err", err)
			return
		}
		if _, err := writer.Write(cw.buf.Bytes()); err != nil {
			slog.Debug("http: zstd write failed", "err", err)
		}
		if err := writer.Close(); err != nil {
			slog.Debug("http: zstd close failed", "err", err)
		}
	} else {
		cw.ResponseWriter.WriteHeader(cw.statusCode)
		if _, err := cw.ResponseWriter.Write(cw.buf.Bytes()); err != nil {
			slog.Debug("http: response write failed", "err", err)
		}
	}
}
