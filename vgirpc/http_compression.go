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
		h.zstdEncoder = nil
		return nil
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevel(level)))
	if err != nil {
		return fmt.Errorf("vgirpc: failed to create zstd encoder: %w", err)
	}
	h.zstdEncoder = enc
	return nil
}

// SetRehydrateFunc sets a callback that reconstructs non-serializable fields
// on deserialized stream state. Called after unpacking a state token in

// when the response has an Arrow content type.
type compressResponseWriter struct {
	http.ResponseWriter
	encoder    *zstd.Encoder
	buf        bytes.Buffer
	statusCode int
}

func (cw *compressResponseWriter) WriteHeader(code int) {
	cw.statusCode = code
}

func (cw *compressResponseWriter) Write(data []byte) (int, error) {
	return cw.buf.Write(data)
}

func (cw *compressResponseWriter) finish() {
	if cw.statusCode == 0 {
		cw.statusCode = http.StatusOK
	}
	data := cw.buf.Bytes()
	if cw.ResponseWriter.Header().Get("Content-Type") == arrowContentType && len(data) > 0 {
		compressed := cw.encoder.EncodeAll(data, make([]byte, 0, len(data)))
		cw.ResponseWriter.Header().Set("Content-Encoding", "zstd")
		cw.ResponseWriter.WriteHeader(cw.statusCode)
		if _, err := cw.ResponseWriter.Write(compressed); err != nil {
			slog.Debug("http: response write failed", "err", err)
		}
	} else {
		cw.ResponseWriter.WriteHeader(cw.statusCode)
		if _, err := cw.ResponseWriter.Write(data); err != nil {
			slog.Debug("http: response write failed", "err", err)
		}
	}
}
