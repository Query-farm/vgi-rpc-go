// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// supportedEncodings lists the codecs the server can produce on responses,
// in preference order (used when emitting the VGI-Supported-Encodings
// capability header and for codec selection from Accept-Encoding).
//
// The wire is byte-identical to the Python codec set in vgi_rpc._codec.
var supportedEncodings = []string{"zstd", "gzip"}

// SetCompressionLevel enables compression of response bodies at the given
// level. The codec is chosen per request from Accept-Encoding intersected
// with the server's supported set: zstd (when offered) wins over gzip.
// Levels: zstd 1–11, gzip 1–9. Pass 0 to disable compression entirely
// (the default).
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

// parseAcceptEncoding returns the codec tokens from an Accept-Encoding
// header in the client's stated preference order. Tokens are lowercased,
// trimmed, and ;q=<weight> suffixes stripped (weight ordering is not
// honoured — clients that care about ordering should list preferred
// codecs first).
func parseAcceptEncoding(header string) []string {
	if header == "" {
		return nil
	}
	out := make([]string, 0, 4)
	seen := make(map[string]struct{}, 4)
	for _, raw := range strings.Split(header, ",") {
		tok := strings.TrimSpace(raw)
		if i := strings.IndexByte(tok, ';'); i >= 0 {
			tok = strings.TrimSpace(tok[:i])
		}
		tok = strings.ToLower(tok)
		if tok == "" {
			continue
		}
		if _, dup := seen[tok]; dup {
			continue
		}
		seen[tok] = struct{}{}
		out = append(out, tok)
	}
	return out
}

// chooseResponseEncoding picks a codec from the client's accept list
// intersected with the server's supported set. Returns "" when there's
// no overlap (caller writes identity). zstd is preferred over gzip when
// both are accepted; the supportedEncodings preference order wins.
func chooseResponseEncoding(accept string) string {
	tokens := parseAcceptEncoding(accept)
	if len(tokens) == 0 {
		return ""
	}
	clientSet := make(map[string]struct{}, len(tokens))
	for _, t := range tokens {
		clientSet[t] = struct{}{}
	}
	for _, enc := range supportedEncodings {
		if _, ok := clientSet[enc]; ok {
			return enc
		}
	}
	return ""
}

// when the response has an Arrow content type.
type compressResponseWriter struct {
	http.ResponseWriter
	encoderLevel int
	encoding     string // "zstd", "gzip", or "" (identity)
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
// client accepts compressed, the buffer is streamed directly into a
// per-request codec writer attached to the underlying ResponseWriter —
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
	canCompress := cw.encoding != "" &&
		cw.ResponseWriter.Header().Get("Content-Type") == arrowContentType &&
		cw.buf.Len() > 0
	if !canCompress {
		cw.ResponseWriter.WriteHeader(cw.statusCode)
		if _, err := cw.ResponseWriter.Write(cw.buf.Bytes()); err != nil {
			slog.Debug("http: response write failed", "err", err)
		}
		return
	}
	cw.ResponseWriter.Header().Set("Content-Encoding", cw.encoding)
	cw.ResponseWriter.WriteHeader(cw.statusCode)
	writer, err := newCompressWriter(cw.encoding, cw.ResponseWriter, cw.encoderLevel)
	if err != nil {
		slog.Debug("http: codec writer init failed", "encoding", cw.encoding, "err", err)
		return
	}
	if _, err := writer.Write(cw.buf.Bytes()); err != nil {
		slog.Debug("http: codec write failed", "encoding", cw.encoding, "err", err)
	}
	if err := writer.Close(); err != nil {
		slog.Debug("http: codec close failed", "encoding", cw.encoding, "err", err)
	}
}

// newCompressWriter constructs a streaming compressor for the chosen
// encoding. Caller writes the buffered body into the returned WriteCloser
// and must Close it to flush trailing bytes.
func newCompressWriter(encoding string, w io.Writer, zstdLevel int) (io.WriteCloser, error) {
	switch encoding {
	case "zstd":
		return zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.EncoderLevel(zstdLevel)))
	case "gzip":
		// gzip's level range is 1–9; clamp the zstd-shaped level into
		// gzip's domain rather than carrying a second config field.
		level := zstdLevel
		if level > gzip.BestCompression {
			level = gzip.BestCompression
		}
		if level < gzip.BestSpeed {
			level = gzip.DefaultCompression
		}
		return gzip.NewWriterLevel(w, level)
	default:
		return nil, fmt.Errorf("unsupported encoding: %q", encoding)
	}
}
