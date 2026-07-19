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
	"sync"

	"github.com/klauspost/compress/zstd"
)

// supportedEncodings lists the codecs the server can produce on responses,
// in preference order (used when emitting the VGI-Supported-Encodings
// capability header and for codec selection from Accept-Encoding).
//
// The wire is byte-identical to the Python codec set in vgi_rpc._codec.
var supportedEncodings = []string{"zstd", "gzip"}

// supportedEncodingsHeaderValue is the rendered VGI-Supported-Encodings
// value. addCapabilityHeaders runs on every response and supportedEncodings
// never changes, so join it once.
var supportedEncodingsHeaderValue = strings.Join(supportedEncodings, ", ")

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
	// Validate the level early by attempting a probe encoder. The probe is
	// discarded; request-time encoders come from a per-(codec, level) pool
	// (see newCompressWriter). klauspost's Encoder is not goroutine-safe,
	// so each request checks one out for the duration of its write and
	// returns it on Close — never sharing one across goroutines.
	probe, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevel(level)))
	if err != nil {
		return fmt.Errorf("vgirpc: failed to create zstd encoder: %w", err)
	}
	_ = probe.Close()
	h.zstdEncoderLevel = level
	return nil
}

// decompressBounded decompresses data with the named coding ("zstd" or
// "gzip"), enforcing maxOutput as a decompressed-size cap when > 0. The
// gzip ISIZE footer carries the size mod 2^32 — never trust it for a bomb
// cap — so both paths use a bounded streaming read, mirroring the Python
// codec.
func decompressBounded(encoding string, data []byte, maxOutput int64) ([]byte, error) {
	var reader io.Reader
	switch encoding {
	case "zstd":
		opts := []zstd.DOption{}
		if maxOutput > 0 {
			opts = append(opts, zstd.WithDecoderMaxMemory(uint64(maxOutput)))
		}
		zr, err := zstd.NewReader(bytes.NewReader(data), opts...)
		if err != nil {
			return nil, fmt.Errorf("zstd decompression init: %w", err)
		}
		defer zr.Close()
		reader = zr
	case "gzip":
		gr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("gzip decompression init: %w", err)
		}
		defer gr.Close()
		reader = gr
	default:
		return nil, &unsupportedEncodingError{Encoding: encoding}
	}

	var out []byte
	var err error
	if maxOutput > 0 {
		out, err = io.ReadAll(io.LimitReader(reader, maxOutput+1))
	} else {
		out, err = io.ReadAll(reader)
	}
	if err != nil {
		return nil, fmt.Errorf("%s decompression: %w", encoding, err)
	}
	if maxOutput > 0 && int64(len(out)) > maxOutput {
		return nil, &RpcError{Type: "ValueError", Message: fmt.Sprintf("Decompressed body exceeds maximum size of %d bytes", maxOutput)}
	}
	return out, nil
}

// DecodeContentEncoding decodes an HTTP body per its Content-Encoding
// header value, or returns it unchanged when nothing applies.
//
// Handles the codings vgi-rpc speaks (zstd, gzip); the header may list
// several applied in order, which are decoded in reverse. Unknown and
// identity codings are left as-is. Intended for an intermediary
// (proxy/gateway) that must read a compressed request/response body to
// inspect or rewrite it. maxOutputSize caps the decompressed size per
// coding when > 0.
func DecodeContentEncoding(data []byte, contentEncoding string, maxOutputSize int64) ([]byte, error) {
	if contentEncoding == "" {
		return data, nil
	}
	result := data
	codings := strings.Split(contentEncoding, ",")
	for i := len(codings) - 1; i >= 0; i-- {
		name := strings.ToLower(strings.TrimSpace(codings[i]))
		switch name {
		case "zstd", "gzip":
			decoded, err := decompressBounded(name, result, maxOutputSize)
			if err != nil {
				return nil, err
			}
			result = decoded
		default:
			// identity / unknown coding — leave as-is.
		}
	}
	return result, nil
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

// Codec writers are pooled per (codec, level). Constructing a zstd encoder
// allocates level-sized window and hash tables and — at the default
// concurrency — spawns GOMAXPROCS worker goroutines; a gzip writer allocates
// a full deflate state. Paying that on every response is the single largest
// per-request allocation when compression is enabled.
//
// klauspost's Encoder is not goroutine-safe, which is exactly what sync.Pool
// plus Reset(w) is for: each request checks out an encoder for the duration
// of the write and returns it on Close. Response bodies are fully buffered
// before compression, so encoder concurrency is pinned to 1 — extra worker
// goroutines buy nothing here and make each pooled encoder far more
// expensive to hold.
var codecWriterPools sync.Map // codecPoolKey -> *sync.Pool

type codecPoolKey struct {
	encoding string
	level    int
}

func codecPool(key codecPoolKey) *sync.Pool {
	if p, ok := codecWriterPools.Load(key); ok {
		return p.(*sync.Pool)
	}
	p := &sync.Pool{New: func() any {
		switch key.encoding {
		case "zstd":
			enc, err := zstd.NewWriter(nil,
				zstd.WithEncoderLevel(zstd.EncoderLevel(key.level)),
				zstd.WithEncoderConcurrency(1))
			if err != nil {
				return err
			}
			return enc
		default: // gzip
			w, err := gzip.NewWriterLevel(nil, key.level)
			if err != nil {
				return err
			}
			return w
		}
	}}
	actual, _ := codecWriterPools.LoadOrStore(key, p)
	return actual.(*sync.Pool)
}

// pooledCodecWriter returns its codec writer to the pool on Close.
type pooledCodecWriter struct {
	io.WriteCloser
	pool *sync.Pool
	// resetNil returns the writer to a state that holds no reference to the
	// request's ResponseWriter, so a pooled entry cannot pin a finished
	// request's memory.
	resetNil func()
}

func (p *pooledCodecWriter) Close() error {
	err := p.WriteCloser.Close()
	p.resetNil()
	p.pool.Put(p.WriteCloser)
	return err
}

// gzipLevelFor clamps the zstd-shaped level into gzip's 1–9 domain rather
// than carrying a second config field.
func gzipLevelFor(zstdLevel int) int {
	if zstdLevel > gzip.BestCompression {
		return gzip.BestCompression
	}
	if zstdLevel < gzip.BestSpeed {
		return gzip.DefaultCompression
	}
	return zstdLevel
}

// newCompressWriter checks out a streaming compressor for the chosen
// encoding. Caller writes the buffered body into the returned WriteCloser
// and must Close it to flush trailing bytes and release it back to the pool.
func newCompressWriter(encoding string, w io.Writer, zstdLevel int) (io.WriteCloser, error) {
	var key codecPoolKey
	switch encoding {
	case "zstd":
		key = codecPoolKey{encoding: "zstd", level: zstdLevel}
	case "gzip":
		key = codecPoolKey{encoding: "gzip", level: gzipLevelFor(zstdLevel)}
	default:
		return nil, fmt.Errorf("unsupported encoding: %q", encoding)
	}

	pool := codecPool(key)
	got := pool.Get()
	if err, isErr := got.(error); isErr {
		return nil, err
	}

	switch enc := got.(type) {
	case *zstd.Encoder:
		enc.Reset(w)
		return &pooledCodecWriter{WriteCloser: enc, pool: pool, resetNil: func() { enc.Reset(nil) }}, nil
	case *gzip.Writer:
		enc.Reset(w)
		return &pooledCodecWriter{WriteCloser: enc, pool: pool, resetNil: func() { enc.Reset(io.Discard) }}, nil
	default:
		return nil, fmt.Errorf("unsupported encoding: %q", encoding)
	}
}
