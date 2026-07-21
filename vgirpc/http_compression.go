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

// supportedEncodings lists the compressed codecs this build speaks, in the
// server's own preference order. Both are compiled in unconditionally
// (klauspost zstd, stdlib gzip) and both work in both directions — decode on
// requests, produce on responses — so this is the full two-way codec set
// before the configuration gate is applied (see producibleResponseEncodings).
// It is the membership test for negotiation, but not the pick order: the
// response codec is chosen in the *client's* order (see chooseResponseEncoding).
//
// The wire is byte-identical to the Python codec set in vgi_rpc._codec.
var supportedEncodings = []string{"zstd", "gzip"}

// Codec negotiation header names. The X-VGI-* pair is VGI's private
// channel for clients whose HTTP stack owns the standard headers — see
// chooseResponseEncoding.
const (
	acceptEncodingHeader        = "Accept-Encoding"
	customAcceptEncodingHeader  = "X-VGI-Accept-Encoding"
	contentEncodingHeader       = "Content-Encoding"
	customContentEncodingHeader = "X-VGI-Content-Encoding"
)

// identityEncoding is the "send it uncompressed" token. Every server can
// always produce it, so a client that lists it ahead of the compressed
// codecs is explicitly opting out of response compression for that request
// (benchmarking, or a proxy that must see the raw body). It is never
// stamped on a response — an identity body is just a body — and it is
// deliberately absent from supportedEncodings, which advertises codecs.
const identityEncoding = "identity"

// DefaultCompressionLevel is the zstd level a freshly constructed
// [HttpServer] starts at. Response compression is ON by default: an Arrow
// response body is large and highly compressible, and the client side of
// this protocol always decompresses, so the only question is which level.
//
// Level 1, not the usual level-3 default, and that is not a size/speed
// tradeoff. Measured on an 8.41 MB Arrow payload, level 1 was 4.7× faster
// than level 3 *and* produced the smaller body — it wins on both axes, so
// there is nothing to trade. Raise it only with a measurement on your own
// payloads; pass 0 to [HttpServer.SetCompressionLevel] to turn compression
// off entirely.
const DefaultCompressionLevel = 1

// applyCompressionLevel stores a validated level and re-renders the
// advertisement derived from it. Every write to zstdEncoderLevel goes
// through here so the two fields cannot drift apart: the capability header
// is a pure function of the level, and rendering it once per configuration
// change keeps it off the per-response path.
func (h *HttpServer) applyCompressionLevel(level int) {
	if level < 0 {
		level = 0
	}
	h.zstdEncoderLevel = level
	h.supportedEncodingsValue = strings.Join(h.producibleResponseEncodings(), ", ")
}

// producibleResponseEncodings returns the codecs this server can actually
// produce right now, in the server's own preference order: runtime-available
// (both encoders are compiled in) and enabled by configuration. nil when
// compression is disabled.
//
// This is the single producibility predicate. Both the negotiation walk and
// the VGI-Supported-Encodings capability advertisement derive from it, so the
// header cannot claim a codec the server would decline to use. Because every
// codec here decodes as well as it encodes, this set is also the two-way
// intersection the capability header is specified to carry.
//
// Note the config gate is one field for both codecs: zstdEncoderLevel is
// zstd-named but governs gzip too (gzip reuses the level via gzipLevelFor),
// so the set is all-or-nothing today.
func (h *HttpServer) producibleResponseEncodings() []string {
	if h.zstdEncoderLevel <= 0 {
		return nil
	}
	return supportedEncodings
}

// SetCompressionLevel overrides the compression level for response bodies.
// The codec is chosen per request from the client's stated preference order
// across X-VGI-Accept-Encoding and Accept-Encoding, intersected with the
// server's supported set (see chooseResponseEncoding). Levels: zstd 1–11,
// gzip 1–9 (gzip reuses this level, clamped by gzipLevelFor).
//
// Calling this at all is optional: a server starts at
// [DefaultCompressionLevel] with compression already on. Pass 0 to turn
// compression off entirely — which also empties the advertised codec set,
// since the level gates both codecs.
func (h *HttpServer) SetCompressionLevel(level int) error {
	if level <= 0 {
		h.applyCompressionLevel(0)
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
	h.applyCompressionLevel(level)
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

// parseAcceptEncoding returns the codec tokens from an Accept-Encoding-style
// header — either Accept-Encoding or X-VGI-Accept-Encoding — in the client's
// stated preference order. Tokens are lowercased, trimmed, de-duplicated
// (first occurrence wins), and ;q=<weight> suffixes stripped. Weights are
// parsed off and ignored, never honoured as ordering: clients state their
// preference by listing preferred codecs first. Tokens this server does not
// know are kept here and filtered by the caller's producibility check.
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

// chooseResponseEncoding picks the response codec from the two accept
// headers a VGI client may send: X-VGI-Accept-Encoding (custom) and the
// standard Accept-Encoding. It returns the chosen codec ("" for identity)
// and whether the choice came only from the custom header.
//
// The client is authoritative: the merged list is walked in the client's
// stated order — the whole custom header first, then anything the standard
// header adds — and the first codec this server can produce wins. The
// server's own supportedEncodings order is not consulted for the pick.
//
// Two reasons the custom header leads. First, an HTTP stack often injects
// its own Accept-Encoding over the caller's head: cpp-httplib (the DuckDB
// engine's client) sends "deflate, gzip, br, zstd", listing gzip before the
// zstd that VGI actually wants, and gzip dominates large Arrow bodies.
// Second — and why ignoring the custom header was a latent bug here rather
// than merely a preference question — browser fetch() cannot set
// Accept-Encoding at all: it is a forbidden header name. A WASM/browser
// client can *only* state its codec preference through X-VGI-Accept-Encoding,
// so a server that reads just the standard header ships every browser
// response uncompressed.
//
// used_custom (the second return) is true only when the winner was offered
// on the custom header and not on the standard one. That client's fetch or
// proxy layer would auto-decode (or mangle) a standard Content-Encoding it
// never asked for, so the caller must stamp X-VGI-Content-Encoding instead.
//
// "identity" is a codec like any other in the walk, except that every server
// can produce it: reaching it before a producible compressed codec ends the
// search with "" — send the body as-is, stamping no encoding header at all.
// That is how a client explicitly opts out of response compression.
//
// producible is the server's codec set from producibleResponseEncodings;
// an empty set can still only yield "" (identity), never a codec.
func chooseResponseEncoding(custom, standard string, producible []string) (string, bool) {
	customTokens := parseAcceptEncoding(custom)
	standardTokens := parseAcceptEncoding(standard)
	if len(customTokens) == 0 && len(standardTokens) == 0 {
		return "", false
	}

	inCustom := make(map[string]struct{}, len(customTokens))
	for _, t := range customTokens {
		inCustom[t] = struct{}{}
	}
	inStandard := make(map[string]struct{}, len(standardTokens))
	for _, t := range standardTokens {
		inStandard[t] = struct{}{}
	}

	merged := make([]string, 0, len(customTokens)+len(standardTokens))
	merged = append(merged, customTokens...)
	for _, t := range standardTokens {
		if _, dup := inCustom[t]; !dup {
			merged = append(merged, t)
		}
	}

	for _, enc := range merged {
		if enc == identityEncoding {
			// Explicitly requested: stop here rather than falling through
			// to a compressed codec listed after it.
			return "", false
		}
		if !containsEncoding(producible, enc) {
			continue
		}
		_, viaCustom := inCustom[enc]
		_, viaStandard := inStandard[enc]
		return enc, viaCustom && !viaStandard
	}
	return "", false
}

// containsEncoding reports membership in an ordered codec list.
func containsEncoding(list []string, enc string) bool {
	for _, s := range list {
		if s == enc {
			return true
		}
	}
	return false
}

// compressResponseWriter buffers a handler's response and, in finish,
// compresses it with the negotiated codec — but only when the response has
// an Arrow content type. HTML pages and error bodies pass through
// uncompressed regardless of what was negotiated.
type compressResponseWriter struct {
	http.ResponseWriter
	encoderLevel int
	encoding     string // "zstd", "gzip", or "" (identity)
	// useCustomHeader stamps X-VGI-Content-Encoding instead of
	// Content-Encoding, for a client that could only state its codec
	// preference on X-VGI-Accept-Encoding (see chooseResponseEncoding).
	useCustomHeader bool
	buf             bytes.Buffer
	statusCode      int
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
	encodingHeader := contentEncodingHeader
	if cw.useCustomHeader {
		encodingHeader = customContentEncodingHeader
	}
	cw.ResponseWriter.Header().Set(encodingHeader, cw.encoding)
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
