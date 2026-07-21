// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Tests for the public DecodeContentEncoding helper (for intermediaries),
// translated from the Python reference's TestDecodeContentEncoding, plus
// the response-codec negotiation (chooseResponseEncoding) that mirrors
// Python's _CompressionMiddleware._pick_response_encoding.

package vgirpc

import (
	"bytes"
	"compress/gzip"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func zstdCompress(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func gzipCompress(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestDecodeContentEncodingZstd(t *testing.T) {
	got, err := DecodeContentEncoding(zstdCompress(t, []byte("hello")), "zstd", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("expected hello, got %q", got)
	}
}

func TestDecodeContentEncodingGzip(t *testing.T) {
	got, err := DecodeContentEncoding(gzipCompress(t, []byte("hello")), "gzip", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("expected hello, got %q", got)
	}
}

func TestDecodeContentEncodingPassthrough(t *testing.T) {
	for _, header := range []string{"", "identity", "br"} {
		got, err := DecodeContentEncoding([]byte("plain"), header, 0)
		if err != nil {
			t.Fatalf("header %q: %v", header, err)
		}
		if !bytes.Equal(got, []byte("plain")) {
			t.Fatalf("header %q: expected plain, got %q", header, got)
		}
	}
}

func TestDecodeContentEncodingMultipleCodingsReversed(t *testing.T) {
	// Codings are applied in header order, so they decode in reverse.
	body := zstdCompress(t, gzipCompress(t, []byte("hello")))
	got, err := DecodeContentEncoding(body, "gzip, zstd", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("expected hello, got %q", got)
	}
}

func TestDecodeContentEncodingOutputCap(t *testing.T) {
	body := zstdCompress(t, bytes.Repeat([]byte("a"), 1024))
	if _, err := DecodeContentEncoding(body, "zstd", 16); err == nil {
		t.Fatal("expected an error when the decompressed size exceeds the cap")
	}
}

// TestChooseResponseEncoding covers the negotiation contract: the client's
// stated order wins, X-VGI-Accept-Encoding leads, "identity" is an explicit
// opt-out, and a winner offered only on the custom header reports usedCustom
// so the caller stamps X-VGI-Content-Encoding.
func TestChooseResponseEncoding(t *testing.T) {
	tests := []struct {
		name     string
		custom   string // X-VGI-Accept-Encoding
		standard string // Accept-Encoding
		// producible defaults to supportedEncodings; set explicitly to
		// model a server whose configuration narrows the set.
		producible     []string
		producibleSet  bool
		wantEncoding   string
		wantUsedCustom bool
	}{
		{
			// The browser/WASM case: fetch() cannot set Accept-Encoding
			// (forbidden header name), so the custom header is all we get.
			name:           "custom header alone",
			custom:         "zstd, gzip",
			wantEncoding:   "zstd",
			wantUsedCustom: true,
		},
		{
			// The cpp-httplib regression case: the client stack injects its
			// own gzip-first Accept-Encoding, but VGI states zstd first.
			// zstd is in both lists, so the standard response header is used.
			name:         "custom outranks injected standard order",
			custom:       "zstd, gzip",
			standard:     "deflate, gzip, br, zstd",
			wantEncoding: "zstd",
		},
		{
			name:         "standard header alone",
			standard:     "gzip, zstd",
			wantEncoding: "gzip",
		},
		{
			name:         "neither header",
			wantEncoding: "",
		},
		{
			name:         "only unproducible codecs offered",
			custom:       "br",
			standard:     "deflate, br, identity",
			wantEncoding: "",
		},
		{
			// q-values are parsed off and ignored, never honoured as weights;
			// the listed order stands.
			name:           "q-values stripped, order preserved",
			custom:         "zstd;q=0.5, gzip",
			wantEncoding:   "zstd",
			wantUsedCustom: true,
		},
		{
			// The client's order is authoritative even when it disagrees
			// with the server's own zstd-first supportedEncodings list.
			name:           "client order beats server preference",
			custom:         "gzip",
			standard:       "zstd",
			wantEncoding:   "gzip",
			wantUsedCustom: true,
		},
		{
			// Unknown codecs are skipped, not treated as a dead end.
			name:         "unknown codec skipped in favour of a known one",
			standard:     "br, deflate, gzip",
			wantEncoding: "gzip",
		},
		{
			// A codec on both headers is not "custom only".
			name:         "winner present on both headers",
			custom:       "gzip",
			standard:     "gzip",
			wantEncoding: "gzip",
		},
		{
			name:         "empty and whitespace tokens ignored",
			standard:     " , ZSTD , ",
			wantEncoding: "zstd",
		},
		// identity: an explicit opt-out, decided by position like any
		// other token. The four rows below are the addendum's worked
		// examples, in order.
		{
			name:         "identity in custom beats compressed standard codecs",
			custom:       "identity",
			standard:     "gzip, zstd",
			wantEncoding: "",
		},
		{
			name:         "identity ahead of zstd in custom wins",
			custom:       "identity, zstd",
			wantEncoding: "",
		},
		{
			name:           "identity after zstd in custom loses",
			custom:         "zstd, identity",
			wantEncoding:   "zstd",
			wantUsedCustom: true,
		},
		{
			name:         "identity first in standard wins",
			standard:     "identity, gzip",
			wantEncoding: "",
		},
		{
			// q-values are ignored for identity too — order alone decides,
			// so identity;q=0 still opts out.
			name:         "identity q-value not honoured",
			custom:       "identity;q=0, zstd",
			wantEncoding: "",
		},
		{
			// Compression disabled by configuration: nothing is producible,
			// so every offer resolves to identity.
			name:          "no producible codecs",
			custom:        "zstd, gzip",
			standard:      "gzip, zstd",
			producible:    nil,
			producibleSet: true,
			wantEncoding:  "",
		},
		{
			// A narrowed producible set skips a codec the client ranked first.
			name:          "first choice not producible falls through",
			custom:        "zstd, gzip",
			producible:    []string{"gzip"},
			producibleSet: true,
			wantEncoding:  "gzip",
			// gzip is offered only on the custom header here.
			wantUsedCustom: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			producible := supportedEncodings
			if tt.producibleSet {
				producible = tt.producible
			}
			enc, usedCustom := chooseResponseEncoding(tt.custom, tt.standard, producible)
			if enc != tt.wantEncoding {
				t.Errorf("encoding = %q, want %q", enc, tt.wantEncoding)
			}
			if usedCustom != tt.wantUsedCustom {
				t.Errorf("usedCustom = %v, want %v", usedCustom, tt.wantUsedCustom)
			}
		})
	}
}

// serveUnaryWithHeaders drives one unary round trip with the given request
// headers and returns the recorder.
func serveUnaryWithHeaders(t *testing.T, h *HttpServer, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	body := encodeRequestBody(t, "add", benchAddParams{A: 1.5, B: 2.5})
	req := httptest.NewRequest(http.MethodPost, "/add", bytes.NewReader(body))
	req.Header.Set("Content-Type", arrowContentType)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	return rec
}

// TestResponseEncodingHeaderStamping checks which response header carries
// the negotiated codec, end to end through the handler stack.
func TestResponseEncodingHeaderStamping(t *testing.T) {
	tests := []struct {
		name                  string
		headers               map[string]string
		wantContentEncoding   string
		wantCustomContentEnc  string
		wantCompressedPayload bool
	}{
		{
			// Browser/WASM: the custom request header forces the custom
			// response header, because that client's fetch layer would
			// mishandle a standard Content-Encoding it never asked for.
			name:                  "custom header alone stamps X-VGI-Content-Encoding",
			headers:               map[string]string{customAcceptEncodingHeader: "zstd, gzip"},
			wantCustomContentEnc:  "zstd",
			wantCompressedPayload: true,
		},
		{
			name: "codec on both headers stamps Content-Encoding",
			headers: map[string]string{
				customAcceptEncodingHeader: "zstd, gzip",
				acceptEncodingHeader:       "deflate, gzip, br, zstd",
			},
			wantContentEncoding:   "zstd",
			wantCompressedPayload: true,
		},
		{
			name:    "no accept headers leaves the body uncompressed",
			headers: nil,
		},
		{
			name:    "unproducible codecs leave the body uncompressed",
			headers: map[string]string{acceptEncodingHeader: "br, deflate"},
		},
		{
			// Explicit opt-out: identity leads, so nothing is compressed
			// and no encoding header is stamped at all.
			name: "identity first opts out of compression",
			headers: map[string]string{
				customAcceptEncodingHeader: "identity",
				acceptEncodingHeader:       "gzip, zstd",
			},
		},
		{
			// identity behind a producible codec does not opt out.
			name:                  "identity after zstd still compresses",
			headers:               map[string]string{customAcceptEncodingHeader: "zstd, identity"},
			wantCustomContentEnc:  "zstd",
			wantCompressedPayload: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newBenchHTTPServer(t)
			if err := h.SetCompressionLevel(3); err != nil {
				t.Fatalf("SetCompressionLevel: %v", err)
			}
			rec := serveUnaryWithHeaders(t, h, tt.headers)

			if got := rec.Header().Get(contentEncodingHeader); got != tt.wantContentEncoding {
				t.Errorf("%s = %q, want %q", contentEncodingHeader, got, tt.wantContentEncoding)
			}
			if got := rec.Header().Get(customContentEncodingHeader); got != tt.wantCustomContentEnc {
				t.Errorf("%s = %q, want %q", customContentEncodingHeader, got, tt.wantCustomContentEnc)
			}
			if !tt.wantCompressedPayload {
				return
			}
			// The stamped codec must actually describe the bytes.
			encoding := tt.wantContentEncoding + tt.wantCustomContentEnc
			decoded, err := DecodeContentEncoding(rec.Body.Bytes(), encoding, 0)
			if err != nil {
				t.Fatalf("decode %s body: %v", encoding, err)
			}
			if len(decoded) == 0 {
				t.Fatal("decoded body is empty")
			}
		})
	}
}

// TestResponseCompressionEnabledByDefault pins the default: a stock server
// that never calls SetCompressionLevel already compresses, at
// DefaultCompressionLevel, and advertises both codecs. Nothing has to be
// configured to get a compressed response.
func TestResponseCompressionEnabledByDefault(t *testing.T) {
	h := newBenchHTTPServer(t)
	if h.zstdEncoderLevel != DefaultCompressionLevel {
		t.Fatalf("default level = %d, want %d", h.zstdEncoderLevel, DefaultCompressionLevel)
	}
	rec := serveUnaryWithHeaders(t, h, map[string]string{
		customAcceptEncodingHeader: "zstd, gzip",
		acceptEncodingHeader:       "gzip, zstd",
	})
	if got := rec.Header().Get(contentEncodingHeader); got != "zstd" {
		t.Errorf("%s = %q, want %q", contentEncodingHeader, got, "zstd")
	}
	// zstd is on both accept headers, so the standard response header carries it.
	if got := rec.Header().Get(customContentEncodingHeader); got != "" {
		t.Errorf("%s = %q, want empty", customContentEncodingHeader, got)
	}
	decoded, err := DecodeContentEncoding(rec.Body.Bytes(), "zstd", 0)
	if err != nil {
		t.Fatalf("decode zstd body: %v", err)
	}
	if len(decoded) == 0 {
		t.Fatal("decoded body is empty")
	}
	assertSupportedEncodings(t, rec.Header(), "zstd, gzip")
}

// TestResponseCompressionExplicitlyDisabled is the other half of the
// default: SetCompressionLevel(0) is the only way to reach the "I speak no
// compression" state, and it must ship identity even when the client asks
// for a codec on either header, advertising that honestly with a
// present-but-empty VGI-Supported-Encodings.
func TestResponseCompressionExplicitlyDisabled(t *testing.T) {
	h := newBenchHTTPServer(t)
	if err := h.SetCompressionLevel(0); err != nil {
		t.Fatalf("SetCompressionLevel(0): %v", err)
	}
	rec := serveUnaryWithHeaders(t, h, map[string]string{
		customAcceptEncodingHeader: "zstd, gzip",
		acceptEncodingHeader:       "gzip, zstd",
	})
	if got := rec.Header().Get(contentEncodingHeader); got != "" {
		t.Errorf("%s = %q, want empty", contentEncodingHeader, got)
	}
	if got := rec.Header().Get(customContentEncodingHeader); got != "" {
		t.Errorf("%s = %q, want empty", customContentEncodingHeader, got)
	}
	assertSupportedEncodings(t, rec.Header(), "")
}

// assertSupportedEncodings checks the capability header is present (the
// present-but-empty case is meaningful: "I speak no compression", as opposed
// to an absent header, which clients read as a legacy zstd-speaking server).
func assertSupportedEncodings(t *testing.T, hdr http.Header, want string) {
	t.Helper()
	values, present := hdr[http.CanonicalHeaderKey(supportedEncodingsHeader)]
	if !present {
		t.Fatalf("%s absent; want present with value %q", supportedEncodingsHeader, want)
	}
	if len(values) != 1 || values[0] != want {
		t.Errorf("%s = %v, want [%q]", supportedEncodingsHeader, values, want)
	}
}

// TestSupportedEncodingsCapability pins the advertisement to the same
// producibility predicate negotiation uses: the codecs the server can do in
// both directions right now, server-preference order, identity omitted.
// OPTIONS {prefix}/health is the dedicated discovery probe.
func TestSupportedEncodingsCapability(t *testing.T) {
	tests := []struct {
		name string
		// level is applied via SetCompressionLevel only when setLevel is
		// true; otherwise the server is left at its constructed default.
		level    int
		setLevel bool
		want     string
	}{
		{name: "compression enabled by default", want: "zstd, gzip"},
		{name: "level raised", level: 3, setLevel: true, want: "zstd, gzip"},
		{name: "compression explicitly disabled", level: 0, setLevel: true, want: ""},
		{name: "negative level disables", level: -1, setLevel: true, want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newBenchHTTPServer(t)
			if tt.setLevel {
				if err := h.SetCompressionLevel(tt.level); err != nil {
					t.Fatalf("SetCompressionLevel(%d): %v", tt.level, err)
				}
			}
			h.InitPages()

			// Over a real connection, not just the recorder's header map:
			// present-but-empty has to survive serialization, since that is
			// the distinction a discovering client reads.
			ts := httptest.NewServer(h)
			defer ts.Close()
			req, err := http.NewRequest(http.MethodOptions, ts.URL+"/health", nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			assertSupportedEncodings(t, resp.Header, tt.want)

			// The advertisement must match what negotiation would really do.
			enc, _ := chooseResponseEncoding("zstd, gzip", "", h.producibleResponseEncodings())
			if tt.want == "" && enc != "" {
				t.Errorf("advertised no codecs but negotiated %q", enc)
			}
			if tt.want != "" && enc == "" {
				t.Errorf("advertised %q but negotiated identity", tt.want)
			}
		})
	}
}
