// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Tests for the public DecodeContentEncoding helper (for intermediaries),
// translated from the Python reference's TestDecodeContentEncoding.

package vgirpc

import (
	"bytes"
	"compress/gzip"
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
