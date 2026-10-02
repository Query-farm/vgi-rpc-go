// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// A compressed body declares a window no larger than itself, so a peer that
// caps the decoder window at a small response limit (vgi-rpc-rust uses the
// advertised max response bytes) still decodes it. Without the content size
// the encoder declares its default multi-megabyte window.
func TestCompressedBodyWindowFitsItsSize(t *testing.T) {
	body := make([]byte, 600<<10)
	for i := range body {
		body[i] = byte(i * 31 % 251)
	}
	for _, level := range []int{1, 2, 3, 4} {
		var compressed bytes.Buffer
		writer, e := newCompressWriter("zstd", &compressed, level, int64(len(body)))
		if e != nil {
			t.Fatal(e)
		}
		if _, e := writer.Write(body); e != nil {
			t.Fatal(e)
		}
		if e := writer.Close(); e != nil {
			t.Fatal(e)
		}
		decoder, e := zstd.NewReader(bytes.NewReader(compressed.Bytes()), zstd.WithDecoderMaxWindow(1<<20))
		if e != nil {
			t.Fatal(e)
		}
		got, e := io.ReadAll(decoder)
		decoder.Close()
		if e != nil {
			t.Fatalf("level %d: a 1 MiB window cap refused a %d-byte body: %v", level, len(body), e)
		}
		if !bytes.Equal(got, body) {
			t.Fatalf("level %d: body changed", level)
		}
	}
}
