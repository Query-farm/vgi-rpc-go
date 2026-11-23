// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"compress/flate"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// The standard token codec (STATE_REF_V1.md §7): every sealed payload this
// package mints -- the HTTP cursor and call tokens, and the inline and stored
// StateRef payloads -- is compressed *before* it is sealed. Once sealed a
// token is ciphertext, and no outer codec can find the redundancy in it.
//
//	sealed_payload := codec (1 byte) || body
//
//	0x00  raw framed plaintext   used when compression does not shrink it
//	0x01  zstd frame             the default; level 3
//	0x02  raw DEFLATE (RFC 1951) for runtimes without zstd
//	other reserved; refused
//
// A reader must accept every codec its own writer can emit. This writer emits
// 0x00 and 0x01; the reader also accepts 0x02, as the Python reference does.
const (
	tokenCodecRaw     = 0x00
	tokenCodecZstd    = 0x01
	tokenCodecDeflate = 0x02
)

// tokenZstdLevel matches the Python reference's choice. At token payload
// sizes it measures the same speed as level 1 and slightly smaller, while
// the levels that compress materially better cost many times the CPU for a
// few hundred bytes.
const tokenZstdLevel = 3

// maxTokenPlaintextBytes bounds decompression of a token (64 MiB). The
// payload is authenticated before it is ever decompressed, so this guards
// against a framework bug rather than an attacker -- but an unbounded
// decompress on a request path is not worth having.
const maxTokenPlaintextBytes = 64 << 20

// errTokenCodec is the one error every codec failure maps to: an unknown tag
// or a body that will not decompress means a payload this server did not
// mint, and callers turn it into their own uniform refusal.
var errTokenCodec = errors.New("malformed token payload")

// One encoder and one decoder for the process. klauspost's EncodeAll and
// DecodeAll are safe for concurrent use, and building either per token would
// allocate level-sized tables on every stream turn.
var (
	tokenZstdOnce    sync.Once
	tokenZstdEncoder *zstd.Encoder
	tokenZstdDecoder *zstd.Decoder
	tokenZstdErr     error
)

func tokenZstd() (*zstd.Encoder, *zstd.Decoder, error) {
	tokenZstdOnce.Do(func() {
		enc, err := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(tokenZstdLevel)),
			// Payloads are buffered whole before sealing, so extra worker
			// goroutines buy nothing and make the shared encoder costlier.
			zstd.WithEncoderConcurrency(1),
		)
		if err != nil {
			tokenZstdErr = err
			return
		}
		dec, err := zstd.NewReader(nil,
			zstd.WithDecoderMaxMemory(maxTokenPlaintextBytes),
			zstd.WithDecoderConcurrency(1),
		)
		if err != nil {
			tokenZstdErr = err
			return
		}
		tokenZstdEncoder, tokenZstdDecoder = enc, dec
	})
	return tokenZstdEncoder, tokenZstdDecoder, tokenZstdErr
}

// codecPack compresses plaintext when that makes it smaller and tags the codec
// used, so a small payload never grows by more than the tag byte.
func codecPack(plaintext []byte) ([]byte, error) {
	enc, _, err := tokenZstd()
	if err != nil {
		return nil, fmt.Errorf("token codec: %w", err)
	}
	packed := enc.EncodeAll(plaintext, make([]byte, 1, len(plaintext)/2+2))
	if len(packed)-1 < len(plaintext) {
		packed[0] = tokenCodecZstd
		return packed, nil
	}
	out := make([]byte, 0, len(plaintext)+1)
	out = append(out, tokenCodecRaw)
	return append(out, plaintext...), nil
}

// codecUnpack reverses codecPack, refusing output past maxOutput bytes. Every
// failure -- empty payload, unknown tag, a body that does not decompress or
// decompresses past the bound -- is errTokenCodec, wrapped with the reason.
func codecUnpack(data []byte, maxOutput int64) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("%w: empty payload", errTokenCodec)
	}
	body := data[1:]
	switch data[0] {
	case tokenCodecRaw:
		if int64(len(body)) > maxOutput {
			return nil, fmt.Errorf("%w: payload exceeds the decompression bound", errTokenCodec)
		}
		return body, nil
	case tokenCodecZstd:
		// A frame that declares its content size is checked against the bound
		// before anything is allocated for it.
		var header zstd.Header
		if err := header.Decode(body); err != nil {
			return nil, fmt.Errorf("%w: zstd body does not decompress", errTokenCodec)
		}
		if header.HasFCS && header.FrameContentSize > uint64(maxOutput) {
			return nil, fmt.Errorf("%w: payload exceeds the decompression bound", errTokenCodec)
		}
		if maxOutput <= maxTokenPlaintextBytes {
			_, dec, err := tokenZstd()
			if err != nil {
				return nil, fmt.Errorf("token codec: %w", err)
			}
			out, derr := dec.DecodeAll(body, nil)
			if derr != nil || int64(len(out)) > maxOutput {
				return nil, fmt.Errorf("%w: zstd body does not decompress within the bound", errTokenCodec)
			}
			return out, nil
		}
		// A bound above the shared decoder's: stream it through a dedicated
		// decoder, reading at most one byte past the bound.
		dec, err := zstd.NewReader(bytes.NewReader(body), zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderMaxMemory(uint64(maxOutput)+1))
		if err != nil {
			return nil, fmt.Errorf("%w: zstd body does not decompress", errTokenCodec)
		}
		defer dec.Close()
		return readBounded(dec, maxOutput)
	case tokenCodecDeflate:
		return readBounded(flate.NewReader(bytes.NewReader(body)), maxOutput)
	default:
		return nil, fmt.Errorf("%w: unknown codec tag 0x%02x", errTokenCodec, data[0])
	}
}

// readBounded drains r, refusing more than maxOutput bytes.
func readBounded(r io.Reader, maxOutput int64) ([]byte, error) {
	out, err := io.ReadAll(io.LimitReader(r, maxOutput+1))
	if err != nil {
		return nil, fmt.Errorf("%w: body does not decompress", errTokenCodec)
	}
	if int64(len(out)) > maxOutput {
		return nil, fmt.Errorf("%w: payload exceeds the decompression bound", errTokenCodec)
	}
	return out, nil
}

// packTokenPayload is codecPack for the HTTP state tokens.
func packTokenPayload(plaintext []byte) ([]byte, error) {
	return codecPack(plaintext)
}

// unpackTokenPayload reverses packTokenPayload under the 64 MiB token bound.
// Any codec failure surfaces as the same uniform error every other token
// failure uses.
func unpackTokenPayload(data []byte) ([]byte, error) {
	out, err := codecUnpack(data, maxTokenPlaintextBytes)
	if err != nil {
		if errors.Is(err, errTokenCodec) {
			return nil, &RpcError{Type: "RuntimeError", Message: "Malformed state token"}
		}
		return nil, err
	}
	return out, nil
}
