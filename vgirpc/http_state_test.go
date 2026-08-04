// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"crypto/rand"
	"strings"
	"testing"
	"time"
)

// Token payloads are compressed *inside* the seal. The ordering is the whole
// point: once a token is sealed it is ciphertext, so the HTTP body codec can
// find no redundancy in it — it recovers only the slack base64 adds, never
// the state's own structure. Compressing before sealing reaches the real
// redundancy.
//
// None of this is visible on the wire, so the cross-language conformance
// suite cannot reach it; docs/WIRE_PROTOCOL.md makes it normative and asks
// each port to pin it with a language-local test like this one.

func TestTokenPayloadCompressesWhenRedundant(t *testing.T) {
	plaintext := bytes.Repeat([]byte("vgi-rpc-state-"), 1000)
	packed, err := packTokenPayload(plaintext)
	if err != nil {
		t.Fatal(err)
	}
	if packed[0] != tokenCodecZstd {
		t.Fatalf("expected the zstd codec tag, got %#x", packed[0])
	}
	if len(packed) >= len(plaintext)/4 {
		t.Fatalf("expected real compression on a redundant payload, got %d from %d",
			len(packed), len(plaintext))
	}
}

func TestTokenPayloadStaysRawWhenIncompressible(t *testing.T) {
	// Random bytes give the codec no redundancy to find. Skipping is what
	// keeps the guarantee one-directional: a token may get smaller, never
	// larger than its plaintext plus the one tag byte.
	plaintext := make([]byte, 64)
	if _, err := rand.Read(plaintext); err != nil {
		t.Fatal(err)
	}
	packed, err := packTokenPayload(plaintext)
	if err != nil {
		t.Fatal(err)
	}
	if packed[0] != tokenCodecRaw {
		t.Fatalf("expected the raw codec tag, got %#x", packed[0])
	}
	if len(packed) != len(plaintext)+1 {
		t.Fatalf("raw payload must not grow beyond the tag byte: %d vs %d",
			len(packed), len(plaintext)+1)
	}
}

func TestTokenPayloadRoundTripsUnderEitherCodec(t *testing.T) {
	incompressible := make([]byte, 32)
	if _, err := rand.Read(incompressible); err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"empty":          {},
		"one-byte":       []byte("x"),
		"incompressible": incompressible,
		"compressible":   bytes.Repeat([]byte("vgi-rpc-state-"), 500),
	}
	for name, plaintext := range cases {
		t.Run(name, func(t *testing.T) {
			packed, err := packTokenPayload(plaintext)
			if err != nil {
				t.Fatal(err)
			}
			got, err := unpackTokenPayload(packed)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, plaintext) {
				t.Fatalf("round trip changed the payload: %d bytes in, %d out",
					len(plaintext), len(got))
			}
		})
	}
}

func TestTokenPayloadRejectsMalformedInput(t *testing.T) {
	// An unknown tag, an empty payload, or a body that will not decompress
	// all mean a token this server did not mint, so all three surface as
	// the same uniform error the caller maps to 400.
	cases := map[string][]byte{
		"empty":        {},
		"unknown-tag":  append([]byte{0x7f}, []byte("payload")...),
		"corrupt-zstd": append([]byte{tokenCodecZstd}, []byte("not-a-zstd-frame")...),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := unpackTokenPayload(data); err == nil {
				t.Fatal("expected an error, got nil")
			}
		})
	}
}

func TestSealedStateTokenShrinksWithACompressibleState(t *testing.T) {
	// End to end: compression inside the seal shrinks the token itself.
	// Guards the ordering rather than the codec — a token sealed around an
	// uncompressed payload comes out *larger* than its input once base64
	// inflation is counted, which is the regression this catches.
	RegisterStateType(compressibleState{})
	h := NewHttpServer(NewServer())
	h.SetTokenTTL(time.Hour)
	state := compressibleState{Blob: strings.Repeat("vgi-rpc-call-state-", 400)}

	callID, err := newCallID()
	if err != nil {
		t.Fatal(err)
	}
	token, err := h.packCursorToken(callID, state, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(token) >= len(state.Blob)/4 {
		t.Fatalf("sealed cursor (%dB) should be far smaller than its state (%dB)",
			len(token), len(state.Blob))
	}

	// And it still round-trips.
	back, err := h.openCursorToken(token, nil)
	if err != nil {
		t.Fatal(err)
	}
	if back.CallID != callID {
		t.Fatalf("call id did not survive: %q vs %q", back.CallID, callID)
	}
	got, ok := back.State.(compressibleState)
	if !ok {
		t.Fatalf("state came back as %T", back.State)
	}
	if got.Blob != state.Blob {
		t.Fatal("state blob did not survive the round trip")
	}
}

// The cursor/call split: the cursor names a call, and only an authenticated
// cursor may resolve one. See docs/WIRE_PROTOCOL.md in the reference repo.

func TestResolveCallFallsBackToTheClientToken(t *testing.T) {
	// The cache is an accelerator, never a contract. With it cold — a
	// restarted worker, an evicted entry, or a node that never saw the
	// /init — the client's echoed call token has to carry the day.
	h := NewHttpServer(NewServer())
	callID, err := newCallID()
	if err != nil {
		t.Fatal(err)
	}
	callToken, err := h.packCallToken(callID, nil, nil, "sid-42")
	if err != nil {
		t.Fatal(err)
	}
	cursor := &cursorTokenData{CreatedAt: time.Now().Unix(), CallID: callID}

	// Minting warms the cache, so drop it to stand in for the node that
	// never saw this stream's /init — the whole case the call token exists
	// for.
	h.callStates = newCallStateCache(defaultCallStateCacheEntries, h.tokenTTL)

	// Cold cache, no call token -> rejected rather than guessed at.
	if _, err := h.resolveCall(cursor, nil, nil); err == nil {
		t.Fatal("expected a cold-cache resolve with no call token to fail")
	}

	// Cold cache, with the token -> resolved, and cached for next time.
	got, err := h.resolveCall(cursor, callToken, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.StreamID != "sid-42" {
		t.Fatalf("stream id %q", got.StreamID)
	}
	if cached := h.callStates.get(callID, nil); cached == nil {
		t.Fatal("a successful resolve should warm the cache")
	}

	// Warm cache -> the presented call token is not consulted at all.
	got2, err := h.resolveCall(cursor, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got2.StreamID != "sid-42" {
		t.Fatalf("stream id %q from cache", got2.StreamID)
	}
}

func TestResolveCallRejectsAMismatchedCallID(t *testing.T) {
	// A cursor may only resolve the call it names. Pairing a cursor with
	// another call's token is refused even though both opened cleanly.
	h := NewHttpServer(NewServer())
	callA, _ := newCallID()
	callB, _ := newCallID()
	tokenB, err := h.packCallToken(callB, nil, nil, "sid-b")
	if err != nil {
		t.Fatal(err)
	}
	cursorA := &cursorTokenData{CreatedAt: time.Now().Unix(), CallID: callA}
	if _, err := h.resolveCall(cursorA, tokenB, nil); err == nil {
		t.Fatal("expected a cursor/call id mismatch to be rejected")
	}
}

func TestCallAndCursorTokensAreNotInterchangeable(t *testing.T) {
	// The two AADs carry different version-tagged prefixes, so a swap fails
	// the AEAD tag check rather than decoding into a payload the reader
	// would misinterpret.
	RegisterStateType(compressibleState{})
	h := NewHttpServer(NewServer())
	callID, _ := newCallID()
	cursor, err := h.packCursorToken(callID, compressibleState{Blob: "x"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	callToken, err := h.packCallToken(callID, nil, nil, "sid")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.openCursorToken(callToken, nil); err == nil {
		t.Fatal("a call token must not open as a cursor")
	}
	var data callTokenData
	if err := h.openToken(callTokenVersion, cursor, callTokenAad(nil), &data); err == nil {
		t.Fatal("a cursor must not open as a call token")
	}
}

// compressibleState is a stream state whose payload is highly redundant, so
// the codec has something to find.
type compressibleState struct {
	Blob string
}
