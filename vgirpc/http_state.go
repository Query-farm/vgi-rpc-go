// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"golang.org/x/crypto/chacha20poly1305"
)

// RehydrateFunc reconstructs non-serializable fields on a deserialized stream
// state. Called by the HTTP server after unpacking a state token. The method
// parameter is the RPC method name (e.g. "init").
type RehydrateFunc func(state interface{}, method string) error

// RegisterStateType registers a concrete type for gob encoding so that it can
// be serialized into HTTP state tokens. Each [ProducerState] and
// [ExchangeState] implementation (and any types they embed) must be
// registered before the first HTTP stream request. Typically this is done
// in a package init() function.
func RegisterStateType(v interface{}) {
	gob.Register(v)
}

// --- State Token ---

type stateTokenData struct {
	CreatedAt int64
	State     interface{}
	SchemaIPC []byte // serialized output schema for dynamic methods; nil for static
	StreamID  string // 32-char lowercase hex; stable across init/continuations of one stream call
}

// stateTokenVersion is the on-wire version byte for state tokens. v4 = AEAD.
const (
	stateTokenVersion = 0x04
	stateTokenNonceLen = chacha20poly1305.NonceSizeX // 24 bytes for XChaCha20-Poly1305
	stateTokenTagLen   = chacha20poly1305.Overhead   // 16 bytes Poly1305 tag
	stateTokenMinLen   = 1 + stateTokenNonceLen + stateTokenTagLen
)

// stateTokenAad builds the AEAD associated data that binds a state token
// to the authenticated caller. Mirrors Python's _compute_aad in
// vgi_rpc/http/server/_state_token.py byte-for-byte: anonymous tokens
// carry b"\x00anonymous"; authenticated tokens carry
// b"\x01" + domain + b"\x00" + principal.
//
// The domain MUST appear between the 0x01 byte and the principal even
// when empty — Python emits b"\x01\x00" + principal in that case, so
// dropping the separator breaks cross-port decryption AND lets a token
// sealed under one auth domain be opened by the same principal under
// another (cross-domain replay). Anonymous and authenticated branches
// produce non-overlapping byte strings so an anonymous token cannot be
// opened by a named principal and vice versa.
func stateTokenAad(auth *AuthContext) []byte {
	prefix := []byte("vgi_rpc.state.v4\x00")
	if auth == nil || !auth.Authenticated {
		return append(prefix, []byte("\x00anonymous")...)
	}
	out := make([]byte, 0, len(prefix)+1+len(auth.Domain)+1+len(auth.Principal))
	out = append(out, prefix...)
	out = append(out, 0x01)
	out = append(out, auth.Domain...)
	out = append(out, 0x00)
	out = append(out, auth.Principal...)
	return out
}

// normalizeTokenKey stretches/compresses an arbitrary-length token key to
// the 32 bytes XChaCha20-Poly1305 requires. Exactly-32-byte keys pass
// through unchanged; any other length is collapsed via SHA-256.  Matches
// the Python port's approach for ergonomic operator-supplied keys.
func normalizeTokenKey(key []byte) []byte {
	if len(key) == chacha20poly1305.KeySize {
		return key
	}
	sum := sha256.Sum256(key)
	return sum[:]
}

func (h *HttpServer) packStateToken(state interface{}, outputSchema *arrow.Schema, auth *AuthContext, streamID string) ([]byte, error) {
	data := stateTokenData{
		CreatedAt: time.Now().Unix(),
		State:     state,
		StreamID:  streamID,
	}
	if outputSchema != nil {
		data.SchemaIPC = serializeSchema(outputSchema)
	}
	var payload bytes.Buffer
	enc := gob.NewEncoder(&payload)
	if err := enc.Encode(&data); err != nil {
		return nil, fmt.Errorf("state token encode: %w", err)
	}

	aead, err := chacha20poly1305.NewX(normalizeTokenKey(h.tokenKey))
	if err != nil {
		return nil, fmt.Errorf("state token cipher: %w", err)
	}
	nonce := make([]byte, stateTokenNonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("state token nonce: %w", err)
	}
	ciphertext := aead.Seal(nil, nonce, payload.Bytes(), stateTokenAad(auth))

	raw := make([]byte, 0, 1+stateTokenNonceLen+len(ciphertext))
	raw = append(raw, stateTokenVersion)
	raw = append(raw, nonce...)
	raw = append(raw, ciphertext...)
	encoded := make([]byte, base64.StdEncoding.EncodedLen(len(raw)))
	base64.StdEncoding.Encode(encoded, raw)
	return encoded, nil
}

func (h *HttpServer) unpackStateToken(token []byte, auth *AuthContext) (*stateTokenData, error) {
	raw, err := base64.StdEncoding.DecodeString(string(token))
	if err != nil {
		return nil, &RpcError{Type: "RuntimeError", Message: "Malformed state token"}
	}

	if len(raw) < stateTokenMinLen {
		return nil, &RpcError{Type: "RuntimeError", Message: "Malformed state token"}
	}

	version := raw[0]
	if version != stateTokenVersion {
		return nil, &RpcError{Type: "RuntimeError",
			Message: fmt.Sprintf("Unsupported state token version %d (expected %d)", version, stateTokenVersion)}
	}

	nonce := raw[1 : 1+stateTokenNonceLen]
	ciphertext := raw[1+stateTokenNonceLen:]

	aead, err := chacha20poly1305.NewX(normalizeTokenKey(h.tokenKey))
	if err != nil {
		return nil, fmt.Errorf("state token cipher: %w", err)
	}
	plaintext, err := aead.Open(nil, nonce, ciphertext, stateTokenAad(auth))
	if err != nil {
		// Map every authenticity failure (bad tag, wrong key, wrong AAD
		// — i.e. cross-principal replay) to a single uniform error so
		// callers cannot distinguish failure modes via timing or message.
		return nil, &RpcError{Type: "RuntimeError", Message: "State token signature verification failed"}
	}

	// NOTE: gob is not designed for untrusted input and could panic or cause
	// type confusion with attacker-crafted payloads. This is acceptable here
	// because AEAD authentication has succeeded above — an attacker cannot
	// reach the gob decoder without knowing the token key. If the threat
	// model changes (e.g. shared keys across trust boundaries), consider
	// switching to a safer serializer (JSON, protobuf).
	var data stateTokenData
	dec := gob.NewDecoder(bytes.NewReader(plaintext))
	if err := dec.Decode(&data); err != nil {
		return nil, fmt.Errorf("state token decode: %w", err)
	}

	age := time.Since(time.Unix(data.CreatedAt, 0))
	if age > h.tokenTTL {
		return nil, &RpcError{Type: "RuntimeError",
			Message: fmt.Sprintf("State token expired (age: %v, ttl: %v)", age, h.tokenTTL)}
	}

	return &data, nil
}

// deserializeSchema recovers an Arrow schema from IPC-serialized bytes.
func deserializeSchema(data []byte) (*arrow.Schema, error) {
	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("schema deserialization: %w", err)
	}
	defer reader.Release()
	return reader.Schema(), nil
}
