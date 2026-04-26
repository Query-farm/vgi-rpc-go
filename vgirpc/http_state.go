// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
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

// stateSigningKey derives a per-principal HMAC key from the server's signing
// key, binding state tokens to the authenticated caller. A token issued to one
// principal will fail signature verification when presented by another, so a
// copied token cannot be used across users. Anonymous callers share the empty
// principal namespace.
func (h *HttpServer) stateSigningKey(auth *AuthContext) []byte {
	var principal string
	if auth != nil {
		principal = auth.Principal
	}
	mac := hmac.New(sha256.New, h.signingKey)
	mac.Write([]byte("vgi-rpc-state-v1|"))
	mac.Write([]byte(principal))
	return mac.Sum(nil)
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

	payloadBytes := payload.Bytes()
	mac := hmac.New(sha256.New, h.stateSigningKey(auth))
	mac.Write(payloadBytes)
	sig := mac.Sum(nil)

	raw := append(payloadBytes, sig...)
	encoded := make([]byte, base64.StdEncoding.EncodedLen(len(raw)))
	base64.StdEncoding.Encode(encoded, raw)
	return encoded, nil
}

func (h *HttpServer) unpackStateToken(token []byte, auth *AuthContext) (*stateTokenData, error) {
	raw, err := base64.StdEncoding.DecodeString(string(token))
	if err != nil {
		return nil, &RpcError{Type: "RuntimeError", Message: "Malformed state token"}
	}
	token = raw

	if len(token) < hmacLen {
		return nil, &RpcError{Type: "RuntimeError", Message: "Malformed state token"}
	}

	payloadBytes := token[:len(token)-hmacLen]
	receivedSig := token[len(token)-hmacLen:]

	mac := hmac.New(sha256.New, h.stateSigningKey(auth))
	mac.Write(payloadBytes)
	expectedSig := mac.Sum(nil)

	if !hmac.Equal(receivedSig, expectedSig) {
		return nil, &RpcError{Type: "RuntimeError", Message: "State token signature verification failed"}
	}

	// NOTE: gob is not designed for untrusted input and could panic or cause
	// type confusion with attacker-crafted payloads. This is acceptable here
	// because the HMAC is verified above — an attacker cannot reach the gob
	// decoder without knowing the signing key. If the threat model changes
	// (e.g. shared keys across trust boundaries), consider switching to a
	// safer serializer (JSON, protobuf).
	var data stateTokenData
	dec := gob.NewDecoder(bytes.NewReader(payloadBytes))
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
