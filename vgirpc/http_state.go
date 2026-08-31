// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"container/list"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/klauspost/compress/zstd"
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

// A stream's state divides into a part fixed for the life of the call and a
// part that advances per turn. Carrying both in one token means every
// continuation re-serializes, re-seals, re-opens and re-parses the fixed part
// — for a typical stream, most of the payload. So the two travel separately;
// see docs/WIRE_PROTOCOL.md in the reference repo, which requires the split.
//
// callTokenData is the fixed half: minted once by /init under
// [MetaCallState], echoed by the client on every subsequent request, and
// never re-issued.
type callTokenData struct {
	CreatedAt int64
	CallID    string // 32-char lowercase hex; binds this call to its cursors
	SchemaIPC []byte // serialized output schema for dynamic methods; nil for static
	StreamID  string // stable across init/continuations of one stream call
}

// cursorTokenData is the advancing half: re-minted every turn under
// [MetaStreamState], and the only token a response returns.
type cursorTokenData struct {
	CreatedAt int64
	CallID    string // the call token this cursor belongs to
	State     interface{}
}

// resolvedCall is what an authenticated CallID resolves to — either from the
// cache or by opening the client's call token.
type resolvedCall struct {
	SchemaIPC []byte
	StreamID  string
}

// defaultCallStateCacheEntries bounds the per-process call cache.
const defaultCallStateCacheEntries = 4096

// callStateCache is a bounded, thread-safe LRU of CallID -> resolvedCall.
//
// A pure accelerator: a miss (cold process, evicted entry, request landing on
// a different node) falls back to opening the call token the client supplied,
// so statelessness is preserved and no request depends on a prior request
// having warmed anything.
//
// The key pairs the CallID recovered from *inside* the cursor's ciphertext
// with the caller's identity. Both parts are authenticated before the lookup
// happens, so a client can neither steer a lookup toward another principal's
// entry nor present a CallID the server never minted.
type callStateCache struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	order   *list.List // front = most recently used
	max     int
	ttl     time.Duration
}

type callStateEntry struct {
	key       string
	expiresAt time.Time
	call      *resolvedCall
}

func newCallStateCache(max int, ttl time.Duration) *callStateCache {
	if ttl <= 0 {
		ttl = time.Hour
	}
	return &callStateCache{
		entries: make(map[string]*list.Element),
		order:   list.New(),
		max:     max,
		ttl:     ttl,
	}
}

// callStateIdentity renders the caller identity half of the cache key.
func callStateIdentity(auth *AuthContext) string {
	if auth == nil || !auth.Authenticated {
		if binding := peerEvidenceBinding(auth); binding != "" {
			return "\x00anonymous\x00" + binding
		}
		return "\x00anonymous"
	}
	identity := auth.Domain + "\x00" + auth.Principal
	if binding := peerEvidenceBinding(auth); binding != "" {
		identity += "\x00" + binding
	}
	return identity
}

func (c *callStateCache) get(callID string, auth *AuthContext) *resolvedCall {
	if c == nil || c.max <= 0 {
		return nil
	}
	key := callID + "\x00" + callStateIdentity(auth)
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[key]
	if !ok {
		return nil
	}
	entry := el.Value.(*callStateEntry)
	if time.Now().After(entry.expiresAt) {
		c.order.Remove(el)
		delete(c.entries, key)
		return nil
	}
	c.order.MoveToFront(el)
	return entry.call
}

func (c *callStateCache) put(callID string, auth *AuthContext, call *resolvedCall) {
	if c == nil || c.max <= 0 {
		return
	}
	key := callID + "\x00" + callStateIdentity(auth)
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[key]; ok {
		el.Value.(*callStateEntry).call = call
		el.Value.(*callStateEntry).expiresAt = time.Now().Add(c.ttl)
		c.order.MoveToFront(el)
		return
	}
	el := c.order.PushFront(&callStateEntry{
		key:       key,
		expiresAt: time.Now().Add(c.ttl),
		call:      call,
	})
	c.entries[key] = el
	for c.order.Len() > c.max {
		oldest := c.order.Back()
		if oldest == nil {
			break
		}
		c.order.Remove(oldest)
		delete(c.entries, oldest.Value.(*callStateEntry).key)
	}
}

// On-wire version bytes. The cursor and call tokens carry independent
// version lines because they change for independent reasons.
//
// Cursor history: v4 = AEAD over the framed plaintext; v5 = AEAD over a
// codec-tagged, compressed payload; v6 = cursor-only, with the schemas and
// stream id moved into the call token.
//
// Each bump matters for rolling deploys: an older plaintext frames
// differently, so a newer reader would mis-parse it. Rejecting the old
// version outright turns that into the same clean 400 as any other stale
// token.
const (
	cursorTokenVersion = 0x06
	callTokenVersion   = 0x01
	stateTokenNonceLen = chacha20poly1305.NonceSizeX // 24 bytes for XChaCha20-Poly1305
	stateTokenTagLen   = chacha20poly1305.Overhead   // 16 bytes Poly1305 tag
	stateTokenMinLen   = 1 + stateTokenNonceLen + stateTokenTagLen
	callIDLen          = 16 // random per-stream id, minted at /init
)

// Codec tags for the token payload, written as the first plaintext byte
// inside the seal. See packTokenPayload.
const (
	tokenCodecRaw  = 0x00
	tokenCodecZstd = 0x01
)

// tokenZstdLevel matches the Python reference's choice. At token payload
// sizes it measures the same speed as level 1 and slightly smaller, while
// the levels that compress materially better cost many times the CPU for a
// few hundred bytes.
const tokenZstdLevel = 3

// maxTokenPlaintextBytes bounds decompression. The payload is authenticated
// before it is ever decompressed, so this guards against a framework bug
// rather than an attacker — but an unbounded decompress on a request path
// is not worth having.
const maxTokenPlaintextBytes = 64 << 20

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

// packTokenPayload compresses a token payload and tags which codec was used.
//
// Compression happens *inside* the seal, and the order is the whole point:
// once sealed, a token is ciphertext, so the HTTP body codec can no longer
// find any redundancy in it — it recovers only the slack base64 adds, never
// the state's own structure. Compressing first reaches the real redundancy.
//
// Compression is skipped when it does not pay, so a small token never grows
// beyond its plaintext plus the one tag byte; the tag means the reader never
// has to guess.
func packTokenPayload(plaintext []byte) ([]byte, error) {
	enc, _, err := tokenZstd()
	if err != nil {
		return nil, fmt.Errorf("state token codec: %w", err)
	}
	packed := enc.EncodeAll(plaintext, make([]byte, 0, len(plaintext)/2+1))
	if len(packed) < len(plaintext) {
		return append([]byte{tokenCodecZstd}, packed...), nil
	}
	return append([]byte{tokenCodecRaw}, plaintext...), nil
}

// unpackTokenPayload reverses packTokenPayload. An unknown tag or a body
// that will not decompress means a token this server did not mint, so both
// surface as the same uniform error every other token failure uses.
func unpackTokenPayload(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, &RpcError{Type: "RuntimeError", Message: "Malformed state token"}
	}
	body := data[1:]
	switch data[0] {
	case tokenCodecRaw:
		return body, nil
	case tokenCodecZstd:
		_, dec, err := tokenZstd()
		if err != nil {
			return nil, fmt.Errorf("state token codec: %w", err)
		}
		out, derr := dec.DecodeAll(body, nil)
		if derr != nil {
			return nil, &RpcError{Type: "RuntimeError", Message: "Malformed state token"}
		}
		return out, nil
	default:
		return nil, &RpcError{Type: "RuntimeError", Message: "Malformed state token"}
	}
}

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
	if peerEvidenceBinding(auth) != "" {
		prefix = []byte("vgi_rpc.state.v5\x00")
	}
	return tokenAad(prefix, auth)
}

// callTokenAad is stateTokenAad's counterpart for call tokens. The prefix
// differs deliberately, so a call token and a cursor token are not
// interchangeable even for the same principal: presenting one where the
// other is expected fails the AEAD tag check rather than decoding into a
// payload the reader would misinterpret.
func callTokenAad(auth *AuthContext) []byte {
	prefix := []byte("vgi_rpc.call.v1\x00")
	if peerEvidenceBinding(auth) != "" {
		prefix = []byte("vgi_rpc.call.v2\x00")
	}
	return tokenAad(prefix, auth)
}

func tokenAad(prefix []byte, auth *AuthContext) []byte {
	binding := peerEvidenceBinding(auth)
	if auth == nil || !auth.Authenticated {
		out := append(prefix, []byte("\x00anonymous")...)
		if binding != "" {
			out = append(out, 0x00)
			out = append(out, binding...)
		}
		return out
	}
	out := make([]byte, 0, len(prefix)+1+len(auth.Domain)+1+len(auth.Principal)+1+len(binding))
	out = append(out, prefix...)
	out = append(out, 0x01)
	out = append(out, auth.Domain...)
	out = append(out, 0x00)
	out = append(out, auth.Principal...)
	if binding != "" {
		out = append(out, 0x00)
		out = append(out, binding...)
	}
	return out
}

func peerEvidenceBinding(auth *AuthContext) string {
	if auth == nil || auth.Claims == nil {
		return ""
	}
	binding, _ := auth.Claims["peer_evidence_binding"].(string)
	return binding
}

// newCallID mints the random id that binds a call token to its cursors.
func newCallID() (string, error) {
	raw := make([]byte, callIDLen)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("call id: %w", err)
	}
	return hex.EncodeToString(raw), nil
}

// sealToken gob-encodes, compresses, AEAD-seals, and base64-encodes a token
// payload. Shared by both token kinds so the envelope stays identical.
func (h *HttpServer) sealToken(version byte, payload interface{}, aad []byte) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(payload); err != nil {
		return nil, fmt.Errorf("state token encode: %w", err)
	}
	packed, err := packTokenPayload(buf.Bytes())
	if err != nil {
		return nil, err
	}

	aead, err := chacha20poly1305.NewX(normalizeTokenKey(h.tokenKey))
	if err != nil {
		return nil, fmt.Errorf("state token cipher: %w", err)
	}
	nonce := make([]byte, stateTokenNonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("state token nonce: %w", err)
	}
	ciphertext := aead.Seal(nil, nonce, packed, aad)

	raw := make([]byte, 0, 1+stateTokenNonceLen+len(ciphertext))
	raw = append(raw, version)
	raw = append(raw, nonce...)
	raw = append(raw, ciphertext...)
	encoded := make([]byte, base64.StdEncoding.EncodedLen(len(raw)))
	base64.StdEncoding.Encode(encoded, raw)
	return encoded, nil
}

// openToken reverses sealToken into out (a pointer to the token struct).
func (h *HttpServer) openToken(version byte, token []byte, aad []byte, out interface{}) error {
	raw, err := base64.StdEncoding.DecodeString(string(token))
	if err != nil {
		return &RpcError{Type: "RuntimeError", Message: "Malformed state token"}
	}
	if len(raw) < stateTokenMinLen {
		return &RpcError{Type: "RuntimeError", Message: "Malformed state token"}
	}
	if raw[0] != version {
		return &RpcError{Type: "RuntimeError",
			Message: fmt.Sprintf("Unsupported state token version %d (expected %d)", raw[0], version)}
	}

	nonce := raw[1 : 1+stateTokenNonceLen]
	ciphertext := raw[1+stateTokenNonceLen:]

	aead, err := chacha20poly1305.NewX(normalizeTokenKey(h.tokenKey))
	if err != nil {
		return fmt.Errorf("state token cipher: %w", err)
	}
	sealed, err := aead.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		// Map every authenticity failure (bad tag, wrong key, wrong AAD
		// — i.e. cross-principal replay) to a single uniform error so
		// callers cannot distinguish failure modes via timing or message.
		return &RpcError{Type: "RuntimeError", Message: "State token signature verification failed"}
	}
	// Decompress only after authentication: nothing an attacker supplies
	// reaches the decoder without the token key.
	plaintext, err := unpackTokenPayload(sealed)
	if err != nil {
		return err
	}

	// NOTE: gob is not designed for untrusted input and could panic or cause
	// type confusion with attacker-crafted payloads. This is acceptable here
	// because AEAD authentication has succeeded above — an attacker cannot
	// reach the gob decoder without knowing the token key.
	if err := gob.NewDecoder(bytes.NewReader(plaintext)).Decode(out); err != nil {
		return fmt.Errorf("state token decode: %w", err)
	}
	return nil
}

// checkTokenAge enforces the TTL after authenticity has been established.
func (h *HttpServer) checkTokenAge(createdAt int64) error {
	age := time.Since(time.Unix(createdAt, 0))
	if age > h.tokenTTL {
		return &RpcError{Type: "RuntimeError",
			Message: fmt.Sprintf("State token expired (age: %v, ttl: %v)", age, h.tokenTTL)}
	}
	return nil
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

// packCallToken seals the half of a stream's state that is fixed for the
// life of the call. Minted once, by /init; never re-issued.
func (h *HttpServer) packCallToken(callID string, outputSchema *arrow.Schema, auth *AuthContext, streamID string) ([]byte, error) {
	data := callTokenData{
		CreatedAt: time.Now().Unix(),
		CallID:    callID,
		StreamID:  streamID,
	}
	if outputSchema != nil {
		data.SchemaIPC = serializeSchema(outputSchema)
	}
	token, err := h.sealToken(callTokenVersion, &data, callTokenAad(auth))
	if err != nil {
		return nil, err
	}
	// Warm the cache with the values we already hold, so this stream's first
	// continuation does not have to open the token it was just handed.
	h.callStates.put(callID, auth, &resolvedCall{SchemaIPC: data.SchemaIPC, StreamID: streamID})
	return token, nil
}

// packCursorToken seals the advancing half. Re-minted every turn; this is
// the only token a response returns.
func (h *HttpServer) packCursorToken(callID string, state interface{}, auth *AuthContext) ([]byte, error) {
	data := cursorTokenData{
		CreatedAt: time.Now().Unix(),
		CallID:    callID,
		State:     state,
	}
	return h.sealToken(cursorTokenVersion, &data, stateTokenAad(auth))
}

// openCursorToken authenticates a cursor and returns its contents.
func (h *HttpServer) openCursorToken(token []byte, auth *AuthContext) (*cursorTokenData, error) {
	var data cursorTokenData
	if err := h.openToken(cursorTokenVersion, token, stateTokenAad(auth), &data); err != nil {
		return nil, err
	}
	if err := h.checkTokenAge(data.CreatedAt); err != nil {
		return nil, err
	}
	return &data, nil
}

// resolveCall recovers a stream's fixed half for an already-authenticated
// cursor.
//
// Order matters here, and it is the whole security argument for the cache.
// The cursor is opened first by the caller; its AEAD tag covers the CallID
// and its AAD covers the caller's identity. Only then is that authenticated
// CallID used as a cache key. A client cannot name a call id the server did
// not mint for it, so a cache hit can never hand back another principal's
// call state — and on a hit the presented call token is not consulted at
// all, which is exactly the work we are trying to avoid.
//
// On a miss (cold process, evicted entry, or a request load-balanced to a
// node that never saw this stream's /init) the client-supplied call token is
// opened and verified, and its embedded CallID must match the one the cursor
// named.
func (h *HttpServer) resolveCall(cursor *cursorTokenData, callToken []byte, auth *AuthContext) (*resolvedCall, error) {
	if got := h.callStates.get(cursor.CallID, auth); got != nil {
		return got, nil
	}
	if len(callToken) == 0 {
		return nil, &RpcError{Type: "RuntimeError", Message: "Missing call token in exchange request"}
	}

	var data callTokenData
	if err := h.openToken(callTokenVersion, callToken, callTokenAad(auth), &data); err != nil {
		return nil, err
	}
	if err := h.checkTokenAge(data.CreatedAt); err != nil {
		return nil, err
	}
	if data.CallID != cursor.CallID {
		// The cursor named a different call. Uniform message: this is only
		// reachable by pairing two tokens the same principal legitimately
		// holds, so it carries no information worth distinguishing.
		return nil, &RpcError{Type: "RuntimeError", Message: "Malformed state token"}
	}

	got := &resolvedCall{SchemaIPC: data.SchemaIPC, StreamID: data.StreamID}
	h.callStates.put(cursor.CallID, auth, got)
	return got, nil
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
