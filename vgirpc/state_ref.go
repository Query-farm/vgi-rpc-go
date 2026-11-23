// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"golang.org/x/crypto/chacha20poly1305"
)

// StateRef, argument references and the turn budget (STATE_REF_V1.md,
// protocol 3.4).
//
// On HTTP every stream continuation echoes the stream's state back to the
// server. A [StateRef] is how a server says "this part of my stream state
// does not change": store it once, carry a small sealed tag in the state,
// read it back on any later turn on any replica:
//
//	func scan(_ context.Context, ctx *vgirpc.CallContext, p scanParams) (*vgirpc.StreamResult, error) {
//		ref, err := ctx.StateRef(p.Query) // only while handling init
//		if err != nil {
//			return nil, err
//		}
//		return &vgirpc.StreamResult{OutputSchema: schema, State: &scanState{Query: ref}}, nil
//	}
//
//	func (s *scanState) Produce(_ context.Context, out *vgirpc.OutputCollector, ctx *vgirpc.CallContext) error {
//		query, err := ctx.Resolve(s.Query) // any turn, any replica
//		...
//	}
//
// The client never parses a StateRef: it echoes the opaque state token as it
// always has. Worker code never branches on transport or storage either --
// the same calls work on stdio, Unix, TCP and shared memory (where the state
// never leaves the process, so the value is simply held in memory) and on
// HTTP with or without storage configured.
//
// Byte layouts (STATE_REF_V1.md §16):
//
//	StateRef (inline)  = 0x01 || codec || body
//	StateRef (stored)  = 0x02 || 0x01 || nonce24 || AEAD(tag_plaintext, STATEREF_TAG_AAD)
//	tag_plaintext      = 0x01 || created_at u64le || size u64le || sha256[32]
//	                     || id_len u16le || object_id
//	stored object      = 0x01 || nonce24 || AEAD(codec || body, STATEREF_OBJECT_AAD)
//	argument ref       = 0x01 || nonce24 || AEAD(0x01 || size u64le || sha256[32]
//	                     || id_len u16le || object_id, ARGREF_AAD)
//
// Sealing scopes, all under the server's one token key:
//
//	STATEREF_TAG_AAD    = "vgi_rpc.stateref.v1\x00"        || BASE_AAD(auth, protocol) || call_id
//	STATEREF_OBJECT_AAD = "vgi_rpc.stateref.object.v1\x00" || BASE_AAD(auth, protocol) || call_id
//	ARGREF_AAD          = "vgi_rpc.argref.v1\x00"          || BASE_AAD(auth, "\x00server")
//
// BASE_AAD is exactly the cursor token's AAD ([stateTokenAad]); call_id is the
// stream's 16 random bytes, minted at /init and sealed inside every cursor.

// Protocol 3.4 StateRef / turn-budget constants.
const (
	// DefaultStateRefInlineBytes is the default VGI_RPC_STATE_REF_INLINE_BYTES:
	// the largest compressed StateRef payload kept inline in a token.
	DefaultStateRefInlineBytes = 16384
	// StateRefInlineBytesEnv caps the inline payload size.
	StateRefInlineBytesEnv = "VGI_RPC_STATE_REF_INLINE_BYTES"
	// TurnBudgetHeadroomBytes is the room left in a continuation request for
	// everything but the two tokens.
	TurnBudgetHeadroomBytes = 4096
	// CursorRuleFloorBytes: a request cap below this disables the cursor rule
	// (with a startup warning) rather than refusing to start.
	CursorRuleFloorBytes = 16384
	// DefaultTokenTTL is the default maximum age of cursor tokens, call tokens
	// and stored StateRef tags: 6 hours, the maximum stream duration.
	DefaultTokenTTL = 6 * time.Hour
)

// Wire-stable error kinds of protocol 3.4 (STATE_REF_V1.md §6; the
// argument-reference kinds are the vgi spec's names).
const (
	ErrorKindInvalidStateRef       = "invalid_state_ref"
	ErrorKindStateRefMissing       = "state_ref_missing"
	ErrorKindStateRefTooLarge      = "state_ref_too_large"
	ErrorKindStateRefMisuse        = "state_ref_misuse"
	ErrorKindCursorTooLarge        = "cursor_too_large"
	ErrorKindStorageUnavailable    = "storage_unavailable"
	ErrorKindInvalidArgumentsRef   = "invalid_arguments_ref"
	ErrorKindArgumentsRefIntegrity = "arguments_ref_integrity"
	ErrorKindArgumentsRefMissing   = "arguments_ref_missing"
)

// CursorRule is what an HTTP server does with a sealed cursor over its turn
// budget (STATE_REF_V1.md §9).
type CursorRule string

const (
	// CursorRuleWarn logs cursor_large and sends the client a WARN log; the
	// stream continues. The default.
	CursorRuleWarn CursorRule = "warn"
	// CursorRuleError refuses the turn with cursor_too_large, only when a
	// request cap is set. A framework that keeps its unchanging state in
	// StateRef passes this.
	CursorRuleError CursorRule = "error"
)

const (
	stateRefTagAADPrefix    = "vgi_rpc.stateref.v1\x00"
	stateRefObjectAADPrefix = "vgi_rpc.stateref.object.v1\x00"
	argRefAADPrefix         = "vgi_rpc.argref.v1\x00"

	stateRefFormInline = 0x01
	stateRefFormStored = 0x02
	sealVersion        = 0x01
	stateRefTagVersion = 0x01
	argRefVersion      = 0x01
	maxObjectIDBytes   = 1024
	stateRefTagFixed   = 1 + 8 + 8 + sha256.Size + 2
	argRefFixed        = 1 + 8 + sha256.Size + 2

	// defaultMaxFetchBytes is the inbound fetch cap without an external
	// config (the ExternalLocationConfig default).
	defaultMaxFetchBytes int64 = 256 * 1024 * 1024
)

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

func stateRefError(typ, kind string, code Code, format string, args ...any) *RpcError {
	return &RpcError{Type: typ, Message: fmt.Sprintf(format, args...), Kind: kind, Code: string(code)}
}

func invalidStateRef(format string, args ...any) *RpcError {
	return stateRefError("InvalidStateRefError", ErrorKindInvalidStateRef, CodeInvalidArgument, format, args...)
}

func stateRefMissing() *RpcError {
	return stateRefError("StateRefMissingError", ErrorKindStateRefMissing, CodeNotFound,
		"stream state object is gone (StateRef object expired or deleted); restart the query")
}

func stateRefTooLarge(format string, args ...any) *RpcError {
	return stateRefError("StateRefTooLargeError", ErrorKindStateRefTooLarge, CodeResourceExhausted, format, args...)
}

func invalidArgumentsRef(format string, args ...any) *RpcError {
	return stateRefError("InvalidArgumentsRefError", ErrorKindInvalidArgumentsRef, CodeInvalidArgument, format, args...)
}

func argumentsRefIntegrity(format string, args ...any) *RpcError {
	return stateRefError("ArgumentsRefIntegrityError", ErrorKindArgumentsRefIntegrity, CodeDataLoss, format, args...)
}

func argumentsRefMissing() *RpcError {
	return stateRefError("ArgumentsRefMissingError", ErrorKindArgumentsRefMissing, CodeNotFound,
		"the uploaded argument object is gone (expired or deleted)")
}

// isInvalidStateRef reports whether err is an invalid_state_ref failure,
// which HTTP answers 400 like any other token failure.
func isInvalidStateRef(err error) bool {
	return err != nil && ErrorKindOf(err) == ErrorKindInvalidStateRef
}

// ---------------------------------------------------------------------------
// Configuration and the turn budget
// ---------------------------------------------------------------------------

// StateRefInlineBytesFromEnv reads VGI_RPC_STATE_REF_INLINE_BYTES (default
// 16384). Anything but a positive integer is an error. getenv defaults to
// os.Getenv.
func StateRefInlineBytesFromEnv(getenv func(string) string) (int64, error) {
	raw := strings.TrimSpace(envLookup(getenv)(StateRefInlineBytesEnv))
	if raw == "" {
		return DefaultStateRefInlineBytes, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer, got %q", StateRefInlineBytesEnv, raw)
	}
	return v, nil
}

// TurnBudget returns the sealed bytes a cursor may take on a continuation:
// floor((maxRequestBytes - 4096) / 1.34) - callTokenBytes. ok is false when
// maxRequestBytes <= 0 (unbounded). The 1.34 converts the request's base64
// metadata size to sealed bytes; the budget may be negative under a tiny cap.
func TurnBudget(maxRequestBytes, callTokenBytes int64) (budget int64, ok bool) {
	if maxRequestBytes <= 0 {
		return 0, false
	}
	return (maxRequestBytes-TurnBudgetHeadroomBytes)*100/134 - callTokenBytes, true
}

// stateRefRuntime is one HTTP server's StateRef machinery: key, storage,
// budgets and cache. The byte-stream transports never have one, because
// their state never leaves the process.
type stateRefRuntime struct {
	tokenKey        []byte
	tokenTTL        time.Duration // 0 disables expiry
	storage         ObjectStore   // nil: refs stay inline, argument refs refused
	maxFetchBytes   int64
	maxRequestBytes int64 // 0: unbounded
	inlineCap       int64
	cursorRule      CursorRule
	cache           *ObjectCache
	now             func() time.Time
}

func (rt *stateRefRuntime) cursorRuleEnabled() bool {
	return rt.maxRequestBytes <= 0 || rt.maxRequestBytes >= CursorRuleFloorBytes
}

func (rt *stateRefRuntime) decompressCap() int64 {
	return max(int64(maxTokenPlaintextBytes), rt.maxFetchBytes)
}

// inlineBytes is state_ref_inline_bytes: min(cap, floor(turn_budget / 4)),
// floored at 0.
func (rt *stateRefRuntime) inlineBytes(callTokenBytes int64) int64 {
	budget, ok := TurnBudget(rt.maxRequestBytes, callTokenBytes)
	if !ok {
		return rt.inlineCap
	}
	return max(0, min(rt.inlineCap, budget/4))
}

// checkCursor applies the cursor rule to one sealed cursor. It returns the
// cursor_large warning text under the warn rule, a cursor_too_large error
// under the error rule, and neither when the cursor fits or the rule is off.
func (rt *stateRefRuntime) checkCursor(cursorBytes, callTokenBytes int64, protocol, method string) (string, error) {
	if !rt.cursorRuleEnabled() {
		return "", nil
	}
	budget, ok := TurnBudget(rt.maxRequestBytes, callTokenBytes)
	if !ok || cursorBytes <= budget {
		return "", nil
	}
	detail := fmt.Sprintf("stream state for %s.%s is %d bytes, over this server's turn budget of %d bytes "+
		"(VGI-Max-Request-Bytes %d). Wrap data that does not change across turns in ctx.state_ref().",
		protocol, method, cursorBytes, budget, rt.maxRequestBytes)
	if rt.cursorRule == CursorRuleError {
		slog.Warn("cursor_too_large: "+detail, "event", "cursor_too_large", "protocol", protocol,
			"method", method, "cursor_bytes", cursorBytes, "budget", budget)
		return "", stateRefError("CursorTooLargeError", ErrorKindCursorTooLarge, CodeResourceExhausted,
			"cursor_too_large: %s", detail)
	}
	slog.Warn("cursor_large: "+detail, "event", "cursor_large", "protocol", protocol,
		"method", method, "cursor_bytes", cursorBytes, "budget", budget)
	return "cursor_large: " + detail, nil
}

// ---------------------------------------------------------------------------
// Sealing
// ---------------------------------------------------------------------------

// sealBytes is the crypto envelope every protocol 3.4 kind uses:
// 0x01 || nonce24 || XChaCha20-Poly1305(plaintext, aad).
func sealBytes(plaintext, key, aad []byte) ([]byte, error) {
	aead, err := chacha20poly1305.NewX(normalizeTokenKey(key))
	if err != nil {
		return nil, fmt.Errorf("seal cipher: %w", err)
	}
	out := make([]byte, 1+stateTokenNonceLen, 1+stateTokenNonceLen+len(plaintext)+stateTokenTagLen)
	out[0] = sealVersion
	if _, err := rand.Read(out[1:]); err != nil {
		return nil, fmt.Errorf("seal nonce: %w", err)
	}
	return aead.Seal(out, out[1:1+stateTokenNonceLen], plaintext, aad), nil
}

var errSealOpen = errors.New("sealed envelope does not open")

// openBytes reverses sealBytes. Every failure is errSealOpen.
func openBytes(sealed, key, aad []byte) ([]byte, error) {
	if len(sealed) < stateTokenMinLen || sealed[0] != sealVersion {
		return nil, errSealOpen
	}
	aead, err := chacha20poly1305.NewX(normalizeTokenKey(key))
	if err != nil {
		return nil, errSealOpen
	}
	plain, err := aead.Open(nil, sealed[1:1+stateTokenNonceLen], sealed[1+stateTokenNonceLen:], aad)
	if err != nil {
		return nil, errSealOpen
	}
	return plain, nil
}

func stateRefTagAAD(base, callID []byte) []byte {
	return concatBytes([]byte(stateRefTagAADPrefix), base, callID)
}

func stateRefObjectAAD(base, callID []byte) []byte {
	return concatBytes([]byte(stateRefObjectAADPrefix), base, callID)
}

// ArgumentRefAAD returns ARGREF_AAD for auth: bound to the principal (and any
// peer-evidence binding), not to a protocol.
func ArgumentRefAAD(auth *AuthContext) []byte {
	return concatBytes([]byte(argRefAADPrefix), stateTokenAad(auth, serverTokenScope))
}

func concatBytes(parts ...[]byte) []byte {
	n := 0
	for _, p := range parts {
		n += len(p)
	}
	out := make([]byte, 0, n)
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// ---------------------------------------------------------------------------
// StateRef
// ---------------------------------------------------------------------------

// StateRef is an immutable value carried in stream state by reference.
//
// Create one with [CallContext.StateRef] while handling a stream's init
// request; store it in a field of your stream state; read it back with
// [CallContext.Resolve] on any turn. State transitions copy the field; they
// never call StateRef again. A StateRef is a small handle: copying it shares
// the same value.
//
// The value is held in memory until the state is first serialized into an
// HTTP state token. That serialization decides the form -- inline (a
// compressed payload inside the token) or stored (uploaded once, a sealed tag
// in the token) -- and caches the encoded bytes, so later serializations
// re-emit them exactly. A StateRef read back from a token holds only those
// bytes. On the byte-stream transports nothing is ever encoded.
//
// StateRef implements gob.GobEncoder/GobDecoder (HTTP state tokens are gob)
// and encoding.BinaryMarshaler/BinaryUnmarshaler (a binary Arrow column).
type StateRef struct {
	d *stateRefData
}

type stateRefData struct {
	mu      sync.Mutex
	value   []byte // in memory (nil once read back from a token)
	record  any    // the record the value was serialized from, if any
	encoded []byte // the decided form, once encoded
	stream  *stateRefStream
}

// IsZero reports whether r was never set.
func (r StateRef) IsZero() bool { return r.d == nil }

// Form is where the value lives: "memory" (not yet encoded), "inline",
// "stored", or "invalid" for bytes with an unknown form. A zero StateRef is
// "".
func (r StateRef) Form() string {
	if r.d == nil {
		return ""
	}
	r.d.mu.Lock()
	defer r.d.mu.Unlock()
	if r.d.encoded == nil {
		return "memory"
	}
	switch {
	case len(r.d.encoded) > 0 && r.d.encoded[0] == stateRefFormInline:
		return "inline"
	case len(r.d.encoded) > 0 && r.d.encoded[0] == stateRefFormStored:
		return "stored"
	default:
		return "invalid"
	}
}

// Encoded returns the encoded bytes once the form is decided, else nil.
func (r StateRef) Encoded() []byte {
	if r.d == nil {
		return nil
	}
	r.d.mu.Lock()
	defer r.d.mu.Unlock()
	if r.d.encoded == nil {
		return nil
	}
	return append([]byte(nil), r.d.encoded...)
}

// StateRefFromBytes rebuilds a StateRef from its encoded bytes (validated only
// when resolved).
func StateRefFromBytes(encoded []byte) StateRef {
	return StateRef{d: &stateRefData{encoded: append([]byte{}, encoded...)}}
}

// String describes the form and size, never the value.
func (r StateRef) String() string {
	if r.d == nil {
		return "StateRef(zero)"
	}
	r.d.mu.Lock()
	defer r.d.mu.Unlock()
	if r.d.encoded == nil {
		return fmt.Sprintf("StateRef(form=memory, bytes=%d)", len(r.d.value))
	}
	return fmt.Sprintf("StateRef(encoded_bytes=%d)", len(r.d.encoded))
}

// GobEncode implements gob.GobEncoder: the field's wire bytes, deciding the
// form on the first serialization inside an HTTP cursor mint.
func (r StateRef) GobEncode() ([]byte, error) { return r.wireBytes() }

// GobDecode implements gob.GobDecoder.
func (r *StateRef) GobDecode(data []byte) error { return r.setWire(data) }

// MarshalBinary implements encoding.BinaryMarshaler (same bytes as GobEncode).
func (r StateRef) MarshalBinary() ([]byte, error) { return r.wireBytes() }

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (r *StateRef) UnmarshalBinary(data []byte) error { return r.setWire(data) }

func (r *StateRef) setWire(data []byte) error {
	if len(data) == 0 {
		r.d = nil
		return nil
	}
	r.d = &stateRefData{encoded: append([]byte{}, data...)}
	return nil
}

// wireBytes returns the encoded form. Inside a cursor mint (the stream's
// encoder is armed) the form is decided, any store performed, and the result
// cached. Outside one -- a caller serializing a state for its own reasons --
// an un-encoded ref renders inline without caching, so the next mint still
// decides properly.
func (r StateRef) wireBytes() ([]byte, error) {
	d := r.d
	if d == nil {
		return []byte{}, nil
	}
	d.mu.Lock()
	if d.encoded != nil {
		out := d.encoded
		d.mu.Unlock()
		return out, nil
	}
	value, stream := d.value, d.stream
	d.mu.Unlock()
	var enc *stateRefEncoder
	if stream != nil {
		enc = stream.current()
	}
	if enc == nil {
		payload, err := codecPack(value)
		if err != nil {
			return nil, err
		}
		return append([]byte{stateRefFormInline}, payload...), nil
	}
	// No lock across the encode: it may upload, and a ref is only ever
	// un-encoded during its own stream's init turn, which one goroutine runs.
	encoded, err := encodeStateRefValue(value, enc)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	if d.encoded == nil {
		d.encoded = encoded
	}
	out := d.encoded
	d.mu.Unlock()
	return out, nil
}

// stateRefStream is the encoder slot for one HTTP stream's init turn. Refs
// created on that turn point at it; the server arms it while it serializes
// the cursor (and only then), so a ref's form is decided exactly at the first
// token mint.
type stateRefStream struct {
	mu  sync.Mutex
	enc *stateRefEncoder
}

type stateRefEncoder struct {
	rt          *stateRefRuntime
	base        []byte
	callID      []byte
	inlineBytes int64
	protocol    string
	method      string
}

func (s *stateRefStream) current() *stateRefEncoder {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enc
}

func (s *stateRefStream) arm(enc *stateRefEncoder) {
	s.mu.Lock()
	s.enc = enc
	s.mu.Unlock()
}

func newStateRefEncoder(rt *stateRefRuntime, auth *AuthContext, protocol, method string, callID []byte, callTokenBytes int64) *stateRefEncoder {
	return &stateRefEncoder{
		rt:          rt,
		base:        stateTokenAad(auth, protocol),
		callID:      callID,
		inlineBytes: rt.inlineBytes(callTokenBytes),
		protocol:    protocol,
		method:      method,
	}
}

// encodeStateRefValue decides a value's form and encodes it (§5.2), storing
// it when it is stored.
func encodeStateRefValue(value []byte, enc *stateRefEncoder) ([]byte, error) {
	payload, err := codecPack(value)
	if err != nil {
		return nil, err
	}
	rt := enc.rt
	if rt.storage == nil || int64(len(payload)) <= enc.inlineBytes {
		slog.Debug("state_ref.store", "event", "state_ref.store", "protocol", enc.protocol, "method", enc.method,
			"payload_bytes", len(payload), "object_bytes", 0, "form", "inline")
		return append([]byte{stateRefFormInline}, payload...), nil
	}
	objectBytes, err := sealBytes(payload, rt.tokenKey, stateRefObjectAAD(enc.base, enc.callID))
	if err != nil {
		return nil, err
	}
	if int64(len(objectBytes)) > rt.maxFetchBytes {
		return nil, stateRefTooLarge("StateRef object for %s.%s is %d bytes, over this server's max_fetch_bytes (%d); "+
			"no replica could fetch it back", enc.protocol, enc.method, len(objectBytes), rt.maxFetchBytes)
	}
	objectID, err := rt.storage.PutObject(objectBytes)
	if err != nil {
		return nil, stateRefError("StorageUnavailableError", ErrorKindStorageUnavailable, CodeUnavailable,
			"StateRef object store failed (%T)", err)
	}
	if n := len(objectID); n < 1 || n > maxObjectIDBytes || !utf8.ValidString(objectID) {
		return nil, fmt.Errorf("storage returned an object_id of %d bytes (must be 1-%d bytes of UTF-8)", n, maxObjectIDBytes)
	}
	digest := sha256.Sum256(objectBytes)
	tag := make([]byte, 0, stateRefTagFixed+len(objectID))
	tag = append(tag, stateRefTagVersion)
	tag = binary.LittleEndian.AppendUint64(tag, uint64(rt.now().Unix()))
	tag = binary.LittleEndian.AppendUint64(tag, uint64(len(objectBytes)))
	tag = append(tag, digest[:]...)
	tag = binary.LittleEndian.AppendUint16(tag, uint16(len(objectID)))
	tag = append(tag, objectID...)
	sealed, err := sealBytes(tag, rt.tokenKey, stateRefTagAAD(enc.base, enc.callID))
	if err != nil {
		return nil, err
	}
	slog.Debug("state_ref.store", "event", "state_ref.store", "protocol", enc.protocol, "method", enc.method,
		"payload_bytes", len(payload), "object_bytes", len(objectBytes), "form", "stored")
	return append([]byte{stateRefFormStored}, sealed...), nil
}

func unpackStateRefValue(payload []byte, cap int64) ([]byte, error) {
	out, err := codecUnpack(payload, cap)
	if err != nil {
		return nil, invalidStateRef("StateRef payload is malformed: %v", err)
	}
	return out, nil
}

// resolveEncodedStateRef resolves encoded StateRef bytes in one stream scope.
// rt is nil off HTTP (only inline refs resolve then).
func resolveEncodedStateRef(encoded []byte, rt *stateRefRuntime, base, callID []byte) ([]byte, error) {
	if len(encoded) == 0 {
		return nil, invalidStateRef("StateRef is empty")
	}
	switch encoded[0] {
	case stateRefFormInline:
		cap := int64(maxTokenPlaintextBytes)
		if rt != nil {
			cap = rt.decompressCap()
		}
		value, err := unpackStateRefValue(encoded[1:], cap)
		if err != nil {
			return nil, err
		}
		slog.Debug("state_ref.resolve", "event", "state_ref.resolve", "form", "inline", "cache", "miss", "bytes", len(value))
		return value, nil
	case stateRefFormStored:
		if rt == nil || base == nil || len(callID) == 0 {
			return nil, invalidStateRef("a stored StateRef cannot be resolved outside an HTTP stream turn")
		}
		return resolveStoredStateRef(encoded[1:], rt, base, callID)
	default:
		return nil, invalidStateRef("StateRef has unknown form 0x%02x", encoded[0])
	}
}

// resolveStoredStateRef resolves a stored ref's tag envelope (§5.3 step 2):
// open, layout, expiry and size checks all before any network I/O, then the
// object cache, then size, digest and the object's own seal.
func resolveStoredStateRef(envelope []byte, rt *stateRefRuntime, base, callID []byte) ([]byte, error) {
	tag, err := openBytes(envelope, rt.tokenKey, stateRefTagAAD(base, callID))
	if err != nil {
		return nil, invalidStateRef("StateRef tag does not open (tampered, or minted for another stream)")
	}
	if len(tag) < stateRefTagFixed || tag[0] != stateRefTagVersion {
		return nil, invalidStateRef("StateRef tag has an unknown version or a malformed layout")
	}
	createdAt := binary.LittleEndian.Uint64(tag[1:9])
	size := binary.LittleEndian.Uint64(tag[9:17])
	var digest [sha256.Size]byte
	copy(digest[:], tag[17:17+sha256.Size])
	idLen := int(binary.LittleEndian.Uint16(tag[17+sha256.Size:]))
	if idLen < 1 || idLen > maxObjectIDBytes || stateRefTagFixed+idLen != len(tag) {
		return nil, invalidStateRef("StateRef tag has a malformed layout")
	}
	objectID := string(tag[stateRefTagFixed:])
	if !utf8.ValidString(objectID) {
		return nil, invalidStateRef("StateRef tag has a malformed layout")
	}
	if rt.tokenTTL > 0 && rt.now().Unix()-int64(createdAt) > int64(rt.tokenTTL/time.Second) {
		return nil, invalidStateRef("StateRef expired")
	}
	if size > uint64(rt.maxFetchBytes) {
		return nil, stateRefTooLarge("StateRef object is %d bytes, over this server's max_fetch_bytes (%d)", size, rt.maxFetchBytes)
	}
	if rt.storage == nil {
		return nil, invalidStateRef("a stored StateRef cannot be resolved: this server has no object storage")
	}
	value, hit, err := rt.cache.GetOrFetch(objectID, func() ([]byte, error) {
		objectBytes, err := fetchObject(rt.storage, objectID, rt.maxFetchBytes, stateRefMissing)
		if err != nil {
			return nil, err
		}
		if uint64(len(objectBytes)) != size {
			return nil, invalidStateRef("StateRef object size mismatch")
		}
		if sha256.Sum256(objectBytes) != digest {
			return nil, invalidStateRef("StateRef object digest mismatch")
		}
		payload, err := openBytes(objectBytes, rt.tokenKey, stateRefObjectAAD(base, callID))
		if err != nil {
			return nil, invalidStateRef("StateRef object does not open")
		}
		return unpackStateRefValue(payload, rt.decompressCap())
	})
	if err != nil {
		return nil, err
	}
	slog.Debug("state_ref.resolve", "event", "state_ref.resolve", "form", "stored",
		"cache", hitLabel(hit), "bytes", len(value))
	return value, nil
}

func hitLabel(hit bool) string {
	if hit {
		return "hit"
	}
	return "miss"
}

// fetchObject is storage.Get with the error mapping both resolvers share: a
// missing object becomes missing(), any other failure storage_unavailable.
func fetchObject(storage ObjectStore, objectID string, maxBytes int64, missing func() *RpcError) ([]byte, error) {
	data, err := storage.Get(objectID, maxBytes)
	if err == nil {
		return data, nil
	}
	if IsObjectMissing(err) {
		return nil, missing()
	}
	var rpcErr *RpcError
	if errors.As(err, &rpcErr) && rpcErr.Kind != "" {
		return nil, rpcErr
	}
	return nil, stateRefError("StorageUnavailableError", ErrorKindStorageUnavailable, CodeUnavailable,
		"object storage fetch failed (%T): %s", err, redactErrorURLs(err.Error()))
}

// redactErrorURLs strips query strings from anything URL-shaped in msg, so a
// storage error never echoes a signed URL.
func redactErrorURLs(msg string) string {
	fields := strings.Fields(msg)
	for i, f := range fields {
		if strings.HasPrefix(f, "http://") || strings.HasPrefix(f, "https://") {
			fields[i] = redactExternalURL(strings.Trim(f, `"'(),`))
		}
	}
	return strings.Join(fields, " ")
}

// ---------------------------------------------------------------------------
// The per-request scope CallContext delegates to
// ---------------------------------------------------------------------------

// stateRefScope is the StateRef view of one request. The framework attaches
// one to every CallContext it builds. On HTTP it carries the server runtime
// and the stream's call id; on the byte-stream transports there is no runtime
// (values never leave memory). init is true only for the context handed to
// the stream method itself -- the one place StateRef() is allowed.
type stateRefScope struct {
	rt       *stateRefRuntime
	auth     *AuthContext
	protocol string
	method   string
	callID   []byte
	init     bool
	stream   *stateRefStream // HTTP init only
	memoMu   sync.Mutex
	memo     map[string]StateRef
}

func (s *stateRefScope) stateRef(value any) (StateRef, error) {
	if !s.init {
		return StateRef{}, stateRefError("StateRefMisuseError", ErrorKindStateRefMisuse, CodeFailedPrecondition,
			"state_ref() may only be called while handling a stream's init request (%s.%s)", s.protocol, s.method)
	}
	var data []byte
	var record any
	switch v := value.(type) {
	case []byte:
		data = append([]byte{}, v...)
	case string:
		data = []byte(v)
	case nil:
		return StateRef{}, fmt.Errorf("StateRef: nil value")
	default:
		var err error
		if m, ok := value.(encoding.BinaryMarshaler); ok {
			data, err = m.MarshalBinary()
		} else {
			data, err = MarshalRecord(value)
		}
		if err != nil {
			return StateRef{}, fmt.Errorf("StateRef: serializing %T: %w", value, err)
		}
		record = value
	}
	s.memoMu.Lock()
	defer s.memoMu.Unlock()
	if ref, ok := s.memo[string(data)]; ok {
		return ref, nil
	}
	ref := StateRef{d: &stateRefData{value: data, record: record, stream: s.stream}}
	if s.memo == nil {
		s.memo = make(map[string]StateRef)
	}
	s.memo[string(data)] = ref
	return ref, nil
}

func (s *stateRefScope) resolve(ref StateRef) ([]byte, error) {
	d := ref.d
	if d == nil {
		return nil, invalidStateRef("StateRef is empty")
	}
	d.mu.Lock()
	value, encoded := d.value, d.encoded
	d.mu.Unlock()
	if value != nil {
		return value, nil
	}
	var base []byte
	if s.rt != nil {
		base = stateTokenAad(s.auth, s.protocol)
	}
	return resolveEncodedStateRef(encoded, s.rt, base, s.callID)
}

func (s *stateRefScope) resolveInto(ref StateRef, out any) error {
	rv := reflect.ValueOf(out)
	if !rv.IsValid() || rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("ResolveInto: out must be a non-nil pointer, got %T", out)
	}
	if d := ref.d; d != nil {
		d.mu.Lock()
		record, inMemory := d.record, d.value != nil
		d.mu.Unlock()
		if inMemory && record != nil {
			// The ref still holds the original record: hand it back rather
			// than round-tripping it (treat it as immutable).
			recordValue := reflect.ValueOf(record)
			switch {
			case recordValue.Type() == rv.Elem().Type():
				rv.Elem().Set(recordValue)
				return nil
			case recordValue.Kind() == reflect.Ptr && recordValue.Elem().Type() == rv.Elem().Type():
				rv.Elem().Set(recordValue.Elem())
				return nil
			}
		}
	}
	data, err := s.resolve(ref)
	if err != nil {
		return err
	}
	switch o := out.(type) {
	case *[]byte:
		*o = data
		return nil
	case *string:
		*o = string(data)
		return nil
	case encoding.BinaryUnmarshaler:
		return o.UnmarshalBinary(data)
	default:
		return UnmarshalRecord(data, out)
	}
}

func (s *stateRefScope) resolveArgumentRef(ref []byte, size int64, sha256Hex string) ([]byte, error) {
	if s.rt == nil {
		return nil, invalidArgumentsRef("argument references can only be resolved by an HTTP server")
	}
	return ResolveArgumentRef(ref, size, sha256Hex, ArgumentRefOptions{
		TokenKey:      s.rt.tokenKey,
		Auth:          s.auth,
		Storage:       s.rt.storage,
		MaxFetchBytes: s.rt.maxFetchBytes,
		Cache:         s.rt.cache,
	})
}

// ---------------------------------------------------------------------------
// Argument references (STATE_REF_V1.md §10.2)
// ---------------------------------------------------------------------------

// ArgumentRef is the opened contents of an argument reference.
type ArgumentRef struct {
	Size     int64  // the sealed byte length
	SHA256   string // the sealed SHA-256, 64 lowercase hex
	ObjectID string // the storage id
}

// MintArgumentRef seals an argument reference for one granted upload, under
// ARGREF_AAD for auth (the __upload_url__/init request's identity).
//
// It is minted from the *claimed* (size, sha256) without seeing the bytes:
// storage enforces them on the PUT where it can, and the resolver verifies
// them on the GET. There is no expiry; on an unauthenticated server a
// reference is a bearer capability for as long as the object lives.
func MintArgumentRef(tokenKey []byte, auth *AuthContext, size int64, sha256Hex, objectID string) ([]byte, error) {
	if size < 1 {
		return nil, fmt.Errorf("argument ref size must be >= 1, got %d", size)
	}
	if !isLowerHexSHA256(sha256Hex) {
		return nil, fmt.Errorf("argument ref sha256 must be 64 lowercase hex characters")
	}
	if n := len(objectID); n < 1 || n > maxObjectIDBytes || !utf8.ValidString(objectID) {
		return nil, fmt.Errorf("object_id must be 1-%d bytes of UTF-8, got %d", maxObjectIDBytes, n)
	}
	digest, _ := hex.DecodeString(sha256Hex)
	plain := make([]byte, 0, argRefFixed+len(objectID))
	plain = append(plain, argRefVersion)
	plain = binary.LittleEndian.AppendUint64(plain, uint64(size))
	plain = append(plain, digest...)
	plain = binary.LittleEndian.AppendUint16(plain, uint16(len(objectID)))
	plain = append(plain, objectID...)
	sealed, err := sealBytes(plain, tokenKey, ArgumentRefAAD(auth))
	if err != nil {
		return nil, err
	}
	slog.Debug("arguments_ref.mint", "event", "arguments_ref.mint", "size", size)
	return sealed, nil
}

// OpenArgumentRef opens an argument reference sealed for auth. Every failure
// -- wrong key, principal or scope, tampering, unknown version, a malformed
// layout -- is invalid_arguments_ref.
func OpenArgumentRef(tokenKey []byte, auth *AuthContext, ref []byte) (ArgumentRef, error) {
	plain, err := openBytes(ref, tokenKey, ArgumentRefAAD(auth))
	if err != nil {
		return ArgumentRef{}, invalidArgumentsRef("argument reference does not open (tampered, or granted to another principal or server)")
	}
	if len(plain) < argRefFixed || plain[0] != argRefVersion {
		return ArgumentRef{}, invalidArgumentsRef("argument reference has an unknown version or a malformed layout")
	}
	size := binary.LittleEndian.Uint64(plain[1:9])
	digest := plain[9 : 9+sha256.Size]
	idLen := int(binary.LittleEndian.Uint16(plain[9+sha256.Size:]))
	if idLen < 1 || idLen > maxObjectIDBytes || argRefFixed+idLen != len(plain) || size > 1<<62 {
		return ArgumentRef{}, invalidArgumentsRef("argument reference has a malformed layout")
	}
	objectID := string(plain[argRefFixed:])
	if !utf8.ValidString(objectID) {
		return ArgumentRef{}, invalidArgumentsRef("argument reference has a malformed layout")
	}
	return ArgumentRef{Size: int64(size), SHA256: hex.EncodeToString(digest), ObjectID: objectID}, nil
}

// ArgumentRefOptions is what [ResolveArgumentRef] needs from the server.
type ArgumentRefOptions struct {
	// TokenKey is the server's AEAD key (shared by every replica).
	TokenKey []byte
	// Auth is the presenting request's authentication context.
	Auth *AuthContext
	// Storage is the server's object store; nil refuses every reference.
	Storage ObjectStore
	// MaxFetchBytes is the server's inbound fetch cap (<= 0: 256 MiB).
	MaxFetchBytes int64
	// Cache is the object cache; nil uses [DefaultObjectCache].
	Cache *ObjectCache
}

// ResolveArgumentRef opens, fetches (through the object cache) and verifies
// an argument reference: the library primitive the vgi layer resolves
// BindRequest.arguments_location with.
//
// Order: validate the claimed (size, sha256); open ref under ARGREF_AAD for
// opts.Auth; check the sealed size and SHA-256 equal the claimed ones; look
// the object_id up in the object cache; on a miss, single-flight
// opts.Storage.Get; verify length and SHA-256; return. Failures carry the
// kinds invalid_arguments_ref (malformed claim, no storage, a ref that does
// not open for this caller, sealed values that differ from the claim),
// arguments_ref_integrity (the fetched bytes differ), arguments_ref_missing
// (storage reports the object gone) and storage_unavailable (transient).
// The returned slice is shared with the cache: treat it as read-only.
func ResolveArgumentRef(ref []byte, size int64, sha256Hex string, opts ArgumentRefOptions) ([]byte, error) {
	if size < 1 {
		return nil, invalidArgumentsRef("argument location size must be an integer >= 1, got %d", size)
	}
	if !isLowerHexSHA256(sha256Hex) {
		return nil, invalidArgumentsRef("argument location sha256 must be 64 lowercase hex characters")
	}
	if opts.Storage == nil {
		return nil, invalidArgumentsRef("this server has no object storage configured to resolve argument references")
	}
	opened, err := OpenArgumentRef(opts.TokenKey, opts.Auth, ref)
	if err != nil {
		return nil, err
	}
	if opened.Size != size || opened.SHA256 != sha256Hex {
		return nil, invalidArgumentsRef("argument reference does not match the size and sha256 it was sent with")
	}
	maxFetch := opts.MaxFetchBytes
	if maxFetch <= 0 {
		maxFetch = defaultMaxFetchBytes
	}
	if size > maxFetch {
		return nil, invalidArgumentsRef("argument object is %d bytes, over this server's max_fetch_bytes (%d)", size, maxFetch)
	}
	cache := opts.Cache
	if cache == nil {
		cache = DefaultObjectCache()
	}
	expected, _ := hex.DecodeString(sha256Hex)
	value, hit, err := cache.GetOrFetch(opened.ObjectID, func() ([]byte, error) {
		data, err := fetchObject(opts.Storage, opened.ObjectID, maxFetch, argumentsRefMissing)
		if err != nil {
			return nil, err
		}
		if int64(len(data)) != size {
			return nil, argumentsRefIntegrity("argument object size mismatch: expected %d, got %d", size, len(data))
		}
		if sum := sha256.Sum256(data); !bytes.Equal(sum[:], expected) {
			return nil, argumentsRefIntegrity("argument object sha256 mismatch")
		}
		return data, nil
	})
	if err != nil {
		return nil, err
	}
	if int64(len(value)) != size {
		// A cached value passed the checks above for this object id; a length
		// change means the id was reused, which storage never does.
		return nil, argumentsRefIntegrity("argument object size mismatch: expected %d, got %d", size, len(value))
	}
	slog.Debug("arguments_ref.resolve", "event", "arguments_ref.resolve", "cache", hitLabel(hit), "bytes", len(value))
	return value, nil
}
