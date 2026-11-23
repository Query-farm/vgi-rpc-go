// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"compress/flate"
	"crypto/rand"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Language-local StateRef tests (STATE_REF_V1.md §15, last paragraph): the
// codec tag on the seal path, that a stored ref re-serializes to identical
// bytes without a second PutObject, the tag's own expiry check (which the
// shared suite cannot tell from call-token expiry), and that argument refs do
// not cross peer-evidence bindings. Everything observable over the wire is in
// the shared suite (vgi_rpc.conformance._state_ref_pytest).

// memObjectStore is an in-memory ObjectStore counting calls.
type memObjectStore struct {
	mu   sync.Mutex
	objs map[string][]byte
	puts atomic.Int64
	gets atomic.Int64
	gate chan struct{} // when set, Get blocks until it is closed
}

func newMemObjectStore() *memObjectStore { return &memObjectStore{objs: map[string][]byte{}} }

func (m *memObjectStore) PutObject(data []byte) (string, error) {
	n := m.puts.Add(1)
	id := fmt.Sprintf("obj-%d", n)
	m.mu.Lock()
	m.objs[id] = append([]byte(nil), data...)
	m.mu.Unlock()
	return id, nil
}

func (m *memObjectStore) Get(id string, maxBytes int64) ([]byte, error) {
	m.gets.Add(1)
	if m.gate != nil {
		<-m.gate
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objs[id]
	if !ok {
		return nil, fmt.Errorf("gone: %w", ErrObjectMissing)
	}
	return data, nil
}

func testRuntime(store ObjectStore, now func() time.Time) *stateRefRuntime {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	return &stateRefRuntime{
		tokenKey: key, tokenTTL: DefaultTokenTTL, storage: store, maxFetchBytes: defaultMaxFetchBytes,
		inlineCap: DefaultStateRefInlineBytes, cursorRule: CursorRuleWarn, cache: NewObjectCache(1 << 20), now: now,
	}
}

func incompressible(n int) []byte {
	out := make([]byte, n)
	_, _ = rand.Read(out)
	return out
}

func TestTokenCodecTags(t *testing.T) {
	large := bytes.Repeat([]byte("vgi-rpc StateRef codec "), 512)
	packed, err := codecPack(large)
	if err != nil || packed[0] != tokenCodecZstd || len(packed) >= len(large)/4 {
		t.Fatalf("a large compressible payload must pack as zstd: tag=%#x len=%d err=%v", packed[0], len(packed), err)
	}
	tiny, _ := codecPack([]byte("abc"))
	if !bytes.Equal(tiny, []byte{tokenCodecRaw, 'a', 'b', 'c'}) {
		t.Fatalf("a tiny payload must stay raw: %x", tiny)
	}
	var deflated bytes.Buffer
	fw, _ := flate.NewWriter(&deflated, flate.BestSpeed)
	_, _ = fw.Write(large)
	_ = fw.Close()
	got, err := codecUnpack(append([]byte{tokenCodecDeflate}, deflated.Bytes()...), maxTokenPlaintextBytes)
	if err != nil || !bytes.Equal(got, large) {
		t.Fatalf("a DEFLATE payload must open: err=%v", err)
	}
	if _, err := codecUnpack([]byte{0x7f, 1, 2}, maxTokenPlaintextBytes); err == nil {
		t.Fatal("an unknown codec tag must be refused")
	}
	// A zstd frame declaring more than the bound is refused before decoding.
	if _, err := codecUnpack(packed, int64(len(large)-1)); err == nil {
		t.Fatal("a frame decompressing past the bound must be refused")
	}
}

func TestStoredStateRefSerializesOnceAndResolves(t *testing.T) {
	store := newMemObjectStore()
	rt := testRuntime(store, time.Now)
	stream := &stateRefStream{}
	scope := &stateRefScope{rt: rt, protocol: "P", method: "m", init: true, stream: stream}
	value := incompressible(40000)
	ref, err := scope.stateRef(value)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := scope.stateRef(append([]byte(nil), value...))
	if again.d != ref.d {
		t.Fatal("byte-equal values within one init must share one ref")
	}
	callID := []byte("0123456789abcdef")
	stream.arm(newStateRefEncoder(rt, nil, "P", "m", callID, 0))
	first, err := ref.GobEncode()
	stream.arm(nil)
	if err != nil {
		t.Fatal(err)
	}
	if first[0] != stateRefFormStored || ref.Form() != "stored" {
		t.Fatalf("an over-limit value with storage must be stored, got form %q", ref.Form())
	}
	second, _ := ref.GobEncode()
	if !bytes.Equal(first, second) || store.puts.Load() != 1 {
		t.Fatalf("re-serialization must emit identical bytes without a second put (puts=%d)", store.puts.Load())
	}

	// Through gob, as a cursor carries it, and resolved by a fresh scope.
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(struct{ R StateRef }{ref}); err != nil {
		t.Fatal(err)
	}
	var decoded struct{ R StateRef }
	if err := gob.NewDecoder(&buf).Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	turn := &stateRefScope{rt: rt, protocol: "P", method: "m", callID: callID}
	got, err := turn.resolve(decoded.R)
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("stored ref must resolve: err=%v", err)
	}
	if _, err := turn.resolve(decoded.R); err != nil || store.gets.Load() != 1 {
		t.Fatalf("second resolve must hit the object cache (gets=%d, err=%v)", store.gets.Load(), err)
	}
	other := &stateRefScope{rt: rt, protocol: "P", method: "m", callID: []byte("fedcba9876543210")}
	if _, err := other.resolve(decoded.R); ErrorKindOf(err) != ErrorKindInvalidStateRef {
		t.Fatalf("a tag from another stream must be invalid_state_ref, got %v", err)
	}
	if _, err := (&stateRefScope{callID: callID}).resolve(decoded.R); ErrorKindOf(err) != ErrorKindInvalidStateRef {
		t.Fatalf("a stored ref off HTTP must be invalid_state_ref, got %v", err)
	}
}

func TestStateRefTagExpiryIsCheckedBeforeAnyGet(t *testing.T) {
	store := newMemObjectStore()
	clock := time.Now()
	rt := testRuntime(store, func() time.Time { return clock })
	rt.tokenTTL = time.Minute
	callID := []byte("0123456789abcdef")
	encoded, err := encodeStateRefValue(incompressible(40000), newStateRefEncoder(rt, nil, "P", "m", callID, 0))
	if err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(2 * time.Minute)
	_, err = resolveEncodedStateRef(encoded, rt, stateTokenAad(nil, "P"), callID)
	if ErrorKindOf(err) != ErrorKindInvalidStateRef || store.gets.Load() != 0 {
		t.Fatalf("an expired tag must be refused with no get: err=%v gets=%d", err, store.gets.Load())
	}
}

func TestStateRefInlineWithoutStorageAndOffHTTP(t *testing.T) {
	rt := testRuntime(nil, time.Now)
	stream := &stateRefStream{}
	scope := &stateRefScope{rt: rt, protocol: "P", method: "m", init: true, stream: stream}
	value := incompressible(40000)
	ref, _ := scope.stateRef(value)
	stream.arm(newStateRefEncoder(rt, nil, "P", "m", []byte("0123456789abcdef"), 0))
	encoded, _ := ref.GobEncode()
	stream.arm(nil)
	if encoded[0] != stateRefFormInline {
		t.Fatal("with no storage an over-limit value stays inline")
	}
	// Byte-stream transports: the value stays in memory, never encoded.
	pipe := &stateRefScope{init: true}
	mem, _ := pipe.stateRef([]byte("x"))
	if mem.Form() != "memory" {
		t.Fatalf("off HTTP a ref is never encoded, got %q", mem.Form())
	}
	if _, err := (&stateRefScope{}).stateRef([]byte("x")); ErrorKindOf(err) != ErrorKindStateRefMisuse {
		t.Fatalf("state_ref outside init must be state_ref_misuse, got %v", err)
	}
}

func TestObjectCacheSingleFlightBoundAndAge(t *testing.T) {
	cache := NewObjectCache(100)
	var fetches atomic.Int64
	gate := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _, err := cache.GetOrFetch("a", func() ([]byte, error) {
				fetches.Add(1)
				<-gate
				return make([]byte, 60), nil
			})
			if err != nil || len(v) != 60 {
				t.Errorf("join failed: %v", err)
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(gate)
	wg.Wait()
	if fetches.Load() != 1 {
		t.Fatalf("concurrent resolves must share one fetch, got %d", fetches.Load())
	}
	_, _, _ = cache.GetOrFetch("b", func() ([]byte, error) { return make([]byte, 60), nil })
	if cache.Contains("a") || cache.Stats().ResidentBytes != 60 || cache.Stats().Evictions != 1 {
		t.Fatalf("the byte bound must evict the LRU entry: %+v", cache.Stats())
	}
	_, _, _ = cache.GetOrFetch("big", func() ([]byte, error) { return make([]byte, 200), nil })
	if cache.Contains("big") {
		t.Fatal("a value over the bound must not be retained")
	}
	if cache.AgeEntries(ObjectCacheMaxAge+time.Second) != 1 || cache.Contains("b") {
		t.Fatal("an entry older than one day must be a miss")
	}
}

func TestArgumentRefsDoNotCrossPeerBindings(t *testing.T) {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	sum := sha256.Sum256([]byte("arg"))
	digest := hex.EncodeToString(sum[:])
	bound := &AuthContext{Domain: "d", Authenticated: true, Principal: "p",
		Claims: map[string]any{"peer_evidence_binding": "binding-1"}}
	ref, err := MintArgumentRef(key, bound, 3, digest, "obj")
	if err != nil {
		t.Fatal(err)
	}
	if opened, err := OpenArgumentRef(key, bound, ref); err != nil || opened.ObjectID != "obj" || opened.SHA256 != digest {
		t.Fatalf("same principal and binding must open: %+v %v", opened, err)
	}
	unbound := &AuthContext{Domain: "d", Authenticated: true, Principal: "p"}
	if _, err := OpenArgumentRef(key, unbound, ref); ErrorKindOf(err) != ErrorKindInvalidArgumentsRef {
		t.Fatalf("a ref must not open without its peer-evidence binding, got %v", err)
	}
	other := &AuthContext{Domain: "d", Authenticated: true, Principal: "p",
		Claims: map[string]any{"peer_evidence_binding": "binding-2"}}
	if _, err := OpenArgumentRef(key, other, ref); ErrorKindOf(err) != ErrorKindInvalidArgumentsRef {
		t.Fatalf("a ref must not open under another binding, got %v", err)
	}
}

func TestTurnBudgetAndInlineLimit(t *testing.T) {
	if _, ok := TurnBudget(0, 100); ok {
		t.Fatal("no request cap means an unbounded budget")
	}
	if b, _ := TurnBudget(32768, 1000); b != (32768-4096)*100/134-1000 {
		t.Fatalf("budget formula: got %d", b)
	}
	rt := &stateRefRuntime{maxRequestBytes: 32768, inlineCap: DefaultStateRefInlineBytes}
	if got := rt.inlineBytes(0); got != ((32768-4096)*100/134)/4 {
		t.Fatalf("inline limit is capped at a quarter of the budget, got %d", got)
	}
	floor := &stateRefRuntime{maxRequestBytes: 9000, cursorRule: CursorRuleError}
	if w, err := floor.checkCursor(1<<20, 0, "P", "m"); w != "" || err != nil {
		t.Fatal("below the 16 KiB floor the cursor rule is off")
	}
	strict := &stateRefRuntime{maxRequestBytes: 32768, cursorRule: CursorRuleError}
	if _, err := strict.checkCursor(1<<20, 0, "P", "m"); ErrorKindOf(err) != ErrorKindCursorTooLarge {
		t.Fatalf("over budget under the error rule is cursor_too_large, got %v", err)
	}
}

func TestAdvertisedUploadCapNeverExceedsFetchCap(t *testing.T) {
	h := NewHttpServer(NewServer())
	if got := h.effectiveMaxUploadBytes(); got != defaultMaxFetchBytes {
		t.Fatalf("unset upload cap advertises the fetch cap, got %d", got)
	}
	h.SetMaxUploadBytes(1 << 20)
	if got := h.effectiveMaxUploadBytes(); got != 1<<20 {
		t.Fatalf("an upload cap under the fetch cap is advertised as set, got %d", got)
	}
	h.SetMaxUploadBytes(1 << 40)
	if got := h.effectiveMaxUploadBytes(); got != defaultMaxFetchBytes {
		t.Fatalf("an upload cap over the fetch cap is clamped, got %d", got)
	}
}

func TestMaxFetchBytesFromEnv(t *testing.T) {
	env := func(kv map[string]string) func(string) string { return func(k string) string { return kv[k] } }
	if v, err := MaxFetchBytesFromEnv(env(nil)); err != nil || v != defaultMaxFetchBytes {
		t.Fatalf("default: %d %v", v, err)
	}
	if v, err := MaxFetchBytesFromEnv(env(map[string]string{EnvMaxUploadBytes: "1073741824"})); err != nil || v != 1<<30 {
		t.Fatalf("an unset fetch cap rises to the upload cap: %d %v", v, err)
	}
	if _, err := MaxFetchBytesFromEnv(env(map[string]string{EnvMaxUploadBytes: "2048", EnvMaxFetchBytes: "1024"})); err == nil {
		t.Fatal("an explicit fetch cap below the upload cap must refuse startup")
	}
}
