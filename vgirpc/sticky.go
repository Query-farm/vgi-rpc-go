// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/crypto/chacha20poly1305"
)

// Sticky session token format mirrors Python's vgi_rpc/http/server/_sticky.py:
//
//   wire:      version:u8(1) | nonce:bytes(24) | ciphertext+tag
//   plaintext: created_at:u64 LE | server_id_len:u8 | server_id_bytes |
//              session_id:bytes(12) | expires_at:u64 LE
//   AAD:       same as stream tokens — b"vgi_rpc.state.v4\x00" + principal tail
//   encoding:  base64url, no padding
const (
	sessionTokenVersion = 0x01
	sessionIDLen        = 12 // bytes; 24 hex chars
	stickyDefaultTTLSec = 300
)

// SessionLostReason* enumerates the reasons _open_session_token can fail;
// all are reported to the client as "session_lost" so a stale or stolen
// token cannot be used to probe whether a session exists.
const (
	sessionLostMalformed    = "malformed session token"
	sessionLostVerification = "session token verification failed"
	sessionLostWrongWorker  = "session token was issued by a different worker (server_id mismatch)"
	sessionLostNotFound     = "session not found, expired, or principal mismatch"
)

// sealSessionToken builds the wire-format sticky session token.
func sealSessionToken(tokenKey []byte, serverID string, sessionID [sessionIDLen]byte, expiresAt int64, aad []byte, now int64) (string, error) {
	sidBytes := []byte(serverID)
	if len(sidBytes) > 255 {
		return "", fmt.Errorf("server_id too long (%d bytes); max 255", len(sidBytes))
	}
	if now == 0 {
		now = time.Now().Unix()
	}
	// Layout: created_at:u64 | server_id_len:u8 | server_id | session_id:12 | expires_at:u64
	plaintext := make([]byte, 0, 8+1+len(sidBytes)+sessionIDLen+8)
	plaintext = binary.LittleEndian.AppendUint64(plaintext, uint64(now))
	plaintext = append(plaintext, byte(len(sidBytes)))
	plaintext = append(plaintext, sidBytes...)
	plaintext = append(plaintext, sessionID[:]...)
	plaintext = binary.LittleEndian.AppendUint64(plaintext, uint64(expiresAt))

	aead, err := chacha20poly1305.NewX(normalizeTokenKey(tokenKey))
	if err != nil {
		return "", fmt.Errorf("sticky cipher init: %w", err)
	}
	nonce := make([]byte, chacha20poly1305.NonceSizeX)
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("sticky nonce: %w", err)
	}
	ciphertext := aead.Seal(nil, nonce, plaintext, aad)

	wire := make([]byte, 0, 1+len(nonce)+len(ciphertext))
	wire = append(wire, sessionTokenVersion)
	wire = append(wire, nonce...)
	wire = append(wire, ciphertext...)

	// base64url, no padding
	return base64.RawURLEncoding.EncodeToString(wire), nil
}

// openSessionToken decodes a wire-format session token and returns the
// (server_id, session_id, expires_at) triple. Every failure path maps to
// SessionLostError so callers cannot distinguish malformed input from a
// cross-principal replay attempt.
func openSessionToken(token string, tokenKey []byte, aad []byte) (string, [sessionIDLen]byte, int64, error) {
	var zero [sessionIDLen]byte
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		// Tolerate accidental padding from clients that don't strip it.
		raw, err = base64.URLEncoding.DecodeString(token)
		if err != nil {
			return "", zero, 0, &SessionLostError{Reason: sessionLostMalformed}
		}
	}
	if len(raw) < 1+chacha20poly1305.NonceSizeX+chacha20poly1305.Overhead {
		return "", zero, 0, &SessionLostError{Reason: sessionLostMalformed}
	}
	if raw[0] != sessionTokenVersion {
		return "", zero, 0, &SessionLostError{Reason: sessionLostMalformed}
	}
	nonce := raw[1 : 1+chacha20poly1305.NonceSizeX]
	ciphertext := raw[1+chacha20poly1305.NonceSizeX:]

	aead, err := chacha20poly1305.NewX(normalizeTokenKey(tokenKey))
	if err != nil {
		return "", zero, 0, &SessionLostError{Reason: sessionLostVerification}
	}
	plaintext, err := aead.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return "", zero, 0, &SessionLostError{Reason: sessionLostVerification}
	}

	if len(plaintext) < 8+1 {
		return "", zero, 0, &SessionLostError{Reason: sessionLostMalformed}
	}
	_ = binary.LittleEndian.Uint64(plaintext[0:8]) // created_at; not used by server
	sidLen := int(plaintext[8])
	headerEnd := 9 + sidLen
	if len(plaintext) != headerEnd+sessionIDLen+8 {
		return "", zero, 0, &SessionLostError{Reason: sessionLostMalformed}
	}
	serverID := string(plaintext[9:headerEnd])
	var sid [sessionIDLen]byte
	copy(sid[:], plaintext[headerEnd:headerEnd+sessionIDLen])
	expiresAt := int64(binary.LittleEndian.Uint64(plaintext[headerEnd+sessionIDLen:]))
	return serverID, sid, expiresAt, nil
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

// sessionEntry is one live session in the per-worker registry. Mirrors
// Python _SessionEntry: state, expiry, principal binding, per-session lock.
type sessionEntry struct {
	state        any
	expiresAt    time.Time
	principalKey string
	// lock serializes same-session concurrent calls. Acquired in the
	// HTTP dispatch path before invoking the handler; released after the
	// response is generated. Different-session calls run in parallel.
	lock sync.Mutex
}

// sessionRegistry is the per-worker in-process dict of live sticky sessions.
// Safe for concurrent use. Eviction is TTL-driven via the reaper goroutine
// and inline on get() when an expired entry is observed.
type sessionRegistry struct {
	defaultTTL  time.Duration
	mu          sync.Mutex
	entries     map[[sessionIDLen]byte]*sessionEntry
	draining    bool
	reaperStop  chan struct{}
	reaperOnce  sync.Once
	reaperWg    sync.WaitGroup
	reaperTick  time.Duration
}

// newSessionRegistry constructs an empty registry with the given default TTL.
// The reaper goroutine is started lazily on first use.
func newSessionRegistry(defaultTTL time.Duration) *sessionRegistry {
	if defaultTTL <= 0 {
		defaultTTL = stickyDefaultTTLSec * time.Second
	}
	return &sessionRegistry{
		defaultTTL: defaultTTL,
		entries:    make(map[[sessionIDLen]byte]*sessionEntry),
		reaperStop: make(chan struct{}),
		reaperTick: time.Second,
	}
}

// DefaultTTL returns the registry's default session TTL.
func (r *sessionRegistry) DefaultTTL() time.Duration { return r.defaultTTL }

// IsDraining reports whether the drain flag is set.
func (r *sessionRegistry) IsDraining() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.draining
}

// SetDraining flips the drain flag. Existing-session calls continue to
// serve; new ctx.OpenSession() calls return *ServerDrainingError.
func (r *sessionRegistry) SetDraining(v bool) {
	r.mu.Lock()
	r.draining = v
	r.mu.Unlock()
}

// open registers state and returns the new session id, the absolute
// expiry, and the live entry pointer. Returns *ServerDrainingError when
// the registry is draining. Callers MUST use the returned entry pointer
// directly rather than re-`get`ing by id — a tight TTL or a racy reaper
// can otherwise evict the entry between open and get and surface a
// confusing "session disappeared" error.
func (r *sessionRegistry) open(state any, ttl time.Duration, principalKey string) ([sessionIDLen]byte, time.Time, *sessionEntry, error) {
	var zero [sessionIDLen]byte
	if ttl <= 0 {
		ttl = r.defaultTTL
	}
	expiresAt := time.Now().Add(ttl)
	entry := &sessionEntry{
		state:        state,
		expiresAt:    expiresAt,
		principalKey: principalKey,
	}
	var sid [sessionIDLen]byte
	if _, err := rand.Read(sid[:]); err != nil {
		return zero, time.Time{}, nil, fmt.Errorf("sticky session id: %w", err)
	}
	r.mu.Lock()
	if r.draining {
		r.mu.Unlock()
		return zero, time.Time{}, nil, &ServerDrainingError{}
	}
	r.entries[sid] = entry
	r.mu.Unlock()
	return sid, expiresAt, entry, nil
}

// get looks up an entry by id, evicting in-line on TTL expiry and returning
// nil on miss / expiry / cross-principal lookup. AAD already binds the
// token to its principal at the crypto layer; the principalKey check is
// defense-in-depth.
func (r *sessionRegistry) get(sid [sessionIDLen]byte, principalKey string) *sessionEntry {
	now := time.Now()
	r.mu.Lock()
	entry, ok := r.entries[sid]
	if !ok {
		r.mu.Unlock()
		return nil
	}
	if entry.expiresAt.Before(now) {
		delete(r.entries, sid)
		r.mu.Unlock()
		closeSessionState(entry.state)
		return nil
	}
	if entry.principalKey != principalKey {
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()
	return entry
}

// close removes the entry and invokes state.Close() if defined. Returns
// true on hit.
func (r *sessionRegistry) close(sid [sessionIDLen]byte) bool {
	r.mu.Lock()
	entry, ok := r.entries[sid]
	if ok {
		delete(r.entries, sid)
	}
	r.mu.Unlock()
	if !ok {
		return false
	}
	closeSessionState(entry.state)
	return true
}

// drainExpired evicts entries past their TTL. Returns the eviction count.
func (r *sessionRegistry) drainExpired(now time.Time) int {
	r.mu.Lock()
	var expired []*sessionEntry
	for sid, entry := range r.entries {
		if entry.expiresAt.Before(now) {
			expired = append(expired, entry)
			delete(r.entries, sid)
		}
	}
	r.mu.Unlock()
	for _, entry := range expired {
		closeSessionState(entry.state)
	}
	return len(expired)
}

// shutdown invokes state.Close() on every live session and clears the registry.
// Called by drain handle's Shutdown for graceful operator-driven teardown.
// Does NOT fire on process crash — same contract as Python.
func (r *sessionRegistry) shutdown() {
	r.mu.Lock()
	entries := make([]*sessionEntry, 0, len(r.entries))
	for _, e := range r.entries {
		entries = append(entries, e)
	}
	r.entries = make(map[[sessionIDLen]byte]*sessionEntry)
	r.mu.Unlock()
	for _, e := range entries {
		closeSessionState(e.state)
	}
}

// ensureReaper starts the daemon goroutine that ticks the TTL sweep.
// Idempotent — only the first call spawns the goroutine.
func (r *sessionRegistry) ensureReaper() {
	r.reaperOnce.Do(func() {
		r.reaperWg.Add(1)
		go r.reaperLoop()
	})
}

// stopReaper signals the reaper to exit and waits for it. Idempotent.
// Called by Shutdown so the reaper goroutine exits cleanly when an
// operator has finished their grace period.
func (r *sessionRegistry) stopReaper() {
	select {
	case <-r.reaperStop:
		// Already stopped.
		return
	default:
	}
	close(r.reaperStop)
	r.reaperWg.Wait()
}

func (r *sessionRegistry) reaperLoop() {
	defer r.reaperWg.Done()
	t := time.NewTicker(r.reaperTick)
	defer t.Stop()
	for {
		select {
		case <-r.reaperStop:
			return
		case now := <-t.C:
			func() {
				defer func() {
					if rv := recover(); rv != nil {
						slog.Debug("sticky reaper tick panicked", "panic", rv)
					}
				}()
				r.drainExpired(now)
			}()
		}
	}
}

// closeSessionState invokes Close() on the state object if it satisfies
// the io.Closer-style interface. Panics and errors are suppressed so
// eviction is never blocked by a misbehaving state.
func closeSessionState(state any) {
	defer func() {
		if rv := recover(); rv != nil {
			slog.Debug("sticky session state close panicked", "panic", rv)
		}
	}()
	if closer, ok := state.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			slog.Debug("sticky session state close failed", "err", err)
		}
		return
	}
	if closer, ok := state.(interface{ Close() }); ok {
		closer.Close()
	}
}

// ---------------------------------------------------------------------------
// Drain handle (operator-facing)
// ---------------------------------------------------------------------------

// DrainHandle is the operator-facing handle returned by
// [HttpServer.DrainHandle] when sticky sessions are enabled. Mirrors
// Python's DrainHandle: Drain flips the flag so subsequent OpenSession
// calls raise ServerDrainingError; Shutdown invokes state.Close() on
// every live session after the grace period.
type DrainHandle struct {
	registry *sessionRegistry
}

// Drain sets the registry's drain flag. New ctx.OpenSession() calls will
// return *ServerDrainingError. Existing-session calls continue to serve.
func (h *DrainHandle) Drain() {
	if h == nil || h.registry == nil {
		return
	}
	h.registry.SetDraining(true)
}

// ClearDrain clears the drain flag — used by the conformance harness to
// reset state between tests. Operators typically don't call this in
// production.
func (h *DrainHandle) ClearDrain() {
	if h == nil || h.registry == nil {
		return
	}
	h.registry.SetDraining(false)
}

// IsDraining reports whether the drain flag is currently set.
func (h *DrainHandle) IsDraining() bool {
	if h == nil || h.registry == nil {
		return false
	}
	return h.registry.IsDraining()
}

// Shutdown invokes state.Close() on every live session and clears the
// registry. Operators call this after the grace period elapses. Also
// stops the registry's reaper goroutine so the worker can exit cleanly
// — subsequent operations on a shut-down registry are no-ops.
func (h *DrainHandle) Shutdown() {
	if h == nil || h.registry == nil {
		return
	}
	h.registry.shutdown()
	h.registry.stopReaper()
}
