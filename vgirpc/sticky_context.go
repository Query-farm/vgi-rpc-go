// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"encoding/hex"
	"fmt"
	"time"
)

// stickySink is the per-request bridge from [CallContext.OpenSession] /
// [CallContext.CloseSession] back into the HTTP middleware. It captures
// the new minted token (so [process_response] knows to set VGI-Session
// on the way out) and the close intent (so the middleware emits
// VGI-Session-Close: true). Mirrors Python's _StickySink in
// vgi_rpc/http/server/_sticky.py.
//
// When an HTTP request enters with a valid VGI-Session token, the
// middleware resolves it eagerly and pre-populates entry / sessionID /
// sessionState so handlers see them on CallContext immediately. When
// the request has no token but VGI-Session-Accept: true, the sink is
// in "accept opens" mode and ctx.OpenSession() succeeds.
type stickySink struct {
	// Configured on entry by HTTP middleware:
	registry    *sessionRegistry
	tokenKey    []byte
	echoHeaders map[string]string
	serverID    string
	auth        *AuthContext
	acceptOpens bool // VGI-Session-Accept: true on the request
	transport   TransportKind

	// Pre-populated by middleware when an existing session was resumed:
	entry      *sessionEntry // the resumed entry, nil when no token presented
	sessionID  [sessionIDLen]byte
	hasSession bool

	// Mutated during dispatch by CallContext.OpenSession / CloseSession:
	mintedToken  string // base64url; emitted as VGI-Session on the response
	closed       bool   // true after CloseSession; emits VGI-Session-Close: true
	sessionIDHex string // 24-char hex; for access-log / debugging
}

// installResumed pre-populates the sink with an existing session entry.
// Called by the middleware when the request carries a valid VGI-Session
// token that resolves to a registry entry.
func (s *stickySink) installResumed(entry *sessionEntry, sid [sessionIDLen]byte) {
	s.entry = entry
	s.sessionID = sid
	s.hasSession = true
	s.sessionIDHex = hex.EncodeToString(sid[:])
}

// currentSession returns the live state object bound to this request,
// or nil when no session is active. After OpenSession() on a fresh
// request, the state is reachable; after CloseSession(), it's nil again.
func (s *stickySink) currentSession() any {
	if s == nil || !s.hasSession || s.closed {
		return nil
	}
	if s.entry == nil {
		return nil
	}
	return s.entry.state
}

// currentSessionID returns the hex-encoded session id for the resumed or
// newly-opened session, or "" when no session is active for this request.
func (s *stickySink) currentSessionID() string {
	if s == nil || !s.hasSession || s.closed {
		return ""
	}
	return s.sessionIDHex
}

// OpenSession registers state in the per-worker sticky session registry
// and arranges for a fresh VGI-Session token to be emitted on the
// response. Mirrors Python CallContext.open_session.
//
// Returns:
//   - *RpcError("RuntimeError") when the transport doesn't carry sticky
//     machinery (pipe / subprocess / unix) or when the request lacks
//     VGI-Session-Accept: true (the leak-prevention guard).
//   - *ServerDrainingError when the server is draining.
//   - nil on success.
//
// ttl is the session's TTL; zero means use the registry's default.
func (ctx *CallContext) OpenSession(state any, ttl time.Duration) error {
	sink := ctx.stickySink
	if sink == nil || sink.registry == nil {
		return &RpcError{
			Type:    "RuntimeError",
			Message: "sticky sessions not available on this transport",
		}
	}
	if !sink.acceptOpens {
		return &RpcError{
			Type:    "RuntimeError",
			Message: "ctx.open_session requires the client to send VGI-Session-Accept: true",
		}
	}
	if sink.hasSession && !sink.closed {
		return &RpcError{
			Type:    "RuntimeError",
			Message: "a session is already bound to this request",
		}
	}
	sid, expiresAt, entry, err := sink.registry.open(state, ttl, principalKeyFromAuth(sink.auth))
	if err != nil {
		return err
	}
	aad := stateTokenAad(sink.auth)
	token, sealErr := sealSessionToken(sink.tokenKey, sink.serverID, sid, expiresAt.Unix(), aad, 0)
	if sealErr != nil {
		// Roll back the registry entry so a partially-opened session
		// doesn't leak when token sealing fails.
		sink.registry.close(sid)
		return fmt.Errorf("sticky session token seal: %w", sealErr)
	}
	// Install on the sink so the response middleware emits VGI-Session.
	// Use the entry returned directly from `open` — re-`get`-ing by id
	// would race with the reaper on tight TTLs and surface a confusing
	// not-in-the-spec error.
	sink.entry = entry
	sink.sessionID = sid
	sink.hasSession = true
	sink.closed = false
	sink.mintedToken = token
	sink.sessionIDHex = hex.EncodeToString(sid[:])
	return nil
}

// CloseSession removes the session bound to this request from the
// registry, invoking state.Close() if defined. Schedules the
// VGI-Session-Close: true response header. Idempotent — calls after the
// first do nothing. Mirrors Python CallContext.close_session.
//
// Returns true on hit (session was bound and is now evicted), false when
// no session was bound to this request.
func (ctx *CallContext) CloseSession() bool {
	sink := ctx.stickySink
	if sink == nil || sink.registry == nil {
		return false
	}
	if !sink.hasSession {
		return false
	}
	hit := sink.registry.close(sink.sessionID)
	sink.closed = true
	sink.entry = nil
	return hit
}

// Session returns the live state object bound to this request via a
// prior OpenSession on this or an earlier call in the same session
// view, or nil when no session is active. Same object identity is
// preserved across calls in the same session.
func (ctx *CallContext) Session() any {
	if ctx.stickySink == nil {
		return nil
	}
	return ctx.stickySink.currentSession()
}

// SessionID returns the 24-character hex session id for the resumed or
// newly-opened session, or "" when no session is active for this request.
// Useful for access-log records.
func (ctx *CallContext) SessionID() string {
	if ctx.stickySink == nil {
		return ""
	}
	return ctx.stickySink.currentSessionID()
}

// principalKeyFromAuth derives a stable string used to partition the
// registry by authenticated principal. AAD already binds tokens to the
// principal cryptographically; this is defense-in-depth. The predicate
// MUST match [stateTokenAad]'s anonymous branch — diverging here would
// let an authenticated request with empty principal hit the anonymous
// AAD slice but the authenticated registry partition (or vice-versa),
// breaking session resolution. Mirrors Python's _StickyMiddleware._principal_key.
func principalKeyFromAuth(auth *AuthContext) string {
	if auth == nil || !auth.Authenticated {
		return "\x00anonymous"
	}
	return auth.Domain + "\x00" + auth.Principal
}
