// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Header names for sticky sessions. Mirrors Python vgi_rpc/http/_common.py.
const (
	stickySessionHeader      = "VGI-Session"
	stickySessionAcceptHeader = "VGI-Session-Accept"
	stickySessionCloseHeader = "VGI-Session-Close"
	stickyEnabledHeader      = "VGI-Sticky-Enabled"
	stickyDefaultTTLHeader   = "VGI-Sticky-Default-TTL"
	stickyEchoHeadersHeader  = "VGI-Sticky-Echo-Headers"
	stickyEchoHeaderPrefix   = "VGI-Echo-"
)

// EnableSticky enables HTTP sticky sessions on this server with the given
// default TTL (zero falls back to 300 seconds, matching Python). After
// EnableSticky the server advertises VGI-Sticky-Enabled on every response
// and routes DELETE {prefix}/__session__ to the idempotent teardown
// endpoint. RPC method handlers may call CallContext.OpenSession to bind
// a Python-style state object to the session token. Idempotent — a second
// call only updates the default TTL.
//
// Sticky sessions are HTTP-only; the pipe / subprocess / unix transports
// raise RuntimeError when ctx.OpenSession is called.
func (h *HttpServer) EnableSticky(defaultTTL time.Duration) {
	if h.stickyRegistry == nil {
		h.stickyRegistry = newSessionRegistry(defaultTTL)
		// Register the DELETE /__session__ idempotent teardown route.
		h.mux.HandleFunc(fmt.Sprintf("DELETE %s/__session__", h.prefix), h.handleStickyDelete)
		return
	}
	if defaultTTL > 0 {
		h.stickyRegistry.defaultTTL = defaultTTL
	}
}

// SetStickyEchoHeaders configures the headers that the server emits as
// VGI-Echo-<name>: <value> on session-opening responses. Clients capture
// each VGI-Echo-* header and replay the inner header on every subsequent
// request in the session view — used for client-driven routing (e.g.
// fly-force-instance-id on Fly.io). Setting an empty map clears the
// echo config. Header names are case-insensitive on the wire.
func (h *HttpServer) SetStickyEchoHeaders(headers map[string]string) {
	if len(headers) == 0 {
		h.stickyEchoHeaders = nil
		return
	}
	clone := make(map[string]string, len(headers))
	for k, v := range headers {
		clone[k] = v
	}
	h.stickyEchoHeaders = clone
}

// DrainHandle returns an operator-facing handle for triggering graceful
// drain on this HTTP server's sticky-session machinery. Returns nil
// when sticky sessions are not enabled (EnableSticky was not called).
// Mirrors Python's vgi_rpc.http.drain_handle().
func (h *HttpServer) DrainHandle() *DrainHandle {
	if h.stickyRegistry == nil {
		return nil
	}
	return &DrainHandle{registry: h.stickyRegistry}
}

// Handle registers a custom HTTP route on the server's mux. The pattern
// uses Go 1.22+ ServeMux syntax (e.g. "POST /custom" or "DELETE /foo").
//
// The route still runs through [HttpServer.ServeHTTP]'s outer envelope —
// CORS headers, capability headers (including VGI-Sticky-Enabled /
// VGI-Supported-Encodings), and response compression all apply. What's
// skipped: the per-RPC authentication callback ([SetAuthenticate]) and
// the dispatch hook ([SetDispatchHook]). Handlers that need auth must
// invoke [HttpServer.AuthenticateRequest] themselves.
//
// Intended for test-only admin endpoints (e.g. the conformance worker's
// /__test_drain__) and server-supplied non-RPC endpoints (health probes,
// readiness probes) that don't fit the unary/stream RPC envelope.
func (h *HttpServer) Handle(pattern string, handler http.HandlerFunc) {
	h.mux.HandleFunc(pattern, handler)
}

// StickyEnabled reports whether the server has sticky sessions enabled.
func (h *HttpServer) StickyEnabled() bool {
	return h.stickyRegistry != nil
}

// stickyCleanup carries the deferrable cleanup paths for a sticky-aware
// request. Wraps the response writer so VGI-Session / VGI-Session-Close /
// VGI-Echo-* headers land on the response at WriteHeader-time without
// the per-handler code paths needing to know about them. ReleaseLock
// releases the per-session mutex after the handler returns. Both are
// no-ops when sticky isn't enabled or no resume happened — callers can
// always defer them without conditional branches.
type stickyCleanup struct {
	sink   *stickySink
	entry  *sessionEntry // non-nil when this request resumed a session
	doneMu bool          // true after ReleaseLock fired; idempotent
}

// ReleaseLock unlocks the per-session mutex acquired during resume.
// Idempotent — safe to defer when no resume actually happened.
func (c *stickyCleanup) ReleaseLock() {
	if c == nil || c.entry == nil || c.doneMu {
		return
	}
	c.entry.lock.Unlock()
	c.doneMu = true
}

// Wrap returns an http.ResponseWriter that flushes the sticky session
// response headers (VGI-Session / VGI-Session-Close / VGI-Echo-*) before
// the first WriteHeader / Write. Returns the underlying writer unchanged
// when no sink is attached so non-sticky requests pay zero overhead.
func (c *stickyCleanup) Wrap(w http.ResponseWriter) http.ResponseWriter {
	if c == nil || c.sink == nil {
		return w
	}
	return &stickyResponseWriter{ResponseWriter: w, sink: c.sink}
}

// stickyResponseWriter is an http.ResponseWriter shim that flushes the
// sticky session headers exactly once — on the first WriteHeader or
// implicit-200 Write — so handlers don't need to coordinate header
// emission with the sticky middleware.
//
// The shim implicitly forwards Header() via embedding. It explicitly
// forwards Flush() so streaming endpoints (SSE, chunked) keep working
// when the wrapper sits in the response writer chain; if the underlying
// writer doesn't implement http.Flusher the call is a no-op.
type stickyResponseWriter struct {
	http.ResponseWriter
	sink    *stickySink
	flushed bool
}

func (s *stickyResponseWriter) WriteHeader(code int) {
	if !s.flushed {
		writeStickyResponseHeaders(s.ResponseWriter, s.sink)
		s.flushed = true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *stickyResponseWriter) Write(p []byte) (int, error) {
	if !s.flushed {
		writeStickyResponseHeaders(s.ResponseWriter, s.sink)
		s.flushed = true
	}
	return s.ResponseWriter.Write(p)
}

// Flush passes through to the underlying writer when it supports
// http.Flusher. Wrapping a ResponseWriter in Go silently strips this
// interface; the explicit forward keeps chunked / SSE responses working
// for streaming methods if sticky is ever wired into those paths.
func (s *stickyResponseWriter) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// installStickyOnRequest is called from the per-request entry path
// (handleUnary / handleStreamInit) before dispatching the method. It
// resolves VGI-Session if present, builds a stickySink and attaches it
// to the supplied CallContext. Returns a non-nil *SessionLostError when
// the presented token doesn't resolve to a live session — caller writes
// a 200 + X-VGI-RPC-Error response with the SessionLostError envelope.
//
// On success when an existing session was resumed, the per-session
// lock is held — caller MUST defer cleanup.ReleaseLock. The cleanup
// value is always non-nil even on error so callers can defer
// unconditionally without nil checks.
//
// Stream continuations / exchanges build the CallContext deeper in the
// dispatch stack, so they use [installStickyOnRequestNoCtx] which
// returns the sink without binding to a context — they assign the sink
// onto each downstream CallContext themselves.
func (h *HttpServer) installStickyOnRequest(r *http.Request, ctx *CallContext, auth *AuthContext) (*stickyCleanup, error) {
	cleanup, err := h.installStickyOnRequestNoCtx(r, auth)
	if cleanup.sink != nil {
		ctx.stickySink = cleanup.sink
	}
	return cleanup, err
}

// installStickyOnRequestNoCtx is the CallContext-less variant used by
// stream continuation / exchange / cancel paths where the CallContext
// is constructed deeper in the dispatch stack. Caller assigns
// `cleanup.sink` onto every CallContext it builds for this request so
// downstream handlers see ctx.Session() / ctx.SessionID() consistently.
func (h *HttpServer) installStickyOnRequestNoCtx(r *http.Request, auth *AuthContext) (*stickyCleanup, error) {
	if h.stickyRegistry == nil {
		return &stickyCleanup{}, nil
	}
	h.stickyRegistry.ensureReaper()

	sink := &stickySink{
		registry:    h.stickyRegistry,
		tokenKey:    h.tokenKey,
		echoHeaders: h.stickyEchoHeaders,
		serverID:    h.server.serverID,
		auth:        auth,
		acceptOpens: strings.EqualFold(strings.TrimSpace(r.Header.Get(stickySessionAcceptHeader)), "true"),
		transport:   TransportKindHTTP,
	}
	cleanup := &stickyCleanup{sink: sink}

	tokenHeader := strings.TrimSpace(r.Header.Get(stickySessionHeader))
	if tokenHeader == "" {
		return cleanup, nil
	}

	aad := stateTokenAad(auth)
	gotServerID, sid, _expiresAt, err := openSessionToken(tokenHeader, h.tokenKey, aad)
	if err != nil {
		return cleanup, err
	}
	if gotServerID != h.server.serverID {
		return cleanup, &SessionLostError{Reason: sessionLostWrongWorker}
	}
	entry := h.stickyRegistry.get(sid, principalKeyFromAuth(auth))
	if entry == nil {
		return cleanup, &SessionLostError{Reason: sessionLostNotFound}
	}

	// Acquire the per-session lock for the duration of dispatch. Released
	// in cleanup.ReleaseLock. Same-session concurrent calls serialize
	// here; different-session calls run in parallel.
	entry.lock.Lock()
	sink.installResumed(entry, sid)
	cleanup.entry = entry
	_ = _expiresAt
	return cleanup, nil
}

// writeStickyResponseHeaders emits VGI-Session and / or VGI-Session-Close
// on the response when the sink's mintedToken or closed flag is set.
// Also emits the configured VGI-Echo-* headers on session-opening responses.
// Must be called before the body is written — headers can't be set after
// WriteHeader.
func writeStickyResponseHeaders(w http.ResponseWriter, sink *stickySink) {
	if sink == nil {
		return
	}
	if sink.mintedToken != "" {
		w.Header().Set(stickySessionHeader, sink.mintedToken)
		for name, value := range sink.echoHeaders {
			w.Header().Set(stickyEchoHeaderPrefix+name, value)
		}
	}
	if sink.closed {
		w.Header().Set(stickySessionCloseHeader, "true")
	}
}

// addStickyCapabilityHeaders emits the capability headers a sticky-aware
// client probes for via OPTIONS /health. Always-on when sticky is enabled
// regardless of the request method — mirror's Python _CapabilitiesMiddleware.
func (h *HttpServer) addStickyCapabilityHeaders(w http.ResponseWriter) {
	if h.stickyRegistry == nil {
		return
	}
	w.Header().Set(stickyEnabledHeader, "true")
	w.Header().Set(stickyDefaultTTLHeader, strconv.FormatInt(int64(h.stickyRegistry.defaultTTL/time.Second), 10))
	if len(h.stickyEchoHeaders) > 0 {
		names := make([]string, 0, len(h.stickyEchoHeaders))
		for k := range h.stickyEchoHeaders {
			names = append(names, k)
		}
		w.Header().Set(stickyEchoHeadersHeader, strings.Join(names, ", "))
	}
}

// handleStickyDelete handles DELETE {prefix}/__session__.
//
// Idempotent best-effort teardown — returns 204 on hit (valid token,
// principal-bound, registry hit, state.Close() invoked), 200 on every
// failure mode (missing header, malformed token, AAD mismatch,
// server_id mismatch, registry miss) so callers cannot probe whether
// a session exists with a stolen token.
func (h *HttpServer) handleStickyDelete(w http.ResponseWriter, r *http.Request) {
	tokenHeader := strings.TrimSpace(r.Header.Get(stickySessionHeader))
	if tokenHeader == "" {
		w.WriteHeader(http.StatusOK)
		return
	}
	// Best-effort authentication for AAD: try to resolve the principal so
	// authenticated DELETEs match their own sessions. Non-fatal — an
	// unauthenticated DELETE just sees the anonymous slice.
	auth := h.authenticateRequest(r)
	if auth == nil {
		auth = Anonymous()
	}
	aad := stateTokenAad(auth)
	gotServerID, sid, _expiresAt, err := openSessionToken(tokenHeader, h.tokenKey, aad)
	if err != nil {
		// Idempotent — don't surface the failure mode.
		w.WriteHeader(http.StatusOK)
		return
	}
	if gotServerID != h.server.serverID {
		w.WriteHeader(http.StatusOK)
		return
	}
	entry := h.stickyRegistry.get(sid, principalKeyFromAuth(auth))
	if entry == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	// Acquire the per-session lock so we serialize with any in-flight
	// call on the same session — matches the documented concurrency
	// contract.
	entry.lock.Lock()
	h.stickyRegistry.close(sid)
	entry.lock.Unlock()
	w.Header().Set(stickySessionCloseHeader, "true")
	w.WriteHeader(http.StatusNoContent)
	_ = _expiresAt
}

// authenticateRequest is a best-effort wrapper that resolves the request's
// authenticated principal. Returns Anonymous() on failure rather than
// raising — the DELETE endpoint is idempotent and shouldn't surface auth
// failures as 401.
func (h *HttpServer) authenticateRequest(r *http.Request) *AuthContext {
	if h.authenticateFunc == nil {
		return Anonymous()
	}
	auth, _ := h.authenticateFunc(r)
	if auth == nil {
		return Anonymous()
	}
	return auth
}
