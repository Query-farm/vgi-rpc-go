// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// CallContext provides request-scoped information and logging to method handlers.
// A CallContext is not safe for concurrent use. If a handler spawns goroutines,
// they must not call [CallContext.ClientLog] without external synchronization.
type CallContext struct {
	// Ctx is the request-scoped context, carrying cancellation and deadlines.
	Ctx context.Context
	// RequestID is the client-supplied identifier for this request, echoed in
	// all response metadata.
	RequestID string
	// ServerID is the server identifier set via [Server.SetServerID].
	ServerID string
	// Method is the name of the RPC method being invoked.
	Method string
	// LogLevel is the client-requested minimum log severity. Log messages
	// below this level are silently discarded by [CallContext.ClientLog].
	LogLevel LogLevel
	// Auth is the authentication context for this request. It is never nil;
	// unauthenticated requests receive [Anonymous].
	Auth *AuthContext
	// PeerEvidence is the immutable transport-identity snapshot for this call.
	// It is empty when no provider ran or the transport does not support peer
	// identity. It is off-wire and never supplied by the RPC client.
	PeerEvidence *PeerEvidenceSet
	// TransportMetadata holds transport-level key/value pairs such as
	// remote_addr, user_agent, and IPC custom metadata.
	TransportMetadata map[string]string
	// InputMetadata carries per-tick / per-batch Arrow custom metadata attached
	// to the input batch that triggered this Produce/Exchange call. Zero-valued
	// when the input batch has no metadata. Used to surface signals like
	// DuckDB's dynamic filter ticks (`vgi_pushdown_filters`) to the handler.
	//
	// It is the same on every transport: that input's own metadata, every
	// turn, less the transport's bookkeeping (MetaStreamState, MetaCallState,
	// MetaCancel). An externalized input carries the fetched payload's
	// metadata, never the pointer's, with the reader's provenance stamp
	// (MetaLocationSource, MetaLocationFetchMs) merged on top. The input batch
	// an exchange handler is given carries exactly this as its own metadata.
	InputMetadata arrow.Metadata
	// Cookies holds the incoming HTTP request cookies.  Empty for non-HTTP
	// transports (pipe, subprocess, Unix socket).
	Cookies map[string]string
	// Kind identifies the transport the server is bound to. Empty string
	// before the first request is dispatched. Use this for per-call
	// branching (e.g. attach HTTP-specific tracing attributes only when
	// Kind == TransportKindHTTP).
	Kind TransportKind
	// Implementation is an optional caller-supplied reference to the service
	// implementation, set via [Server.SetImplementation]. Mirrors Python's
	// CallContext.implementation — framework-driven callbacks (cancel hooks,
	// stream state lifecycle) read it to dispatch through helper methods on
	// the impl without holding their own captured reference. nil by default.
	Implementation any
	// ResponseLimitBytes is the effective hard HTTP response limit after
	// application, hosting, and client-advertised limits are combined. Zero
	// means unbounded or not applicable. It is off-wire and immutable.
	ResponseLimitBytes int64
	// PreferredResponseBytes is the application's preferred response target,
	// clamped to ResponseLimitBytes when a hard limit is present. Zero means no
	// preference was configured.
	PreferredResponseBytes int64
	// stickySink is the per-request bridge to the HTTP sticky session
	// machinery. nil on non-HTTP transports and on HTTP servers without
	// sticky enabled — CallContext.OpenSession / CloseSession check for
	// this and raise RuntimeError when nil. Mirrors Python's
	// _current_sticky_sink contextvar pattern.
	stickySink        *stickySink
	logs              []LogMessage
	responseCookies   []CookieSpec
	cookieSinkEnabled bool
	// externalRef is the pre-published ref the handler answered with via
	// RespondWithExternalRef; nil when it returned its value normally.
	externalRef *ExternalRef
	// externalRefAllowed is set by the unary dispatchers for methods that
	// return a value -- the only calls a ref can answer.
	externalRefAllowed bool
	// stateRefs is the framework's StateRef view of this request: whether
	// StateRef() is allowed (only on a stream method's own context) and, on
	// HTTP, the server's key, storage and object cache. nil gives a scope
	// that resolves in-memory and inline refs and refuses StateRef().
	stateRefs *stateRefScope
}

func (ctx *CallContext) stateRefScope() *stateRefScope {
	if ctx.stateRefs == nil {
		ctx.stateRefs = &stateRefScope{auth: ctx.Auth, method: ctx.Method}
	}
	return ctx.stateRefs
}

// StateRef wraps a value that does not change for the life of a stream
// (STATE_REF_V1.md). Store the returned [StateRef] in a field of the stream
// state and read it back with [CallContext.Resolve] on any turn. On HTTP a
// large value is stored once and only a ~130-byte sealed tag rides each
// continuation; a small one is carried inline, compressed. On the byte-stream
// transports the value is held in memory. Worker code is the same everywhere.
//
// value is a []byte or string, an [encoding.BinaryMarshaler], or a
// vgirpc-tagged struct (serialized with [MarshalRecord]). Treat it as
// immutable afterwards.
//
// Only valid on the context passed to the stream method itself -- the
// handling of the stream's init request. Anywhere else (Produce, Exchange,
// OnCancel, unary calls) it fails with kind state_ref_misuse, code
// FAILED_PRECONDITION. Within one init, byte-equal values share one ref and
// one stored object. A ref belongs to its stream and must not be copied into
// another.
func (ctx *CallContext) StateRef(value any) (StateRef, error) {
	return ctx.stateRefScope().stateRef(value)
}

// Resolve returns the value bytes a [StateRef] names. Valid on every turn and
// every transport. A stored ref is fetched through the process's object cache
// (one Get per process per object, single-flight), verified and opened.
// Failures: invalid_state_ref (unknown form, a tag that does not open for this
// stream and principal, an expired tag, a digest mismatch; HTTP 400),
// state_ref_missing, state_ref_too_large, storage_unavailable. The returned
// slice may be shared: treat it as read-only.
func (ctx *CallContext) Resolve(ref StateRef) ([]byte, error) {
	return ctx.stateRefScope().resolve(ref)
}

// ResolveInto resolves ref and decodes the value into out: a *[]byte or
// *string, an [encoding.BinaryUnmarshaler], or a pointer to a vgirpc-tagged
// struct ([UnmarshalRecord]). While the ref still holds the record it was
// created from, that record is copied into out without a round trip.
func (ctx *CallContext) ResolveInto(ref StateRef, out any) error {
	return ctx.stateRefScope().resolveInto(ref, out)
}

// ResolveArgumentRef resolves a sealed argument reference minted by this
// server's __upload_url__ grant (the grant's ref column) for the bytes the
// caller claims alongside it: it opens ref under this request's principal,
// checks its sealed size and SHA-256 equal size and sha256Hex, then fetches
// the object through the object cache and verifies it. Only an HTTP server
// can resolve one (it holds the key and the storage). See
// [ResolveArgumentRef] for the error kinds. The returned slice is shared with
// the cache: treat it as read-only.
func (ctx *CallContext) ResolveArgumentRef(ref []byte, size int64, sha256Hex string) ([]byte, error) {
	return ctx.stateRefScope().resolveArgumentRef(ref, size, sha256Hex)
}

// ClientLog records a log message that will be sent to the client.
// The message is only recorded if its level is at or above the client-requested log level.
func (ctx *CallContext) ClientLog(level LogLevel, msg string, extras ...KV) {
	if logLevelPriority(level) > logLevelPriority(ctx.LogLevel) {
		return
	}
	logMsg := LogMessage{
		Level:   level,
		Message: msg,
	}
	if len(extras) > 0 {
		logMsg.Extras = make(map[string]string, len(extras))
		for _, kv := range extras {
			logMsg.Extras[kv.Key] = kv.Value
		}
	}
	ctx.logs = append(ctx.logs, logMsg)
}

// drainLogs returns and clears all accumulated log messages.
func (ctx *CallContext) drainLogs() []LogMessage {
	logs := ctx.logs
	ctx.logs = nil
	return logs
}

// CookieAttrs holds the optional attributes for a Set-Cookie header.
// The zero value is valid and produces a cookie with only name=value.
type CookieAttrs struct {
	Expires     time.Time
	MaxAge      int
	Domain      string
	Path        string
	Secure      bool
	HttpOnly    bool
	SameSite    http.SameSite
	Partitioned bool
}

// CookieSpec is a queued cookie mutation to apply to the HTTP response.
// Created by [CallContext.SetCookie] / [CallContext.DeleteCookie] and
// consumed by the HTTP unary handler after dispatch completes.
type CookieSpec struct {
	Name   string
	Value  string
	Delete bool
	CookieAttrs
}

// errCookieNotUnaryHTTP is returned by SetCookie / DeleteCookie when the
// call is not a unary RPC served over HTTP (e.g. streaming method or
// pipe/subprocess transport).
var errCookieNotUnaryHTTP = errors.New("SetCookie/DeleteCookie is only supported inside unary RPC methods served over HTTP")

// SetCookie queues a Set-Cookie header for the HTTP response.  Only valid
// inside a unary RPC method served over HTTP; returns an error otherwise.
func (ctx *CallContext) SetCookie(name, value string, attrs CookieAttrs) error {
	if !ctx.cookieSinkEnabled {
		return errCookieNotUnaryHTTP
	}
	ctx.responseCookies = append(ctx.responseCookies, CookieSpec{
		Name:        name,
		Value:       value,
		CookieAttrs: attrs,
	})
	return nil
}

// DeleteCookie queues a cookie deletion on the HTTP response.  Only valid
// inside a unary RPC method served over HTTP.
func (ctx *CallContext) DeleteCookie(name, path, domain string) error {
	if !ctx.cookieSinkEnabled {
		return errCookieNotUnaryHTTP
	}
	ctx.responseCookies = append(ctx.responseCookies, CookieSpec{
		Name:   name,
		Delete: true,
		CookieAttrs: CookieAttrs{
			Path:   path,
			Domain: domain,
		},
	})
	return nil
}

// enableCookieSink marks this CallContext as able to accept Set-Cookie
// directives.  Called by the unary HTTP handler before dispatch.
func (ctx *CallContext) enableCookieSink() {
	ctx.cookieSinkEnabled = true
}

// drainCookies returns and clears all queued cookie mutations.
func (ctx *CallContext) drainCookies() []CookieSpec {
	cookies := ctx.responseCookies
	ctx.responseCookies = nil
	return cookies
}
