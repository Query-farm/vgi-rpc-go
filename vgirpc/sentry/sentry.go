// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Package sentry provides Sentry error reporting and optional performance
// monitoring for vgi-rpc, mirroring the Python reference's vgi_rpc.sentry
// module.
//
// Users must initialise the Sentry SDK separately via sentry.Init(...) —
// this module does NOT manage the DSN or SDK lifecycle. After Init,
// pass the server to Instrument:
//
//	import (
//	    "github.com/getsentry/sentry-go"
//	    vgisentry "github.com/Query-farm/vgi-rpc/vgirpc/sentry"
//	)
//
//	sentry.Init(sentry.ClientOptions{Dsn: "https://..."})
//	server := vgirpc.NewServer()
//	// ... register methods ...
//	vgisentry.Instrument(server, nil) // default config
//
// The dispatch hook tags every event with rpc.method, rpc.method_type,
// auth.domain, and auth.authenticated for Issues-side filtering, and
// attaches Sentry user info from auth.principal + configurable claim
// mapping. Exceptions are reported via sentry.CaptureException with the
// full RPC scope attached. Performance monitoring (a Sentry transaction
// per dispatch) is opt-in to avoid duplicate tracing alongside OTel.
//
// # Trace Explorer recipe
//
// Every dispatch attaches rpc.system, rpc.service, rpc.method, and
// rpc.method_type as data fields on the current span. Streams also
// carry rpc.stream_id so sibling /init and /exchange turns of one
// logical stream call can be correlated.
//
// # Per-call parameter recording
//
// Python's record_params / tag_params features are not yet supported on
// the Go port: vgi-rpc-go fires DispatchHook.OnDispatchStart BEFORE
// parameter deserialisation, so the typed params struct is not
// available to the hook. The remaining surface (auth, claims,
// custom tags, error capture, transactions) is fully supported.
package sentry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/getsentry/sentry-go"
)

// Config configures the vgirpc Sentry dispatch hook. Mirrors
// vgi_rpc.sentry.SentryConfig on the Python side.
type Config struct {
	// EnableErrorCapture causes the hook to call
	// sentry.CaptureException for handler errors. Default: true.
	EnableErrorCapture bool

	// EnablePerformance starts a Sentry transaction per dispatch. Opt-in
	// (default false) to avoid duplicate tracing when running alongside
	// OpenTelemetry and to conserve Sentry quota.
	EnablePerformance bool

	// RecordRequestContext attaches an "rpc" scope context with method
	// name, service, and server_id, plus auth tags and the Sentry user.
	// Default: true.
	RecordRequestContext bool

	// CustomTags are applied to every Sentry event (set on every
	// dispatch's scope).
	CustomTags map[string]string

	// IgnoredErrorTypes is a list of *vgirpc.RpcError.Type values that
	// should NOT be captured. Useful for filtering expected client
	// errors (e.g. "PermissionError", "ValueError"). Non-RpcError
	// errors are matched by their Go type name via fmt.Sprintf("%T", err).
	IgnoredErrorTypes []string

	// OpName is the Sentry transaction operation name when
	// EnablePerformance is true. Default: "rpc.server".
	OpName string

	// ClaimTags maps JWT claim keys to Sentry tag names, e.g.
	// {"tenant_id": "auth.tenant_id"}. Missing claims are skipped.
	ClaimTags map[string]string

	// UserClaimMap populates Sentry's user object fields from JWT
	// claims, e.g. {"username": "preferred_username", "email": "email"}.
	// Default: {"username": "preferred_username", "email": "email",
	// "name": "name"}. user.id is always set from auth.principal and
	// is not configurable here.
	UserClaimMap map[string]string

	// SetTransactionName overrides the WSGI / Sentry-default
	// transaction name with "rpc {method}". Default: true.
	SetTransactionName bool
}

// DefaultConfig returns a Config with sensible defaults matching Python's
// SentryConfig() constructor.
func DefaultConfig() *Config {
	return &Config{
		EnableErrorCapture:   true,
		EnablePerformance:    false,
		RecordRequestContext: true,
		OpName:               "rpc.server",
		UserClaimMap: map[string]string{
			"username": "preferred_username",
			"email":    "email",
			"name":     "name",
		},
		SetTransactionName: true,
	}
}

// ShortHash returns a stable 12-char hex prefix of sha256(value), suitable
// for tagging high-cardinality opaque IDs (attach_id, transaction_id,
// UUIDs, JWT subs) without exhausting Sentry's tag-value distribution UI.
// Same input always yields the same prefix; collisions are negligible at
// 12 chars for any realistic per-process volume. Returns the empty string
// for an empty input so callers can pass through optional fields without
// a nil check.
func ShortHash(value string) string {
	return ShortHashN(value, 12)
}

// ShortHashN is ShortHash with a caller-specified prefix length.
func ShortHashN(value string, length int) string {
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	enc := hex.EncodeToString(sum[:])
	if length <= 0 || length > len(enc) {
		length = len(enc)
	}
	return enc[:length]
}

// Instrument attaches a Sentry dispatch hook to server. Must be called
// before server.Serve() — not thread-safe during dispatch. If cfg is
// nil, DefaultConfig is used. Returns the same server for chaining.
//
// Unlike Python's instrument_server_sentry, this does NOT strip a
// pre-existing Sentry hook: vgi-rpc-go's Server holds at most one
// DispatchHook (no composite chain in core), so Instrument replaces
// whichever hook was previously installed via SetDispatchHook. If you
// need a composite (AccessLog + Sentry + OTel), wrap them with a
// caller-side multiplexing DispatchHook before calling SetDispatchHook.
func Instrument(server *vgirpc.Server, cfg *Config) *vgirpc.Server {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	hook := &dispatchHook{cfg: cfg, serviceName: server.ServiceName(), serverID: server.ServerID()}
	server.SetDispatchHook(hook)
	return server
}

// dispatchHook implements vgirpc.DispatchHook with Sentry reporting.
type dispatchHook struct {
	cfg         *Config
	serviceName string
	serverID    string
}

type hookToken struct {
	span        *sentry.Span // non-nil only when EnablePerformance
	hub         *sentry.Hub
	methodName  string
	methodType  string
}

// OnDispatchStart attaches RPC scope, tags, and user info to the
// request-scoped Sentry hub and (optionally) opens a transaction span.
func (h *dispatchHook) OnDispatchStart(ctx context.Context, info vgirpc.DispatchInfo) (context.Context, vgirpc.HookToken) {
	// Prefer a per-request hub injected by sentry-go's HTTP middleware so
	// concurrent requests don't share scope. Fall back to a clone of the
	// global hub when no middleware is in play (pipe / stdio transports).
	hub := sentry.GetHubFromContext(ctx)
	if hub == nil {
		hub = sentry.CurrentHub().Clone()
		ctx = sentry.SetHubOnContext(ctx, hub)
	}

	// Transaction renaming: in sentry-go the transaction name lives on
	// the root span (sentry.TransactionFromContext), not on the scope.
	// Apply BEFORE ConfigureScope so the rename also wins when the
	// request-scoped transaction was started by sentry-go/http middleware.
	if h.cfg.SetTransactionName {
		if tx := sentry.TransactionFromContext(ctx); tx != nil {
			tx.Name = fmt.Sprintf("rpc %s", info.Method)
		}
	}

	hub.ConfigureScope(func(scope *sentry.Scope) {
		// Scope tags — Issues-side filtering of error events. Method name
		// and type are bounded (one tag value per registered method);
		// claim_tags + custom_tags are operator-curated.
		scope.SetTag("rpc.method", info.Method)
		scope.SetTag("rpc.method_type", info.MethodType)

		if h.cfg.RecordRequestContext {
			scope.SetContext("rpc", map[string]interface{}{
				"method":      info.Method,
				"method_type": info.MethodType,
				"service":     h.serviceName,
				"server_id":   h.serverID,
			})

			user := sentry.User{}
			if info.Auth != nil {
				if info.Auth.Principal != "" {
					user.ID = info.Auth.Principal
				}
				for userField, claimKey := range h.cfg.UserClaimMap {
					if v, ok := info.Auth.Claims[claimKey].(string); ok && v != "" {
						switch userField {
						case "username":
							user.Username = v
						case "email":
							user.Email = v
						case "name":
							user.Name = v
						}
					}
				}
				if info.Auth.Domain != "" {
					scope.SetTag("auth.domain", info.Auth.Domain)
				}
				if info.Auth.Authenticated {
					scope.SetTag("auth.authenticated", "true")
				} else {
					scope.SetTag("auth.authenticated", "false")
				}
				for claimKey, tagName := range h.cfg.ClaimTags {
					if v, ok := info.Auth.Claims[claimKey]; ok && v != nil {
						scope.SetTag(tagName, fmt.Sprintf("%v", v))
					}
				}
			}
			if user.ID != "" || user.Username != "" || user.Email != "" || user.Name != "" {
				scope.SetUser(user)
			}
		}

		for k, v := range h.cfg.CustomTags {
			scope.SetTag(k, v)
		}

		// Stream correlation: the StreamID is a uuid4-derived 32-char
		// hex that's stable across all turns of one logical stream call.
		// Span data only, never a scope tag (unbounded cardinality would
		// pollute Sentry's tag distribution UI).
		if info.StreamID != "" {
			scope.SetExtra("rpc.stream_id", info.StreamID)
		}
	})

	var span *sentry.Span
	if h.cfg.EnablePerformance {
		op := h.cfg.OpName
		if op == "" {
			op = "rpc.server"
		}
		span = sentry.StartSpan(ctx, op, sentry.WithTransactionName(fmt.Sprintf("vgi_rpc/%s", info.Method)))
		ctx = span.Context()
	}

	// Attach the searchable Trace Explorer fields on the current span
	// (request-scoped span when sentry-go/http middleware is in play, or
	// the transaction span we just started). set_data is the Python
	// equivalent of Go's Span.SetData.
	if currentSpan := sentry.SpanFromContext(ctx); currentSpan != nil {
		currentSpan.SetData("rpc.system", "vgi_rpc")
		currentSpan.SetData("rpc.service", h.serviceName)
		currentSpan.SetData("rpc.method", info.Method)
		currentSpan.SetData("rpc.method_type", info.MethodType)
		if info.StreamID != "" {
			currentSpan.SetData("rpc.stream_id", info.StreamID)
		}
	}

	return ctx, &hookToken{
		span:       span,
		hub:        hub,
		methodName: info.Method,
		methodType: info.MethodType,
	}
}

// OnDispatchEnd captures unhandled errors and finalises any opened span.
func (h *dispatchHook) OnDispatchEnd(ctx context.Context, token vgirpc.HookToken, info vgirpc.DispatchInfo, stats *vgirpc.CallStatistics, err error) {
	tok, ok := token.(*hookToken)
	if !ok {
		return
	}

	if err != nil && h.cfg.EnableErrorCapture && !h.isIgnored(err) {
		if tok.hub != nil {
			tok.hub.CaptureException(err)
		} else {
			sentry.CaptureException(err)
		}
	}

	if tok.span != nil {
		if err != nil {
			tok.span.Status = sentry.SpanStatusInternalError
		} else {
			tok.span.Status = sentry.SpanStatusOK
		}
		tok.span.Finish()
	}
}

// isIgnored reports whether err matches the configured IgnoredErrorTypes
// list. *vgirpc.RpcError matches against its Type field; other errors
// match against their Go type name (matching Python's
// isinstance(error, ignored_exceptions) on a best-effort basis).
func (h *dispatchHook) isIgnored(err error) bool {
	if len(h.cfg.IgnoredErrorTypes) == 0 {
		return false
	}
	var name string
	if rpcErr, ok := err.(*vgirpc.RpcError); ok {
		name = rpcErr.Type
	} else {
		name = fmt.Sprintf("%T", err)
	}
	for _, t := range h.cfg.IgnoredErrorTypes {
		if name == t {
			return true
		}
	}
	return false
}

// Compile-time check that dispatchHook satisfies DispatchHook.
var _ vgirpc.DispatchHook = (*dispatchHook)(nil)
