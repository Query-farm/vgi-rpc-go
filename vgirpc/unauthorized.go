// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// AuthReason is the machine-readable code on a 401, from the closed set in
// docs/unauthorized-spec.md §3. A client switches on it: refresh on
// [AuthReasonExpiredCredential], give up on [AuthReasonInsufficientScope].
//
// Readers MUST treat an unrecognised code as [AuthReasonUnauthorized] — that
// means the server is newer, not broken.
type AuthReason string

const (
	// AuthReasonMissingCredential — no credential was presented at all.
	AuthReasonMissingCredential AuthReason = "missing_credential"
	// AuthReasonInvalidCredential — a credential was presented and rejected.
	AuthReasonInvalidCredential AuthReason = "invalid_credential"
	// AuthReasonExpiredCredential — well-formed, outside its validity window.
	AuthReasonExpiredCredential AuthReason = "expired_credential"
	// AuthReasonInsufficientScope — identified, but not permitted. Deliberately
	// a 401 rather than 403: the authenticate callback runs before any method
	// is resolved, so there is no route yet whose permissions could be
	// evaluated. A service wanting a true 403 raises it from the method body.
	AuthReasonInsufficientScope AuthReason = "insufficient_scope"
	// AuthReasonProxyRequired — the request carried no evidence of arriving
	// through the trusted proxy. Derived from server configuration, never from
	// the request; see [HttpServer.SetProxyAuthHeaders].
	AuthReasonProxyRequired AuthReason = "proxy_required"
	// AuthReasonUnauthorized — refused, unclassified. The fallback.
	AuthReasonUnauthorized AuthReason = "unauthorized"
)

// Response headers carrying the rejection's machine-readable shape. Both
// describe a rejection, so neither is emitted on a successful response —
// they are not capability advertisements.
const (
	// HeaderAuthReason carries one [AuthReason] on every 401.
	HeaderAuthReason = "VGI-Auth-Reason"
	// HeaderAuthProxyRequired is "true" on 401s from a service whose auth
	// depends on a proxy. Omitted otherwise — never "false".
	HeaderAuthProxyRequired = "VGI-Auth-Proxy-Required"
)

// AuthFailure is an authentication rejection that names its reason.
//
// Return one from an [AuthenticateFunc] to classify the refusal; any other
// error lands on [AuthReasonUnauthorized]. Guessing a finer code from an
// unclassified failure would mean matching on message text, which
// misclassifies the moment someone rewords a string.
type AuthFailure struct {
	// Reason is the code reported on the wire.
	Reason AuthReason
	// Detail is human-readable and may be empty. Free text — never a
	// verifier's per-attempt state, which would leak which stage rejected.
	Detail string
}

func (e *AuthFailure) Error() string {
	if e.Detail == "" {
		return string(e.Reason)
	}
	return fmt.Sprintf("%s: %s", e.Reason, e.Detail)
}

// NewAuthFailure builds an [AuthFailure]. Convenience for the common
// `return nil, NewAuthFailure(reason, detail)` shape.
func NewAuthFailure(reason AuthReason, detail string) *AuthFailure {
	return &AuthFailure{Reason: reason, Detail: detail}
}

// classifyAuthError maps an authenticate-callback error onto a reason code
// and detail. Anything that does not name a reason is unclassified.
func classifyAuthError(err error) (AuthReason, string) {
	var failure *AuthFailure
	if ok := asAuthFailure(err, &failure); ok {
		reason := failure.Reason
		if reason == "" {
			reason = AuthReasonUnauthorized
		}
		return reason, failure.Detail
	}
	if rpcErr, ok := err.(*RpcError); ok {
		// A conservative guess from the error's *type*, mirroring the Python
		// reference: PermissionError means the caller got as far as being
		// identified. Guessing more finely would mean matching on message
		// text, which misclassifies the moment someone rewords a string.
		if rpcErr.Type == "PermissionError" {
			return AuthReasonInsufficientScope, rpcErr.Message
		}
		return AuthReasonUnauthorized, rpcErr.Message
	}
	return AuthReasonUnauthorized, err.Error()
}

// asAuthFailure is a tiny stand-in for errors.As so this file stays
// dependency-free at the call site.
func asAuthFailure(err error, out **AuthFailure) bool {
	for err != nil {
		if f, ok := err.(*AuthFailure); ok {
			*out = f
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// proxyHint is the operator-facing note attached under §5. The wording is
// not normative — it must convey that the service is only reachable through
// its proxy, which headers the proxy must set, and that a rejection here is
// at least as likely to be a proxy misconfiguration as a bad credential.
func (h *HttpServer) proxyHint() string {
	headers := h.proxyAuthHeaders()
	if len(headers) == 0 {
		return ""
	}
	return fmt.Sprintf(
		"This service only accepts requests that arrive through its configured "+
			"reverse proxy, which must set the %s header(s). A rejection here is at "+
			"least as likely to be a proxy misconfiguration as a bad credential — "+
			"check that the proxy is forwarding them before re-issuing credentials.",
		strings.Join(headers, ", "))
}

// proxyAuthHeaders returns the proxy-injected headers this server's
// authentication depends on, or nil when it depends on none.
//
// Derived from configuration rather than from what failed on this request,
// so every 401 says the same thing (spec §5). A proxy-proof gate contributes
// only in require mode: in allow mode an absent proof never denies, so the
// note would misdirect.
func (h *HttpServer) proxyAuthHeaders() []string {
	var out []string
	if h.proxyProofRequired {
		out = append(out, ProofHeader)
	}
	out = append(out, h.extraProxyAuthHeaders...)
	return out
}

// SetProxyAuthHeaders declares the proxy-injected headers this service's
// authentication depends on, for a custom AuthenticateFunc the framework
// cannot introspect. The built-in proxy-proof gate registers its own header
// in require mode, so this is only needed on top of that.
func (h *HttpServer) SetProxyAuthHeaders(headers ...string) {
	h.extraProxyAuthHeaders = append([]string(nil), headers...)
}

// writeUnauthorized renders the standardized 401 of
// docs/unauthorized-spec.md §4: the reason header, a no-store cache
// directive, the proxy note when this service's auth depends on a proxy,
// and either the JSON envelope or the HTML page depending on Accept.
func (h *HttpServer) writeUnauthorized(w http.ResponseWriter, r *http.Request, reason AuthReason, detail string) {
	if reason == "" {
		reason = AuthReasonUnauthorized
	}
	hint := h.proxyHint()
	if hint != "" {
		// The note is derived from configuration, so it is the same on every
		// 401 this server produces — including ones whose reason is not
		// proxy_required. That is what lets it coexist with the uniform
		// rejection rule of the proxy-proof spec: it discloses nothing about
		// which stage rejected this attempt.
		w.Header().Set(HeaderAuthProxyRequired, "true")
	}
	w.Header().Set(HeaderAuthReason, string(reason))
	// A 401 is per-request and flips to 200 on the next attempt with a
	// credential, so it must never be held by a shared cache.
	w.Header().Set("Cache-Control", "no-store")
	if h.wwwAuthenticate != "" {
		w.Header().Set("WWW-Authenticate", h.wwwAuthenticate)
	}

	// §4.2 lets a service skip the HTML page entirely and always answer with
	// JSON; what it must never do is answer a non-HTML request with HTML.
	// This port takes the JSON-only option, so Accept does not change the
	// body — and the reason header, the part clients actually parse, is set
	// either way above.

	body := map[string]any{
		"error":  "unauthorized",
		"reason": string(reason),
		"detail": detail,
	}
	if hint != "" {
		// Absent, not empty, when it does not apply — its presence alone is
		// then a usable signal.
		body["proxy_hint"] = hint
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = w.Write(encoded)
}
