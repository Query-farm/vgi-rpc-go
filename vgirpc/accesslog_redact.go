// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"log/slog"
	"regexp"
	"sync/atomic"
)

// RedactedClaim is the placeholder substituted for a sensitive claim value.
const RedactedClaim = "[redacted]"

// ClaimRedactor takes an authenticated principal's raw claims and returns
// what should be logged. Install one with [SetClaimRedactor].
type ClaimRedactor func(claims map[string]any) map[string]any

// defaultClaimRedactPattern names the claims whose *values* are replaced.
//
// The first group is credentials — the same set observability code elsewhere
// treats as sensitive, so the two do not disagree about what a secret is. The
// rest are standard OIDC claims that are personal data: an access log outlives
// the token it describes by months or years, and is shipped to systems chosen
// for searchability rather than for holding PII.
var defaultClaimRedactPattern = regexp.MustCompile(
	`(?i)password|token|secret|key|authorization` +
		`|email|phone|address|birthdate|gender` +
		`|^name$|given_name|family_name|middle_name|nickname|preferred_username|picture|profile|website`)

// RedactClaims returns claims with sensitive values replaced by
// [RedactedClaim]. It is the default policy.
//
// Matching is **key-based**: a value is matched on the name it arrived under,
// never on its content. A claim called "context" holding an email address is
// not caught, and cannot be without guessing at free text — a boundary worth
// stating rather than pretending to exceed.
//
// Values are replaced rather than dropped, so the record still shows which
// claims the credential carried. "Did this token carry an email claim?" is a
// question an audit log exists to answer; "what was it?" is not.
func RedactClaims(claims map[string]any) map[string]any {
	out := make(map[string]any, len(claims))
	for k, v := range claims {
		if defaultClaimRedactPattern.MatchString(k) {
			out[k] = RedactedClaim
		} else {
			out[k] = v
		}
	}
	return out
}

// NoClaimRedaction passes claims through verbatim. Only for logs a service
// owns end to end.
func NoClaimRedaction(claims map[string]any) map[string]any {
	out := make(map[string]any, len(claims))
	for k, v := range claims {
		out[k] = v
	}
	return out
}

// claimRedactor holds the installed policy; nil means [RedactClaims].
var claimRedactor atomic.Pointer[ClaimRedactor]

// SetClaimRedactor installs the policy applied to access-log claims. Pass
// [NoClaimRedaction] to disable redaction, or nil to restore the default.
//
// Replace the default when it is wrong for a deployment — either too strict
// (an internal service that needs the values) or not strict enough (custom
// claim names carrying personal data).
func SetClaimRedactor(r ClaimRedactor) {
	if r == nil {
		claimRedactor.Store(nil)
		return
	}
	claimRedactor.Store(&r)
}

// applyClaimRedaction runs the installed redactor, failing closed.
//
// A redactor that panics must not take the request down with it, but it must
// also not fail *open*: the claims are dropped entirely rather than emitted
// unredacted, since an emitter that silently reverts to verbatim claims is
// the exact failure this machinery exists to prevent.
func applyClaimRedaction(claims map[string]any) (out map[string]any) {
	defer func() {
		if rv := recover(); rv != nil {
			slog.Warn("access log: claim redactor panicked; dropping claims from the record", "err", rv)
			out = nil
		}
	}()
	if p := claimRedactor.Load(); p != nil {
		return (*p)(claims)
	}
	return RedactClaims(claims)
}
