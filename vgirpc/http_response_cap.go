// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// Response-cap header names. Mirror Python's vgi_rpc/http/_common.py so
// cross-implementation clients can probe capabilities consistently.
const (
	maxResponseBytesHeader             = "VGI-Max-Response-Bytes"
	maxExternalizedResponseBytesHeader = "VGI-Max-Externalized-Response-Bytes"
	externalizationEnabledHeader       = "VGI-Externalization-Enabled"
)

// SetMaxResponseBytes caps the HTTP body size (the bytes that literally
// land on the wire). Externalised payloads do not count against this —
// only their tiny pointer batches reach the wire. The cap is *hard* for
// unary and stream-exchange (overshoot surfaces as 200 + EXCEPTION batch
// with X-VGI-RPC-Error). Producer turns are strict as well: an oversized
// response is discarded in full and never exposes a continuation cursor.
// Set to 0 to disable.
func (h *HttpServer) SetMaxResponseBytes(n int64) {
	validateConfiguredResponseBudget(n, "max response bytes")
	h.maxResponseBytes = n
}

// SetHostingMaxResponseBytes sets an optional response limit imposed by the
// hosting platform. The effective limit is the smallest positive application,
// hosting, and client-advertised limit.
func (h *HttpServer) SetHostingMaxResponseBytes(n int64) {
	validateConfiguredResponseBudget(n, "hosting max response bytes")
	h.hostingMaxResponseBytes = n
}

// SetPreferredResponseBytes gives worker code a target below the hard limit.
// The per-request value is clamped to the effective response limit and exposed
// only through CallContext and OutputCollector; it is not a wire header.
func (h *HttpServer) SetPreferredResponseBytes(n int64) {
	validateConfiguredResponseBudget(n, "preferred response bytes")
	h.preferredResponseBytes = n
}

func validateConfiguredResponseBudget(n int64, name string) {
	if n != 0 && (n < minResponseBudgetBytes || n > maxSafeDecimal) {
		panic(fmt.Sprintf("vgirpc: %s must be 0 or between %d and %d", name, minResponseBudgetBytes, maxSafeDecimal))
	}
}

// SetMaxExternalizedResponseBytes caps the total bytes uploaded to
// external storage during one HTTP response. Always *hard* — externalised
// uploads have no escape valve, so overshoot surfaces as 200 + EXCEPTION
// batch with X-VGI-RPC-Error. Set to 0 to disable.
func (h *HttpServer) SetMaxExternalizedResponseBytes(n int64) {
	h.maxExternalizedResponseBytes = n
}

// externalCapError reports that a response would push the call past
// max_externalized_response_bytes.
//
// It exists as a distinct type so a caller can tell an operator cap refusal
// apart from an ordinary handler failure. Producer streams need that
// distinction: an in-handler error rides a plain 200 with an EXCEPTION
// batch, while a cap refusal answers with the error-signalling status that
// [HttpServer.writeArrow] rewrites to 200 + X-VGI-RPC-Error: true — the
// same contract unary and exchange already follow.
type externalCapError struct{ msg string }

func (e *externalCapError) Error() string { return e.msg }

// ErrorType is the exception type name the client sees. The Python
// reference raises a plain RuntimeError for this overshoot, and without
// this the wire would carry Go's "%T" fallback — a Go-internal type name
// in a cross-language error payload.
func (e *externalCapError) ErrorType() string { return "RuntimeError" }

// newExternalCapError builds the overshoot error every external-cap check
// raises. `projected` is the total the call would reach, not just this
// batch's contribution.
//
// The wording is asserted on verbatim by the cross-language conformance
// suite, so it must stay byte-identical to the Python reference — including
// the leading capital, which is why this is the one place that composes it.
func newExternalCapError(method string, projected, capBytes int64) *externalCapError {
	return &externalCapError{msg: fmt.Sprintf(
		"Externalised payload exceeds max_externalized_response_bytes (%d > %d) for method %q",
		projected, capBytes, method)}
}

type responseCapError struct{ msg string }

func (e *responseCapError) Error() string     { return e.msg }
func (e *responseCapError) ErrorType() string { return "ResponseTooLargeError" }

func newResponseCapError(method string, projected, capBytes int64) *responseCapError {
	return &responseCapError{msg: fmt.Sprintf(
		"HTTP body exceeds max_response_bytes (%d > %d) for method %q",
		projected, capBytes, method)}
}

// predictExternalizeBytes estimates the external upload this batch would
// incur if MaybeExternalizeBatch were invoked on it right now, or 0 if the
// batch would not be externalised.
//
// Mirrors the threshold logic in MaybeExternalizeBatch so a pre-flight
// check matches what the upload helper actually does. Used to refuse a
// violating upload BEFORE incurring the storage round-trip — the
// operator's intent in setting max_externalized_response_bytes is "don't
// emit data beyond this per call," not "emit and then complain."
//
// The estimate is the in-memory Arrow buffer size, which is a lower bound
// on the raw IPC bytes the cap is actually charged (IPC adds framing).
// Deliberately so: a pre-flight that overestimated would refuse calls that
// would in fact have fitted. On the unary and exchange paths the post-flush
// [enforceResponseBudgets] check closes the gap; producers have only the
// pre-flight, matching the Python reference.
func predictExternalizeBytes(batch arrow.RecordBatch, config *ExternalLocationConfig) int64 {
	if config == nil || config.Storage == nil {
		return 0
	}
	if batch.NumRows() == 0 {
		return 0
	}
	size := batchBufferSize(batch)
	if size < config.threshold() {
		return 0
	}
	return size
}

// enforceResponseBudgets returns a non-nil error if either configured
// cap was exceeded. Wire body bytes are independent of external bytes:
// externalised payloads leave only tiny pointer batches on the wire and
// don't count toward the body cap, but they DO count toward the external
// cap.
//
// The error message contains one of the literal tokens the cross-language
// conformance tests assert on: "max_response_bytes" or
// "max_externalized_response_bytes".
func enforceResponseBudgets(method string, wireBytes, externalBytes, wireCap, externalCap int64) error {
	if wireCap > 0 && wireBytes > wireCap {
		return newResponseCapError(method, wireBytes, wireCap)
	}
	if externalCap > 0 && externalBytes > externalCap {
		return newExternalCapError(method, externalBytes, externalCap)
	}
	return nil
}
