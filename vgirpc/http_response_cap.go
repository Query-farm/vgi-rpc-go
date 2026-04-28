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
	maxResponseBytesHeader              = "VGI-Max-Response-Bytes"
	maxExternalizedResponseBytesHeader  = "VGI-Max-Externalized-Response-Bytes"
	externalizationEnabledHeader        = "VGI-Externalization-Enabled"
)

// SetMaxResponseBytes caps the HTTP body size (the bytes that literally
// land on the wire). Externalised payloads do not count against this —
// only their tiny pointer batches reach the wire. The cap is *hard* for
// unary and stream-exchange (overshoot surfaces as 200 + EXCEPTION batch
// with X-VGI-RPC-Error) and *soft* for producer streams (continuation
// tokens cover overshoot). Set to 0 to disable.
func (h *HttpServer) SetMaxResponseBytes(n int64) {
	h.maxResponseBytes = n
}

// SetMaxExternalizedResponseBytes caps the total bytes uploaded to
// external storage during one HTTP response. Always *hard* — externalised
// uploads have no escape valve, so overshoot surfaces as 200 + EXCEPTION
// batch with X-VGI-RPC-Error. Set to 0 to disable.
func (h *HttpServer) SetMaxExternalizedResponseBytes(n int64) {
	h.maxExternalizedResponseBytes = n
}

// predictExternalizeBytes returns the byte size of the external upload
// that would happen if MaybeExternalizeBatch were invoked on this batch
// right now, or 0 if the batch would not be externalised.
//
// Mirrors the threshold logic in MaybeExternalizeBatch so a pre-flight
// check matches what the upload helper actually does. Used to refuse a
// violating upload BEFORE incurring the storage round-trip — the
// operator's intent in setting max_externalized_response_bytes is "don't
// emit data beyond this per call," not "emit and then complain."
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
		return fmt.Errorf("HTTP body exceeds max_response_bytes (%d > %d) for method %q", wireBytes, wireCap, method)
	}
	if externalCap > 0 && externalBytes > externalCap {
		return fmt.Errorf("Externalised payload exceeds max_externalized_response_bytes (%d > %d) for method %q", externalBytes, externalCap, method)
	}
	return nil
}
