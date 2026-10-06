// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"context"
	"strings"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// conformance.Secondary.v1 -- the second application protocol every
// conformance worker hosts, beside ConformanceService.
//
// Normative in the reference's tools/cross-port/specs/MULTI_PROTOCOL_HOSTING.md
// §2. It makes three things observable that a single-protocol worker hides:
//
//   - Routing by pair. echo_string repeats the name and signature of
//     ConformanceService.echo_string; this one prefixes its reply, so a server
//     keying dispatch on the bare method name answers with a wrong value rather
//     than a coincidentally right one.
//   - A per-binding version gate. The secondary declares NO protocol_version
//     while the primary declares 2.0.0, so a server gating every call against
//     the primary's version refuses the secondary's callers.
//   - The error model. fail raises whatever code and kind the caller names,
//     with fixed details (one catalog type, a RetryInfo when asked, and one
//     type no client knows); fail_oversized raises details over the 4 KiB cap.
//
// Exported so an SDK built on this port (vgi-go) hosts the identical protocol
// in its fixture worker instead of a hand-copied one.

// SecondaryProtocolName is the secondary's wire name.
const SecondaryProtocolName = "conformance.Secondary.v1"

// SecondaryProtocolHash is the pinned canonical digest of the secondary.
const SecondaryProtocolHash = "58557cf1611546ad22d1c379bc3ce1b04166082f78375e9fc959f0086347eab6"

const (
	secondaryEchoPrefix   = "secondary:"
	secondaryInvalidKind  = "invalid_code"
	secondaryOversizeKind = "details_oversized"
	secondaryPadding      = 5000
)

type secondaryEchoParams struct {
	Value string `vgirpc:"value"`
}

type secondaryFailParams struct {
	Code              string  `vgirpc:"code"`
	Kind              string  `vgirpc:"kind"`
	RetryDelaySeconds float64 `vgirpc:"retry_delay_seconds"`
}

type secondaryNoParams struct{}

// NewSecondary builds conformance.Secondary.v1, ready to host with
// [vgirpc.Server.AddProtocol].
func NewSecondary() *vgirpc.Server {
	p := vgirpc.NewProtocol(SecondaryProtocolName)
	vgirpc.Unary(p, "echo_string", func(_ context.Context, _ *vgirpc.CallContext, in secondaryEchoParams) (string, error) {
		return secondaryEchoPrefix + in.Value, nil
	})
	vgirpc.UnaryVoid(p, "fail", func(_ context.Context, _ *vgirpc.CallContext, in secondaryFailParams) error {
		return secondaryFail(in.Code, in.Kind, in.RetryDelaySeconds)
	})
	vgirpc.UnaryVoid(p, "fail_oversized", func(context.Context, *vgirpc.CallContext, secondaryNoParams) error {
		return &vgirpc.StatusError{
			Code:    vgirpc.CodeResourceExhausted,
			Kind:    secondaryOversizeKind,
			Message: "details over the 4 KiB cap",
			// The small RetryInfo first, on purpose: a server dropping only the
			// element that does not fit keeps it, and the error then reads as
			// retryable.
			Details: []vgirpc.ErrorDetail{
				vgirpc.RetryInfo{RetryDelaySeconds: 1},
				vgirpc.ErrorInfo{Metadata: map[string]string{"padding": strings.Repeat("x", secondaryPadding)}},
			},
		}
	})
	return p
}

func secondaryFail(code, kind string, delay float64) error {
	if !vgirpc.Code(code).Valid() {
		return &vgirpc.StatusError{
			Code:    vgirpc.CodeInvalidArgument,
			Kind:    secondaryInvalidKind,
			Message: "code " + code + " is not a canonical error code",
			Details: []vgirpc.ErrorDetail{vgirpc.BadRequest{FieldViolations: []vgirpc.FieldViolation{
				{Field: "code", Description: "must be a canonical code name"},
			}}},
		}
	}
	details := []vgirpc.ErrorDetail{
		vgirpc.ErrorInfo{Metadata: map[string]string{"fixture": SecondaryProtocolName}},
	}
	if delay > 0 {
		details = append(details, vgirpc.RetryInfo{RetryDelaySeconds: delay})
	}
	details = append(details, vgirpc.RawDetail{
		"@type": SecondaryProtocolName + ".Probe",
		"note":  "clients ignore detail types they do not know",
	})
	return &vgirpc.StatusError{
		Code:    vgirpc.Code(code),
		Kind:    kind, // "" is absent on the wire, not an empty string
		Message: "conformance: requested failure " + code,
		Details: details,
	}
}
