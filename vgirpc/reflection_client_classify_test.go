// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"errors"
	"fmt"
	"testing"
)

// The answers older servers give to list_protocols are all read as "no
// reflection"; every other failure propagates as itself.
func TestClassifyReflectionNotHosted(t *testing.T) {
	notHosted := map[string]error{
		"current":  &RpcError{Type: "ProtocolNotSupportedError", Message: "x", Kind: "protocol_not_supported", Code: "UNIMPLEMENTED"},
		"old-type": &RpcError{Type: "MethodNotImplementedError", Message: "no list_protocols"},
		"kind":     &RpcError{Type: "AttributeError", Message: "x", Kind: "method_not_implemented"},
		"code":     &RpcError{Type: "Whatever", Message: "x", Code: "UNIMPLEMENTED"},
		"http-404": &HTTPStatusError{StatusCode: 404, Detail: "not found", RequestID: "r1"},
		"wrapped":  fmt.Errorf("call: %w", &RpcError{Type: "X", Kind: "protocol_not_supported"}),
	}
	for name, err := range notHosted {
		t.Run(name, func(t *testing.T) {
			var target *ReflectionNotSupportedError
			if !errors.As(classifyReflectionNotHosted(err), &target) {
				t.Fatalf("%v not classified as no reflection", err)
			}
		})
	}
	var target *ReflectionNotSupportedError
	if errors.As(classifyReflectionNotHosted(notHosted["http-404"]), &target); target.RequestID != "r1" {
		t.Fatalf("404 request id lost: %+v", target.RpcError)
	}

	others := map[string]error{
		"app-error":   &RpcError{Type: "ValueError", Message: "boom"},
		"http-500":    &HTTPStatusError{StatusCode: 500, Detail: "boom"},
		"unavailable": &RpcError{Type: "TransportError", Message: "pipe closed", Code: "UNAVAILABLE"},
		"plain":       errors.New("vgirpc: TCP client is closed"),
	}
	for name, err := range others {
		t.Run(name, func(t *testing.T) {
			if got := classifyReflectionNotHosted(err); got != err {
				t.Fatalf("%v reclassified as %v", err, got)
			}
		})
	}
}
