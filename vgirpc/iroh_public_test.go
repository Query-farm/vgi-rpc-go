// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc_test

import (
	"errors"
	"testing"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

func TestPublicIrohContractSmoke(t *testing.T) {
	const id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	endpoint, err := vgirpc.ParseIrohEndpoint("iroh://" + id)
	if err != nil || endpoint.ALPN != vgirpc.IrohArrowMuxALPN {
		t.Fatalf("public parser failed: %#v, %v", endpoint, err)
	}
	_, err = vgirpc.ParseIrohEndpoint("httpi://" + id + "/vgi/")
	var failure *vgirpc.IrohTransportError
	if !errors.As(err, &failure) || failure.DispatchCertainty != vgirpc.IrohNotSent {
		t.Fatalf("public structured error unavailable: %#v", err)
	}
}
