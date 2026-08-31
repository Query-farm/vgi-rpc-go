// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package vgirpc

import (
	"context"
	"net"

	"github.com/tailscale/go-winio"
	"golang.org/x/sys/windows"
)

func tailscalePlatformLocalAPITransport(context.Context) (tailscaleLocalAPITransportConfig, error) {
	return tailscaleWindowsPipeTransport(tailscaleWindowsPipe)
}

func tailscaleExplicitNamedPipeLocalAPITransport(path string) (tailscaleLocalAPITransportConfig, error) {
	return tailscaleWindowsPipeTransport(path)
}

func tailscaleWindowsPipeTransport(path string) (tailscaleLocalAPITransportConfig, error) {
	if err := tailscaleValidateNamedPipe(path); err != nil {
		return tailscaleLocalAPITransportConfig{}, err
	}
	return tailscaleNamedPipeLocalAPITransport(path, func(ctx context.Context, path string) (net.Conn, error) {
		return winio.DialPipeAccessImpLevel(
			ctx,
			path,
			windows.GENERIC_READ|windows.GENERIC_WRITE,
			winio.PipeImpLevelIdentification,
		)
	}), nil
}
