// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

//go:build !darwin && !windows

package vgirpc

import "context"

func tailscalePlatformLocalAPITransport(context.Context) (tailscaleLocalAPITransportConfig, error) {
	return tailscaleUnixLocalAPITransport(tailscaleDefaultSocket), nil
}

func tailscaleExplicitNamedPipeLocalAPITransport(string) (tailscaleLocalAPITransportConfig, error) {
	return tailscaleLocalAPITransportConfig{}, errTailscaleNamedPipePlatform
}
