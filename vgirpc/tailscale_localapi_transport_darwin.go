// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

//go:build darwin

package vgirpc

import (
	"context"
	"os"
	"os/exec"
	"strconv"
)

func tailscalePlatformLocalAPITransport(ctx context.Context) (tailscaleLocalAPITransportConfig, error) {
	hooks := tailscaleMacDiscoveryHooks{
		runCommand: func(ctx context.Context, name string, arguments ...string) ([]byte, error) {
			return exec.CommandContext(ctx, name, arguments...).Output()
		},
		userID:    os.Getuid,
		readLink:  os.Readlink,
		readFile:  os.ReadFile,
		sharedDir: tailscaleMacSharedDir,
	}
	port, token, err := tailscaleDiscoverMacLocalAPI(ctx, hooks)
	if err != nil {
		// Official safesocket behavior falls back to the conventional Unix
		// socket when neither GUI variant has usable same-user proof files.
		return tailscaleUnixLocalAPITransport(tailscaleDefaultSocket), nil
	}
	return tailscaleDirectLocalAPITransport("http://127.0.0.1:"+strconv.Itoa(port), token), nil
}

func tailscaleExplicitNamedPipeLocalAPITransport(string) (tailscaleLocalAPITransportConfig, error) {
	return tailscaleLocalAPITransportConfig{}, errTailscaleNamedPipePlatform
}
