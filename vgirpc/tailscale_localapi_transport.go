// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	tailscaleWindowsPipe       = `\\.\pipe\ProtectedPrefix\Administrators\Tailscale\tailscaled`
	tailscaleMacSharedDir      = "/Library/Tailscale"
	tailscaleDiscoveryMaxBytes = 1 << 20
)

var errTailscaleNamedPipePlatform = errors.New("tailscale LocalAPI named pipes are only supported on Windows")

type tailscaleLocalAPITransportConfig struct {
	baseURL     string
	password    string
	dialContext func(context.Context, string, string) (net.Conn, error)
}

func tailscaleDirectLocalAPITransport(endpoint, password string) tailscaleLocalAPITransportConfig {
	dialer := &net.Dialer{}
	return tailscaleLocalAPITransportConfig{
		baseURL: endpoint, password: password, dialContext: dialer.DialContext,
	}
}

func tailscaleUnixLocalAPITransport(path string) tailscaleLocalAPITransportConfig {
	dialer := &net.Dialer{}
	return tailscaleLocalAPITransportConfig{
		baseURL: "http://" + tailscaleLocalAPIHost,
		dialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", path)
		},
	}
}

func tailscaleNamedPipeLocalAPITransport(
	path string,
	dialPipe func(context.Context, string) (net.Conn, error),
) tailscaleLocalAPITransportConfig {
	return tailscaleLocalAPITransportConfig{
		baseURL: "http://" + tailscaleLocalAPIHost,
		dialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialPipe(ctx, path)
		},
	}
}

type tailscaleMacDiscoveryHooks struct {
	runCommand func(context.Context, string, ...string) ([]byte, error)
	userID     func() int
	readLink   func(string) (string, error)
	readFile   func(string) ([]byte, error)
	sharedDir  string
}

// tailscaleDiscoverMacLocalAPI follows Tailscale safesocket's order: the App
// Store IPNExtension same-user-proof file first, then the standalone system
// extension's /Library/Tailscale files. Hooks keep the OS discovery logic
// independently testable without weakening production path checks.
func tailscaleDiscoverMacLocalAPI(ctx context.Context, hooks tailscaleMacDiscoveryHooks) (int, string, error) {
	if hooks.runCommand == nil || hooks.userID == nil || hooks.readLink == nil || hooks.readFile == nil {
		return 0, "", fmt.Errorf("incomplete macOS LocalAPI discovery hooks")
	}
	output, err := hooks.runCommand(ctx, "lsof", "-n", "-a", fmt.Sprintf("-u%d", hooks.userID()), "-c", "IPNExtension", "-F")
	if err == nil && len(output) <= tailscaleDiscoveryMaxBytes {
		if port, token, parseErr := tailscaleMacAppStoreCredentials(output); parseErr == nil {
			return port, token, nil
		}
	}
	sharedDir := hooks.sharedDir
	if sharedDir == "" {
		sharedDir = tailscaleMacSharedDir
	}
	portText, err := hooks.readLink(filepath.Join(sharedDir, "ipnport"))
	if err != nil {
		return 0, "", fmt.Errorf("tailscale macOS LocalAPI credentials not found: %w", err)
	}
	port, err := tailscaleMacPort(strings.TrimSpace(portText))
	if err != nil {
		return 0, "", err
	}
	tokenBytes, err := hooks.readFile(filepath.Join(sharedDir, "sameuserproof-"+strconv.Itoa(port)))
	if err != nil {
		return 0, "", fmt.Errorf("read Tailscale macOS same-user proof: %w", err)
	}
	token := strings.TrimSpace(string(tokenBytes))
	if err := tailscaleMacToken(token); err != nil {
		return 0, "", err
	}
	return port, token, nil
}

func tailscaleMacAppStoreCredentials(output []byte) (int, string, error) {
	if len(output) > tailscaleDiscoveryMaxBytes || !utf8.Valid(output) {
		return 0, "", fmt.Errorf("invalid Tailscale macOS lsof output")
	}
	const marker = ".tailscale.ipn.macos/sameuserproof-"
	for _, line := range strings.Split(string(output), "\n") {
		_, suffix, found := strings.Cut(line, marker)
		if !found {
			continue
		}
		portText, token, found := strings.Cut(strings.TrimSpace(suffix), "-")
		if !found || strings.Contains(token, "-") {
			continue
		}
		port, err := tailscaleMacPort(portText)
		if err != nil || tailscaleMacToken(token) != nil {
			continue
		}
		return port, token, nil
	}
	return 0, "", errors.New("tailscale App Store same-user proof not found")
}

func tailscaleMacPort(value string) (int, error) {
	port, err := strconv.Atoi(value)
	if err != nil || port < 1 || port > 65_535 {
		return 0, fmt.Errorf("invalid Tailscale macOS LocalAPI port")
	}
	return port, nil
}

func tailscaleMacToken(value string) error {
	// safesocket creates ten random bytes and renders them as lowercase hex.
	if len(value) != 20 {
		return fmt.Errorf("invalid Tailscale macOS same-user proof token")
	}
	for _, character := range value {
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
			return fmt.Errorf("invalid Tailscale macOS same-user proof token")
		}
	}
	return nil
}

func tailscaleValidateNamedPipe(path string) error {
	if path == "" || !utf8.ValidString(path) || strings.ContainsRune(path, 0) || tailscaleHasControl(path) {
		return fmt.Errorf("invalid Tailscale LocalAPI named pipe path")
	}
	return nil
}
