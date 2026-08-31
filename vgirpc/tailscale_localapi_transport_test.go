// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const tailscaleTestMacToken = "0123456789abcdefabcd"

func TestTailscaleMacAppStoreDiscoveryPrecedesStandalone(t *testing.T) {
	var command string
	var arguments []string
	readStandalone := false
	hooks := tailscaleMacDiscoveryHooks{
		runCommand: func(_ context.Context, name string, args ...string) ([]byte, error) {
			command = name
			arguments = append([]string(nil), args...)
			return []byte("p42\nn/Users/alice/Library/Group Containers/W5364U7YZB.group.io.tailscale.ipn.macos/sameuserproof-49152-" + tailscaleTestMacToken + "\n"), nil
		},
		userID: func() int { return 501 },
		readLink: func(string) (string, error) {
			readStandalone = true
			return "", os.ErrNotExist
		},
		readFile: func(string) ([]byte, error) {
			readStandalone = true
			return nil, os.ErrNotExist
		},
		sharedDir: "/Library/Tailscale",
	}
	port, token, err := tailscaleDiscoverMacLocalAPI(context.Background(), hooks)
	if err != nil || port != 49152 || token != tailscaleTestMacToken {
		t.Fatalf("discovery = (%d, %q, %v)", port, token, err)
	}
	if command != "lsof" || !reflect.DeepEqual(arguments, []string{"-n", "-a", "-u501", "-c", "IPNExtension", "-F"}) {
		t.Fatalf("command = %q %q", command, arguments)
	}
	if readStandalone {
		t.Fatal("standalone files read despite valid App Store credentials")
	}
}

func TestTailscaleMacStandaloneDiscoveryFallback(t *testing.T) {
	temporary := t.TempDir()
	var readPath string
	hooks := tailscaleMacDiscoveryHooks{
		runCommand: func(context.Context, string, ...string) ([]byte, error) {
			return nil, errors.New("IPNExtension not running")
		},
		userID: func() int { return 501 },
		readLink: func(path string) (string, error) {
			if path != filepath.Join(temporary, "ipnport") {
				t.Fatalf("readLink path = %q", path)
			}
			return "49153", nil
		},
		readFile: func(path string) ([]byte, error) {
			readPath = path
			return []byte(tailscaleTestMacToken + "\n"), nil
		},
		sharedDir: temporary,
	}
	port, token, err := tailscaleDiscoverMacLocalAPI(context.Background(), hooks)
	if err != nil || port != 49153 || token != tailscaleTestMacToken {
		t.Fatalf("discovery = (%d, %q, %v)", port, token, err)
	}
	if readPath != filepath.Join(temporary, "sameuserproof-49153") {
		t.Fatalf("readFile path = %q", readPath)
	}
}

func TestTailscaleMacDiscoveryRejectsStaleOrMalformedCredentials(t *testing.T) {
	tests := []struct {
		name  string
		lsof  []byte
		port  string
		token string
	}{
		{name: "invalid-app-and-missing-standalone", lsof: []byte("nsameuserproof-49152-short\n")},
		{name: "invalid-port", port: "70000", token: tailscaleTestMacToken},
		{name: "uppercase-token", port: "49152", token: "0123456789ABCDEFABCD"},
		{name: "token-with-control", port: "49152", token: "0123456789abcdefabc\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hooks := tailscaleMacDiscoveryHooks{
				runCommand: func(context.Context, string, ...string) ([]byte, error) { return test.lsof, nil },
				userID:     func() int { return 501 },
				readLink: func(string) (string, error) {
					if test.port == "" {
						return "", os.ErrNotExist
					}
					return test.port, nil
				},
				readFile: func(string) ([]byte, error) { return []byte(test.token), nil },
			}
			if _, _, err := tailscaleDiscoverMacLocalAPI(context.Background(), hooks); err == nil {
				t.Fatal("malformed discovery unexpectedly succeeded")
			}
		})
	}
}

func TestTailscaleNamedPipeTransportUsesInjectedDialer(t *testing.T) {
	wantErr := errors.New("dial sentinel")
	var gotPath string
	config := tailscaleNamedPipeLocalAPITransport(tailscaleWindowsPipe, func(_ context.Context, path string) (net.Conn, error) {
		gotPath = path
		return nil, wantErr
	})
	if config.baseURL != "http://"+tailscaleLocalAPIHost || config.password != "" {
		t.Fatalf("named-pipe config = %#v", config)
	}
	_, err := config.dialContext(context.Background(), "tcp", "ignored:80")
	if !errors.Is(err, wantErr) || gotPath != tailscaleWindowsPipe {
		t.Fatalf("dial = (%q, %v)", gotPath, err)
	}
}

func TestTailscaleDefaultPlatformTransportAndNamedPipeValidation(t *testing.T) {
	config, err := tailscalePlatformLocalAPITransport(context.Background())
	nativeSocket := config.baseURL == "http://"+tailscaleLocalAPIHost && config.password == ""
	macSameUserProof := strings.HasPrefix(config.baseURL, "http://127.0.0.1:") && config.password != ""
	if err != nil || (!nativeSocket && !macSameUserProof) || config.dialContext == nil {
		t.Fatalf("default platform transport invalid: endpoint=%q has_password=%v err=%v", config.baseURL, config.password != "", err)
	}
	for _, path := range []string{"", "bad\x00pipe", "bad\npipe"} {
		if err := tailscaleValidateNamedPipe(path); err == nil {
			t.Errorf("named pipe %q unexpectedly accepted", path)
		}
	}
}
