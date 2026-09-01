// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"
)

func tcpConnectionPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan net.Conn, 1)
	errs := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			errs <- acceptErr
			return
		}
		accepted <- conn
	}()
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	select {
	case server := <-accepted:
		return server, client
	case err := <-errs:
		_ = client.Close()
		t.Fatal(err)
	case <-time.After(time.Second):
		_ = client.Close()
		t.Fatal("accept timed out")
	}
	return nil, nil
}

func TestPrepareTcpConnectionIdentityTrustsProxyBeforeSnapshot(t *testing.T) {
	server, client := tcpConnectionPair(t)
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()
	preamble := proxyV2IPv4()
	if _, err := client.Write(append(preamble, 0xaa)); err != nil {
		t.Fatal(err)
	}

	options := TcpServerOptions{
		ProxyProtocolV2Required:     true,
		TrustedProxyAddresses:       []string{"127.0.0.1"},
		ProxyPreambleTimeout:        time.Second,
		MaximumProxyPreambleBytes:   536,
		IdentityResolutionTimeout:   time.Second,
		IdentityResolverConcurrency: 1,
		ServiceName:                 "svc:vgi-test",
		ResolveIdentity: func(_ context.Context, resolution *PeerResolutionContext) (*AuthContext, *PeerEvidenceSet, error) {
			if resolution.AssertedPeer() != "192.0.2.7:12345" ||
				resolution.DestinationAddress() != "198.51.100.9:9400" ||
				resolution.ServiceName() != "svc:vgi-test" {
				return nil, nil, fmt.Errorf("unexpected resolution snapshot: asserted=%q destination=%q service=%q",
					resolution.AssertedPeer(), resolution.DestinationAddress(), resolution.ServiceName())
			}
			return &AuthContext{Domain: "test", Authenticated: true, Principal: "alice"}, EmptyPeerEvidence(), nil
		},
	}
	ctx, _, err := prepareTcpConnectionIdentity(context.Background(), server, options,
		map[netip.Addr]struct{}{netip.MustParseAddr("127.0.0.1"): {}}, make(chan struct{}, 1))
	if err != nil {
		t.Fatal(err)
	}
	auth, _ := identityFromConnectionContext(ctx)
	if !auth.Authenticated || auth.Principal != "alice" {
		t.Fatalf("identity not installed: %#v", auth)
	}
	following := []byte{0}
	if _, err := io.ReadFull(server, following); err != nil || following[0] != 0xaa {
		t.Fatalf("VGI byte was not preserved: byte=%x err=%v", following, err)
	}
}

func TestPrepareTcpConnectionIdentityPromotesForwardedIrohPeer(t *testing.T) {
	server, client := tcpConnectionPair(t)
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()
	if _, err := client.Write(append(proxyV2Iroh(), 0xaa)); err != nil {
		t.Fatal(err)
	}
	options := TcpServerOptions{
		ProxyProtocolV2Required: true, TrustedProxyAddresses: []string{"127.0.0.1"},
		ProxyPreambleTimeout: time.Second, MaximumProxyPreambleBytes: 536,
		IdentityResolutionTimeout: time.Second, IrohProxyIssuer: "production-mesh",
		PeerAuthenticationPolicy: PeerIdentityPrimary("iroh"),
	}
	ctx, _, err := prepareTcpConnectionIdentity(context.Background(), server, options,
		map[netip.Addr]struct{}{netip.MustParseAddr("127.0.0.1"): {}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	auth, evidence := identityFromConnectionContext(ctx)
	if !auth.Authenticated || evidence.Status("iroh") != PeerIdentityAvailable {
		t.Fatalf("Iroh identity not installed: auth=%#v status=%s", auth, evidence.Status("iroh"))
	}
	identity := evidence.ForProvider("iroh")[0]
	if identity.Issuer() != "production-mesh" || identity.SubjectKey() != "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f" {
		t.Fatalf("unexpected Iroh identity: issuer=%q subject=%q", identity.Issuer(), identity.SubjectKey())
	}
	if identity.Assurance() != IdentityAssuranceConfiguredProxy || identity.Attributes()["original_assurance"] != "cryptographic_peer" {
		t.Fatalf("unexpected Iroh assurance: %s %#v", identity.Assurance(), identity.Attributes())
	}
	following := []byte{0}
	if _, err := io.ReadFull(server, following); err != nil || following[0] != 0xaa {
		t.Fatalf("VGI byte was not preserved: byte=%x err=%v", following, err)
	}
}

func TestPrepareTcpConnectionIdentityRejectsUntrustedPeerBeforeRead(t *testing.T) {
	server, client := tcpConnectionPair(t)
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()
	options := TcpServerOptions{
		ProxyProtocolV2Required: true, ProxyPreambleTimeout: time.Second,
		MaximumProxyPreambleBytes: 536, IdentityResolutionTimeout: time.Second,
	}
	started := time.Now()
	_, _, err := prepareTcpConnectionIdentity(context.Background(), server, options,
		map[netip.Addr]struct{}{netip.MustParseAddr("192.0.2.1"): {}}, nil)
	if err == nil {
		t.Fatal("untrusted immediate peer accepted")
	}
	if time.Since(started) > 100*time.Millisecond {
		t.Fatal("untrusted peer was read before rejection")
	}
}

func TestPrepareTcpConnectionIdentityBoundsLingeringResolver(t *testing.T) {
	firstServer, firstClient := net.Pipe()
	defer func() { _ = firstServer.Close(); _ = firstClient.Close() }()
	release := make(chan struct{})
	defer close(release)
	options := TcpServerOptions{
		IdentityResolutionTimeout:   20 * time.Millisecond,
		IdentityResolverConcurrency: 1,
		ResolveIdentity: func(context.Context, *PeerResolutionContext) (*AuthContext, *PeerEvidenceSet, error) {
			<-release
			return Anonymous(), EmptyPeerEvidence(), nil
		},
	}
	slots := make(chan struct{}, 1)
	if _, _, err := prepareTcpConnectionIdentity(context.Background(), firstServer, options, nil, slots); err == nil {
		t.Fatal("non-cooperative resolver did not time out")
	}
	secondServer, secondClient := net.Pipe()
	defer func() { _ = secondServer.Close(); _ = secondClient.Close() }()
	started := time.Now()
	if _, _, err := prepareTcpConnectionIdentity(context.Background(), secondServer, options, nil, slots); err == nil {
		t.Fatal("lingering resolver capacity was not retained")
	}
	if time.Since(started) > 10*time.Millisecond {
		t.Fatal("capacity exhaustion did not fail immediately")
	}
}

func TestTcpIdentityErrorClassNeverIncludesResolverDetail(t *testing.T) {
	const secret = "secret-daemon-token-certificate-capability"
	for name, testCase := range map[string]struct {
		err  error
		want string
	}{
		"unavailable": {NewAuthUnavailable(secret), "unavailable"},
		"rejected":    {NewAuthFailure(AuthReasonInvalidCredential, secret), "rejected"},
		"failed":      {fmt.Errorf("resolver failed with %s", secret), "failed"},
	} {
		t.Run(name, func(t *testing.T) {
			if got := tcpIdentityErrorClass(testCase.err); got != testCase.want || strings.Contains(got, secret) {
				t.Fatalf("class = %q, want %q", got, testCase.want)
			}
		})
	}
}

func TestTcpPeerExtensionPanicsAreContainedAndRedacted(t *testing.T) {
	const secret = "secret-tcp-provider-policy-panic-value"
	tests := []struct {
		name    string
		options TcpServerOptions
	}{
		{
			name: "provider",
			options: TcpServerOptions{
				IdentityResolutionTimeout:   time.Second,
				IdentityResolverConcurrency: 1,
				ResolveIdentity: func(context.Context, *PeerResolutionContext) (*AuthContext, *PeerEvidenceSet, error) {
					panic(secret)
				},
			},
		},
		{
			name: "policy",
			options: TcpServerOptions{
				IdentityResolutionTimeout: time.Second,
				PeerAuthenticationPolicy: func(*PeerEvidenceSet, *AuthContext) (*AuthContext, error) {
					panic(secret)
				},
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			server, client := net.Pipe()
			defer func() { _ = server.Close(); _ = client.Close() }()
			var slots chan struct{}
			if testCase.options.ResolveIdentity != nil {
				slots = make(chan struct{}, 1)
			}
			_, _, err := prepareTcpConnectionIdentity(context.Background(), server, testCase.options, nil, slots)
			if err == nil {
				t.Fatal("panicking extension was accepted")
			}
			if strings.Contains(err.Error(), secret) || tcpIdentityErrorClass(err) != "failed" {
				t.Fatalf("panic was not safely classified: %v", err)
			}
		})
	}
}

func TestProxyV2FixtureHasExpectedLength(t *testing.T) {
	fixture := proxyV2IPv4()
	if got := int(binary.BigEndian.Uint16(fixture[14:16])); got != len(fixture)-16 {
		t.Fatalf("fixture length = %d", got)
	}
}
