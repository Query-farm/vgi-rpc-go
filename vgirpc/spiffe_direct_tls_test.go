// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"
)

type directTLSFixture struct {
	server *tls.Config
	client *tls.Config
}

func directTLSConfig(t *testing.T, clientID string) directTLSFixture {
	t.Helper()
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test root"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		BasicConstraintsValid: true, IsCA: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	issue := func(serial int64, template *x509.Certificate) tls.Certificate {
		t.Helper()
		key, keyErr := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if keyErr != nil {
			t.Fatal(keyErr)
		}
		template.SerialNumber = big.NewInt(serial)
		template.NotBefore = now.Add(-time.Minute)
		template.NotAfter = now.Add(time.Hour)
		template.BasicConstraintsValid = true
		der, createErr := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
		if createErr != nil {
			t.Fatal(createErr)
		}
		return tls.Certificate{Certificate: [][]byte{der, caDER}, PrivateKey: key}
	}
	serverCertificate := issue(2, &x509.Certificate{
		Subject: pkix.Name{CommonName: "server"}, DNSNames: []string{"localhost"},
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	parsedID, err := url.Parse(clientID)
	if err != nil {
		t.Fatal(err)
	}
	clientCertificate := issue(3, &x509.Certificate{
		Subject: pkix.Name{CommonName: "client"}, URIs: []*url.URL{parsedID},
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	})
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	return directTLSFixture{
		server: &tls.Config{Certificates: []tls.Certificate{serverCertificate}, ClientCAs: roots, MinVersion: tls.VersionTLS13},
		client: &tls.Config{Certificates: []tls.Certificate{clientCertificate}, RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS13},
	}
}

type preparedTLSResult struct {
	ctx  context.Context
	conn net.Conn
	err  error
}

func prepareTLSConnection(t *testing.T, options TcpServerOptions, clientConfig *tls.Config, proxy bool) preparedTLSResult {
	t.Helper()
	if err := prepareDirectSpiffeTLSOptions(&options); err != nil {
		t.Fatal(err)
	}
	server, client := tcpConnectionPair(t)
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	trusted := map[netip.Addr]struct{}{}
	if proxy {
		trusted[netip.MustParseAddr("127.0.0.1")] = struct{}{}
		if _, err := client.Write(proxyV2IPv4()); err != nil {
			t.Fatal(err)
		}
	}
	result := make(chan preparedTLSResult, 1)
	go func() {
		ctx, conn, err := prepareTcpConnectionIdentity(context.Background(), server, options, trusted, make(chan struct{}, 1))
		result <- preparedTLSResult{ctx: ctx, conn: conn, err: err}
	}()
	tlsClient := tls.Client(client, clientConfig.Clone())
	if err := tlsClient.Handshake(); err != nil {
		t.Fatal(err)
	}
	if _, err := tlsClient.Write([]byte{0xa5}); err != nil {
		t.Fatal(err)
	}
	select {
	case prepared := <-result:
		if prepared.err == nil {
			var following [1]byte
			if _, err := io.ReadFull(prepared.conn, following[:]); err != nil || following[0] != 0xa5 {
				t.Fatalf("post-handshake byte = %x, %v", following, err)
			}
		}
		return prepared
	case <-time.After(2 * time.Second):
		t.Fatal("direct TLS preparation timed out")
	}
	return preparedTLSResult{}
}

func TestDirectSpiffeTLSProducesObservedConnectionEvidence(t *testing.T) {
	fixture := directTLSConfig(t, "spiffe://example.org/ns/default/sa/client")
	prepared := prepareTLSConnection(t, TcpServerOptions{
		TLSConfig: fixture.server, TLSHandshakeTimeout: time.Second,
		SpiffeTrustDomains: []string{"example.org"},
	}, fixture.client, false)
	if prepared.err != nil {
		t.Fatal(prepared.err)
	}
	auth, evidence := identityFromConnectionContext(prepared.ctx)
	if auth.Authenticated {
		t.Fatal("direct TLS implicitly authenticated the connection without a policy")
	}
	identity, err := evidence.RequireUsableProvider(spiffeProvider)
	if err != nil {
		t.Fatal(err)
	}
	if identity.SubjectKey() != "spiffe://example.org/ns/default/sa/client" ||
		identity.EvidenceSource() != "direct_tls" || identity.Assurance() != IdentityAssuranceCryptographicPeer ||
		identity.Transport() != "tcp" || identity.ProxyAddress() != "" {
		t.Fatalf("unexpected direct TLS identity: subject=%q source=%q assurance=%q transport=%q proxy=%q",
			identity.SubjectKey(), identity.EvidenceSource(), identity.Assurance(), identity.Transport(), identity.ProxyAddress())
	}
}

func TestDirectSpiffeTLSProxyPreamblePrecedesHandshakeAndPolicy(t *testing.T) {
	fixture := directTLSConfig(t, "spiffe://example.org/workload/client")
	prepared := prepareTLSConnection(t, TcpServerOptions{
		ProxyProtocolV2Required: true, TrustedProxyAddresses: []string{"127.0.0.1"},
		ProxyPreambleTimeout: time.Second, MaximumProxyPreambleBytes: 536,
		TLSConfig: fixture.server, TLSHandshakeTimeout: time.Second,
		SpiffeTrustDomains: []string{"example.org"}, PeerAuthenticationPolicy: PeerIdentityPrimary(spiffeProvider),
	}, fixture.client, true)
	if prepared.err != nil {
		t.Fatal(prepared.err)
	}
	auth, evidence := identityFromConnectionContext(prepared.ctx)
	if !auth.Authenticated || auth.Domain != spiffeProvider || auth.Claims["subject"] != "spiffe://example.org/workload/client" {
		t.Fatalf("explicit policy did not authenticate direct SVID: %#v", auth)
	}
	identity := evidence.ForProvider(spiffeProvider)[0]
	if identity.SourceAddress() != "192.0.2.7:12345" || !strings.HasPrefix(identity.ProxyAddress(), "127.0.0.1:") {
		t.Fatalf("PROXY-before-TLS addresses = source %q proxy %q", identity.SourceAddress(), identity.ProxyAddress())
	}
}

func TestDirectSpiffeTLSRejectsDisallowedTrustDomainAfterVerifiedHandshake(t *testing.T) {
	fixture := directTLSConfig(t, "spiffe://other.org/workload/client")
	prepared := prepareTLSConnection(t, TcpServerOptions{
		TLSConfig: fixture.server, TLSHandshakeTimeout: time.Second,
		SpiffeTrustDomains: []string{"example.org"},
	}, fixture.client, false)
	if prepared.err == nil || !strings.Contains(prepared.err.Error(), "trust domain is not allowed") {
		t.Fatalf("disallowed trust domain error = %v", prepared.err)
	}
}

func TestDirectSpiffeTLSRequiresClientCertificateOnTheWire(t *testing.T) {
	fixture := directTLSConfig(t, "spiffe://example.org/workload/client")
	options := TcpServerOptions{
		TLSConfig: fixture.server, TLSHandshakeTimeout: time.Second,
		SpiffeTrustDomains: []string{"example.org"},
	}
	if err := prepareDirectSpiffeTLSOptions(&options); err != nil {
		t.Fatal(err)
	}
	server, client := tcpConnectionPair(t)
	defer func() { _ = server.Close(); _ = client.Close() }()
	result := make(chan error, 1)
	go func() {
		_, _, err := prepareTcpConnectionIdentity(context.Background(), server, options, nil, nil)
		result <- err
	}()
	clientConfig := fixture.client.Clone()
	clientConfig.Certificates = nil
	_ = tls.Client(client, clientConfig).Handshake()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("client without a certificate completed direct mTLS")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("missing-client-certificate handshake did not fail promptly")
	}
}

func TestDirectSpiffeTLSHandshakeIsBounded(t *testing.T) {
	fixture := directTLSConfig(t, "spiffe://example.org/workload/client")
	options := TcpServerOptions{
		TLSConfig: fixture.server, TLSHandshakeTimeout: 20 * time.Millisecond,
		SpiffeTrustDomains: []string{"example.org"},
	}
	if err := prepareDirectSpiffeTLSOptions(&options); err != nil {
		t.Fatal(err)
	}
	server, client := net.Pipe()
	defer func() { _ = server.Close(); _ = client.Close() }()
	started := time.Now()
	_, _, err := prepareTcpConnectionIdentity(context.Background(), server, options, nil, nil)
	if err == nil {
		t.Fatal("silent TLS peer was accepted")
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("TLS handshake exceeded its deadline: %s", elapsed)
	}
}

func TestDirectSpiffeTLSConfigurationFailsClosedAndIsCloned(t *testing.T) {
	fixture := directTLSConfig(t, "spiffe://example.org/workload/client")
	if err := prepareDirectSpiffeTLSOptions(&TcpServerOptions{SpiffeTrustDomains: []string{"example.org"}}); err == nil {
		t.Fatal("SPIFFE trust domain without TLS was accepted")
	}
	if err := prepareDirectSpiffeTLSOptions(&TcpServerOptions{TLSConfig: fixture.server}); err == nil {
		t.Fatal("direct TLS without a SPIFFE trust domain was accepted")
	}
	if err := prepareDirectSpiffeTLSOptions(&TcpServerOptions{TLSConfig: fixture.server, SpiffeTrustDomains: []string{"example.org"}, TLSHandshakeTimeout: -time.Second}); err == nil {
		t.Fatal("negative TLS handshake timeout was accepted")
	}
	missingRoots := fixture.server.Clone()
	missingRoots.ClientCAs = nil
	if err := prepareDirectSpiffeTLSOptions(&TcpServerOptions{TLSConfig: missingRoots, SpiffeTrustDomains: []string{"example.org"}}); err == nil {
		t.Fatal("TLS config without client trust roots was accepted")
	}
	callback := fixture.server.Clone()
	callback.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) { return &tls.Config{}, nil }
	if err := prepareDirectSpiffeTLSOptions(&TcpServerOptions{TLSConfig: callback, SpiffeTrustDomains: []string{"example.org"}}); err == nil {
		t.Fatal("GetConfigForClient escape hatch was accepted")
	}
	original := fixture.server.Clone()
	original.ClientAuth = tls.NoClientCert
	options := TcpServerOptions{TLSConfig: original, SpiffeTrustDomains: []string{"example.org"}}
	if err := prepareDirectSpiffeTLSOptions(&options); err != nil {
		t.Fatal(err)
	}
	if options.TLSConfig == original || options.TLSConfig.ClientAuth != tls.RequireAndVerifyClientCert || original.ClientAuth != tls.NoClientCert {
		t.Fatal("direct TLS did not clone and enforce RequireAndVerifyClientCert")
	}
	if options.TLSConfig.ClientCAs == original.ClientCAs {
		t.Fatal("direct TLS retained the caller-owned client trust pool")
	}
	preparedRoots := options.TLSConfig.ClientCAs.Clone()
	extra := directTLSConfig(t, "spiffe://example.org/workload/extra")
	for _, der := range extra.client.Certificates[0].Certificate[1:] {
		certificate, err := x509.ParseCertificate(der)
		if err != nil {
			t.Fatal(err)
		}
		original.ClientCAs.AddCert(certificate)
	}
	if !options.TLSConfig.ClientCAs.Equal(preparedRoots) {
		t.Fatal("mutating caller trust roots changed the prepared TLS snapshot")
	}
	originalByte := options.TLSConfig.Certificates[0].Certificate[0][0]
	original.Certificates[0].Certificate[0][0] ^= 0xff
	if options.TLSConfig.Certificates[0].Certificate[0][0] != originalByte {
		t.Fatal("mutating caller certificate DER changed the prepared TLS snapshot")
	}
}

func TestTcpSecurityStagesShareOneMonotonicSetupBudget(t *testing.T) {
	fixture := directTLSConfig(t, "spiffe://example.org/workload/client")
	remaining := make(chan time.Duration, 1)
	options := TcpServerOptions{
		ProxyProtocolV2Required: true, ProxyPreambleTimeout: 300 * time.Millisecond,
		MaximumProxyPreambleBytes: 536, TLSConfig: fixture.server,
		TLSHandshakeTimeout: 300 * time.Millisecond, SpiffeTrustDomains: []string{"example.org"},
		IdentityResolutionTimeout: 300 * time.Millisecond,
		ResolveIdentity: func(_ context.Context, resolution *PeerResolutionContext) (*AuthContext, *PeerEvidenceSet, error) {
			remaining <- time.Until(resolution.Deadline())
			return Anonymous(), EmptyPeerEvidence(), nil
		},
	}
	if err := prepareDirectSpiffeTLSOptions(&options); err != nil {
		t.Fatal(err)
	}
	server, client := tcpConnectionPair(t)
	defer func() { _ = server.Close(); _ = client.Close() }()
	result := make(chan error, 1)
	go func() {
		_, _, err := prepareTcpConnectionIdentity(context.Background(), server, options,
			map[netip.Addr]struct{}{netip.MustParseAddr("127.0.0.1"): {}}, make(chan struct{}, 1))
		result <- err
	}()
	time.Sleep(150 * time.Millisecond)
	if _, err := client.Write(proxyV2IPv4()); err != nil {
		t.Fatal(err)
	}
	if err := tls.Client(client, fixture.client.Clone()).Handshake(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shared connection setup did not complete")
	}
	select {
	case left := <-remaining:
		if left <= 0 || left >= 220*time.Millisecond {
			t.Fatalf("identity stage received a fresh timeout instead of the shared budget: %s", left)
		}
	default:
		t.Fatal("identity resolver did not report its remaining setup budget")
	}
}

func TestDirectSpiffeTLSRejectsDuplicateResolverEvidence(t *testing.T) {
	fixture := directTLSConfig(t, "spiffe://example.org/workload/client")
	duplicate, err := NewPeerIdentity(PeerIdentityOptions{
		Provider: spiffeProvider, EvidenceSource: "test", Assurance: IdentityAssuranceLocalDaemon,
		Issuer: "spiffe://example.org", Transport: "tcp", SubjectKind: PeerSubjectWorkload,
		SubjectKey: "spiffe://example.org/duplicate", SubjectStability: SubjectStabilityStable, SubjectVerified: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	duplicateResult, _ := NewAvailablePeerIdentityResult(spiffeProvider, duplicate)
	duplicateEvidence, _ := NewPeerEvidenceSet(duplicateResult)
	prepared := prepareTLSConnection(t, TcpServerOptions{
		TLSConfig: fixture.server, TLSHandshakeTimeout: time.Second,
		SpiffeTrustDomains: []string{"example.org"}, IdentityResolutionTimeout: time.Second,
		ResolveIdentity: func(context.Context, *PeerResolutionContext) (*AuthContext, *PeerEvidenceSet, error) {
			return Anonymous(), duplicateEvidence, nil
		},
	}, fixture.client, false)
	if prepared.err == nil || !strings.Contains(prepared.err.Error(), "duplicate peer identity provider") {
		t.Fatalf("duplicate SPIFFE evidence error = %v", prepared.err)
	}
}
