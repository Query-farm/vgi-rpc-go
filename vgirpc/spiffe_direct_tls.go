// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sort"
	"time"
)

const defaultTLSHandshakeTimeout = 5 * time.Second

// prepareDirectSpiffeTLSOptions validates and snapshots the caller's TLS
// boundary before the listener accepts connections. GetConfigForClient is
// deliberately rejected because a callback could replace the enforced mTLS
// policy during the handshake.
func prepareDirectSpiffeTLSOptions(options *TcpServerOptions) error {
	if options.TLSConfig == nil {
		if len(options.SpiffeTrustDomains) != 0 {
			return fmt.Errorf("vgirpc: SPIFFE trust domains require a TCP TLS config")
		}
		return nil
	}
	if len(options.SpiffeTrustDomains) == 0 {
		return fmt.Errorf("vgirpc: direct TCP TLS requires at least one SPIFFE trust domain")
	}
	if options.TLSHandshakeTimeout < 0 {
		return fmt.Errorf("vgirpc: TLS handshake timeout must not be negative")
	}
	if options.TLSHandshakeTimeout == 0 {
		options.TLSHandshakeTimeout = defaultTLSHandshakeTimeout
	}
	if options.TLSConfig.ClientCAs == nil {
		return fmt.Errorf("vgirpc: direct SPIFFE TLS requires client trust roots")
	}
	if len(options.TLSConfig.Certificates) == 0 && options.TLSConfig.GetCertificate == nil {
		return fmt.Errorf("vgirpc: direct SPIFFE TLS requires a server certificate")
	}
	if options.TLSConfig.GetConfigForClient != nil {
		return fmt.Errorf("vgirpc: direct SPIFFE TLS does not allow GetConfigForClient")
	}
	domains, err := directSpiffeTrustDomains(options.SpiffeTrustDomains)
	if err != nil {
		return err
	}
	options.SpiffeTrustDomains = domains
	config := options.TLSConfig.Clone()
	config.ClientCAs = options.TLSConfig.ClientCAs.Clone()
	if options.TLSConfig.RootCAs != nil {
		config.RootCAs = options.TLSConfig.RootCAs.Clone()
	}
	config.Certificates = cloneTLSCertificates(options.TLSConfig.Certificates)
	// NameToCertificate is deprecated but Clone preserves a caller-provided
	// map. Rebuild it from our deep-cloned certificate slice so later caller
	// mutation cannot change the active server snapshot.
	//lint:ignore SA1019 Compatibility hardening for deprecated caller input.
	if options.TLSConfig.NameToCertificate != nil {
		//lint:ignore SA1019 Compatibility hardening for deprecated caller input.
		config.NameToCertificate = nil
		//lint:ignore SA1019 Compatibility hardening for deprecated caller input.
		config.BuildNameToCertificate()
	}
	config.ClientAuth = tls.RequireAndVerifyClientCert
	options.TLSConfig = config
	return nil
}

func cloneTLSCertificates(certificates []tls.Certificate) []tls.Certificate {
	if certificates == nil {
		return nil
	}
	cloned := make([]tls.Certificate, len(certificates))
	for index, certificate := range certificates {
		cloned[index] = certificate
		cloned[index].Certificate = make([][]byte, len(certificate.Certificate))
		for chainIndex, der := range certificate.Certificate {
			cloned[index].Certificate[chainIndex] = append([]byte(nil), der...)
		}
		cloned[index].OCSPStaple = append([]byte(nil), certificate.OCSPStaple...)
		cloned[index].SignedCertificateTimestamps = make([][]byte, len(certificate.SignedCertificateTimestamps))
		for timestampIndex, timestamp := range certificate.SignedCertificateTimestamps {
			cloned[index].SignedCertificateTimestamps[timestampIndex] = append([]byte(nil), timestamp...)
		}
		// Force crypto/tls to parse a leaf from the detached DER instead of
		// retaining a caller-owned, mutable x509.Certificate pointer.
		cloned[index].Leaf = nil
	}
	return cloned
}

func directSpiffeTrustDomains(values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("vgirpc: at least one SPIFFE trust domain is required")
	}
	seen := make(map[string]struct{}, len(values))
	for _, domain := range values {
		if !spiffeTrustDomainPattern.MatchString(domain) {
			return nil, fmt.Errorf("vgirpc: invalid SPIFFE trust domain %q", domain)
		}
		if _, duplicate := seen[domain]; duplicate {
			return nil, fmt.Errorf("vgirpc: duplicate SPIFFE trust domain %q", domain)
		}
		seen[domain] = struct{}{}
	}
	domains := append([]string(nil), values...)
	sort.Strings(domains)
	return domains, nil
}

func acceptDirectSpiffeTLS(
	ctx context.Context,
	conn net.Conn,
	options TcpServerOptions,
	immediate string,
	asserted string,
	destination string,
) (net.Conn, *PeerIdentityResult, error) {
	deadline := tcpStageDeadline(ctx, options.TLSHandshakeTimeout)
	if err := conn.SetDeadline(deadline); err != nil {
		return nil, nil, fmt.Errorf("vgirpc: set TLS handshake deadline: %w", err)
	}
	handshakeCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	tlsConn := tls.Server(conn, options.TLSConfig)
	if err := tlsConn.HandshakeContext(handshakeCtx); err != nil {
		_ = conn.SetDeadline(time.Time{})
		return nil, nil, fmt.Errorf("vgirpc: direct SPIFFE TLS handshake: %w", err)
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return nil, nil, fmt.Errorf("vgirpc: clear TLS handshake deadline: %w", err)
	}
	state := tlsConn.ConnectionState()
	if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
		return nil, nil, fmt.Errorf("vgirpc: direct SPIFFE TLS did not verify a client certificate")
	}
	domains := make(map[string]struct{}, len(options.SpiffeTrustDomains))
	for _, domain := range options.SpiffeTrustDomains {
		domains[domain] = struct{}{}
	}
	resolution, err := NewPeerResolutionContext("tcp", PeerResolutionOptions{
		ImmediatePeer: immediate, AssertedPeer: asserted, DestinationAddress: destination,
		ServiceName: options.ServiceName,
	})
	if err != nil {
		return nil, nil, err
	}
	identity, err := spiffeIdentityFromCertificateEvidence(
		state.PeerCertificates[0], domains, "direct_tls",
		IdentityAssuranceCryptographicPeer, "tcp", resolution,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("vgirpc: invalid direct X.509-SVID: %w", err)
	}
	result, err := NewAvailablePeerIdentityResult(spiffeProvider, identity)
	if err != nil {
		return nil, nil, err
	}
	return tlsConn, result, nil
}

func appendPeerIdentityResult(evidence *PeerEvidenceSet, result *PeerIdentityResult) (*PeerEvidenceSet, error) {
	if evidence == nil {
		evidence = EmptyPeerEvidence()
	}
	providers := make([]string, 0, len(evidence.statuses))
	for provider := range evidence.statuses {
		providers = append(providers, provider)
	}
	sort.Strings(providers)
	results := make([]*PeerIdentityResult, 0, len(providers)+1)
	for _, provider := range providers {
		status := evidence.statuses[provider]
		var existing *PeerIdentityResult
		var err error
		if status == PeerIdentityAvailable {
			existing, err = NewAvailablePeerIdentityResult(provider, evidence.ForProvider(provider)...)
		} else {
			existing, err = NewPeerIdentityResult(provider, status)
		}
		if err != nil {
			return nil, err
		}
		results = append(results, existing)
	}
	results = append(results, result)
	return NewPeerEvidenceSet(results...)
}
