// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"fmt"
	"net/netip"
	"regexp"
	"unicode/utf8"
)

const (
	irohProvider                = "iroh"
	IrohForwardedEndpointHeader = "VGI-Forwarded-Iroh-Endpoint"
)

var canonicalIrohEndpoint = regexp.MustCompile(`^[0-9a-f]{64}$`)

// IrohForwardedHeaderOptions configure trusted bridge identity evidence.
type IrohForwardedHeaderOptions struct {
	Issuer                string
	TrustedProxyAddresses []string
}

// IrohForwardedHeaderIdentityProvider resolves a bridge-sanitized EndpointId.
type IrohForwardedHeaderIdentityProvider struct {
	issuer         string
	trustedProxies map[netip.Addr]struct{}
}

// NewIrohForwardedHeaderIdentityProvider creates an opt-in HTTP adapter.
func NewIrohForwardedHeaderIdentityProvider(options IrohForwardedHeaderOptions) (*IrohForwardedHeaderIdentityProvider, error) {
	if options.Issuer == "" || !utf8.ValidString(options.Issuer) || containsIrohControl(options.Issuer) {
		return nil, fmt.Errorf("vgirpc: Iroh issuer must be a non-empty Unicode string without controls")
	}
	if len(options.TrustedProxyAddresses) == 0 {
		return nil, fmt.Errorf("vgirpc: at least one exact Iroh bridge address is required")
	}
	trusted := make(map[netip.Addr]struct{}, len(options.TrustedProxyAddresses))
	for _, value := range options.TrustedProxyAddresses {
		address, err := netip.ParseAddr(value)
		if err != nil || address.Zone() != "" {
			return nil, fmt.Errorf("vgirpc: Iroh bridge %q is not an exact IP address", value)
		}
		address = address.Unmap()
		if _, duplicate := trusted[address]; duplicate {
			return nil, fmt.Errorf("vgirpc: duplicate normalized Iroh bridge address %q", value)
		}
		trusted[address] = struct{}{}
	}
	return &IrohForwardedHeaderIdentityProvider{issuer: options.Issuer, trustedProxies: trusted}, nil
}

func (p *IrohForwardedHeaderIdentityProvider) Provider() string { return irohProvider }

// Resolve validates the immediate bridge before consuming the identity header.
func (p *IrohForwardedHeaderIdentityProvider) Resolve(_ context.Context, resolution *PeerResolutionContext) (*PeerIdentityResult, error) {
	if resolution == nil || !p.trusts(resolution.ImmediatePeer()) {
		return NewPeerIdentityResult(irohProvider, PeerIdentityUntrustedProxy)
	}
	endpointID, present, err := resolution.Header(IrohForwardedEndpointHeader)
	if err != nil {
		return NewPeerIdentityResult(irohProvider, PeerIdentityInvalid)
	}
	if !present {
		return NewPeerIdentityResult(irohProvider, PeerIdentityNoMatch)
	}
	if !canonicalIrohEndpoint.MatchString(endpointID) {
		return NewPeerIdentityResult(irohProvider, PeerIdentityInvalid)
	}
	identity, err := NewPeerIdentity(PeerIdentityOptions{
		Provider: irohProvider, EvidenceSource: "http_proxy",
		Assurance: IdentityAssuranceConfiguredProxy, Issuer: p.issuer, Transport: "http",
		SubjectKind: PeerSubjectEndpoint, SubjectKey: endpointID,
		SubjectStability: SubjectStabilityStable, SubjectVerified: true,
		Attributes:    map[string]any{"original_assurance": string(IdentityAssuranceCryptographicPeer)},
		SourceAddress: endpointID, ProxyAddress: resolution.ImmediatePeer(),
	})
	if err != nil {
		return NewPeerIdentityResult(irohProvider, PeerIdentityInvalid)
	}
	return NewAvailablePeerIdentityResult(irohProvider, identity)
}

func (p *IrohForwardedHeaderIdentityProvider) trusts(value string) bool {
	address, err := netip.ParseAddr(value)
	if err != nil {
		return false
	}
	_, ok := p.trustedProxies[address.Unmap()]
	return ok
}

func containsIrohControl(value string) bool {
	for _, character := range value {
		if character <= 0x1f || character == 0x7f {
			return true
		}
	}
	return false
}
