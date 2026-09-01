// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"strings"
	"testing"
)

const testIrohEndpoint = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"

func irohHTTPResolution(t *testing.T, peer string, values []string) *PeerResolutionContext {
	t.Helper()
	resolution, err := NewPeerResolutionContext("http", PeerResolutionOptions{
		ImmediatePeer: peer,
		Headers:       map[string][]string{IrohForwardedEndpointHeader: values},
	})
	if err != nil {
		t.Fatal(err)
	}
	return resolution
}

func TestIrohForwardedHeaderIdentityProvider(t *testing.T) {
	provider, err := NewIrohForwardedHeaderIdentityProvider(IrohForwardedHeaderOptions{
		Issuer: "production-mesh", TrustedProxyAddresses: []string{"127.0.0.1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := provider.Resolve(context.Background(), irohHTTPResolution(t, "127.0.0.1", []string{testIrohEndpoint}))
	if err != nil || result.Status() != PeerIdentityAvailable {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	identity := result.Identities()[0]
	if identity.Issuer() != "production-mesh" || identity.SubjectKey() != testIrohEndpoint || identity.Assurance() != IdentityAssuranceConfiguredProxy {
		t.Fatalf("unexpected identity: issuer=%q subject=%q assurance=%q", identity.Issuer(), identity.SubjectKey(), identity.Assurance())
	}
	evidence, _ := NewPeerEvidenceSet(result)
	auth, err := PeerIdentityPrimary(irohProvider)(evidence, Anonymous())
	if err != nil || !auth.Authenticated || !strings.Contains(auth.Principal, "/production-mesh/") {
		t.Fatalf("identity was not promoted: auth=%#v err=%v", auth, err)
	}
}

func TestIrohForwardedHeaderIdentityProviderFailsClosed(t *testing.T) {
	provider, err := NewIrohForwardedHeaderIdentityProvider(IrohForwardedHeaderOptions{
		Issuer: "production-mesh", TrustedProxyAddresses: []string{"127.0.0.1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		peer   string
		values []string
		status PeerIdentityStatus
	}{
		{"untrusted", "192.0.2.1", []string{testIrohEndpoint}, PeerIdentityUntrustedProxy},
		{"missing", "127.0.0.1", nil, PeerIdentityNoMatch},
		{"duplicate", "127.0.0.1", []string{testIrohEndpoint, testIrohEndpoint}, PeerIdentityInvalid},
		{"uppercase", "127.0.0.1", []string{strings.ToUpper(testIrohEndpoint)}, PeerIdentityInvalid},
		{"short", "127.0.0.1", []string{"00"}, PeerIdentityInvalid},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			result, resolveErr := provider.Resolve(context.Background(), irohHTTPResolution(t, testCase.peer, testCase.values))
			if resolveErr != nil || result.Status() != testCase.status {
				t.Fatalf("status=%s err=%v", result.Status(), resolveErr)
			}
		})
	}
}
