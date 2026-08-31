// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

const (
	testIssuer     = "tailnet:test"
	testCapability = "query.farm/cap/vgi-test"
	testTag        = "tag:vgi-ci-client"
)

func TestValidateContextRequiresCanonicalTCPAuthentication(t *testing.T) {
	identity := mustIdentity(t, vgirpc.PeerIdentityOptions{
		Provider: providerName, EvidenceSource: "localapi", Assurance: vgirpc.IdentityAssuranceLocalDaemon,
		Issuer: testIssuer, Transport: "tcp", SubjectKind: vgirpc.PeerSubjectTaggedNode,
		SubjectKey: "node:stable", SubjectStability: vgirpc.SubjectStabilityStable, SubjectVerified: true,
		Attributes: map[string]any{
			"tags":              []string{testTag},
			"capability_target": map[string]any{"kind": "destination_ip", "value": "100.64.0.1"},
		},
		Capabilities:         map[string]any{testCapability: []any{map[string]any{"role": "caller"}}},
		CapabilitiesVerified: true,
	})
	evidence := mustEvidence(t, identity)
	auth, err := vgirpc.PeerIdentityPrimary(providerName)(evidence, vgirpc.Anonymous())
	if err != nil {
		t.Fatalf("primary authentication: %v", err)
	}
	want := expectation{
		Issuer: testIssuer, Transport: "tcp", EvidenceSource: "localapi",
		Assurance: vgirpc.IdentityAssuranceLocalDaemon, SubjectKind: vgirpc.PeerSubjectTaggedNode,
		SubjectStability: vgirpc.SubjectStabilityStable, SubjectVerified: true,
		Capability: testCapability, CapabilityTarget: "destination_ip", Tag: testTag,
		Authenticated: true, AuthDomain: providerName, PrincipalMatches: true, BindingPresent: true,
	}
	ctx := &vgirpc.CallContext{Auth: auth, PeerEvidence: evidence}
	if err := validateContext(ctx, want); err != nil {
		t.Fatalf("valid TCP context rejected: %v", err)
	}

	wrongPrincipal := cloneAuth(auth)
	wrongPrincipal.Principal = "peer/tailscale/tailnet%3Atest/node%3Aother"
	if err := validateContext(&vgirpc.CallContext{Auth: wrongPrincipal, PeerEvidence: evidence}, want); err == nil {
		t.Fatal("non-canonical authentication principal was accepted")
	}

	wrongBinding := cloneAuth(auth)
	wrongBinding.Claims["peer_evidence_binding"] = "wrong"
	if err := validateContext(&vgirpc.CallContext{Auth: wrongBinding, PeerEvidence: evidence}, want); err == nil {
		t.Fatal("incorrect evidence binding was accepted")
	}

	wrongIssuer := want
	wrongIssuer.Issuer = "tailnet:other"
	if err := validateContext(ctx, wrongIssuer); err == nil {
		t.Fatal("incorrect issuer was accepted")
	}

	wrongTarget := want
	wrongTarget.CapabilityTarget = "service"
	if err := validateContext(ctx, wrongTarget); err == nil {
		t.Fatal("incorrect capability target was accepted")
	}
}

func TestValidateContextRequiresAnonymousBoundServeEvidence(t *testing.T) {
	identity := mustIdentity(t, vgirpc.PeerIdentityOptions{
		Provider: providerName, EvidenceSource: "serve_proxy", Assurance: vgirpc.IdentityAssuranceConfiguredProxy,
		Issuer: testIssuer, Transport: "http", SubjectKind: vgirpc.PeerSubjectUnknown,
		SubjectStability:     vgirpc.SubjectStabilityNone,
		Capabilities:         map[string]any{testCapability: []any{map[string]any{"role": "caller"}}},
		CapabilitiesVerified: true, ProxyAddress: "127.0.0.1",
	})
	evidence := mustEvidence(t, identity)
	auth, err := vgirpc.RequirePeerIdentity(providerName)(evidence, vgirpc.Anonymous())
	if err != nil {
		t.Fatalf("require peer identity: %v", err)
	}
	want := expectation{
		Issuer: testIssuer, Transport: "http", EvidenceSource: "serve_proxy",
		Assurance: vgirpc.IdentityAssuranceConfiguredProxy, SubjectKind: vgirpc.PeerSubjectUnknown,
		SubjectStability: vgirpc.SubjectStabilityNone, Capability: testCapability,
		BindingPresent: true, ProxyPresent: true,
	}
	if err := validateContext(&vgirpc.CallContext{Auth: auth, PeerEvidence: evidence}, want); err != nil {
		t.Fatalf("valid Serve context rejected: %v", err)
	}

	spoofed := cloneAuth(auth)
	spoofed.Authenticated = true
	spoofed.Domain = providerName
	spoofed.Principal = "login:attacker@example.invalid"
	if err := validateContext(&vgirpc.CallContext{Auth: spoofed, PeerEvidence: evidence}, want); err == nil {
		t.Fatal("Serve spoof promoted an anonymous capability identity")
	}
}

func TestValidateSnapshotRequiresIssuerBindingAndAnonymousServeShape(t *testing.T) {
	want := expectation{
		Issuer: testIssuer, Transport: "http", EvidenceSource: "serve_proxy",
		Assurance: vgirpc.IdentityAssuranceConfiguredProxy, SubjectKind: vgirpc.PeerSubjectUnknown,
		SubjectStability: vgirpc.SubjectStabilityNone, Capability: testCapability,
		BindingPresent: true, ProxyPresent: true, SpoofLogin: "attacker@example.invalid",
	}
	valid := []byte(`{
		"provider_status":{"tailscale":"available"},
		"identities":[{
			"provider":"tailscale","issuer":"tailnet:test","evidence_source":"serve_proxy",
			"assurance":"configured_proxy","transport":"http","subject_kind":"unknown",
			"subject_stability":"none","subject_verified":false,"subject_fingerprint":null,
			"tags":[],"capability_names":["query.farm/cap/vgi-test"],
			"capabilities_verified":true,"capability_target":null,"proxy_present":true
		}],
		"auth":{"authenticated":false,"domain":null,"principal_fingerprint":null,
			"principal_matches_identity":false,"peer_evidence_binding_present":true}
	}`)
	if err := validateSnapshot(valid, want); err != nil {
		t.Fatalf("valid snapshot rejected: %v", err)
	}

	for name, raw := range map[string][]byte{
		"issuer":  replaceJSON(valid, `"issuer":"tailnet:test"`, `"issuer":"tailnet:other"`),
		"binding": replaceJSON(valid, `"peer_evidence_binding_present":true`, `"peer_evidence_binding_present":false`),
		"subject": replaceJSON(valid, `"subject_fingerprint":null`, `"subject_fingerprint":"attacker"`),
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateSnapshot(raw, want); err == nil {
				t.Fatal("invalid snapshot was accepted")
			}
		})
	}
}

func mustIdentity(t *testing.T, options vgirpc.PeerIdentityOptions) *vgirpc.PeerIdentity {
	t.Helper()
	identity, err := vgirpc.NewPeerIdentity(options)
	if err != nil {
		t.Fatalf("new peer identity: %v", err)
	}
	return identity
}

func mustEvidence(t *testing.T, identity *vgirpc.PeerIdentity) *vgirpc.PeerEvidenceSet {
	t.Helper()
	result, err := vgirpc.NewAvailablePeerIdentityResult(providerName, identity)
	if err != nil {
		t.Fatalf("new peer identity result: %v", err)
	}
	evidence, err := vgirpc.NewPeerEvidenceSet(result)
	if err != nil {
		t.Fatalf("new peer evidence: %v", err)
	}
	return evidence
}

func cloneAuth(auth *vgirpc.AuthContext) *vgirpc.AuthContext {
	claims := make(map[string]any, len(auth.Claims))
	for key, value := range auth.Claims {
		claims[key] = value
	}
	return &vgirpc.AuthContext{
		Domain: auth.Domain, Authenticated: auth.Authenticated, Principal: auth.Principal, Claims: claims,
	}
}

func replaceJSON(raw []byte, old, replacement string) []byte {
	return []byte(strings.Replace(string(raw), old, replacement, 1))
}
