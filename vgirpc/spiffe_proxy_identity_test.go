// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/url"
	"testing"
	"time"
)

func spiffeTestCertificate(t *testing.T, uris []string, ca bool) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	parsedURIs := make([]*url.URL, 0, len(uris))
	for _, value := range uris {
		parsed, err := url.Parse(value)
		if err != nil {
			t.Fatal(err)
		}
		parsedURIs = append(parsedURIs, parsed)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		URIs: parsedURIs, BasicConstraintsValid: true, IsCA: ca,
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}
	if ca {
		template.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageCRLSign
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return url.PathEscape(string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})))
}

func spiffeTestContext(t *testing.T, headers map[string][]string) *PeerResolutionContext {
	t.Helper()
	resolution, err := NewPeerResolutionContext("http", PeerResolutionOptions{ImmediatePeer: "127.0.0.1", AssertedPeer: "10.0.0.7:1234", Headers: headers})
	if err != nil {
		t.Fatal(err)
	}
	return resolution
}

func spiffeTestBoundary() SpiffeProxyOptions {
	return SpiffeProxyOptions{TrustDomains: []string{"example.org"}, TrustedProxyAddresses: []string{"127.0.0.1"}}
}

func TestSpiffeX509HeaderProviderRequiresVerifiedValidSVID(t *testing.T) {
	provider, err := NewSpiffeX509HeaderProvider(SpiffeX509HeaderOptions{SpiffeProxyOptions: spiffeTestBoundary(), VerificationHeader: "X-Client-Cert-Verified"})
	if err != nil {
		t.Fatal(err)
	}
	certificate := spiffeTestCertificate(t, []string{"spiffe://example.org/ns/default/sa/worker"}, false)
	result, err := provider.Resolve(context.Background(), spiffeTestContext(t, map[string][]string{
		"X-SSL-Client-Cert": {certificate}, "X-Client-Cert-Verified": {"true"},
	}))
	if err != nil || result.Status() != PeerIdentityAvailable {
		t.Fatalf("Resolve() = (%v, %v)", result, err)
	}
	identity := result.Identities()[0]
	if identity.SubjectKey() != "spiffe://example.org/ns/default/sa/worker" || identity.Issuer() != "spiffe://example.org" ||
		identity.Assurance() != IdentityAssuranceConfiguredProxy || identity.SourceAddress() != "10.0.0.7:1234" {
		t.Fatalf("identity = subject=%q issuer=%q assurance=%q", identity.SubjectKey(), identity.Issuer(), identity.Assurance())
	}
	missing, _ := provider.Resolve(context.Background(), spiffeTestContext(t, map[string][]string{"X-SSL-Client-Cert": {certificate}}))
	if missing.Status() != PeerIdentityInvalid {
		t.Fatalf("missing verification = %s", missing.Status())
	}
	duplicate, _ := provider.Resolve(context.Background(), spiffeTestContext(t, map[string][]string{
		"X-SSL-Client-Cert": {certificate, certificate}, "X-Client-Cert-Verified": {"true"},
	}))
	if duplicate.Status() != PeerIdentityInvalid {
		t.Fatalf("duplicate certificate = %s", duplicate.Status())
	}
	untrustedContext, _ := NewPeerResolutionContext("http", PeerResolutionOptions{ImmediatePeer: "127.0.0.2"})
	untrusted, _ := provider.Resolve(context.Background(), untrustedContext)
	if untrusted.Status() != PeerIdentityUntrustedProxy {
		t.Fatalf("untrusted = %s", untrusted.Status())
	}
}

func TestSpiffeX509HeaderProviderRejectsInvalidProfiles(t *testing.T) {
	provider, _ := NewSpiffeX509HeaderProvider(SpiffeX509HeaderOptions{SpiffeProxyOptions: spiffeTestBoundary(), VerificationHeader: "X-Verified"})
	certificates := []string{
		spiffeTestCertificate(t, []string{"spiffe://other.org/workload"}, false),
		spiffeTestCertificate(t, []string{"spiffe://example.org/one", "spiffe://example.org/two"}, false),
		spiffeTestCertificate(t, []string{"spiffe://example.org/workload"}, true),
		spiffeTestCertificate(t, []string{"spiffe://example.org/a%2Fb"}, false),
	}
	for index, certificate := range certificates {
		result, err := provider.Resolve(context.Background(), spiffeTestContext(t, map[string][]string{"X-SSL-Client-Cert": {certificate}, "X-Verified": {"true"}}))
		if err != nil || result.Status() != PeerIdentityInvalid {
			t.Errorf("case %d = (%v, %v)", index, result, err)
		}
	}
}

func TestNamedCertificateSpiffeProviders(t *testing.T) {
	certificate := spiffeTestCertificate(t, []string{"spiffe://example.org/workload"}, false)
	tests := []struct {
		name                        string
		factory                     func(SpiffeProxyOptions) (PeerIdentityProvider, error)
		cert, verify, value, source string
	}{
		{"nginx", NewNginxSpiffeProvider, "X-SSL-Client-Cert", "X-SSL-Client-Verify", "SUCCESS", "nginx_mtls"},
		{"azure", NewAzureApplicationGatewaySpiffeProvider, "X-Client-Certificate", "X-Client-Certificate-Verification", "SUCCESS", "azure_application_gateway_mtls_strict"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider, err := test.factory(spiffeTestBoundary())
			if err != nil {
				t.Fatal(err)
			}
			missing, _ := provider.Resolve(context.Background(), spiffeTestContext(t, map[string][]string{test.cert: {certificate}}))
			if missing.Status() != PeerIdentityInvalid {
				t.Fatalf("missing verification = %s", missing.Status())
			}
			available, err := provider.Resolve(context.Background(), spiffeTestContext(t, map[string][]string{test.cert: {certificate}, test.verify: {test.value}}))
			if err != nil || available.Status() != PeerIdentityAvailable || available.Identities()[0].EvidenceSource() != test.source {
				t.Fatalf("Resolve() = (%v, %v)", available, err)
			}
		})
	}
	aws, err := NewAWSALBSpiffeProvider(spiffeTestBoundary())
	if err != nil {
		t.Fatal(err)
	}
	available, err := aws.Resolve(context.Background(), spiffeTestContext(t, map[string][]string{"X-Amzn-Mtls-Clientcert-Leaf": {certificate}}))
	if err != nil || available.Status() != PeerIdentityAvailable || available.Identities()[0].EvidenceSource() != "aws_alb_mtls_verify" {
		t.Fatalf("AWS Resolve() = (%v, %v)", available, err)
	}
}

func TestGCPLoadBalancerSpiffeProviderRequiresAllSignals(t *testing.T) {
	provider, err := NewGCPLoadBalancerSpiffeProvider(GCPSpiffeOptions{SpiffeProxyOptions: spiffeTestBoundary()})
	if err != nil {
		t.Fatal(err)
	}
	headers := map[string][]string{"X-Client-Cert-Present": {"true"}, "X-Client-Cert-Chain-Verified": {"true"}, "X-Client-Cert-Spiffe-Id": {"spiffe://example.org/client"}}
	available, err := provider.Resolve(context.Background(), spiffeTestContext(t, headers))
	if err != nil || available.Status() != PeerIdentityAvailable {
		t.Fatalf("Resolve() = (%v, %v)", available, err)
	}
	headers["X-Client-Cert-Chain-Verified"] = []string{"false"}
	invalid, _ := provider.Resolve(context.Background(), spiffeTestContext(t, headers))
	if invalid.Status() != PeerIdentityInvalid {
		t.Fatalf("unverified = %s", invalid.Status())
	}
	noCertificate, _ := provider.Resolve(context.Background(), spiffeTestContext(t, map[string][]string{"X-Client-Cert-Present": {"false"}}))
	if noCertificate.Status() != PeerIdentityNoMatch {
		t.Fatalf("no certificate = %s", noCertificate.Status())
	}
}

func TestEnvoyXFCCSpiffeProviderRejectsChainsAndMalformedFields(t *testing.T) {
	provider, err := NewEnvoyXFCCSpiffeProvider(EnvoyXFCCSpiffeOptions{SpiffeProxyOptions: spiffeTestBoundary()})
	if err != nil {
		t.Fatal(err)
	}
	valid := "By=spiffe://mesh.example/proxy;Hash=" + string(make([]byte, 0)) + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;URI=\"spiffe://example.org/client\""
	available, err := provider.Resolve(context.Background(), spiffeTestContext(t, map[string][]string{"X-Forwarded-Client-Cert": {valid}}))
	if err != nil || available.Status() != PeerIdentityAvailable {
		t.Fatalf("valid Resolve() = (%v, %v)", available, err)
	}
	invalid := []string{
		valid + ",Hash=" + "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" + ";URI=spiffe://example.org/other",
		"URI=spiffe://example.org/client",
		"Hash=abc;URI=spiffe://example.org/client",
		"Hash=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;URI=spiffe://example.org/one;URI=spiffe://example.org/two",
		"Hash=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;Hash=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb;URI=spiffe://example.org/client",
		"Hash=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;URI=spiffe://other.org/client",
		"Hash=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;URI=spiffe://example.org/client%ZZ",
		"Unknown=value;Hash=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;URI=spiffe://example.org/client",
	}
	for index, header := range invalid {
		result, err := provider.Resolve(context.Background(), spiffeTestContext(t, map[string][]string{"X-Forwarded-Client-Cert": {header}}))
		if err != nil || result.Status() != PeerIdentityInvalid {
			t.Errorf("case %d = (%v, %v)", index, result, err)
		}
	}
}

func TestValidateSpiffeIDCanonicalForm(t *testing.T) {
	if domain, err := ValidateSpiffeID("spiffe://example.org/ns/default/sa/worker", []string{"example.org"}); err != nil || domain != "example.org" {
		t.Fatalf("valid ID = (%q, %v)", domain, err)
	}
	for _, value := range []string{"spiffe://example.org/a%2Fb", "spiffe://example.org/a//b", "spiffe://example.org/a/../b", "spiffe://example.org/a/", "spiffe://example.org/a:b", "spiffe://other.org/a"} {
		if _, err := ValidateSpiffeID(value, []string{"example.org"}); err == nil {
			t.Errorf("invalid ID %q accepted", value)
		}
	}
}

func TestSpiffeCaseVariantHeaderNamesRejectedAtRawBoundary(t *testing.T) {
	_, err := NewPeerResolutionContext("http", PeerResolutionOptions{Headers: map[string][]string{"X-Client-Cert": {"one"}, "x-client-cert": {"two"}}})
	if err == nil {
		t.Fatal("case-varied duplicate headers unexpectedly accepted")
	}
}
