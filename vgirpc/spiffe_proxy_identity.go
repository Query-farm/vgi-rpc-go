// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/netip"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

const spiffeProvider = "spiffe"

var (
	spiffeTrustDomainPattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9._-]{0,253}[a-z0-9])?$`)
	spiffePathPattern        = regexp.MustCompile(`^/(?:[A-Za-z0-9._-]+)(?:/[A-Za-z0-9._-]+)*$`)
	spiffeXFCCKeyPattern     = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]*$`)
	spiffeSHA256Pattern      = regexp.MustCompile(`^[0-9A-Fa-f]{64}$`)
)

// SpiffeProxyOptions define the trust boundary shared by HTTP proxy adapters.
type SpiffeProxyOptions struct {
	TrustDomains          []string
	TrustedProxyAddresses []string
	MaxHeaderBytes        int
}

type spiffeProxyBoundary struct {
	trustDomains map[string]struct{}
	trustedPeers map[netip.Addr]struct{}
	maxBytes     int
}

func newSpiffeProxyBoundary(options SpiffeProxyOptions) (spiffeProxyBoundary, error) {
	if len(options.TrustDomains) == 0 || len(options.TrustedProxyAddresses) == 0 {
		return spiffeProxyBoundary{}, fmt.Errorf("vgirpc: SPIFFE trust domains and trusted proxy addresses are required")
	}
	maxBytes := options.MaxHeaderBytes
	if maxBytes == 0 {
		maxBytes = 16_384
	}
	if maxBytes < 0 {
		return spiffeProxyBoundary{}, fmt.Errorf("vgirpc: SPIFFE maximum header size must be positive")
	}
	domains := make(map[string]struct{}, len(options.TrustDomains))
	for _, domain := range options.TrustDomains {
		if !spiffeTrustDomainPattern.MatchString(domain) {
			return spiffeProxyBoundary{}, fmt.Errorf("vgirpc: invalid SPIFFE trust domain %q", domain)
		}
		domains[domain] = struct{}{}
	}
	peers := make(map[netip.Addr]struct{}, len(options.TrustedProxyAddresses))
	for _, value := range options.TrustedProxyAddresses {
		peer, err := netip.ParseAddr(value)
		if err != nil {
			return spiffeProxyBoundary{}, fmt.Errorf("vgirpc: SPIFFE trusted proxy %q is not an exact IP address", value)
		}
		peers[peer.Unmap()] = struct{}{}
	}
	return spiffeProxyBoundary{trustDomains: domains, trustedPeers: peers, maxBytes: maxBytes}, nil
}

func (b spiffeProxyBoundary) trusts(value string) bool {
	address, err := netip.ParseAddr(value)
	if err != nil {
		return false
	}
	_, ok := b.trustedPeers[address.Unmap()]
	return ok
}

// ValidateSpiffeID validates a canonical workload SPIFFE ID against an allowed
// trust-domain set and returns its trust domain.
func ValidateSpiffeID(value string, trustDomains []string) (string, error) {
	domains := make(map[string]struct{}, len(trustDomains))
	for _, domain := range trustDomains {
		if !spiffeTrustDomainPattern.MatchString(domain) {
			return "", fmt.Errorf("invalid SPIFFE trust domain")
		}
		domains[domain] = struct{}{}
	}
	return validateSpiffeID(value, domains)
}

func validateSpiffeID(value string, trustDomains map[string]struct{}) (string, error) {
	if value == "" || len(value) > 2048 || !utf8.ValidString(value) || strings.Contains(value, "%") {
		return "", fmt.Errorf("invalid SPIFFE ID size or encoding")
	}
	for _, character := range value {
		if character > 0x7f || character < 0x20 || character == 0x7f {
			return "", fmt.Errorf("SPIFFE ID must contain canonical ASCII")
		}
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "spiffe" || parsed.Opaque != "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Port() != "" {
		return "", fmt.Errorf("invalid SPIFFE ID URL components")
	}
	trustDomain := parsed.Hostname()
	if parsed.Host != trustDomain || !spiffeTrustDomainPattern.MatchString(trustDomain) {
		return "", fmt.Errorf("invalid SPIFFE trust domain")
	}
	if _, ok := trustDomains[trustDomain]; !ok {
		return "", fmt.Errorf("SPIFFE trust domain is not allowed")
	}
	if !spiffePathPattern.MatchString(parsed.Path) {
		return "", fmt.Errorf("SPIFFE path is not canonical")
	}
	for _, segment := range strings.Split(parsed.Path, "/") {
		if segment == "." || segment == ".." {
			return "", fmt.Errorf("SPIFFE path contains a dot segment")
		}
	}
	return trustDomain, nil
}

// SpiffeX509HeaderOptions configure a proxy-provided verified X.509-SVID.
type SpiffeX509HeaderOptions struct {
	SpiffeProxyOptions
	CertificateHeader  string
	VerificationHeader string
	VerificationValue  string
	EvidenceSource     string
}

type spiffeX509HeaderProvider struct {
	boundary            spiffeProxyBoundary
	certificateHeader   string
	verificationHeader  string
	verificationValue   string
	evidenceSource      string
	requireVerification bool
}

// NewSpiffeX509HeaderProvider creates a strict generic certificate-header
// provider. A positive per-request verification header is mandatory.
func NewSpiffeX509HeaderProvider(options SpiffeX509HeaderOptions) (PeerIdentityProvider, error) {
	if options.CertificateHeader == "" {
		options.CertificateHeader = "X-SSL-Client-Cert"
	}
	if options.VerificationValue == "" {
		options.VerificationValue = "true"
	}
	if options.EvidenceSource == "" {
		options.EvidenceSource = "verified_certificate_header"
	}
	if options.VerificationHeader == "" {
		return nil, fmt.Errorf("vgirpc: SPIFFE verification header is required")
	}
	return newSpiffeCertificateProvider(options, true)
}

func newSpiffeCertificateProvider(options SpiffeX509HeaderOptions, requireVerification bool) (PeerIdentityProvider, error) {
	boundary, err := newSpiffeProxyBoundary(options.SpiffeProxyOptions)
	if err != nil {
		return nil, err
	}
	if !validSpiffeHeaderName(options.CertificateHeader) || (requireVerification && !validSpiffeHeaderName(options.VerificationHeader)) ||
		strings.EqualFold(options.CertificateHeader, options.VerificationHeader) || tailscaleHasControl(options.VerificationValue) {
		return nil, fmt.Errorf("vgirpc: invalid or ambiguous SPIFFE proxy headers")
	}
	return &spiffeX509HeaderProvider{boundary, options.CertificateHeader, options.VerificationHeader,
		options.VerificationValue, options.EvidenceSource, requireVerification}, nil
}

func (p *spiffeX509HeaderProvider) Provider() string { return spiffeProvider }

func (p *spiffeX509HeaderProvider) Resolve(_ context.Context, resolution *PeerResolutionContext) (*PeerIdentityResult, error) {
	if resolution == nil || !p.boundary.trusts(resolution.ImmediatePeer()) {
		return NewPeerIdentityResult(spiffeProvider, PeerIdentityUntrustedProxy)
	}
	raw, present, err := resolution.Header(p.certificateHeader)
	if err != nil {
		return NewPeerIdentityResult(spiffeProvider, PeerIdentityInvalid)
	}
	if !present || raw == "" {
		return NewPeerIdentityResult(spiffeProvider, PeerIdentityNoMatch)
	}
	if p.requireVerification {
		verified, found, headerErr := resolution.Header(p.verificationHeader)
		if headerErr != nil || !found || len(verified) > 64 || verified != p.verificationValue {
			return NewPeerIdentityResult(spiffeProvider, PeerIdentityInvalid)
		}
	}
	certificate, err := decodeSpiffeCertificateHeader(raw, p.boundary.maxBytes)
	if err != nil {
		return NewPeerIdentityResult(spiffeProvider, PeerIdentityInvalid)
	}
	identity, err := spiffeIdentityFromCertificate(certificate, p.boundary.trustDomains, p.evidenceSource, resolution)
	if err != nil {
		return NewPeerIdentityResult(spiffeProvider, PeerIdentityInvalid)
	}
	return NewAvailablePeerIdentityResult(spiffeProvider, identity)
}

func decodeSpiffeCertificateHeader(raw string, maxBytes int) (*x509.Certificate, error) {
	if len(raw) > maxBytes || !utf8.ValidString(raw) || tailscaleHasControl(raw) {
		return nil, fmt.Errorf("invalid SPIFFE certificate header")
	}
	decoded, err := strictSpiffeCertificatePercentDecode(raw)
	if err != nil || len(decoded) > maxBytes || strings.Count(decoded, "-----BEGIN CERTIFICATE-----") != 1 || strings.Count(decoded, "-----END CERTIFICATE-----") != 1 {
		return nil, fmt.Errorf("invalid SPIFFE PEM certificate header")
	}
	block, rest := pem.Decode([]byte(decoded))
	if block == nil || block.Type != "CERTIFICATE" || len(strings.TrimSpace(string(rest))) != 0 {
		return nil, fmt.Errorf("SPIFFE header must contain one certificate")
	}
	return x509.ParseCertificate(block.Bytes)
}

func spiffeIdentityFromCertificate(cert *x509.Certificate, domains map[string]struct{}, evidenceSource string, resolution *PeerResolutionContext) (*PeerIdentity, error) {
	return spiffeIdentityFromCertificateEvidence(cert, domains, evidenceSource,
		IdentityAssuranceConfiguredProxy, "http", resolution)
}

func spiffeIdentityFromCertificateEvidence(
	cert *x509.Certificate,
	domains map[string]struct{},
	evidenceSource string,
	assurance IdentityAssurance,
	transport string,
	resolution *PeerResolutionContext,
) (*PeerIdentity, error) {
	now := time.Now()
	if now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
		return nil, fmt.Errorf("X.509-SVID outside validity period")
	}
	if len(cert.URIs) != 1 {
		return nil, fmt.Errorf("X.509-SVID requires exactly one URI SAN")
	}
	trustDomain, err := validateSpiffeID(cert.URIs[0].String(), domains)
	if err != nil {
		return nil, err
	}
	sanCritical := false
	keyUsageCritical := false
	extendedUsagePresent := false
	for _, extension := range cert.Extensions {
		switch extension.Id.String() {
		case "2.5.29.17":
			sanCritical = extension.Critical
		case "2.5.29.15":
			keyUsageCritical = extension.Critical
		case "2.5.29.37":
			extendedUsagePresent = true
		}
	}
	if len(cert.Subject.Names) == 0 && !sanCritical {
		return nil, fmt.Errorf("subjectless X.509-SVID requires critical SAN")
	}
	if !cert.BasicConstraintsValid || cert.IsCA {
		return nil, fmt.Errorf("X.509-SVID leaf cannot be CA")
	}
	if !keyUsageCritical || cert.KeyUsage&x509.KeyUsageDigitalSignature == 0 || cert.KeyUsage&(x509.KeyUsageCertSign|x509.KeyUsageCRLSign) != 0 {
		return nil, fmt.Errorf("invalid X.509-SVID key usage")
	}
	if extendedUsagePresent {
		client, server := false, false
		for _, usage := range cert.ExtKeyUsage {
			client = client || usage == x509.ExtKeyUsageClientAuth
			server = server || usage == x509.ExtKeyUsageServerAuth
		}
		if !client || !server {
			return nil, fmt.Errorf("invalid X.509-SVID extended key usage")
		}
	}
	id := cert.URIs[0].String()
	sourceAddress := resolution.AssertedPeer()
	proxyAddress := resolution.ImmediatePeer()
	if assurance == IdentityAssuranceCryptographicPeer {
		if sourceAddress == "" {
			sourceAddress = resolution.ImmediatePeer()
			proxyAddress = ""
		}
	}
	return NewPeerIdentity(PeerIdentityOptions{
		Provider: spiffeProvider, EvidenceSource: evidenceSource, Assurance: assurance,
		Issuer: "spiffe://" + trustDomain, Transport: transport, SubjectKind: PeerSubjectWorkload,
		SubjectKey: id, SubjectStability: SubjectStabilityStable, SubjectVerified: true,
		SourceAddress: sourceAddress, ProxyAddress: proxyAddress,
	})
}

// NewNginxSpiffeProvider consumes nginx's verified client certificate headers.
func NewNginxSpiffeProvider(options SpiffeProxyOptions) (PeerIdentityProvider, error) {
	return newSpiffeCertificateProvider(SpiffeX509HeaderOptions{SpiffeProxyOptions: options,
		CertificateHeader: "X-SSL-Client-Cert", VerificationHeader: "X-SSL-Client-Verify",
		VerificationValue: "SUCCESS", EvidenceSource: "nginx_mtls"}, true)
}

// NewAzureApplicationGatewaySpiffeProvider consumes strict-mode rewrite headers.
func NewAzureApplicationGatewaySpiffeProvider(options SpiffeProxyOptions) (PeerIdentityProvider, error) {
	return newSpiffeCertificateProvider(SpiffeX509HeaderOptions{SpiffeProxyOptions: options,
		CertificateHeader: "X-Client-Certificate", VerificationHeader: "X-Client-Certificate-Verification",
		VerificationValue: "SUCCESS", EvidenceSource: "azure_application_gateway_mtls_strict"}, true)
}

// NewAWSALBSpiffeProvider consumes the leaf header from an ALB listener that the
// operator guarantees is configured in mTLS verify mode.
func NewAWSALBSpiffeProvider(options SpiffeProxyOptions) (PeerIdentityProvider, error) {
	return newSpiffeCertificateProvider(SpiffeX509HeaderOptions{SpiffeProxyOptions: options,
		CertificateHeader: "X-Amzn-Mtls-Clientcert-Leaf", EvidenceSource: "aws_alb_mtls_verify"}, false)
}

// GCPSpiffeOptions configure frontend-mTLS custom request headers.
type GCPSpiffeOptions struct {
	SpiffeProxyOptions
	SpiffeIDHeader      string
	PresentHeader       string
	ChainVerifiedHeader string
	ErrorHeader         string
}

type gcpSpiffeProvider struct {
	boundary                       spiffeProxyBoundary
	id, present, verified, failure string
}

// NewGCPLoadBalancerSpiffeProvider consumes all GCP frontend validation signals.
func NewGCPLoadBalancerSpiffeProvider(options GCPSpiffeOptions) (PeerIdentityProvider, error) {
	boundary, err := newSpiffeProxyBoundary(options.SpiffeProxyOptions)
	if err != nil {
		return nil, err
	}
	if options.SpiffeIDHeader == "" {
		options.SpiffeIDHeader = "X-Client-Cert-Spiffe-Id"
	}
	if options.PresentHeader == "" {
		options.PresentHeader = "X-Client-Cert-Present"
	}
	if options.ChainVerifiedHeader == "" {
		options.ChainVerifiedHeader = "X-Client-Cert-Chain-Verified"
	}
	if options.ErrorHeader == "" {
		options.ErrorHeader = "X-Client-Cert-Error"
	}
	headers := []string{options.SpiffeIDHeader, options.PresentHeader, options.ChainVerifiedHeader, options.ErrorHeader}
	seen := map[string]struct{}{}
	for _, header := range headers {
		key := strings.ToLower(header)
		if !validSpiffeHeaderName(header) {
			return nil, fmt.Errorf("vgirpc: invalid GCP mTLS header")
		}
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("vgirpc: duplicate GCP mTLS header")
		}
		seen[key] = struct{}{}
	}
	return &gcpSpiffeProvider{boundary, options.SpiffeIDHeader, options.PresentHeader, options.ChainVerifiedHeader, options.ErrorHeader}, nil
}

func (p *gcpSpiffeProvider) Provider() string { return spiffeProvider }
func (p *gcpSpiffeProvider) Resolve(_ context.Context, resolution *PeerResolutionContext) (*PeerIdentityResult, error) {
	if resolution == nil || !p.boundary.trusts(resolution.ImmediatePeer()) {
		return NewPeerIdentityResult(spiffeProvider, PeerIdentityUntrustedProxy)
	}
	id, idOK, e1 := resolution.Header(p.id)
	present, _, e2 := resolution.Header(p.present)
	verified, verifiedOK, e3 := resolution.Header(p.verified)
	failure, failureOK, e4 := resolution.Header(p.failure)
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil {
		return NewPeerIdentityResult(spiffeProvider, PeerIdentityInvalid)
	}
	if present == "false" && (!verifiedOK || verified == "false") && !idOK {
		return NewPeerIdentityResult(spiffeProvider, PeerIdentityNoMatch)
	}
	if present != "true" || !verifiedOK || verified != "true" || (failureOK && failure != "") || !idOK || id == "" {
		return NewPeerIdentityResult(spiffeProvider, PeerIdentityInvalid)
	}
	trustDomain, err := validateSpiffeID(id, p.boundary.trustDomains)
	if err != nil {
		return NewPeerIdentityResult(spiffeProvider, PeerIdentityInvalid)
	}
	identity, err := NewPeerIdentity(PeerIdentityOptions{Provider: spiffeProvider, EvidenceSource: "gcp_load_balancer_mtls",
		Assurance: IdentityAssuranceConfiguredProxy, Issuer: "spiffe://" + trustDomain, Transport: "http",
		SubjectKind: PeerSubjectWorkload, SubjectKey: id, SubjectStability: SubjectStabilityStable, SubjectVerified: true,
		Attributes:    map[string]any{"client_certificate_present": true, "client_certificate_chain_verified": true},
		SourceAddress: resolution.AssertedPeer(), ProxyAddress: resolution.ImmediatePeer()})
	if err != nil {
		return NewPeerIdentityResult(spiffeProvider, PeerIdentityInvalid)
	}
	return NewAvailablePeerIdentityResult(spiffeProvider, identity)
}

// EnvoyXFCCSpiffeOptions configure strict SANITIZE_SET XFCC evidence.
type EnvoyXFCCSpiffeOptions struct {
	SpiffeProxyOptions
	Header string
}
type envoyXFCCSpiffeProvider struct {
	boundary spiffeProxyBoundary
	header   string
}

// NewEnvoyXFCCSpiffeProvider consumes exactly one text-format SANITIZE_SET element.
func NewEnvoyXFCCSpiffeProvider(options EnvoyXFCCSpiffeOptions) (PeerIdentityProvider, error) {
	boundary, err := newSpiffeProxyBoundary(options.SpiffeProxyOptions)
	if err != nil {
		return nil, err
	}
	if options.Header == "" {
		options.Header = "X-Forwarded-Client-Cert"
	}
	if !validSpiffeHeaderName(options.Header) {
		return nil, fmt.Errorf("vgirpc: invalid Envoy XFCC header")
	}
	return &envoyXFCCSpiffeProvider{boundary, options.Header}, nil
}
func (p *envoyXFCCSpiffeProvider) Provider() string { return spiffeProvider }
func (p *envoyXFCCSpiffeProvider) Resolve(_ context.Context, resolution *PeerResolutionContext) (*PeerIdentityResult, error) {
	if resolution == nil || !p.boundary.trusts(resolution.ImmediatePeer()) {
		return NewPeerIdentityResult(spiffeProvider, PeerIdentityUntrustedProxy)
	}
	raw, present, err := resolution.Header(p.header)
	if err != nil {
		return NewPeerIdentityResult(spiffeProvider, PeerIdentityInvalid)
	}
	if !present {
		return NewPeerIdentityResult(spiffeProvider, PeerIdentityNoMatch)
	}
	fields, err := parseEnvoyXFCC(raw, p.boundary.maxBytes)
	if err != nil {
		return NewPeerIdentityResult(spiffeProvider, PeerIdentityInvalid)
	}
	uris, hashes := fields["uri"], fields["hash"]
	if len(uris) != 1 || len(hashes) != 1 || !spiffeSHA256Pattern.MatchString(hashes[0]) {
		return NewPeerIdentityResult(spiffeProvider, PeerIdentityInvalid)
	}
	trustDomain, err := validateSpiffeID(uris[0], p.boundary.trustDomains)
	if err != nil {
		return NewPeerIdentityResult(spiffeProvider, PeerIdentityInvalid)
	}
	attributes := map[string]any{"certificate_sha256": strings.ToLower(hashes[0])}
	if by := fields["by"]; len(by) > 0 {
		attributes["proxy_identities"] = by
	}
	identity, err := NewPeerIdentity(PeerIdentityOptions{Provider: spiffeProvider, EvidenceSource: "envoy_xfcc_sanitize_set",
		Assurance: IdentityAssuranceConfiguredProxy, Issuer: "spiffe://" + trustDomain, Transport: "http",
		SubjectKind: PeerSubjectWorkload, SubjectKey: uris[0], SubjectStability: SubjectStabilityStable, SubjectVerified: true,
		Attributes: attributes, SourceAddress: resolution.AssertedPeer(), ProxyAddress: resolution.ImmediatePeer()})
	if err != nil {
		return NewPeerIdentityResult(spiffeProvider, PeerIdentityInvalid)
	}
	return NewAvailablePeerIdentityResult(spiffeProvider, identity)
}

func parseEnvoyXFCC(raw string, maxBytes int) (map[string][]string, error) {
	if len(raw) > maxBytes || !utf8.ValidString(raw) || tailscaleHasControl(raw) {
		return nil, fmt.Errorf("invalid Envoy XFCC")
	}
	for _, character := range raw {
		if character > 0x7f {
			return nil, fmt.Errorf("envoy XFCC must be ASCII")
		}
	}
	elements, err := splitEnvoyXFCC(raw, ',')
	if err != nil || len(elements) != 1 || strings.TrimSpace(elements[0]) == "" {
		return nil, fmt.Errorf("envoy XFCC must contain one element")
	}
	fields := map[string][]string{}
	pairs, err := splitEnvoyXFCC(elements[0], ';')
	if err != nil {
		return nil, err
	}
	allowed := map[string]bool{"by": true, "hash": true, "cert": true, "chain": true, "subject": true, "uri": true, "dns": true, "issuer": true}
	for _, rawPair := range pairs {
		pair := strings.TrimSpace(rawPair)
		keyRaw, valueRaw, found := strings.Cut(pair, "=")
		keyRaw = strings.TrimSpace(keyRaw)
		key := strings.ToLower(keyRaw)
		if !found || !spiffeXFCCKeyPattern.MatchString(keyRaw) || !allowed[key] {
			return nil, fmt.Errorf("malformed Envoy XFCC field")
		}
		value, err := envoyXFCCValue(strings.TrimSpace(valueRaw))
		if err != nil {
			return nil, err
		}
		if key == "by" || key == "uri" || key == "cert" || key == "chain" {
			value, err = strictSpiffePercentDecode(value)
			if err != nil {
				return nil, err
			}
		}
		if key != "by" && key != "uri" && key != "dns" && len(fields[key]) > 0 {
			return nil, fmt.Errorf("duplicate Envoy XFCC singleton")
		}
		fields[key] = append(fields[key], value)
	}
	return fields, nil
}

func splitEnvoyXFCC(value string, delimiter byte) ([]string, error) {
	parts := []string{}
	var current strings.Builder
	quoted, escaped := false, false
	for index := 0; index < len(value); index++ {
		character := value[index]
		if escaped {
			if character != '"' && character != '\\' {
				return nil, fmt.Errorf("unsupported XFCC escape")
			}
			current.WriteByte(character)
			escaped = false
		} else if quoted && character == '\\' {
			escaped = true
		} else if character == '"' {
			quoted = !quoted
			current.WriteByte(character)
		} else if character == delimiter && !quoted {
			parts = append(parts, current.String())
			current.Reset()
		} else {
			current.WriteByte(character)
		}
	}
	if quoted || escaped {
		return nil, fmt.Errorf("unterminated XFCC quoted value")
	}
	return append(parts, current.String()), nil
}

func envoyXFCCValue(value string) (string, error) {
	if strings.HasPrefix(value, `"`) || strings.HasSuffix(value, `"`) {
		if len(value) < 2 || value[0] != '"' || value[len(value)-1] != '"' {
			return "", fmt.Errorf("malformed quoted XFCC value")
		}
		return value[1 : len(value)-1], nil
	}
	if value == "" || strings.ContainsAny(value, ",;=") {
		return "", fmt.Errorf("invalid unquoted XFCC value")
	}
	return value, nil
}

func strictSpiffePercentDecode(value string) (string, error) {
	decoded, err := strictSpiffePercentDecodeRaw(value)
	if err != nil || tailscaleHasControl(decoded) {
		return "", fmt.Errorf("invalid percent-decoded value")
	}
	return decoded, nil
}

func strictSpiffeCertificatePercentDecode(value string) (string, error) {
	decoded, err := strictSpiffePercentDecodeRaw(value)
	if err != nil {
		return "", err
	}
	for _, character := range decoded {
		if character > 0x7f || (character < 0x20 && character != '\r' && character != '\n') || character == 0x7f {
			return "", fmt.Errorf("invalid decoded certificate header")
		}
	}
	return decoded, nil
}

func strictSpiffePercentDecodeRaw(value string) (string, error) {
	for index := 0; index < len(value); index++ {
		if value[index] == '%' {
			if index+2 >= len(value) || !isHexByte(value[index+1]) || !isHexByte(value[index+2]) {
				return "", fmt.Errorf("invalid percent escape")
			}
			index += 2
		}
	}
	decoded, err := url.PathUnescape(value)
	if err != nil || !utf8.ValidString(decoded) {
		return "", fmt.Errorf("invalid percent-decoded value")
	}
	return decoded, nil
}
func isHexByte(value byte) bool {
	return value >= '0' && value <= '9' || value >= 'a' && value <= 'f' || value >= 'A' && value <= 'F'
}
func validSpiffeHeaderName(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", character)) {
			return false
		}
	}
	return true
}
