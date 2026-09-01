// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// vgi-rpc-tailnet-go is the live-Tailnet qualification adapter for the Go
// implementation. It is intentionally a conformance tool, not a production
// proxy or a second deployment surface.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"time"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

const providerName = "tailscale"

type echoParams struct {
	Value string `vgirpc:"value"`
}

type expectation struct {
	Issuer           string
	Transport        string
	EvidenceSource   string
	Assurance        vgirpc.IdentityAssurance
	SubjectKind      vgirpc.PeerSubjectKind
	SubjectStability vgirpc.SubjectStability
	SubjectVerified  bool
	Capability       string
	CapabilityTarget string
	TargetValue      string
	Tag              string
	Authenticated    bool
	AuthDomain       string
	PrincipalMatches bool
	BindingPresent   bool
	ProxyPresent     bool
	SpoofLogin       string
}

type snapshot struct {
	ProviderStatus map[string]string `json:"provider_status"`
	Identities     []struct {
		Provider             string   `json:"provider"`
		Issuer               string   `json:"issuer"`
		EvidenceSource       string   `json:"evidence_source"`
		Assurance            string   `json:"assurance"`
		Transport            string   `json:"transport"`
		SubjectKind          string   `json:"subject_kind"`
		SubjectStability     string   `json:"subject_stability"`
		SubjectVerified      bool     `json:"subject_verified"`
		SubjectFingerprint   *string  `json:"subject_fingerprint"`
		Tags                 []string `json:"tags"`
		CapabilityNames      []string `json:"capability_names"`
		CapabilitiesVerified bool     `json:"capabilities_verified"`
		CapabilityTarget     any      `json:"capability_target"`
		ProxyPresent         bool     `json:"proxy_present"`
	} `json:"identities"`
	Auth struct {
		Authenticated              bool    `json:"authenticated"`
		Domain                     *string `json:"domain"`
		PrincipalFingerprint       *string `json:"principal_fingerprint"`
		PrincipalMatchesIdentity   bool    `json:"principal_matches_identity"`
		PeerEvidenceBindingPresent bool    `json:"peer_evidence_binding_present"`
	} `json:"auth"`
}

func main() {
	if len(os.Args) < 2 {
		fatalf("usage: %s client-tcp|client-http|server-tcp|server-http [options]", os.Args[0])
	}
	var err error
	switch os.Args[1] {
	case "client-tcp":
		err = runTCPClient(os.Args[2:])
	case "client-http":
		err = runHTTPClient(os.Args[2:])
	case "server-tcp":
		err = runTCPServer(os.Args[2:])
	case "server-http":
		err = runHTTPServer(os.Args[2:])
	default:
		err = fmt.Errorf("unknown mode %q", os.Args[1])
	}
	if err != nil {
		fatalf("%v", err)
	}
}

func runTCPClient(args []string) error {
	flags := flag.NewFlagSet("client-tcp", flag.ContinueOnError)
	host := flags.String("host", "", "Tailnet TCP worker host")
	port := flags.Int("port", 0, "Tailnet TCP worker port")
	proxy := flags.String("proxy", "", "optional socks5h proxy URL")
	issuer := flags.String("expected-issuer", "", "expected Tailnet issuer namespace")
	evidenceSource := flags.String("expected-evidence-source", "localapi", "expected evidence source")
	assurance := flags.String("expected-assurance", "local_daemon", "expected assurance")
	subjectKind := flags.String("expected-subject-kind", "tagged_node", "expected subject kind")
	subjectStability := flags.String("expected-subject-stability", "stable", "expected subject stability")
	capability := flags.String("expected-capability", "", "expected application capability")
	targetKind := flags.String("expected-target-kind", "", "expected capability target kind")
	targetValue := flags.String("expected-target-value", "", "expected capability target value")
	tag := flags.String("expected-tag", "", "expected Tailnet node tag")
	authenticated := flags.Bool("expect-authenticated", false, "expect primary authentication")
	expectProxy := flags.Bool("expect-proxy", false, "expect a proxy address in peer evidence")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *host == "" || *port == 0 || *issuer == "" || *capability == "" {
		return errors.New("--host, --port, --expected-issuer, and --expected-capability are required")
	}
	options := []vgirpc.TcpClientOption{
		vgirpc.WithTcpClientConnectTimeout(20 * time.Second),
		vgirpc.WithTcpClientProtocolVersion("2.0.0"),
	}
	if *proxy != "" {
		options = append(options, vgirpc.WithTcpClientProxy(*proxy))
	}
	client, err := vgirpc.NewTcpClient(context.Background(), *host, *port, options...)
	if err != nil {
		return err
	}
	defer client.Close()
	params := array.NewRecordBatch(arrow.NewSchema(nil, nil), nil, 0)
	defer params.Release()
	resultSchema := arrow.NewSchema([]arrow.Field{{Name: "result", Type: arrow.BinaryTypes.String}}, nil)
	want := expectation{
		Issuer: *issuer, Transport: "tcp",
		EvidenceSource: *evidenceSource, Assurance: vgirpc.IdentityAssurance(*assurance),
		SubjectKind: vgirpc.PeerSubjectKind(*subjectKind), SubjectStability: vgirpc.SubjectStability(*subjectStability),
		SubjectVerified: true, Capability: *capability, CapabilityTarget: *targetKind, TargetValue: *targetValue, Tag: *tag,
		Authenticated: *authenticated, AuthDomain: providerName,
		PrincipalMatches: *authenticated, BindingPresent: true, ProxyPresent: *expectProxy,
	}
	for range 2 {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		result, callErr := client.CallUnary(ctx, "snapshot", params, resultSchema)
		cancel()
		if callErr != nil {
			return callErr
		}
		raw := result.Batch.Column(0).(*array.String).Value(0)
		result.Release()
		if err := validateSnapshot([]byte(raw), want); err != nil {
			return err
		}
	}
	fmt.Println("Go TCP client Tailnet probe passed")
	return nil
}

func runHTTPClient(args []string) error {
	flags := flag.NewFlagSet("client-http", flag.ContinueOnError)
	url := flags.String("url", "", "Tailnet HTTPS worker URL")
	proxy := flags.String("proxy", "", "optional socks5h proxy URL")
	spoofLogin := flags.String("spoof-login", "", "untrusted login header used by the spoofing assertion")
	issuer := flags.String("expected-issuer", "", "expected Tailnet issuer namespace")
	evidenceSource := flags.String("expected-evidence-source", "serve_proxy", "expected evidence source")
	assurance := flags.String("expected-assurance", "configured_proxy", "expected assurance")
	subjectKind := flags.String("expected-subject-kind", "unknown", "expected subject kind")
	subjectStability := flags.String("expected-subject-stability", "none", "expected subject stability")
	capability := flags.String("expected-capability", "", "expected application capability")
	targetKind := flags.String("expected-target-kind", "", "expected capability target kind")
	targetValue := flags.String("expected-target-value", "", "expected capability target value")
	authenticated := flags.Bool("expect-authenticated", false, "expect primary authentication")
	expectProxy := flags.Bool("expect-proxy", false, "expect a proxy address in peer evidence")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *url == "" || *issuer == "" || *capability == "" || *spoofLogin == "" {
		return errors.New("--url, --expected-issuer, --expected-capability, and --spoof-login are required")
	}
	options := []vgirpc.HttpClientOption{}
	if *proxy != "" {
		options = append(options, vgirpc.WithClientTCPProxy(*proxy))
	}
	if *spoofLogin != "" {
		options = append(options, vgirpc.WithClientHeader("Tailscale-User-Login", *spoofLogin))
	}
	client, err := vgirpc.NewHttpClient(*url, options...)
	if err != nil {
		return err
	}
	defer client.Close()

	params := array.NewRecordBatch(arrow.NewSchema(nil, nil), nil, 0)
	defer params.Release()
	resultSchema := arrow.NewSchema([]arrow.Field{{Name: "result", Type: arrow.BinaryTypes.String}}, nil)
	want := expectation{
		Issuer: *issuer, Transport: "http",
		EvidenceSource: *evidenceSource, Assurance: vgirpc.IdentityAssurance(*assurance),
		SubjectKind: vgirpc.PeerSubjectKind(*subjectKind), SubjectStability: vgirpc.SubjectStability(*subjectStability),
		SubjectVerified: false, Capability: *capability, CapabilityTarget: *targetKind, TargetValue: *targetValue,
		Authenticated: *authenticated, BindingPresent: true, ProxyPresent: *expectProxy, SpoofLogin: *spoofLogin,
	}
	for range 2 {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		result, callErr := client.CallUnary(ctx, "snapshot", params, resultSchema)
		cancel()
		if callErr != nil {
			return callErr
		}
		raw := result.Batch.Column(0).(*array.String).Value(0)
		result.Release()
		if err := validateSnapshot([]byte(raw), want); err != nil {
			return err
		}
	}
	fmt.Println("Go HTTP client Tailnet probe passed")
	return nil
}

func runTCPServer(args []string) error {
	flags := flag.NewFlagSet("server-tcp", flag.ContinueOnError)
	host := flags.String("host", "0.0.0.0", "listen host")
	port := flags.Int("port", 19400, "listen port")
	issuer := flags.String("issuer", "", "Tailnet issuer namespace")
	socket := flags.String("localapi-socket", "/var/run/tailscale/tailscaled.sock", "tailscaled LocalAPI socket")
	capability := flags.String("expected-capability", "", "required application capability")
	tag := flags.String("expected-tag", "", "required client tag")
	proxyRequired := flags.Bool("proxy-protocol-v2", false, "require a trusted PROXY protocol v2 preamble")
	trustedProxy := flags.String("trusted-proxy-address", "", "exact trusted PROXY protocol sender IP")
	serviceName := flags.String("service-name", "", "Tailscale Service capability target")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *issuer == "" || *capability == "" || *tag == "" {
		return errors.New("--issuer, --expected-capability, and --expected-tag are required")
	}
	provider, err := vgirpc.NewTailscaleLocalAPIIdentityProvider(vgirpc.TailscaleLocalAPIOptions{
		Issuer: *issuer, UnixSocket: *socket,
	})
	if err != nil {
		return err
	}
	targetKind := "destination_ip"
	if *serviceName != "" {
		targetKind = "service"
	}
	want := expectation{
		Issuer: *issuer, Transport: "tcp",
		EvidenceSource: "localapi", Assurance: vgirpc.IdentityAssuranceLocalDaemon,
		SubjectKind: vgirpc.PeerSubjectTaggedNode, SubjectStability: vgirpc.SubjectStabilityStable,
		SubjectVerified: true, Capability: *capability, CapabilityTarget: targetKind, TargetValue: *serviceName, Tag: *tag,
		Authenticated: true, AuthDomain: providerName, PrincipalMatches: true, BindingPresent: true,
		ProxyPresent: *proxyRequired,
	}
	server := probeServer(want)
	serverOptions := vgirpc.TcpServerOptions{
		OnBound: func(host string, port int) { fmt.Printf("TCP:%s:%d\n", host, port) },
		ResolveIdentity: func(ctx context.Context, resolution *vgirpc.PeerResolutionContext) (*vgirpc.AuthContext, *vgirpc.PeerEvidenceSet, error) {
			result, err := provider.Resolve(ctx, resolution)
			if err != nil {
				return nil, nil, err
			}
			evidence, err := vgirpc.NewPeerEvidenceSet(result)
			return vgirpc.Anonymous(), evidence, err
		},
		PeerAuthenticationPolicy: vgirpc.PeerIdentityPrimary(providerName),
	}
	if err := applyTCPProxyOptions(&serverOptions, *proxyRequired, *trustedProxy, *serviceName); err != nil {
		return err
	}
	return server.RunTcpWithOptions(*host, *port, serverOptions)
}

func applyTCPProxyOptions(
	options *vgirpc.TcpServerOptions,
	required bool,
	trustedProxy string,
	serviceName string,
) error {
	if required && trustedProxy == "" {
		return errors.New("--trusted-proxy-address is required with --proxy-protocol-v2")
	}
	var trusted []string
	if trustedProxy != "" {
		address, err := netip.ParseAddr(trustedProxy)
		if err != nil || address.Zone() != "" {
			return errors.New("--trusted-proxy-address must be one exact IPv4 or IPv6 address")
		}
		trusted = []string{address.Unmap().String()}
	}
	options.ProxyProtocolV2Required = required
	options.TrustedProxyAddresses = trusted
	options.ServiceName = serviceName
	return nil
}

func runHTTPServer(args []string) error {
	flags := flag.NewFlagSet("server-http", flag.ContinueOnError)
	host := flags.String("host", "127.0.0.1", "listen host")
	port := flags.Int("port", 18080, "listen port")
	issuer := flags.String("issuer", "", "Tailnet issuer namespace")
	capability := flags.String("expected-capability", "", "required application capability")
	trustedIPv4 := flags.String("trusted-proxy-ipv4", "127.0.0.1", "trusted Serve proxy IPv4 address")
	trustedIPv6 := flags.String("trusted-proxy-ipv6", "::1", "trusted Serve proxy IPv6 address")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *issuer == "" || *capability == "" {
		return errors.New("--issuer and --expected-capability are required")
	}
	provider, err := vgirpc.NewTailscaleServeIdentityProvider(vgirpc.TailscaleServeOptions{
		Issuer: *issuer, TrustedProxyAddresses: []string{*trustedIPv4, *trustedIPv6},
	})
	if err != nil {
		return err
	}
	want := expectation{
		Issuer: *issuer, Transport: "http",
		EvidenceSource: "serve_proxy", Assurance: vgirpc.IdentityAssuranceConfiguredProxy,
		SubjectKind: vgirpc.PeerSubjectUnknown, SubjectStability: vgirpc.SubjectStabilityNone,
		SubjectVerified: false, Capability: *capability, BindingPresent: true, ProxyPresent: true,
	}
	handler := vgirpc.NewHttpServer(probeServer(want))
	handler.SetPeerIdentityProviders(provider)
	handler.SetPeerAuthenticationPolicy(vgirpc.RequirePeerIdentity(providerName))
	listener, err := net.Listen("tcp", net.JoinHostPort(*host, fmt.Sprint(*port)))
	if err != nil {
		return err
	}
	fmt.Printf("HTTP:%s:%d\n", *host, *port)
	return http.Serve(listener, handler)
}

func probeServer(want expectation) *vgirpc.Server {
	server := vgirpc.NewServer()
	server.SetServiceName("ConformanceService")
	server.SetProtocolVersion("2.0.0")
	vgirpc.Unary(server, "echo_string", func(_ context.Context, ctx *vgirpc.CallContext, params echoParams) (string, error) {
		if err := validateContext(ctx, want); err != nil {
			return "", &vgirpc.RpcError{Type: "PermissionError", Message: err.Error()}
		}
		return params.Value, nil
	})
	return server
}

func validateContext(ctx *vgirpc.CallContext, want expectation) error {
	if ctx == nil || ctx.PeerEvidence == nil || ctx.Auth == nil {
		return errors.New("missing call identity context")
	}
	if got := ctx.PeerEvidence.Status(providerName); got != vgirpc.PeerIdentityAvailable {
		return fmt.Errorf("tailscale status %q, want available", got)
	}
	identities := ctx.PeerEvidence.ForProvider(providerName)
	if len(identities) != 1 {
		return fmt.Errorf("tailscale identities %d, want 1", len(identities))
	}
	identity := identities[0]
	if identity.Issuer() != want.Issuer || identity.Transport() != want.Transport ||
		identity.EvidenceSource() != want.EvidenceSource || identity.Assurance() != want.Assurance ||
		identity.SubjectKind() != want.SubjectKind || identity.SubjectStability() != want.SubjectStability ||
		identity.SubjectVerified() != want.SubjectVerified {
		return fmt.Errorf("unexpected tailscale identity shape")
	}
	if identity.SubjectVerified() != (identity.SubjectKey() != "") {
		return errors.New("tailscale subject verification and subject presence disagree")
	}
	if !identity.CapabilitiesVerified() {
		return errors.New("tailscale capabilities were not verified")
	}
	if _, ok := identity.Capabilities()[want.Capability]; !ok {
		return fmt.Errorf("missing capability %q", want.Capability)
	}
	if want.Tag != "" && !containsString(identity.Attributes()["tags"], want.Tag) {
		return fmt.Errorf("missing tag %q", want.Tag)
	}
	if !capabilityTargetMatches(identity.Attributes()["capability_target"], want.CapabilityTarget, want.TargetValue) {
		return errors.New("capability target did not match expectation")
	}
	if (identity.ProxyAddress() != "") != want.ProxyPresent {
		return errors.New("unexpected proxy-address presence")
	}
	if ctx.Auth.Authenticated != want.Authenticated {
		return fmt.Errorf("authenticated=%t, want %t", ctx.Auth.Authenticated, want.Authenticated)
	}
	if ctx.Auth.Domain != want.AuthDomain {
		return errors.New("unexpected authentication domain")
	}
	principalMatches := false
	if identity.SubjectKey() != "" {
		principal, err := identity.CanonicalPrincipal()
		if err != nil {
			return errors.New("could not derive canonical peer principal")
		}
		principalMatches = ctx.Auth.Principal == principal
	} else if ctx.Auth.Principal != "" {
		return errors.New("subjectless evidence produced an authentication principal")
	}
	if principalMatches != want.PrincipalMatches {
		return errors.New("authentication principal did not match peer identity")
	}
	wantBinding := ctx.PeerEvidence.BindingDigest([]string{providerName}, nil)
	binding, bindingOK := stringClaim(ctx.Auth.Claims, "peer_evidence_binding")
	if (bindingOK && binding == wantBinding) != want.BindingPresent {
		return errors.New("unexpected peer-evidence binding claim")
	}
	if want.Authenticated {
		for name, expected := range map[string]string{
			"issuer": want.Issuer, "subject_kind": string(want.SubjectKind),
			"assurance": string(want.Assurance), "evidence_source": want.EvidenceSource,
			"subject": identity.SubjectKey(),
		} {
			if actual, ok := stringClaim(ctx.Auth.Claims, name); !ok || actual != expected {
				return errors.New("peer-primary authentication claims did not match identity evidence")
			}
		}
	}
	return nil
}

func validateSnapshot(raw []byte, want expectation) error {
	var got snapshot
	if err := json.Unmarshal(raw, &got); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}
	if got.ProviderStatus[providerName] != "available" || len(got.Identities) != 1 {
		return errors.New("snapshot did not contain one available Tailscale identity")
	}
	i := got.Identities[0]
	if i.Provider != providerName || i.Issuer != want.Issuer || i.Transport != want.Transport ||
		i.EvidenceSource != want.EvidenceSource || i.Assurance != string(want.Assurance) ||
		i.SubjectKind != string(want.SubjectKind) || i.SubjectStability != string(want.SubjectStability) ||
		i.SubjectVerified != want.SubjectVerified || !i.CapabilitiesVerified ||
		!contains(i.CapabilityNames, want.Capability) || i.ProxyPresent != want.ProxyPresent ||
		!capabilityTargetMatches(i.CapabilityTarget, want.CapabilityTarget, want.TargetValue) ||
		got.Auth.Authenticated != want.Authenticated || stringValue(got.Auth.Domain) != want.AuthDomain ||
		got.Auth.PrincipalMatchesIdentity != want.PrincipalMatches ||
		got.Auth.PeerEvidenceBindingPresent != want.BindingPresent {
		return fmt.Errorf("unexpected Tailnet snapshot: %s", raw)
	}
	if (i.SubjectFingerprint != nil) != want.SubjectVerified ||
		(got.Auth.PrincipalFingerprint != nil) != want.Authenticated {
		return errors.New("snapshot subject or principal presence did not match authentication shape")
	}
	if want.SpoofLogin != "" && i.SubjectFingerprint != nil {
		spoofed := fmt.Sprintf("%x", sha256.Sum256([]byte("login:"+want.SpoofLogin)))
		if *i.SubjectFingerprint == spoofed {
			return errors.New("serve trusted a client-supplied identity header")
		}
	}
	return nil
}

func capabilityTargetKind(value any) string {
	target, ok := value.(map[string]any)
	if !ok {
		return ""
	}
	kind, _ := target["kind"].(string)
	return kind
}

func capabilityTargetMatches(value any, expectedKind, expectedValue string) bool {
	if capabilityTargetKind(value) != expectedKind {
		return false
	}
	if expectedValue == "" {
		return true
	}
	target, ok := value.(map[string]any)
	if !ok {
		return false
	}
	actual, _ := target["value"].(string)
	return actual == expectedValue
}

func stringClaim(claims map[string]any, name string) (string, bool) {
	if claims == nil {
		return "", false
	}
	value, ok := claims[name].(string)
	return value, ok
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func containsString(value any, wanted string) bool {
	switch values := value.(type) {
	case []any:
		for _, value := range values {
			if text, ok := value.(string); ok && text == wanted {
				return true
			}
		}
	case []string:
		return contains(values, wanted)
	}
	return false
}

func fatalf(format string, values ...any) {
	fmt.Fprintf(os.Stderr, "vgi-rpc-tailnet-go: "+format+"\n", values...)
	os.Exit(1)
}
