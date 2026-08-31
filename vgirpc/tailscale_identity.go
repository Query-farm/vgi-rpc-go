// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	tailscaleProvider             = "tailscale"
	tailscaleLocalAPIHost         = "local-tailscaled.sock"
	tailscaleDefaultSocket        = "/var/run/tailscale/tailscaled.sock"
	tailscaleDefaultTimeout       = 5 * time.Second
	tailscaleDefaultHeaderBytes   = 16_384
	tailscaleDefaultResponseBytes = 65_536
	tailscaleDefaultResponseHeads = 32_768
)

var (
	tailscaleQWords      = regexp.MustCompile(`(?i)^=\?utf-8\?q\?[^?]*\?=(?: +=\?utf-8\?q\?[^?]*\?=)*$`)
	tailscaleServiceName = regexp.MustCompile(`^svc:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$`)
)

// TailscaleServeOptions configure trusted Tailscale Serve header evidence.
type TailscaleServeOptions struct {
	Issuer                string
	TrustedProxyAddresses []string
	MaxHeaderBytes        int
}

// TailscaleServeIdentityProvider resolves identity headers emitted by a
// specifically trusted Tailscale Serve proxy.
type TailscaleServeIdentityProvider struct {
	issuer         string
	trustedProxies map[netip.Addr]struct{}
	maxHeaderBytes int
}

// NewTailscaleServeIdentityProvider creates an opt-in Serve identity adapter.
// TrustedProxyAddresses must contain exact IP addresses, not CIDRs or hostnames.
func NewTailscaleServeIdentityProvider(options TailscaleServeOptions) (*TailscaleServeIdentityProvider, error) {
	if options.Issuer == "" || !utf8.ValidString(options.Issuer) || tailscaleHasControl(options.Issuer) {
		return nil, fmt.Errorf("vgirpc: Tailscale issuer must be a non-empty Unicode string without controls")
	}
	if len(options.TrustedProxyAddresses) == 0 {
		return nil, fmt.Errorf("vgirpc: at least one exact Tailscale Serve proxy address is required")
	}
	maxHeaderBytes := options.MaxHeaderBytes
	if maxHeaderBytes == 0 {
		maxHeaderBytes = tailscaleDefaultHeaderBytes
	}
	if maxHeaderBytes < 0 {
		return nil, fmt.Errorf("vgirpc: Tailscale Serve maximum header size must be positive")
	}
	trusted := make(map[netip.Addr]struct{}, len(options.TrustedProxyAddresses))
	for _, value := range options.TrustedProxyAddresses {
		address, err := netip.ParseAddr(value)
		if err != nil {
			return nil, fmt.Errorf("vgirpc: Tailscale Serve trusted proxy %q is not an exact IP address", value)
		}
		trusted[address.Unmap()] = struct{}{}
	}
	return &TailscaleServeIdentityProvider{
		issuer: options.Issuer, trustedProxies: trusted, maxHeaderBytes: maxHeaderBytes,
	}, nil
}

func (p *TailscaleServeIdentityProvider) Provider() string { return tailscaleProvider }

// Resolve validates the immediate proxy before consuming any Tailscale header.
func (p *TailscaleServeIdentityProvider) Resolve(_ context.Context, resolution *PeerResolutionContext) (*PeerIdentityResult, error) {
	if resolution == nil || !p.trusts(resolution.ImmediatePeer()) {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityUntrustedProxy)
	}
	read := func(name string) (string, bool, bool) {
		value, present, err := resolution.Header(name)
		return value, present, err == nil
	}
	funnel, funnelPresent, ok := read("Tailscale-Funnel-Request")
	if !ok {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityInvalid)
	}
	loginRaw, loginPresent, ok := read("Tailscale-User-Login")
	if !ok {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityInvalid)
	}
	nameRaw, namePresent, ok := read("Tailscale-User-Name")
	if !ok {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityInvalid)
	}
	profileRaw, profilePresent, ok := read("Tailscale-User-Profile-Pic")
	if !ok {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityInvalid)
	}
	capabilitiesRaw, capabilitiesPresent, ok := read("Tailscale-App-Capabilities")
	if !ok {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityInvalid)
	}
	if funnelPresent {
		if funnel == "?1" {
			return NewPeerIdentityResult(tailscaleProvider, PeerIdentityNotApplicable)
		}
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityInvalid)
	}

	decode := func(raw string, present bool) (string, error) {
		if !present {
			return "", nil
		}
		return decodeTailscaleServeHeader(raw, p.maxHeaderBytes)
	}
	login, err := decode(loginRaw, loginPresent)
	if err != nil {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityInvalid)
	}
	displayName, err := decode(nameRaw, namePresent)
	if err != nil {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityInvalid)
	}
	if _, err = decode(profileRaw, profilePresent); err != nil {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityInvalid)
	}
	capabilities := map[string]any{}
	if capabilitiesPresent {
		capabilities, err = decodeTailscaleCapabilities(capabilitiesRaw, p.maxHeaderBytes)
		if err != nil {
			return NewPeerIdentityResult(tailscaleProvider, PeerIdentityInvalid)
		}
	}
	if loginPresent && login == "" {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityInvalid)
	}
	if (namePresent || profilePresent) && login == "" {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityInvalid)
	}
	if login == "" && len(capabilities) == 0 {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityNoMatch)
	}

	attributes := map[string]any{}
	subjectKind := PeerSubjectUnknown
	subjectStability := SubjectStabilityNone
	subjectKey := ""
	subjectVerified := false
	if login != "" {
		attributes["user_login"] = login
		subjectKind = PeerSubjectUser
		subjectStability = SubjectStabilityLogin
		subjectKey = "login:" + login
		subjectVerified = true
	}
	if displayName != "" {
		attributes["user_display_name"] = displayName
	}
	identity, err := NewPeerIdentity(PeerIdentityOptions{
		Provider: tailscaleProvider, EvidenceSource: "serve_proxy",
		Assurance: IdentityAssuranceConfiguredProxy, Issuer: p.issuer, Transport: "http",
		SubjectKind: subjectKind, SubjectKey: subjectKey, SubjectStability: subjectStability,
		SubjectVerified: subjectVerified, Attributes: attributes, Capabilities: capabilities,
		CapabilitiesVerified: capabilitiesPresent, SourceAddress: resolution.AssertedPeer(),
		ProxyAddress: resolution.ImmediatePeer(),
	})
	if err != nil {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityInvalid)
	}
	return NewAvailablePeerIdentityResult(tailscaleProvider, identity)
}

func (p *TailscaleServeIdentityProvider) trusts(value string) bool {
	address, err := netip.ParseAddr(value)
	if err != nil {
		return false
	}
	_, ok := p.trustedProxies[address.Unmap()]
	return ok
}

func decodeTailscaleServeHeader(value string, maxBytes int) (string, error) {
	if len(value) > maxBytes || !utf8.ValidString(value) || tailscaleHasControl(value) {
		return "", fmt.Errorf("invalid or oversized Tailscale Serve header")
	}
	for _, character := range value {
		if character > 0x7f {
			return "", fmt.Errorf("tailscale Serve header must be ASCII or UTF-8 Q encoded")
		}
	}
	if !strings.HasPrefix(value, "=?") {
		return value, nil
	}
	if !tailscaleQWords.MatchString(value) {
		return "", fmt.Errorf("tailscale Serve header is not strict RFC 2047 UTF-8 Q encoding")
	}
	decoded, err := new(mime.WordDecoder).DecodeHeader(value)
	if err != nil || !utf8.ValidString(decoded) || len(decoded) > maxBytes || tailscaleHasControl(decoded) {
		return "", fmt.Errorf("invalid decoded Tailscale Serve header")
	}
	return decoded, nil
}

func decodeTailscaleCapabilities(raw string, maxBytes int) (map[string]any, error) {
	decoded, err := decodeTailscaleServeHeader(raw, maxBytes)
	if err != nil {
		return nil, err
	}
	value, err := decodeTailscaleJSON([]byte(decoded), int64(maxBytes))
	if err != nil {
		return nil, err
	}
	capabilities, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("tailscale capabilities must be a JSON object")
	}
	for name, rawEntries := range capabilities {
		if name == "" || len(name) > 512 || !strings.Contains(name, "/") || tailscaleHasControl(name) {
			return nil, fmt.Errorf("invalid Tailscale application capability name")
		}
		entries, ok := rawEntries.([]any)
		if !ok {
			return nil, fmt.Errorf("tailscale application capability value must be an array")
		}
		for _, entry := range entries {
			if _, ok := entry.(map[string]any); !ok {
				return nil, fmt.Errorf("tailscale application capability entries must be objects")
			}
		}
	}
	return capabilities, nil
}

// TailscaleLocalAPIOptions configure direct LocalAPI WhoIs evidence.
type TailscaleLocalAPIOptions struct {
	Issuer                 string
	UnixSocket             string
	NamedPipe              string
	Endpoint               string
	Password               string
	Timeout                time.Duration
	MaxResponseBytes       int64
	MaxResponseHeaderBytes int64
}

// TailscaleLocalAPIIdentityProvider resolves a fresh WhoIs snapshot for every
// request. It does not invoke the CLI, cache results, or honor proxy environment
// variables.
type TailscaleLocalAPIIdentityProvider struct {
	issuer           string
	baseURL          string
	password         string
	timeout          time.Duration
	maxResponseBytes int64
	client           *http.Client
	transport        *http.Transport
}

// NewTailscaleLocalAPIIdentityProvider creates a LocalAPI adapter. With no
// transport override it discovers the native Unix socket, Windows named pipe,
// or macOS GUI same-user-proof endpoint for the current platform.
func NewTailscaleLocalAPIIdentityProvider(options TailscaleLocalAPIOptions) (*TailscaleLocalAPIIdentityProvider, error) {
	if options.Issuer == "" || !utf8.ValidString(options.Issuer) || tailscaleHasControl(options.Issuer) {
		return nil, fmt.Errorf("vgirpc: Tailscale issuer must be a non-empty Unicode string without controls")
	}
	configuredTransports := 0
	for _, configured := range []bool{options.UnixSocket != "", options.NamedPipe != "", options.Endpoint != ""} {
		if configured {
			configuredTransports++
		}
	}
	if configuredTransports > 1 {
		return nil, fmt.Errorf("vgirpc: configure only one Tailscale LocalAPI transport")
	}
	if !utf8.ValidString(options.Password) || tailscaleHasControl(options.Password) {
		return nil, fmt.Errorf("vgirpc: Tailscale LocalAPI password contains invalid characters")
	}
	timeout := options.Timeout
	if timeout == 0 {
		timeout = tailscaleDefaultTimeout
	}
	if timeout < 0 {
		return nil, fmt.Errorf("vgirpc: Tailscale LocalAPI timeout must be positive")
	}
	maxResponseBytes := options.MaxResponseBytes
	if maxResponseBytes == 0 {
		maxResponseBytes = tailscaleDefaultResponseBytes
	}
	maxResponseHeaderBytes := options.MaxResponseHeaderBytes
	if maxResponseHeaderBytes == 0 {
		maxResponseHeaderBytes = tailscaleDefaultResponseHeads
	}
	if maxResponseBytes < 0 || maxResponseBytes == int64(^uint64(0)>>1) || maxResponseHeaderBytes < 0 {
		return nil, fmt.Errorf("vgirpc: Tailscale LocalAPI response limits must be positive")
	}

	var localTransport tailscaleLocalAPITransportConfig
	switch {
	case options.Endpoint != "":
		parsed, err := url.Parse(options.Endpoint)
		if err != nil || parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil ||
			(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
			return nil, fmt.Errorf("vgirpc: Tailscale LocalAPI endpoint must be an HTTP origin without userinfo or path")
		}
		localTransport = tailscaleDirectLocalAPITransport("http://"+parsed.Host, options.Password)
	case options.UnixSocket != "":
		if options.Password != "" {
			return nil, fmt.Errorf("vgirpc: Tailscale LocalAPI password is only valid with an HTTP endpoint")
		}
		if !utf8.ValidString(options.UnixSocket) || strings.ContainsRune(options.UnixSocket, 0) {
			return nil, fmt.Errorf("vgirpc: invalid Tailscale LocalAPI Unix socket path")
		}
		localTransport = tailscaleUnixLocalAPITransport(options.UnixSocket)
	case options.NamedPipe != "":
		if options.Password != "" {
			return nil, fmt.Errorf("vgirpc: Tailscale LocalAPI password is only valid with an HTTP endpoint")
		}
		if err := tailscaleValidateNamedPipe(options.NamedPipe); err != nil {
			return nil, err
		}
		var err error
		localTransport, err = tailscaleExplicitNamedPipeLocalAPITransport(options.NamedPipe)
		if err != nil {
			return nil, err
		}
	default:
		if options.Password != "" {
			return nil, fmt.Errorf("vgirpc: Tailscale LocalAPI password requires an explicit HTTP endpoint")
		}
		discoveryContext, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		var err error
		localTransport, err = tailscalePlatformLocalAPITransport(discoveryContext)
		if err != nil {
			return nil, fmt.Errorf("vgirpc: discover Tailscale LocalAPI transport: %w", err)
		}
	}

	transport := &http.Transport{
		Proxy: nil, DialContext: localTransport.dialContext, DisableCompression: true,
		ForceAttemptHTTP2: false, MaxResponseHeaderBytes: maxResponseHeaderBytes,
	}
	client := &http.Client{
		Transport:     transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
	}
	return &TailscaleLocalAPIIdentityProvider{
		issuer: options.Issuer, baseURL: localTransport.baseURL, password: localTransport.password,
		timeout: timeout, maxResponseBytes: maxResponseBytes, client: client, transport: transport,
	}, nil
}

func (p *TailscaleLocalAPIIdentityProvider) Provider() string { return tailscaleProvider }

// Resolve queries the official LocalAPI WhoIs endpoint within the caller and
// provider deadlines.
func (p *TailscaleLocalAPIIdentityProvider) Resolve(ctx context.Context, resolution *PeerResolutionContext) (*PeerIdentityResult, error) {
	if resolution == nil {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityInvalid)
	}
	source := resolution.AssertedPeer()
	if source == "" {
		source = resolution.SourceEndpoint()
	}
	if source == "" {
		source = resolution.ImmediatePeer()
	}
	if source == "" {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityNotApplicable)
	}
	if !utf8.ValidString(source) || len(source) > 4096 || tailscaleHasControl(source) {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityInvalid)
	}
	query := url.Values{"addr": {source}, "proto": {"tcp"}}
	target := map[string]any{"kind": "node"}
	if resolution.ServiceName() != "" {
		serviceName := resolution.ServiceName()
		if !tailscaleServiceName.MatchString(serviceName) {
			return NewPeerIdentityResult(tailscaleProvider, PeerIdentityInvalid)
		}
		query.Set("svc_name", serviceName)
		target = map[string]any{"kind": "service", "value": serviceName}
	} else if resolution.DestinationAddress() != "" {
		destination, err := tailscaleDestinationIP(resolution.DestinationAddress())
		if err != nil {
			return NewPeerIdentityResult(tailscaleProvider, PeerIdentityInvalid)
		}
		query.Set("dst_ip", destination)
		target = map[string]any{"kind": "destination_ip", "value": destination}
	}

	requestContext, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	if deadline := resolution.Deadline(); !deadline.IsZero() {
		var deadlineCancel context.CancelFunc
		requestContext, deadlineCancel = context.WithDeadline(requestContext, deadline)
		defer deadlineCancel()
	}
	request, err := http.NewRequestWithContext(requestContext, http.MethodGet,
		p.baseURL+"/localapi/v0/whois?"+query.Encode(), nil)
	if err != nil {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityInvalid)
	}
	request.Host = tailscaleLocalAPIHost
	request.Header.Set("Accept", "application/json")
	if p.password != "" {
		request.SetBasicAuth("", p.password)
	}
	response, err := p.client.Do(request)
	if err != nil {
		return NewPeerIdentityResult(tailscaleProvider, tailscaleLocalAPIErrorStatus(requestContext, err))
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityPermissionDenied)
	}
	if response.StatusCode == http.StatusNotFound {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityNoMatch)
	}
	if response.StatusCode >= 500 && response.StatusCode <= 599 {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityUnavailable)
	}
	if response.StatusCode != http.StatusOK {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityInvalid)
	}
	contentTypes := response.Header.Values("Content-Type")
	if len(contentTypes) != 1 || strings.ToLower(strings.TrimSpace(strings.SplitN(contentTypes[0], ";", 2)[0])) != "application/json" {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityInvalid)
	}
	if response.ContentLength > p.maxResponseBytes {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityInvalid)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, p.maxResponseBytes+1))
	if err != nil {
		return NewPeerIdentityResult(tailscaleProvider, tailscaleLocalAPIErrorStatus(requestContext, err))
	}
	if int64(len(body)) > p.maxResponseBytes {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityInvalid)
	}
	value, err := decodeTailscaleJSON(body, p.maxResponseBytes)
	if err != nil {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityInvalid)
	}
	identity, err := p.identity(value, resolution, source, target)
	if err != nil {
		return NewPeerIdentityResult(tailscaleProvider, PeerIdentityInvalid)
	}
	return NewAvailablePeerIdentityResult(tailscaleProvider, identity)
}

func (p *TailscaleLocalAPIIdentityProvider) identity(value any, resolution *PeerResolutionContext, source string, target map[string]any) (*PeerIdentity, error) {
	payload, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("LocalAPI WhoIs response must be an object")
	}
	node, ok := payload["Node"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("LocalAPI WhoIs response is missing Node")
	}
	stableID, err := tailscaleOptionalString(node, "StableID")
	if err != nil || tailscaleHasControl(stableID) {
		return nil, fmt.Errorf("invalid LocalAPI StableID")
	}
	nodeName, err := tailscaleOptionalString(node, "Name")
	if err != nil || tailscaleHasControl(nodeName) {
		return nil, fmt.Errorf("invalid LocalAPI node name")
	}
	tags, err := tailscaleTags(node["Tags"])
	if err != nil {
		return nil, err
	}
	capabilities := map[string]any{}
	if rawCapabilities, exists := payload["CapMap"]; exists && rawCapabilities != nil {
		capabilities, ok = rawCapabilities.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("LocalAPI CapMap must be an object")
		}
	}
	for name, rawEntries := range capabilities {
		if name == "" || tailscaleHasControl(name) {
			return nil, fmt.Errorf("invalid LocalAPI capability name")
		}
		if _, ok := rawEntries.([]any); !ok {
			return nil, fmt.Errorf("LocalAPI capability values must be arrays")
		}
	}
	attributes := map[string]any{"tags": tags, "capability_target": target}
	if stableID != "" {
		attributes["node_id"] = stableID
	}
	if nodeName != "" {
		attributes["node_name"] = nodeName
	}
	subjectKind := PeerSubjectUser
	subjectKey := ""
	if len(tags) > 0 {
		if stableID == "" {
			return nil, fmt.Errorf("tagged LocalAPI node lacks StableID")
		}
		subjectKind = PeerSubjectTaggedNode
		subjectKey = "node:" + stableID
	} else {
		profile, ok := payload["UserProfile"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("untagged LocalAPI node lacks UserProfile")
		}
		userID, err := tailscalePositiveInteger(profile["ID"])
		if err != nil {
			return nil, fmt.Errorf("untagged LocalAPI node lacks stable user ID")
		}
		subjectKey = "user:" + userID
		attributes["user_id"] = userID
		for sourceName, targetName := range map[string]string{
			"LoginName": "user_login", "DisplayName": "user_display_name",
		} {
			field, err := tailscaleOptionalString(profile, sourceName)
			if err != nil || tailscaleHasControl(field) {
				return nil, fmt.Errorf("invalid LocalAPI user profile")
			}
			if field != "" {
				attributes[targetName] = field
			}
		}
	}
	return NewPeerIdentity(PeerIdentityOptions{
		Provider: tailscaleProvider, EvidenceSource: "localapi", Assurance: IdentityAssuranceLocalDaemon,
		Issuer: p.issuer, Transport: resolution.Transport(), SubjectKind: subjectKind,
		SubjectKey: subjectKey, SubjectStability: SubjectStabilityStable, SubjectVerified: true,
		Attributes: attributes, Capabilities: capabilities, CapabilitiesVerified: true,
		SourceAddress: tailscaleSourceIP(source),
	})
}

func tailscaleSourceIP(source string) string {
	if host, _, err := net.SplitHostPort(source); err == nil {
		return host
	}
	return source
}

func tailscaleLocalAPIErrorStatus(ctx context.Context, err error) PeerIdentityStatus {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return PeerIdentityUnavailable
	}
	var urlError *url.Error
	if errors.As(err, &urlError) {
		err = urlError.Err
	}
	var operationError *net.OpError
	if errors.As(err, &operationError) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return PeerIdentityUnavailable
	}
	return PeerIdentityInvalid
}

func tailscaleDestinationIP(value string) (string, error) {
	if address, err := netip.ParseAddr(value); err == nil {
		return address.Unmap().String(), nil
	}
	host, _, err := net.SplitHostPort(value)
	if err != nil {
		return "", fmt.Errorf("destination must contain an IP address")
	}
	address, err := netip.ParseAddr(host)
	if err != nil {
		return "", fmt.Errorf("destination must contain an IP address")
	}
	return address.Unmap().String(), nil
}

func tailscaleOptionalString(value map[string]any, key string) (string, error) {
	raw, exists := value[key]
	if !exists || raw == nil {
		return "", nil
	}
	text, ok := raw.(string)
	if !ok || !utf8.ValidString(text) {
		return "", fmt.Errorf("%s must be a string", key)
	}
	return text, nil
}

func tailscaleTags(value any) ([]string, error) {
	if value == nil {
		return []string{}, nil
	}
	rawTags, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("LocalAPI node tags must be an array")
	}
	tags := make([]string, 0, len(rawTags))
	for _, rawTag := range rawTags {
		tag, ok := rawTag.(string)
		if !ok || !strings.HasPrefix(tag, "tag:") || tailscaleHasControl(tag) {
			return nil, fmt.Errorf("invalid LocalAPI node tag")
		}
		tags = append(tags, tag)
	}
	return tags, nil
}

func tailscalePositiveInteger(value any) (string, error) {
	number, ok := value.(json.Number)
	if !ok {
		return "", fmt.Errorf("value is not an integer")
	}
	integer, err := strconv.ParseInt(string(number), 10, 64)
	if err != nil || integer <= 0 {
		return "", fmt.Errorf("value is not a positive integer")
	}
	return strconv.FormatInt(integer, 10), nil
}

func tailscaleHasControl(value string) bool {
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return true
		}
	}
	return false
}

func decodeTailscaleJSON(data []byte, maxBytes int64) (any, error) {
	if int64(len(data)) > maxBytes || !utf8.Valid(data) || !tailscaleValidJSONEscapes(data) {
		return nil, fmt.Errorf("invalid or oversized Tailscale JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	count := 0
	value, err := tailscaleJSONValue(decoder, 0, &count)
	if err != nil {
		return nil, err
	}
	if _, err = decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("trailing Tailscale JSON data")
	}
	return value, nil
}

func tailscaleJSONValue(decoder *json.Decoder, depth int, count *int) (any, error) {
	if depth > 16 {
		return nil, fmt.Errorf("tailscale JSON exceeds maximum depth")
	}
	*count++
	if *count > 4096 {
		return nil, fmt.Errorf("tailscale JSON exceeds maximum value count")
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return token, nil
	}
	switch delimiter {
	case '{':
		object := map[string]any{}
		for decoder.More() {
			rawKey, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			key, ok := rawKey.(string)
			if !ok {
				return nil, fmt.Errorf("tailscale JSON object key is not a string")
			}
			if _, duplicate := object[key]; duplicate {
				return nil, fmt.Errorf("duplicate Tailscale JSON key %q", key)
			}
			value, err := tailscaleJSONValue(decoder, depth+1, count)
			if err != nil {
				return nil, err
			}
			object[key] = value
		}
		if closing, err := decoder.Token(); err != nil || closing != json.Delim('}') {
			return nil, fmt.Errorf("unterminated Tailscale JSON object")
		}
		return object, nil
	case '[':
		array := []any{}
		for decoder.More() {
			value, err := tailscaleJSONValue(decoder, depth+1, count)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		if closing, err := decoder.Token(); err != nil || closing != json.Delim(']') {
			return nil, fmt.Errorf("unterminated Tailscale JSON array")
		}
		return array, nil
	default:
		return nil, fmt.Errorf("unexpected Tailscale JSON delimiter")
	}
}

func tailscaleValidJSONEscapes(data []byte) bool {
	inString := false
	for index := 0; index < len(data); index++ {
		switch data[index] {
		case '"':
			inString = !inString
		case '\\':
			if !inString || index+1 >= len(data) {
				continue
			}
			if data[index+1] != 'u' {
				index++
				continue
			}
			first, ok := tailscaleHex4(data, index+2)
			if !ok {
				return false
			}
			index += 5
			if first >= 0xd800 && first <= 0xdbff {
				if index+6 >= len(data) || data[index+1] != '\\' || data[index+2] != 'u' {
					return false
				}
				second, ok := tailscaleHex4(data, index+3)
				if !ok || second < 0xdc00 || second > 0xdfff {
					return false
				}
				index += 6
			} else if first >= 0xdc00 && first <= 0xdfff {
				return false
			}
		}
	}
	return true
}

func tailscaleHex4(data []byte, start int) (uint16, bool) {
	if start+4 > len(data) {
		return 0, false
	}
	value, err := strconv.ParseUint(string(data[start:start+4]), 16, 16)
	return uint16(value), err == nil
}
