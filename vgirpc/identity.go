// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

// PeerIdentityStatus is the outcome of resolving one identity provider.
type PeerIdentityStatus string

const (
	PeerIdentityOff              PeerIdentityStatus = "off"
	PeerIdentityNotApplicable    PeerIdentityStatus = "not_applicable"
	PeerIdentityAvailable        PeerIdentityStatus = "available"
	PeerIdentityUnavailable      PeerIdentityStatus = "unavailable"
	PeerIdentityPermissionDenied PeerIdentityStatus = "permission_denied"
	PeerIdentityNoMatch          PeerIdentityStatus = "no_match"
	PeerIdentityInvalid          PeerIdentityStatus = "invalid"
	PeerIdentityUntrustedProxy   PeerIdentityStatus = "untrusted_proxy"
)

// IdentityAssurance describes how peer evidence was verified.
type IdentityAssurance string

const (
	IdentityAssuranceCryptographicPeer IdentityAssurance = "cryptographic_peer"
	IdentityAssuranceLocalDaemon       IdentityAssurance = "local_daemon"
	IdentityAssuranceConfiguredProxy   IdentityAssurance = "configured_proxy"
)

// PeerSubjectKind classifies the subject named by peer evidence.
type PeerSubjectKind string

const (
	PeerSubjectUser       PeerSubjectKind = "user"
	PeerSubjectTaggedNode PeerSubjectKind = "tagged_node"
	PeerSubjectWorkload   PeerSubjectKind = "workload"
	PeerSubjectEndpoint   PeerSubjectKind = "endpoint"
	PeerSubjectUnknown    PeerSubjectKind = "unknown"
)

// SubjectStability describes whether a provider subject is safe as a durable principal.
type SubjectStability string

const (
	SubjectStabilityStable SubjectStability = "stable"
	SubjectStabilityLogin  SubjectStability = "login"
	SubjectStabilityNone   SubjectStability = "none"
)

// PeerResolutionOptions are snapshotted by NewPeerResolutionContext.
type PeerResolutionOptions struct {
	ImmediatePeer      string
	SourceEndpoint     string
	AssertedPeer       string
	DestinationAddress string
	Authority          string
	ServiceName        string
	Headers            map[string][]string
	Metadata           map[string]any
	Deadline           time.Time
}

// PeerResolutionContext is an immutable, transport-neutral request snapshot.
type PeerResolutionContext struct {
	transport          string
	immediatePeer      string
	sourceEndpoint     string
	assertedPeer       string
	destinationAddress string
	authority          string
	serviceName        string
	headers            map[string][]string
	metadataJSON       []byte
	deadline           time.Time
}

// NewPeerResolutionContext validates and snapshots provider input.
func NewPeerResolutionContext(transport string, options PeerResolutionOptions) (*PeerResolutionContext, error) {
	if transport == "" || !utf8.ValidString(transport) {
		return nil, fmt.Errorf("vgirpc: peer transport must not be empty")
	}
	for name, value := range map[string]string{
		"immediate peer": options.ImmediatePeer, "source endpoint": options.SourceEndpoint,
		"asserted peer":       options.AssertedPeer,
		"destination address": options.DestinationAddress, "authority": options.Authority,
		"service name": options.ServiceName,
	} {
		if !utf8.ValidString(value) {
			return nil, fmt.Errorf("vgirpc: %s contains invalid UTF-8", name)
		}
	}
	headers := make(map[string][]string, len(options.Headers))
	headerBytes, headerValues := 0, 0
	for name, values := range options.Headers {
		if name == "" || !utf8.ValidString(name) || containsIdentityControl(name) {
			return nil, fmt.Errorf("vgirpc: invalid peer-resolution header name")
		}
		key := strings.ToLower(name)
		if _, exists := headers[key]; exists {
			return nil, NewAuthFailure(AuthReasonInvalidCredential, "case-varied duplicate peer identity header")
		}
		cloned := append([]string(nil), values...)
		for _, value := range cloned {
			if !utf8.ValidString(value) || containsIdentityControl(value) {
				return nil, fmt.Errorf("vgirpc: invalid peer-resolution header value: %s", name)
			}
			headerBytes += len(name) + len(value)
		}
		headerValues += len(cloned)
		if headerValues > 256 || headerBytes > 65_536 {
			return nil, fmt.Errorf("vgirpc: peer-resolution headers exceed safety limits")
		}
		headers[key] = cloned
	}
	metadataJSON, err := canonicalJSONMap(options.Metadata)
	if err != nil {
		return nil, fmt.Errorf("vgirpc: invalid peer metadata: %w", err)
	}
	return &PeerResolutionContext{
		transport:          transport,
		immediatePeer:      options.ImmediatePeer,
		sourceEndpoint:     options.SourceEndpoint,
		assertedPeer:       options.AssertedPeer,
		destinationAddress: options.DestinationAddress,
		authority:          options.Authority,
		serviceName:        options.ServiceName,
		headers:            headers,
		metadataJSON:       metadataJSON,
		deadline:           options.Deadline,
	}, nil
}

func containsIdentityControl(value string) bool {
	return strings.ContainsAny(value, "\r\n\x00")
}

func (c *PeerResolutionContext) Transport() string          { return c.transport }
func (c *PeerResolutionContext) ImmediatePeer() string      { return c.immediatePeer }
func (c *PeerResolutionContext) SourceEndpoint() string     { return c.sourceEndpoint }
func (c *PeerResolutionContext) AssertedPeer() string       { return c.assertedPeer }
func (c *PeerResolutionContext) DestinationAddress() string { return c.destinationAddress }
func (c *PeerResolutionContext) Authority() string          { return c.authority }
func (c *PeerResolutionContext) ServiceName() string        { return c.serviceName }
func (c *PeerResolutionContext) Deadline() time.Time        { return c.deadline }

// Header returns one header value and rejects ambiguous duplicates.
func (c *PeerResolutionContext) Header(name string) (string, bool, error) {
	values := c.headers[strings.ToLower(name)]
	switch len(values) {
	case 0:
		return "", false, nil
	case 1:
		return values[0], true, nil
	default:
		return "", false, NewAuthFailure(AuthReasonInvalidCredential, "duplicate peer identity header")
	}
}

// Metadata returns a detached JSON-compatible copy of provider metadata.
func (c *PeerResolutionContext) Metadata() map[string]any {
	return decodeJSONMap(c.metadataJSON)
}

// PeerIdentityOptions are validated and snapshotted by NewPeerIdentity.
type PeerIdentityOptions struct {
	Provider             string
	EvidenceSource       string
	Assurance            IdentityAssurance
	Issuer               string
	Transport            string
	SubjectKind          PeerSubjectKind
	SubjectKey           string
	SubjectStability     SubjectStability
	SubjectVerified      bool
	Attributes           map[string]any
	Capabilities         map[string]any
	CapabilitiesVerified bool
	SourceAddress        string
	ProxyAddress         string
}

// PeerIdentity is an immutable snapshot of verified or observed evidence.
type PeerIdentity struct {
	provider             string
	evidenceSource       string
	assurance            IdentityAssurance
	issuer               string
	transport            string
	subjectKind          PeerSubjectKind
	subjectKey           string
	subjectStability     SubjectStability
	subjectVerified      bool
	attributesJSON       []byte
	capabilitiesJSON     []byte
	capabilitiesVerified bool
	sourceAddress        string
	proxyAddress         string
}

// NewPeerIdentity validates invariants and snapshots structured evidence.
func NewPeerIdentity(options PeerIdentityOptions) (*PeerIdentity, error) {
	if options.Provider == "" || options.EvidenceSource == "" || options.Issuer == "" || options.Transport == "" {
		return nil, fmt.Errorf("vgirpc: provider, evidence source, issuer, and transport are required")
	}
	for name, value := range map[string]string{
		"provider": options.Provider, "evidence source": options.EvidenceSource,
		"issuer": options.Issuer, "transport": options.Transport,
		"subject key": options.SubjectKey, "source address": options.SourceAddress,
		"proxy address": options.ProxyAddress,
	} {
		if !utf8.ValidString(value) {
			return nil, fmt.Errorf("vgirpc: %s contains invalid UTF-8", name)
		}
	}
	if options.SubjectKind == "" {
		options.SubjectKind = PeerSubjectUnknown
	}
	if options.SubjectStability == "" {
		options.SubjectStability = SubjectStabilityNone
	}
	if options.Assurance != IdentityAssuranceCryptographicPeer && options.Assurance != IdentityAssuranceLocalDaemon && options.Assurance != IdentityAssuranceConfiguredProxy {
		return nil, fmt.Errorf("vgirpc: invalid peer identity assurance %q", options.Assurance)
	}
	if options.SubjectKind != PeerSubjectUser && options.SubjectKind != PeerSubjectTaggedNode && options.SubjectKind != PeerSubjectWorkload && options.SubjectKind != PeerSubjectEndpoint && options.SubjectKind != PeerSubjectUnknown {
		return nil, fmt.Errorf("vgirpc: invalid peer subject kind %q", options.SubjectKind)
	}
	if options.SubjectStability != SubjectStabilityStable && options.SubjectStability != SubjectStabilityLogin && options.SubjectStability != SubjectStabilityNone {
		return nil, fmt.Errorf("vgirpc: invalid peer subject stability %q", options.SubjectStability)
	}
	if options.SubjectVerified && options.SubjectKey == "" {
		return nil, fmt.Errorf("vgirpc: verified peer identity requires a subject key")
	}
	if options.SubjectKey == "" && options.SubjectStability != SubjectStabilityNone {
		return nil, fmt.Errorf("vgirpc: subjectless peer identity must use none stability")
	}
	attributes, err := canonicalJSONMap(options.Attributes)
	if err != nil {
		return nil, fmt.Errorf("vgirpc: invalid peer attributes: %w", err)
	}
	capabilities, err := canonicalJSONMap(options.Capabilities)
	if err != nil {
		return nil, fmt.Errorf("vgirpc: invalid peer capabilities: %w", err)
	}
	return &PeerIdentity{
		provider:             options.Provider,
		evidenceSource:       options.EvidenceSource,
		assurance:            options.Assurance,
		issuer:               options.Issuer,
		transport:            options.Transport,
		subjectKind:          options.SubjectKind,
		subjectKey:           options.SubjectKey,
		subjectStability:     options.SubjectStability,
		subjectVerified:      options.SubjectVerified,
		attributesJSON:       attributes,
		capabilitiesJSON:     capabilities,
		capabilitiesVerified: options.CapabilitiesVerified,
		sourceAddress:        options.SourceAddress,
		proxyAddress:         options.ProxyAddress,
	}, nil
}

func (i *PeerIdentity) Provider() string                   { return i.provider }
func (i *PeerIdentity) EvidenceSource() string             { return i.evidenceSource }
func (i *PeerIdentity) Assurance() IdentityAssurance       { return i.assurance }
func (i *PeerIdentity) Issuer() string                     { return i.issuer }
func (i *PeerIdentity) Transport() string                  { return i.transport }
func (i *PeerIdentity) SubjectKind() PeerSubjectKind       { return i.subjectKind }
func (i *PeerIdentity) SubjectKey() string                 { return i.subjectKey }
func (i *PeerIdentity) SubjectStability() SubjectStability { return i.subjectStability }
func (i *PeerIdentity) SubjectVerified() bool              { return i.subjectVerified }
func (i *PeerIdentity) CapabilitiesVerified() bool         { return i.capabilitiesVerified }
func (i *PeerIdentity) SourceAddress() string              { return i.sourceAddress }
func (i *PeerIdentity) ProxyAddress() string               { return i.proxyAddress }
func (i *PeerIdentity) Attributes() map[string]any         { return decodeJSONMap(i.attributesJSON) }
func (i *PeerIdentity) Capabilities() map[string]any       { return decodeJSONMap(i.capabilitiesJSON) }

// CanonicalPrincipal namespaces a stable subject by provider and issuer.
func (i *PeerIdentity) CanonicalPrincipal() (string, error) {
	if i.subjectKey == "" {
		return "", fmt.Errorf("vgirpc: subjectless peer evidence has no canonical principal")
	}
	return "peer/" + percentIdentity(i.provider) + "/" + percentIdentity(i.issuer) + "/" + percentIdentity(i.subjectKey), nil
}

func percentIdentity(value string) string {
	const hexChars = "0123456789ABCDEF"
	var out strings.Builder
	for _, b := range []byte(value) {
		if (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || strings.ContainsRune("-._~", rune(b)) {
			out.WriteByte(b)
		} else {
			out.WriteByte('%')
			out.WriteByte(hexChars[b>>4])
			out.WriteByte(hexChars[b&15])
		}
	}
	return out.String()
}

// PeerIdentityResult is one provider's validated resolution outcome.
type PeerIdentityResult struct {
	provider   string
	status     PeerIdentityStatus
	identities []*PeerIdentity
}

// NewPeerIdentityResult constructs a result without identities.
func NewPeerIdentityResult(provider string, status PeerIdentityStatus) (*PeerIdentityResult, error) {
	if provider == "" || !utf8.ValidString(provider) || status == PeerIdentityAvailable || !validPeerIdentityStatus(status) {
		return nil, fmt.Errorf("vgirpc: non-available peer result requires provider and status")
	}
	return &PeerIdentityResult{provider: provider, status: status}, nil
}

func validPeerIdentityStatus(status PeerIdentityStatus) bool {
	switch status {
	case PeerIdentityOff, PeerIdentityNotApplicable, PeerIdentityAvailable, PeerIdentityUnavailable,
		PeerIdentityPermissionDenied, PeerIdentityNoMatch, PeerIdentityInvalid, PeerIdentityUntrustedProxy:
		return true
	default:
		return false
	}
}

// NewAvailablePeerIdentityResult constructs an available result.
func NewAvailablePeerIdentityResult(provider string, identities ...*PeerIdentity) (*PeerIdentityResult, error) {
	if provider == "" || !utf8.ValidString(provider) || len(identities) == 0 {
		return nil, fmt.Errorf("vgirpc: available peer result requires identities")
	}
	cloned := append([]*PeerIdentity(nil), identities...)
	for _, identity := range cloned {
		if identity == nil || identity.provider != provider {
			return nil, fmt.Errorf("vgirpc: peer result provider mismatch")
		}
	}
	return &PeerIdentityResult{provider: provider, status: PeerIdentityAvailable, identities: cloned}, nil
}

func (r *PeerIdentityResult) Provider() string           { return r.provider }
func (r *PeerIdentityResult) Status() PeerIdentityStatus { return r.status }
func (r *PeerIdentityResult) Identities() []*PeerIdentity {
	return append([]*PeerIdentity(nil), r.identities...)
}

// PeerEvidenceSet is an immutable snapshot of every provider result.
type PeerEvidenceSet struct {
	identities []*PeerIdentity
	statuses   map[string]PeerIdentityStatus
}

var emptyPeerEvidence = &PeerEvidenceSet{statuses: map[string]PeerIdentityStatus{}}

// EmptyPeerEvidence returns the shared empty evidence snapshot.
func EmptyPeerEvidence() *PeerEvidenceSet { return emptyPeerEvidence }

// NewPeerEvidenceSet combines results and rejects duplicate providers.
func NewPeerEvidenceSet(results ...*PeerIdentityResult) (*PeerEvidenceSet, error) {
	if len(results) == 0 {
		return EmptyPeerEvidence(), nil
	}
	evidence := &PeerEvidenceSet{statuses: make(map[string]PeerIdentityStatus, len(results))}
	for _, result := range results {
		if result == nil {
			return nil, fmt.Errorf("vgirpc: nil peer identity result")
		}
		if _, exists := evidence.statuses[result.provider]; exists {
			return nil, fmt.Errorf("vgirpc: duplicate peer identity provider: %s", result.provider)
		}
		if !utf8.ValidString(result.provider) || !validPeerIdentityStatus(result.status) ||
			(result.status == PeerIdentityAvailable) != (len(result.identities) > 0) {
			return nil, fmt.Errorf("vgirpc: invalid peer identity result")
		}
		evidence.statuses[result.provider] = result.status
		evidence.identities = append(evidence.identities, result.identities...)
	}
	return evidence, nil
}

func (e *PeerEvidenceSet) Status(provider string) PeerIdentityStatus {
	if e == nil {
		return PeerIdentityOff
	}
	if status, ok := e.statuses[provider]; ok {
		return status
	}
	return PeerIdentityOff
}

func (e *PeerEvidenceSet) Identities() []*PeerIdentity {
	if e == nil {
		return nil
	}
	return append([]*PeerIdentity(nil), e.identities...)
}

func (e *PeerEvidenceSet) ForProvider(provider string) []*PeerIdentity {
	if e == nil {
		return nil
	}
	var matches []*PeerIdentity
	for _, identity := range e.identities {
		if identity.provider == provider {
			matches = append(matches, identity)
		}
	}
	return matches
}

func (e *PeerEvidenceSet) UniqueVerifiedSubject(provider string) (*PeerIdentity, error) {
	var match *PeerIdentity
	for _, identity := range e.ForProvider(provider) {
		if identity.subjectVerified && identity.subjectKey != "" && identity.subjectStability == SubjectStabilityStable {
			if match != nil {
				return nil, NewAuthFailure(AuthReasonInvalidCredential, "peer provider produced ambiguous verified subjects")
			}
			match = identity
		}
	}
	if match == nil {
		return nil, NewAuthFailure(AuthReasonMissingCredential, "peer provider did not produce a verified stable subject")
	}
	return match, nil
}

func (e *PeerEvidenceSet) RequireUsableProvider(provider string) (*PeerIdentity, error) {
	switch e.Status(provider) {
	case PeerIdentityUnavailable, PeerIdentityPermissionDenied:
		return nil, NewAuthUnavailable(fmt.Sprintf("peer identity provider %q is unavailable", provider))
	case PeerIdentityInvalid:
		return nil, NewAuthFailure(AuthReasonInvalidCredential, "peer identity provider rejected evidence")
	case PeerIdentityUntrustedProxy:
		return nil, NewAuthFailure(AuthReasonProxyRequired, "peer identity provider rejected its proxy boundary")
	}
	return e.UniqueVerifiedSubject(provider)
}

// RequireAvailableProvider accepts valid evidence, including capability-only evidence,
// without promoting it to an authentication principal.
func (e *PeerEvidenceSet) RequireAvailableProvider(provider string) ([]*PeerIdentity, error) {
	switch e.Status(provider) {
	case PeerIdentityUnavailable, PeerIdentityPermissionDenied:
		return nil, NewAuthUnavailable(fmt.Sprintf("peer identity provider %q is unavailable", provider))
	case PeerIdentityInvalid:
		return nil, NewAuthFailure(AuthReasonInvalidCredential, "peer identity provider rejected evidence")
	case PeerIdentityUntrustedProxy:
		return nil, NewAuthFailure(AuthReasonProxyRequired, "peer identity provider rejected its proxy boundary")
	case PeerIdentityAvailable:
		identities := e.ForProvider(provider)
		if len(identities) > 0 {
			return identities, nil
		}
	}
	return nil, NewAuthFailure(AuthReasonMissingCredential, "peer identity provider did not produce evidence")
}

// BindingDigest hashes all authorization-relevant peer evidence.
func (e *PeerEvidenceSet) BindingDigest(providers []string, applicationAuth *AuthContext) string {
	providers = uniqueSortedStrings(providers)
	digest := sha256.New()
	for _, provider := range providers {
		hashIdentityField(digest, provider)
		hashIdentityField(digest, string(e.Status(provider)))
		var identities [][]string
		for _, identity := range e.ForProvider(provider) {
			identities = append(identities, []string{
				identity.provider, identity.issuer, identity.subjectKey, string(identity.assurance),
				identity.evidenceSource, identity.transport, string(identity.subjectKind),
				string(identity.subjectStability), fmt.Sprintf("%t", identity.subjectVerified),
				fmt.Sprintf("%t", identity.capabilitiesVerified), "",
				"", string(identity.attributesJSON), string(identity.capabilitiesJSON),
			})
		}
		sort.Slice(identities, func(a, b int) bool { return compareIdentityFields(identities[a], identities[b]) < 0 })
		for _, fields := range identities {
			for _, field := range fields {
				hashIdentityField(digest, field)
			}
		}
	}
	if applicationAuth != nil {
		hashIdentityField(digest, "application_auth")
		hashIdentityField(digest, applicationAuth.Domain)
		hashIdentityField(digest, applicationAuth.Principal)
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func compareIdentityFields(a, b []string) int {
	for index := range a {
		if a[index] < b[index] {
			return -1
		}
		if a[index] > b[index] {
			return 1
		}
	}
	return 0
}

func hashIdentityField(digest hash.Hash, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len([]byte(value))))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write([]byte(value))
}

func uniqueSortedStrings(values []string) []string {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func canonicalJSONMap(value map[string]any) ([]byte, error) {
	if value == nil {
		return []byte("{}"), nil
	}
	count := 0
	if err := validatePeerJSON(reflect.ValueOf(value), 1, &count); err != nil {
		return nil, err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(data) > 65_536 {
		return nil, fmt.Errorf("peer evidence exceeds maximum JSON byte size")
	}
	return data, nil
}

func validatePeerJSON(value reflect.Value, depth int, count *int) error {
	if depth > 16 {
		return fmt.Errorf("peer evidence exceeds maximum JSON depth")
	}
	if !value.IsValid() || (value.Kind() == reflect.Interface && value.IsNil()) {
		*count++
		return nil
	}
	if value.Kind() == reflect.Interface {
		return validatePeerJSON(value.Elem(), depth, count)
	}
	*count++
	if *count > 4_096 {
		return fmt.Errorf("peer evidence exceeds maximum JSON value count")
	}
	switch value.Kind() {
	case reflect.String:
		if !utf8.ValidString(value.String()) {
			return fmt.Errorf("peer evidence contains invalid UTF-8")
		}
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
	case reflect.Float32, reflect.Float64:
		if number := value.Float(); math.IsNaN(number) || math.IsInf(number, 0) {
			return fmt.Errorf("peer evidence numbers must be finite")
		}
	case reflect.Map:
		if value.Type().Key().Kind() != reflect.String {
			return fmt.Errorf("peer evidence object keys must be strings")
		}
		iterator := value.MapRange()
		for iterator.Next() {
			if !utf8.ValidString(iterator.Key().String()) {
				return fmt.Errorf("peer evidence contains invalid UTF-8 object key")
			}
			if err := validatePeerJSON(iterator.Value(), depth+1, count); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		for index := 0; index < value.Len(); index++ {
			if err := validatePeerJSON(value.Index(index), depth+1, count); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("peer evidence contains non-JSON value of type %s", value.Type())
	}
	return nil
}

func decodeJSONMap(data []byte) map[string]any {
	if len(data) == 0 {
		return map[string]any{}
	}
	var value map[string]any
	_ = json.Unmarshal(data, &value)
	return value
}

// PeerIdentityProvider resolves one provider without exceeding the supplied context deadline.
type PeerIdentityProvider interface {
	Provider() string
	Resolve(context.Context, *PeerResolutionContext) (*PeerIdentityResult, error)
}

// PeerAuthenticationPolicy composes application auth and peer evidence.
type PeerAuthenticationPolicy func(*PeerEvidenceSet, *AuthContext) (*AuthContext, error)

// invokePeerIdentityProvider contains provider panics at the extension boundary.
// Recovered values are deliberately discarded because they may contain tokens,
// capabilities, certificate material, or other provider-controlled secrets.
func invokePeerIdentityProvider(
	provider PeerIdentityProvider,
	ctx context.Context,
	resolution *PeerResolutionContext,
) (result *PeerIdentityResult, err error) {
	defer func() {
		if recover() != nil {
			result = nil
			err = fmt.Errorf("peer identity provider failed")
		}
	}()
	return provider.Resolve(ctx, resolution)
}

// invokePeerAuthenticationPolicy contains policy panics without exposing the
// recovered value to callers or the process-wide panic logger.
func invokePeerAuthenticationPolicy(
	policy PeerAuthenticationPolicy,
	evidence *PeerEvidenceSet,
	auth *AuthContext,
) (resolved *AuthContext, err error) {
	defer func() {
		if recover() != nil {
			resolved = nil
			err = fmt.Errorf("peer authentication policy failed")
		}
	}()
	return policy(evidence, auth)
}

// PeerIdentityLinker rejects conflicting application and transport identities.
type PeerIdentityLinker func(*AuthContext, map[string]*PeerIdentity) error

func ObservePeerIdentity(_ *PeerEvidenceSet, auth *AuthContext) (*AuthContext, error) {
	return auth, nil
}

func RequirePeerIdentity(provider string) PeerAuthenticationPolicy {
	return func(evidence *PeerEvidenceSet, auth *AuthContext) (*AuthContext, error) {
		if _, err := evidence.RequireAvailableProvider(provider); err != nil {
			return nil, err
		}
		return withPeerEvidenceBinding(auth, evidence, []string{provider}, nil), nil
	}
}

func PeerIdentityPrimary(provider string) PeerAuthenticationPolicy {
	return func(evidence *PeerEvidenceSet, _ *AuthContext) (*AuthContext, error) {
		identity, err := evidence.RequireUsableProvider(provider)
		if err != nil {
			return nil, err
		}
		principal, err := identity.CanonicalPrincipal()
		if err != nil {
			return nil, err
		}
		return &AuthContext{Domain: provider, Authenticated: true, Principal: principal, Claims: map[string]any{
			"issuer": identity.issuer, "subject_kind": string(identity.subjectKind),
			"assurance": string(identity.assurance), "evidence_source": identity.evidenceSource,
			"subject": identity.subjectKey, "peer_evidence_binding": evidence.BindingDigest([]string{provider}, nil),
		}}, nil
	}
}

func AnyOfPeerIdentities(providers ...string) (PeerAuthenticationPolicy, error) {
	if len(providers) == 0 {
		return nil, fmt.Errorf("vgirpc: at least one peer provider is required")
	}
	providers = append([]string(nil), providers...)
	return func(evidence *PeerEvidenceSet, auth *AuthContext) (*AuthContext, error) {
		for _, provider := range providers {
			switch evidence.Status(provider) {
			case PeerIdentityInvalid:
				return nil, NewAuthFailure(AuthReasonInvalidCredential, "peer identity provider rejected evidence")
			case PeerIdentityUntrustedProxy:
				return nil, NewAuthFailure(AuthReasonProxyRequired, "peer identity provider rejected its proxy boundary")
			case PeerIdentityAvailable:
				if _, err := evidence.UniqueVerifiedSubject(provider); err != nil && len(eligiblePeerSubjects(evidence, provider)) > 1 {
					return nil, err
				}
			}
		}
		if auth != nil && auth.Authenticated {
			return auth, nil
		}
		for _, provider := range providers {
			if evidence.Status(provider) != PeerIdentityAvailable || len(eligiblePeerSubjects(evidence, provider)) != 1 {
				continue
			}
			return PeerIdentityPrimary(provider)(evidence, auth)
		}
		for _, provider := range providers {
			if status := evidence.Status(provider); status == PeerIdentityUnavailable || status == PeerIdentityPermissionDenied {
				return nil, NewAuthUnavailable("no usable authentication factor; a peer provider is unavailable")
			}
		}
		return nil, NewAuthFailure(AuthReasonMissingCredential, "no configured provider produced a verified subject")
	}, nil
}

func eligiblePeerSubjects(evidence *PeerEvidenceSet, provider string) []*PeerIdentity {
	var eligible []*PeerIdentity
	for _, identity := range evidence.ForProvider(provider) {
		if identity.subjectVerified && identity.subjectKey != "" && identity.subjectStability == SubjectStabilityStable {
			eligible = append(eligible, identity)
		}
	}
	return eligible
}

func AllOfPeerIdentities(providers []string, principalProvider string, linker PeerIdentityLinker) (PeerAuthenticationPolicy, error) {
	if len(providers) == 0 || linker == nil {
		return nil, fmt.Errorf("vgirpc: all-of requires providers and an identity linker")
	}
	providers = append([]string(nil), providers...)
	if principalProvider == "" {
		principalProvider = providers[0]
	}
	found := false
	for _, provider := range providers {
		found = found || provider == principalProvider
	}
	if !found {
		return nil, fmt.Errorf("vgirpc: principal provider must be one of the required providers")
	}
	return func(evidence *PeerEvidenceSet, auth *AuthContext) (*AuthContext, error) {
		if auth == nil || !auth.Authenticated {
			return nil, NewAuthFailure(AuthReasonMissingCredential, "all-of requires application authentication")
		}
		identities := make(map[string]*PeerIdentity, len(providers))
		for _, provider := range providers {
			identity, err := evidence.RequireUsableProvider(provider)
			if err != nil {
				return nil, err
			}
			identities[provider] = identity
		}
		if err := linker(auth, identities); err != nil {
			return nil, err
		}
		primary, err := PeerIdentityPrimary(principalProvider)(evidence, auth)
		if err != nil {
			return nil, err
		}
		primary.Claims["application_domain"] = auth.Domain
		primary.Claims["application_principal"] = auth.Principal
		primary.Claims["peer_evidence_binding"] = evidence.BindingDigest(providers, auth)
		return primary, nil
	}, nil
}

func withPeerEvidenceBinding(auth *AuthContext, evidence *PeerEvidenceSet, providers []string, applicationAuth *AuthContext) *AuthContext {
	if auth == nil {
		auth = Anonymous()
	}
	claims := make(map[string]any, len(auth.Claims)+1)
	for key, value := range auth.Claims {
		claims[key] = value
	}
	claims["peer_evidence_binding"] = evidence.BindingDigest(providers, applicationAuth)
	return &AuthContext{Domain: auth.Domain, Authenticated: auth.Authenticated, Principal: auth.Principal, Claims: claims}
}
