// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

// Protocol version negotiation, v1 (vgi-rpc-python
// tools/cross-port/specs/PROTOCOL_VERSIONS_V1.md; WIRE_PROTOCOL.md §13).
//
// Every server advertises, per protocol it gates, the inclusive window of
// major.minor versions it accepts:
//
//	VGI-Protocol-Versions: vgi.v3=3.4-3.4, vgi.secret.v1=1.0-1.0
//
// on every HTTP response, and as the vgi_rpc.protocol_versions metadata value
// on reflection results, __transport_options__ replies and
// protocol_version_unsupported refusals on every transport. A client keeps an
// [EndpointNegotiation] per endpoint, learns the server's windows from
// whatever it sees first, and speaks the highest version both sides support.

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

const (
	// ProtocolVersionsHeader is the HTTP response header carrying the server's
	// windows. Present on every response; empty when nothing is gated.
	ProtocolVersionsHeader = "VGI-Protocol-Versions"
	// ClientProtocolVersionsHeader is the HTTP request header carrying the
	// client's own windows. Informational: a server never refuses because of it.
	ClientProtocolVersionsHeader = "VGI-Client-Protocol-Versions"
	// MetaProtocolVersions carries the server's windows on reflection results,
	// __transport_options__ replies and refusals.
	MetaProtocolVersions = "vgi_rpc.protocol_versions"
	// MetaClientProtocolVersions carries the client's own windows on a request.
	// Informational.
	MetaClientProtocolVersions = "vgi_rpc.protocol_versions_client"
	// ErrorKindProtocolVersionUnsupported is the error_kind of a refusal.
	ErrorKindProtocolVersionUnsupported = "protocol_version_unsupported"
	// ErrorKindProtocolVersionMismatch is the kind servers released before
	// negotiation refused with. Clients still recognise it.
	ErrorKindProtocolVersionMismatch = "protocol_version_mismatch"
)

// MajorMinor is a negotiated protocol version: only major.minor is
// negotiated, patch is informational.
type MajorMinor struct {
	Major int
	Minor int
}

// Less reports whether m is older than o.
func (m MajorMinor) Less(o MajorMinor) bool {
	return m.Major < o.Major || (m.Major == o.Major && m.Minor < o.Minor)
}

// String renders "MAJOR.MINOR".
func (m MajorMinor) String() string {
	return strconv.Itoa(m.Major) + "." + strconv.Itoa(m.Minor)
}

var majorMinorRegex = regexp.MustCompile(`^(0|[1-9]\d*)\.(0|[1-9]\d*)$`)

// parseMajorMinor reads "MAJOR.MINOR"; a full "MAJOR.MINOR.PATCH" is accepted
// and its patch dropped.
func parseMajorMinor(text string) (MajorMinor, error) {
	text = strings.TrimSpace(text)
	if m := majorMinorRegex.FindStringSubmatch(text); m != nil {
		major, _ := strconv.Atoi(m[1])
		minor, _ := strconv.Atoi(m[2])
		return MajorMinor{major, minor}, nil
	}
	major, minor, _, err := parseSemver(text)
	if err != nil {
		return MajorMinor{}, err
	}
	return MajorMinor{major, minor}, nil
}

// VersionRange is an inclusive window of major.minor versions.
type VersionRange struct {
	Min MajorMinor
	Max MajorMinor
}

// ParseVersionRange reads "MIN-MAX" (each MAJOR.MINOR), or a single
// MAJOR.MINOR meaning "V-V".
func ParseVersionRange(text string) (VersionRange, error) {
	lo, hi, found := strings.Cut(strings.TrimSpace(text), "-")
	low, err := parseMajorMinor(lo)
	if err != nil {
		return VersionRange{}, fmt.Errorf("protocol version range %q: %w", text, err)
	}
	high := low
	if found {
		if high, err = parseMajorMinor(hi); err != nil {
			return VersionRange{}, fmt.Errorf("protocol version range %q: %w", text, err)
		}
	}
	if high.Less(low) {
		return VersionRange{}, fmt.Errorf("protocol version range %q has min above max", text)
	}
	return VersionRange{Min: low, Max: high}, nil
}

// ExactVersionRange is the single-version window of a semver's major.minor.
func ExactVersionRange(version string) (VersionRange, error) {
	mm, err := parseMajorMinor(version)
	if err != nil {
		return VersionRange{}, err
	}
	return VersionRange{Min: mm, Max: mm}, nil
}

// Contains reports whether v lies inside the window.
func (r VersionRange) Contains(v MajorMinor) bool {
	return !v.Less(r.Min) && !r.Max.Less(v)
}

// String renders the wire form "MIN-MAX", always both ends.
func (r VersionRange) String() string {
	return r.Min.String() + "-" + r.Max.String()
}

// ProtocolWindow names one protocol's window, for [FormatProtocolVersions].
type ProtocolWindow struct {
	Name  string
	Range VersionRange
}

// FormatProtocolVersions renders "name=MIN-MAX[, name=MIN-MAX]..." in order.
func FormatProtocolVersions(windows []ProtocolWindow) string {
	parts := make([]string, 0, len(windows))
	for _, w := range windows {
		parts = append(parts, w.Name+"="+w.Range.String())
	}
	return strings.Join(parts, ", ")
}

var protocolVersionsNameRegex = regexp.MustCompile(`^[A-Za-z0-9_.:/-]+$`)

// ParseProtocolVersions parses a VGI-Protocol-Versions value. Tolerant: an
// entry that does not parse is skipped (that protocol is then unadvertised),
// whitespace around entries is ignored, and a repeated name keeps its first
// window. Empty when the value is empty or wholly unreadable.
func ParseProtocolVersions(text string) map[string]VersionRange {
	out := map[string]VersionRange{}
	for _, entry := range strings.Split(text, ",") {
		name, rng, found := strings.Cut(strings.TrimSpace(entry), "=")
		name = strings.TrimSpace(name)
		if !found || !protocolVersionsNameRegex.MatchString(name) {
			continue
		}
		if _, dup := out[name]; dup {
			continue
		}
		r, err := ParseVersionRange(rng)
		if err != nil {
			continue
		}
		out[name] = r
	}
	return out
}

// HighestCommon returns the highest major.minor inside both windows, and
// false when they are disjoint.
func HighestCommon(a, b VersionRange) (MajorMinor, bool) {
	top := a.Max
	if b.Max.Less(top) {
		top = b.Max
	}
	if top.Less(a.Min) || top.Less(b.Min) {
		return MajorMinor{}, false
	}
	return top, true
}

// ClientVersioning is what a client bound to one protocol speaks: the newest
// version (Version, canonical semver) and the window it supports, whose Max is
// Version's major.minor.
type ClientVersioning struct {
	Protocol string
	Version  string
	Range    VersionRange
	patch    int
}

// NewClientVersioning builds a client's versioning for protocol. version is
// the newest version spoken (canonical semver). versionRange widens the window
// downward ("3.4-3.5"); empty means exactly version's major.minor. Its Max
// must equal version's major.minor.
func NewClientVersioning(protocol, version, versionRange string) (*ClientVersioning, error) {
	major, minor, patch, err := parseSemver(version)
	if err != nil {
		return nil, err
	}
	top := MajorMinor{major, minor}
	rng := VersionRange{Min: top, Max: top}
	if versionRange != "" {
		if rng, err = ParseVersionRange(versionRange); err != nil {
			return nil, err
		}
		if rng.Max != top {
			return nil, fmt.Errorf(
				"protocol version range %q must end at protocol_version %s's major.minor (%s): "+
					"protocol_version is the newest version spoken, and the range widens it downward only",
				versionRange, version, top)
		}
	}
	return &ClientVersioning{Protocol: protocol, Version: version, Range: rng, patch: patch}, nil
}

// Advertised is the client's VGI-Client-Protocol-Versions / metadata value.
func (c *ClientVersioning) Advertised() string {
	return FormatProtocolVersions([]ProtocolWindow{{c.Protocol, c.Range}})
}

// WireVersion is the MAJOR.MINOR.PATCH to send for a negotiated major.minor:
// the client's own patch rides along.
func (c *ClientVersioning) WireVersion(mm MajorMinor) string {
	return mm.String() + "." + strconv.Itoa(c.patch)
}

// NoCommonVersionError reports that the client's and server's windows for a
// protocol do not overlap. Raised client-side, before sending. Its kind is
// protocol_version_unsupported, the same as a server refusal, so a caller
// handles both in one place.
type NoCommonVersionError struct {
	Protocol    string
	ClientRange VersionRange
	ServerRange VersionRange
	Endpoint    string
}

func (e *NoCommonVersionError) Error() string {
	where := ""
	if e.Endpoint != "" {
		where = " at " + e.Endpoint
	}
	p := e.Protocol
	var advice string
	if e.ServerRange.Max.Less(e.ClientRange.Min) {
		advice = fmt.Sprintf(
			"The server is too old (this client is too new for it): upgrade the worker to one supporting "+
				"%s %s or newer, or use a client that supports %s %s.",
			p, e.ClientRange.Min, p, e.ServerRange.Max)
	} else {
		advice = fmt.Sprintf(
			"The client is too old (the server is too new for it): upgrade the client to one supporting "+
				"%s %s or newer, or use a worker that supports %s %s.",
			p, e.ServerRange.Min, p, e.ClientRange.Max)
	}
	return fmt.Sprintf(
		"%s: no common %s version. This client supports %s %s; the server%s supports %s %s. %s",
		ErrorKindProtocolVersionUnsupported, p, p, e.ClientRange, where, p, e.ServerRange, advice)
}

// ErrorKind returns protocol_version_unsupported.
func (e *NoCommonVersionError) ErrorKind() string { return ErrorKindProtocolVersionUnsupported }

// ErrorCode returns FAILED_PRECONDITION.
func (e *NoCommonVersionError) ErrorCode() Code { return CodeFailedPrecondition }

// legacyServerVersionRegex reads the version a pre-spec refusal names
// ("Server: 3.4.0", "server requires 3.4.0").
var legacyServerVersionRegex = regexp.MustCompile(`(?:Server:\s*|server requires\s+)(\d+\.\d+)(?:\.\d+)?`)

// IsVersionRefusal reports whether err is a server's version refusal: this
// spec's protocol_version_unsupported or the pre-spec protocol_version_mismatch.
func IsVersionRefusal(err error) bool {
	var carrier errorKindCarrier
	if !errors.As(err, &carrier) {
		return false
	}
	kind := carrier.ErrorKind()
	return kind == ErrorKindProtocolVersionUnsupported || kind == ErrorKindProtocolVersionMismatch
}

// rangesFromRefusal returns the server windows a refusal's ErrorInfo carries.
func rangesFromRefusal(err *RpcError) (map[string]VersionRange, bool) {
	info, ok := err.ErrorInfo()
	if !ok {
		return nil, false
	}
	value, ok := info.Metadata[MetaProtocolVersions]
	if !ok {
		return nil, false
	}
	return ParseProtocolVersions(value), true
}

// legacyServerVersion returns the single server version a pre-spec refusal
// names, in its message or its precondition descriptions.
func legacyServerVersion(err *RpcError) (MajorMinor, bool) {
	kind := err.ErrorKind()
	if kind != ErrorKindProtocolVersionMismatch && kind != ErrorKindProtocolVersionUnsupported {
		return MajorMinor{}, false
	}
	texts := []string{err.Message}
	if failure, ok := err.PreconditionFailure(); ok {
		for _, v := range failure.Violations {
			texts = append(texts, v.Description)
		}
	}
	for _, text := range texts {
		if m := legacyServerVersionRegex.FindStringSubmatch(text); m != nil {
			if mm, perr := parseMajorMinor(m[1]); perr == nil {
				return mm, true
			}
		}
	}
	return MajorMinor{}, false
}

// EndpointNegotiation is the negotiation state of one endpoint (one worker
// location, connection pool or connection), shared by every protocol bound on
// it. Safe for concurrent use.
//
// The choice per protocol (spec §6):
//  1. the server advertised a window for it: the highest common version, or
//     [NoCommonVersionError] when there is none;
//  2. else a pre-spec refusal named a single server version: that one;
//  3. else the client's own newest version.
type EndpointNegotiation struct {
	endpoint   string
	mu         sync.Mutex
	advertised map[string]VersionRange
	legacy     map[string]MajorMinor
	seen       bool
}

// NewEndpointNegotiation starts empty. endpoint is used only in messages.
func NewEndpointNegotiation(endpoint string) *EndpointNegotiation {
	return &EndpointNegotiation{
		endpoint:   endpoint,
		advertised: map[string]VersionRange{},
		legacy:     map[string]MajorMinor{},
	}
}

// Endpoint returns where the server is.
func (n *EndpointNegotiation) Endpoint() string { return n.endpoint }

// Advertised returns a copy of the windows learned so far.
func (n *EndpointNegotiation) Advertised() map[string]VersionRange {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make(map[string]VersionRange, len(n.advertised))
	for k, v := range n.advertised {
		out[k] = v
	}
	return out
}

// HasAdvertisement reports whether any response so far carried one.
func (n *EndpointNegotiation) HasAdvertisement() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.seen
}

// Observe records an advertisement read from a response. A present value
// replaces what was learned: the newest response is the server's current
// statement (a redeploy behind one URL changes it).
func (n *EndpointNegotiation) Observe(value string) {
	parsed := ParseProtocolVersions(value)
	n.mu.Lock()
	n.advertised = parsed
	n.seen = true
	n.legacy = map[string]MajorMinor{}
	n.mu.Unlock()
}

// ObserveHeader records the VGI-Protocol-Versions header when the response
// carries it (present-but-empty included); absent leaves the state alone.
func (n *EndpointNegotiation) ObserveHeader(h http.Header) {
	if values, ok := h[http.CanonicalHeaderKey(ProtocolVersionsHeader)]; ok {
		value := ""
		if len(values) > 0 {
			value = values[0]
		}
		n.Observe(value)
	}
}

// ObserveMetadata records the vgi_rpc.protocol_versions key of a batch's
// custom metadata, when present.
func (n *EndpointNegotiation) ObserveMetadata(md map[string]string) {
	if value, ok := md[MetaProtocolVersions]; ok {
		n.Observe(value)
	}
}

// ObserveRefusal learns from a protocol_version_unsupported (or pre-spec
// protocol_version_mismatch) refusal of a request addressed to protocol.
func (n *EndpointNegotiation) ObserveRefusal(protocol string, err error) {
	var rpcErr *RpcError
	if !errors.As(err, &rpcErr) || !IsVersionRefusal(rpcErr) {
		return
	}
	if ranges, ok := rangesFromRefusal(rpcErr); ok && len(ranges) > 0 {
		n.mu.Lock()
		n.advertised = ranges
		n.seen = true
		n.legacy = map[string]MajorMinor{}
		n.mu.Unlock()
		return
	}
	if legacy, ok := legacyServerVersion(rpcErr); ok {
		n.mu.Lock()
		if !n.seen {
			n.legacy[protocol] = legacy
		}
		n.mu.Unlock()
	}
}

// Choose returns the major.minor to speak for client's protocol.
func (n *EndpointNegotiation) Choose(client *ClientVersioning) (MajorMinor, error) {
	n.mu.Lock()
	server, advertised := n.advertised[client.Protocol]
	legacy, hasLegacy := n.legacy[client.Protocol]
	n.mu.Unlock()
	if advertised {
		common, ok := HighestCommon(client.Range, server)
		if !ok {
			return MajorMinor{}, &NoCommonVersionError{
				Protocol: client.Protocol, ClientRange: client.Range, ServerRange: server, Endpoint: n.endpoint,
			}
		}
		return common, nil
	}
	if hasLegacy {
		if !client.Range.Contains(legacy) {
			return MajorMinor{}, &NoCommonVersionError{
				Protocol: client.Protocol, ClientRange: client.Range,
				ServerRange: VersionRange{Min: legacy, Max: legacy}, Endpoint: n.endpoint,
			}
		}
		return legacy, nil
	}
	return client.Range.Max, nil
}

// WireVersion returns the vgi_rpc.protocol_version value to send for client's
// protocol: the chosen major.minor plus the client's patch.
func (n *EndpointNegotiation) WireVersion(client *ClientVersioning) (string, error) {
	mm, err := n.Choose(client)
	if err != nil {
		return "", err
	}
	return client.WireVersion(mm), nil
}

// isRetrySafeProtocol reports whether a refused call to protocol may be re-sent
// once at a renegotiated version: reflection only, since Go clients cannot
// declare a method idempotent.
func isRetrySafeProtocol(protocol string) bool {
	return protocol == ReflectionProtocolName
}
