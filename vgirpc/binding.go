// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgirpc

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// One protocol hosted by a server, with everything dispatch needs.
//
// A server hosts one or more protocols and resolves the pair (protocol,
// method). Method names may collide across protocols -- that is what makes
// protocols independently authorable, and a port that merges them into one
// namespace is not conformant.

// protocolNameRe is the name grammar: an identifier, optionally dot-qualified,
// carrying its major version as the last component (vgi_rpc.Reflection.v1).
//
// Validated on both carriers -- at registration, and again on the routing key
// read off the wire. An unvalidated name from a request reaches error messages,
// log fields and metric labels, where arbitrary bytes do not belong.
var protocolNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.]*$`)

// reservedProtocolPrefix is reserved for protocols the framework itself
// defines. An application claiming vgi_rpc.Reflection.v1 would shadow the one
// surface a client can trust before it knows anything else about the server.
const reservedProtocolPrefix = "vgi_rpc."

// maxProtocolNameBytes bounds a name that crosses process boundaries both as
// metadata and as a URL path segment.
const maxProtocolNameBytes = 255

// ValidateProtocolName reports whether name can be a protocol's wire identity.
//
// allowReserved permits the vgi_rpc. prefix, and is set only where the
// framework registers its own protocols.
func ValidateProtocolName(name string, allowReserved bool) error {
	if name == "" {
		return fmt.Errorf("a protocol name may not be empty")
	}
	if len(name) > maxProtocolNameBytes {
		return fmt.Errorf("protocol name exceeds %d bytes; it begins %.64q", maxProtocolNameBytes, name)
	}
	if !protocolNameRe.MatchString(name) {
		return fmt.Errorf(
			"protocol name %q is not an identifier, optionally dot-qualified; expected something like 'vgi.Identity.v1'",
			name,
		)
	}
	if !allowReserved && strings.HasPrefix(name, reservedProtocolPrefix) {
		return fmt.Errorf(
			"protocol name %q claims the reserved %q prefix, which is for protocols the framework defines",
			name, reservedProtocolPrefix,
		)
	}
	return nil
}

// protocolBinding is one hosted protocol.
type protocolBinding struct {
	// Name is the wire identity -- the routing key.
	Name string
	// Methods is this protocol's method table alone.
	Methods map[string]*methodInfo
	// Version is the declared protocol_version, or "" when opted out.
	Version string
	// VersionParts is Version parsed, used by the dispatch-boundary gate.
	VersionParts [3]int
	// VersionSet is true when Version is non-empty.
	VersionSet bool
	// VersionExempt skips the version gate for this binding. Set for
	// reflection, which is what a version-mismatched client calls to learn
	// *what* mismatched -- gating it would deny the client the diagnosis it
	// came for.
	VersionExempt bool
	// Hash is this protocol's canonical fingerprint.
	Hash string
	// Impl is the object implementing this protocol's methods. A single object
	// may implement several protocols, so bindings can share one.
	Impl any
}

// ProtocolNotSpecifiedError reports a request carrying no routing key.
//
// Distinct from ProtocolNotSupportedError on purpose: the first says the caller
// did not say which protocol it meant, the second that it named one this server
// does not host, and a client acts differently on each.
type ProtocolNotSpecifiedError struct {
	Hosted []string
	// Detail replaces the generic message when the routing key is present but
	// unusable as a carrier -- a percent sign in the HTTP path segment, say.
	Detail string
}

func (e *ProtocolNotSpecifiedError) Error() string {
	if e.Detail != "" {
		return e.Detail
	}
	return fmt.Sprintf(
		"request carries no '%s' routing key; every request must name the protocol it addresses. This server hosts: %v",
		MetaProtocol, e.Hosted,
	)
}

// ErrorKind returns the stable machine-readable category.
func (e *ProtocolNotSpecifiedError) ErrorKind() string { return "protocol_not_specified" }

// ProtocolNotSupportedError reports a protocol this server does not host.
//
// Also the answer for an incompatible major version, since the major is part of
// the name: a routing answer every proxy, WAF and load balancer understands
// without an Arrow parser.
type ProtocolNotSupportedError struct {
	Requested string
	Hosted    []string
	Detail    string
}

func (e *ProtocolNotSupportedError) Error() string {
	if e.Detail != "" {
		return e.Detail
	}
	return fmt.Sprintf("this server does not host protocol %q. Hosted: %v", e.Requested, e.Hosted)
}

// ErrorKind returns the stable machine-readable category.
func (e *ProtocolNotSupportedError) ErrorKind() string { return "protocol_not_supported" }

// bindings returns every protocol this server hosts, primary first.
//
// The primary is projected from the server's own method table rather than
// stored, so the existing registration paths -- which all write s.methods --
// keep working untouched.
func (s *Server) bindings() map[string]*protocolBinding {
	out := make(map[string]*protocolBinding, 1+len(s.extraBindings))
	out[s.primaryProtocolName()] = &protocolBinding{
		Name:         s.primaryProtocolName(),
		Methods:      s.methods,
		Version:      s.protocolVersion,
		VersionParts: s.protocolVersionParts,
		VersionSet:   s.protocolVersionSet,
		// The canonical hash, not the legacy byte-based one: ProtocolHash()
		// still digests serialized IPC, which is not stable across Arrow
		// implementations and so cannot be compared with another port.
		Hash: s.canonicalHash(),
		Impl: s.implementation,
	}
	for name, b := range s.extraBindings {
		out[name] = b
	}
	return out
}

// dispatchLabel returns the protocol name and canonical hash an access record
// must carry for a dispatch that resolved to b.
//
// docs/access-log-spec.md §3: protocol is "the wire name of the protocol that
// owns the dispatched method … not a server-wide default", and protocol_hash is
// that protocol's canonical digest -- "the registry key when decoding archived
// records". Both sides of that pair have to come from the same place or a
// record names one protocol and carries another's digest, which is worse than
// either field being wrong alone: it is well-formed, it passes the schema, and
// it decodes against the wrong description with nothing about it looking wrong.
//
// Returning both together is deliberate. Every emit site that fills one fills
// the other, and a site that reads the name from the binding while reaching for
// the server's hash is exactly the bug this exists to prevent.
//
// A nil binding means a framework endpoint owned by no protocol
// (__transport_options__, __upload_url__) -- or a method that never resolved.
// Those log the server's primary, which the spec prescribes rather than
// tolerates.
//
// The hash is the canonical one, not the legacy byte-based digest the retired
// __describe__ payload carried: the canonical hash is what compares across
// ports, and what the access-log conformance validator asserts.
func (s *Server) dispatchLabel(b *protocolBinding) (protocol, hash string) {
	if b == nil {
		return s.primaryProtocolName(), s.canonicalHash()
	}
	return b.Name, b.Hash
}

// primaryProtocolName is the wire name of the application protocol.
func (s *Server) primaryProtocolName() string {
	if s.serviceName != "" {
		return s.serviceName
	}
	return "Service"
}

// AddProtocol hosts an additional protocol alongside the primary.
//
// allowReserved is for the framework's own protocols only; an application
// passing true would be able to shadow reflection.
func (s *Server) AddProtocol(b *protocolBinding, allowReserved bool) error {
	if err := ValidateProtocolName(b.Name, allowReserved); err != nil {
		return err
	}
	if b.Name == s.primaryProtocolName() {
		return fmt.Errorf(
			"protocol %q is already hosted as the primary; the name is the routing key, so it must be unique",
			b.Name,
		)
	}
	if s.extraBindings == nil {
		s.extraBindings = make(map[string]*protocolBinding)
	}
	if existing, ok := s.extraBindings[b.Name]; ok {
		return fmt.Errorf(
			"two protocols are hosted under the same name %q (%v and %v); the name is the routing key, so it must be unique",
			b.Name, existing.Name, b.Name,
		)
	}
	s.extraBindings[b.Name] = b
	return nil
}

// resolve maps one request's (protocol, method) pair to a method.
//
// The routing key is required here, including against a server hosting exactly
// one protocol: an exemption would let an intermediary that rebuilds a request
// and drops the field land silently on whichever protocol happened to be first,
// rather than being told.
//
// "Here" is load-bearing. On the raw transports -- stdio, unix, named pipes,
// TCP -- vgi_rpc.protocol is the ONLY carrier, so absent really is unroutable
// and this refusal is the whole of the rule. HTTP reaches this function with
// the protocol read off the PATH, which is never empty once the route matched,
// and the metadata field there is a second carrier whose absence is accepted:
// see HttpServer.checkProtocolCarriage and IDENTITY_V1_SPEC.md §5c. Both halves
// are pinned by the shared conformance suite, so neither can drift into the
// other.
//
// The three failures are deliberately distinct, and a client depends on the
// difference -- particularly the last, which is the documented capability-probe
// signal: a client testing for an optional method must be able to tell "you do
// not speak this protocol" from "you speak it but lack this method".
func (s *Server) resolve(protocol, method string) (*methodInfo, *protocolBinding, error) {
	all := s.bindings()
	if protocol == "" {
		return nil, nil, &ProtocolNotSpecifiedError{Hosted: sortedKeys(all)}
	}
	// Checked before the lookup so an arbitrary request-supplied string never
	// reaches an error message, a log field or a metric label.
	if err := ValidateProtocolName(protocol, true); err != nil {
		return nil, nil, &ProtocolNotSupportedError{
			Requested: protocol,
			Hosted:    sortedKeys(all),
			Detail:    fmt.Sprintf("'%s' is not a protocol name: %v", MetaProtocol, err),
		}
	}
	binding, ok := all[protocol]
	if !ok {
		return nil, nil, &ProtocolNotSupportedError{Requested: protocol, Hosted: sortedKeys(all)}
	}
	info, ok := binding.Methods[method]
	if !ok {
		return nil, nil, &MethodNotImplementedError{
			Method:  method,
			Message: fmt.Sprintf("protocol %q has no method %q. Available: %v", protocol, method, sortedKeys(binding.Methods)),
		}
	}
	return info, binding, nil
}

// sortedKeys returns a map's keys in sorted order, so two runs of the same
// server produce the same error text.
func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// canonicalHash returns the primary protocol's canonical fingerprint.
//
// Computed once and cached: it is read on every reflection call, and the
// method table does not change after registration.
func (s *Server) canonicalHash() string {
	s.canonicalHashOnce.Do(func() {
		h, err := bindingHash(s.primaryProtocolName(), s.methods)
		if err != nil {
			// A type with no canonical token. Leaving the hash empty is the
			// honest answer -- a wrong digest would have two ports silently
			// disagree about agreeing.
			s.canonicalHashValue = ""
			return
		}
		s.canonicalHashValue = h
	})
	return s.canonicalHashValue
}
