// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgirpc

import (
	"context"
	"fmt"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
)

// vgi_rpc.Reflection.v1 -- discovery as an ordinary co-hosted protocol.
//
// Introspection used to be a hardcoded method name, __describe__, answered from
// a pre-built batch before dispatch. That made it a thing every port had to
// hand-implement, in a bespoke format, outside the machinery that serves every
// other method -- which is how the ports drifted. Here it is a protocol like any
// other: its methods are registered normally and its payload is an ordinary
// generated schema.
//
// Following gRPC's reflection service and D-Bus's org.freedesktop.DBus, it is
// co-hosted rather than special-cased. Its own major version sits in its name,
// so an incompatible reflection is a routing failure a client can act on rather
// than a mis-parse.
//
// Exempt from the protocol_version gate: this is the protocol a
// version-mismatched client calls to learn *what* mismatched, and gating it
// would deny the client the diagnosis it came for.
//
// Minor skew must be survivable, which means a decoder reads by field name,
// ignores columns it does not know, and defaults columns that are absent --
// and errors on an absent field that has no default, since zero-filling a
// required field hands a client a description that is wrong rather than absent.
// One rule follows and binds every port: a field added in a minor version must
// carry a default.

// ReflectionProtocolName is the wire name of the reflection protocol.
//
// Fixed, and the one protocol name a client may know a priori: it is the
// bootstrap, so there is nothing to discover it with.
const ReflectionProtocolName = "vgi_rpc.Reflection.v1"

// retiredDescribeMethod is the method introspection used to be.
//
// The name survives the handler by exactly one use: saying where introspection
// went. A stale client told only "no such method" cannot tell "retired" from
// "this server was built without introspection", and the two need opposite
// fixes -- one is a client to update, the other a server to reconfigure.
const retiredDescribeMethod = "__describe__"

// retiredDescribeError refuses __describe__ by naming its replacement.
//
// Only __describe__ is special-cased. Every other reserved name keeps the plain
// capability answer, which is what a client probing for an optional method
// needs: "this server does not have it" is the whole content of that question,
// and a redirection would be noise.
//
// Both transports build the refusal here so a caller cannot get two different
// stories about where introspection went depending on how it connected.
func retiredDescribeError() *MethodNotImplementedError {
	return &MethodNotImplementedError{
		Method: retiredDescribeMethod,
		Message: "'" + retiredDescribeMethod + "' was retired. Introspection is now the '" +
			ReflectionProtocolName + "' protocol: call 'list_protocols' for what this server hosts, " +
			"then 'describe' for one protocol's methods.",
	}
}

// IdempotencyLevels are the values MethodInfo.Idempotency may take, borrowed
// from gRPC's idempotency_level.
//
// With an HTTP transport and a policy proxy in the path, retries will happen;
// without this nothing on the wire says what is safe to retry.
//
//   - "unknown"         -- the default; a caller must assume the worst.
//   - "no_side_effects" -- a read; safe to retry and safe to issue twice.
//   - "idempotent"      -- has side effects, but repeating it is equivalent to
//     performing it once.
var IdempotencyLevels = []string{"unknown", "no_side_effects", "idempotent"}

// StreamKinds are the values MethodInfo.StreamKind may take for a stream.
//
// Whether a stream is an exchange is decided by the implementation, not by the
// protocol, so a server describing its own surface often cannot say --
// "unknown" is the honest answer and is spelled rather than left null.
var StreamKinds = []string{"unknown", "producer", "exchange"}

// MethodInfoDesc is one method's wire surface.
//
// Schemas travel as serialized Arrow IPC rather than as a structural
// description: a client's whole purpose in asking is to get a schema it can
// hand to its own Arrow implementation, and IPC is the one representation every
// port already reads. The *hash* is what compares across ports, and it is taken
// over the decoded structure precisely so these bytes need not match.
type MethodInfoDesc struct {
	Name       string `vgirpc:"name"`
	MethodType string `vgirpc:"method_type"`
	HasReturn  bool   `vgirpc:"has_return"`
	HasHeader  bool   `vgirpc:"has_header"`
	// StreamKind is "" for unary; otherwise one of [StreamKinds].
	StreamKind string `vgirpc:"stream_kind"`
	// ParamsSchemaIPC and friends are empty rather than null when absent: a
	// nullable column costs every port a null check on a value it will only
	// ever treat as absent.
	ParamsSchemaIPC    []byte `vgirpc:"params_schema_ipc"`
	ResultSchemaIPC    []byte `vgirpc:"result_schema_ipc"`
	HeaderSchemaIPC    []byte `vgirpc:"header_schema_ipc"`
	Idempotency        string `vgirpc:"idempotency"`
	Deprecated         bool   `vgirpc:"deprecated"`
	DeprecationMessage string `vgirpc:"deprecation_message"`
}

// ProtocolSummaryDesc is one hosted protocol without its methods.
//
// Enough to decide whether to fetch the full description: a client that already
// knows a hash can skip the round trip entirely.
type ProtocolSummaryDesc struct {
	Protocol           string   `vgirpc:"protocol"`
	ProtocolVersion    string   `vgirpc:"protocol_version"`
	ProtocolHash       string   `vgirpc:"protocol_hash"`
	Deprecated         bool     `vgirpc:"deprecated"`
	DeprecationMessage string   `vgirpc:"deprecation_message"`
	Features           []string `vgirpc:"features"`
}

// ServiceDescriptionDesc is one protocol's full description.
//
// Carries no server identity: two processes serving the same protocol must
// describe it identically, or the description is not a property of the
// protocol. Server identity lives on [ProtocolListDesc], which is a statement
// about a server.
type ServiceDescriptionDesc struct {
	Protocol           string           `vgirpc:"protocol"`
	ProtocolVersion    string           `vgirpc:"protocol_version"`
	ProtocolHash       string           `vgirpc:"protocol_hash"`
	Deprecated         bool             `vgirpc:"deprecated"`
	DeprecationMessage string           `vgirpc:"deprecation_message"`
	Features           []string         `vgirpc:"features"`
	Methods            []MethodInfoDesc `vgirpc:"methods"`
}

// ProtocolListDesc is what this server hosts.
type ProtocolListDesc struct {
	ServerID       string                `vgirpc:"server_id"`
	ServerVersion  string                `vgirpc:"server_version"`
	RequestVersion string                `vgirpc:"request_version"`
	Protocols      []ProtocolSummaryDesc `vgirpc:"protocols"`
}

// describeParams is the single parameter of the describe method.
type describeParams struct {
	Protocol string `vgirpc:"protocol"`
}

// listProtocolsParams is empty: list_protocols takes nothing.
type listProtocolsParams struct{}

// RegisterReflection hosts vgi_rpc.Reflection.v1 on s.
//
// Registered after the application protocols so it appears in its own output
// without being special-cased, and so the primary stays the application
// protocol -- which is what the single-protocol accessors report.
func RegisterReflection(s *Server) error {
	sub := NewServer()
	sub.SetServiceName(ReflectionProtocolName)

	Unary(sub, "list_protocols", func(_ context.Context, _ *CallContext, _ listProtocolsParams) (ProtocolListDesc, error) {
		return s.listProtocols(), nil
	})
	Unary(sub, "describe", func(_ context.Context, _ *CallContext, p describeParams) (ServiceDescriptionDesc, error) {
		return s.describeProtocol(p.Protocol)
	})

	binding := &protocolBinding{
		Name:    ReflectionProtocolName,
		Methods: sub.methods,
		// Exempt: this is what a version-mismatched client calls to learn what
		// mismatched.
		VersionExempt: true,
		Impl:          sub,
	}
	hash, err := bindingHash(ReflectionProtocolName, sub.methods)
	if err != nil {
		return err
	}
	binding.Hash = hash
	return s.AddProtocol(binding, true)
}

// listProtocols answers the cheap question: what is here, and has it changed.
func (s *Server) listProtocols() ProtocolListDesc {
	all := s.bindings()
	names := sortedKeys(all)
	out := ProtocolListDesc{
		ServerID:       s.serverID,
		RequestVersion: ProtocolVersion,
		Protocols:      make([]ProtocolSummaryDesc, 0, len(names)),
	}
	for _, name := range names {
		b := all[name]
		out.Protocols = append(out.Protocols, ProtocolSummaryDesc{
			Protocol:        b.Name,
			ProtocolVersion: b.Version,
			ProtocolHash:    b.Hash,
			Features:        []string{},
		})
	}
	return out
}

// describeProtocol answers the expensive question, asked once.
func (s *Server) describeProtocol(protocol string) (ServiceDescriptionDesc, error) {
	all := s.bindings()
	b, ok := all[protocol]
	if !ok {
		return ServiceDescriptionDesc{}, &ProtocolNotSupportedError{
			Requested: protocol,
			Hosted:    sortedKeys(all),
		}
	}
	names := sortedKeys(b.Methods)
	desc := ServiceDescriptionDesc{
		Protocol:        b.Name,
		ProtocolVersion: b.Version,
		ProtocolHash:    b.Hash,
		Features:        []string{},
		Methods:         make([]MethodInfoDesc, 0, len(names)),
	}
	// Sorted so two ports iterating differently-ordered maps still agree.
	sort.Strings(names)
	for _, name := range names {
		info := b.Methods[name]
		desc.Methods = append(desc.Methods, MethodInfoDesc{
			Name:       info.Name,
			MethodType: methodTypeString(info.Type),
			// Only a unary method returns a value. A stream's ResultSchema is
			// the (empty) Protocol-level return, not a result the caller
			// receives, so reporting has_return for one describes a shape the
			// client will never see.
			HasReturn:          unaryHasReturn(info),
			HasHeader:          info.HasHeader,
			StreamKind:         streamKindFor(info),
			ParamsSchemaIPC:    schemaIPC(info.ParamsSchema),
			ResultSchemaIPC:    schemaIPC(info.ResultSchema),
			HeaderSchemaIPC:    schemaIPC(info.HeaderSchema),
			Idempotency:        "unknown",
			Deprecated:         false,
			DeprecationMessage: "",
		})
	}
	return desc, nil
}

// unaryHasReturn reports whether a method returns a value to its caller.
//
// A stream's ResultSchema is the (empty) Protocol-level return, not something
// the caller receives, and a void unary method carries a zero-field schema
// rather than a nil one -- so neither "ResultSchema != nil" nor the method type
// alone is the question being asked.
func unaryHasReturn(info *methodInfo) bool {
	return info.Type == MethodUnary && info.ResultSchema != nil && info.ResultSchema.NumFields() > 0
}

// schemaIPC serializes a schema, or returns empty bytes when there is none.
//
// Empty rather than null: a nullable column costs every port a null check on a
// value it will only ever treat as absent.
func schemaIPC(schema *arrow.Schema) []byte {
	if schema == nil {
		return []byte{}
	}
	return serializeSchema(schema)
}

// streamKindFor returns the stream kind, or "" for a unary method.
func streamKindFor(info *methodInfo) string {
	switch info.Type {
	case MethodUnary:
		return ""
	case MethodProducer:
		return "producer"
	case MethodExchange:
		return "exchange"
	default:
		// MethodDynamic: the state type is decided at runtime, so the kind is
		// only knowable if the registration stated it -- which it can, because a
		// dynamic *schema* is a separate question from producer-vs-exchange.
		if info.DeclaredStreamKind != "" {
			return info.DeclaredStreamKind
		}
		return "unknown"
	}
}

// hashMethodsOf projects a binding's method table into the hash inputs.
//
// Split out of [bindingHash] so a test can hand the same projection to
// [CanonicalDescription] and diff the preimage: a hash mismatch between ports is
// otherwise one bit of information.
func hashMethodsOf(methods map[string]*methodInfo) []HashMethod {
	hm := make([]HashMethod, 0, len(methods))
	for _, info := range methods {
		hm = append(hm, HashMethod{
			Name:         info.Name,
			MethodType:   methodTypeString(info.Type),
			HasReturn:    unaryHasReturn(info),
			HasHeader:    info.HasHeader,
			ParamsSchema: info.ParamsSchema,
			ResultSchema: info.ResultSchema,
			HeaderSchema: info.HeaderSchema,
		})
	}
	return hm
}

// bindingHash computes one binding's canonical fingerprint.
func bindingHash(name string, methods map[string]*methodInfo) (string, error) {
	h, err := ComputeProtocolHash(name, hashMethodsOf(methods))
	if err != nil {
		return "", fmt.Errorf("computing protocol hash for %q: %w", name, err)
	}
	return h, nil
}
