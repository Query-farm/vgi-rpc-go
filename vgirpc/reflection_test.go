package vgirpc

import (
	"context"
	"testing"
)

type reflEcho struct {
	Value string `vgi:"value"`
}

func newReflServer(t *testing.T) *Server {
	t.Helper()
	s := NewServer()
	s.SetServiceName("demo.App.v1")
	s.SetProtocolVersion("2.1.0")
	Unary(s, "echo", func(_ context.Context, _ *CallContext, p reflEcho) (string, error) {
		return p.Value, nil
	})
	if err := RegisterReflection(s); err != nil {
		t.Fatalf("RegisterReflection: %v", err)
	}
	return s
}

func TestReflectionIsRegisteredAsAProtocol(t *testing.T) {
	// It routes on (protocol, method) like everything else.
	s := newReflServer(t)
	if _, ok := s.bindings()[ReflectionProtocolName]; !ok {
		t.Fatalf("reflection not hosted; have %v", sortedKeys(s.bindings()))
	}
}

func TestReflectionIsVersionExempt(t *testing.T) {
	// This is what a mismatched client calls to learn what mismatched; gating
	// it would deny the client the diagnosis it came for.
	s := newReflServer(t)
	if !s.bindings()[ReflectionProtocolName].VersionExempt {
		t.Error("reflection must be exempt from the version gate")
	}
}

func TestPrimaryIsStillTheApplicationProtocol(t *testing.T) {
	// Reflection registers after the application protocols, so the accessors
	// that report "the" protocol must not start saying "Reflection".
	if got := newReflServer(t).primaryProtocolName(); got != "demo.App.v1" {
		t.Errorf("primary = %q, want demo.App.v1", got)
	}
}

func TestListProtocolsIncludesItself(t *testing.T) {
	// Self-description is not special-cased: a client discovers reflection the
	// same way it discovers everything else.
	got := map[string]bool{}
	for _, p := range newReflServer(t).listProtocols().Protocols {
		got[p.Protocol] = true
	}
	for _, want := range []string{"demo.App.v1", ReflectionProtocolName} {
		if !got[want] {
			t.Errorf("list_protocols omitted %q", want)
		}
	}
}

func TestDescribeReportsVersionAndHash(t *testing.T) {
	desc, err := newReflServer(t).describeProtocol("demo.App.v1")
	if err != nil {
		t.Fatal(err)
	}
	if desc.ProtocolVersion != "2.1.0" {
		t.Errorf("version = %q", desc.ProtocolVersion)
	}
	if len(desc.ProtocolHash) != 64 {
		t.Errorf("hash = %q, want 64 hex chars", desc.ProtocolHash)
	}
	if len(desc.Methods) != 1 || desc.Methods[0].Name != "echo" {
		t.Errorf("methods = %+v", desc.Methods)
	}
	if desc.Methods[0].StreamKind != "" {
		t.Errorf("a unary method must have an empty stream_kind, got %q", desc.Methods[0].StreamKind)
	}
	if desc.Methods[0].Idempotency != "unknown" {
		t.Errorf("idempotency must default to unknown, got %q", desc.Methods[0].Idempotency)
	}
}

func TestDescribeDescribesItself(t *testing.T) {
	desc, err := newReflServer(t).describeProtocol(ReflectionProtocolName)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, m := range desc.Methods {
		names[m.Name] = true
	}
	if !names["describe"] || !names["list_protocols"] {
		t.Errorf("reflection must describe itself, got %v", names)
	}
}

func TestDescribeUnknownProtocolIsNotSupported(t *testing.T) {
	// Named, not silently empty: an empty description reads as "no methods".
	if _, err := newReflServer(t).describeProtocol("demo.Nope.v1"); err == nil {
		t.Fatal("expected ProtocolNotSupportedError")
	} else if _, ok := err.(*ProtocolNotSupportedError); !ok {
		t.Errorf("got %T, want *ProtocolNotSupportedError", err)
	}
}

func TestDescriptionCarriesNoServerIdentity(t *testing.T) {
	// Two processes serving one protocol must describe it identically, or the
	// description is not a property of the protocol.
	s := newReflServer(t)
	s.SetServerID("abc123")
	desc, err := s.describeProtocol("demo.App.v1")
	if err != nil {
		t.Fatal(err)
	}
	if desc.ProtocolHash == "" {
		t.Fatal("no hash")
	}
	// Server identity belongs on list_protocols, which is a statement about a
	// server rather than about a protocol.
	if s.listProtocols().ServerID != "abc123" {
		t.Error("list_protocols must carry the server id")
	}
}

func TestApplicationCannotClaimTheReservedPrefix(t *testing.T) {
	// An application claiming vgi_rpc.Reflection.v1 could shadow the one
	// surface a client trusts before it knows anything else about the server.
	if err := ValidateProtocolName("vgi_rpc.Reflection.v1", false); err == nil {
		t.Error("expected the reserved prefix to be refused for applications")
	}
	if err := ValidateProtocolName("app.vgi_rpc.v1", false); err != nil {
		t.Errorf("the guard is on the prefix, not a substring: %v", err)
	}
}
