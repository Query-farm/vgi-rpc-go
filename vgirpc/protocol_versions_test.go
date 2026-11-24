// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func TestParseProtocolVersionsIsTolerant(t *testing.T) {
	got := ParseProtocolVersions(" vgi.v3=3.4-3.4 ,vgi.secret.v1=1.0, bad, x=1.0-0.1, =1.0, vgi.v3=9.9-9.9, y=01.0-1.0, z=3.4.2-3.5.0")
	want := map[string]string{"vgi.v3": "3.4-3.4", "vgi.secret.v1": "1.0-1.0", "z": "3.4-3.5"}
	if len(got) != len(want) {
		t.Fatalf("parsed %v, want %v", got, want)
	}
	for name, rng := range want {
		if got[name].String() != rng {
			t.Fatalf("%s = %s, want %s", name, got[name], rng)
		}
	}
	if len(ParseProtocolVersions("")) != 0 {
		t.Fatal("empty value parsed entries")
	}
	if FormatProtocolVersions([]ProtocolWindow{
		{"vgi.v3", VersionRange{MajorMinor{3, 4}, MajorMinor{3, 4}}},
		{"vgi.secret.v1", VersionRange{MajorMinor{1, 0}, MajorMinor{1, 0}}},
	}) != "vgi.v3=3.4-3.4, vgi.secret.v1=1.0-1.0" {
		t.Fatal("format")
	}
}

func TestHighestCommon(t *testing.T) {
	r := func(s string) VersionRange {
		v, err := ParseVersionRange(s)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	if mm, ok := HighestCommon(r("3.4-3.5"), r("3.4-3.4")); !ok || mm != (MajorMinor{3, 4}) {
		t.Fatalf("got %v %v", mm, ok)
	}
	if _, ok := HighestCommon(r("3.5-3.6"), r("3.4-3.4")); ok {
		t.Fatal("disjoint windows overlapped")
	}
}

func TestGateMessages(t *testing.T) {
	window := VersionRange{MajorMinor{3, 4}, MajorMinor{3, 4}}
	adv := "vgi.v3=3.4-3.4"
	cases := []struct {
		version, clientVersions string
		present                 bool
		want                    string
	}{
		{"3.6.0", "vgi.v3=3.5-3.6", true, "protocol_version_unsupported: client speaks vgi.v3 3.6 (supports 3.5-3.6); " +
			"this server supports vgi.v3 3.4-3.4. The server is too old (the client is too new for it). Upgrade the " +
			"worker, or use a client that supports vgi.v3 3.4."},
		{"3.3.1", "", true, "protocol_version_unsupported: client speaks vgi.v3 3.3; this server supports vgi.v3 " +
			"3.4-3.4. The client is too old (this server is too new for it). Upgrade the client, or use a worker that " +
			"supports vgi.v3 3.3."},
		{"", "", false, "protocol_version_unsupported: the client did not send vgi_rpc.protocol_version for vgi.v3; " +
			"this server supports vgi.v3 3.4-3.4. This is either a vgi-rpc framework bug or a non-VGI client " +
			"connecting to a VGI worker."},
		{"3.4", "", true, "protocol_version_unsupported: the client sent a malformed protocol_version '3.4' for " +
			"vgi.v3 (expected canonical semver MAJOR.MINOR.PATCH); this server supports vgi.v3 3.4-3.4."},
	}
	for _, c := range cases {
		err := gateVersion("vgi.v3", window, c.version, c.present, c.clientVersions, adv)
		if err == nil || err.Message != c.want {
			t.Fatalf("gate(%q) =\n%v\nwant\n%s", c.version, err, c.want)
		}
		if err.ErrorKind() != ErrorKindProtocolVersionUnsupported || err.ErrorMetadata()[MetaProtocolVersions] != adv {
			t.Fatalf("kind/metadata: %v %v", err.ErrorKind(), err.ErrorMetadata())
		}
	}
	if err := gateVersion("vgi.v3", window, "3.4.9", true, "!!", adv); err != nil {
		t.Fatalf("in-window version refused: %v", err)
	}
}

func newNegotiatedServer(t *testing.T, version, override string) *Server {
	t.Helper()
	s := NewServer()
	s.SetServiceName("conformance.Negotiated.v1")
	s.SetProtocolVersion(version)
	Unary(s, "ping", func(_ context.Context, call *CallContext, _ struct{}) (string, error) {
		return call.TransportMetadata[MetaProtocolVersion], nil
	})
	unversioned := NewProtocol("demo.Unversioned.v1")
	Unary(unversioned, "ping", func(context.Context, *CallContext, struct{}) (string, error) { return "", nil })
	if err := s.AddProtocol(unversioned); err != nil {
		t.Fatal(err)
	}
	if err := RegisterReflection(s); err != nil {
		t.Fatal(err)
	}
	if override != "" {
		if err := s.SetProtocolVersionRanges(map[string]string{"conformance.Negotiated.v1": override}); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func TestAdvertisementListsOnlyGatedProtocols(t *testing.T) {
	s := newNegotiatedServer(t, "3.4.0", "")
	if got := s.ProtocolVersions(); got != "conformance.Negotiated.v1=3.4-3.4" {
		t.Fatalf("advertisement %q", got)
	}
	if err := s.SetProtocolVersionRanges(map[string]string{ReflectionProtocolName: "1.0-1.0"}); err == nil {
		t.Fatal("overriding reflection was accepted")
	}
	if err := s.SetProtocolVersionRanges(map[string]string{"conformance.Negotiated.v1": "3.3-3.3"}); err != nil {
		t.Fatal(err)
	}
	if got := s.ProtocolVersions(); got != "conformance.Negotiated.v1=3.3-3.3" {
		t.Fatalf("overridden advertisement %q", got)
	}
	empty := NewServer()
	empty.SetServiceName("demo.Plain.v1")
	if empty.ProtocolVersions() != "" {
		t.Fatal("an ungated server advertised something")
	}
}

func TestDeclaredRangeMustEndAtTheVersion(t *testing.T) {
	s := NewServer()
	s.SetProtocolVersion("3.4.0")
	s.SetProtocolVersionRange("3.3-3.4")
	defer func() {
		if recover() == nil {
			t.Fatal("a range not ending at the declared version was accepted")
		}
	}()
	s.SetProtocolVersionRange("3.3-3.5")
}

func TestHeaderOnEveryHTTPResponse(t *testing.T) {
	h := NewHttpServer(newNegotiatedServer(t, "3.4.0", ""))
	h.SetCorsOrigins("*")
	for _, req := range []*http.Request{
		httptest.NewRequest(http.MethodHead, "/health", nil),
		httptest.NewRequest(http.MethodOptions, "/health", nil),
		httptest.NewRequest(http.MethodPost, "/nope.Unhosted.v1/x", strings.NewReader("junk")),
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if got := rec.Header().Values(ProtocolVersionsHeader); len(got) != 1 || got[0] != "conformance.Negotiated.v1=3.4-3.4" {
			t.Fatalf("%s %s (%d): header %v", req.Method, req.URL.Path, rec.Code, got)
		}
		if !strings.Contains(rec.Header().Get("Access-Control-Expose-Headers"), ProtocolVersionsHeader) {
			t.Fatalf("%s %s: not exposed", req.Method, req.URL.Path)
		}
	}
	if !strings.Contains(defaultCorsAllowHeaders, ClientProtocolVersionsHeader) {
		t.Fatal("preflight default does not allow the client header")
	}
}

// pingCounter records every protocol_version a test server's ping receives.
type pingCounter struct {
	mu       sync.Mutex
	received []string
}

func (p *pingCounter) wrap(t *testing.T, s *Server) http.Handler {
	t.Helper()
	h := NewHttpServer(s)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/conformance.Negotiated.v1/ping") {
			p.mu.Lock()
			p.received = append(p.received, "?")
			p.mu.Unlock()
		}
		h.ServeHTTP(w, r)
	})
}

func pingOnce(t *testing.T, c *HttpClient) (string, error) {
	t.Helper()
	params := emptyBatch(arrow.NewSchema(nil, nil))
	defer params.Release()
	res, err := c.CallUnary(context.Background(), "ping", params, nil)
	if err != nil {
		return "", err
	}
	defer res.Release()
	return res.Batch.Column(0).(*array.String).Value(0), nil
}

func TestHTTPClientNegotiates(t *testing.T) {
	var counter pingCounter
	srv := httptest.NewServer(counter.wrap(t, newNegotiatedServer(t, "3.4.0", "")))
	defer srv.Close()
	c, err := NewHttpClient(srv.URL, WithClientProtocol("conformance.Negotiated.v1"),
		WithClientProtocolVersion("3.5.2"), WithClientProtocolVersionRange("3.4-3.5"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = pingOnce(t, c)
	if !IsVersionRefusal(err) {
		t.Fatalf("first call (3.5.2 against 3.4-3.4) = %v, want a refusal", err)
	}
	got, err := pingOnce(t, c)
	if err != nil || got != "3.4.2" {
		t.Fatalf("second call = %q, %v; want 3.4.2", got, err)
	}

	// Disjoint: fail without sending once known.
	d, err := NewHttpClient(srv.URL, WithClientProtocol("conformance.Negotiated.v1"),
		WithClientProtocolVersion("3.6.0"), WithClientProtocolVersionRange("3.5-3.6"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err := pingOnce(t, d); !IsVersionRefusal(err) {
		t.Fatalf("disjoint first call = %v", err)
	}
	before := len(counter.received)
	_, err = pingOnce(t, d)
	var noCommon *NoCommonVersionError
	if !errors.As(err, &noCommon) || !strings.Contains(err.Error(), "3.4-3.4") || !strings.Contains(err.Error(), "3.5-3.6") {
		t.Fatalf("disjoint second call = %v", err)
	}
	if len(counter.received) != before {
		t.Fatal("the client sent after learning there is no common version")
	}
}

func TestLegacyRefusalIsParsed(t *testing.T) {
	n := NewEndpointNegotiation("")
	v, err := NewClientVersioning("p.v1", "3.5.2", "3.4-3.5")
	if err != nil {
		t.Fatal(err)
	}
	n.ObserveRefusal("p.v1", &RpcError{Kind: ErrorKindProtocolVersionMismatch,
		Message: "VGI client/worker protocol_version mismatch for protocol 'p.v1'.\n  Client: 3.5.2\n  Server: 3.4.0\n"})
	if wire, err := n.WireVersion(v); err != nil || wire != "3.4.2" {
		t.Fatalf("after a legacy refusal: %q %v", wire, err)
	}
}

func TestByteStreamReflectionAdvertisesAndWideClientProbes(t *testing.T) {
	server := newNegotiatedServer(t, "3.4.0", "")
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	go server.serveTcpConn(context.Background(), serverConn)
	client := newTcpClientFromConn(clientConn, tcpClientConfig{
		protocol:        "conformance.Negotiated.v1",
		protocolVersion: "3.5.1",
		protocolRange:   "3.4-3.5",
		maxRequest:      defaultClientMaxRequestBytes,
		maxResponse:     defaultClientMaxDecodedResponseBytes,
	})
	defer client.Close()
	params := emptyBatch(arrow.NewSchema(nil, nil))
	defer params.Release()
	res, err := client.CallUnary(context.Background(), "ping", params, nil)
	if err != nil {
		t.Fatalf("probing client was refused: %v", err)
	}
	defer res.Release()
	if got := res.Batch.Column(0).(*array.String).Value(0); got != "3.4.1" {
		t.Fatalf("probing client spoke %q, want 3.4.1", got)
	}
	if adv := client.Negotiation().Advertised(); adv["conformance.Negotiated.v1"].String() != "3.4-3.4" {
		t.Fatalf("learned %v", adv)
	}
}

func TestOverrideRefusesTheDeclaredVersion(t *testing.T) {
	s := newNegotiatedServer(t, "3.4.0", "3.3-3.3")
	b := s.bindings()["conformance.Negotiated.v1"]
	err := s.gate(b, map[string]string{MetaProtocolVersion: "3.4.0"})
	if err == nil || !strings.Contains(err.Message, "this server supports conformance.Negotiated.v1 3.3-3.3") {
		t.Fatalf("declared version against a 3.3-3.3 override: %v", err)
	}
	if err := s.gate(b, map[string]string{MetaProtocolVersion: "3.3.7"}); err != nil {
		t.Fatalf("3.3.7 against 3.3-3.3: %v", err)
	}
}
