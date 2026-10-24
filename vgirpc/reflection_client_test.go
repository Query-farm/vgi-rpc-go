// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc_test

// ListProtocols / DescribeProtocol: reflection over a held connection.
//
// Every client this port hands out -- HTTP, TCP, Unix, raw Iroh and
// HTTP-over-Iroh -- against two servers: this port's own conformance worker
// and the Python reference conformance worker (set VGI_RPC_PYTHON to an
// interpreter with vgi_rpc installed; those cases skip without it). Both host
// ConformanceService, then conformance.Secondary.v1, then reflection.
//
// Each client dials through a counting forwarder, so "the held connection is
// reused" is an observable fact -- exactly one connection accepted per client
// -- rather than an inference from the calls succeeding.

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Query-farm/vgi-rpc-go/conformance"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

const (
	reflPrimary        = "ConformanceService"
	reflSecondary      = conformance.SecondaryProtocolName
	reflPrimaryVersion = "2.0.0"
	reflIrohID         = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

var reflHex64 = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ---------------------------------------------------------------------------
// Counting forwarder
// ---------------------------------------------------------------------------

// countingForwarder accepts on its own listener, counts every accepted
// connection, and splices each one to the upstream server.
type countingForwarder struct {
	listener net.Listener
	accepted atomic.Int64
}

func newCountingForwarder(t *testing.T, network, upstreamNetwork, upstreamAddr string) *countingForwarder {
	t.Helper()
	var listener net.Listener
	var err error
	if network == "unix" {
		dir, dirErr := os.MkdirTemp("", "vgirefl")
		if dirErr != nil {
			t.Fatal(dirErr)
		}
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
		listener, err = net.Listen("unix", filepath.Join(dir, "f.sock"))
	} else {
		listener, err = net.Listen("tcp", "127.0.0.1:0")
	}
	if err != nil {
		t.Fatal(err)
	}
	f := &countingForwarder{listener: listener}
	var conns sync.WaitGroup
	t.Cleanup(func() {
		_ = listener.Close()
	})
	go func() {
		for {
			down, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			f.accepted.Add(1)
			conns.Add(1)
			go func() {
				defer conns.Done()
				defer down.Close()
				up, dialErr := net.Dial(upstreamNetwork, upstreamAddr)
				if dialErr != nil {
					return
				}
				defer up.Close()
				done := make(chan struct{}, 2)
				go func() { _, _ = io.Copy(up, down); done <- struct{}{} }()
				go func() { _, _ = io.Copy(down, up); done <- struct{}{} }()
				<-done
			}()
		}
	}()
	return f
}

func (f *countingForwarder) addr() string { return f.listener.Addr().String() }

// ---------------------------------------------------------------------------
// Servers
// ---------------------------------------------------------------------------

// reflServer is one running server: where to reach it, per transport.
type reflServer struct {
	network string // "tcp" or "unix" -- the upstream the forwarder dials
	addr    string
	http    bool // whether addr speaks HTTP rather than raw framing
}

// newGoServer builds this port's conformance worker shape: ConformanceService,
// conformance.Secondary.v1, then reflection when reflection is true.
func newGoServer(t *testing.T, reflection bool) *vgirpc.Server {
	t.Helper()
	server := vgirpc.NewServer()
	server.SetServiceName(reflPrimary)
	server.SetServerID("conformance-go")
	server.SetProtocolVersion(reflPrimaryVersion)
	conformance.RegisterMethods(server)
	if err := server.AddProtocol(conformance.NewSecondary()); err != nil {
		t.Fatal(err)
	}
	if reflection {
		if err := vgirpc.RegisterReflection(server); err != nil {
			t.Fatal(err)
		}
	}
	return server
}

func startGoServer(t *testing.T, transport string, reflection bool) reflServer {
	t.Helper()
	server := newGoServer(t, reflection)
	switch transport {
	case "http":
		httpServer := httptest.NewServer(vgirpc.NewHttpServer(server))
		t.Cleanup(httpServer.Close)
		return reflServer{network: "tcp", addr: httpServer.Listener.Addr().String(), http: true}
	case "tcp":
		bound := make(chan string, 1)
		go func() {
			_ = server.RunTcp("127.0.0.1", 0, 0, func(host string, port int) {
				bound <- net.JoinHostPort(host, strconv.Itoa(port))
			})
		}()
		select {
		case addr := <-bound:
			return reflServer{network: "tcp", addr: addr}
		case <-time.After(10 * time.Second):
			t.Fatal("Go TCP server did not bind")
		}
	case "unix":
		dir, err := os.MkdirTemp("", "vgirefl")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
		bound := make(chan string, 1)
		go func() {
			_ = server.RunUnix(filepath.Join(dir, "s.sock"), 0, func(path string) { bound <- path })
		}()
		select {
		case path := <-bound:
			return reflServer{network: "unix", addr: path}
		case <-time.After(10 * time.Second):
			t.Fatal("Go Unix server did not bind")
		}
	}
	t.Fatalf("unknown transport %q", transport)
	return reflServer{}
}

// startPythonServer runs the reference conformance worker
// (python -m vgi_rpc.conformance._cli), which hosts conformance.Secondary.v1
// always and reflection only with --describe.
func startPythonServer(t *testing.T, transport string, reflection bool) reflServer {
	t.Helper()
	python := os.Getenv("VGI_RPC_PYTHON")
	if python == "" {
		t.Skip("set VGI_RPC_PYTHON to run against the Python reference conformance worker")
	}
	args := []string{"-m", "vgi_rpc.conformance._cli"}
	if reflection {
		args = append(args, "--describe")
	}
	var unixPath string
	switch transport {
	case "http":
		args = append(args, "--http", "0")
	case "tcp":
		args = append(args, "--tcp", "127.0.0.1:0")
	case "unix":
		dir, err := os.MkdirTemp("", "vgirefl")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
		unixPath = filepath.Join(dir, "p.sock")
		args = append(args, "--unix", unixPath)
	default:
		t.Fatalf("unknown transport %q", transport)
	}
	cmd := exec.Command(python, args...)
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start Python conformance worker: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatalf("read Python worker readiness: %v", err)
	}
	line = strings.TrimSpace(line)
	var srv reflServer
	switch {
	case strings.HasPrefix(line, "PORT:"):
		srv = reflServer{network: "tcp", addr: "127.0.0.1:" + strings.TrimPrefix(line, "PORT:"), http: true}
	case strings.HasPrefix(line, "TCP:"):
		srv = reflServer{network: "tcp", addr: strings.TrimPrefix(line, "TCP:")}
	case strings.HasPrefix(line, "UNIX:"):
		srv = reflServer{network: "unix", addr: unixPath}
	default:
		t.Fatalf("unexpected Python readiness line %q", line)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		conn, dialErr := net.DialTimeout(srv.network, srv.addr, 100*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Python worker not accepting on %s: %v", srv.addr, dialErr)
		}
		time.Sleep(25 * time.Millisecond)
	}
	// The readiness probe above was a connection of its own; a
	// single-threaded reference server must finish with it before the
	// client under test connects.
	time.Sleep(50 * time.Millisecond)
	return srv
}

// ---------------------------------------------------------------------------
// Clients
// ---------------------------------------------------------------------------

// reflClient is a client under test and the forwarder it dialled through.
type reflClient struct {
	target interface {
		vgirpc.ReflectionTarget
		CallUnary(context.Context, string, arrow.RecordBatch, *arrow.Schema) (*vgirpc.ClientBatch, error)
	}
	fwd *countingForwarder
}

// tcpIrohDialer is a raw Iroh dialer whose "Iroh connection" is a plain TCP
// connection: the framing above the byte stream is what is under test.
type tcpIrohDialer struct{ addr string }

func (d tcpIrohDialer) DialIroh(ctx context.Context, _ vgirpc.IrohEndpoint, _ vgirpc.IrohClientOptions) (net.Conn, error) {
	var dialer net.Dialer
	return dialer.DialContext(ctx, "tcp", d.addr)
}

// tcpIrohHTTPProvider carries httpi:// requests over a plain TCP HTTP
// transport to addr.
type tcpIrohHTTPProvider struct{ addr string }

type closingTransport struct{ *http.Transport }

func (c closingTransport) Close() error { c.CloseIdleConnections(); return nil }

func (p tcpIrohHTTPProvider) OpenIrohHTTP(context.Context, vgirpc.IrohEndpoint, vgirpc.IrohClientOptions) (vgirpc.IrohHTTPTransport, error) {
	addr := p.addr
	return closingTransport{&http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "tcp", addr)
		},
	}}, nil
}

// reflTransports are the client kinds, each with the server transport it
// rides on.
var reflTransports = []struct {
	name   string
	server string
}{
	{"http", "http"},
	{"tcp", "tcp"},
	{"unix", "unix"},
	{"iroh", "tcp"},
	{"httpi", "http"},
}

func dialRefl(t *testing.T, client string, srv reflServer, protocol string) reflClient {
	t.Helper()
	ctx := context.Background()
	fwdNetwork := "tcp"
	if client == "unix" {
		fwdNetwork = "unix"
	}
	fwd := newCountingForwarder(t, fwdNetwork, srv.network, srv.addr)
	// ConformanceService declares protocol_version 2.0.0 and gates every call
	// on it; the secondary declares none.
	version := ""
	if protocol == reflPrimary {
		version = reflPrimaryVersion
	}
	httpOpts := []vgirpc.HttpClientOption{vgirpc.WithClientProtocol(protocol), vgirpc.WithClientProtocolVersion(version)}
	tcpOpts := []vgirpc.TcpClientOption{vgirpc.WithTcpClientProtocol(protocol), vgirpc.WithTcpClientProtocolVersion(version)}
	switch client {
	case "http":
		c, err := vgirpc.NewHttpClient("http://"+fwd.addr(), httpOpts...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(c.Close)
		return reflClient{target: c, fwd: fwd}
	case "httpi":
		c, err := vgirpc.NewIrohHTTPClient(ctx, "httpi://"+reflIrohID, tcpIrohHTTPProvider{addr: fwd.addr()},
			vgirpc.IrohClientOptions{}, httpOpts...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(c.Close)
		return reflClient{target: c, fwd: fwd}
	case "tcp":
		host, rawPort, _ := net.SplitHostPort(fwd.addr())
		port, _ := strconv.Atoi(rawPort)
		c, err := vgirpc.NewTcpClient(ctx, host, port, tcpOpts...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return reflClient{target: c, fwd: fwd}
	case "unix":
		c, err := vgirpc.NewUnixClient(ctx, fwd.addr(), tcpOpts...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return reflClient{target: c, fwd: fwd}
	case "iroh":
		c, err := vgirpc.NewIrohClient(ctx, "iroh://"+reflIrohID, tcpIrohDialer{addr: fwd.addr()},
			vgirpc.IrohClientOptions{}, tcpOpts...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return reflClient{target: c, fwd: fwd}
	}
	t.Fatalf("unknown client %q", client)
	return reflClient{}
}

type reflServerKind struct {
	name  string
	start func(*testing.T, string, bool) reflServer
}

var reflServers = []reflServerKind{
	{"go", startGoServer},
	{"python", startPythonServer},
}

func skipUnixOnWindows(t *testing.T, transport string) {
	t.Helper()
	if transport == "unix" && runtime.GOOS == "windows" {
		t.Skip("Unix sockets are not exercised on Windows")
	}
}

// echoString calls echo_string on the client's own protocol.
func echoString(t *testing.T, c reflClient, value string) string {
	t.Helper()
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	builder.Append(value)
	column := builder.NewArray()
	builder.Release()
	params := array.NewRecordBatch(
		arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String}}, nil),
		[]arrow.Array{column}, 1)
	column.Release()
	defer params.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := c.target.CallUnary(ctx, "echo_string", params, nil)
	if err != nil {
		t.Fatalf("echo_string(%q) on the held connection: %v", value, err)
	}
	defer result.Release()
	return result.Batch.Column(0).(*array.String).Value(0)
}

func reflCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func protocolNames(hosted []vgirpc.HostedProtocol) []string {
	names := make([]string, 0, len(hosted))
	for _, p := range hosted {
		names = append(names, p.Name)
	}
	return names
}

func methodNames(desc *vgirpc.ClientServiceDescription) []string {
	names := make([]string, 0, len(desc.Methods))
	for _, m := range desc.Methods {
		names = append(names, m.Name)
	}
	slices.Sort(names)
	return names
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestReflectionClientEveryTransport runs listing, describe, unknown-protocol
// and connection reuse on one held connection per client kind and server.
func TestReflectionClientEveryTransport(t *testing.T) {
	for _, server := range reflServers {
		for _, transport := range reflTransports {
			t.Run(server.name+"/"+transport.name, func(t *testing.T) {
				skipUnixOnWindows(t, transport.server)
				srv := server.start(t, transport.server, true)
				c := dialRefl(t, transport.name, srv, reflPrimary)
				ctx := reflCtx(t)

				if got := echoString(t, c, "a"); got != "a" {
					t.Fatalf("echo_string = %q", got)
				}

				hosted, err := vgirpc.ListProtocols(ctx, c.target)
				if err != nil {
					t.Fatalf("ListProtocols: %v", err)
				}
				names := protocolNames(hosted)
				if len(names) < 3 || names[0] != reflPrimary || names[1] != reflSecondary ||
					names[2] != vgirpc.ReflectionProtocolName {
					t.Fatalf("listing order = %v, want [%s %s %s ...]", names, reflPrimary, reflSecondary,
						vgirpc.ReflectionProtocolName)
				}
				for _, p := range hosted {
					if !reflHex64.MatchString(p.Hash) {
						t.Fatalf("%s hash %q is not 64 lowercase hex", p.Name, p.Hash)
					}
				}
				if hosted[0].Deprecated || hosted[0].DeprecationMessage != "" ||
					hosted[0].Features == nil || len(hosted[0].Features) != 0 {
					t.Fatalf("primary defaults wrong: %+v", hosted[0])
				}
				if hosted[1].Hash != conformance.SecondaryProtocolHash {
					t.Fatalf("secondary hash = %s, want pinned %s", hosted[1].Hash, conformance.SecondaryProtocolHash)
				}

				desc, err := vgirpc.DescribeProtocol(ctx, c.target, reflPrimary)
				if err != nil {
					t.Fatalf("DescribeProtocol(primary): %v", err)
				}
				if desc.ProtocolName != reflPrimary || desc.ProtocolHash != hosted[0].Hash || desc.ServerID == "" {
					t.Fatalf("primary description = %+v", desc)
				}
				methods := methodNames(desc)
				for _, want := range []string{"echo_string", "echo_int", "produce_n"} {
					if !slices.Contains(methods, want) {
						t.Fatalf("primary methods %v lack %s", methods, want)
					}
				}
				for _, m := range desc.Methods {
					if m.Name == "echo_string" && m.MethodType != "unary" {
						t.Fatalf("echo_string method type = %q", m.MethodType)
					}
				}
				reflection, err := vgirpc.DescribeProtocol(ctx, c.target, vgirpc.ReflectionProtocolName)
				if err != nil {
					t.Fatalf("DescribeProtocol(reflection): %v", err)
				}
				if got := methodNames(reflection); !slices.Equal(got, []string{"describe", "list_protocols"}) {
					t.Fatalf("reflection methods = %v", got)
				}

				// An unhosted name is the server's protocol_not_supported, not
				// "no reflection".
				_, err = vgirpc.DescribeProtocol(ctx, c.target, "nope.v1")
				var notSupported *vgirpc.ReflectionNotSupportedError
				var rpcErr *vgirpc.RpcError
				if errors.As(err, &notSupported) {
					t.Fatalf("unknown protocol reported as no reflection: %v", err)
				}
				if !errors.As(err, &rpcErr) || rpcErr.Kind != "protocol_not_supported" {
					t.Fatalf("unknown protocol error = %#v, want kind protocol_not_supported", err)
				}

				if got := echoString(t, c, "b"); got != "b" {
					t.Fatalf("echo_string after reflection = %q", got)
				}
				again, err := c.target.(interface {
					ListProtocols(context.Context) ([]vgirpc.HostedProtocol, error)
				}).ListProtocols(ctx)
				if err != nil || again[0].Name != reflPrimary {
					t.Fatalf("method form ListProtocols = %v, %v", protocolNames(again), err)
				}
				if got := echoString(t, c, "c"); got != "c" {
					t.Fatalf("echo_string after second listing = %q", got)
				}
				if n := c.fwd.accepted.Load(); n != 1 {
					t.Fatalf("client opened %d connections, want exactly the one it holds", n)
				}
			})
		}
	}
}

// TestReflectionClientBoundToAnyProtocol: the target's own protocol does not
// matter, only its connection does.
func TestReflectionClientBoundToAnyProtocol(t *testing.T) {
	for _, server := range reflServers {
		for _, transport := range reflTransports {
			t.Run(server.name+"/"+transport.name, func(t *testing.T) {
				skipUnixOnWindows(t, transport.server)
				srv := server.start(t, transport.server, true)
				c := dialRefl(t, transport.name, srv, reflSecondary)
				ctx := reflCtx(t)
				hosted, err := vgirpc.ListProtocols(ctx, c.target)
				if err != nil {
					t.Fatal(err)
				}
				if names := protocolNames(hosted); len(names) < 2 || names[0] != reflPrimary || names[1] != reflSecondary {
					t.Fatalf("listing from a secondary-bound client = %v", names)
				}
				if got := echoString(t, c, "x"); got != "secondary:x" {
					t.Fatalf("secondary echo_string = %q", got)
				}
				if n := c.fwd.accepted.Load(); n != 1 {
					t.Fatalf("client opened %d connections, want 1", n)
				}
			})
		}
	}
}

// TestReflectionClientServerWithoutReflection: a server that does not host
// reflection is a ReflectionNotSupportedError carrying the server's fields --
// never an inferred or empty listing -- and the connection survives it.
func TestReflectionClientServerWithoutReflection(t *testing.T) {
	for _, server := range reflServers {
		for _, transport := range reflTransports {
			t.Run(server.name+"/"+transport.name, func(t *testing.T) {
				skipUnixOnWindows(t, transport.server)
				srv := server.start(t, transport.server, false)
				c := dialRefl(t, transport.name, srv, reflPrimary)
				ctx := reflCtx(t)

				hosted, err := vgirpc.ListProtocols(ctx, c.target)
				var notSupported *vgirpc.ReflectionNotSupportedError
				if !errors.As(err, &notSupported) {
					t.Fatalf("ListProtocols = (%v, %v), want ReflectionNotSupportedError", protocolNames(hosted), err)
				}
				if hosted != nil {
					t.Fatalf("a listing was inferred: %v", protocolNames(hosted))
				}
				if notSupported.Kind != "protocol_not_supported" || notSupported.Code != "UNIMPLEMENTED" {
					t.Fatalf("server fields not carried: kind=%q code=%q", notSupported.Kind, notSupported.Code)
				}
				var rpcErr *vgirpc.RpcError
				if !errors.As(err, &rpcErr) || rpcErr.Kind != "protocol_not_supported" {
					t.Fatalf("errors.As(*RpcError) failed on %#v", err)
				}
				if !strings.Contains(err.Error(), vgirpc.ReflectionProtocolName) {
					t.Fatalf("error does not name reflection: %v", err)
				}

				if got := echoString(t, c, "after"); got != "after" {
					t.Fatalf("echo_string after no-reflection = %q", got)
				}

				_, err = vgirpc.DescribeProtocol(ctx, c.target, reflPrimary)
				if !errors.As(err, &notSupported) {
					t.Fatalf("DescribeProtocol = %v, want ReflectionNotSupportedError", err)
				}
				if got := echoString(t, c, "again"); got != "again" {
					t.Fatalf("echo_string after describe = %q", got)
				}
				if n := c.fwd.accepted.Load(); n != 1 {
					t.Fatalf("client opened %d connections, want 1", n)
				}
			})
		}
	}
}

// TestReflectionClientAgreesAcrossPorts: this port's server and the reference
// report identical listings (names, order and hashes) for the same protocols.
func TestReflectionClientAgreesAcrossPorts(t *testing.T) {
	ctx := reflCtx(t)
	var listings [][]vgirpc.HostedProtocol
	for _, server := range reflServers {
		srv := server.start(t, "http", true)
		c := dialRefl(t, "http", srv, reflPrimary)
		hosted, err := vgirpc.ListProtocols(ctx, c.target)
		if err != nil {
			t.Fatalf("%s: %v", server.name, err)
		}
		listings = append(listings, hosted)
	}
	render := func(hosted []vgirpc.HostedProtocol) string {
		var b strings.Builder
		for _, p := range hosted {
			fmt.Fprintf(&b, "%s@%s=%s;", p.Name, p.Version, p.Hash)
		}
		return b.String()
	}
	if render(listings[0]) != render(listings[1]) {
		t.Fatalf("listings differ:\n go:     %s\n python: %s", render(listings[0]), render(listings[1]))
	}
}

func TestReflectionClientRejectsNilTarget(t *testing.T) {
	var client *vgirpc.TcpClient
	if _, err := vgirpc.ListProtocols(context.Background(), client); err == nil {
		t.Fatal("nil client accepted")
	}
	if _, err := vgirpc.DescribeProtocol(context.Background(), nil, "x"); err == nil {
		t.Fatal("nil target accepted")
	}
}
