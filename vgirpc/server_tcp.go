// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
)

// TcpConnectionIdentityResolver converts one trusted socket snapshot into the
// authentication/evidence snapshot installed for every call on that raw
// connection. The resolver must honor both ctx and resolution.Deadline().
type TcpConnectionIdentityResolver func(context.Context, *PeerResolutionContext) (*AuthContext, *PeerEvidenceSet, error)

// TcpServerOptions configures direct TCP identity or a trusted PROXY v2
// boundary without changing the raw VGI wire protocol.
type TcpServerOptions struct {
	IdleTimeout time.Duration
	OnBound     func(string, int)

	ProxyProtocolV2Required bool
	// Exact IP addresses only. Loopback is not trusted implicitly because that
	// would trust every other process on the host.
	TrustedProxyAddresses     []string
	ProxyPreambleTimeout      time.Duration
	MaximumProxyPreambleBytes int
	ServiceName               string
	// IrohProxyIssuer enables the fixed VGI Iroh EndpointId TLV on trusted
	// PROXY/UNSPEC connections. The issuer is operator-controlled and never
	// accepted from the proxy preamble.
	IrohProxyIssuer string

	// TLSConfig enables direct mutual TLS. The configuration is cloned before
	// use and forced to RequireAndVerifyClientCert. SpiffeTrustDomains must also
	// be configured so the verified client leaf can be validated as an
	// X.509-SVID and exposed as off-wire peer evidence.
	TLSConfig           *tls.Config
	TLSHandshakeTimeout time.Duration
	SpiffeTrustDomains  []string

	IdentityResolutionTimeout   time.Duration
	IdentityResolverConcurrency int
	ResolveIdentity             TcpConnectionIdentityResolver
	// PeerAuthenticationPolicy explicitly decides whether transport evidence
	// authenticates the connection. Nil observes evidence without changing the
	// authentication returned by ResolveIdentity.
	PeerAuthenticationPolicy PeerAuthenticationPolicy
}

// RunTcp serves the RPC protocol over a raw AF_INET (TCP) socket — the network
// analog of [Server.RunUnix]. It speaks the SAME raw Arrow-IPC framing as the
// Unix transport; only the listening socket differs (host:port instead of a
// filesystem path).
//
// Binds (host, port), invokes onBound(host, actualPort) once it is listening
// (the worker prints the launcher readiness marker, "TCP:<host>:<port>", there;
// when port == 0 the OS chooses a free port, reported via onBound), then accepts
// connections in a loop, serving each in its own goroutine with the same
// per-connection serve loop as RunUnix.
//
// Nagle's algorithm is disabled (TCP_NODELAY) on every accepted connection so
// the lockstep request/response framing is not delayed coalescing writes. Go's
// net package already sets SO_REUSEADDR on the listener.
//
// When idleTimeout > 0 the server self-shuts-down after that long with zero
// active connections; a startup grace of max(idleTimeout, 60s) applies so a
// client always has time to connect.
//
// SECURITY: raw TCP carries no authentication or encryption unless configured
// through RunTcpWithOptions. Bind this compatibility API to a trusted network
// only; the empty/default host is loopback ("127.0.0.1").
func (s *Server) RunTcp(host string, port int, idleTimeout time.Duration, onBound func(string, int)) error {
	return s.RunTcpWithOptions(host, port, TcpServerOptions{
		IdleTimeout: idleTimeout,
		OnBound:     onBound,
	})
}

// RunTcpWithOptions serves raw VGI over TCP and optionally resolves one
// off-wire identity snapshot per accepted connection. When PROXY v2 and TLS
// are both configured, the trusted PROXY preamble is consumed before the TLS
// handshake. Direct TLS evidence is observed unless an explicit
// PeerAuthenticationPolicy promotes or requires it.
func (s *Server) RunTcpWithOptions(host string, port int, options TcpServerOptions) error {
	// Ignore SIGPIPE so writes to a closed socket return errors instead of
	// killing the process (mirrors RunStdio / RunUnix).
	signal.Ignore(syscall.SIGPIPE)

	if options.ProxyPreambleTimeout <= 0 {
		options.ProxyPreambleTimeout = time.Second
	}
	if options.MaximumProxyPreambleBytes == 0 {
		options.MaximumProxyPreambleBytes = defaultMaxProxyV2Bytes
	}
	if options.IdentityResolutionTimeout <= 0 {
		options.IdentityResolutionTimeout = 5 * time.Second
	}
	if err := prepareDirectSpiffeTLSOptions(&options); err != nil {
		return err
	}
	if options.IdentityResolverConcurrency == 0 {
		options.IdentityResolverConcurrency = 64
	}
	if options.MaximumProxyPreambleBytes < proxyProtocolV2FixedBytes ||
		options.IdentityResolverConcurrency < 0 {
		return fmt.Errorf("vgirpc: TCP proxy and identity limits must be positive")
	}
	trustedProxies := make(map[netip.Addr]struct{}, len(options.TrustedProxyAddresses))
	for _, configured := range options.TrustedProxyAddresses {
		address, err := netip.ParseAddr(configured)
		if err != nil || address.Zone() != "" {
			return fmt.Errorf("vgirpc: trusted proxy must be an exact IPv4 or IPv6 address: %q", configured)
		}
		address = address.Unmap()
		if _, duplicate := trustedProxies[address]; duplicate {
			return fmt.Errorf("vgirpc: duplicate trusted proxy address: %q", configured)
		}
		trustedProxies[address] = struct{}{}
	}
	if options.ProxyProtocolV2Required && len(trustedProxies) == 0 {
		return fmt.Errorf("vgirpc: PROXY v2 requires at least one exact trusted proxy address")
	}
	if options.IrohProxyIssuer != "" {
		if !utf8.ValidString(options.IrohProxyIssuer) || containsIrohControl(options.IrohProxyIssuer) {
			return fmt.Errorf("vgirpc: Iroh proxy issuer must be Unicode text without controls")
		}
		if !options.ProxyProtocolV2Required {
			return fmt.Errorf("vgirpc: Iroh proxy identity requires PROXY v2")
		}
	}
	var identitySlots chan struct{}
	if options.ResolveIdentity != nil {
		if options.IdentityResolverConcurrency <= 0 {
			return fmt.Errorf("vgirpc: identity resolver concurrency must be positive")
		}
		identitySlots = make(chan struct{}, options.IdentityResolverConcurrency)
	}

	if host == "" {
		host = "127.0.0.1"
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(host, fmt.Sprintf("%d", port)))
	if err != nil {
		return fmt.Errorf("listen tcp %s:%d: %w", host, port, err)
	}
	tl := ln.(*net.TCPListener)
	defer func() { _ = tl.Close() }()

	boundPort := tl.Addr().(*net.TCPAddr).Port
	if options.OnBound != nil {
		options.OnBound(host, boundPort)
	}

	var (
		mu       sync.Mutex
		active   int
		timer    *time.Timer
		shutdown bool
		wg       sync.WaitGroup
	)
	// arm/disarm the idle timer (caller holds mu).
	disarm := func() {
		if timer != nil {
			timer.Stop()
			timer = nil
		}
	}
	arm := func(d time.Duration) {
		disarm()
		timer = time.AfterFunc(d, func() {
			mu.Lock()
			defer mu.Unlock()
			if active == 0 {
				shutdown = true
				_ = tl.Close() // unblock Accept
			}
		})
	}
	if options.IdleTimeout > 0 {
		grace := options.IdleTimeout
		if grace < 60*time.Second {
			grace = 60 * time.Second
		}
		mu.Lock()
		arm(grace)
		mu.Unlock()
	}

	ctx := context.Background()
	for {
		conn, err := tl.Accept()
		if err != nil {
			mu.Lock()
			requested := shutdown
			mu.Unlock()
			if !requested && !isTransportClosed(err) {
				slog.Error("tcp accept error", "err", err)
			}
			break
		}
		// Disable Nagle so lockstep framing flushes immediately.
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}
		mu.Lock()
		active++
		disarm()
		mu.Unlock()
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			connectionCtx, preparedConn, err := prepareTcpConnectionIdentity(ctx, c, options, trustedProxies, identitySlots)
			if err != nil {
				// An application-owned aggregate resolver may wrap daemon
				// tokens, capabilities, certificate text, or attacker input in
				// its error. Log only the fixed failure class at this boundary.
				slog.Warn("tcp connection identity rejected", "class", tcpIdentityErrorClass(err))
			} else {
				s.serveTcpConn(connectionCtx, preparedConn)
			}
			_ = c.Close()
			mu.Lock()
			active--
			if active == 0 && options.IdleTimeout > 0 && !shutdown {
				arm(options.IdleTimeout)
			}
			mu.Unlock()
		}(conn)
	}
	mu.Lock()
	disarm()
	mu.Unlock()
	wg.Wait()
	return nil
}

func prepareTcpConnectionIdentity(
	ctx context.Context,
	conn net.Conn,
	options TcpServerOptions,
	trustedProxies map[netip.Addr]struct{},
	identitySlots chan struct{},
) (context.Context, net.Conn, error) {
	setupTimeout := tcpConnectionSetupTimeout(options)
	setupCtx := ctx
	if setupTimeout > 0 {
		var cancel context.CancelFunc
		setupCtx, cancel = context.WithTimeout(ctx, setupTimeout)
		defer cancel()
	}
	immediate := conn.RemoteAddr().String()
	destination := conn.LocalAddr().String()
	asserted := ""
	var forwardedIrohResult *PeerIdentityResult
	if options.ProxyProtocolV2Required {
		tcpPeer, ok := conn.RemoteAddr().(*net.TCPAddr)
		if !ok {
			return nil, nil, fmt.Errorf("vgirpc: PROXY v2 requires a TCP immediate peer")
		}
		peerAddress, valid := netip.AddrFromSlice(tcpPeer.IP)
		if !valid {
			return nil, nil, fmt.Errorf("vgirpc: immediate proxy address is invalid")
		}
		if _, trusted := trustedProxies[peerAddress.Unmap()]; !trusted {
			return nil, nil, fmt.Errorf("vgirpc: immediate peer is not a trusted PROXY v2 sender")
		}
		if err := conn.SetReadDeadline(tcpStageDeadline(setupCtx, options.ProxyPreambleTimeout)); err != nil {
			return nil, nil, fmt.Errorf("vgirpc: set PROXY v2 preamble deadline: %w", err)
		}
		proxyAddress, err := ReadProxyProtocolV2WithOptions(conn, options.MaximumProxyPreambleBytes, ProxyProtocolV2Options{
			AllowIrohIdentity: options.IrohProxyIssuer != "",
		})
		_ = conn.SetReadDeadline(time.Time{})
		if err != nil {
			return nil, nil, err
		}
		if proxyAddress.HasIrohEndpointID {
			endpointKey := hex.EncodeToString(proxyAddress.IrohEndpointID[:])
			identity, identityErr := NewPeerIdentity(PeerIdentityOptions{
				Provider: "iroh", EvidenceSource: "proxy_protocol_v2",
				Assurance: IdentityAssuranceConfiguredProxy, Issuer: options.IrohProxyIssuer,
				Transport: "tcp", SubjectKind: PeerSubjectEndpoint, SubjectKey: endpointKey,
				SubjectStability: SubjectStabilityStable, SubjectVerified: true,
				Attributes:    map[string]any{"original_assurance": string(IdentityAssuranceCryptographicPeer)},
				SourceAddress: endpointKey, ProxyAddress: immediate,
			})
			if identityErr != nil {
				return nil, nil, identityErr
			}
			forwardedIrohResult, identityErr = NewAvailablePeerIdentityResult("iroh", identity)
			if identityErr != nil {
				return nil, nil, identityErr
			}
		} else {
			asserted = proxyAddress.Source.String()
			destination = proxyAddress.Destination.String()
		}
	}
	var directSpiffeResult *PeerIdentityResult
	if options.TLSConfig != nil {
		prepared, result, err := acceptDirectSpiffeTLS(setupCtx, conn, options, immediate, asserted, destination)
		if err != nil {
			return nil, nil, err
		}
		conn = prepared
		directSpiffeResult = result
	}
	if options.ResolveIdentity == nil && directSpiffeResult == nil && forwardedIrohResult == nil && options.PeerAuthenticationPolicy == nil {
		return ctx, conn, nil
	}

	auth := Anonymous()
	evidence := EmptyPeerEvidence()
	if options.ResolveIdentity != nil {
		var err error
		auth, evidence, err = resolveTcpConnectionIdentity(setupCtx, options, immediate, asserted, destination, identitySlots)
		if err != nil {
			return nil, nil, err
		}
		if auth == nil {
			auth = Anonymous()
		}
		if evidence == nil {
			evidence = EmptyPeerEvidence()
		}
	}
	if directSpiffeResult != nil {
		var err error
		evidence, err = appendPeerIdentityResult(evidence, directSpiffeResult)
		if err != nil {
			return nil, nil, err
		}
	}
	if forwardedIrohResult != nil {
		var err error
		evidence, err = appendPeerIdentityResult(evidence, forwardedIrohResult)
		if err != nil {
			return nil, nil, err
		}
	}
	if options.PeerAuthenticationPolicy != nil {
		var err error
		auth, err = invokePeerAuthenticationPolicy(options.PeerAuthenticationPolicy, evidence, auth)
		if err != nil {
			return nil, nil, err
		}
	}
	connectionCtx, err := WithConnectionIdentity(ctx, auth, evidence)
	return connectionCtx, conn, err
}

func tcpIdentityErrorClass(err error) string {
	var unavailable *AuthUnavailableError
	if errors.As(err, &unavailable) || errors.Is(err, context.DeadlineExceeded) {
		return "unavailable"
	}
	var failure *AuthFailure
	var rpcError *RpcError
	if errors.As(err, &failure) || (errors.As(err, &rpcError) &&
		(rpcError.Type == "ValueError" || rpcError.Type == "PermissionError")) {
		return "rejected"
	}
	return "failed"
}

func tcpConnectionSetupTimeout(options TcpServerOptions) time.Duration {
	var timeout time.Duration
	if options.ProxyProtocolV2Required && options.ProxyPreambleTimeout > timeout {
		timeout = options.ProxyPreambleTimeout
	}
	if options.TLSConfig != nil && options.TLSHandshakeTimeout > timeout {
		timeout = options.TLSHandshakeTimeout
	}
	if options.ResolveIdentity != nil && options.IdentityResolutionTimeout > timeout {
		timeout = options.IdentityResolutionTimeout
	}
	return timeout
}

func tcpStageDeadline(ctx context.Context, stageTimeout time.Duration) time.Time {
	deadline := time.Now().Add(stageTimeout)
	if setupDeadline, ok := ctx.Deadline(); ok && setupDeadline.Before(deadline) {
		return setupDeadline
	}
	return deadline
}

func resolveTcpConnectionIdentity(
	ctx context.Context,
	options TcpServerOptions,
	immediate string,
	asserted string,
	destination string,
	identitySlots chan struct{},
) (*AuthContext, *PeerEvidenceSet, error) {

	resolutionCtx, cancel := context.WithTimeout(ctx, options.IdentityResolutionTimeout)
	defer cancel()
	deadline, _ := resolutionCtx.Deadline()
	resolution, err := NewPeerResolutionContext("tcp", PeerResolutionOptions{
		ImmediatePeer: tcpEndpointHost(immediate), SourceEndpoint: immediate, AssertedPeer: asserted,
		DestinationAddress: destination, ServiceName: options.ServiceName,
		Metadata: map[string]any{"remote_addr": immediate}, Deadline: deadline,
	})
	if err != nil {
		return nil, nil, err
	}
	select {
	case identitySlots <- struct{}{}:
	case <-resolutionCtx.Done():
		return nil, nil, NewAuthUnavailable("peer identity resolution timed out")
	default:
		return nil, nil, NewAuthUnavailable("peer identity resolver capacity exhausted")
	}
	type outcome struct {
		auth     *AuthContext
		evidence *PeerEvidenceSet
		err      error
	}
	completed := make(chan outcome, 1)
	go func() {
		defer func() { <-identitySlots }()
		auth, evidence, err := invokeTcpConnectionIdentityResolver(options.ResolveIdentity, resolutionCtx, resolution)
		completed <- outcome{auth: auth, evidence: evidence, err: err}
	}()
	select {
	case result := <-completed:
		if result.err != nil {
			return nil, nil, result.err
		}
		return result.auth, result.evidence, nil
	case <-resolutionCtx.Done():
		return nil, nil, NewAuthUnavailable("peer identity resolution timed out")
	}
}

func invokeTcpConnectionIdentityResolver(
	resolver TcpConnectionIdentityResolver,
	ctx context.Context,
	resolution *PeerResolutionContext,
) (auth *AuthContext, evidence *PeerEvidenceSet, err error) {
	defer func() {
		if recover() != nil {
			auth = nil
			evidence = nil
			err = fmt.Errorf("peer identity provider failed")
		}
	}()
	return resolver(ctx, resolution)
}

func tcpEndpointHost(endpoint string) string {
	host, _, err := net.SplitHostPort(endpoint)
	if err == nil {
		return host
	}
	return endpoint
}

// serveTcpConn runs the serve loop over a single AF_INET connection, mirroring
// serveUnixConn but advertising the tcp transport kind to dispatch hooks.
func (s *Server) serveTcpConn(ctx context.Context, conn net.Conn) {
	if err := s.notifyTransport(TransportKindTcp, nil); err != nil {
		return
	}
	// Per-connection shared-memory segment cache (see ServeWithContext).
	shmConn := &shmConnState{}
	defer shmConn.close()
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		if err := s.serveOne(ctx, conn, conn, shmConn); err != nil {
			if err != io.EOF && !isTransportClosed(err) {
				slog.Error("tcp serve loop error", "err", err)
			}
			return
		}
	}
}
