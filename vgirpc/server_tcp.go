// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

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
// SECURITY: raw TCP carries NO authentication or encryption. Bind it to a
// trusted network only; the empty/default host is loopback ("127.0.0.1"). For
// untrusted networks use the HTTP transport, which carries auth middleware and
// TLS via the fronting server.
func (s *Server) RunTcp(host string, port int, idleTimeout time.Duration, onBound func(string, int)) error {
	// Ignore SIGPIPE so writes to a closed socket return errors instead of
	// killing the process (mirrors RunStdio / RunUnix).
	signal.Ignore(syscall.SIGPIPE)

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
	if onBound != nil {
		onBound(host, boundPort)
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
	if idleTimeout > 0 {
		grace := idleTimeout
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
			s.serveTcpConn(ctx, c)
			_ = c.Close()
			mu.Lock()
			active--
			if active == 0 && idleTimeout > 0 && !shutdown {
				arm(idleTimeout)
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
