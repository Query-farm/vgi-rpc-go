// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// RunUnix serves the RPC protocol over an AF_UNIX socket at path — the worker
// side of the VGI "launcher" transport. It binds the socket, invokes
// onBound(path) once it is listening (the worker prints the launcher readiness
// marker, "UNIX:<path>", there), then accepts connections in a loop, serving
// each in its own goroutine with the same per-connection serve loop as stdio.
//
// When idleTimeout > 0 the server self-shuts-down after that long with zero
// active connections; a startup grace of max(idleTimeout, 60s) applies so a
// client always has time to connect. The socket file is removed on return.
func (s *Server) RunUnix(path string, idleTimeout time.Duration, onBound func(string)) error {
	// Ignore SIGPIPE so writes to a closed socket return errors instead of
	// killing the process (mirrors RunStdio).
	signal.Ignore(syscall.SIGPIPE)

	// Remove any stale socket file, then bind.
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		return fmt.Errorf("listen unix %q: %w", path, err)
	}
	// Tighten permissions to owner-only.
	_ = os.Chmod(path, 0o600)
	ul := ln.(*net.UnixListener)
	defer func() {
		_ = ul.Close()
		_ = os.Remove(path)
	}()

	if onBound != nil {
		onBound(path)
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
				_ = ul.Close() // unblock Accept
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
		conn, err := ul.Accept()
		if err != nil {
			mu.Lock()
			requested := shutdown
			mu.Unlock()
			if !requested && !isTransportClosed(err) {
				slog.Error("unix accept error", "err", err)
			}
			break
		}
		mu.Lock()
		active++
		disarm()
		mu.Unlock()
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			s.serveUnixConn(ctx, c)
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

// serveUnixConn runs the serve loop over a single AF_UNIX connection, mirroring
// ServeWithContext but advertising the unix transport kind to dispatch hooks.
func (s *Server) serveUnixConn(ctx context.Context, conn net.Conn) {
	if err := s.notifyTransport(TransportKindUnix, nil); err != nil {
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
				slog.Error("unix serve loop error", "err", err)
			}
			return
		}
	}
}
