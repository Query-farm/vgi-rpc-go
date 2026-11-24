// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgigcs

import (
	"context"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Storage timeouts, matching vgirpc's storage client (and vgi-rpc-python's):
// no whole-request deadline, a 30 s connect, and a stall timeout -- a
// transfer fails only when no byte moves for that long. This module keeps
// its own copy so it builds against released vgirpc cores.
const (
	envStorageStallTimeout     = "VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS"
	storageConnectTimeout      = 30 * time.Second
	defaultStorageStallTimeout = 30 * time.Second
	storageWriteChunk          = 64 << 10
)

// stallTimeout resolves a configured stall timeout: configured when > 0,
// else VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS when a positive number, else 30 s.
func stallTimeout(configured time.Duration) time.Duration {
	if configured > 0 {
		return configured
	}
	raw := strings.TrimSpace(os.Getenv(envStorageStallTimeout))
	if seconds, err := strconv.ParseFloat(raw, 64); err == nil && seconds > 0 && seconds <= 86400 {
		return time.Duration(seconds * float64(time.Second))
	}
	return defaultStorageStallTimeout
}

// stallConn refreshes a read+write deadline before every socket operation
// (writes in chunks of at most 64 KiB), so an operation fails only after the
// stall timeout passes with no progress in either direction.
type stallConn struct {
	net.Conn
	stall time.Duration
}

func (c *stallConn) Read(p []byte) (int, error) {
	if err := c.Conn.SetDeadline(time.Now().Add(c.stall)); err != nil {
		return 0, err
	}
	return c.Conn.Read(p)
}

func (c *stallConn) Write(p []byte) (int, error) {
	written := 0
	for written < len(p) {
		end := min(written+storageWriteChunk, len(p))
		if err := c.Conn.SetDeadline(time.Now().Add(c.stall)); err != nil {
			return written, err
		}
		n, err := c.Conn.Write(p[written:end])
		written += n
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

// stallDialer dials with the connect timeout and wraps the connection in a
// stallConn.
func stallDialer(stall time.Duration) func(ctx context.Context, network, addr string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: storageConnectTimeout, KeepAlive: 30 * time.Second}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &stallConn{Conn: conn, stall: stall}, nil
	}
}
