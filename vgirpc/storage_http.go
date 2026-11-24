// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Storage HTTP timeouts.
//
// Every request to a storage host (signed PUT/GET to R2, S3 or GCS, inbound
// pointer fetches, request uploads to vended URLs) runs on a client built by
// [NewStorageHTTPClient]. Such a client has no whole-request deadline: a
// large transfer on a slow link is never killed while bytes are moving. It
// fails only when the connection cannot be established within
// [StorageConnectTimeout], or when no byte is sent or received for the stall
// timeout (default [DefaultStorageStallTimeout], overridden by
// VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS). The values match vgi-rpc-python's
// storage client (httpx with a 30 s connect/read/write timeout).
const (
	// EnvStorageStallTimeout overrides the storage stall timeout, in
	// seconds (a positive number; anything else keeps the default).
	EnvStorageStallTimeout = "VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS"
	// StorageConnectTimeout bounds the TCP connect and the TLS handshake of
	// a storage request.
	StorageConnectTimeout = 30 * time.Second
	// DefaultStorageStallTimeout is how long a storage transfer may make no
	// progress (no byte written or read) before it fails.
	DefaultStorageStallTimeout = 30 * time.Second
	// storageWriteChunk bounds one socket write, so the stall timeout
	// applies per chunk rather than to one large write of a whole body.
	storageWriteChunk = 64 << 10
)

// StorageStallTimeoutFromEnv reads VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS; it
// returns [DefaultStorageStallTimeout] when the variable is unset or not a
// positive number. getenv defaults to os.Getenv.
func StorageStallTimeoutFromEnv(getenv func(string) string) time.Duration {
	if getenv == nil {
		getenv = os.Getenv
	}
	raw := strings.TrimSpace(getenv(EnvStorageStallTimeout))
	if raw == "" {
		return DefaultStorageStallTimeout
	}
	seconds, err := strconv.ParseFloat(raw, 64)
	if err != nil || seconds <= 0 || seconds > 86400 {
		return DefaultStorageStallTimeout
	}
	return time.Duration(seconds * float64(time.Second))
}

// stallConn refreshes a read+write deadline before every socket operation,
// so an operation fails only after stall without progress in either
// direction. Refreshing both on a write matters: net/http's read loop sits
// in a Read while a long request body is being written, and must not time
// out while the upload is progressing.
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

// NewStorageTransport returns an HTTP/1.1 transport for storage hosts: a
// [StorageConnectTimeout] connect and TLS handshake, the proxy from the
// environment, and a stall timeout (<= 0: [StorageStallTimeoutFromEnv]) on
// every socket read and write, including the wait for response headers.
func NewStorageTransport(stall time.Duration) *http.Transport {
	if stall <= 0 {
		stall = StorageStallTimeoutFromEnv(nil)
	}
	dialer := &net.Dialer{Timeout: StorageConnectTimeout, KeepAlive: 30 * time.Second}
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return &stallConn{Conn: conn, stall: stall}, nil
		},
		TLSHandshakeTimeout:   StorageConnectTimeout,
		ExpectContinueTimeout: time.Second,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       stall,
	}
}

// NewStorageHTTPClient returns a bare client for storage hosts: the
// [NewStorageTransport] transport, no cookie jar, no redirects, and no
// whole-request Timeout.
func NewStorageHTTPClient(stall time.Duration) *http.Client {
	return &http.Client{Transport: NewStorageTransport(stall), CheckRedirect: noRedirects}
}

// newStorageFetchClient is a storage client for inbound pointer fetches:
// the storage transport, no Timeout, and the default redirect policy, which
// the fetch replaces with its own validated, bounded one.
func newStorageFetchClient(transport http.RoundTripper) *http.Client {
	return &http.Client{Transport: transport}
}

var (
	sharedStorageOnce        sync.Once
	sharedStorageClient      *http.Client
	sharedStorageFetchClient *http.Client
)

func initSharedStorage() {
	transport := NewStorageTransport(0)
	sharedStorageClient = &http.Client{Transport: transport, CheckRedirect: noRedirects}
	sharedStorageFetchClient = newStorageFetchClient(transport)
}

// sharedStorageHTTPClient is the process-wide bare storage client (stall
// timeout from the environment, no redirects), for storage requests made
// without a configured client.
func sharedStorageHTTPClient() *http.Client {
	sharedStorageOnce.Do(initSharedStorage)
	return sharedStorageClient
}

// sharedStorageFetchHTTPClient is the process-wide pointer-fetch client
// (same transport; redirects decided per fetch).
func sharedStorageFetchHTTPClient() *http.Client {
	sharedStorageOnce.Do(initSharedStorage)
	return sharedStorageFetchClient
}
