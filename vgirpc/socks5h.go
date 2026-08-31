// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/idna"
)

type socks5hDialer struct {
	proxyAddress string
}

func newSOCKS5HDialer(rawURL string) (*socks5hDialer, error) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme != "socks5h" || u.Hostname() == "" || u.Port() == "" {
		return nil, errors.New("vgirpc: TCP proxy must be a socks5h://host:port URL")
	}
	if u.User != nil {
		return nil, errors.New("vgirpc: SOCKS5h authentication is not supported")
	}
	if u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return nil, errors.New("vgirpc: SOCKS5h proxy URL must not contain a path, query, or fragment")
	}
	port, err := strconv.ParseUint(u.Port(), 10, 16)
	if err != nil || port == 0 {
		return nil, errors.New("vgirpc: SOCKS5h proxy port must be between 1 and 65535")
	}
	return &socks5hDialer{proxyAddress: net.JoinHostPort(u.Hostname(), strconv.FormatUint(port, 10))}, nil
}

func (d *socks5hDialer) DialContext(ctx context.Context, network, target string) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, fmt.Errorf("vgirpc: SOCKS5h does not support network %q", network)
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", d.proxyAddress)
	if err != nil {
		return nil, fmt.Errorf("vgirpc: connecting to SOCKS5h proxy: %w", err)
	}
	succeeded := false
	defer func() {
		if !succeeded {
			_ = conn.Close()
		}
	}()

	// A context can be cancelled without carrying a deadline. Closing the
	// connection makes every handshake read/write observe that cancellation.
	stopCancellation := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopCancellation()
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return nil, fmt.Errorf("vgirpc: setting SOCKS5h deadline: %w", err)
		}
	}

	if err := writeAll(conn, []byte{0x05, 0x01, 0x00}); err != nil {
		return nil, fmt.Errorf("vgirpc: SOCKS5h greeting: %w", err)
	}
	greeting := make([]byte, 2)
	if _, err := io.ReadFull(conn, greeting); err != nil {
		return nil, fmt.Errorf("vgirpc: SOCKS5h greeting response: %w", err)
	}
	if greeting[0] != 0x05 || greeting[1] != 0x00 {
		return nil, fmt.Errorf("vgirpc: SOCKS5h proxy rejected NO AUTH (version=%d method=%d)", greeting[0], greeting[1])
	}

	request, err := socks5ConnectRequest(target)
	if err != nil {
		return nil, err
	}
	if err := writeAll(conn, request); err != nil {
		return nil, fmt.Errorf("vgirpc: SOCKS5h connect request: %w", err)
	}
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, fmt.Errorf("vgirpc: SOCKS5h connect response: %w", err)
	}
	if header[0] != 0x05 || header[2] != 0x00 {
		return nil, errors.New("vgirpc: malformed SOCKS5h connect response")
	}
	if header[1] != 0x00 {
		return nil, fmt.Errorf("vgirpc: SOCKS5h proxy rejected target (reply=%d)", header[1])
	}
	if err := discardSOCKS5Address(conn, header[3]); err != nil {
		return nil, err
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("vgirpc: clearing SOCKS5h deadline: %w", err)
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetNoDelay(true); err != nil {
			return nil, fmt.Errorf("vgirpc: setting TCP_NODELAY: %w", err)
		}
	}
	if !stopCancellation() {
		return nil, fmt.Errorf("vgirpc: SOCKS5h handshake cancelled: %w", ctx.Err())
	}
	succeeded = true
	return conn, nil
}

func socks5ConnectRequest(target string) ([]byte, error) {
	host, rawPort, err := net.SplitHostPort(target)
	if err != nil {
		return nil, fmt.Errorf("vgirpc: invalid SOCKS5h target %q: %w", target, err)
	}
	port, err := strconv.ParseUint(rawPort, 10, 16)
	if err != nil || port == 0 {
		return nil, errors.New("vgirpc: SOCKS5h target port must be between 1 and 65535")
	}
	request := []byte{0x05, 0x01, 0x00}
	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			request = append(request, 0x01)
			request = append(request, v4...)
		} else {
			request = append(request, 0x04)
			request = append(request, ip.To16()...)
		}
	} else {
		asciiHost, err := idna.Lookup.ToASCII(strings.TrimSuffix(host, "."))
		if err != nil || asciiHost == "" || len(asciiHost) > 255 {
			return nil, fmt.Errorf("vgirpc: invalid SOCKS5h target hostname %q", host)
		}
		request = append(request, 0x03, byte(len(asciiHost)))
		request = append(request, asciiHost...)
	}
	return binary.BigEndian.AppendUint16(request, uint16(port)), nil
}

func discardSOCKS5Address(r io.Reader, addressType byte) error {
	length := 0
	switch addressType {
	case 0x01:
		length = net.IPv4len
	case 0x04:
		length = net.IPv6len
	case 0x03:
		one := []byte{0}
		if _, err := io.ReadFull(r, one); err != nil {
			return fmt.Errorf("vgirpc: SOCKS5h domain length: %w", err)
		}
		length = int(one[0])
		if length == 0 {
			return errors.New("vgirpc: SOCKS5h response has an empty domain address")
		}
	default:
		return fmt.Errorf("vgirpc: SOCKS5h response has unsupported address type %d", addressType)
	}
	if _, err := io.CopyN(io.Discard, r, int64(length+2)); err != nil {
		return fmt.Errorf("vgirpc: truncated SOCKS5h bound address: %w", err)
	}
	return nil
}

func writeAll(w io.Writer, value []byte) error {
	for len(value) > 0 {
		n, err := w.Write(value)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrUnexpectedEOF
		}
		value = value[n:]
	}
	return nil
}
