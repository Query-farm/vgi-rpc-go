// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"encoding/binary"
	"fmt"
	"io"
	"net/netip"
)

var proxyProtocolV2Signature = [12]byte{0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51, 0x55, 0x49, 0x54, 0x0a}

const (
	proxyProtocolV2FixedBytes = 16
	defaultMaxProxyV2Bytes    = 536
)

// ProxyProtocolV2Address is the asserted TCP source and destination from one
// trusted PROXY protocol v2 preamble.
type ProxyProtocolV2Address struct {
	Source      netip.AddrPort
	Destination netip.AddrPort
}

// ReadProxyProtocolV2 reads exactly one bounded version-2 preamble. Bytes after
// the declared preamble remain unread for the VGI Arrow IPC decoder.
func ReadProxyProtocolV2(r io.Reader, maximumBytes int) (*ProxyProtocolV2Address, error) {
	if maximumBytes == 0 {
		maximumBytes = defaultMaxProxyV2Bytes
	}
	if maximumBytes < proxyProtocolV2FixedBytes {
		return nil, fmt.Errorf("vgirpc: maximum PROXY v2 bytes must be at least 16")
	}
	fixed := make([]byte, proxyProtocolV2FixedBytes)
	if _, err := io.ReadFull(r, fixed); err != nil {
		return nil, fmt.Errorf("vgirpc: truncated PROXY v2 fixed preamble: %w", err)
	}
	total := proxyProtocolV2FixedBytes + int(binary.BigEndian.Uint16(fixed[14:16]))
	if total > maximumBytes {
		return nil, fmt.Errorf("vgirpc: PROXY v2 preamble exceeds configured limit")
	}
	preamble := make([]byte, total)
	copy(preamble, fixed)
	if _, err := io.ReadFull(r, preamble[proxyProtocolV2FixedBytes:]); err != nil {
		return nil, fmt.Errorf("vgirpc: truncated PROXY v2 body: %w", err)
	}
	return ParseProxyProtocolV2(preamble, maximumBytes)
}

// ParseProxyProtocolV2 validates one exact preamble. Only the PROXY command
// with TCP over IPv4/IPv6 is accepted. Unknown TLVs are bounded, structurally
// validated, and ignored.
func ParseProxyProtocolV2(preamble []byte, maximumBytes int) (*ProxyProtocolV2Address, error) {
	if maximumBytes == 0 {
		maximumBytes = defaultMaxProxyV2Bytes
	}
	if len(preamble) < proxyProtocolV2FixedBytes {
		return nil, fmt.Errorf("vgirpc: truncated PROXY v2 fixed preamble")
	}
	if len(preamble) > maximumBytes {
		return nil, fmt.Errorf("vgirpc: PROXY v2 preamble exceeds configured limit")
	}
	if string(preamble[:12]) != string(proxyProtocolV2Signature[:]) {
		return nil, fmt.Errorf("vgirpc: missing PROXY v2 signature")
	}
	if preamble[12]&0xf0 != 0x20 {
		return nil, fmt.Errorf("vgirpc: unsupported PROXY protocol version")
	}
	if preamble[12]&0x0f != 0x01 {
		return nil, fmt.Errorf("vgirpc: PROXY v2 LOCAL command is not accepted")
	}
	expected := proxyProtocolV2FixedBytes + int(binary.BigEndian.Uint16(preamble[14:16]))
	if len(preamble) != expected {
		return nil, fmt.Errorf("vgirpc: truncated or overlong PROXY v2 preamble")
	}
	body := preamble[proxyProtocolV2FixedBytes:]
	result := &ProxyProtocolV2Address{}
	addressBytes := 0
	switch preamble[13] {
	case 0x11: // INET + STREAM
		addressBytes = 12
		if len(body) < addressBytes {
			return nil, fmt.Errorf("vgirpc: truncated PROXY v2 TCP/IPv4 address block")
		}
		result.Source = netip.AddrPortFrom(netip.AddrFrom4([4]byte(body[0:4])), binary.BigEndian.Uint16(body[8:10]))
		result.Destination = netip.AddrPortFrom(netip.AddrFrom4([4]byte(body[4:8])), binary.BigEndian.Uint16(body[10:12]))
	case 0x21: // INET6 + STREAM
		addressBytes = 36
		if len(body) < addressBytes {
			return nil, fmt.Errorf("vgirpc: truncated PROXY v2 TCP/IPv6 address block")
		}
		result.Source = netip.AddrPortFrom(netip.AddrFrom16([16]byte(body[0:16])).Unmap(), binary.BigEndian.Uint16(body[32:34]))
		result.Destination = netip.AddrPortFrom(netip.AddrFrom16([16]byte(body[16:32])).Unmap(), binary.BigEndian.Uint16(body[34:36]))
	default:
		return nil, fmt.Errorf("vgirpc: PROXY v2 requires TCP over IPv4 or IPv6")
	}
	for offset := addressBytes; offset < len(body); {
		if len(body)-offset < 3 {
			return nil, fmt.Errorf("vgirpc: truncated PROXY v2 TLV header")
		}
		length := int(binary.BigEndian.Uint16(body[offset+1 : offset+3]))
		offset += 3
		if length > len(body)-offset {
			return nil, fmt.Errorf("vgirpc: truncated PROXY v2 TLV value")
		}
		offset += length
	}
	return result, nil
}
