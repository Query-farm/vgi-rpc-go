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
	// VgiIrohEndpointTLV is the fixed VGI Iroh identity TLV in the PROXY v2
	// experimental range. It is meaningful only on an explicitly enabled,
	// trusted PROXY/UNSPEC listener.
	VgiIrohEndpointTLV     byte = 0xe0
	vgiIrohEndpointVersion      = 1
	vgiIrohEndpointBytes        = 33
)

// ProxyProtocolV2Options controls identity-bearing extensions. The zero value
// retains the strict TCP/IPv4-or-IPv6 parser behavior.
type ProxyProtocolV2Options struct {
	AllowIrohIdentity bool
}

// ProxyProtocolV2Address is the asserted TCP source and destination from one
// trusted PROXY protocol v2 preamble.
type ProxyProtocolV2Address struct {
	Source            netip.AddrPort
	Destination       netip.AddrPort
	IrohEndpointID    [32]byte
	HasIrohEndpointID bool
}

// ReadProxyProtocolV2 reads exactly one bounded version-2 preamble. Bytes after
// the declared preamble remain unread for the VGI Arrow IPC decoder.
func ReadProxyProtocolV2(r io.Reader, maximumBytes int) (*ProxyProtocolV2Address, error) {
	return ReadProxyProtocolV2WithOptions(r, maximumBytes, ProxyProtocolV2Options{})
}

// ReadProxyProtocolV2WithOptions reads one bounded preamble with explicitly
// enabled identity extensions.
func ReadProxyProtocolV2WithOptions(r io.Reader, maximumBytes int, options ProxyProtocolV2Options) (*ProxyProtocolV2Address, error) {
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
	return ParseProxyProtocolV2WithOptions(preamble, maximumBytes, options)
}

// ParseProxyProtocolV2 validates one exact preamble. Only the PROXY command
// with TCP over IPv4/IPv6 is accepted. Unknown TLVs are bounded, structurally
// validated, and ignored.
func ParseProxyProtocolV2(preamble []byte, maximumBytes int) (*ProxyProtocolV2Address, error) {
	return ParseProxyProtocolV2WithOptions(preamble, maximumBytes, ProxyProtocolV2Options{})
}

// ParseProxyProtocolV2WithOptions validates one exact preamble. Iroh identity
// permits only PROXY/UNSPEC with one fixed versioned EndpointId TLV.
func ParseProxyProtocolV2WithOptions(preamble []byte, maximumBytes int, options ProxyProtocolV2Options) (*ProxyProtocolV2Address, error) {
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
	case 0x00: // UNSPEC, only for a non-IP Iroh peer
		if !options.AllowIrohIdentity {
			return nil, fmt.Errorf("vgirpc: PROXY v2 requires TCP over IPv4 or IPv6")
		}
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
		tlvType := body[offset]
		length := int(binary.BigEndian.Uint16(body[offset+1 : offset+3]))
		offset += 3
		if length > len(body)-offset {
			return nil, fmt.Errorf("vgirpc: truncated PROXY v2 TLV value")
		}
		value := body[offset : offset+length]
		if tlvType == VgiIrohEndpointTLV && options.AllowIrohIdentity {
			if result.HasIrohEndpointID {
				return nil, fmt.Errorf("vgirpc: duplicate VGI Iroh identity TLV")
			}
			if length != vgiIrohEndpointBytes || value[0] != vgiIrohEndpointVersion {
				return nil, fmt.Errorf("vgirpc: invalid VGI Iroh identity TLV")
			}
			copy(result.IrohEndpointID[:], value[1:])
			result.HasIrohEndpointID = true
		}
		offset += length
	}
	if preamble[13] == 0x00 && !result.HasIrohEndpointID {
		return nil, fmt.Errorf("vgirpc: PROXY/UNSPEC requires one VGI Iroh identity TLV")
	}
	if result.HasIrohEndpointID && preamble[13] != 0x00 {
		return nil, fmt.Errorf("vgirpc: VGI Iroh identity requires PROXY/UNSPEC")
	}
	return result, nil
}
