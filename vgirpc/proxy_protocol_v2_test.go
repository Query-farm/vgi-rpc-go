// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func proxyV2Prefix(command, family byte, bodyBytes int) []byte {
	value := append([]byte(nil), proxyProtocolV2Signature[:]...)
	value = append(value, 0x20|command, family, 0, 0)
	binary.BigEndian.PutUint16(value[14:16], uint16(bodyBytes))
	return value
}

func proxyV2IPv4() []byte {
	value := proxyV2Prefix(0x01, 0x11, 12)
	return append(value, 192, 0, 2, 7, 198, 51, 100, 9, 0x30, 0x39, 0x24, 0xb8)
}

func TestProxyProtocolV2ParsesTCPAndPreservesFollowingBytes(t *testing.T) {
	preamble := proxyV2IPv4()
	stream := bytes.NewBuffer(append(append([]byte(nil), preamble...), 0xaa, 0xbb))
	address, err := ReadProxyProtocolV2(stream, 536)
	if err != nil {
		t.Fatal(err)
	}
	if got := address.Source.String(); got != "192.0.2.7:12345" {
		t.Fatalf("source = %s", got)
	}
	if got := address.Destination.String(); got != "198.51.100.9:9400" {
		t.Fatalf("destination = %s", got)
	}
	if got := stream.Bytes(); !bytes.Equal(got, []byte{0xaa, 0xbb}) {
		t.Fatalf("following bytes consumed: %x", got)
	}
}

func TestProxyProtocolV2ParsesIPv6AndMappedIPv4(t *testing.T) {
	value := proxyV2Prefix(0x01, 0x21, 36)
	body := make([]byte, 36)
	copy(body[:16], []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 0, 2, 8})
	copy(body[16:32], []byte{0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2})
	binary.BigEndian.PutUint16(body[32:34], 443)
	binary.BigEndian.PutUint16(body[34:36], 9400)
	value = append(value, body...)
	address, err := ParseProxyProtocolV2(value, 536)
	if err != nil {
		t.Fatal(err)
	}
	if got := address.Source.String(); got != "192.0.2.8:443" {
		t.Fatalf("mapped source = %s", got)
	}
	if got := address.Destination.String(); got != "[2001:db8::2]:9400" {
		t.Fatalf("destination = %s", got)
	}
}

func TestProxyProtocolV2RejectsUnsafeForms(t *testing.T) {
	tests := map[string][]byte{
		"local":       append(proxyV2Prefix(0x00, 0x11, 12), make([]byte, 12)...),
		"udp":         append(proxyV2Prefix(0x01, 0x12, 12), make([]byte, 12)...),
		"unspecified": proxyV2Prefix(0x01, 0x00, 0),
		"truncated":   proxyV2IPv4()[:27],
		"overlong":    append(proxyV2IPv4(), 0),
	}
	for name, value := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseProxyProtocolV2(value, 536); err == nil {
				t.Fatal("unsafe PROXY v2 preamble accepted")
			}
		})
	}

	tlv := proxyV2IPv4()
	binary.BigEndian.PutUint16(tlv[14:16], 18)
	tlv = append(tlv, 0xee, 0, 3, 1, 2, 3)
	if _, err := ParseProxyProtocolV2(tlv, 536); err != nil {
		t.Fatalf("bounded unknown TLV rejected: %v", err)
	}
	if _, err := ParseProxyProtocolV2(tlv[:len(tlv)-1], 536); err == nil {
		t.Fatal("truncated TLV accepted")
	}
}
