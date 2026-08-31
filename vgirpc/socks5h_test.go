// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSOCKS5HDialerUsesProxySideHostnameResolutionAndPartialReplies(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	targets := make(chan []byte, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		greeting := make([]byte, 3)
		_, _ = io.ReadFull(conn, greeting)
		for _, value := range []byte{0x05, 0x00} {
			_, _ = conn.Write([]byte{value})
		}
		header := make([]byte, 5)
		_, _ = io.ReadFull(conn, header)
		request := append([]byte(nil), header...)
		nameAndPort := make([]byte, int(header[4])+2)
		_, _ = io.ReadFull(conn, nameAndPort)
		request = append(request, nameAndPort...)
		targets <- request
		for _, value := range []byte{0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0x24, 0xb8} {
			_, _ = conn.Write([]byte{value})
		}
		_, _ = io.Copy(io.Discard, conn)
	}()

	dialer, err := newSOCKS5HDialer("socks5h://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := dialer.DialContext(ctx, "tcp", "BÜCHER.example:9400")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	request := <-targets
	wantName := "xn--bcher-kva.example"
	if request[0] != 0x05 || request[1] != 0x01 || request[3] != 0x03 {
		t.Fatalf("connect request header = %v", request[:5])
	}
	if got := string(request[5 : len(request)-2]); got != wantName {
		t.Fatalf("proxy target = %q, want %q", got, wantName)
	}
	if got := int(request[len(request)-2])<<8 | int(request[len(request)-1]); got != 9400 {
		t.Fatalf("proxy target port = %d, want 9400", got)
	}
}

func TestSOCKS5HConnectRequestSupportsIPTargets(t *testing.T) {
	v4, err := socks5ConnectRequest("192.0.2.1:443")
	if err != nil {
		t.Fatal(err)
	}
	if v4[3] != 0x01 {
		t.Fatalf("IPv4 address type = %d", v4[3])
	}
	v6, err := socks5ConnectRequest("[2001:db8::1]:443")
	if err != nil {
		t.Fatal(err)
	}
	if v6[3] != 0x04 {
		t.Fatalf("IPv6 address type = %d", v6[3])
	}
}

func TestSOCKS5HRejectsEmptyDomainInConnectReply(t *testing.T) {
	if err := discardSOCKS5Address(strings.NewReader("\x00\x00\x50"), 0x03); err == nil {
		t.Fatal("empty SOCKS5h response domain was accepted")
	}
}

func TestSOCKS5HProxyURLValidation(t *testing.T) {
	for _, value := range []string{
		"http://127.0.0.1:1055",
		"socks5h://user:password@127.0.0.1:1055",
		"socks5h://127.0.0.1",
		"socks5h://127.0.0.1:0",
		"socks5h://127.0.0.1:1055/path",
		"socks5h://127.0.0.1:1055?x=y",
	} {
		t.Run(value, func(t *testing.T) {
			if _, err := newSOCKS5HDialer(value); err == nil {
				t.Fatalf("newSOCKS5HDialer(%q) succeeded", value)
			}
		})
	}
}

func TestSOCKS5HHandshakeHonorsCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- conn
		}
	}()
	dialer, err := newSOCKS5HDialer("socks5h://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, dialErr := dialer.DialContext(ctx, "tcp", "worker.example:9400")
		done <- dialErr
	}()
	conn := <-accepted
	defer conn.Close()
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled handshake succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled SOCKS5h handshake did not return")
	}
}

func TestClientTCPProxyCannotBeCombinedWithInjectedHTTPClient(t *testing.T) {
	_, err := NewHttpClient(
		"http://worker.example:9400",
		WithClientTCPProxy("socks5h://127.0.0.1:1055"),
		WithClientHTTPClient(&http.Client{}),
	)
	if err == nil {
		t.Fatal("conflicting client transport options succeeded")
	}
}

func TestClientTCPProxyCarriesHTTPWithoutLocalTargetResolution(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	targets := make(chan string, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		greeting := make([]byte, 3)
		_, _ = io.ReadFull(conn, greeting)
		_, _ = conn.Write([]byte{0x05, 0x00})
		header := make([]byte, 5)
		_, _ = io.ReadFull(conn, header)
		name := make([]byte, int(header[4]))
		_, _ = io.ReadFull(conn, name)
		port := make([]byte, 2)
		_, _ = io.ReadFull(conn, port)
		targets <- string(name)
		_, _ = conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 80})
		request, readErr := http.ReadRequest(bufio.NewReader(conn))
		if readErr != nil {
			return
		}
		_ = request.Body.Close()
		_, _ = io.WriteString(conn, "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
	}()

	client, err := NewHttpClient(
		"http://worker.vgi-test.invalid:9400",
		WithClientTCPProxy("socks5h://"+listener.Addr().String()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, client.baseURL.String()+"/health", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.inner.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("HTTP status = %d", response.StatusCode)
	}
	if got := <-targets; !strings.EqualFold(got, "worker.vgi-test.invalid") {
		t.Fatalf("SOCKS target = %q", got)
	}
}

func TestClientTCPProxyFailureNeverFallsBackToDirectTCP(t *testing.T) {
	var directRequests atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		directRequests.Add(1)
	}))
	defer target.Close()
	unusedProxy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxyAddress := unusedProxy.Addr().String()
	unusedProxy.Close()
	client, err := NewHttpClient(target.URL, WithClientTCPProxy("socks5h://"+proxyAddress))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.inner.Do(request); err == nil {
		t.Fatal("request unexpectedly succeeded after proxy failure")
	}
	if got := directRequests.Load(); got != 0 {
		t.Fatalf("proxy failure fell back to direct HTTP: %d request(s)", got)
	}
}
