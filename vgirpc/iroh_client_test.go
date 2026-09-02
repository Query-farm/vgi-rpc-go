// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

const testIrohID = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestParseIrohEndpoint(t *testing.T) {
	raw, err := ParseIrohEndpoint("iroh://" + testIrohID)
	if err != nil || raw.ALPN != IrohArrowMuxALPN || raw.EndpointIDBytes[0] != 0x01 {
		t.Fatalf("unexpected raw endpoint: %#v, %v", raw, err)
	}
	http, err := ParseIrohEndpoint("httpi://" + testIrohID + "/api/v1")
	if err != nil || http.ALPN != IrohHTTPALPN || http.BasePath != "/api/v1" {
		t.Fatalf("unexpected HTTP endpoint: %#v, %v", http, err)
	}
}

func TestIrohCanonicalFixture(t *testing.T) {
	data, err := os.ReadFile("testdata/iroh_transport_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		ALPNs map[string]string `json:"alpns"`
		URIs  []struct {
			URI      string `json:"uri"`
			Valid    bool   `json:"valid"`
			Scheme   string `json:"scheme"`
			BasePath string `json:"base_path"`
		} `json:"uri_cases"`
		Errors []struct {
			Stage     IrohErrorStage        `json:"stage"`
			Category  IrohErrorCategory     `json:"category"`
			Certainty IrohDispatchCertainty `json:"dispatch_certainty"`
		} `json:"error_cases"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.ALPNs["iroh"] != IrohArrowMuxALPN || fixture.ALPNs["httpi"] != IrohHTTPALPN {
		t.Fatal("ALPN fixture mismatch")
	}
	validStages := map[IrohErrorStage]bool{IrohStageParse: true, IrohStageBind: true, IrohStageResolve: true, IrohStageConnect: true, IrohStageALPN: true, IrohStageOpenStream: true, IrohStageWrite: true, IrohStageRead: true, IrohStageCancel: true, IrohStageClose: true, IrohStageInternal: true}
	validCategories := map[IrohErrorCategory]bool{IrohCategoryInvalidInput: true, IrohCategoryUnsupported: true, IrohCategoryUnavailable: true, IrohCategoryTimeout: true, IrohCategoryProtocol: true, IrohCategoryConnectionReset: true, IrohCategoryCancelled: true, IrohCategoryAuthentication: true, IrohCategoryResourceExhausted: true, IrohCategoryInternal: true}
	validCertainties := map[IrohDispatchCertainty]bool{IrohNotSent: true, IrohUnknown: true, IrohSent: true}
	for _, vector := range fixture.URIs {
		endpoint, parseErr := ParseIrohEndpoint(vector.URI)
		if vector.Valid {
			if parseErr != nil {
				t.Errorf("valid URI %q: %v", vector.URI, parseErr)
				continue
			}
			if endpoint.Scheme != vector.Scheme || endpoint.BasePath != vector.BasePath {
				t.Errorf("URI %q mismatch: %#v", vector.URI, endpoint)
			}
		} else {
			var failure *IrohTransportError
			if parseErr == nil || !errors.As(parseErr, &failure) || failure.Stage != IrohStageParse || failure.Category != IrohCategoryInvalidInput || failure.DispatchCertainty != IrohNotSent {
				t.Errorf("invalid URI %q returned %#v", vector.URI, parseErr)
			}
		}
	}
	for _, vector := range fixture.Errors {
		if !validStages[vector.Stage] || !validCategories[vector.Category] || !validCertainties[vector.Certainty] {
			t.Errorf("unknown error vector: %#v", vector)
		}
	}
}

func TestParseIrohEndpointRejectsNonCanonicalForms(t *testing.T) {
	invalid := []string{
		"iroh://" + strings.ToUpper(testIrohID), "iroh://" + testIrohID + "/",
		"iroh://" + testIrohID + ":443", "iroh://user@" + testIrohID,
		"httpi://" + testIrohID + "/a//b", "httpi://" + testIrohID + "/a/../b",
		"httpi://" + testIrohID + "/bad%2", "httpi://" + testIrohID + "?x=1",
	}
	for _, value := range invalid {
		if _, err := ParseIrohEndpoint(value); err == nil {
			t.Errorf("ParseIrohEndpoint(%q) succeeded", value)
		}
	}
}

type testIrohDialer struct {
	endpoint IrohEndpoint
	options  IrohClientOptions
	client   net.Conn
	server   net.Conn
}

func (dialer *testIrohDialer) DialIroh(_ context.Context, endpoint IrohEndpoint, options IrohClientOptions) (net.Conn, error) {
	dialer.endpoint = endpoint
	dialer.options = options
	dialer.client, dialer.server = net.Pipe()
	return dialer.client, nil
}

func TestNewIrohClientUsesExplicitDialer(t *testing.T) {
	dialer := &testIrohDialer{}
	client, err := NewIrohClient(context.Background(), "iroh://"+testIrohID, dialer, IrohClientOptions{
		RemoteRelayURL: "https://relay.example", DirectAddresses: []string{"127.0.0.1:4433"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if dialer.endpoint.ALPN != IrohArrowMuxALPN {
		t.Fatalf("wrong ALPN %q", dialer.endpoint.ALPN)
	}
	if len(dialer.options.SecretKey) != 32 {
		t.Fatal("default identity was not materialized")
	}
	if dialer.options.RemoteRelayURL != "https://relay.example" || len(dialer.options.DirectAddresses) != 1 {
		t.Fatal("remote address hints were not forwarded")
	}
	_ = client.Close()
	_ = dialer.server.Close()
	if _, err := NewIrohClient(context.Background(), "httpi://"+testIrohID, dialer, IrohClientOptions{}); err == nil {
		t.Fatal("httpi endpoint unexpectedly accepted by raw client")
	}
}

func TestNewIrohClientUsesProcessStableDefaultIdentity(t *testing.T) {
	first, second := &testIrohDialer{}, &testIrohDialer{}
	firstClient, err := NewIrohClient(context.Background(), "iroh://"+testIrohID, first, IrohClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = firstClient.Close()
	_ = first.server.Close()
	secondClient, err := NewIrohClient(context.Background(), "iroh://"+testIrohID, second, IrohClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = secondClient.Close()
	_ = second.server.Close()
	if !bytes.Equal(first.options.SecretKey, second.options.SecretKey) {
		t.Fatal("default identity changed across connections")
	}
}

func TestIrohClientReportsCancellationBeforeWrite(t *testing.T) {
	dialer := &testIrohDialer{}
	client, err := NewIrohClient(context.Background(), "iroh://"+testIrohID, dialer, IrohClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer dialer.server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.CallUnary(ctx, "unused", nil, nil)
	var failure *IrohTransportError
	if !errors.As(err, &failure) || failure.Stage != IrohStageCancel || failure.Category != IrohCategoryCancelled || failure.DispatchCertainty != IrohNotSent {
		t.Fatalf("unexpected cancellation: %#v", err)
	}
}

type blockingIrohDialer struct{}

func (blockingIrohDialer) DialIroh(ctx context.Context, _ IrohEndpoint, _ IrohClientOptions) (net.Conn, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestNewIrohClientEnforcesConnectTimeout(t *testing.T) {
	_, err := NewIrohClient(context.Background(), "iroh://"+testIrohID, blockingIrohDialer{}, IrohClientOptions{ConnectTimeout: time.Millisecond, IOTimeout: time.Second})
	var failure *IrohTransportError
	if !errors.As(err, &failure) || failure.Category != IrohCategoryTimeout || failure.DispatchCertainty != IrohNotSent {
		t.Fatalf("unexpected timeout: %#v", err)
	}
}

func TestNewIrohClientRejectsTcpDialOptions(t *testing.T) {
	_, err := NewIrohClient(context.Background(), "iroh://"+testIrohID, &testIrohDialer{}, IrohClientOptions{}, WithTcpClientProxy("socks5h://127.0.0.1:1055"))
	var failure *IrohTransportError
	if !errors.As(err, &failure) || failure.Category != IrohCategoryInvalidInput {
		t.Fatalf("unexpected option error: %#v", err)
	}
	_, err = NewIrohClient(context.Background(), "iroh://"+testIrohID, &testIrohDialer{},
		IrohClientOptions{}, WithTcpClientConnectTimeout(time.Second))
	if !errors.As(err, &failure) || failure.Category != IrohCategoryInvalidInput {
		t.Fatalf("unexpected connect-option error: %#v", err)
	}
}
