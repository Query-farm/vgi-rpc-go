// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Command vgi-rpc-negotiating-client is the conformance_negotiating_client
// hook of the shared suite (PROTOCOL_VERSIONS_V1 §9, TestClientNegotiation):
// an HTTP client of this port bound to one protocol, declaring a newest
// version and a supported window, negotiating per §6.
//
//	vgi-rpc-negotiating-client <base-url> <protocol> <version> <version-range>
//
// Each line "ping" on stdin makes one call of the protocol's ping() method
// (no arguments, utf8 result) on the same client, so negotiation state carries
// across calls, and prints one JSON line: {"result": "..."} on success, or
// {"error_kind", "error_code", "error_message"} on failure. EOF exits.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type reply struct {
	Result       *string `json:"result,omitempty"`
	ErrorKind    string  `json:"error_kind,omitempty"`
	ErrorCode    string  `json:"error_code,omitempty"`
	ErrorMessage string  `json:"error_message,omitempty"`
}

func main() {
	if len(os.Args) != 5 {
		fmt.Fprintln(os.Stderr, "usage: vgi-rpc-negotiating-client <base-url> <protocol> <version> <version-range>")
		os.Exit(2)
	}
	baseURL, protocol, version, versionRange := os.Args[1], os.Args[2], os.Args[3], os.Args[4]
	client, err := vgirpc.NewHttpClient(baseURL,
		vgirpc.WithClientProtocol(protocol),
		vgirpc.WithClientProtocolVersion(version),
		vgirpc.WithClientProtocolVersionRange(versionRange),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer client.Close()

	out := json.NewEncoder(os.Stdout)
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) != "ping" {
			_ = out.Encode(reply{ErrorKind: "driver_error", ErrorMessage: "unknown command " + scanner.Text()})
			continue
		}
		_ = out.Encode(ping(client))
	}
}

func ping(client *vgirpc.HttpClient) reply {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	params := array.NewRecordBatch(arrow.NewSchema(nil, nil), nil, 0)
	defer params.Release()
	result, err := client.CallUnary(ctx, "ping", params, nil)
	if err != nil {
		return errorReply(err)
	}
	defer result.Release()
	batch := result.Batch
	if batch.NumCols() != 1 || batch.NumRows() != 1 {
		return reply{ErrorKind: "driver_error", ErrorMessage: fmt.Sprintf(
			"ping returned %d columns x %d rows", batch.NumCols(), batch.NumRows())}
	}
	col, ok := batch.Column(0).(*array.String)
	if !ok {
		return reply{ErrorKind: "driver_error", ErrorMessage: "ping result is not utf8: " + batch.Column(0).DataType().String()}
	}
	value := col.Value(0)
	return reply{Result: &value}
}

func errorReply(err error) reply {
	out := reply{ErrorMessage: err.Error()}
	var rpcErr *vgirpc.RpcError
	if errors.As(err, &rpcErr) {
		out.ErrorMessage = rpcErr.Message
		out.ErrorKind = rpcErr.Kind
		out.ErrorCode = rpcErr.Code
		return out
	}
	var noCommon *vgirpc.NoCommonVersionError
	if errors.As(err, &noCommon) {
		out.ErrorKind = noCommon.ErrorKind()
		out.ErrorCode = string(noCommon.ErrorCode())
		return out
	}
	return out
}
