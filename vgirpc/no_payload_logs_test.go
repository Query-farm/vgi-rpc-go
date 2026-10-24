// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// No payload value reaches a log, at any level.
//
// The access log used to carry the whole request (request_data, base64 Arrow
// IPC) when its debug switch was on. vgi-rpc cannot know which parameters are
// secret -- a VGI catalog_attach carries API keys and passwords in its options
// -- so that was a credential leak waiting for someone to turn verbose logging
// on. These tests put a sentinel secret in a request argument and in stream
// state, run calls with every log this package writes at its most verbose,
// and assert the sentinel appears nowhere in the captured output: not raw,
// and not in any of the three base64 alignments it could take inside an
// encoded blob.

package vgirpc

import (
	"bytes"
	"context"
	"encoding/base64"
	"log/slog"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

const logSentinel = "SENTINEL-sk_live_7f3a9c1e5b2d4086"

type secretParams struct {
	Secret string `vgirpc:"secret"`
}

// secretExchangeState carries the sentinel across turns, so over HTTP it
// rides every sealed state token.
type secretExchangeState struct {
	Secret string
	Turns  int64
}

func (s *secretExchangeState) Exchange(_ context.Context, input arrow.RecordBatch, out *OutputCollector, _ *CallContext) error {
	s.Turns++
	return out.EmitMap(map[string][]interface{}{"value": {s.Turns}})
}

func newSecretServer(hook DispatchHook) *Server {
	RegisterStateType(&secretExchangeState{})
	s := NewServer()
	Unary(s, "echo_secret", func(_ context.Context, _ *CallContext, p secretParams) (string, error) {
		return "ok", nil
	})
	Exchange(s, "secret_exchange", regressionSchema, regressionSchema,
		func(_ context.Context, _ *CallContext, p secretParams) (*StreamResult, error) {
			return &StreamResult{OutputSchema: regressionSchema, State: &secretExchangeState{Secret: p.Secret}}, nil
		})
	s.SetDispatchHook(hook)
	return s
}

// captureDebugLogs routes the default slog logger to buf at its most verbose
// level for the duration of the test.
func captureDebugLogs(t *testing.T, buf *bytes.Buffer) {
	t.Helper()
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.Level(-100)})))
	t.Cleanup(func() { slog.SetDefault(prev) })
}

// assertNoSentinel fails when the sentinel, or any base64 alignment of it,
// appears in out.
func assertNoSentinel(t *testing.T, what, out string) {
	t.Helper()
	if strings.Contains(out, logSentinel) {
		t.Fatalf("%s carries the sentinel secret in plaintext:\n%s", what, out)
	}
	for shift := 0; shift < 3; shift++ {
		enc := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", shift) + logSentinel))
		// The first group of four holds the prefix bytes and the last may
		// hold padding or what follows; everything between is the sentinel's.
		core := enc[4 : len(enc)-4]
		if strings.Contains(out, core) {
			t.Fatalf("%s carries the sentinel secret base64-encoded (alignment %d):\n%s", what, shift, out)
		}
	}
}

func TestNoPayloadInLogsHTTP(t *testing.T) {
	var accessLog, debugLog bytes.Buffer
	captureDebugLogs(t, &debugLog)
	h := NewHttpServer(newSecretServer(NewAccessLogHook(&accessLog, "test")))
	ts := httptest.NewServer(h)
	defer ts.Close()
	client, err := NewHttpClient(ts.URL, WithClientProtocol(testProtocol))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	params := buildParamsBatch(t, secretParams{Secret: logSentinel})
	defer params.Release()
	schema, err := resultSchema(reflect.TypeOf(""))
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.CallUnary(context.Background(), "echo_secret", params, schema)
	if err != nil {
		t.Fatalf("unary: %v", err)
	}
	result.Release()

	stream, err := client.OpenExchange(context.Background(), "secret_exchange", params,
		ClientStreamSchema{Input: regressionSchema, Output: regressionSchema})
	if err != nil {
		t.Fatalf("open exchange: %v", err)
	}
	for i := int64(1); i <= 3; i++ {
		in := regressionBatch(t, i)
		out, xerr := stream.Exchange(context.Background(), in)
		in.Release()
		if xerr != nil {
			t.Fatalf("exchange turn %d: %v", i, xerr)
		}
		if got := out.Batch.Column(0).(*array.Int64).Value(0); got != i {
			t.Fatalf("turn %d: state did not carry across turns (got %d)", i, got)
		}
		out.Release()
	}
	stream.Close()

	records := strings.Count(accessLog.String(), "\n")
	if records < 3 {
		t.Fatalf("expected access-log records for the unary call and every exchange turn, got %d:\n%s", records, accessLog.String())
	}
	if !strings.Contains(accessLog.String(), `"request_fields":[{"name":"secret","type":"utf8"}]`) {
		t.Fatalf("the request's shape is missing from the access log:\n%s", accessLog.String())
	}
	assertNoSentinel(t, "the access log", accessLog.String())
	assertNoSentinel(t, "the debug log", debugLog.String())
}

func TestNoPayloadInLogsPipe(t *testing.T) {
	var accessLog, debugLog bytes.Buffer
	captureDebugLogs(t, &debugLog)
	s := newSecretServer(NewAccessLogHook(&accessLog, "test"))

	params := buildParamsBatch(t, secretParams{Secret: logSentinel})
	request := append([]byte(nil), regressionRequest(t, "echo_secret", params)...)
	params.Release()

	var response bytes.Buffer
	if err := s.serveOne(context.Background(), bytes.NewReader(request), &response, &shmConnState{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(accessLog.String(), `"request_rows":1`) {
		t.Fatalf("the request's shape is missing from the access log:\n%s", accessLog.String())
	}
	assertNoSentinel(t, "the access log", accessLog.String())
	assertNoSentinel(t, "the debug log", debugLog.String())
}
