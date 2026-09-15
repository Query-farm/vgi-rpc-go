// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// The namespaced route shape, {prefix}/{protocol}/{method}, and the rules that
// keep the path honest about what the worker will dispatch.
//
// The path is a projection of the canonical vgi_rpc.protocol metadata field.
// Its whole reason to exist is that an edge device can route and apply policy
// on it without an Arrow parser -- which is only true if the projection is
// faithful, so every way the two carriers can disagree is a rejection here.

type nsEchoParams struct {
	Value string `vgirpc:"value"`
}

type nsCountState struct {
	Remaining int
}

func (s *nsCountState) Exchange(_ context.Context, in arrow.RecordBatch, out *OutputCollector, _ *CallContext) error {
	s.Remaining--
	builder := array.NewInt64Builder(defaultAllocator())
	defer builder.Release()
	builder.Append(int64(in.NumRows()))
	arr := builder.NewInt64Array()
	defer arr.Release()
	return out.EmitArrays([]arrow.Array{arr}, 1)
}

var nsCountSchema = arrow.NewSchema([]arrow.Field{{Name: "rows", Type: arrow.PrimitiveTypes.Int64}}, nil)

// newNamespacedServer hosts all three protocols a co-hosting server can carry:
// the application's own, vgi_rpc.Reflection.v1, and vgi_rpc.Identity.v1.
func newNamespacedServer(t *testing.T) *Server {
	t.Helper()
	RegisterStateType(&nsCountState{})
	s := NewServer()
	s.SetServiceName("demo.App.v1")
	Unary(s, "echo", func(_ context.Context, _ *CallContext, p nsEchoParams) (string, error) {
		return p.Value, nil
	})
	Exchange(s, "count", nsCountSchema, nsCountSchema,
		func(context.Context, *CallContext, struct{}) (*StreamResult, error) {
			return &StreamResult{
				OutputSchema: nsCountSchema,
				InputSchema:  nsCountSchema,
				State:        &nsCountState{Remaining: 4},
			}, nil
		})
	if err := RegisterReflection(s); err != nil {
		t.Fatalf("RegisterReflection: %v", err)
	}
	impl, err := NewIdentity(IdentityConfig{
		MintGrant: func(principal, purpose string, scopes []string, ttlSeconds int64) (IssuedGrant, error) {
			_, _ = purpose, scopes
			return IssuedGrant{
				Token:     "grant-for-" + principal,
				GrantID:   "gid-1",
				ExpiresAt: float64(time.Now().Add(time.Hour).Unix()),
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewIdentity: %v", err)
	}
	if err := RegisterIdentity(s, impl); err != nil {
		t.Fatalf("RegisterIdentity: %v", err)
	}
	return s
}

// nsPost frames a unary request and POSTs it at an arbitrary path, so a test
// can put something on the path that no client would ever build.
func nsPost(t *testing.T, h *HttpServer, path, protocol, method string, params any) *httptest.ResponseRecorder {
	t.Helper()
	body := encodeRequestBodyFor(t, protocol, method, params)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", arrowContentType)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// nsErrorKind returns the vgi_rpc.error_kind of the first EXCEPTION batch in an
// IPC response, or "" when the response carries none.
func nsErrorKind(t *testing.T, body []byte) string {
	t.Helper()
	r, err := ipc.NewReader(bytes.NewReader(body))
	if err != nil {
		return ""
	}
	defer r.Release()
	for r.Next() {
		rb, ok := r.RecordBatch().(arrow.RecordBatchWithMetadata)
		if !ok {
			continue
		}
		md := rb.Metadata()
		if level, _ := md.GetValue(MetaLogLevel); level != string(LogException) {
			continue
		}
		kind, _ := md.GetValue(MetaErrorKind)
		return kind
	}
	return ""
}

// nsEchoedValue returns the string demo.App.v1's echo answered with, or "" if
// the response carries no such result.
func nsEchoedValue(t *testing.T, body []byte) string {
	t.Helper()
	r, err := ipc.NewReader(bytes.NewReader(body))
	if err != nil {
		return ""
	}
	defer r.Release()
	for r.Next() {
		batch := r.RecordBatch()
		if batch.NumRows() == 0 {
			continue
		}
		indices := batch.Schema().FieldIndices("result")
		if len(indices) == 0 {
			continue
		}
		if col, ok := batch.Column(indices[0]).(*array.String); ok && col.Len() > 0 {
			return col.Value(0)
		}
	}
	return ""
}

// Every co-hosted protocol is reachable over HTTP at its own namespaced path.
// Before this, only the primary application protocol had a route at all, which
// left reflection and identity raw-transport-only -- and identity's
// issue_grant needs an auth_time claim, which only ever arrives on an
// OIDC/JWT credential, i.e. over HTTP. The protocol was reachable only on the
// transports where it must refuse by design.
func TestNamespacedRoutesReachEveryHostedProtocol(t *testing.T) {
	h := NewHttpServer(newNamespacedServer(t))
	for _, tc := range []struct {
		name     string
		protocol string
		method   string
		params   any
	}{
		{"application", "demo.App.v1", "echo", nsEchoParams{Value: "hi"}},
		{"reflection", ReflectionProtocolName, "list_protocols", struct{}{}},
		{"identity", IdentityProtocolName, "issue_grant", issueGrantParams{Purpose: "backup", TTLSeconds: 60}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := nsPost(t, h, "/"+tc.protocol+"/"+tc.method, tc.protocol, tc.method, tc.params)
			if rec.Code == http.StatusNotFound {
				t.Fatalf("%s/%s is not routed over HTTP", tc.protocol, tc.method)
			}
			if kind := nsErrorKind(t, rec.Body.Bytes()); kind == "protocol_not_supported" || kind == "method_not_implemented" {
				t.Fatalf("%s/%s answered %s", tc.protocol, tc.method, kind)
			}
		})
	}
}

// The path resolved the call; the canonical metadata field has to say the same.
// Left unchecked, edge policy applies to one protocol while the worker
// dispatches another: the Content-Length/Transfer-Encoding shape.
func TestPathAndMetadataProtocolMustAgree(t *testing.T) {
	h := NewHttpServer(newNamespacedServer(t))
	// The path names the application protocol; the metadata names reflection.
	// Both exist, so this cannot be mistaken for an unknown-protocol failure.
	rec := nsPost(t, h, "/demo.App.v1/echo", ReflectionProtocolName, "echo", nsEchoParams{Value: "hi"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for disagreeing carriers", rec.Code)
	}
	if kind := nsErrorKind(t, rec.Body.Bytes()); kind != "protocol_not_supported" {
		t.Fatalf("error_kind = %q, want protocol_not_supported", kind)
	}
}

// Accepted on HTTP, where the path segment already resolved the binding.
//
// This test asserted the opposite until IDENTITY_V1_SPEC.md §5c settled it, and
// the reversal is worth recording rather than quietly editing: "vgi_rpc.protocol
// is required on every request" was the original brief, two ports implemented it
// strictly, and one became unshippable -- the shared conformance harness's
// recovery probe (_adversarial_http.py) requires a 200 here, because a client
// that omits the field on HTTP is conformant and a worker that refuses it is
// not. The rule now splits by carrier: required where the metadata field is the
// only one (see Server.resolve), optional where the path is a second.
//
// The cost is stated in checkProtocolCarriage: a path rewrite by an intermediary
// is no longer detectable for a request that omits the key, because an
// intermediary that rewrites the path cannot reach into the Arrow body to match
// it. Deliberate, and the narrower gap of the two.
func TestAbsentProtocolMetadataIsAcceptedOverHTTP(t *testing.T) {
	h := NewHttpServer(newNamespacedServer(t))
	batch := buildParamsBatch(t, nsEchoParams{Value: "hi"})
	defer batch.Release()
	var buf bytes.Buffer
	if err := WriteRequest(&buf, "echo", batch, "", ""); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/demo.App.v1/echo", bytes.NewReader(buf.Bytes()))
	req.Header.Set("Content-Type", arrowContentType)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (error_kind %q), want 200: the path segment is the only "+
			"carrier this request used, and it is the one that resolved the binding",
			rec.Code, nsErrorKind(t, rec.Body.Bytes()))
	}
	// Dispatched, not merely admitted, and dispatched to the protocol the PATH
	// named. A status-only check passes against a 200 carrying an EXCEPTION
	// batch, and an echo that answers is the only thing that shows the request
	// reached demo.App.v1's handler rather than being admitted and dropped.
	if got := nsEchoedValue(t, rec.Body.Bytes()); got != "hi" {
		t.Fatalf("echoed %q, want \"hi\" (error_kind %q): admitted but never dispatched",
			got, nsErrorKind(t, rec.Body.Bytes()))
	}
}

// The raw transports keep the strict rule, because there the metadata field is
// the only carrier and absent really is unroutable.
//
// Stated as its own case rather than left to the HTTP one's absence: the two
// halves of §5c are a split, and a port that relaxed both would pass every HTTP
// assertion above while landing a fieldless stdio request on whichever protocol
// happened to be registered first.
func TestAbsentProtocolMetadataIsRefusedOnRawTransports(t *testing.T) {
	s := newNamespacedServer(t)
	batch := buildParamsBatch(t, nsEchoParams{Value: "hi"})
	defer batch.Release()
	var request bytes.Buffer
	if err := WriteRequest(&request, "echo", batch, "", ""); err != nil {
		t.Fatal(err)
	}
	var response bytes.Buffer
	if err := s.serveOne(context.Background(), bytes.NewReader(request.Bytes()), &response, &shmConnState{}); err != nil {
		t.Fatal(err)
	}
	if kind := nsErrorKind(t, response.Bytes()); kind != "protocol_not_specified" {
		t.Fatalf("error_kind = %q, want protocol_not_specified -- on a raw transport "+
			"vgi_rpc.protocol is the only carrier there is", kind)
	}
}

// The protocol charset never requires percent-encoding, so a percent sign is a
// bug or an attempt to have the edge and the worker read different strings.
// The segment is compared raw and never decoded: if the server decoded
// "demo%2EApp%2Ev1" it would route the request the edge saw as a different
// path, which is precisely the split-view this rule closes.
//
// DO NOT "simplify" the implementation to read r.PathValue("protocol"). This
// test exists because that is exactly what the first version did, and it is
// wrong in a way that is invisible: net/http's ServeMux percent-decodes every
// path segment BEFORE the handler runs, so PathValue can never contain a
// percent sign. A server checking PathValue has a `%` ban that cannot fire,
// while the edge in front of it matched the escaped string -- the
// Content-Length/Transfer-Encoding shape, one request read two ways, shipped
// green. The check must read r.URL.EscapedPath(); see rawProtocolSegment in
// http_routing.go, and TestPercentEscapedHostedProtocolDoesNotResolve below,
// which fails against a PathValue implementation.
func TestPercentInProtocolSegmentIsRefusedWithoutDecoding(t *testing.T) {
	h := NewHttpServer(newNamespacedServer(t))
	for _, raw := range []string{"demo%2EApp%2Ev1", "demo.App.v1%00", "%64emo.App.v1"} {
		t.Run(raw, func(t *testing.T) {
			// httptest.NewRequest preserves RawPath, and Go's mux hands the
			// wildcard its DECODED form -- so the check has to run on a value
			// that still carries the percent sign, which is what this asserts.
			body := encodeRequestBodyFor(t, "demo.App.v1", "echo", nsEchoParams{Value: "hi"})
			req := httptest.NewRequest(http.MethodPost, "/"+raw+"/echo", bytes.NewReader(body))
			req.Header.Set("Content-Type", arrowContentType)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404 for a percent-encoded protocol segment", rec.Code)
			}
			if kind := nsErrorKind(t, rec.Body.Bytes()); kind != "protocol_not_specified" {
				t.Fatalf("error_kind = %q, want protocol_not_specified", kind)
			}
		})
	}
}

// A percent-escaped path must never decode into a protocol this server hosts.
//
// This is the concrete attack the raw comparison closes, and it is asserted
// separately from the generic refusal above because it is the one case where a
// status code cannot tell you what happened: "demo%2EApp%2Ev1" decodes to a
// protocol this server really does host, so a PathValue implementation answers
// 200 and dispatches. The `called` flag, not the status, is what makes that
// visible.
//
// Measured, not assumed: rewriting rawProtocolSegment to return
// r.PathValue("protocol") turns both this test and the one above red -- above,
// because "%64emo.App.v1" decodes to a hosted name and routes while
// "demo.App.v1%00" decodes to a name that fails the grammar and so reports
// protocol_not_supported instead of the percent refusal.
func TestPercentEscapedHostedProtocolDoesNotResolve(t *testing.T) {
	h := NewHttpServer(newNamespacedServer(t))
	var called bool
	s := h.server
	Unary(s, "watch", func(context.Context, *CallContext, struct{}) (string, error) {
		called = true
		return "", nil
	})
	body := encodeRequestBodyFor(t, "demo.App.v1", "watch", struct{}{})
	req := httptest.NewRequest(http.MethodPost, "/demo%2EApp%2Ev1/watch", bytes.NewReader(body))
	req.Header.Set("Content-Type", arrowContentType)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if called {
		t.Fatal("a percent-escaped protocol segment was decoded and dispatched")
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

// Unknown protocol is 404, matching unknown-method and gRPC's use of
// UNIMPLEMENTED for both. The two stay distinguishable by error_kind, because
// a client probing for an optional method has to tell "you do not speak this
// protocol" from "you speak it but lack this method".
func TestUnknownProtocolIs404AndDistinctFromUnknownMethod(t *testing.T) {
	h := NewHttpServer(newNamespacedServer(t))

	rec := nsPost(t, h, "/other.App.v1/echo", "other.App.v1", "echo", nsEchoParams{Value: "hi"})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown protocol status = %d, want 404", rec.Code)
	}
	if kind := nsErrorKind(t, rec.Body.Bytes()); kind != "protocol_not_supported" {
		t.Fatalf("unknown protocol error_kind = %q, want protocol_not_supported", kind)
	}

	rec = nsPost(t, h, "/demo.App.v1/nope", "demo.App.v1", "nope", struct{}{})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown method status = %d, want 404", rec.Code)
	}
	// Same status, different kind -- which is the whole point: a client probing
	// for an optional method reads the kind, not the status line.
	if kind := nsErrorKind(t, rec.Body.Bytes()); kind != "MethodNotImplementedError" {
		t.Fatalf("unknown method error_kind = %q, want MethodNotImplementedError", kind)
	}
}

// A name that cannot match the grammar is rejected before it is looked up, so a
// request-supplied path segment never reaches an error message, a log field or
// a metric label.
func TestUngrammaticalProtocolSegmentIsRefusedBeforeLookup(t *testing.T) {
	h := NewHttpServer(newNamespacedServer(t))
	rec := nsPost(t, h, "/1nvalid-name/echo", "demo.App.v1", "echo", nsEchoParams{Value: "hi"})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if kind := nsErrorKind(t, rec.Body.Bytes()); kind != "protocol_not_supported" {
		t.Fatalf("error_kind = %q, want protocol_not_supported", kind)
	}
}

// The reserved, server-level route belongs to no protocol, so it stays flat.
// The flat route is not a catch-all: anything that is not a reserved name 404s
// there rather than falling through to a protocol lookup.
func TestReservedRoutesStayFlatAndAreNotACatchAll(t *testing.T) {
	h := NewHttpServer(newNamespacedServer(t))
	h.InitPages()

	// __describe__ is the name the flat route still recognises -- to refuse it.
	// It reaching the reserved handler at all is what this asserts: a bare
	// method name, by contrast, must not.
	describeBody := encodeRequestBodyFor(t, "demo.App.v1", retiredDescribeMethod, struct{}{})
	req := httptest.NewRequest(http.MethodPost, "/"+retiredDescribeMethod, bytes.NewReader(describeBody))
	req.Header.Set("Content-Type", arrowContentType)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("%s status = %d, want 404 -- it is retired: %s",
			retiredDescribeMethod, rec.Code, rec.Body.String())
	}
	if kind := nsErrorKind(t, rec.Body.Bytes()); kind != "MethodNotImplementedError" {
		t.Fatalf("error_kind = %q, want MethodNotImplementedError", kind)
	}

	req = httptest.NewRequest(http.MethodPost, "/echo", bytes.NewReader(describeBody))
	req.Header.Set("Content-Type", arrowContentType)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("bare method status = %d, want 404 -- the flat route is not a catch-all", rec.Code)
	}
}

// issue_grant is the whole reason this work exists: it requires an auth_time
// claim, which only arrives on an OIDC/JWT credential, i.e. over HTTP. Peer
// identity on TCP or unix authenticates a principal but carries no auth_time,
// so before the namespaced routes the method was reachable only on the
// transports where it must refuse by design.
func TestIdentityIssueGrantSucceedsOverHTTPWithAuthTime(t *testing.T) {
	h := NewHttpServer(newNamespacedServer(t))
	h.SetAuthenticate(func(*http.Request) (*AuthContext, error) {
		return &AuthContext{
			Domain:        "jwt",
			Authenticated: true,
			Principal:     "alice@example.test",
			Claims: map[string]any{
				"sub":       "alice@example.test",
				"iss":       "https://idp.example.test",
				"auth_time": float64(time.Now().Add(-30 * time.Second).Unix()),
			},
		}, nil
	})

	rec := nsPost(t, h, "/"+IdentityProtocolName+"/issue_grant",
		IdentityProtocolName, "issue_grant",
		issueGrantParams{Purpose: "nightly-backup", Scopes: []string{"read"}, TTLSeconds: 3600})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	if kind := nsErrorKind(t, rec.Body.Bytes()); kind != "" {
		t.Fatalf("issue_grant refused with %s: %s", kind, rec.Body.String())
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte("grant-for-alice@example.test")) {
		t.Fatalf("response does not carry the minted grant: %q", rec.Body.String())
	}
}

// The same credential without auth_time must still be refused, so the test
// above is measuring the claim and not merely the route.
func TestIdentityIssueGrantOverHTTPStillNeedsAuthTime(t *testing.T) {
	h := NewHttpServer(newNamespacedServer(t))
	h.SetAuthenticate(func(*http.Request) (*AuthContext, error) {
		return &AuthContext{Domain: "bearer", Authenticated: true, Principal: "alice@example.test"}, nil
	})
	rec := nsPost(t, h, "/"+IdentityProtocolName+"/issue_grant",
		IdentityProtocolName, "issue_grant", issueGrantParams{Purpose: "backup", TTLSeconds: 60})
	if kind := nsErrorKind(t, rec.Body.Bytes()); kind != "stale_auth" {
		t.Fatalf("error_kind = %q, want stale_auth", kind)
	}
}

// A continuation must stay on the protocol its stream started on. The protocol
// is bound into the cursor and call tokens' AEAD associated data rather than
// compared in application code, so a cross-protocol continuation fails the tag
// check -- which also covers the call-state cache-hit path, where the call
// token is never opened at all. For a stream, continuations are most of the
// requests, so without this edge visibility silently does not hold.
func TestCrossProtocolContinuationIsRejectedAsAnInvalidToken(t *testing.T) {
	s := newNamespacedServer(t)
	// A second application protocol declaring the same method name, so the
	// replayed path resolves and the refusal can only come from the token.
	sub := NewServer()
	sub.SetServiceName("other.App.v1")
	Exchange(sub, "count", nsCountSchema, nsCountSchema,
		func(context.Context, *CallContext, struct{}) (*StreamResult, error) {
			return &StreamResult{OutputSchema: nsCountSchema, InputSchema: nsCountSchema, State: &nsCountState{Remaining: 4}}, nil
		})
	if err := s.AddProtocol(&protocolBinding{Name: "other.App.v1", Methods: sub.methods, Impl: sub}, false); err != nil {
		t.Fatal(err)
	}
	h := NewHttpServer(s)

	params := emptyBatch(arrow.NewSchema(nil, nil))
	defer params.Release()
	var initBody bytes.Buffer
	if err := WriteRequest(&initBody, "count", params, "demo.App.v1", ""); err != nil {
		t.Fatal(err)
	}
	initReq := httptest.NewRequest(http.MethodPost, "/demo.App.v1/count/init", bytes.NewReader(initBody.Bytes()))
	initReq.Header.Set("Content-Type", arrowContentType)
	initRec := httptest.NewRecorder()
	h.ServeHTTP(initRec, initReq)
	if initRec.Code != http.StatusOK {
		t.Fatalf("init status = %d: %s", initRec.Code, initRec.Body.String())
	}
	cursor, callToken := FindStreamTokens(initRec.Body.Bytes())
	if cursor == nil || callToken == nil {
		t.Fatal("init response carried no stream tokens")
	}

	input := emptyBatch(nsCountSchema)
	defer input.Release()
	withMeta := array.NewRecordBatchWithMetadata(input.Schema(), input.Columns(), input.NumRows(),
		arrow.NewMetadata([]string{MetaStreamState, MetaCallState}, []string{string(cursor), string(callToken)}))
	defer withMeta.Release()
	var exBody bytes.Buffer
	writer := ipc.NewWriter(&exBody, ipc.WithSchema(nsCountSchema))
	if err := writer.Write(withMeta); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	// Same tokens, same method name, a protocol that genuinely hosts it.
	exReq := httptest.NewRequest(http.MethodPost, "/other.App.v1/count/exchange", bytes.NewReader(exBody.Bytes()))
	exReq.Header.Set("Content-Type", arrowContentType)
	exRec := httptest.NewRecorder()
	h.ServeHTTP(exRec, exReq)
	if exRec.Code != http.StatusBadRequest {
		t.Fatalf("cross-protocol continuation status = %d, want 400: %s", exRec.Code, exRec.Body.String())
	}
	if !strings.Contains(exRec.Body.String(), "signature verification failed") {
		t.Fatalf("cross-protocol continuation was not refused as an invalid token: %s", exRec.Body.String())
	}

	// The same continuation on its own protocol still works, so the refusal
	// above is the protocol binding and not a broken token.
	okReq := httptest.NewRequest(http.MethodPost, "/demo.App.v1/count/exchange", bytes.NewReader(exBody.Bytes()))
	okReq.Header.Set("Content-Type", arrowContentType)
	okRec := httptest.NewRecorder()
	h.ServeHTTP(okRec, okReq)
	if okRec.Code != http.StatusOK {
		t.Fatalf("same-protocol continuation status = %d: %s", okRec.Code, okRec.Body.String())
	}
}

// The native client builds the namespaced path and stamps the routing key, so
// a round trip through it exercises both carriers end to end.
func TestNativeClientDrivesNamespacedRoutes(t *testing.T) {
	h := NewHttpServer(newNamespacedServer(t))
	ts := httptest.NewServer(h)
	defer ts.Close()

	client, err := NewHttpClient(ts.URL, WithClientProtocol("demo.App.v1"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	params := buildParamsBatch(t, nsEchoParams{Value: "round-trip"})
	defer params.Release()
	schema, err := resultSchema(reflect.TypeOf(""))
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.CallUnary(context.Background(), "echo", params, schema)
	if err != nil {
		t.Fatalf("namespaced unary call: %v", err)
	}
	defer result.Release()
	if got := result.Batch.Column(0).(*array.String).Value(0); got != "round-trip" {
		t.Fatalf("echo = %q, want round-trip", got)
	}

	// A client pointed at a protocol this server does not host is refused by
	// the route, not by the body -- the answer a proxy can act on.
	other, err := NewHttpClient(ts.URL, WithClientProtocol("other.App.v1"))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.CallUnary(context.Background(), "echo", params, schema); err == nil {
		t.Fatal("a call to an unhosted protocol succeeded")
	}
}

// A client with no protocol has nothing to put in the path and would be refused
// on arrival, so it is refused at construction where the message names the fix.
func TestHttpClientRequiresAProtocol(t *testing.T) {
	if _, err := NewHttpClient("http://example.test"); err == nil {
		t.Fatal("a client with no routing key was constructed")
	}
}

// RPC endpoints are POST-only, and a two-segment GET whose first segment cannot
// be a hosted protocol was never an RPC endpoint -- so it is 404, not 405.
// Without the distinction any unrelated two-segment path, a /.well-known/...
// document among them, matches the wildcard route and is answered "method not
// allowed" by it.
func TestGetOnTheRpcRouteIs404UnlessTheProtocolIsHosted(t *testing.T) {
	h := NewHttpServer(newNamespacedServer(t))
	h.SetEnableNotFoundPage(false)
	h.InitPages()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/demo.App.v1/echo", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET on a hosted protocol = %d, want 405", rec.Code)
	}
	if got := rec.Header().Get("Allow"); got != "POST" {
		t.Fatalf("Allow = %q, want POST", got)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/.well-known/something", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET on a non-protocol two-segment path = %d, want 404", rec.Code)
	}
}
