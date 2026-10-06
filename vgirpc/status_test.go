// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// Ports of the reference's tests/test_error_model.py: the parts of the error
// model that are pure functions (encoding, the cap, the catalog rules, typed
// decoding) or unexported, and the traceback switch the shared suite cannot
// reconfigure. What reaches
// the wire is asserted by the shared suite's TestErrorModelRoundTrip and
// TestErrorModelOnTheWire, not here.

func TestTheCodeSetIsGrpcsSixteen(t *testing.T) {
	if len(Codes) != 16 {
		t.Fatalf("len(Codes) = %d, want 16", len(Codes))
	}
	if Code("OK").Valid() {
		t.Fatal("OK is not an error code")
	}
	for _, raw := range []string{"", "OK", "unavailable", "14", "NOPE"} {
		if got := ParseCode(raw); got != CodeUnknown {
			t.Errorf("ParseCode(%q) = %q, want UNKNOWN", raw, got)
		}
	}
}

func TestEveryKindDeclaresItsCode(t *testing.T) {
	cases := []struct {
		err  error
		kind string
		code Code
	}{
		{&MethodNotImplementedError{Method: "m"}, "method_not_implemented", CodeUnimplemented},
		{&ProtocolNotSupportedError{}, "protocol_not_supported", CodeUnimplemented},
		{&ProtocolNotSpecifiedError{}, "protocol_not_specified", CodeInvalidArgument},
		{&ProtocolVersionError{}, "protocol_version_mismatch", CodeFailedPrecondition},
		{&SessionLostError{}, "session_lost", CodeAborted},
		{&ServerDrainingError{}, "server_draining", CodeUnavailable},
		{&IdentityUnavailableError{}, "identity_unavailable", CodeUnavailable},
		{&StaleAuthError{}, "stale_auth", CodeUnauthenticated},
		{&IntrospectionRefusedError{}, "introspection_refused", CodePermissionDenied},
		{&GrantRefusedError{}, "grant_refused", CodePermissionDenied},
		{&TokenUnresolvedError{}, "token_unresolved", CodeNotFound},
	}
	for _, tc := range cases {
		if got := ErrorKindOf(tc.err); got != tc.kind {
			t.Errorf("%T kind = %q, want %q", tc.err, got, tc.kind)
		}
		if got := ErrorCodeOf(tc.err); got != tc.code {
			t.Errorf("%T code = %q, want %q", tc.err, got, tc.code)
		}
	}
	if got := ErrorCodeOf(fmt.Errorf("plain")); got != CodeUnknown {
		t.Errorf("unclassified error code = %q, want UNKNOWN", got)
	}
}

// A handler that wraps a typed error must not lose its classification: the
// model is read with errors.As, never a type assertion.
func TestWrappedErrorsKeepTheirModel(t *testing.T) {
	err := fmt.Errorf("lookup: %w", &IdentityUnavailableError{RetryAfter: 9})
	m := errorModelOf(err)
	if m.code != CodeUnavailable || m.kind != "identity_unavailable" {
		t.Fatalf("wrapped model = %q/%q", m.code, m.kind)
	}
	if string(m.details) != `[{"@type":"vgi_rpc.RetryInfo","retry_delay_seconds":9}]` {
		t.Fatalf("wrapped details = %s", m.details)
	}
	extra := buildErrorExtra(err, m, false)
	if !strings.Contains(extra, `"exception_type":"IdentityUnavailableError"`) {
		t.Fatalf("wrapped exception_type lost: %s", extra)
	}
}

var catalogSamples = []struct {
	detail ErrorDetail
	wire   string
}{
	{ErrorInfo{Metadata: map[string]string{"k": "v"}}, `{"@type":"vgi_rpc.ErrorInfo","metadata":{"k":"v"}}`},
	{RetryInfo{RetryDelaySeconds: 7}, `{"@type":"vgi_rpc.RetryInfo","retry_delay_seconds":7}`},
	{RetryInfo{RetryDelaySeconds: 2.5}, `{"@type":"vgi_rpc.RetryInfo","retry_delay_seconds":2.5}`},
	{BadRequest{FieldViolations: []FieldViolation{{Field: "code", Description: "bad"}}},
		`{"@type":"vgi_rpc.BadRequest","field_violations":[{"field":"code","description":"bad"}]}`},
	{PreconditionFailure{Violations: []PreconditionViolation{{Type: "t", Subject: "s", Description: "d"}}},
		`{"@type":"vgi_rpc.PreconditionFailure","violations":[{"type":"t","subject":"s","description":"d"}]}`},
	{QuotaFailure{Violations: []QuotaViolation{{Subject: "s", Description: "d"}}},
		`{"@type":"vgi_rpc.QuotaFailure","violations":[{"subject":"s","description":"d"}]}`},
	{ResourceInfo{ResourceType: "report", ResourceName: "r1", Owner: "o", Description: "d"},
		`{"@type":"vgi_rpc.ResourceInfo","resource_type":"report","resource_name":"r1","owner":"o","description":"d"}`},
	{Help{Links: []HelpLink{{Description: "docs", URL: "https://example.com"}}},
		`{"@type":"vgi_rpc.Help","links":[{"description":"docs","url":"https://example.com"}]}`},
	{LocalizedMessage{Locale: "en-US", Message: "Try later"},
		`{"@type":"vgi_rpc.LocalizedMessage","locale":"en-US","message":"Try later"}`},
}

func TestEachCatalogTypeRoundTrips(t *testing.T) {
	for _, tc := range catalogSamples {
		got, err := tc.detail.MarshalJSON()
		if err != nil || string(got) != tc.wire {
			t.Errorf("%s: MarshalJSON = %s, %v; want %s", tc.detail.DetailType(), got, err, tc.wire)
		}
		decoded := decodeErrorDetails("[" + tc.wire + "]")
		if len(decoded) != 1 {
			t.Fatalf("%s: decoded %d objects", tc.detail.DetailType(), len(decoded))
		}
		if parsed := ParseErrorDetail(decoded[0]); !reflect.DeepEqual(parsed, tc.detail) {
			t.Errorf("%s: ParseErrorDetail = %#v, want %#v", tc.detail.DetailType(), parsed, tc.detail)
		}
	}
}

func TestUnknownOrMalformedDetailsAreIgnored(t *testing.T) {
	for _, raw := range []string{
		`{"@type":"conformance.Secondary.v1.Probe"}`,
		`{"@type":"vgi_rpc.RetryInfo","retry_delay_seconds":"soon"}`,
		`{"@type":"vgi_rpc.RetryInfo","retry_delay_seconds":-1}`,
		`{"@type":"vgi_rpc.ErrorInfo","metadata":{"k":1}}`,
		`{"no":"type"}`,
	} {
		decoded := decodeErrorDetails("[" + raw + "]")
		if len(decoded) != 1 {
			t.Fatalf("%s: the raw object must be kept, got %v", raw, decoded)
		}
		if parsed := ParseErrorDetail(decoded[0]); parsed != nil {
			t.Errorf("%s: typed access returned %#v; it must skip it", raw, parsed)
		}
	}
}

// V5: at the cap it is sent; one byte over and nothing is -- never a prefix.
func TestEncodeDropsTheWholeArrayOverTheCap(t *testing.T) {
	small := RetryInfo{RetryDelaySeconds: 1}
	base, ok := encodeErrorDetails([]ErrorDetail{small, ErrorInfo{Metadata: map[string]string{"p": ""}}})
	if !ok {
		t.Fatal("base array refused")
	}
	room := MaxErrorDetailsBytes - len(base)
	exact := ErrorInfo{Metadata: map[string]string{"p": strings.Repeat("x", room)}}
	fits, ok := encodeErrorDetails([]ErrorDetail{small, exact})
	if !ok || len(fits) != MaxErrorDetailsBytes {
		t.Fatalf("exactly-at-cap array: ok=%v len=%d", ok, len(fits))
	}
	over := ErrorInfo{Metadata: map[string]string{"p": strings.Repeat("x", room+1)}}
	if got, ok := encodeErrorDetails([]ErrorDetail{small, over}); ok {
		t.Fatalf("over-cap array was sent (%d bytes); it must be dropped whole", len(got))
	}
	// The reference vector: one ErrorInfo is 51 + N bytes compactly.
	at := ErrorInfo{Metadata: map[string]string{"p": strings.Repeat("x", 4045)}}
	if got, ok := encodeErrorDetails([]ErrorDetail{at}); !ok || len(got) != 4096 {
		t.Fatalf("N=4045: ok=%v len=%d, want 4096 bytes sent", ok, len(got))
	}
	at.Metadata["p"] += "x"
	if _, ok := encodeErrorDetails([]ErrorDetail{at}); ok {
		t.Fatal("N=4046 must be dropped")
	}
}

func TestTheCapIsMeasuredInUTF8Bytes(t *testing.T) {
	text := strings.Repeat("é", MaxErrorDetailsBytes/2)
	if _, ok := encodeErrorDetails([]ErrorDetail{LocalizedMessage{Locale: "fr", Message: text}}); ok {
		t.Fatal("2048 x 'é' is over the cap in bytes and must be dropped")
	}
}

// V6: rule violations are dropped at emission.
func TestRuleViolationsAreDroppedAtEmission(t *testing.T) {
	for name, details := range map[string][]ErrorDetail{
		"repeated":    {RetryInfo{RetryDelaySeconds: 1}, RetryInfo{RetryDelaySeconds: 2}},
		"made up":     {RawDetail{"@type": "vgi_rpc.Made.Up"}},
		"unqualified": {RawDetail{"@type": "Unqualified"}},
		"no type":     {RawDetail{"note": "no type"}},
		"negative":    {RetryInfo{RetryDelaySeconds: -1}},
	} {
		if got, ok := encodeErrorDetails(details); ok {
			t.Errorf("%s: sent %s", name, got)
		}
		if name != "negative" && ValidateErrorDetails(details) == nil {
			t.Errorf("%s: ValidateErrorDetails accepted it", name)
		}
	}
}

// V7: the client decode is tolerant.
func TestDecodeIsTolerant(t *testing.T) {
	for _, raw := range []string{"", "{", `{"@type":"vgi_rpc.RetryInfo"}`, "7"} {
		if got := decodeErrorDetails(raw); len(got) != 0 {
			t.Errorf("decodeErrorDetails(%q) = %v, want empty", raw, got)
		}
	}
	got := decodeErrorDetails(`[1, {"@type": "x.Y"}]`)
	if !reflect.DeepEqual(got, []map[string]any{{"@type": "x.Y"}}) {
		t.Errorf("non-object elements must be skipped, objects kept: %v", got)
	}
}

func TestRetryability(t *testing.T) {
	retry := map[string]any{"@type": RetryInfoType, "retry_delay_seconds": 1.0}
	for _, tc := range []struct {
		code    string
		details []map[string]any
		want    bool
	}{
		{"UNAVAILABLE", nil, true},
		{"RESOURCE_EXHAUSTED", []map[string]any{retry}, true},
		{"RESOURCE_EXHAUSTED", nil, false},
		{"ABORTED", []map[string]any{retry}, false},
		{"INTERNAL", []map[string]any{retry}, false},
		{"UNKNOWN", nil, false},
		{"", nil, false},
		{"bogus", nil, false},
	} {
		if got := (&RpcError{Code: tc.code, Details: tc.details}).IsRetryable(); got != tc.want {
			t.Errorf("%q with %d details: IsRetryable = %v, want %v", tc.code, len(tc.details), got, tc.want)
		}
	}
}

func decodeExceptionMetadata(t *testing.T, err error, includeTraceback bool) map[string]string {
	t.Helper()
	var buf bytes.Buffer
	schema := arrow.NewSchema(nil, nil)
	if werr := writeErrorResponse(&buf, schema, err, "srv", "", includeTraceback); werr != nil {
		t.Fatal(werr)
	}
	r, rerr := ipc.NewReader(&buf)
	if rerr != nil {
		t.Fatal(rerr)
	}
	defer r.Release()
	if !r.Next() {
		t.Fatal("no batch")
	}
	return recordMetadata(r.RecordBatch())
}

// The same array rides top-level and in log_extra, and the client reads it back.
func TestDetailsRideTopLevelAndInLogExtra(t *testing.T) {
	err := &StatusError{Code: CodeUnavailable, Kind: "down", Message: "x",
		Details: []ErrorDetail{RetryInfo{RetryDelaySeconds: 3}}}
	md := decodeExceptionMetadata(t, err, false)
	want := `[{"@type":"vgi_rpc.RetryInfo","retry_delay_seconds":3}]`
	if md[MetaErrorDetails] != want || md[MetaErrorCode] != "UNAVAILABLE" || md[MetaErrorKind] != "down" {
		t.Fatalf("top-level = %v", md)
	}
	var extra map[string]json.RawMessage
	if jerr := json.Unmarshal([]byte(md[MetaLogExtra]), &extra); jerr != nil {
		t.Fatal(jerr)
	}
	if string(extra["error_details"]) != want || string(extra["error_code"]) != `"UNAVAILABLE"` {
		t.Fatalf("log_extra = %s", md[MetaLogExtra])
	}
	for _, k := range []string{"traceback", "frames"} {
		if _, present := extra[k]; present {
			t.Errorf("log_extra carries %q with tracebacks omitted", k)
		}
	}
	client := rpcErrorFromMetadata(md)
	if client.Code != "UNAVAILABLE" || client.Kind != "down" || !client.IsRetryable() {
		t.Fatalf("client error = %+v", client)
	}
	if ri, ok := client.RetryInfo(); !ok || ri.RetryDelaySeconds != 3 {
		t.Fatalf("RetryInfo = %v, %v", ri, ok)
	}
}

// The client falls back to the log_extra mirror when a top-level key is absent,
// and reports "" -- not UNKNOWN -- when both are.
func TestClientReadsTheMirrorAndNeverInventsACode(t *testing.T) {
	md := map[string]string{
		MetaLogMessage: "m",
		MetaLogExtra:   `{"exception_type":"E","error_code":"ABORTED","error_kind":"k","error_details":[{"@type":"x.Y"}]}`,
	}
	got := rpcErrorFromMetadata(md)
	if got.Code != "ABORTED" || got.Kind != "k" || len(got.Details) != 1 {
		t.Fatalf("mirror fallback = %+v", got)
	}
	bare := rpcErrorFromMetadata(map[string]string{MetaLogMessage: "m", MetaLogExtra: `{"exception_type":"E"}`})
	if bare.Code != "" || bare.Kind != "" || bare.Details != nil {
		t.Fatalf("a pre-model server's error must decode with no code: %+v", bare)
	}
	broken := rpcErrorFromMetadata(map[string]string{
		MetaErrorCode: "INTERNAL", MetaErrorDetails: `{"not":"an array"}`,
		MetaLogExtra: `{"exception_type":"E","error_details":"nope"}`,
	})
	if broken.Code != "INTERNAL" || broken.Type != "E" || len(broken.Details) != 0 {
		t.Fatalf("malformed details must decode as empty, not fail: %+v", broken)
	}
}

func TestOversizedDetailsAreDroppedFromBoth(t *testing.T) {
	err := &StatusError{Code: CodeResourceExhausted, Kind: "big", Message: "x", Details: []ErrorDetail{
		RetryInfo{RetryDelaySeconds: 1},
		ErrorInfo{Metadata: map[string]string{"padding": strings.Repeat("x", 5000)}},
	}}
	md := decodeExceptionMetadata(t, err, true)
	if _, present := md[MetaErrorDetails]; present {
		t.Fatal("top-level details present over the cap")
	}
	if strings.Contains(md[MetaLogExtra], "error_details") {
		t.Fatal("log_extra.error_details present over the cap")
	}
	if md[MetaErrorCode] != "RESOURCE_EXHAUSTED" || md[MetaErrorKind] != "big" {
		t.Fatalf("code and kind must survive: %v", md)
	}
	if !strings.Contains(md[MetaLogExtra], `"traceback"`) {
		t.Fatal("includeTraceback=true sent no traceback")
	}
}

func TestVersionMismatchNamesTheProtocolInAPrecondition(t *testing.T) {
	err := gateVersion("ConformanceService", "2.0.0", [3]int{2, 0, 0}, "1.0.0", true)
	client := rpcErrorFromMetadata(decodeExceptionMetadata(t, err, false))
	pf, ok := client.PreconditionFailure()
	if !ok || len(pf.Violations) != 1 || pf.Violations[0].Type != "protocol_version" ||
		pf.Violations[0].Subject != "ConformanceService" {
		t.Fatalf("PreconditionFailure = %+v, %v", pf, ok)
	}
}

// Tracebacks are on by default on every transport, and the operator's switch
// turns them off for the whole server. The shared suite asserts the default;
// it cannot reconfigure a worker, so the switch is asserted here, end to end
// over HTTP (the transport that used to omit them).
func TestTheOmitSettingReachesTheWire(t *testing.T) {
	for _, omit := range []bool{false, true} {
		s := NewServer()
		s.SetServiceName("demo.Fail.v1")
		if omit {
			s.SetIncludeTracebacks(false)
		}
		UnaryVoid(s, "fail", func(context.Context, *CallContext, struct{}) error {
			return &StatusError{Code: CodeInternal, Message: "boom"}
		})
		ts := httptest.NewServer(NewHttpServer(s))
		client, err := NewHttpClient(ts.URL, WithClientProtocol("demo.Fail.v1"))
		if err != nil {
			t.Fatal(err)
		}
		empty := array.NewRecordBatch(arrow.NewSchema(nil, nil), nil, 1)
		_, callErr := client.CallUnary(context.Background(), "fail", empty, nil)
		empty.Release()
		client.Close()
		ts.Close()
		var rpcErr *RpcError
		if !errors.As(callErr, &rpcErr) {
			t.Fatalf("omit=%v: got %T %v, want an *RpcError", omit, callErr, callErr)
		}
		if omit && rpcErr.Traceback != "" {
			t.Errorf("tracebacks turned off, yet one was sent: %.80q", rpcErr.Traceback)
		}
		if !omit && rpcErr.Traceback == "" {
			t.Error("tracebacks are on by default on every transport, HTTP included; none was sent")
		}
		if rpcErr.Code != "INTERNAL" {
			t.Errorf("omit=%v: the setting must not touch the error model; code = %q", omit, rpcErr.Code)
		}
	}
}

func TestRpcErrorTypedAccessors(t *testing.T) {
	var details []map[string]any
	for i, s := range catalogSamples {
		if i == 2 { // the second RetryInfo would repeat a type
			details = append(details, map[string]any{"@type": "x.Y"})
			continue
		}
		details = append(details, decodeErrorDetails("[" + s.wire + "]")[0])
	}
	err := &RpcError{Code: "UNAVAILABLE", Kind: "k", Details: details}
	if ei, ok := err.ErrorInfo(); !ok || ei.Metadata["k"] != "v" {
		t.Error("ErrorInfo")
	}
	if ri, ok := err.RetryInfo(); !ok || ri.RetryDelaySeconds != 7 {
		t.Error("RetryInfo")
	}
	_, a := err.BadRequest()
	_, b := err.PreconditionFailure()
	_, c := err.QuotaFailure()
	_, d := err.ResourceInfo()
	_, e := err.Help()
	_, f := err.LocalizedMessage()
	if !(a && b && c && d && e && f) {
		t.Error("an accessor missed its detail")
	}
	if n := len(err.TypedDetails()); n != 8 {
		t.Errorf("TypedDetails() = %d, want 8 (the unknown type skipped)", n)
	}
	if len(err.Details) != 9 {
		t.Error("the raw details must keep the unknown type")
	}
}

// --- Identity translation (WIRE_PROTOCOL.md §16) ---

func TestResolveTokenTranslatesAuthUnavailable(t *testing.T) {
	impl, err := NewIdentity(IdentityConfig{
		ResolveToken: func(string) (TokenIdentity, bool, error) {
			return TokenIdentity{}, false, fmt.Errorf("lookup: %w", &AuthUnavailableError{Detail: "sidecar down", RetryAfter: 7})
		},
		IntrospectPrincipals: []string{"proxy"},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, got := impl.IntrospectToken("opaque", idCtx(idAuth("proxy", true, noAuthTime)))
	var unavailable *IdentityUnavailableError
	if !errors.As(got, &unavailable) || unavailable.RetryAfterSeconds() != 7 {
		t.Fatalf("resolve hook: got %T %v, want identity_unavailable with retry 7", got, got)
	}
}

func TestMintGrantTranslatesAuthUnavailable(t *testing.T) {
	impl, err := NewIdentity(IdentityConfig{
		MintGrant: func(string, string, []string, int64) (IssuedGrant, error) {
			return IssuedGrant{}, &AuthUnavailableError{Detail: "store down", RetryAfter: 7}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, got := impl.IssueGrant("p", nil, 60, idCtx(idAuth("alice", true, float64(time.Now().Unix()))))
	var unavailable *IdentityUnavailableError
	if !errors.As(got, &unavailable) || unavailable.RetryAfterSeconds() != 7 {
		t.Fatalf("mint hook: got %T %v, want identity_unavailable with retry 7", got, got)
	}
}

// --- Hosting (WIRE_PROTOCOL.md §3.1): the Go-API half ---

func TestAddProtocolRefusesTheReservedPrefix(t *testing.T) {
	s := NewServer()
	s.SetServiceName("demo.App.v1")
	for _, name := range []string{"vgi_rpc.Reflection.v1", "vgi_rpc.Anything.v1"} {
		p := NewProtocol(name)
		reached := false
		UnaryVoid(p, "noop", func(context.Context, *CallContext, struct{}) error { reached = true; return nil })
		if err := s.AddProtocol(p); err == nil || !strings.Contains(err.Error(), "reserved") {
			t.Errorf("%s: AddProtocol = %v, want a reserved-prefix refusal", name, err)
		}
		if _, _, err := s.resolve(name, "noop"); err == nil {
			t.Errorf("%s: a refused protocol is routable", name)
		}
		if reached {
			t.Errorf("%s: handler reached", name)
		}
	}
}

func TestAddProtocolRefusesDuplicatesAndLateRegistration(t *testing.T) {
	s := NewServer()
	s.SetServiceName("demo.App.v1")
	if err := s.AddProtocol(NewProtocol("demo.App.v1")); err == nil {
		t.Error("the primary's own name was accepted")
	}
	if err := s.AddProtocol(NewProtocol("demo.Other.v1")); err != nil {
		t.Fatal(err)
	}
	if err := s.AddProtocol(NewProtocol("demo.Other.v1")); err == nil {
		t.Error("a repeated name was accepted")
	}
	if err := s.AddProtocol(NewProtocol("demo.Third.v1")); err != nil {
		t.Fatal(err)
	}
	if got := s.orderedBindingNames(); !reflect.DeepEqual(got, []string{"demo.App.v1", "demo.Other.v1", "demo.Third.v1"}) {
		t.Errorf("order = %v, want registration order, primary first", got)
	}
	if err := s.notifyTransport(TransportKindPipe, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.AddProtocol(NewProtocol("demo.Late.v1")); err == nil {
		t.Error("a protocol was added after the server started serving")
	}
}

// The set is sealed by the first request over HTTP, and by a raw serve loop
// whose serve-start hook refused the binding -- serving began either way.
func TestRegistrationAfterServingFailsOnEveryPath(t *testing.T) {
	s := NewServer()
	s.SetServiceName("demo.App.v1")
	ts := httptest.NewServer(NewHttpServer(s))
	if err := s.AddProtocol(NewProtocol("demo.Before.v1")); err != nil {
		t.Fatalf("building an HttpServer is not serving: %v", err)
	}
	resp, err := ts.Client().Get(ts.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	ts.Close()
	if err := s.AddProtocol(NewProtocol("demo.Late.v1")); err == nil || !strings.Contains(err.Error(), "fixed") {
		t.Fatalf("AddProtocol after an HTTP request = %v, want a refusal", err)
	}

	refused := NewServer()
	refused.SetServeStartHook(func(TransportKind, map[string]bool) error { return errors.New("no") })
	refused.Serve(bytes.NewReader(nil), &bytes.Buffer{})
	if err := refused.AddProtocol(NewProtocol("demo.Late.v1")); err == nil {
		t.Fatal("AddProtocol after a refused serve start was accepted")
	}
}
