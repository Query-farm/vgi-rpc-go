// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Access-log behaviour the cross-language harness structurally cannot reach.
//
// The record *shape* is validated by `vgi-rpc-test --access-log`, which reads
// the JSONL a worker wrote and checks it against access_log.schema.json. What
// that cannot see is everything about records that were never written, or
// written differently under load: whether sampling is deterministic per call,
// whether a full async queue reports what it dropped, whether a redactor that
// blows up fails closed, and whether response_bytes reports the compressed
// body rather than the uncompressed one it is trivially confusable with.
// Those are asserted here.

package vgirpc

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- helpers ---------------------------------------------------------------

// decodeRecords parses a buffer of JSONL access-log records.
func decodeRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("access-log line is not JSON: %v (%q)", err, line)
		}
		out = append(out, rec)
	}
	return out
}

// dispatchOnce drives one hook lifecycle and returns the records it wrote.
func dispatchOnce(t *testing.T, hook *AccessLogHook, buf *bytes.Buffer, info DispatchInfo, err error) []map[string]any {
	t.Helper()
	ctx, token := hook.OnDispatchStart(context.Background(), info)
	hook.OnDispatchEnd(ctx, token, info, nil, err)
	return decodeRecords(t, buf)
}

func unaryInfo() DispatchInfo {
	return DispatchInfo{
		Method:       "echo",
		MethodType:   DispatchMethodUnary,
		ServerID:     "test-server",
		Protocol:     "TestService",
		ProtocolHash: strings.Repeat("a", 64),
		Auth:         Anonymous(),
	}
}

// --- sampling --------------------------------------------------------------

// A sampler that dropped errors would leave a consumer unable to read a
// falling error count as a fix landing rather than as the dice going the
// other way, so status=error bypasses the decision entirely.
func TestAccessLogSamplerAlwaysKeepsErrors(t *testing.T) {
	sampler, err := newAccessLogSampler(0.0)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 100 {
		record := map[string]any{"status": "error", "request_id": string(rune('a' + i%26))}
		if !sampler.keep(record) {
			t.Fatalf("error record %d was sampled out", i)
		}
	}
	// The same rate must drop everything that is not an error, or the test
	// above proves nothing.
	if sampler.keep(map[string]any{"status": "ok", "request_id": "kept?"}) {
		t.Fatal("rate 0.0 kept a successful call")
	}
}

// The decision is a function of the call's identity, not of a PRNG, so every
// record of one stream shares its init's fate. Random per-record sampling
// would shred a multi-record call into fragments indistinguishable from data
// loss.
func TestAccessLogSamplerIsDeterministicPerCall(t *testing.T) {
	sampler, err := newAccessLogSampler(0.5)
	if err != nil {
		t.Fatal(err)
	}
	// Enough ids that both outcomes are represented; otherwise "always the
	// same answer" could be satisfied by a sampler that keeps everything.
	var kept, dropped int
	for i := range 200 {
		streamID := strings.Repeat("0", 28) + string("0123456789abcdef"[i%16]) + "abc"
		first := sampler.keep(map[string]any{"status": "ok", "stream_id": streamID})
		for range 5 {
			again := sampler.keep(map[string]any{"status": "ok", "stream_id": streamID})
			if again != first {
				t.Fatalf("stream %s got two different decisions", streamID)
			}
		}
		if first {
			kept++
		} else {
			dropped++
		}
	}
	if kept == 0 || dropped == 0 {
		t.Fatalf("rate 0.5 over 200 ids produced kept=%d dropped=%d; the determinism check is vacuous", kept, dropped)
	}
}

// stream_id first, so a continuation carrying no request_id still shares its
// init's fate.
func TestAccessLogSamplerPrefersStreamIDOverRequestID(t *testing.T) {
	sampler, err := newAccessLogSampler(0.5)
	if err != nil {
		t.Fatal(err)
	}
	streamID := strings.Repeat("b", 32)
	base := sampler.keep(map[string]any{"status": "ok", "stream_id": streamID, "request_id": "init"})
	cont := sampler.keep(map[string]any{"status": "ok", "stream_id": streamID, "request_id": "continuation"})
	if base != cont {
		t.Fatal("records of one stream got different decisions because request_id differed")
	}
}

// A consumer counting calls from a sampled log has to divide by the rate, so
// it has to be in the record rather than only in a deployment's flags.
func TestAccessLogSamplerStampsRate(t *testing.T) {
	sampler, err := newAccessLogSampler(1.0)
	if err != nil {
		t.Fatal(err)
	}
	record := map[string]any{"status": "ok", "request_id": "x"}
	if !sampler.keep(record) {
		t.Fatal("rate 1.0 dropped a record")
	}
	if _, present := record["sample_rate"]; present {
		t.Fatal("sample_rate must be absent when the emitter logs everything")
	}

	sampler, err = newAccessLogSampler(1.0 - 1e-9)
	if err != nil {
		t.Fatal(err)
	}
	kept := 0
	for i := range 50 {
		record := map[string]any{"status": "ok", "request_id": string(rune('a' + i%26))}
		if sampler.keep(record) {
			kept++
			if record["sample_rate"] == nil {
				t.Fatal("a sampled-in record carries no sample_rate")
			}
		}
	}
	if kept == 0 {
		t.Fatal("a rate just below 1.0 kept nothing")
	}
}

// 100 meaning "100%" must not silently log everything, so the rate is
// rejected where it is configured rather than at the first request.
func TestAccessLogSampleRateOutOfRangeFailsAtConfiguration(t *testing.T) {
	hook := NewAccessLogHook(&bytes.Buffer{}, "")
	for _, rate := range []float64{100, -0.5, 1.0000001} {
		if err := hook.SetSampleRate(rate); err == nil {
			t.Fatalf("sample rate %v was accepted", rate)
		}
	}
	if err := hook.SetSampleRate(0.25); err != nil {
		t.Fatalf("sample rate 0.25 was rejected: %v", err)
	}
}

// --- asynchronous emission -------------------------------------------------

// A log that loses records without saying so is worse than a slow one: a
// consumer cannot tell a quiet period from a lossy one. Full means drop, and
// the next record through has to carry the count.
func TestAccessLogAsyncReportsDroppedRecords(t *testing.T) {
	var mu sync.Mutex
	var written []map[string]any
	started := make(chan struct{}, 1)
	release := make(chan struct{})

	async, err := newAsyncEmitter(1, func(record map[string]any) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		mu.Lock()
		written = append(written, record)
		mu.Unlock()
	})
	if err != nil {
		t.Fatal(err)
	}

	// r1 is picked up by the writer, which then parks: the queue is empty
	// again and the next enqueue is guaranteed to fit.
	async.enqueue(map[string]any{"n": "r1"})
	<-started
	async.enqueue(map[string]any{"n": "r2"}) // fills the 1-slot queue
	async.enqueue(map[string]any{"n": "r3"}) // dropped
	async.enqueue(map[string]any{"n": "r4"}) // dropped

	close(release)
	// Wait for the queue to drain so the next enqueue is the "first record
	// through after a drop".
	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		n := len(written)
		mu.Unlock()
		if n >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("writer did not drain")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	async.enqueue(map[string]any{"n": "r5"})
	async.close()

	mu.Lock()
	defer mu.Unlock()
	if len(written) != 3 {
		t.Fatalf("expected 3 records through a 1-slot queue, got %d: %v", len(written), written)
	}
	for _, r := range written[:2] {
		if _, present := r["dropped_records"]; present {
			t.Fatalf("record %v carries dropped_records but nothing had been dropped yet", r)
		}
	}
	if got := written[2]["dropped_records"]; got != int64(2) {
		t.Fatalf("expected dropped_records=2 on the first record after the drop, got %v", got)
	}
}

// A drop must not block, which is the whole reason the queue is bounded: an
// unbounded queue turns a stalled disk into an OOM, a blocking put restores
// the latency the goroutine was meant to remove.
func TestAccessLogAsyncNeverBlocksWhenFull(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	async, err := newAsyncEmitter(1, func(map[string]any) { <-release })
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 1000 {
			async.enqueue(map[string]any{"n": i})
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("enqueue blocked on a full queue")
	}
}

func TestAccessLogAsyncRejectsNonPositiveQueue(t *testing.T) {
	if _, err := newAsyncEmitter(0, func(map[string]any) {}); err == nil {
		t.Fatal("queue size 0 was accepted")
	}
}

// --- claim redaction -------------------------------------------------------

// Which claims a credential carried is a question an audit log exists to
// answer; what they contained is not. So the keys survive and the values do
// not.
func TestRedactClaimsReplacesValuesAndKeepsKeys(t *testing.T) {
	claims := map[string]any{
		"sub":            "user-42",
		"email":          "alice@example.com",
		"phone_number":   "+15555550100",
		"api_key":        "sk-live-secret",
		"refresh_token":  "rt-secret",
		"password":       "hunter2",
		"given_name":     "Alice",
		"scope":          "read write",
		"tenant":         "acme",
		"Authorization":  "Bearer abc",
		"custom_context": "alice@example.com",
	}
	got := RedactClaims(claims)

	if len(got) != len(claims) {
		t.Fatalf("redaction dropped keys: %d in, %d out", len(claims), len(got))
	}
	for _, k := range []string{"email", "phone_number", "api_key", "refresh_token", "password", "given_name", "Authorization"} {
		if got[k] != RedactedClaim {
			t.Errorf("claim %q was not redacted: %v", k, got[k])
		}
	}
	// Key-based, never content-based: a claim holding an email under a name
	// that says nothing about it is not caught, and cannot be without
	// guessing at free text.
	for _, k := range []string{"sub", "scope", "tenant", "custom_context"} {
		if got[k] != claims[k] {
			t.Errorf("claim %q should have passed through, got %v", k, got[k])
		}
	}
}

// The default policy must reach the record without the caller doing anything.
func TestAccessLogRedactsClaimsByDefault(t *testing.T) {
	var buf bytes.Buffer
	hook := NewAccessLogHook(&buf, "")
	info := unaryInfo()
	info.Auth = &AuthContext{
		Domain: "bearer", Authenticated: true, Principal: "alice",
		Claims: map[string]any{"sub": "user-42", "email": "alice@example.com"},
	}
	records := dispatchOnce(t, hook, &buf, info, nil)

	claims, ok := records[0]["claims"].(map[string]any)
	if !ok {
		t.Fatalf("no claims on the record: %v", records[0])
	}
	if claims["email"] != RedactedClaim {
		t.Errorf("email reached the log as %v", claims["email"])
	}
	if claims["sub"] != "user-42" {
		t.Errorf("sub should have passed through, got %v", claims["sub"])
	}
}

// A redactor that blows up must not fail *open*: reverting to verbatim claims
// is the exact outcome this machinery exists to prevent.
func TestClaimRedactorFailsClosed(t *testing.T) {
	t.Cleanup(func() { SetClaimRedactor(nil) })
	SetClaimRedactor(func(map[string]any) map[string]any {
		panic("redactor is broken")
	})

	var buf bytes.Buffer
	hook := NewAccessLogHook(&buf, "")
	info := unaryInfo()
	info.Auth = &AuthContext{
		Domain: "bearer", Authenticated: true, Principal: "alice",
		Claims: map[string]any{"email": "alice@example.com"},
	}
	records := dispatchOnce(t, hook, &buf, info, nil)

	if _, present := records[0]["claims"]; present {
		t.Fatalf("a panicking redactor let claims through: %v", records[0]["claims"])
	}
}

func TestNoClaimRedactionOptOut(t *testing.T) {
	t.Cleanup(func() { SetClaimRedactor(nil) })
	SetClaimRedactor(NoClaimRedaction)

	var buf bytes.Buffer
	hook := NewAccessLogHook(&buf, "")
	info := unaryInfo()
	info.Auth = &AuthContext{
		Domain: "bearer", Authenticated: true, Principal: "alice",
		Claims: map[string]any{"email": "alice@example.com"},
	}
	records := dispatchOnce(t, hook, &buf, info, nil)

	claims := records[0]["claims"].(map[string]any)
	if claims["email"] != "alice@example.com" {
		t.Fatalf("opt-out did not pass the claim through: %v", claims["email"])
	}
}

// --- truncation marker -----------------------------------------------------

// "payload_omitted" and true carried one meaning between them until they were
// split. A consumer scanning for real data loss has to be able to filter the
// common case out, so level-gated omission must never report itself as
// size-driven shedding.
func TestAccessLogPayloadOmissionIsNotTruncation(t *testing.T) {
	var buf bytes.Buffer
	hook := NewAccessLogHook(&buf, "")
	info := unaryInfo()
	info.RequestData = []byte("not-really-arrow-but-non-empty")
	records := dispatchOnce(t, hook, &buf, info, nil)

	if got := records[0]["truncated"]; got != "payload_omitted" {
		t.Fatalf("expected truncated=payload_omitted, got %#v", got)
	}
	if got := records[0]["truncated"]; got == true {
		t.Fatal("level-gated omission reported itself as size-driven shedding")
	}
	if _, present := records[0]["request_data"]; present {
		t.Fatal("request_data present at INFO level")
	}
	if records[0]["original_request_bytes"] == nil {
		t.Fatal("original_request_bytes missing, so the dropped size is unknowable")
	}
}

func TestAccessLogDebugCarriesPayloadAndNoMarker(t *testing.T) {
	var buf bytes.Buffer
	hook := NewAccessLogHook(&buf, "")
	hook.SetDebug(true)
	info := unaryInfo()
	info.RequestData = []byte("payload")
	records := dispatchOnce(t, hook, &buf, info, nil)

	if records[0]["request_data"] == nil {
		t.Fatal("debug mode dropped request_data")
	}
	if _, present := records[0]["truncated"]; present {
		t.Fatalf("nothing was omitted, but the record is marked truncated=%v", records[0]["truncated"])
	}
}

// --- trace correlation -----------------------------------------------------

func TestAccessLogTraceCorrelation(t *testing.T) {
	t.Cleanup(func() { SetTraceContextProvider(nil) })
	traceID := strings.Repeat("1", 32)
	spanID := strings.Repeat("2", 16)
	SetTraceContextProvider(func(context.Context) (string, string) { return traceID, spanID })

	var buf bytes.Buffer
	hook := NewAccessLogHook(&buf, "")
	records := dispatchOnce(t, hook, &buf, unaryInfo(), nil)

	if records[0]["trace_id"] != traceID || records[0]["span_id"] != spanID {
		t.Fatalf("trace correlation missing: %v", records[0])
	}
}

// Both or neither: a record carrying one half of the pair is useless for
// correlation and fails the cross-language schema, so a malformed answer is
// treated as no answer.
func TestAccessLogTraceCorrelationBothOrNeither(t *testing.T) {
	t.Cleanup(func() { SetTraceContextProvider(nil) })
	cases := map[string][2]string{
		"span only":     {"", strings.Repeat("2", 16)},
		"trace only":    {strings.Repeat("1", 32), ""},
		"dashed uuid":   {"11111111-1111-1111-1111-111111111111", strings.Repeat("2", 16)},
		"uppercase hex": {strings.ToUpper(strings.Repeat("a", 32)), strings.Repeat("2", 16)},
		"short span":    {strings.Repeat("1", 32), "2222"},
	}
	for name, pair := range cases {
		t.Run(name, func(t *testing.T) {
			SetTraceContextProvider(func(context.Context) (string, string) { return pair[0], pair[1] })
			var buf bytes.Buffer
			hook := NewAccessLogHook(&buf, "")
			records := dispatchOnce(t, hook, &buf, unaryInfo(), nil)
			if _, present := records[0]["trace_id"]; present {
				t.Errorf("trace_id emitted for %s: %v", name, records[0]["trace_id"])
			}
			if _, present := records[0]["span_id"]; present {
				t.Errorf("span_id emitted for %s: %v", name, records[0]["span_id"])
			}
		})
	}
}

// Observability must not be able to fail a request.
func TestAccessLogTraceProviderPanicIsContained(t *testing.T) {
	t.Cleanup(func() { SetTraceContextProvider(nil) })
	SetTraceContextProvider(func(context.Context) (string, string) { panic("provider is broken") })

	var buf bytes.Buffer
	hook := NewAccessLogHook(&buf, "")
	records := dispatchOnce(t, hook, &buf, unaryInfo(), nil)
	if _, present := records[0]["trace_id"]; present {
		t.Fatal("a panicking provider produced a trace_id")
	}
}

// --- egress accounting -----------------------------------------------------

type bigParams struct {
	N int64 `vgirpc:"n"`
}

// newEgressTestServer serves one method whose result is highly compressible,
// so response_bytes and output_bytes cannot be confused for each other.
func newEgressTestServer(t *testing.T, hook *AccessLogHook) *HttpServer {
	t.Helper()
	s := NewServer()
	s.SetServiceName("EgressService")
	Unary(s, "big", func(_ context.Context, _ *CallContext, p bigParams) (string, error) {
		return strings.Repeat("a", int(p.N)), nil
	})
	s.SetDispatchHook(hook)
	return NewHttpServer(s)
}

// input_bytes/output_bytes measure logical Arrow buffers; response_bytes
// measures what left the process. Conflating them is how an egress bill ends
// up wrong by orders of magnitude, so the test asserts the gap rather than
// merely asserting the field exists.
func TestAccessLogResponseBytesIsPostCompression(t *testing.T) {
	var buf bytes.Buffer
	hook := NewAccessLogHook(&buf, "")
	h := newEgressTestServer(t, hook)

	body := encodeRequestBody(t, "big", bigParams{N: 200000})
	req := httptest.NewRequest(http.MethodPost, "/big", bytes.NewReader(body))
	req.Header.Set("Content-Type", arrowContentType)
	req.Header.Set("Accept-Encoding", "zstd")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if enc := rec.Header().Get(contentEncodingHeader); enc != "zstd" {
		t.Fatalf("response was not compressed (Content-Encoding=%q); the test cannot distinguish the two figures", enc)
	}

	records := decodeRecords(t, &buf)
	if len(records) != 1 {
		t.Fatalf("expected one record, got %d", len(records))
	}
	responseBytes, ok := records[0]["response_bytes"].(float64)
	if !ok {
		t.Fatalf("no response_bytes on the record: %v", records[0])
	}
	outputBytes, ok := records[0]["output_bytes"].(float64)
	if !ok {
		t.Fatalf("no output_bytes on the record: %v", records[0])
	}
	if int(responseBytes) != rec.Body.Len() {
		t.Fatalf("response_bytes=%d but %d bytes were written to the wire", int(responseBytes), rec.Body.Len())
	}
	if responseBytes >= outputBytes {
		t.Fatalf("response_bytes=%v is not below output_bytes=%v; the record is reporting the uncompressed body",
			responseBytes, outputBytes)
	}
}

// The request figure is the body as received, before decompression — what the
// peer actually sent.
func TestAccessLogRequestBytesIsOnWireSize(t *testing.T) {
	var buf bytes.Buffer
	hook := NewAccessLogHook(&buf, "")
	h := newEgressTestServer(t, hook)

	body := encodeRequestBody(t, "big", bigParams{N: 8})
	req := httptest.NewRequest(http.MethodPost, "/big", bytes.NewReader(body))
	req.Header.Set("Content-Type", arrowContentType)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}

	records := decodeRecords(t, &buf)
	if got := records[0]["request_bytes"]; got != float64(len(body)) {
		t.Fatalf("request_bytes=%v, body was %d bytes", got, len(body))
	}
}

// --- correlation id --------------------------------------------------------

// The header was advertised in Access-Control-Expose-Headers long before
// anything emitted it, which passed the shared suite's exposure assertion
// vacuously. Emission has no cross-language assertion to hang this on, so it
// is guarded here.
func TestHTTPRequestIDIsEchoedWhenSupplied(t *testing.T) {
	h := newTestHttpServer(t)
	h.InitPages()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set(requestIDHeader, "caller-supplied-id")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get(requestIDHeader); got != "caller-supplied-id" {
		t.Fatalf("expected the caller's id echoed, got %q", got)
	}
}

func TestHTTPRequestIDIsMintedWhenAbsentOrOversized(t *testing.T) {
	h := newTestHttpServer(t)
	h.InitPages()

	for name, supplied := range map[string]string{
		"absent":    "",
		"blank":     "   ",
		"oversized": strings.Repeat("x", maxRequestIDLength+1),
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/health", nil)
			if supplied != "" {
				req.Header.Set(requestIDHeader, supplied)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			got := rec.Header().Get(requestIDHeader)
			if !isLowerHex(got, 16) {
				t.Fatalf("expected a minted 16-char hex id, got %q", got)
			}
		})
	}
}

// The correlation id is most useful on the responses nobody planned for, so
// it has to be set before dispatch rather than on the success path.
func TestHTTPRequestIDRidesErrorResponses(t *testing.T) {
	h := newTestHttpServer(t)
	h.InitPages()

	req := httptest.NewRequest(http.MethodPost, "/no_such_method", nil)
	req.Header.Set("Content-Type", arrowContentType)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get(requestIDHeader); got == "" {
		t.Fatal("an error response carries no X-Request-ID")
	}
}

// A request that sent no correlation id of its own still gets one in the
// record, which is also what keeps the sampler's per-call key stable.
func TestAccessLogFallsBackToTransportRequestID(t *testing.T) {
	var buf bytes.Buffer
	hook := NewAccessLogHook(&buf, "")
	h := newEgressTestServer(t, hook)

	body := encodeRequestBody(t, "big", bigParams{N: 4})
	req := httptest.NewRequest(http.MethodPost, "/big", bytes.NewReader(body))
	req.Header.Set("Content-Type", arrowContentType)
	req.Header.Set(requestIDHeader, "trace-me")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	records := decodeRecords(t, &buf)
	if got := records[0]["request_id"]; got != "trace-me" {
		t.Fatalf("request_id=%v, expected the transport's id", got)
	}
}
