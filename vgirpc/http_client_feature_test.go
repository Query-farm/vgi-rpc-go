// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// These cover the client-side halves the cross-language suite reaches only
// through a live peer: the pieces are pure enough to pin here, and pinning
// them here means a regression names itself instead of arriving as one of
// fourteen hundred conformance failures.

func TestDecodeErrorEnvelopeRecoversRemoteError(t *testing.T) {
	// A 4xx that carries an Arrow exception envelope: the status is for the
	// intermediaries, the envelope is for the caller. Reading only the status
	// threw the caller's half away and left a binary blob as the message.
	body := errorEnvelopeBytes(t, "ValueError", "boom", "Traceback (most recent call last):\n")
	decoded := decodeErrorEnvelope(body)
	if decoded == nil {
		t.Fatal("decodeErrorEnvelope returned nil for an exception envelope")
	}
	if decoded.Type != "ValueError" {
		t.Errorf("error type = %q, want ValueError", decoded.Type)
	}
	if decoded.Message != "boom" {
		t.Errorf("error message = %q, want boom", decoded.Message)
	}
	if decoded.Traceback == "" {
		t.Error("remote traceback was dropped")
	}
}

func TestDecodeErrorEnvelopeIgnoresNonArrowBodies(t *testing.T) {
	// A 401's JSON envelope, a proxy's HTML page, an empty 404: for these the
	// status really is the whole answer, and reinterpreting them would turn a
	// transport refusal into a fabricated RPC error.
	for name, body := range map[string][]byte{
		"empty": nil,
		"json":  []byte(`{"error":"invalid_token"}`),
		"html":  []byte("<html><body>502 Bad Gateway</body></html>"),
	} {
		if decoded := decodeErrorEnvelope(body); decoded != nil {
			t.Errorf("%s body decoded as an RPC error: %+v", name, decoded)
		}
	}
}

func TestCapabilitySnapshotReadsOptionalFeatures(t *testing.T) {
	headers := http.Header{}
	headers.Set(acceptMaxResponseBytesSupportHeader, "true")
	headers.Set(uploadURLHeader, "true")
	headers.Set(maxUploadBytesHeader, "16777216")
	headers.Set(supportedEncodingsHeader, "zstd, gzip, identity")
	headers.Set(stickyEnabledHeader, "true")
	headers.Set(stickyDefaultTTLHeader, "300")
	headers.Set(stickyEchoHeadersHeader, "x-vgi-conformance-echo, Backend")
	headers.Set(maxExternalizedResponseBytesHeader, "65536")

	caps, err := ParseHTTPServerCapabilities(headers)
	if err != nil {
		t.Fatalf("ParseHTTPServerCapabilities: %v", err)
	}
	if !caps.UploadURLSupport || caps.MaxUploadBytes != 16777216 {
		t.Errorf("upload capability = %v/%d", caps.UploadURLSupport, caps.MaxUploadBytes)
	}
	if !caps.StickyEnabled || caps.StickyDefaultTTL != 300 {
		t.Errorf("sticky capability = %v/%d", caps.StickyEnabled, caps.StickyDefaultTTL)
	}
	if got := caps.SupportedEncodings; len(got) != 3 || got[0] != "zstd" || got[2] != "identity" {
		t.Errorf("supported encodings = %v", got)
	}
	if got := caps.StickyEchoHeaders; len(got) != 2 || got[0] != "x-vgi-conformance-echo" {
		t.Errorf("sticky echo headers = %v", got)
	}
	if caps.MaxExternalizedResponseBytes != 65536 {
		t.Errorf("max externalized = %d", caps.MaxExternalizedResponseBytes)
	}
}

func TestCapabilitySnapshotToleratesMalformedOptionalValues(t *testing.T) {
	// These headers ride on every response, not just the capability probe, so
	// failing the parse would turn one malformed advertisement of a feature the
	// caller never asked for into total unavailability.
	headers := http.Header{}
	headers.Set(stickyDefaultTTLHeader, "soon")
	headers.Set(maxUploadBytesHeader, "-1")
	caps, err := ParseHTTPServerCapabilities(headers)
	if err != nil {
		t.Fatalf("ParseHTTPServerCapabilities: %v", err)
	}
	if caps.StickyDefaultTTL != 0 || caps.MaxUploadBytes != 0 {
		t.Errorf("malformed optional capabilities were not treated as unadvertised: %+v", caps)
	}
}

func TestSessionEchoHeadersKeepTheServersSpelling(t *testing.T) {
	// net/http canonicalises response header names, so the name after the
	// VGI-Echo- prefix comes back re-cased. The captured map is a
	// caller-visible dictionary, so a caller looking up the name the server
	// advertised would miss. The advertisement itself is a header *value*,
	// which nobody canonicalises, and it is the way back.
	client := &HttpClient{}
	client.BeginSession("")
	headers := http.Header{}
	headers.Set(stickyEchoHeadersHeader, "x-vgi-conformance-echo")
	headers.Set(stickyEchoHeaderPrefix+"x-vgi-conformance-echo", "conformance-fixed-marker")
	headers.Set(stickySessionHeader, "token-1")
	client.observeSessionHeaders(headers)

	echo := client.CurrentEchoHeaders()
	if echo["x-vgi-conformance-echo"] != "conformance-fixed-marker" {
		t.Errorf("echo headers = %v, want the advertised spelling as the key", echo)
	}
	if client.CurrentSessionToken() != "token-1" {
		t.Errorf("session token = %q", client.CurrentSessionToken())
	}
}

func TestSessionCloseClearsTheScope(t *testing.T) {
	client := &HttpClient{}
	client.BeginSession("resumed")
	closing := http.Header{}
	closing.Set(stickySessionCloseHeader, "true")
	client.observeSessionHeaders(closing)
	if token := client.CurrentSessionToken(); token != "" {
		t.Errorf("token survived VGI-Session-Close: %q", token)
	}
	if len(client.CurrentEchoHeaders()) != 0 {
		t.Error("echo headers survived VGI-Session-Close")
	}
}

func TestSessionHeadersAreNotStampedOutsideASession(t *testing.T) {
	// The opt-in is what makes a server mint a token, so stamping it always
	// would open a session for every caller that never asked for one.
	client := &HttpClient{}
	headers := http.Header{}
	client.applySessionHeaders(headers)
	if len(headers) != 0 {
		t.Errorf("headers stamped with no session open: %v", headers)
	}
}

// errorEnvelopeBytes frames the zero-row EXCEPTION batch a server sends.
func errorEnvelopeBytes(t *testing.T, exceptionType, message, traceback string) []byte {
	t.Helper()
	schema := arrow.NewSchema(nil, nil)
	batch := array.NewRecordBatch(schema, nil, 0)
	defer batch.Release()
	extra := `{"exception_type": "` + exceptionType + `", "traceback": ` + quoteJSON(traceback) + `}`
	annotated := array.NewRecordBatchWithMetadata(schema, nil, 0, arrow.NewMetadata(
		[]string{MetaLogLevel, MetaLogMessage, MetaLogExtra},
		[]string{string(LogException), message, extra},
	))
	defer annotated.Release()
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := writer.Write(annotated); err != nil {
		t.Fatalf("write error envelope: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close error envelope: %v", err)
	}
	return buf.Bytes()
}

func quoteJSON(value string) string {
	var out bytes.Buffer
	out.WriteByte('"')
	for _, r := range value {
		switch r {
		case '"':
			out.WriteString(`\"`)
		case '\\':
			out.WriteString(`\\`)
		case '\n':
			out.WriteString(`\n`)
		default:
			out.WriteRune(r)
		}
	}
	out.WriteByte('"')
	return out.String()
}
