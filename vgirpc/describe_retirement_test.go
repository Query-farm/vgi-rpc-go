// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// __describe__ is retired, and a caller is told that rather than left to guess.
//
// "Retired" and "this server was built without introspection" are
// indistinguishable from the caller's side, and they need opposite fixes: one
// is a client to update, the other a server to reconfigure. A bare capability
// answer sends a stale client looking for a configuration flag that no longer
// exists.

// exceptionMessage returns the exception_message of the first EXCEPTION batch
// in an IPC response, or "" when the response carries none.
func exceptionMessage(t *testing.T, body []byte) string {
	t.Helper()
	r, err := ipc.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("open response: %v", err)
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
		extra, _ := md.GetValue(MetaLogExtra)
		var decoded errorExtra
		if err := json.Unmarshal([]byte(extra), &decoded); err != nil {
			t.Fatalf("decode error metadata: %v", err)
		}
		return decoded.ExceptionMessage
	}
	return ""
}

// requireRetirementRefusal asserts a message a stale client can act on without
// reading a changelog: the protocol that replaced __describe__, and both of the
// entry points it has to call.
func requireRetirementRefusal(t *testing.T, message string) {
	t.Helper()
	if message == "" {
		t.Fatal("response carried no exception message")
	}
	if !strings.Contains(strings.ToLower(message), "retired") {
		t.Errorf("refusal does not say the method is retired: %q", message)
	}
	for _, want := range []string{ReflectionProtocolName, "list_protocols", "describe"} {
		if !strings.Contains(message, want) {
			t.Errorf("refusal does not name %q: %q", want, message)
		}
	}
}

func TestRetiredDescribeNamesItsReplacementOnRawTransports(t *testing.T) {
	s := newReflServer(t)

	params := emptyBatch(arrow.NewSchema(nil, nil))
	defer params.Release()
	var request bytes.Buffer
	if err := WriteRequest(&request, retiredDescribeMethod, params, "demo.App.v1", ""); err != nil {
		t.Fatal(err)
	}

	var response bytes.Buffer
	if err := s.serveOne(context.Background(), bytes.NewReader(request.Bytes()), &response, &shmConnState{}); err != nil {
		t.Fatal(err)
	}
	requireRetirementRefusal(t, exceptionMessage(t, response.Bytes()))
}

func TestRetiredDescribeNamesItsReplacementOverHTTP(t *testing.T) {
	h := NewHttpServer(newReflServer(t))
	h.InitPages()

	body := encodeRequestBodyFor(t, "demo.App.v1", retiredDescribeMethod, struct{}{})
	req := httptest.NewRequest(http.MethodPost, "/"+retiredDescribeMethod, bytes.NewReader(body))
	req.Header.Set("Content-Type", arrowContentType)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: %s", rec.Code, rec.Body.String())
	}
	requireRetirementRefusal(t, exceptionMessage(t, rec.Body.Bytes()))
}

// One story, whichever way the caller connected. A client that gets the
// redirection on HTTP and a bare "no such method" on stdio learns the wrong
// thing from whichever transport it happened to try first.
func TestBothTransportsGiveTheSameRefusal(t *testing.T) {
	s := newReflServer(t)

	params := emptyBatch(arrow.NewSchema(nil, nil))
	defer params.Release()
	var request bytes.Buffer
	if err := WriteRequest(&request, retiredDescribeMethod, params, "demo.App.v1", ""); err != nil {
		t.Fatal(err)
	}
	var raw bytes.Buffer
	if err := s.serveOne(context.Background(), bytes.NewReader(request.Bytes()), &raw, &shmConnState{}); err != nil {
		t.Fatal(err)
	}

	h := NewHttpServer(s)
	h.InitPages()
	req := httptest.NewRequest(http.MethodPost, "/"+retiredDescribeMethod,
		bytes.NewReader(encodeRequestBodyFor(t, "demo.App.v1", retiredDescribeMethod, struct{}{})))
	req.Header.Set("Content-Type", arrowContentType)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got, want := exceptionMessage(t, rec.Body.Bytes()), exceptionMessage(t, raw.Bytes()); got != want {
		t.Errorf("HTTP refusal %q differs from the raw-transport refusal %q", got, want)
	}
}

// Only __describe__ is special-cased. A client probing for an optional reserved
// method is asking a capability question, and "this server does not have it" is
// the whole answer -- pointing it at reflection would be noise.
func TestAnotherReservedNameKeepsTheGenericAnswer(t *testing.T) {
	h := NewHttpServer(newReflServer(t))
	h.InitPages()

	body := encodeRequestBodyFor(t, "demo.App.v1", "__not_a_thing__", struct{}{})
	req := httptest.NewRequest(http.MethodPost, "/__not_a_thing__", bytes.NewReader(body))
	req.Header.Set("Content-Type", arrowContentType)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: %s", rec.Code, rec.Body.String())
	}
	msg := exceptionMessage(t, rec.Body.Bytes())
	if strings.Contains(strings.ToLower(msg), "retired") {
		t.Errorf("an unrelated reserved name got the retirement redirection: %q", msg)
	}
}

// The server answers no method named __describe__ -- the refusal is the whole
// of what is left of it.
func TestDescribeIsNotRegisteredAsAMethod(t *testing.T) {
	s := newReflServer(t)
	for name, b := range s.bindings() {
		if _, ok := b.Methods[retiredDescribeMethod]; ok {
			t.Errorf("protocol %q still registers %s", name, retiredDescribeMethod)
		}
	}
}
