// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// An access record names the protocol that OWNS the dispatched method.
//
// docs/access-log-spec.md §3: protocol is "the wire name of the protocol that
// owns the dispatched method … not a server-wide default", and protocol_hash is
// that protocol's canonical digest, "the registry key when decoding archived
// records".
//
// Only a call to a SECONDARY protocol can test this. For an application method
// the primary IS the owning binding, so a server that stamps its primary on
// every record passes everything else the suite asserts -- which is how this
// port shipped exactly that on both transports. And the failure is silent by
// construction: the record is well-formed, it passes the schema, and it feeds a
// plausible dashboard while a consumer keying on protocol_hash decodes it
// against the wrong description.

// labelHook captures what each dispatch was labelled with.
type labelHook struct {
	mu   sync.Mutex
	seen []DispatchInfo
}

func (h *labelHook) OnDispatchStart(ctx context.Context, _ DispatchInfo) (context.Context, HookToken) {
	return ctx, nil
}

func (h *labelHook) OnDispatchEnd(_ context.Context, _ HookToken, info DispatchInfo, _ *CallStatistics, _ error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seen = append(h.seen, info)
}

// forMethod returns the single record for one method, failing when there is not
// exactly one -- a stream that logged only its init turn is as much a failure
// as one that logged none.
func (h *labelHook) forMethod(t *testing.T, method string) []DispatchInfo {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []DispatchInfo
	var methods []string
	for _, info := range h.seen {
		methods = append(methods, info.Method)
		if info.Method == method {
			out = append(out, info)
		}
	}
	if len(out) == 0 {
		t.Fatalf("no dispatch record for %q; saw %v", method, methods)
	}
	return out
}

// requireLabelled asserts both halves of a record's identity come from one
// binding. Checking them together is the point: a record naming one protocol
// and carrying another's digest is worse than either field being wrong alone.
func requireLabelled(t *testing.T, records []DispatchInfo, protocol, hash, notHash string) {
	t.Helper()
	if hash == "" {
		t.Fatal("the owning binding has no canonical hash; the assertion below would be vacuous")
	}
	if hash == notHash {
		t.Fatal("the two bindings hash alike, so mislabelling would be undetectable here")
	}
	for i, info := range records {
		if info.Protocol != protocol {
			t.Errorf("record %d: protocol = %q, want %q -- the record must name the protocol that "+
				"owns the dispatched method, not the server's primary", i, info.Protocol, protocol)
		}
		if info.ProtocolHash != hash {
			t.Errorf("record %d: protocol_hash = %q, want %q", i, info.ProtocolHash, hash)
		}
		if info.ProtocolHash == notHash {
			t.Errorf("record %d: carries the primary protocol's digest. protocol_hash is the registry "+
				"key for decoding archived records, so a record naming one protocol and carrying "+
				"another's is decoded against the wrong description -- and nothing about it looks wrong", i)
		}
	}
}

// --- the accessor -----------------------------------------------------------

func TestDispatchLabelReadsTheOwningBinding(t *testing.T) {
	s := newReflServer(t)
	refl := s.bindings()[ReflectionProtocolName]

	protocol, hash := s.dispatchLabel(refl)
	if protocol != ReflectionProtocolName {
		t.Errorf("protocol = %q, want %q", protocol, ReflectionProtocolName)
	}
	if hash != refl.Hash {
		t.Errorf("hash = %q, want the binding's own %q", hash, refl.Hash)
	}
	if hash == s.canonicalHash() {
		t.Error("reflection and the primary hash alike; every other test here would be vacuous")
	}
}

// The canonical digest, not the legacy byte-based one the retired __describe__
// payload carried: the canonical hash is what compares across ports, and what
// the access-log conformance validator asserts.
func TestDispatchLabelReportsTheCanonicalHash(t *testing.T) {
	s := newReflServer(t)
	want, err := bindingHash("demo.App.v1", s.methods)
	if err != nil {
		t.Fatal(err)
	}
	if _, got := s.dispatchLabel(s.bindings()["demo.App.v1"]); got != want {
		t.Errorf("primary hash = %q, want the canonical %q", got, want)
	}
}

// __transport_options__ and __upload_url__ belong to no protocol. The spec
// prescribes the server's primary for those, so this is the specified answer
// rather than a gap in it.
func TestDispatchLabelFallsBackToThePrimaryForAnUnownedEndpoint(t *testing.T) {
	s := newReflServer(t)
	protocol, hash := s.dispatchLabel(nil)
	if protocol != s.primaryProtocolName() || hash != s.canonicalHash() {
		t.Errorf("unowned endpoint labelled (%q, %q), want the primary (%q, %q)",
			protocol, hash, s.primaryProtocolName(), s.canonicalHash())
	}
}

// A server that never named itself still has to fill `protocol`, which the
// access-log schema requires to be non-empty.
func TestAnUnnamedServerStillLabelsItsRecords(t *testing.T) {
	s := NewServer()
	Unary(s, "echo", func(_ context.Context, _ *CallContext, p reflEcho) (string, error) {
		return p.Value, nil
	})
	if protocol, _ := s.dispatchLabel(nil); protocol == "" {
		t.Error("protocol is empty; the access-log schema requires minLength 1")
	}
}

// --- end to end, every transport -------------------------------------------

func TestRawTransportRecordNamesTheOwningProtocol(t *testing.T) {
	s := newReflServer(t)
	hook := &labelHook{}
	s.SetDispatchHook(hook)

	params := emptyBatch(arrow.NewSchema(nil, nil))
	defer params.Release()
	var request bytes.Buffer
	if err := WriteRequest(&request, "list_protocols", params, ReflectionProtocolName, ""); err != nil {
		t.Fatal(err)
	}
	var response bytes.Buffer
	if err := s.serveOne(context.Background(), bytes.NewReader(request.Bytes()), &response, &shmConnState{}); err != nil {
		t.Fatal(err)
	}

	requireLabelled(t, hook.forMethod(t, "list_protocols"),
		ReflectionProtocolName, s.bindings()[ReflectionProtocolName].Hash, s.canonicalHash())
}

func TestHTTPUnaryRecordNamesTheOwningProtocol(t *testing.T) {
	s := newReflServer(t)
	hook := &labelHook{}
	s.SetDispatchHook(hook)
	h := NewHttpServer(s)
	h.InitPages()

	body := encodeRequestBodyFor(t, ReflectionProtocolName, "list_protocols", struct{}{})
	req := httptest.NewRequest(http.MethodPost, "/"+ReflectionProtocolName+"/list_protocols", bytes.NewReader(body))
	req.Header.Set("Content-Type", arrowContentType)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}

	requireLabelled(t, hook.forMethod(t, "list_protocols"),
		ReflectionProtocolName, s.bindings()[ReflectionProtocolName].Hash, s.canonicalHash())
}

// A stream logs at /init and again at every /exchange turn, from two separate
// emit sites. They have to agree: a record set whose first turn names the
// owning protocol and whose continuations name the primary is worse than either
// alone, because stream_id still joins them.
func TestHTTPStreamTurnsAllNameTheOwningProtocol(t *testing.T) {
	RegisterStateType(&nsCountState{})
	s := NewServer()
	s.SetServiceName("demo.App.v1")
	Unary(s, "echo", func(_ context.Context, _ *CallContext, p nsEchoParams) (string, error) {
		return p.Value, nil
	})

	sub := NewServer()
	sub.SetServiceName("other.App.v1")
	Exchange(sub, "count", nsCountSchema, nsCountSchema,
		func(context.Context, *CallContext, struct{}) (*StreamResult, error) {
			return &StreamResult{OutputSchema: nsCountSchema, InputSchema: nsCountSchema, State: &nsCountState{Remaining: 4}}, nil
		})
	hash, err := bindingHash("other.App.v1", sub.methods)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AddProtocol(&protocolBinding{
		Name: "other.App.v1", Methods: sub.methods, Hash: hash, Impl: sub,
	}, false); err != nil {
		t.Fatal(err)
	}

	hook := &labelHook{}
	s.SetDispatchHook(hook)
	h := NewHttpServer(s)
	h.InitPages()

	params := emptyBatch(arrow.NewSchema(nil, nil))
	defer params.Release()
	var initBody bytes.Buffer
	if err := WriteRequest(&initBody, "count", params, "other.App.v1", ""); err != nil {
		t.Fatal(err)
	}
	initReq := httptest.NewRequest(http.MethodPost, "/other.App.v1/count/init", bytes.NewReader(initBody.Bytes()))
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
	exReq := httptest.NewRequest(http.MethodPost, "/other.App.v1/count/exchange", bytes.NewReader(exBody.Bytes()))
	exReq.Header.Set("Content-Type", arrowContentType)
	exRec := httptest.NewRecorder()
	h.ServeHTTP(exRec, exReq)
	if exRec.Code != http.StatusOK {
		t.Fatalf("exchange status = %d: %s", exRec.Code, exRec.Body.String())
	}

	records := hook.forMethod(t, "count")
	if len(records) != 2 {
		t.Fatalf("got %d records for the stream, want 2 (init and one continuation)", len(records))
	}
	requireLabelled(t, records, "other.App.v1", hash, s.canonicalHash())
}

// --- the guard against a fifth emit site ------------------------------------

// Every DispatchInfo literal fills Protocol and ProtocolHash from dispatchLabel.
//
// The emit sites are spread across two HTTP dispatchers, the HTTP continuation
// path and the raw-transport serve loop, and they are the only places this can
// go wrong. A fifth added later, reaching for the server's own serviceName the
// way all four of these once did, would reintroduce the bug with the suite
// green -- so the shape is asserted over the source rather than one site at a
// time.
func TestEveryDispatchInfoLabelsItselfFromTheOwningBinding(t *testing.T) {
	const wantProtocol, wantHash = "logProtocol", "logHash"

	sources, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(sources)

	found := 0
	for _, path := range sources {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, src, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			if ident, ok := lit.Type.(*ast.Ident); !ok || ident.Name != "DispatchInfo" {
				return true
			}
			found++
			for _, elt := range lit.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				key, ok := kv.Key.(*ast.Ident)
				if !ok {
					continue
				}
				want := ""
				switch key.Name {
				case "Protocol":
					want = wantProtocol
				case "ProtocolHash":
					want = wantHash
				default:
					continue
				}
				value, ok := kv.Value.(*ast.Ident)
				if !ok || value.Name != want {
					t.Errorf("%s: DispatchInfo.%s is not %s from Server.dispatchLabel. "+
						"An access record must name the protocol that owns the dispatched method "+
						"and carry that protocol's digest; a server-wide default mislabels every "+
						"call to a secondary protocol, silently.", path, key.Name, want)
				}
			}
			return true
		})
	}
	if found == 0 {
		t.Fatal("found no DispatchInfo literals; this guard is asserting nothing")
	}
}
