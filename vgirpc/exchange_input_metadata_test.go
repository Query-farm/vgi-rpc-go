// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// What an exchange method is handed as its input's metadata, on every transport.
//
// The rule (WIRE_PROTOCOL.md, "Stream exchange (HTTP)" and §12): each input's
// own custom metadata reaches the method, every turn, less the transport's
// bookkeeping -- the stream cursor, the call token and the cancel marker. An
// externalized input is handed the *fetched payload's* metadata plus the
// reader's provenance stamp (vgi_rpc.location.source / .fetch_ms), never the
// pointer's.
//
// The shared conformance suite covers the parts a fixture reading
// CallContext.InputMetadata can observe over the transports it drives. What it
// cannot reach, and this file does:
//
//   - the batch the method is handed. A Go handler can read metadata off the
//     input batch itself as well as off InputMetadata; over HTTP the batch
//     used to be the request batch as read, still carrying the sealed cursor
//     and the call token, so the two views disagreed and one of them leaked.
//   - an externalized input over the byte-stream transports, which the suite
//     only externalizes over HTTP. Both transports dropped the provenance
//     stamp: HTTP re-read the metadata off the fetched batch after resolving
//     it, the pipe threw the resolved metadata away.
//   - an /init request carrying a transport key, which no honest client sends
//     but which reached a producer's first tick unstripped.
//
// Requests here are fabricated -- the test plays the client -- and every
// response comes from a real Server.

package vgirpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// probeInputKey is the application key the probe reports the value of.
const probeInputKey = "vgi.test.input"

var inputMetadataProbeSchema = arrow.NewSchema([]arrow.Field{
	{Name: "seen", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "keys", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "batch_keys", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "source", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

// inputMetadataProbe answers every input with one row describing the metadata
// it was handed, through both of the views a Go handler has: InputMetadata
// (`seen`, `keys`, `source`) and the input batch's own (`batch_keys`).
type inputMetadataProbe struct{ Turns int }

func (p *inputMetadataProbe) Exchange(_ context.Context, input arrow.RecordBatch, out *OutputCollector, callCtx *CallContext) error {
	p.Turns++
	var batchMeta arrow.Metadata
	if annotated, ok := input.(arrow.RecordBatchWithMetadata); ok {
		batchMeta = annotated.Metadata()
	}
	seen, _ := callCtx.InputMetadata.GetValue(probeInputKey)
	source, _ := callCtx.InputMetadata.GetValue(MetaLocationSource)
	return out.EmitMap(map[string][]interface{}{
		"seen":       {seen},
		"keys":       {sortedMetadataKeys(callCtx.InputMetadata)},
		"batch_keys": {sortedMetadataKeys(batchMeta)},
		"source":     {source},
	})
}

func sortedMetadataKeys(meta arrow.Metadata) string {
	keys := append([]string(nil), meta.Keys()...)
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

// probeRow is one reported turn.
type probeRow struct {
	seen, keys, batchKeys, source string
}

func (r probeRow) hasKey(key string) bool {
	for _, k := range strings.Split(r.keys, ",") {
		if k == key {
			return true
		}
	}
	return false
}

func newInputMetadataProbeServer(storage *httptest.Server) *Server {
	RegisterStateType(&inputMetadataProbe{})
	s := NewServer()
	if storage != nil {
		// No URL validator: the storage double is plain-HTTP loopback.
		s.SetExternalLocation(&ExternalLocationConfig{HTTPClient: storage.Client()})
	}
	Exchange(s, "input_metadata_probe", inputMetadataProbeSchema, regressionSchema,
		func(context.Context, *CallContext, regressionParams) (*StreamResult, error) {
			return &StreamResult{
				OutputSchema: inputMetadataProbeSchema,
				InputSchema:  regressionSchema,
				State:        &inputMetadataProbe{},
			}, nil
		})
	return s
}

// payloadStore serves one uploaded payload: the object store an externalizing
// client PUTs its oversized input to.
func payloadStore(t *testing.T, payload []byte) (*httptest.Server, string, string) {
	t.Helper()
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/payload" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", arrowContentType)
		_, _ = w.Write(payload)
	}))
	t.Cleanup(store.Close)
	sum := sha256.Sum256(payload)
	return store, store.URL + "/payload", hex.EncodeToString(sum[:])
}

// pointerBody builds the zero-row pointer batch an externalizing client sends
// in place of its input, with extra metadata of its own on top.
func pointerBody(t *testing.T, url, checksum string, extra map[string]string) []byte {
	t.Helper()
	pointer, location := MakeExternalLocationBatch(regressionSchema, url, checksum)
	defer pointer.Release()
	keys := append([]string(nil), location.Keys()...)
	values := append([]string(nil), location.Values()...)
	for k, v := range extra {
		keys = append(keys, k)
		values = append(values, v)
	}
	return regressionIPC(t, pointer, arrow.NewMetadata(keys, values))
}

func inputBody(t *testing.T, value int64, meta map[string]string) []byte {
	t.Helper()
	batch := regressionBatch(t, value)
	defer batch.Release()
	keys := make([]string, 0, len(meta))
	values := make([]string, 0, len(meta))
	for k, v := range meta {
		keys = append(keys, k)
		values = append(values, v)
	}
	return regressionIPC(t, batch, arrow.NewMetadata(keys, values))
}

// readProbeRows decodes a response stream into its reported rows and the last
// continuation cursor it carried, failing on an error batch.
func readProbeRows(t *testing.T, body []byte) ([]probeRow, []byte) {
	t.Helper()
	reader, err := ipc.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("open response: %v", err)
	}
	defer reader.Release()
	var rows []probeRow
	var cursor []byte
	for reader.Next() {
		rec := reader.RecordBatch()
		meta := batchMetadata(rec)
		if level, isLog := meta.GetValue(MetaLogLevel); isLog {
			if level == string(LogException) {
				message, _ := meta.GetValue(MetaLogMessage)
				t.Fatalf("server answered with an error: %s", message)
			}
			continue
		}
		if v, ok := meta.GetValue(MetaStreamState); ok {
			cursor = []byte(v)
		}
		for i := 0; i < int(rec.NumRows()); i++ {
			col := func(name string) string {
				idx := rec.Schema().FieldIndices(name)
				if len(idx) != 1 {
					t.Fatalf("response has no %q column: %s", name, rec.Schema())
				}
				return rec.Column(idx[0]).(*array.String).Value(i)
			}
			rows = append(rows, probeRow{col("seen"), col("keys"), col("batch_keys"), col("source")})
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("read response: %v", err)
	}
	return rows, cursor
}

func postArrow(t *testing.T, h *HttpServer, path string, body []byte) []byte {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", arrowContentType)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: expected 200, got %d: %s", path, rec.Code, rec.Body.String())
	}
	return rec.Body.Bytes()
}

// assertNoTransportKeys fails when either view of a turn's metadata carries
// the transport's bookkeeping, and when the two views disagree.
func assertNoTransportKeys(t *testing.T, turn string, row probeRow) {
	t.Helper()
	for _, key := range []string{MetaStreamState, MetaCallState, MetaCancel} {
		if strings.Contains(row.keys, key) {
			t.Errorf("%s: %s is transport bookkeeping and reached InputMetadata: keys=%q", turn, key, row.keys)
		}
		if strings.Contains(row.batchKeys, key) {
			t.Errorf("%s: %s is transport bookkeeping and rode the input batch handed to Exchange: batch keys=%q",
				turn, key, row.batchKeys)
		}
	}
	if row.batchKeys != row.keys {
		t.Errorf("%s: the input batch and InputMetadata disagree: batch keys=%q, InputMetadata keys=%q",
			turn, row.batchKeys, row.keys)
	}
}

// assertResolvedPayloadMetadata fails unless a turn was handed the fetched
// payload's metadata with the reader's provenance stamp, and nothing of the
// pointer's.
func assertResolvedPayloadMetadata(t *testing.T, turn string, row probeRow, url string) {
	t.Helper()
	if row.seen != "from-payload" {
		t.Errorf("%s: an externalized input's metadata is the fetched payload's, not the pointer's; saw %q (keys=%q)",
			turn, row.seen, row.keys)
	}
	for _, stamped := range []string{MetaLocationSource, MetaLocationFetchMs} {
		if !row.hasKey(stamped) {
			t.Errorf("%s: the reader stamps %s on a resolved input; keys=%q", turn, stamped, row.keys)
		}
	}
	if row.source != url {
		t.Errorf("%s: %s must be the URL that was fetched; got %q, want %q", turn, MetaLocationSource, row.source, url)
	}
	for _, pointerKey := range []string{MetaLocation, MetaLocationSHA256} {
		if row.hasKey(pointerKey) {
			t.Errorf("%s: pointer key %s reached the method; keys=%q", turn, pointerKey, row.keys)
		}
	}
	assertNoTransportKeys(t, turn, row)
}

// TestHTTPExchangeInputMetadata drives one HTTP exchange through three inline
// turns with different metadata each (a frozen or carried-over value fails a
// later turn; the last carries none) and one externalized turn whose pointer
// and payload disagree on the application key.
func TestHTTPExchangeInputMetadata(t *testing.T) {
	// The payload an externalizing client uploaded: its own application key,
	// and -- as an HTTP client's input batch does -- both stream tokens,
	// filled in once /init has issued them.
	var payload []byte
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", arrowContentType)
		_, _ = w.Write(payload)
	}))
	defer store.Close()
	url := store.URL + "/payload"

	h := NewHttpServer(newInputMetadataProbeServer(store))
	h.InitPages()
	const base = "/Service/input_metadata_probe"

	params := regressionBatch(t, 1)
	initBody := postArrow(t, h, base+"/init", regressionRequest(t, "input_metadata_probe", params))
	params.Release()
	cursor, call := FindStreamTokens(initBody)
	if cursor == nil || call == nil {
		t.Fatal("/init response carried no stream tokens")
	}

	turns := []struct {
		name string
		meta map[string]string
		seen string
	}{
		{"turn 1", map[string]string{probeInputKey: "first"}, "first"},
		{"turn 2", map[string]string{probeInputKey: "second", "vgi.test.extra": "1"}, "second"},
		{"turn 3 (no metadata of its own)", map[string]string{}, ""},
	}
	for i, turn := range turns {
		meta := map[string]string{MetaStreamState: string(cursor), MetaCallState: string(call)}
		for k, v := range turn.meta {
			meta[k] = v
		}
		rows, next := readProbeRows(t, postArrow(t, h, base+"/exchange", inputBody(t, int64(i), meta)))
		if len(rows) != 1 {
			t.Fatalf("%s: expected one row per input, got %d", turn.name, len(rows))
		}
		row := rows[0]
		if row.seen != turn.seen {
			t.Errorf("%s: each input's own metadata reaches Exchange; saw %q, want %q (keys=%q)",
				turn.name, row.seen, turn.seen, row.keys)
		}
		if i == 1 && !row.hasKey("vgi.test.extra") {
			t.Errorf("%s: every application key passes, not only one; keys=%q", turn.name, row.keys)
		}
		if i == 2 && row.keys != "" {
			t.Errorf("%s: metadata must not carry over into a later turn; keys=%q", turn.name, row.keys)
		}
		assertNoTransportKeys(t, turn.name, row)
		if next == nil {
			t.Fatalf("%s: exchange response carried no continuation cursor", turn.name)
		}
		cursor = next
	}

	// The externalized turn.
	payload = inputBody(t, 7, map[string]string{
		MetaStreamState: string(cursor),
		MetaCallState:   string(call),
		probeInputKey:   "from-payload",
	})
	sum := sha256.Sum256(payload)
	body := pointerBody(t, url, hex.EncodeToString(sum[:]), map[string]string{
		MetaStreamState: string(cursor),
		MetaCallState:   string(call),
		probeInputKey:   "from-pointer",
	})
	rows, _ := readProbeRows(t, postArrow(t, h, base+"/exchange", body))
	if len(rows) != 1 {
		t.Fatalf("externalized turn: expected one row, got %d", len(rows))
	}
	assertResolvedPayloadMetadata(t, "externalized turn", rows[0], url)
}

// TestPipeExchangeInputMetadata is the same contract over the byte-stream
// transports (pipe, subprocess, unix, tcp all run serveStream): an inline turn,
// an externalized one, and a bare one.
func TestPipeExchangeInputMetadata(t *testing.T) {
	// An HTTP-shaped writer seals its stream tokens into the payload along
	// with its own key; neither token is the method's to see here either.
	payload := inputBody(t, 7, map[string]string{
		MetaStreamState: "sealed-cursor",
		MetaCallState:   "sealed-call",
		probeInputKey:   "from-payload",
	})
	store, url, checksum := payloadStore(t, payload)
	s := newInputMetadataProbeServer(store)

	params := regressionBatch(t, 1)
	request := append([]byte(nil), regressionRequest(t, "input_metadata_probe", params)...)
	params.Release()

	var turns bytes.Buffer
	w := ipc.NewWriter(&turns, ipc.WithSchema(regressionSchema))
	write := func(rec arrow.RecordBatch, meta arrow.Metadata) {
		t.Helper()
		wrapped := array.NewRecordBatchWithMetadata(rec.Schema(), rec.Columns(), rec.NumRows(), meta)
		defer wrapped.Release()
		if err := w.Write(wrapped); err != nil {
			t.Fatal(err)
		}
	}
	first := regressionBatch(t, 1)
	write(first, arrow.NewMetadata([]string{probeInputKey}, []string{"first"}))
	first.Release()
	pointer, location := MakeExternalLocationBatch(regressionSchema, url, checksum)
	write(pointer, arrow.NewMetadata(
		append(append([]string(nil), location.Keys()...), probeInputKey),
		append(append([]string(nil), location.Values()...), "from-pointer")))
	pointer.Release()
	bare := regressionBatch(t, 3)
	write(bare, arrow.Metadata{})
	bare.Release()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	request = append(request, turns.Bytes()...)

	var response bytes.Buffer
	if err := s.serveOne(context.Background(), bytes.NewReader(request), &response, &shmConnState{}); err != nil {
		t.Fatal(err)
	}
	rows, _ := readProbeRows(t, response.Bytes())
	if len(rows) != 3 {
		t.Fatalf("expected one row per input (3), got %d", len(rows))
	}
	if rows[0].seen != "first" || rows[0].keys != probeInputKey {
		t.Errorf("inline turn: want only %s=first, got seen=%q keys=%q", probeInputKey, rows[0].seen, rows[0].keys)
	}
	assertNoTransportKeys(t, "inline turn", rows[0])
	assertResolvedPayloadMetadata(t, "externalized turn", rows[1], url)
	if rows[2].keys != "" || rows[2].batchKeys != "" {
		t.Errorf("bare turn: must see no metadata; keys=%q batch keys=%q", rows[2].keys, rows[2].batchKeys)
	}
}

// TestHTTPProducerInitTickStripsTransportKeys: a producer's first turn runs
// inside /init, so the /init request's metadata is its first tick's -- less
// the transport's keys, exactly as on a continuation tick.
func TestHTTPProducerInitTickStripsTransportKeys(t *testing.T) {
	RegisterStateType(&tickMetaProducer{})
	tickMetaProbe = &tickMetaRecorder{}

	s := NewServer()
	Producer(s, "tick_meta", regressionSchema,
		func(context.Context, *CallContext, regressionParams) (*StreamResult, error) {
			return &StreamResult{OutputSchema: regressionSchema, State: &tickMetaProducer{}}, nil
		})
	h := NewHttpServer(s)
	h.InitPages()

	// A well-formed /init request with a first-tick key of its own and the
	// three transport keys a crafted client could add.
	params := regressionBatch(t, 1)
	reader, err := ipc.NewReader(bytes.NewReader(regressionRequest(t, "tick_meta", params)))
	params.Release()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatal("request carried no batch")
	}
	rec := reader.RecordBatch()
	meta := batchMetadata(rec)
	keys := append(append([]string(nil), meta.Keys()...),
		"vgi_pushdown_filters", MetaStreamState, MetaCallState, MetaCancel)
	values := append(append([]string(nil), meta.Values()...),
		"value < 500", "crafted-cursor", "crafted-call", "1")
	postArrow(t, h, "/Service/tick_meta/init", regressionIPC(t, rec, arrow.NewMetadata(keys, values)))

	ticks := tickMetaProbe.snapshot()
	if len(ticks) != 1 {
		t.Fatalf("expected the /init turn's one produce call, got %d", len(ticks))
	}
	first := ticks[0]
	if got, ok := first.GetValue("vgi_pushdown_filters"); !ok || got != "value < 500" {
		t.Fatalf("the /init request's own metadata must reach the first tick; keys=%v", first.Keys())
	}
	for _, key := range []string{MetaStreamState, MetaCallState, MetaCancel} {
		if v, present := first.GetValue(key); present {
			t.Errorf("transport key %s reached a producer's first tick (value %q)", key, v)
		}
	}
}
