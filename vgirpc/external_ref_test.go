// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/klauspost/compress/zstd"
)

// Language-local coverage for ExternalRef / PublishExternal (the Go API
// surface) and the unexported dispatch path. The wire behaviour itself is
// covered cross-language by the shared TestExternalRef group.

var (
	stringType    = reflect.TypeOf("")
	refParamsType = reflect.TypeOf(refParams{})
)

const validDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestNewExternalRefValidation(t *testing.T) {
	cases := []struct {
		name, url, digest string
		ok                bool
	}{
		{"url and digest", "https://x/y", validDigest, true},
		{"no digest", "https://x/y", "", true},
		{"empty url", "", validDigest, false},
		{"uppercase digest", "https://x/y", strings.ToUpper(validDigest), false},
		{"short digest", "https://x/y", validDigest[:63], false},
		{"long digest", "https://x/y", validDigest + "0", false},
		{"non-hex digest", "https://x/y", "g" + validDigest[1:], false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ref, err := NewExternalRef(tc.url, tc.digest)
			if tc.ok {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if ref.URL() != tc.url || ref.SHA256() != tc.digest {
					t.Fatalf("ref = (%q, %q), want (%q, %q)", ref.URL(), ref.SHA256(), tc.url, tc.digest)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected an error for url=%q digest=%q", tc.url, tc.digest)
			}
		})
	}
}

func TestExternalRefPointerBatch(t *testing.T) {
	for _, digest := range []string{validDigest, ""} {
		ref, err := NewExternalRef("https://x/y", digest)
		if err != nil {
			t.Fatal(err)
		}
		batch, meta := ref.PointerBatch(regressionSchema)
		if batch.NumRows() != 0 || !batch.Schema().Equal(regressionSchema) {
			t.Fatalf("pointer batch: %d rows, schema %v", batch.NumRows(), batch.Schema())
		}
		batch.Release()
		if loc, _ := metaGet(meta, MetaLocation); loc != "https://x/y" {
			t.Fatalf("location = %q", loc)
		}
		got, has := metaGet(meta, MetaLocationSHA256)
		if has != (digest != "") || got != digest {
			t.Fatalf("sha256 key = (%q, %v), want %q", got, has, digest)
		}
	}
}

// readSingleBatch decodes an uploaded IPC stream, requiring exactly one batch.
func readSingleBatch(t *testing.T, data []byte) arrow.RecordBatch {
	t.Helper()
	r, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	var out arrow.RecordBatch
	for r.Next() {
		if out != nil {
			t.Fatal("uploaded stream holds more than one batch")
		}
		out = r.RecordBatch()
		out.Retain()
	}
	if err := r.Err(); err != nil {
		t.Fatal(err)
	}
	if out == nil {
		t.Fatal("uploaded stream holds no batch")
	}
	return out
}

func TestPublishExternal(t *testing.T) {
	t.Run("uncompressed", func(t *testing.T) {
		storage := newMockStorage()
		batch := regressionBatch(t, 7)
		defer batch.Release()
		ref, err := PublishExternal(batch, storage, nil, true)
		if err != nil {
			t.Fatal(err)
		}
		if storage.counter != 1 {
			t.Fatalf("uploads = %d, want 1", storage.counter)
		}
		if storage.lastContentEncoding != "" {
			t.Fatalf("content encoding = %q, want none", storage.lastContentEncoding)
		}
		data := storage.data[ref.URL()]
		sum := sha256.Sum256(data)
		if ref.SHA256() != hex.EncodeToString(sum[:]) {
			t.Fatalf("ref digest %q does not match the uploaded bytes", ref.SHA256())
		}
		got := readSingleBatch(t, data)
		defer got.Release()
		if got.NumRows() != 1 || got.Column(0).(*array.Int64).Value(0) != 7 {
			t.Fatalf("uploaded batch does not round-trip")
		}
		// Byte-for-byte what the per-call externalizer would have uploaded.
		want, err := serializeBatchAsIPC(batch, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, want) {
			t.Fatal("published bytes differ from the externalizer's serialization")
		}
	})

	t.Run("zstd digest covers raw bytes", func(t *testing.T) {
		storage := newMockStorage()
		batch := regressionBatch(t, 8)
		defer batch.Release()
		ref, err := PublishExternal(batch, storage, &Compression{Algorithm: "zstd", Level: 3}, true)
		if err != nil {
			t.Fatal(err)
		}
		if storage.lastContentEncoding != "zstd" {
			t.Fatalf("content encoding = %q, want zstd", storage.lastContentEncoding)
		}
		dec, err := zstd.NewReader(nil)
		if err != nil {
			t.Fatal(err)
		}
		defer dec.Close()
		raw, err := dec.DecodeAll(storage.data[ref.URL()], nil)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		if ref.SHA256() != hex.EncodeToString(sum[:]) {
			t.Fatal("digest must be taken over the pre-compression bytes")
		}
	})

	t.Run("without digest", func(t *testing.T) {
		storage := newMockStorage()
		batch := regressionBatch(t, 9)
		defer batch.Release()
		ref, err := PublishExternal(batch, storage, nil, false)
		if err != nil {
			t.Fatal(err)
		}
		if ref.SHA256() != "" || ref.URL() == "" || storage.counter != 1 {
			t.Fatalf("ref = (%q, %q), uploads %d", ref.URL(), ref.SHA256(), storage.counter)
		}
	})

	t.Run("rejects non-1-row batches and nil storage", func(t *testing.T) {
		storage := newMockStorage()
		for _, n := range []int{0, 2} {
			batch := makeBatch(n)
			if _, err := PublishExternal(batch, storage, nil, true); err == nil {
				t.Fatalf("expected an error for a %d-row batch", n)
			}
			batch.Release()
		}
		if storage.counter != 0 {
			t.Fatalf("a rejected batch was uploaded")
		}
		batch := regressionBatch(t, 1)
		defer batch.Release()
		if _, err := PublishExternal(batch, nil, nil, true); err == nil {
			t.Fatal("expected an error without storage")
		}
	})
}

func TestPublishExternalResult(t *testing.T) {
	storage := newMockStorage()
	ref, err := PublishExternalResult("hello", storage, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	got := readSingleBatch(t, storage.data[ref.URL()])
	defer got.Release()
	want, err := resultSchema(stringType)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Schema().Equal(want) {
		t.Fatalf("schema %v, want the Unary result schema %v", got.Schema(), want)
	}
	if got.NumRows() != 1 || got.Column(0).(*array.String).Value(0) != "hello" {
		t.Fatal("uploaded value does not round-trip")
	}
}

type refParams struct {
	Digest bool `vgirpc:"digest"`
}

// dispatchUnary runs one unary call over the raw byte-stream or HTTP path and
// returns the response body.
func dispatchUnary(t *testing.T, s *Server, transport, method string, params arrow.RecordBatch, configure func(*HttpServer)) []byte {
	t.Helper()
	body := regressionRequest(t, method, params)
	if transport == "raw" {
		var response bytes.Buffer
		if err := s.serveOne(context.Background(), bytes.NewReader(body), &response, &shmConnState{}); err != nil {
			t.Fatal(err)
		}
		return response.Bytes()
	}
	h := NewHttpServer(s)
	if configure != nil {
		configure(h)
	}
	h.InitPages()
	req := httptest.NewRequest(http.MethodPost, "/Service/"+method, bytes.NewReader(body))
	req.Header.Set("Content-Type", arrowContentType)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if w.Header().Get("X-VGI-RPC-Error") != "" {
		t.Fatalf("response flagged as an error: %q", w.Body.String())
	}
	return w.Body.Bytes()
}

// dataBatches returns the non-log batches of a response stream with metadata.
func dataBatches(t *testing.T, body []byte) ([]arrow.RecordBatch, []arrow.Metadata, *arrow.Schema) {
	t.Helper()
	r, err := ipc.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	var batches []arrow.RecordBatch
	var metas []arrow.Metadata
	for r.Next() {
		rec := r.RecordBatch()
		meta := batchMetadata(rec)
		if _, isLog := metaGet(meta, MetaLogLevel); isLog {
			continue
		}
		rec.Retain()
		batches = append(batches, rec)
		metas = append(metas, meta)
	}
	if err := r.Err(); err != nil {
		t.Fatal(err)
	}
	return batches, metas, r.Schema()
}

func TestUnaryExternalRefWritesPointer(t *testing.T) {
	for _, transport := range []string{"raw", "http"} {
		for _, withStorage := range []bool{false, true} {
			name := transport + "/no-storage"
			if withStorage {
				name = transport + "/storage-threshold-1"
			}
			t.Run(name, func(t *testing.T) {
				storage := &recordingStorage{}
				s := NewServer()
				if withStorage {
					// Even with storage and a 1-byte threshold, a ref is
					// written as-is: nothing is built or uploaded.
					s.SetExternalLocation(&ExternalLocationConfig{Storage: storage, ExternalizeThresholdBytes: 1})
				}
				Unary(s, "ref", func(_ context.Context, call *CallContext, p refParams) (string, error) {
					digest := ""
					if p.Digest {
						digest = validDigest
					}
					ref, err := NewExternalRef("https://published.invalid/object", digest)
					if err != nil {
						return "", err
					}
					call.ClientLog(LogInfo, "answering with a ref")
					return "ignored", call.RespondWithExternalRef(ref)
				})
				for _, digest := range []bool{true, false} {
					params := refParamsBatch(t, digest)
					body := dispatchUnary(t, s, transport, "ref", params, func(h *HttpServer) {
						// A ref uploads nothing, so the externalized cap
						// cannot refuse it however small.
						h.SetMaxExternalizedResponseBytes(1)
					})
					params.Release()
					batches, metas, schema := dataBatches(t, body)
					if len(batches) != 1 {
						t.Fatalf("expected one data batch, got %d", len(batches))
					}
					if batches[0].NumRows() != 0 {
						t.Fatalf("pointer batch has %d rows", batches[0].NumRows())
					}
					want, _ := resultSchema(stringType)
					if !schema.Equal(want) {
						t.Fatalf("response schema %v, want %v", schema, want)
					}
					batches[0].Release()
					if loc, _ := metaGet(metas[0], MetaLocation); loc != "https://published.invalid/object" {
						t.Fatalf("location = %q", loc)
					}
					got, has := metaGet(metas[0], MetaLocationSHA256)
					if has != digest || (digest && got != validDigest) {
						t.Fatalf("sha256 key = (%q, %v) with digest=%v", got, has, digest)
					}
				}
				if storage.uploads != 0 {
					t.Fatalf("a ref was uploaded %d times", storage.uploads)
				}
			})
		}
	}
}

func refParamsBatch(t *testing.T, digest bool) arrow.RecordBatch {
	t.Helper()
	schema, err := structToSchema(refParamsType)
	if err != nil {
		t.Fatal(err)
	}
	b := array.NewBooleanBuilder(defaultAllocator())
	defer b.Release()
	b.Append(digest)
	col := b.NewArray()
	defer col.Release()
	return array.NewRecordBatch(schema, []arrow.Array{col}, 1)
}

func TestRespondWithExternalRefRefusals(t *testing.T) {
	ref, err := NewExternalRef("https://published.invalid/object", "")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("void method", func(t *testing.T) {
		for _, transport := range []string{"raw", "http"} {
			s := NewServer()
			UnaryVoid(s, "void_ref", func(_ context.Context, call *CallContext, _ regressionParams) error {
				return call.RespondWithExternalRef(ref)
			})
			params := regressionBatch(t, 1)
			body := regressionRequest(t, "void_ref", params)
			params.Release()
			if transport == "raw" {
				var response bytes.Buffer
				if err := s.serveOne(context.Background(), bytes.NewReader(body), &response, &shmConnState{}); err != nil {
					t.Fatal(err)
				}
				requireRuntimeError(t, response.Bytes(), "only supported by unary methods that return a value")
				continue
			}
			h := NewHttpServer(s)
			h.InitPages()
			req := httptest.NewRequest(http.MethodPost, "/Service/void_ref", bytes.NewReader(body))
			req.Header.Set("Content-Type", arrowContentType)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			requireRuntimeError(t, w.Body.Bytes(), "only supported by unary methods that return a value")
		}
	})

	t.Run("zero-value ref", func(t *testing.T) {
		call := &CallContext{externalRefAllowed: true}
		if err := call.RespondWithExternalRef(ExternalRef{}); err == nil {
			t.Fatal("expected the zero-value ref to be refused")
		}
		if call.externalRef != nil {
			t.Fatal("a refused ref was recorded")
		}
	})

	t.Run("handler error wins", func(t *testing.T) {
		s := NewServer()
		Unary(s, "ref_then_fail", func(_ context.Context, call *CallContext, _ regressionParams) (string, error) {
			_ = call.RespondWithExternalRef(ref)
			return "", &RpcError{Type: "RuntimeError", Message: "handler failed after all"}
		})
		params := regressionBatch(t, 1)
		body := regressionRequest(t, "ref_then_fail", params)
		params.Release()
		var response bytes.Buffer
		if err := s.serveOne(context.Background(), bytes.NewReader(body), &response, &shmConnState{}); err != nil {
			t.Fatal(err)
		}
		requireRuntimeError(t, response.Bytes(), "handler failed after all")
	})
}
