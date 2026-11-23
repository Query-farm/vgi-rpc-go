// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// TestPublicUploadURLContractIsExported guards the __upload_url__ wire
// contract (upload-URL protocol v2) so intermediaries needn't copy it.
// Mirrors the Python reference's test_public_upload_url_contract_is_exported.
func TestPublicUploadURLContractIsExported(t *testing.T) {
	if UploadURLMethod != "__upload_url__" {
		t.Fatalf("expected __upload_url__, got %q", UploadURLMethod)
	}
	if MaxUploadURLCount != 100 {
		t.Fatalf("expected 100, got %d", MaxUploadURLCount)
	}
	if UploadURLProtocolVersion != "2" {
		t.Fatalf("expected upload-URL protocol 2, got %q", UploadURLProtocolVersion)
	}

	if got := UploadURLParamsSchema.NumFields(); got != 1 {
		t.Fatalf("expected 1 params field, got %d", got)
	}
	f := UploadURLParamsSchema.Field(0)
	list, ok := f.Type.(*arrow.ListType)
	if f.Name != "uploads" || !ok {
		t.Fatalf("expected list uploads field, got %s", f)
	}
	entry, ok := list.Elem().(*arrow.StructType)
	if !ok || entry.NumFields() != 2 ||
		entry.Field(0).Name != "size" || entry.Field(0).Type.ID() != arrow.INT64 || entry.Field(0).Nullable ||
		entry.Field(1).Name != "sha256" || entry.Field(1).Type.ID() != arrow.STRING || entry.Field(1).Nullable {
		t.Fatalf("expected struct<size: int64 not null, sha256: utf8 not null>, got %s", list.Elem())
	}

	// object_id and ref: the protocol 3.4 grant columns (STATE_REF_V1.md §10.2).
	wantNames := []string{"upload_url", "download_url", "expires_at", "upload_headers", "object_id", "ref"}
	if got := UploadURLResponseSchema.NumFields(); got != len(wantNames) {
		t.Fatalf("expected %d response fields, got %d", len(wantNames), got)
	}
	for i, name := range wantNames {
		if got := UploadURLResponseSchema.Field(i).Name; got != name {
			t.Fatalf("expected response field %d to be %q, got %q", i, name, got)
		}
	}
	if f := UploadURLResponseSchema.Field(2); f.Type.ID() != arrow.TIMESTAMP {
		t.Fatalf("expected timestamp expires_at, got %s", f.Type)
	}
	if f := UploadURLResponseSchema.Field(3); f.Type.ID() != arrow.MAP {
		t.Fatalf("expected map upload_headers, got %s", f.Type)
	}
	if f := UploadURLResponseSchema.Field(4); f.Type.ID() != arrow.STRING || f.Nullable {
		t.Fatalf("expected utf8 not null object_id, got %s", f)
	}
	if f := UploadURLResponseSchema.Field(5); f.Type.ID() != arrow.BINARY || f.Nullable {
		t.Fatalf("expected binary not null ref, got %s", f)
	}
}

const testSHA = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"

// uploadsBatch frames a request batch the way the client does.
func uploadsBatch(t *testing.T, entries []UploadRequest) arrow.RecordBatch {
	t.Helper()
	builder := array.NewListBuilder(defaultAllocator(), uploadEntryType)
	defer builder.Release()
	values := builder.ValueBuilder().(*array.StructBuilder)
	builder.Append(true)
	for _, e := range entries {
		values.Append(true)
		values.FieldBuilder(0).(*array.Int64Builder).Append(e.Size)
		values.FieldBuilder(1).(*array.StringBuilder).Append(e.SHA256)
	}
	col := builder.NewArray()
	defer col.Release()
	return array.NewRecordBatch(UploadURLParamsSchema, []arrow.Array{col}, 1)
}

func TestParseUploadRequests(t *testing.T) {
	ok := uploadsBatch(t, []UploadRequest{{Size: 10, SHA256: testSHA}, {Size: 1, SHA256: testSHA}})
	defer ok.Release()
	got, err := parseUploadRequests(ok, 0)
	if err != nil || len(got) != 2 || got[0].Size != 10 || got[1].SHA256 != testSHA {
		t.Fatalf("parse = %+v, %v", got, err)
	}

	tooMany := make([]UploadRequest, MaxUploadURLCount+1)
	for i := range tooMany {
		tooMany[i] = UploadRequest{Size: 1, SHA256: testSHA}
	}
	cases := map[string]struct {
		entries []UploadRequest
		max     int64
		want    string
	}{
		"empty":     {nil, 0, "must have 1-100 entries"},
		"too many":  {tooMany, 0, "must have 1-100 entries"},
		"size zero": {[]UploadRequest{{Size: 0, SHA256: testSHA}}, 0, "size must be an integer >= 1, got 0"},
		"upper hex": {[]UploadRequest{{Size: 1, SHA256: strings.ToUpper(testSHA)}}, 0, "64 lowercase hex"},
		"short hex": {[]UploadRequest{{Size: 1, SHA256: testSHA[:63]}}, 0, "64 lowercase hex"},
		"over max":  {[]UploadRequest{{Size: 11, SHA256: testSHA}}, 10, "exceeds VGI-Max-Upload-Bytes"},
	}
	for name, tc := range cases {
		batch := uploadsBatch(t, tc.entries)
		_, err := parseUploadRequests(batch, tc.max)
		batch.Release()
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", name, err, tc.want)
		}
	}

	// The retired v1 shape is refused by name.
	countSchema := arrow.NewSchema([]arrow.Field{{Name: "count", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewInt64Builder(defaultAllocator())
	b.Append(1)
	col := b.NewArray()
	b.Release()
	countBatch := array.NewRecordBatch(countSchema, []arrow.Array{col}, 1)
	col.Release()
	defer countBatch.Release()
	_, err = parseUploadRequests(countBatch, 0)
	if err == nil || !strings.Contains(err.Error(), "retired count shape") ||
		!strings.Contains(err.Error(), "VGI-Upload-URL-Support: 2") {
		t.Fatalf("count shape: %v", err)
	}
}

func TestUploadURLVersionHeader(t *testing.T) {
	for raw, want := range map[string]int{"": 0, "false": 0, "true": 1, "TRUE": 1, "2": 2, "3": 3, "x": 0, "-1": 0, "+2": 0, "02": 0, "0": 0} {
		h := http.Header{}
		if raw != "" {
			h.Set(uploadURLHeader, raw)
		}
		if got := uploadURLVersion(h); got != want {
			t.Errorf("%q: version %d, want %d", raw, got, want)
		}
	}
}

// A client never uploads to a v1 server: the blank URLs it vends would accept
// any bytes, and there is no fallback to the count shape.
func TestClientRefusesV1UploadServer(t *testing.T) {
	c := &HttpClient{requestExternal: &requestExternalizer{}}
	c.observeCapabilities(HTTPServerCapabilities{UploadURLSupport: true, UploadURLVersion: 1, MaxRequestBytes: 4})
	_, err := c.externalizeRequestBody(context.Background(), []byte("too large body"))
	if err == nil || !strings.Contains(err.Error(), "upload protocol is too old") {
		t.Fatalf("err = %v", err)
	}
}

// A request pointer must carry vgi_rpc.location.sha256; the refusal comes
// before any fetch.
func TestRequestPointerRequiresSHA256(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64}}, nil)
	batch, meta := MakeExternalLocationBatch(schema, "https://storage.invalid/obj")
	defer batch.Release()
	_, _, err := ResolveExternalRequestLocation(batch, meta, &ExternalLocationConfig{})
	if err == nil || !strings.Contains(err.Error(), "missing vgi_rpc.location.sha256") {
		t.Fatalf("err = %v", err)
	}
}
