// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// The public __upload_url__ wire contract (upload-URL protocol v2). An
// intermediary that terminates or serves the upload-URL flow needs the method
// name, both schemas, and the entry cap; exporting them here means it doesn't
// have to copy them. Mirrors the Python reference's vgi_rpc.http exports
// (UPLOAD_URL_METHOD / UPLOAD_URL_PARAMS_SCHEMA / UPLOAD_URL_RESPONSE_SCHEMA /
// MAX_UPLOAD_URL_COUNT).
const (
	// UploadURLMethod is the synthetic method name a client sends to
	// POST {prefix}/__upload_url__/init.
	UploadURLMethod = "__upload_url__"
	// MaxUploadURLCount caps the number of entries in one request's
	// uploads list; a request with more is refused, not clamped.
	MaxUploadURLCount = 100
	// UploadURLProtocolVersion is the VGI-Upload-URL-Support value this
	// implementation advertises and requires: v2, the sized (size, sha256)
	// request. v1 ("true", a bare count) is retired with no fallback.
	UploadURLProtocolVersion = "2"
)

// uploadEntryType is the element of the request's uploads list: one entry
// per upload, the exact byte length and lowercase hex SHA-256 of the body the
// client will PUT.
var uploadEntryType = arrow.StructOf(
	arrow.Field{Name: "size", Type: arrow.PrimitiveTypes.Int64},
	arrow.Field{Name: "sha256", Type: arrow.BinaryTypes.String},
)

// UploadURLParamsSchema is the request schema for the __upload_url__/init
// endpoint: a single non-null uploads column of
// list<struct<size: int64 not null, sha256: utf8 not null>>, 1 to
// [MaxUploadURLCount] entries.
var UploadURLParamsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "uploads", Type: arrow.ListOf(uploadEntryType)},
}, nil)

// UploadURLResponseSchema is the response schema for the __upload_url__/init
// endpoint: one row per request entry, in order. upload_headers are the
// headers the client must send on the PUT, verbatim (possibly none).
// object_id and ref (protocol 3.4, STATE_REF_V1.md §10.2) are the storage id
// of the object the PUT creates and a sealed argument reference for it, bound
// to the requesting principal; clients read every column by name and may
// ignore these two.
var UploadURLResponseSchema = arrow.NewSchema([]arrow.Field{
	{Name: "upload_url", Type: arrow.BinaryTypes.String},
	{Name: "download_url", Type: arrow.BinaryTypes.String},
	{Name: "expires_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
	{Name: "upload_headers", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)},
	{Name: "object_id", Type: arrow.BinaryTypes.String},
	{Name: "ref", Type: arrow.BinaryTypes.Binary},
}, nil)

// UploadRequest is one entry of an __upload_url__/init request.
type UploadRequest struct {
	// Size is the exact byte length of the body; at least 1.
	Size int64
	// SHA256 is the body's SHA-256, 64 lowercase hex characters (the same
	// encoding as vgi_rpc.location.sha256).
	SHA256 string
}

// handleUploadURLInit serves POST {prefix}/__upload_url__/init.
//
// Request: an Arrow IPC stream with vgi_rpc.method="__upload_url__" and one
// row whose uploads column lists a (size, sha256) entry per upload. Response:
// an Arrow IPC stream with one batch of (upload_url, download_url, expires_at,
// upload_headers) rows, one per entry, in order. Malformed requests -- the
// retired count shape included -- are refused with 400 before any URL is
// minted.
func (h *HttpServer) handleUploadURLInit(w http.ResponseWriter, r *http.Request) {
	if h.uploadURLProvider == nil {
		http.NotFound(w, r)
		return
	}
	// Authenticated exactly like a protocol method -- same authenticators,
	// same peer-evidence binding, same anonymous fallback -- because the
	// argument references minted below are bound to this AuthContext and are
	// opened later under the AuthContext of the call presenting them.
	identity := h.authenticateIdentity(w, r)
	if identity == nil {
		return
	}
	auth := identity.auth
	var budgetOK bool
	r, budgetOK = h.applyResponseBudget(w, r, UploadURLResponseSchema)
	if !budgetOK {
		return
	}
	if ct := r.Header.Get("Content-Type"); ct != arrowContentType {
		h.writeHttpError(w, http.StatusUnsupportedMediaType,
			fmt.Errorf("unsupported content type: %s", ct), UploadURLResponseSchema)
		return
	}

	body, err := h.readHTTPBody(r)
	if err != nil {
		h.writeBodyReadError(w, err, UploadURLResponseSchema)
		return
	}

	req, err := ReadRequest(bytes.NewReader(body))
	if err != nil {
		h.writeHttpError(w, http.StatusBadRequest, err, UploadURLResponseSchema)
		return
	}
	defer req.Batch.Release()

	if req.Method != UploadURLMethod {
		h.writeHttpError(w, http.StatusBadRequest, &RpcError{
			Type:    "TypeError",
			Message: fmt.Sprintf("Method mismatch: expected %q, got %q", UploadURLMethod, req.Method),
		}, UploadURLResponseSchema)
		return
	}

	entries, refusal := parseUploadRequests(req.Batch, h.effectiveMaxUploadBytes())
	if refusal != nil {
		h.writeHttpError(w, http.StatusBadRequest, refusal, UploadURLResponseSchema)
		return
	}

	urls := make([]UploadURL, 0, len(entries))
	for _, entry := range entries {
		u, gerr := h.uploadURLProvider.GenerateUploadURL(entry.Size, entry.SHA256)
		if gerr != nil {
			h.writeHttpError(w, http.StatusInternalServerError, gerr, UploadURLResponseSchema)
			return
		}
		if u.ObjectID == "" {
			h.writeHttpError(w, http.StatusInternalServerError, &RpcError{Type: "RuntimeError",
				Message: fmt.Sprintf("%T.GenerateUploadURL returned no ObjectID (required since vgi-rpc protocol 3.4)", h.uploadURLProvider)},
				UploadURLResponseSchema)
			return
		}
		// One sealed argument reference per entry, minted from the claimed
		// (size, sha256) under this request's AuthContext.
		ref, merr := MintArgumentRef(h.tokenKey, auth, entry.Size, entry.SHA256, u.ObjectID)
		if merr != nil {
			h.writeHttpError(w, http.StatusInternalServerError, merr, UploadURLResponseSchema)
			return
		}
		u.Ref = ref
		urls = append(urls, u)
	}

	resultBatch := buildUploadURLBatch(urls)
	defer resultBatch.Release()

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(UploadURLResponseSchema))
	if werr := writer.Write(resultBatch); werr != nil {
		h.logIPCWriteErr("upload-url-batch", UploadURLMethod, werr)
	}
	if cerr := writer.Close(); cerr != nil {
		h.logIPCWriteErr("close", UploadURLMethod, cerr)
	}
	budget := responseBudgetFromContext(r.Context())
	if capErr := enforceResponseBudgets(UploadURLMethod, int64(buf.Len()), 0, budget.Limit, 0); capErr != nil {
		buf.Reset()
		h.logIPCWriteErr("upload-url-cap-error", UploadURLMethod,
			writeErrorResponse(&buf, UploadURLResponseSchema, capErr, h.server.serverID, "", h.server.traceFor(UploadURLMethod)))
		h.writeArrow(w, http.StatusInternalServerError, buf.Bytes())
		return
	}
	h.writeArrow(w, http.StatusOK, buf.Bytes())
}

// uploadRefusal is the error every malformed request gets: a 400 carrying an
// RPC error batch, before any URL is minted.
func uploadRefusal(format string, args ...any) error {
	return &RpcError{Type: "ValueError", Message: fmt.Sprintf(format, args...)}
}

// parseUploadRequests reads and validates the v2 request: the uploads list of
// (size, sha256) entries. A request in the retired v1 shape (a count column,
// or no uploads column at all) is refused by name, so a stale client learns
// what to upgrade to rather than getting a URL it cannot use. maxUploadBytes
// (0 = unconfigured) bounds every entry's size.
func parseUploadRequests(batch arrow.RecordBatch, maxUploadBytes int64) ([]UploadRequest, error) {
	schema := batch.Schema()
	uploadsIdx := -1
	for i, field := range schema.Fields() {
		if field.Name == "count" {
			return nil, retiredCountShape()
		}
		if field.Name == "uploads" {
			uploadsIdx = i
		}
	}
	if uploadsIdx < 0 {
		return nil, retiredCountShape()
	}
	if batch.NumRows() != 1 {
		return nil, uploadRefusal("__upload_url__ request expects exactly one row, got %d", batch.NumRows())
	}
	list, ok := batch.Column(uploadsIdx).(*array.List)
	if !ok {
		return nil, uploadRefusal("__upload_url__ 'uploads' must be list<struct<size: int64, sha256: utf8>>, got %s", schema.Field(uploadsIdx).Type)
	}
	if list.IsNull(0) {
		return nil, uploadRefusal("__upload_url__ 'uploads' must have 1-%d entries, got null", MaxUploadURLCount)
	}
	elems, ok := list.ListValues().(*array.Struct)
	if !ok {
		return nil, uploadRefusal("__upload_url__ 'uploads' must be list<struct<size: int64, sha256: utf8>>, got %s", schema.Field(uploadsIdx).Type)
	}
	structType := elems.DataType().(*arrow.StructType)
	sizeIdx, hasSize := structType.FieldIdx("size")
	shaIdx, hasSHA := structType.FieldIdx("sha256")
	if !hasSize || !hasSHA {
		return nil, uploadRefusal("__upload_url__ 'uploads' entries must have size and sha256 fields, got %s", structType)
	}
	sizes, ok := elems.Field(sizeIdx).(*array.Int64)
	if !ok {
		return nil, uploadRefusal("__upload_url__ uploads[].size must be int64, got %s", structType.Field(sizeIdx).Type)
	}
	hashes, ok := elems.Field(shaIdx).(*array.String)
	if !ok {
		return nil, uploadRefusal("__upload_url__ uploads[].sha256 must be utf8, got %s", structType.Field(shaIdx).Type)
	}
	start, end := list.ValueOffsets(0)
	n := int(end - start)
	if n < 1 || n > MaxUploadURLCount {
		return nil, uploadRefusal("__upload_url__ 'uploads' must have 1-%d entries, got %d", MaxUploadURLCount, n)
	}
	out := make([]UploadRequest, 0, n)
	for i := 0; i < n; i++ {
		row := int(start) + i
		// Per entry, in the reference's order: size, then sha256, then the
		// upload cap.
		if elems.IsNull(row) || sizes.IsNull(row) {
			return nil, uploadRefusal("__upload_url__ uploads[%d].size must be an integer >= 1, got null", i)
		}
		size := sizes.Value(row)
		if size < 1 {
			return nil, uploadRefusal("__upload_url__ uploads[%d].size must be an integer >= 1, got %d", i, size)
		}
		if hashes.IsNull(row) || !isLowerHexSHA256(hashes.Value(row)) {
			return nil, uploadRefusal("__upload_url__ uploads[%d].sha256 must be 64 lowercase hex characters", i)
		}
		hash := hashes.Value(row)
		if maxUploadBytes > 0 && size > maxUploadBytes {
			return nil, uploadRefusal("__upload_url__ uploads[%d].size %d exceeds VGI-Max-Upload-Bytes (%d)", i, size, maxUploadBytes)
		}
		out = append(out, UploadRequest{Size: size, SHA256: hash})
	}
	return out, nil
}

// retiredCountShape refuses an upload-URL v1 request.
func retiredCountShape() error {
	return uploadRefusal("__upload_url__ request uses the retired count shape: this server requires " +
		"VGI-Upload-URL-Support: 2 and an 'uploads' parameter (list<struct<size: int64, sha256: utf8>>, one entry per upload)")
}

// buildUploadURLBatch frames the response rows. upload_headers is always
// present, empty when the backend signs nothing beyond the URL, with keys
// sorted so the wire is deterministic.
func buildUploadURLBatch(urls []UploadURL) arrow.RecordBatch {
	mem := defaultAllocator()
	uploadB := array.NewStringBuilder(mem)
	defer uploadB.Release()
	downloadB := array.NewStringBuilder(mem)
	defer downloadB.Release()
	expiresB := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})
	defer expiresB.Release()
	headersB := array.NewMapBuilderWithType(mem, UploadURLResponseSchema.Field(3).Type.(*arrow.MapType))
	defer headersB.Release()
	objectIDB := array.NewStringBuilder(mem)
	defer objectIDB.Release()
	refB := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer refB.Release()
	keyB := headersB.KeyBuilder().(*array.StringBuilder)
	itemB := headersB.ItemBuilder().(*array.StringBuilder)

	for _, u := range urls {
		uploadB.Append(u.UploadURL)
		downloadB.Append(u.DownloadURL)
		objectIDB.Append(u.ObjectID)
		refB.Append(u.Ref)
		expiresB.Append(arrow.Timestamp(u.ExpiresAt.UTC().UnixMicro()))
		headersB.Append(true)
		names := make([]string, 0, len(u.Headers))
		for name := range u.Headers {
			// Never Content-Length: the client sends a plain one for exactly
			// the declared size, and a provider's copy (the vendor grant's
			// required_headers carries one) is not for it to repeat.
			if strings.EqualFold(name, "Content-Length") {
				continue
			}
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			keyB.Append(name)
			itemB.Append(u.Headers[name])
		}
	}

	uploadArr := uploadB.NewArray()
	defer uploadArr.Release()
	downloadArr := downloadB.NewArray()
	defer downloadArr.Release()
	expiresArr := expiresB.NewArray()
	defer expiresArr.Release()
	headersArr := headersB.NewArray()
	defer headersArr.Release()
	objectIDArr := objectIDB.NewArray()
	defer objectIDArr.Release()
	refArr := refB.NewArray()
	defer refArr.Release()
	return array.NewRecordBatch(UploadURLResponseSchema,
		[]arrow.Array{uploadArr, downloadArr, expiresArr, headersArr, objectIDArr, refArr}, int64(len(urls)))
}
