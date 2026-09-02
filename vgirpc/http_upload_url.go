// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"fmt"
	"net/http"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// The public __upload_url__ wire contract. An intermediary that terminates
// or serves the upload-URL flow needs the method name, both schemas, and
// the count cap; exporting them here means it doesn't have to copy them.
// Mirrors the Python reference's vgi_rpc.http exports (UPLOAD_URL_METHOD /
// UPLOAD_URL_PARAMS_SCHEMA / UPLOAD_URL_RESPONSE_SCHEMA /
// MAX_UPLOAD_URL_COUNT).
const (
	// UploadURLMethod is the synthetic method name a client sends to
	// POST {prefix}/__upload_url__/init.
	UploadURLMethod = "__upload_url__"
	// MaxUploadURLCount caps the number of URL pairs generated per request.
	MaxUploadURLCount = 100
)

// UploadURLParamsSchema is the request schema for the __upload_url__/init
// endpoint: a single int64 "count" column with the number of URL pairs to
// generate. Mirrors the Python _UPLOAD_URL_PARAMS_SCHEMA.
var UploadURLParamsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "count", Type: arrow.PrimitiveTypes.Int64},
}, nil)

// UploadURLResponseSchema is the response schema for the __upload_url__/init
// endpoint. Mirrors the Python _UPLOAD_URL_SCHEMA so cross-language
// clients can decode the response.
var UploadURLResponseSchema = arrow.NewSchema([]arrow.Field{
	{Name: "upload_url", Type: arrow.BinaryTypes.String},
	{Name: "download_url", Type: arrow.BinaryTypes.String},
	{Name: "expires_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
}, nil)

// handleUploadURLInit serves POST {prefix}/__upload_url__/init.
//
// Request: an Arrow IPC stream with vgi_rpc.method="__upload_url__" and a
// single int64 "count" column containing the number of URL pairs to
// generate. Response: an Arrow IPC stream with one batch of (upload_url,
// download_url, expires_at) rows.
func (h *HttpServer) handleUploadURLInit(w http.ResponseWriter, r *http.Request) {
	if h.uploadURLProvider == nil {
		http.NotFound(w, r)
		return
	}
	if h.authenticateIdentity(w, r) == nil {
		return
	}
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

	count := extractCount(req.Batch)
	if count < 1 {
		count = 1
	}
	if count > MaxUploadURLCount {
		count = MaxUploadURLCount
	}

	urls := make([]UploadURL, 0, count)
	for range count {
		u, gerr := h.uploadURLProvider.GenerateUploadURL(arrow.NewSchema(nil, nil))
		if gerr != nil {
			h.writeHttpError(w, http.StatusInternalServerError, gerr, UploadURLResponseSchema)
			return
		}
		urls = append(urls, u)
	}

	mem := defaultAllocator()
	uploadB := array.NewStringBuilder(mem)
	defer uploadB.Release()
	downloadB := array.NewStringBuilder(mem)
	defer downloadB.Release()
	expiresB := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})
	defer expiresB.Release()

	for _, u := range urls {
		uploadB.Append(u.UploadURL)
		downloadB.Append(u.DownloadURL)
		expiresB.Append(arrow.Timestamp(u.ExpiresAt.UTC().UnixMicro()))
	}

	uploadArr := uploadB.NewArray()
	defer uploadArr.Release()
	downloadArr := downloadB.NewArray()
	defer downloadArr.Release()
	expiresArr := expiresB.NewArray()
	defer expiresArr.Release()

	resultBatch := array.NewRecordBatch(UploadURLResponseSchema,
		[]arrow.Array{uploadArr, downloadArr, expiresArr}, int64(len(urls)))
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
			writeErrorResponse(&buf, UploadURLResponseSchema, capErr, h.server.serverID, "", h.server.debugErrors))
		h.writeArrow(w, http.StatusInternalServerError, buf.Bytes())
		return
	}
	h.writeArrow(w, http.StatusOK, buf.Bytes())
}

// extractCount pulls the int64 "count" field from the upload-URL request
// batch. Returns 1 if the field is absent or not an int64 (mirrors the
// Python default).
func extractCount(batch arrow.RecordBatch) int {
	for i := 0; i < int(batch.NumCols()); i++ {
		if batch.Schema().Field(i).Name != "count" {
			continue
		}
		col := batch.Column(i)
		if intArr, ok := col.(*array.Int64); ok && intArr.Len() > 0 {
			return int(intArr.Value(0))
		}
		return 1
	}
	return 1
}
