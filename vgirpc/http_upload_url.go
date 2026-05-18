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

const (
	uploadURLMethod    = "__upload_url__"
	maxUploadURLCount  = 100
)

// uploadURLSchema is the response schema for the __upload_url__/init
// endpoint. Mirrors the Python _UPLOAD_URL_SCHEMA so cross-language
// clients can decode the response.
var uploadURLSchema = arrow.NewSchema([]arrow.Field{
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
	if ct := r.Header.Get("Content-Type"); ct != arrowContentType {
		h.writeHttpError(w, http.StatusUnsupportedMediaType,
			fmt.Errorf("unsupported content type: %s", ct), uploadURLSchema)
		return
	}

	body, err := h.readHTTPBody(r)
	if err != nil {
		h.writeBodyReadError(w, err, uploadURLSchema)
		return
	}

	req, err := ReadRequest(bytes.NewReader(body))
	if err != nil {
		h.writeHttpError(w, http.StatusBadRequest, err, uploadURLSchema)
		return
	}
	defer req.Batch.Release()

	if req.Method != uploadURLMethod {
		h.writeHttpError(w, http.StatusBadRequest, &RpcError{
			Type:    "TypeError",
			Message: fmt.Sprintf("Method mismatch: expected %q, got %q", uploadURLMethod, req.Method),
		}, uploadURLSchema)
		return
	}

	count := extractCount(req.Batch)
	if count < 1 {
		count = 1
	}
	if count > maxUploadURLCount {
		count = maxUploadURLCount
	}

	urls := make([]UploadURL, 0, count)
	for range count {
		u, gerr := h.uploadURLProvider.GenerateUploadURL(arrow.NewSchema(nil, nil))
		if gerr != nil {
			h.writeHttpError(w, http.StatusInternalServerError, gerr, uploadURLSchema)
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

	resultBatch := array.NewRecordBatch(uploadURLSchema,
		[]arrow.Array{uploadArr, downloadArr, expiresArr}, int64(len(urls)))
	defer resultBatch.Release()

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(uploadURLSchema))
	if werr := writer.Write(resultBatch); werr != nil {
		h.logIPCWriteErr("upload-url-batch", uploadURLMethod, werr)
	}
	if cerr := writer.Close(); cerr != nil {
		h.logIPCWriteErr("close", uploadURLMethod, cerr)
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

