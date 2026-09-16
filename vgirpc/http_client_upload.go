// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Upload URLs, client half.
//
// The server has vended these since external storage landed; nothing on this
// side asked for them, so a Go client facing a request too large to send
// inline had no way to take the route the server was holding open for it.
//
// This is a bootstrap call and is shaped like one. It goes to the framework
// route {prefix}/__upload_url__/init, below any protocol, and it carries no
// vgi_rpc.protocol key -- deliberately, and the Python reference says so in as
// many words: a request with no routing key is structurally exempt from the
// server's dispatch-boundary check, which is what lets a client ask for upload
// URLs before it has decided which protocol it is going to speak.

// RequestUploadURLs asks the server for count pre-signed upload/download URL
// pairs.
//
// count is clamped by the server to [1, MaxUploadURLCount]. A server with no
// upload-URL provider does not route this endpoint at all and answers 404,
// which surfaces as an RpcError of type NotSupported rather than a transport
// failure: "this server cannot do that" is an answer, not an outage.
func (c *HttpClient) RequestUploadURLs(ctx context.Context, count int) ([]UploadURL, error) {
	if c == nil {
		return nil, fmt.Errorf("vgirpc: HTTP client is nil")
	}
	if count < 1 {
		count = 1
	}
	body, err := c.uploadURLRequestBody(count)
	if err != nil {
		return nil, err
	}
	response, err := c.post(ctx, UploadURLMethod+"/init", body)
	if err != nil {
		var status *HTTPStatusError
		if errors.As(err, &status) && status.StatusCode == 404 {
			return nil, &RpcError{Type: "NotSupported", Message: "server does not support upload URLs"}
		}
		return nil, err
	}
	// Deliberately not enforced against [UploadURLResponseSchema]. The three
	// columns are what matters and they are read by name; the reference marks
	// them nullable where this port does not, and refusing the whole response
	// over a nullability flag would deny a client the upload route on the one
	// peer whose behaviour defines it.
	parsed, err := c.parseMain(response, nil, true)
	if err != nil {
		return nil, err
	}
	defer parsed.release()
	urls := make([]UploadURL, 0, count)
	for _, batch := range parsed.batches {
		decoded, derr := decodeUploadURLBatch(batch.Batch)
		if derr != nil {
			return nil, derr
		}
		urls = append(urls, decoded...)
	}
	return urls, nil
}

// uploadURLRequestBody frames the one-row count batch this endpoint expects.
//
// Built here rather than through initialBody because that stamps the client's
// routing key, and this request must not carry one.
func (c *HttpClient) uploadURLRequestBody(count int) ([]byte, error) {
	requestID, err := clientRequestID()
	if err != nil {
		return nil, err
	}
	builder := array.NewInt64Builder(defaultAllocator())
	defer builder.Release()
	builder.Append(int64(count))
	column := builder.NewArray()
	defer column.Release()
	batch := array.NewRecordBatch(UploadURLParamsSchema, []arrow.Array{column}, 1)
	defer batch.Release()
	return encodeClientBatch(batch, map[string]string{
		MetaMethod:         UploadURLMethod,
		MetaRequestVersion: ProtocolVersion,
		MetaRequestID:      requestID,
	}, c.maxRequest)
}

// decodeUploadURLBatch reads one (upload_url, download_url, expires_at) batch.
//
// By name rather than by position: this is a framework endpoint whose reply a
// client decodes without a compiled declaration to check it against, so the
// column order is the server's business.
func decodeUploadURLBatch(batch arrow.RecordBatch) ([]UploadURL, error) {
	column := func(name string) arrow.Array {
		for i, field := range batch.Schema().Fields() {
			if field.Name == name {
				return batch.Column(i)
			}
		}
		return nil
	}
	uploads, ok := column("upload_url").(*array.String)
	if !ok {
		return nil, &RpcError{Type: "ProtocolError", Message: "upload URL response has no string 'upload_url' column"}
	}
	downloads, ok := column("download_url").(*array.String)
	if !ok {
		return nil, &RpcError{Type: "ProtocolError", Message: "upload URL response has no string 'download_url' column"}
	}
	expiries, _ := column("expires_at").(*array.Timestamp)
	unit := arrow.Microsecond
	if expiries != nil {
		if timestampType, isTimestamp := expiries.DataType().(*arrow.TimestampType); isTimestamp {
			unit = timestampType.Unit
		}
	}
	out := make([]UploadURL, 0, int(batch.NumRows()))
	for row := range int(batch.NumRows()) {
		entry := UploadURL{}
		if !uploads.IsNull(row) {
			entry.UploadURL = uploads.Value(row)
		}
		if !downloads.IsNull(row) {
			entry.DownloadURL = downloads.Value(row)
		}
		if expiries != nil && !expiries.IsNull(row) {
			entry.ExpiresAt = expiries.Value(row).ToTime(unit).UTC()
		}
		out = append(out, entry)
	}
	return out, nil
}
