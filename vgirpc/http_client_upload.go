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

// RequestUploadURLs asks the server for one pre-signed upload/download URL
// pair per entry (upload-URL protocol v2), each signed for that entry's exact
// size and SHA-256 where the backend can. The result is in request order, and
// every entry's Headers must be sent on its PUT verbatim.
//
// The server refuses (rather than clamps) an empty list, more than
// [MaxUploadURLCount] entries, a size below 1, a hash that is not 64
// lowercase hex characters, and a size over its VGI-Max-Upload-Bytes. A
// server with no upload-URL provider does not route this endpoint at all and
// answers 404, which surfaces as an RpcError of type NotSupported rather than
// a transport failure: "this server cannot do that" is an answer, not an
// outage.
func (c *HttpClient) RequestUploadURLs(ctx context.Context, uploads []UploadRequest) ([]UploadURL, error) {
	if c == nil {
		return nil, fmt.Errorf("vgirpc: HTTP client is nil")
	}
	body, err := c.uploadURLRequestBody(uploads)
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
	// Deliberately not enforced against [UploadURLResponseSchema]. The
	// columns are what matters and they are read by name; the reference marks
	// them nullable where this port does not, and refusing the whole response
	// over a nullability flag would deny a client the upload route on the one
	// peer whose behaviour defines it.
	parsed, err := c.parseMain(response, nil, true)
	if err != nil {
		return nil, err
	}
	defer parsed.release()
	urls := make([]UploadURL, 0, len(uploads))
	for _, batch := range parsed.batches {
		decoded, derr := decodeUploadURLBatch(batch.Batch)
		if derr != nil {
			return nil, derr
		}
		urls = append(urls, decoded...)
	}
	if len(urls) != len(uploads) {
		return nil, &RpcError{
			Type:    "ProtocolError",
			Message: fmt.Sprintf("server returned %d upload URLs for %d requested", len(urls), len(uploads)),
		}
	}
	return urls, nil
}

// uploadURLRequestBody frames the one-row uploads batch this endpoint expects.
//
// Built here rather than through initialBody because that stamps the client's
// routing key, and this request must not carry one.
func (c *HttpClient) uploadURLRequestBody(uploads []UploadRequest) ([]byte, error) {
	requestID, err := clientRequestID()
	if err != nil {
		return nil, err
	}
	builder := array.NewListBuilder(defaultAllocator(), uploadEntryType)
	defer builder.Release()
	entries := builder.ValueBuilder().(*array.StructBuilder)
	sizes := entries.FieldBuilder(0).(*array.Int64Builder)
	hashes := entries.FieldBuilder(1).(*array.StringBuilder)
	builder.Append(true)
	for _, upload := range uploads {
		entries.Append(true)
		sizes.Append(upload.Size)
		hashes.Append(upload.SHA256)
	}
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

// decodeUploadURLBatch reads one (upload_url, download_url, expires_at,
// upload_headers) batch.
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
	headers, _ := column("upload_headers").(*array.Map)
	var headerKeys, headerItems *array.String
	if headers != nil {
		headerKeys, _ = headers.Keys().(*array.String)
		headerItems, _ = headers.Items().(*array.String)
		if headerKeys == nil || headerItems == nil {
			return nil, &RpcError{Type: "ProtocolError", Message: "upload URL response 'upload_headers' is not map<utf8, utf8>"}
		}
	}
	// Protocol 3.4 grant columns, read by name and optional: a server
	// without them still answers a v2 client.
	objectIDs, _ := column("object_id").(*array.String)
	refs, _ := column("ref").(*array.Binary)
	out := make([]UploadURL, 0, int(batch.NumRows()))
	for row := range int(batch.NumRows()) {
		entry := UploadURL{Headers: map[string]string{}}
		if objectIDs != nil && !objectIDs.IsNull(row) {
			entry.ObjectID = objectIDs.Value(row)
		}
		if refs != nil && !refs.IsNull(row) {
			entry.Ref = append([]byte(nil), refs.Value(row)...)
		}
		if headers != nil && !headers.IsNull(row) {
			start, end := headers.ValueOffsets(row)
			for i := int(start); i < int(end); i++ {
				if headerItems.IsNull(i) {
					continue
				}
				entry.Headers[headerKeys.Value(i)] = headerItems.Value(i)
			}
		}
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
