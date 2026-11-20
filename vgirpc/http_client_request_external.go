// © Copyright 2025-2026, Query.Farm LLC - https://query.farm

package vgirpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// Client-vended request externalization.
//
// A server caps the request body it will accept inline and answers 413 above
// it -- and then holds a door open: {prefix}/__upload_url__ vends a signed PUT
// the client uploads to, after which the request itself is a zero-row pointer
// batch naming the download URL. The Go client had the response half of this
// protocol and not the request half, so a large request simply failed against
// any server with a cap, and the door the server was holding open went unused.
//
// Two triggers, matching the reference. Pre-emptive when the advertised cap is
// already known and the body exceeds it; reactive on a 413, which also warms
// the cap so the next oversized body skips the wasted round trip. Neither ever
// probes OPTIONS speculatively: a client that discovers capabilities it does
// not need has paid for nothing.

// requestExternalizer holds the capability snapshot the request path needs.
type requestExternalizer struct {
	mu   sync.Mutex
	caps *HTTPServerCapabilities
}

// maybeExternalizeRequest replaces an oversized body with a pointer body when
// the cap is already known to reject it.
//
// Returns the body unchanged whenever the answer is not already in hand: the
// 413 path below is the fallback, and it is cheaper to be told once than to
// probe before every call.
func (c *HttpClient) maybeExternalizeRequest(ctx context.Context, body []byte) ([]byte, bool, error) {
	caps := c.cachedCapabilities()
	if caps == nil || !caps.UploadURLSupport || caps.MaxRequestBytes <= 0 {
		return body, false, nil
	}
	if int64(len(body)) <= caps.MaxRequestBytes {
		return body, false, nil
	}
	// A failure here is reported rather than swallowed. Falling back to
	// sending the oversized body anyway buys a guaranteed 413 and a second
	// attempt at the same upload, and then reports the 413 -- so a rejected
	// URL, a refused PUT or a storage outage all surface as "request too
	// large", which is the one thing that is not wrong with them.
	externalized, err := c.externalizeRequestBody(ctx, body)
	if err != nil {
		return nil, false, err
	}
	return externalized, true, nil
}

// cachedCapabilities returns the advertised capabilities if any response has
// already carried them, without issuing a probe.
func (c *HttpClient) cachedCapabilities() *HTTPServerCapabilities {
	if c == nil || c.requestExternal == nil {
		return nil
	}
	c.requestExternal.mu.Lock()
	defer c.requestExternal.mu.Unlock()
	return c.requestExternal.caps
}

// observeCapabilities records what a response advertised.
func (c *HttpClient) observeCapabilities(caps HTTPServerCapabilities) {
	if c == nil || c.requestExternal == nil {
		return
	}
	c.requestExternal.mu.Lock()
	defer c.requestExternal.mu.Unlock()
	snapshot := caps
	c.requestExternal.caps = &snapshot
}

// externalizeRequestBody uploads body to a server-vended URL and returns the
// pointer body that replaces it.
func (c *HttpClient) externalizeRequestBody(ctx context.Context, body []byte) ([]byte, error) {
	caps := c.cachedCapabilities()
	if caps == nil {
		discovered, err := c.DiscoverCapabilities(ctx)
		if err != nil {
			return nil, err
		}
		c.observeCapabilities(discovered)
		caps = &discovered
	}
	if !caps.UploadURLSupport {
		return nil, &RpcError{
			Type:    "RequestTooLarge",
			Message: "request exceeds max_request_bytes and the server does not advertise upload_url_support",
		}
	}
	if caps.MaxUploadBytes > 0 && int64(len(body)) > caps.MaxUploadBytes {
		return nil, &RpcError{
			Type:    "RequestTooLarge",
			Message: fmt.Sprintf("request of %d bytes exceeds the advertised max_upload_bytes of %d", len(body), caps.MaxUploadBytes),
		}
	}
	// v2 only. A v1 server vends blank URLs that storage accepts any bytes
	// on, and asks for a count this client no longer sends; there is no
	// fallback to that shape.
	if caps.UploadURLVersion < uploadURLProtocolVersion {
		return nil, &RpcError{
			Type: "ProtocolError",
			Message: fmt.Sprintf("Server upload protocol is too old: it advertises %s v%d and this client needs %d",
				uploadURLHeader, caps.UploadURLVersion, uploadURLProtocolVersion),
		}
	}
	if caps.UploadURLVersion > uploadURLProtocolVersion {
		return nil, &RpcError{
			Type: "ProtocolError",
			Message: fmt.Sprintf("Server upload protocol is newer than this client: it advertises %s: %d and this client speaks %d",
				uploadURLHeader, caps.UploadURLVersion, uploadURLProtocolVersion),
		}
	}
	digest := sha256.Sum256(body)
	sha256Hex := hex.EncodeToString(digest[:])
	urls, err := c.RequestUploadURLs(ctx, []UploadRequest{{Size: int64(len(body)), SHA256: sha256Hex}})
	if err != nil {
		return nil, err
	}
	if len(urls) == 0 {
		return nil, &RpcError{Type: "ProtocolError", Message: "server returned no upload URLs"}
	}
	// The validator that guards a *download* guards an upload too: both are
	// addresses the peer chose, and the PUT is the one that carries data out.
	if c.external != nil && c.external.URLValidator != nil {
		if err := c.external.URLValidator(urls[0].UploadURL); err != nil {
			return nil, &RpcError{Type: "ProtocolError", Message: fmt.Sprintf("upload URL rejected by validator: %v", err)}
		}
		if err := c.external.URLValidator(urls[0].DownloadURL); err != nil {
			return nil, &RpcError{Type: "ProtocolError", Message: fmt.Sprintf("download URL rejected by validator: %v", err)}
		}
	}
	if err := c.putExternalRequest(ctx, urls[0].UploadURL, urls[0].Headers, body); err != nil {
		return nil, err
	}
	return buildPointerRequestBody(body, urls[0].DownloadURL, sha256Hex)
}

// uploadURLProtocolVersion is [UploadURLProtocolVersion] as a number.
const uploadURLProtocolVersion = 2

// uploadContentType is the PUT's Content-Type when the vended headers name
// none: application/octet-stream, not the Arrow media type, because a URL
// that signs the content type (GCS) is signed for octet-stream. Matches the
// reference's _UPLOAD_CONTENT_TYPE.
const uploadContentType = "application/octet-stream"

// storageUploadClient carries request uploads to vended URLs. It is bare on
// purpose -- not the RPC client, whose cookie jar or credential-injecting
// transport would otherwise reach the storage host (which may also refuse a
// second auth mechanism) -- and it never follows a redirect, which could carry
// the signed URL somewhere nobody vetted.
var storageUploadClient = &http.Client{CheckRedirect: noRedirects}

// putExternalRequest uploads the request body to a vended URL with exactly
// the vended headers.
//
// Deliberately without the client's default headers, cookies or transport:
// the target is object storage rather than the worker, the URL carries its own
// signature, and an extra header can invalidate one. The body goes with a
// plain Content-Length (never chunked), which a sized URL's signature covers;
// a Content-Length among the vended headers is ignored for that reason.
// Errors name the URL without its (signed) query.
func (c *HttpClient) putExternalRequest(ctx context.Context, uploadURL string, headers map[string]string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, uploadURL, bytes.NewReader(body))
	if err != nil {
		return &RpcError{Type: "ExternalUploadFailed", Message: fmt.Sprintf("build upload request for %s", redactExternalURL(uploadURL))}
	}
	req.ContentLength = int64(len(body))
	req.Header.Set("Content-Type", uploadContentType)
	for name, value := range headers {
		if strings.EqualFold(name, "Content-Length") {
			continue
		}
		req.Header.Set(name, value)
	}
	resp, err := storageUploadClient.Do(req)
	if err != nil {
		return &RpcError{Type: "ExternalUploadFailed", Message: fmt.Sprintf("PUT to upload URL %s failed", redactExternalURL(uploadURL))}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &RpcError{
			Type:    "ExternalUploadFailed",
			Message: fmt.Sprintf("PUT to upload URL failed: HTTP %d", resp.StatusCode),
		}
	}
	return nil
}

// buildPointerRequestBody rewrites an inline request as a pointer request.
//
// The outer batch keeps the original's custom metadata -- method, routing key,
// request id, stream cursor -- because the server routes on the outer batch
// and only then resolves the pointer to read the parameters. Strip any of it
// and the request no longer names what it is.
func buildPointerRequestBody(originalBody []byte, locationURL, sha256Hex string) ([]byte, error) {
	reader, err := ipc.NewReader(bytes.NewReader(originalBody))
	if err != nil {
		return nil, &RpcError{Type: "ProtocolError", Message: fmt.Sprintf("read request body for externalization: %v", err)}
	}
	defer reader.Release()
	if !reader.Next() {
		return nil, &RpcError{Type: "ProtocolError", Message: "request body carried no batch to externalize"}
	}
	record := reader.RecordBatch()
	metadata := recordMetadata(record)
	pointer, pointerMeta := MakeExternalLocationBatch(record.Schema(), locationURL, sha256Hex)
	defer pointer.Release()
	for i, key := range pointerMeta.Keys() {
		metadata[key] = pointerMeta.Values()[i]
	}
	keys := make([]string, 0, len(metadata))
	values := make([]string, 0, len(metadata))
	for key, value := range metadata {
		keys = append(keys, key)
		values = append(values, value)
	}
	annotated := array.NewRecordBatchWithMetadata(
		pointer.Schema(), pointer.Columns(), pointer.NumRows(), arrow.NewMetadata(keys, values))
	defer annotated.Release()
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(pointer.Schema()))
	if err := writer.Write(annotated); err != nil {
		_ = writer.Close()
		return nil, &RpcError{Type: "ProtocolError", Message: fmt.Sprintf("write pointer request body: %v", err)}
	}
	if err := writer.Close(); err != nil {
		return nil, &RpcError{Type: "ProtocolError", Message: fmt.Sprintf("close pointer request body: %v", err)}
	}
	return buf.Bytes(), nil
}
