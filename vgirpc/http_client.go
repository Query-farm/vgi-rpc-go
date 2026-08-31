// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

const (
	defaultClientMaxRequestBytes         = int64(256 << 20)
	defaultClientMaxEncodedResponseBytes = int64(256 << 20)
	defaultClientMaxDecodedResponseBytes = int64(256 << 20)
)

// ClientBatch is one Arrow result returned by [HttpClient]. The caller owns
// Batch and must call Release when finished. Metadata excludes framework-only
// continuation tokens.
type ClientBatch struct {
	Batch    arrow.RecordBatch
	Metadata map[string]string
}

// Release releases the Arrow buffers owned by b. It is safe to call once on a
// nil ClientBatch.
func (b *ClientBatch) Release() {
	if b != nil && b.Batch != nil {
		b.Batch.Release()
		b.Batch = nil
	}
}

// ClientStreamSchema declares the exact Arrow schemas for a stream. Input is
// required for exchange streams and must be nil for producers. Output is
// always required. Header is optional.
//
// Every batch sent through Exchange is checked against Input before any bytes
// are written. This makes all-null and zero-row batches retain their declared
// types, child fields, and nullability instead of relying on value inference.
type ClientStreamSchema struct {
	Input  *arrow.Schema
	Output *arrow.Schema
	Header *arrow.Schema
}

// ClientLogHandler receives client-directed log batches. It runs synchronously
// while each response body is parsed, but concurrent calls on one HttpClient
// may invoke it concurrently. Handlers must be concurrency-safe and return
// promptly.
type ClientLogHandler func(LogMessage)

type httpClientConfig struct {
	inner            *http.Client
	prefix           string
	headers          http.Header
	protocolVersion  string
	maxRequest       int64
	maxEncoded       int64
	maxDecoded       int64
	onLog            ClientLogHandler
	closeIdleOnClose bool
	tcpProxy         string
	customHTTPClient bool
}

// HttpClientOption configures [NewHttpClient].
type HttpClientOption func(*httpClientConfig) error

// WithClientHTTPClient supplies the net/http client used for all requests.
// Ownership stays with the caller; [HttpClient.Close] will not close its idle
// connections.
func WithClientHTTPClient(client *http.Client) HttpClientOption {
	return func(cfg *httpClientConfig) error {
		if client == nil {
			return errors.New("vgirpc: client HTTP client must not be nil")
		}
		cfg.inner = client
		cfg.closeIdleOnClose = false
		cfg.customHTTPClient = true
		return nil
	}
}

// WithClientTCPProxy routes every HTTP connection through an explicit
// SOCKS5h proxy. The target hostname is resolved by the proxy, which is
// required for Tailscale userspace networking and MagicDNS names. Only NO
// AUTH is supported. Proxy failure never falls back to a direct connection.
// This option cannot be combined with [WithClientHTTPClient].
func WithClientTCPProxy(proxyURL string) HttpClientOption {
	return func(cfg *httpClientConfig) error {
		if _, err := newSOCKS5HDialer(proxyURL); err != nil {
			return err
		}
		cfg.tcpProxy = proxyURL
		return nil
	}
}

// WithClientPrefix mounts RPC endpoints below prefix (for example, "/vgi").
// The default is the server root.
func WithClientPrefix(prefix string) HttpClientOption {
	return func(cfg *httpClientConfig) error {
		if strings.ContainsAny(prefix, "?#") {
			return fmt.Errorf("vgirpc: invalid client prefix %q", prefix)
		}
		if prefix == "" || prefix == "/" {
			cfg.prefix = ""
			return nil
		}
		cfg.prefix = "/" + strings.Trim(prefix, "/")
		return nil
	}
}

// WithClientHeader adds a header sent on every request, such as Authorization.
func WithClientHeader(name, value string) HttpClientOption {
	return func(cfg *httpClientConfig) error {
		if strings.TrimSpace(name) == "" || strings.ContainsAny(name, "\r\n") {
			return errors.New("vgirpc: invalid client header name")
		}
		if strings.ContainsAny(value, "\r\n") {
			return errors.New("vgirpc: invalid client header value")
		}
		cfg.headers.Add(name, value)
		return nil
	}
}

// WithClientProtocolVersion stamps the application's declared protocol
// version on unary and stream-init requests.
func WithClientProtocolVersion(version string) HttpClientOption {
	return func(cfg *httpClientConfig) error {
		if version != "" {
			if _, _, _, err := parseSemver(version); err != nil {
				return err
			}
		}
		cfg.protocolVersion = version
		return nil
	}
}

// WithClientResponseLimits independently caps encoded network bytes and the
// decoded response body. Both limits must be positive.
func WithClientResponseLimits(maxEncoded, maxDecoded int64) HttpClientOption {
	return func(cfg *httpClientConfig) error {
		if maxEncoded <= 0 || maxDecoded <= 0 {
			return errors.New("vgirpc: client response limits must be positive")
		}
		cfg.maxEncoded = maxEncoded
		cfg.maxDecoded = maxDecoded
		return nil
	}
}

// WithClientRequestLimit caps the encoded Arrow request body. The limit must
// be positive.
func WithClientRequestLimit(maxBytes int64) HttpClientOption {
	return func(cfg *httpClientConfig) error {
		if maxBytes <= 0 {
			return errors.New("vgirpc: client request limit must be positive")
		}
		cfg.maxRequest = maxBytes
		return nil
	}
}

// WithClientLogHandler installs a callback for client-directed log batches.
func WithClientLogHandler(handler ClientLogHandler) HttpClientOption {
	return func(cfg *httpClientConfig) error {
		cfg.onLog = handler
		return nil
	}
}

// HttpClient is a blocking native client for the stateless VGI-RPC HTTP
// transport. It is safe to use concurrently for unary calls and to own
// multiple independent streams. A single [HttpClientStream] must be driven by
// only one goroutine at a time.
type HttpClient struct {
	baseURL          *url.URL
	inner            *http.Client
	prefix           string
	headers          http.Header
	protocolVersion  string
	maxRequest       int64
	maxEncoded       int64
	maxDecoded       int64
	onLog            ClientLogHandler
	closeIdleOnClose bool
	closed           atomic.Bool
}

// HTTPStatusError is a transport-level non-2xx HTTP response. Detail is a
// bounded text response and RequestID is the server's X-Request-ID correlation
// value, when present.
type HTTPStatusError struct {
	StatusCode int
	Detail     string
	RequestID  string
}

func (e *HTTPStatusError) Error() string {
	if e.RequestID != "" {
		return fmt.Sprintf("HTTP %d: %s (request_id=%s)", e.StatusCode, e.Detail, e.RequestID)
	}
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, e.Detail)
}

// NewHttpClient constructs a native HTTP RPC client. baseURL must be an
// absolute http(s) URL without credentials, query, or fragment. The default
// request timeout is 30 seconds and the default request/response caps are
// 256 MiB.
func NewHttpClient(baseURL string, options ...HttpClientOption) (*HttpClient, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("vgirpc: invalid client base URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("vgirpc: unsupported client URL scheme %q", u.Scheme)
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("vgirpc: client base URL must not contain credentials, query, or fragment")
	}
	u.Path = strings.TrimRight(u.Path, "/")
	cfg := httpClientConfig{
		inner:            &http.Client{Timeout: 30 * time.Second},
		headers:          make(http.Header),
		maxRequest:       defaultClientMaxRequestBytes,
		maxEncoded:       defaultClientMaxEncodedResponseBytes,
		maxDecoded:       defaultClientMaxDecodedResponseBytes,
		closeIdleOnClose: true,
	}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&cfg); err != nil {
			return nil, err
		}
	}
	if cfg.tcpProxy != "" {
		if cfg.customHTTPClient {
			return nil, errors.New("vgirpc: WithClientTCPProxy cannot be combined with WithClientHTTPClient")
		}
		dialer, err := newSOCKS5HDialer(cfg.tcpProxy)
		if err != nil {
			return nil, err
		}
		cfg.inner.Transport = &http.Transport{Proxy: nil, DialContext: dialer.DialContext}
	}
	return &HttpClient{
		baseURL:          u,
		inner:            cfg.inner,
		prefix:           cfg.prefix,
		headers:          cfg.headers.Clone(),
		protocolVersion:  cfg.protocolVersion,
		maxRequest:       cfg.maxRequest,
		maxEncoded:       cfg.maxEncoded,
		maxDecoded:       cfg.maxDecoded,
		onLog:            cfg.onLog,
		closeIdleOnClose: cfg.closeIdleOnClose,
	}, nil
}

// Close prevents new RPCs and closes idle connections owned by the default
// net/http client. It does not close an injected client. Active streams remain
// caller-owned and should be closed before the client.
func (c *HttpClient) Close() {
	if c == nil || c.closed.Swap(true) {
		return
	}
	if c.closeIdleOnClose {
		c.inner.CloseIdleConnections()
	}
}

// CallUnary calls a unary method and returns its one data batch. expected is
// the exact declared result schema; pass nil only for intentionally dynamic
// methods. The caller owns the returned batch.
func (c *HttpClient) CallUnary(
	ctx context.Context,
	method string,
	params arrow.RecordBatch,
	expected *arrow.Schema,
) (*ClientBatch, error) {
	body, err := c.initialBody(method, params)
	if err != nil {
		return nil, err
	}
	response, err := c.post(ctx, method, body)
	if err != nil {
		return nil, err
	}
	parsed, err := c.parseMain(response, expected, true)
	if err != nil {
		return nil, err
	}
	defer parsed.releaseExceptFirst()
	if len(parsed.batches) != 1 {
		return nil, &RpcError{Type: "ProtocolError", Message: fmt.Sprintf("unary response contained %d data batches", len(parsed.batches))}
	}
	result := parsed.batches[0]
	parsed.batches[0] = nil
	return result, nil
}

// OpenProducer starts a producer stream. schemas.Input must be nil and
// schemas.Output must be the declared output schema.
func (c *HttpClient) OpenProducer(
	ctx context.Context,
	method string,
	params arrow.RecordBatch,
	schemas ClientStreamSchema,
) (*HttpClientStream, error) {
	if schemas.Input != nil {
		return nil, errors.New("vgirpc: producer input schema must be nil")
	}
	return c.openStream(ctx, method, params, schemas, false)
}

// OpenExchange starts a lockstep exchange stream. Both Input and Output are
// required and are enforced exactly for every exchange turn.
func (c *HttpClient) OpenExchange(
	ctx context.Context,
	method string,
	params arrow.RecordBatch,
	schemas ClientStreamSchema,
) (*HttpClientStream, error) {
	if schemas.Input == nil {
		return nil, errors.New("vgirpc: exchange input schema is required")
	}
	return c.openStream(ctx, method, params, schemas, true)
}

func (c *HttpClient) openStream(
	ctx context.Context,
	method string,
	params arrow.RecordBatch,
	schemas ClientStreamSchema,
	exchange bool,
) (*HttpClientStream, error) {
	if schemas.Output == nil {
		return nil, errors.New("vgirpc: stream output schema is required")
	}
	body, err := c.initialBody(method, params)
	if err != nil {
		return nil, err
	}
	response, err := c.post(ctx, method+"/init", body)
	if err != nil {
		return nil, err
	}
	raw := bytes.NewReader(response.body)
	var header *ClientBatch
	if schemas.Header != nil {
		parsedHeader, err := c.parseIPCStream(raw, schemas.Header, true)
		if err != nil {
			return nil, response.wrap(err)
		}
		defer parsedHeader.release()
		if len(parsedHeader.batches) != 1 {
			return nil, &RpcError{Type: "ProtocolError", Message: "stream header response did not contain exactly one batch"}
		}
		header = parsedHeader.batches[0]
		parsedHeader.batches[0] = nil
	}
	parsed, err := c.parseIPCStream(raw, schemas.Output, false)
	if err != nil {
		if header != nil {
			header.Release()
		}
		return nil, response.wrap(err)
	}
	if raw.Len() != 0 {
		parsed.release()
		if header != nil {
			header.Release()
		}
		return nil, &RpcError{Type: "ProtocolError", Message: "trailing bytes after stream init response"}
	}
	if exchange {
		if len(parsed.batches) != 0 {
			parsed.release()
			if header != nil {
				header.Release()
			}
			return nil, &RpcError{Type: "ProtocolError", Message: "exchange init response contained unexpected data"}
		}
		if parsed.token == "" || parsed.callToken == "" {
			parsed.release()
			if header != nil {
				header.Release()
			}
			return nil, &RpcError{Type: "ProtocolError", Message: "exchange init response must contain cursor and call tokens"}
		}
	} else if len(parsed.batches) > 1 {
		count := len(parsed.batches)
		parsed.release()
		if header != nil {
			header.Release()
		}
		return nil, &RpcError{Type: "ProtocolError", Message: fmt.Sprintf("producer init response contained %d data batches", count)}
	}
	if response.status < 200 || response.status >= 300 || response.rpcError {
		parsed.release()
		if header != nil {
			header.Release()
		}
		if response.status < 200 || response.status >= 300 {
			return nil, &RpcError{Type: "TransportError", Message: fmt.Sprintf("HTTP %d: %s", response.status, boundedText(response.body))}
		}
		return nil, &RpcError{Type: "ProtocolError", Message: "stream init declared an RPC error but contained no exception envelope"}
	}
	return &HttpClientStream{
		client:    c,
		method:    method,
		schemas:   schemas,
		exchange:  exchange,
		header:    header,
		pending:   parsed.batches,
		token:     parsed.token,
		callToken: parsed.callToken,
		finished:  parsed.token == "",
	}, nil
}

func (c *HttpClient) initialBody(method string, params arrow.RecordBatch) ([]byte, error) {
	if err := validateMethod(method); err != nil {
		return nil, err
	}
	requestID, err := clientRequestID()
	if err != nil {
		return nil, err
	}
	metadata := recordMetadata(params)
	stripClientControlMetadata(metadata)
	metadata[MetaMethod] = method
	metadata[MetaRequestVersion] = ProtocolVersion
	metadata[MetaRequestID] = requestID
	if c.protocolVersion != "" {
		metadata[MetaProtocolVersion] = c.protocolVersion
	}
	return encodeClientBatch(params, metadata, c.maxRequest)
}

type clientHTTPResponse struct {
	status   int
	body     []byte
	rpcError bool
}

func (r clientHTTPResponse) wrap(err error) error {
	var rpcErr *RpcError
	if errors.As(err, &rpcErr) {
		return err
	}
	return err
}

func (c *HttpClient) post(ctx context.Context, endpoint string, body []byte) (clientHTTPResponse, error) {
	if c == nil || c.closed.Load() {
		return clientHTTPResponse{}, errors.New("vgirpc: HTTP client is closed")
	}
	if int64(len(body)) > c.maxRequest {
		return clientHTTPResponse{}, &RpcError{Type: "TransportError", Message: fmt.Sprintf("request body exceeds client limit (%d > %d bytes)", len(body), c.maxRequest)}
	}
	u := *c.baseURL
	u.Path = strings.TrimRight(c.baseURL.Path, "/") + c.prefix + "/" + endpoint
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return clientHTTPResponse{}, fmt.Errorf("vgirpc: build HTTP request: %w", err)
	}
	req.Header = c.headers.Clone()
	req.Header.Set("Content-Type", arrowContentType)
	req.Header.Set(customAcceptEncodingHeader, "zstd, gzip, identity")
	req.Header.Set(acceptEncodingHeader, "zstd, gzip, identity")
	resp, err := c.inner.Do(req)
	if err != nil {
		return clientHTTPResponse{}, &RpcError{Type: "TransportError", Message: fmt.Sprintf("HTTP request failed: %v", err)}
	}
	defer resp.Body.Close()
	if resp.ContentLength > c.maxEncoded {
		return clientHTTPResponse{}, &RpcError{Type: "TransportError", Message: fmt.Sprintf("encoded HTTP response exceeds client limit (%d > %d bytes)", resp.ContentLength, c.maxEncoded)}
	}
	encoded, err := io.ReadAll(io.LimitReader(resp.Body, c.maxEncoded+1))
	if err != nil {
		return clientHTTPResponse{}, &RpcError{Type: "TransportError", Message: fmt.Sprintf("read HTTP response: %v", err)}
	}
	if int64(len(encoded)) > c.maxEncoded {
		return clientHTTPResponse{}, &RpcError{Type: "TransportError", Message: fmt.Sprintf("encoded HTTP response exceeds client limit (%d bytes)", c.maxEncoded)}
	}
	encoding := strings.TrimSpace(resp.Header.Get(contentEncodingHeader))
	if encoding == "" {
		encoding = strings.TrimSpace(resp.Header.Get(customContentEncodingHeader))
	}
	if err := validateClientContentEncoding(encoding); err != nil {
		return clientHTTPResponse{}, err
	}
	decoded := encoded
	if encoding != "" && !strings.EqualFold(encoding, identityEncoding) {
		decoded, err = DecodeContentEncoding(encoded, encoding, c.maxDecoded)
		if err != nil {
			return clientHTTPResponse{}, &RpcError{Type: "TransportError", Message: fmt.Sprintf("decode HTTP response: %v", err)}
		}
	}
	if int64(len(decoded)) > c.maxDecoded {
		return clientHTTPResponse{}, &RpcError{Type: "TransportError", Message: fmt.Sprintf("decoded HTTP response exceeds client limit (%d > %d bytes)", len(decoded), c.maxDecoded)}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return clientHTTPResponse{}, &HTTPStatusError{
			StatusCode: resp.StatusCode,
			Detail:     boundedText(decoded),
			RequestID:  resp.Header.Get(requestIDHeader),
		}
	}
	return clientHTTPResponse{
		status:   resp.StatusCode,
		body:     decoded,
		rpcError: strings.EqualFold(resp.Header.Get(rpcErrorHeader), "true"),
	}, nil
}

func (c *HttpClient) parseMain(response clientHTTPResponse, expected *arrow.Schema, tokenIsData bool) (*parsedClientStream, error) {
	raw := bytes.NewReader(response.body)
	parsed, err := c.parseIPCStream(raw, expected, tokenIsData)
	if err != nil {
		return nil, response.wrap(err)
	}
	if raw.Len() != 0 {
		parsed.release()
		return nil, &RpcError{Type: "ProtocolError", Message: "trailing bytes after HTTP RPC response"}
	}
	if response.rpcError {
		parsed.release()
		return nil, &RpcError{Type: "ProtocolError", Message: "HTTP response declared an RPC error but contained no exception envelope"}
	}
	return parsed, nil
}

// HttpClientStream owns one producer or exchange lifecycle. Header, Next, and
// Exchange return owned batches that the caller must Release. Close is
// idempotent and releases any unread batches.
type HttpClientStream struct {
	client    *HttpClient
	method    string
	schemas   ClientStreamSchema
	exchange  bool
	header    *ClientBatch
	pending   []*ClientBatch
	token     string
	callToken string
	finished  bool
	closed    bool
}

// Header returns an owned retained copy of the optional stream header. The
// caller must Release it. A nil result means the stream declared no header.
func (s *HttpClientStream) Header() *ClientBatch {
	if s == nil || s.header == nil {
		return nil
	}
	s.header.Batch.Retain()
	metadata := make(map[string]string, len(s.header.Metadata))
	for key, value := range s.header.Metadata {
		metadata[key] = value
	}
	return &ClientBatch{Batch: s.header.Batch, Metadata: metadata}
}

// Finished reports whether the worker has ended this stream.
func (s *HttpClientStream) Finished() bool { return s.finished }

// Next returns the next producer batch. ok is false at end-of-stream. The
// caller owns a returned batch and must Release it.
func (s *HttpClientStream) Next(ctx context.Context) (batch *ClientBatch, ok bool, err error) {
	if s.closed {
		return nil, false, errors.New("vgirpc: stream is closed")
	}
	if s.exchange {
		return nil, false, errors.New("vgirpc: Next is only valid on producer streams")
	}
	for {
		if len(s.pending) > 0 {
			batch = s.pending[0]
			s.pending = s.pending[1:]
			return batch, true, nil
		}
		if s.finished || s.token == "" {
			s.finished = true
			return nil, false, nil
		}
		body, err := s.continuationBody(false, nil)
		if err != nil {
			return nil, false, err
		}
		response, err := s.client.post(ctx, s.method+"/exchange", body)
		if err != nil {
			return nil, false, err
		}
		parsed, err := s.client.parseMain(response, s.schemas.Output, false)
		if err != nil {
			return nil, false, err
		}
		if len(parsed.batches) > 1 {
			count := len(parsed.batches)
			parsed.release()
			return nil, false, &RpcError{Type: "ProtocolError", Message: fmt.Sprintf("producer response contained %d data batches", count)}
		}
		s.pending = parsed.batches
		s.token = parsed.token
		if parsed.callToken != "" {
			s.callToken = parsed.callToken
		}
		s.finished = s.token == ""
	}
}

// Exchange sends one lockstep input and returns exactly one output batch. The
// input schema must exactly equal the declaration passed to OpenExchange. The
// caller owns the returned batch.
func (s *HttpClientStream) Exchange(ctx context.Context, input arrow.RecordBatch) (*ClientBatch, error) {
	if s.closed {
		return nil, errors.New("vgirpc: stream is closed")
	}
	if !s.exchange {
		return nil, errors.New("vgirpc: Exchange is only valid on exchange streams")
	}
	if s.finished || s.token == "" {
		return nil, &RpcError{Type: "ProtocolError", Message: "exchange stream has no continuation token"}
	}
	if !clientSchemasEqual(input.Schema(), s.schemas.Input) {
		return nil, &RpcError{Type: "TypeError", Message: fmt.Sprintf("exchange input schema mismatch: expected %s, got %s", s.schemas.Input, input.Schema())}
	}
	body, err := s.continuationBody(false, input)
	if err != nil {
		return nil, err
	}
	// An exchange is non-idempotent: once bytes may reach the server, the old
	// cursor must never be sent again after a timeout, reset, malformed body,
	// or any other ambiguous outcome. Only a completely parsed response can
	// reactivate the session with its newly minted cursor.
	s.token = ""
	s.finished = true
	response, err := s.client.post(ctx, s.method+"/exchange", body)
	if err != nil {
		return nil, err
	}
	parsed, err := s.client.parseMain(response, s.schemas.Output, true)
	if err != nil {
		return nil, err
	}
	defer parsed.releaseExceptFirst()
	if len(parsed.batches) != 1 {
		return nil, &RpcError{Type: "ProtocolError", Message: fmt.Sprintf("exchange response contained %d data batches", len(parsed.batches))}
	}
	if parsed.token == "" {
		return nil, &RpcError{Type: "ProtocolError", Message: "exchange response did not contain a new continuation token"}
	}
	s.token = parsed.token
	if parsed.callToken != "" {
		s.callToken = parsed.callToken
	}
	s.finished = s.token == ""
	result := parsed.batches[0]
	parsed.batches[0] = nil
	return result, nil
}

// Cancel best-effort signals cancellation and always transitions the local
// stream to finished. It is idempotent.
func (s *HttpClientStream) Cancel(ctx context.Context) error {
	if s.closed || s.finished || s.token == "" {
		s.finished = true
		return nil
	}
	body, err := s.continuationBody(true, nil)
	if err == nil {
		var response clientHTTPResponse
		response, err = s.client.post(ctx, s.method+"/exchange", body)
		if err == nil {
			var parsed *parsedClientStream
			parsed, err = s.client.parseMain(response, s.schemas.Output, false)
			if parsed != nil {
				defer parsed.release()
				if len(parsed.batches) != 0 || parsed.token != "" {
					err = &RpcError{Type: "ProtocolError", Message: "cancel response unexpectedly continued the stream"}
				}
			}
		}
	}
	s.finished = true
	s.token = ""
	return err
}

// Close releases local stream resources and prevents further use. It does not
// perform network I/O; call Cancel explicitly when the worker must observe
// cancellation. Close is idempotent.
func (s *HttpClientStream) Close() {
	if s == nil || s.closed {
		return
	}
	for _, batch := range s.pending {
		batch.Release()
	}
	s.pending = nil
	if s.header != nil {
		s.header.Release()
		s.header = nil
	}
	s.closed = true
}

func (s *HttpClientStream) continuationBody(cancel bool, input arrow.RecordBatch) ([]byte, error) {
	requestID, err := clientRequestID()
	if err != nil {
		return nil, err
	}
	if input == nil {
		input = emptyBatch(arrow.NewSchema(nil, nil))
		defer input.Release()
	}
	metadata := recordMetadata(input)
	stripClientControlMetadata(metadata)
	metadata[MetaStreamState] = s.token
	metadata[MetaRequestID] = requestID
	if s.callToken != "" {
		metadata[MetaCallState] = s.callToken
	}
	if cancel {
		metadata[MetaCancel] = "1"
	}
	return encodeClientBatch(input, metadata, s.client.maxRequest)
}

type parsedClientStream struct {
	batches   []*ClientBatch
	token     string
	callToken string
}

func (p *parsedClientStream) release() {
	if p == nil {
		return
	}
	for _, batch := range p.batches {
		batch.Release()
	}
	p.batches = nil
}

func (p *parsedClientStream) releaseExceptFirst() {
	if p == nil {
		return
	}
	for i := 1; i < len(p.batches); i++ {
		p.batches[i].Release()
	}
	if len(p.batches) > 1 {
		p.batches = p.batches[:1]
	}
}

func (c *HttpClient) parseIPCStream(raw *bytes.Reader, expected *arrow.Schema, tokenIsData bool) (*parsedClientStream, error) {
	reader, err := ipc.NewReader(raw)
	if err != nil {
		return nil, &RpcError{Type: "ProtocolError", Message: fmt.Sprintf("read Arrow IPC response: %v", err)}
	}
	defer reader.Release()
	if expected != nil && !clientSchemasEqual(reader.Schema(), expected) {
		return nil, &RpcError{Type: "TypeError", Message: fmt.Sprintf("response schema mismatch: expected %s, got %s", expected, reader.Schema())}
	}
	parsed := &parsedClientStream{}
	for reader.Next() {
		record := reader.RecordBatch()
		record.Retain()
		metadata := recordMetadata(record)
		if level := metadata[MetaLogLevel]; record.NumRows() == 0 && level != "" {
			record.Release()
			if level == string(LogException) {
				parsed.release()
				return nil, rpcErrorFromMetadata(metadata)
			}
			if c.onLog != nil {
				c.onLog(logMessageFromMetadata(metadata))
			}
			continue
		}
		token := metadata[MetaStreamState]
		if token != "" {
			parsed.token = token
			delete(metadata, MetaStreamState)
		}
		if call := metadata[MetaCallState]; call != "" {
			parsed.callToken = call
			delete(metadata, MetaCallState)
		}
		if metadata[MetaLocation] != "" {
			record.Release()
			parsed.release()
			return nil, &RpcError{Type: "ProtocolError", Message: "external-location responses require an external resolver"}
		}
		if token != "" && record.NumRows() == 0 && !tokenIsData {
			record.Release()
			continue
		}
		parsed.batches = append(parsed.batches, &ClientBatch{Batch: record, Metadata: metadata})
	}
	if err := reader.Err(); err != nil {
		parsed.release()
		return nil, &RpcError{Type: "ProtocolError", Message: fmt.Sprintf("read Arrow IPC response batch: %v", err)}
	}
	return parsed, nil
}

func clientSchemasEqual(left, right *arrow.Schema) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.Equal(right) && left.Metadata().Equal(right.Metadata())
}

func encodeClientBatch(batch arrow.RecordBatch, metadata map[string]string, maxBytes int64) ([]byte, error) {
	keys := make([]string, 0, len(metadata))
	values := make([]string, 0, len(metadata))
	for key, value := range metadata {
		keys = append(keys, key)
		values = append(values, value)
	}
	md := arrow.NewMetadata(keys, values)
	annotated := array.NewRecordBatchWithMetadata(batch.Schema(), batch.Columns(), batch.NumRows(), md)
	defer annotated.Release()
	out := &cappedClientBuffer{limit: maxBytes}
	writer := ipc.NewWriter(out, ipc.WithSchema(batch.Schema()))
	writeErr := writer.Write(annotated)
	closeErr := writer.Close()
	if writeErr != nil {
		return nil, fmt.Errorf("vgirpc: write request batch: %w", writeErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("vgirpc: close request IPC stream: %w", closeErr)
	}
	return out.Bytes(), nil
}

type cappedClientBuffer struct {
	buf   bytes.Buffer
	limit int64
}

func (w *cappedClientBuffer) Write(p []byte) (int, error) {
	if int64(w.buf.Len())+int64(len(p)) > w.limit {
		return 0, &RpcError{Type: "TransportError", Message: fmt.Sprintf("request IPC exceeds client limit (%d bytes)", w.limit)}
	}
	return w.buf.Write(p)
}

func (w *cappedClientBuffer) Bytes() []byte { return w.buf.Bytes() }

func recordMetadata(batch arrow.RecordBatch) map[string]string {
	out := make(map[string]string)
	withMetadata, ok := batch.(arrow.RecordBatchWithMetadata)
	if !ok {
		return out
	}
	md := withMetadata.Metadata()
	keys, values := md.Keys(), md.Values()
	for i := range keys {
		out[keys[i]] = values[i]
	}
	return out
}

func stripClientControlMetadata(metadata map[string]string) {
	for _, key := range []string{
		MetaMethod,
		MetaRequestVersion,
		MetaRequestID,
		MetaProtocolVersion,
		MetaStreamState,
		MetaCallState,
		MetaCancel,
		MetaLocation,
		MetaLocationSHA256,
		MetaShmOffset,
		MetaShmLength,
		MetaShmSegmentName,
		MetaShmSegmentSize,
		MetaShmSource,
		MetaServerID,
		MetaErrorKind,
		MetaLogMessage,
		MetaLogExtra,
	} {
		delete(metadata, key)
	}
}

func rpcErrorFromMetadata(metadata map[string]string) *RpcError {
	err := &RpcError{
		Type:      "Exception",
		Message:   metadata[MetaLogMessage],
		RequestID: metadata[MetaRequestID],
		Kind:      metadata[MetaErrorKind],
	}
	var extra struct {
		ExceptionType string `json:"exception_type"`
		Traceback     string `json:"traceback"`
	}
	if json.Unmarshal([]byte(metadata[MetaLogExtra]), &extra) == nil {
		if extra.ExceptionType != "" {
			err.Type = extra.ExceptionType
		}
		err.Traceback = extra.Traceback
	}
	return err
}

func logMessageFromMetadata(metadata map[string]string) LogMessage {
	message := LogMessage{
		Level:   LogLevel(metadata[MetaLogLevel]),
		Message: metadata[MetaLogMessage],
		Extras:  make(map[string]string),
	}
	var extras map[string]any
	if json.Unmarshal([]byte(metadata[MetaLogExtra]), &extras) == nil {
		for key, value := range extras {
			message.Extras[key] = fmt.Sprint(value)
		}
	}
	return message
}

func validateMethod(method string) error {
	if method == "" || strings.ContainsAny(method, "/?#") {
		return fmt.Errorf("vgirpc: invalid RPC method %q", method)
	}
	return nil
}

func clientRequestID() (string, error) {
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("vgirpc: generate request id: %w", err)
	}
	return hex.EncodeToString(random[:]), nil
}

func boundedText(body []byte) string {
	const max = 4096
	if len(body) > max {
		body = body[:max]
	}
	text := strings.TrimSpace(string(body))
	if text == "" {
		return "non-Arrow error response"
	}
	return text
}

func validateClientContentEncoding(header string) error {
	if strings.TrimSpace(header) == "" {
		return nil
	}
	for _, raw := range strings.Split(header, ",") {
		name := strings.ToLower(strings.TrimSpace(raw))
		switch name {
		case "zstd", "gzip", identityEncoding:
		default:
			return &RpcError{Type: "TransportError", Message: fmt.Sprintf("unsupported HTTP response Content-Encoding %q", name)}
		}
	}
	return nil
}
