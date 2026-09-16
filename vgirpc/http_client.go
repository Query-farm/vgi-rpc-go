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
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/klauspost/compress/zstd"
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
// declared for exchange streams and must be nil for producers. Header is
// declared when the method carries one.
//
// A batch sent through Exchange is checked against Input, when one is
// declared, before any bytes are written. This makes all-null and zero-row
// batches retain their declared types, child fields, and nullability instead
// of relying on value inference.
//
// A nil schema means "do not enforce", exactly as the expected parameter of
// [HttpClient.CallUnary] does. That is for a caller that discovered the method
// at runtime rather than from a compiled declaration and so has no schema to
// enforce; a caller that has one should pass it, because an unenforced schema
// is a type error found later and further away. Because a nil Header cannot
// then distinguish "no header" from "a header of unknown shape", set HasHeader
// to read one without declaring its schema.
type ClientStreamSchema struct {
	Input  *arrow.Schema
	Output *arrow.Schema
	Header *arrow.Schema
	// HasHeader reads a header whose schema is not declared. Ignored when
	// Header is non-nil, which already says a header is present.
	HasHeader bool
}

// ClientLogHandler receives client-directed log batches. It runs synchronously
// while each response body is parsed, but concurrent calls on one HttpClient
// may invoke it concurrently. Handlers must be concurrency-safe and return
// promptly.
type ClientLogHandler func(LogMessage)

type httpClientConfig struct {
	inner   *http.Client
	prefix  string
	headers http.Header
	// protocol is the routing key stamped on every request.
	protocol            string
	protocolVersion     string
	maxRequest          int64
	maxEncoded          int64
	maxDecoded          int64
	acceptedMaxResponse int64
	responseLimitsSet   bool
	acceptedResponseSet bool
	external            *ExternalLocationConfig
	compressRequests    bool
	compressionLevel    int
	onLog               ClientLogHandler
	closeIdleOnClose    bool
	tcpProxy            string
	customHTTPClient    bool
	ownedTransport      io.Closer
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

// WithClientProtocol sets the routing key stamped on every request.
//
// The wire protocol requires vgi_rpc.protocol on every request, including
// against a server hosting exactly one protocol, so a client that does not set
// this is refused with protocol_not_specified. That is deliberate: an exemption
// would let an intermediary that rebuilds a request and drops the field land
// silently on whichever protocol the server registered first.
func WithClientProtocol(protocol string) HttpClientOption {
	return func(cfg *httpClientConfig) error {
		if protocol != "" {
			if err := ValidateProtocolName(protocol, true); err != nil {
				return err
			}
		}
		cfg.protocol = protocol
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
// decoded response body. Both limits must be positive. The smaller limit also
// bounds the accepted response budget advertised on the wire; it must remain
// at least 64 KiB after all options are combined.
func WithClientResponseLimits(maxEncoded, maxDecoded int64) HttpClientOption {
	return func(cfg *httpClientConfig) error {
		if maxEncoded <= 0 || maxDecoded <= 0 {
			return errors.New("vgirpc: client response limits must be positive")
		}
		cfg.maxEncoded = maxEncoded
		cfg.maxDecoded = maxDecoded
		cfg.responseLimitsSet = true
		return nil
	}
}

// WithClientAcceptedMaxResponseBytes sets the decoded response size the
// client advertises in VGI-Accept-Max-Response-Bytes. The value must be a
// positive integer representable exactly in every supported SDK. By itself it
// raises the default encoded and decoded ceilings to the same value; explicit
// WithClientResponseLimits values instead clamp the effective advertisement.
func WithClientAcceptedMaxResponseBytes(maxBytes int64) HttpClientOption {
	return func(cfg *httpClientConfig) error {
		if maxBytes < minResponseBudgetBytes || maxBytes > maxSafeDecimal {
			return fmt.Errorf("vgirpc: accepted response limit must be between %d and %d", minResponseBudgetBytes, maxSafeDecimal)
		}
		cfg.acceptedMaxResponse = maxBytes
		cfg.acceptedResponseSet = true
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

// WithClientRequestCompression compresses request bodies with zstd at level.
//
// Off by default, and deliberately: request params are usually small enough
// that framing costs more than it saves, and an intermediary in front of the
// server may not carry a Content-Encoding nobody asked it for. A caller
// shipping large params turns it on.
//
// level is the codec's four-value speed enum (1 fastest to 4 best), not
// zstd's own 1-22 scale -- passing 9 fails construction rather than silently
// sending an uncompressed body, which is the right failure but a surprising
// one if you expected zstd's numbering.
func WithClientRequestCompression(level int) HttpClientOption {
	return func(cfg *httpClientConfig) error {
		probe, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevel(level)))
		if err != nil {
			return fmt.Errorf("vgirpc: invalid client request compression level %d: %w", level, err)
		}
		probe.Close()
		cfg.compressRequests = true
		cfg.compressionLevel = level
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
	baseURL *url.URL
	inner   *http.Client
	prefix  string
	headers http.Header
	// protocol is the routing key stamped on every request. Required by the
	// wire protocol even against a single-protocol server, so a client that
	// leaves it empty is refused rather than silently landing on whichever
	// protocol the server happened to register first.
	protocol               string
	protocolVersion        string
	maxRequest             int64
	maxEncoded             int64
	maxDecoded             int64
	acceptedMaxResponse    int64
	external               *ExternalLocationConfig
	requestExternal        *requestExternalizer
	compressRequests       bool
	compressionLevel       int
	sessionMu              sync.Mutex
	sessions               []*clientSession
	onLog                  ClientLogHandler
	closeIdleOnClose       bool
	ownedTransport         io.Closer
	closed                 atomic.Bool
	responseBudgetMu       sync.Mutex
	responseBudgetVerified bool
	serverMaxResponse      int64
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
		inner:               &http.Client{Timeout: 30 * time.Second},
		headers:             make(http.Header),
		maxRequest:          defaultClientMaxRequestBytes,
		maxEncoded:          defaultClientMaxEncodedResponseBytes,
		maxDecoded:          defaultClientMaxDecodedResponseBytes,
		acceptedMaxResponse: defaultClientMaxDecodedResponseBytes,
		closeIdleOnClose:    true,
	}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&cfg); err != nil {
			return nil, err
		}
	}
	// The advertised decoded willingness must be a limit this client can
	// actually enforce. An explicit accepted limit raises the default local
	// ceilings, while independently configured transport ceilings clamp it.
	// Resolve this once after all options so option order cannot change it.
	if cfg.acceptedResponseSet && !cfg.responseLimitsSet {
		cfg.maxEncoded = cfg.acceptedMaxResponse
		cfg.maxDecoded = cfg.acceptedMaxResponse
	}
	if !cfg.acceptedResponseSet {
		cfg.acceptedMaxResponse = minPositive(cfg.maxEncoded, cfg.maxDecoded, maxSafeDecimal)
	}
	cfg.acceptedMaxResponse = minPositive(cfg.acceptedMaxResponse, cfg.maxEncoded, cfg.maxDecoded, maxSafeDecimal)
	if cfg.acceptedMaxResponse < minResponseBudgetBytes {
		return nil, fmt.Errorf("vgirpc: effective accepted response limit must be at least %d", minResponseBudgetBytes)
	}
	// Required, with no single-protocol exemption: the routing key rides both
	// as vgi_rpc.protocol and as a path segment, and neither can be synthesized
	// here. A client that names no protocol has nothing to put in the path and
	// would be refused with protocol_not_specified on arrival anyway, so it is
	// refused at construction where the message can name the fix.
	if cfg.protocol == "" {
		return nil, errors.New("vgirpc: a client protocol is required; pass WithClientProtocol")
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
		baseURL:             u,
		inner:               cfg.inner,
		prefix:              cfg.prefix,
		headers:             cfg.headers.Clone(),
		protocol:            cfg.protocol,
		protocolVersion:     cfg.protocolVersion,
		maxRequest:          cfg.maxRequest,
		maxEncoded:          cfg.maxEncoded,
		maxDecoded:          cfg.maxDecoded,
		acceptedMaxResponse: cfg.acceptedMaxResponse,
		external:            cfg.external,
		requestExternal:     &requestExternalizer{},
		compressRequests:    cfg.compressRequests,
		compressionLevel:    cfg.compressionLevel,
		onLog:               cfg.onLog,
		closeIdleOnClose:    cfg.closeIdleOnClose,
		ownedTransport:      cfg.ownedTransport,
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
	if c.ownedTransport != nil {
		_ = c.ownedTransport.Close()
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
	return c.callUnaryOn(ctx, c.protocol, method, params, expected)
}

// callUnaryOn is [HttpClient.CallUnary] addressed to an explicit protocol.
func (c *HttpClient) callUnaryOn(
	ctx context.Context,
	protocol string,
	method string,
	params arrow.RecordBatch,
	expected *arrow.Schema,
) (*ClientBatch, error) {
	body, err := c.initialBodyFor(protocol, method, params)
	if err != nil {
		return nil, err
	}
	response, err := c.post(ctx, c.rpcPathFor(protocol, method, ""), body)
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

// OpenProducer starts a producer stream. schemas.Input must be nil;
// schemas.Output is the declared output schema, or nil to accept whatever the
// server sends.
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

// OpenExchange starts a lockstep exchange stream. Input and Output are
// enforced exactly for every exchange turn when declared.
func (c *HttpClient) OpenExchange(
	ctx context.Context,
	method string,
	params arrow.RecordBatch,
	schemas ClientStreamSchema,
) (*HttpClientStream, error) {
	return c.openStream(ctx, method, params, schemas, true)
}

func (c *HttpClient) openStream(
	ctx context.Context,
	method string,
	params arrow.RecordBatch,
	schemas ClientStreamSchema,
	exchange bool,
) (*HttpClientStream, error) {
	body, err := c.initialBody(method, params)
	if err != nil {
		return nil, err
	}
	response, err := c.post(ctx, c.rpcPath(method, "/init"), body)
	if err != nil {
		return nil, err
	}
	raw := bytes.NewReader(response.body)
	var header *ClientBatch
	if schemas.Header != nil || schemas.HasHeader {
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
		// An exchange must not preload output: the first turn has not been
		// sent yet. What is being refused is *data*, so a batch with no rows
		// and no columns does not count -- that is how a zero-column schema is
		// framed, it carries nothing, and rejecting the stream over it refuses
		// a peer that has said nothing at all.
		for _, batch := range parsed.batches {
			if batch.Batch.NumRows() == 0 && batch.Batch.NumCols() == 0 {
				continue
			}
			parsed.release()
			if header != nil {
				header.Release()
			}
			return nil, &RpcError{Type: "ProtocolError", Message: "exchange init response contained unexpected data"}
		}
		parsed.release()
		if parsed.token == "" || parsed.callToken == "" {
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
	return c.initialBodyFor(c.protocol, method, params)
}

// initialBodyFor encodes one request addressed to protocol rather than to the
// client's own routing key. Only a bootstrap protocol a client may call without
// being configured for it -- vgi_rpc.Reflection.v1 -- uses this.
func (c *HttpClient) initialBodyFor(protocol, method string, params arrow.RecordBatch) ([]byte, error) {
	if err := validateMethod(method); err != nil {
		return nil, err
	}
	requestID, err := clientRequestID()
	if err != nil {
		return nil, err
	}
	metadata := recordMetadata(params)
	// A caller's own application protocol version, if it stamped one. The
	// client's configured value wins -- it is the version this client was
	// built against -- but a generic caller relaying a request it did not
	// author has no way to configure one, and dropping what it stamped would
	// silently downgrade the call to unversioned.
	callerProtocolVersion := metadata[MetaProtocolVersion]
	stripClientControlMetadata(metadata)
	metadata[MetaMethod] = method
	metadata[MetaProtocol] = protocol
	metadata[MetaRequestVersion] = ProtocolVersion
	metadata[MetaRequestID] = requestID
	switch {
	case c.protocolVersion != "":
		metadata[MetaProtocolVersion] = c.protocolVersion
	case callerProtocolVersion != "":
		metadata[MetaProtocolVersion] = callerProtocolVersion
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

// rpcPath renders one RPC endpoint below the client's prefix.
//
// The protocol is a path segment, not only a metadata field: it is the
// projection an edge device routes on, and a client that sends only the
// metadata leaves every intermediary in front of the worker unable to tell one
// protocol from another. suffix is "" for unary, "/init" or "/exchange" for
// streams.
func (c *HttpClient) rpcPath(method, suffix string) string {
	return c.rpcPathFor(c.protocol, method, suffix)
}

// rpcPathFor renders an endpoint below a protocol other than the client's own.
func (c *HttpClient) rpcPathFor(protocol, method, suffix string) string {
	return protocol + "/" + method + suffix
}

func (c *HttpClient) post(ctx context.Context, endpoint string, body []byte) (clientHTTPResponse, error) {
	if c == nil || c.closed.Load() {
		return clientHTTPResponse{}, errors.New("vgirpc: HTTP client is closed")
	}
	if err := c.ensureResponseBudgetSupport(ctx); err != nil {
		return clientHTTPResponse{}, err
	}
	// Externalising a request means asking the server for an upload URL, which
	// is itself a request. Excluding that one route is what terminates the
	// recursion; it is also the only route below a prefix rather than below a
	// routing key, so no application method can collide with it.
	if !isFrameworkEndpoint(endpoint) {
		externalized, replaced, err := c.maybeExternalizeRequest(ctx, body)
		if err != nil {
			return clientHTTPResponse{}, err
		}
		if replaced {
			return c.postOnce(ctx, endpoint, externalized)
		}
		response, err := c.postOnce(ctx, endpoint, body)
		var status *HTTPStatusError
		if err != nil && errors.As(err, &status) && status.StatusCode == http.StatusRequestEntityTooLarge {
			// The server has now told us its cap; retrying through the
			// upload-URL route is the whole reason it answers 413 rather than
			// closing the connection.
			//
			// Safe to repeat even for an exchange turn, which is otherwise
			// never retried: 413 is the one non-2xx that says the body was
			// refused *before* dispatch, so no handler ran and no cursor was
			// consumed. Every ambiguous outcome -- a timeout, a reset, a
			// malformed reply -- still poisons the turn. The reference
			// retries a 413 on the exchange path for the same reason.
			retried, buildErr := c.externalizeRequestBody(ctx, body)
			if buildErr != nil {
				return clientHTTPResponse{}, err
			}
			return c.postOnce(ctx, endpoint, retried)
		}
		return response, err
	}
	return c.postOnce(ctx, endpoint, body)
}

// isFrameworkEndpoint reports whether a path is the server's own upload-URL
// route rather than an RPC method.
//
// The other framework route, session deletion, never reaches here: it is a
// DELETE built directly rather than a POST through this path.
func isFrameworkEndpoint(endpoint string) bool {
	return strings.HasPrefix(endpoint, UploadURLMethod)
}

func (c *HttpClient) postOnce(ctx context.Context, endpoint string, body []byte) (clientHTTPResponse, error) {
	if int64(len(body)) > c.maxRequest {
		return clientHTTPResponse{}, &RpcError{Type: "TransportError", Message: fmt.Sprintf("request body exceeds client limit (%d > %d bytes)", len(body), c.maxRequest)}
	}
	encodedBody, requestEncoding, err := c.encodeRequestBody(body)
	if err != nil {
		return clientHTTPResponse{}, err
	}
	u := *c.baseURL
	u.Path = strings.TrimRight(c.baseURL.Path, "/") + c.prefix + "/" + endpoint
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(encodedBody))
	if err != nil {
		return clientHTTPResponse{}, fmt.Errorf("vgirpc: build HTTP request: %w", err)
	}
	req.Header = c.headers.Clone()
	req.Header.Set("Content-Type", arrowContentType)
	if requestEncoding != "" {
		req.Header.Set(contentEncodingHeader, requestEncoding)
	}
	c.applySessionHeaders(req.Header)
	req.Header.Set(customAcceptEncodingHeader, "zstd, gzip, identity")
	req.Header.Set(acceptEncodingHeader, "zstd, gzip, identity")
	req.Header.Set(acceptMaxResponseBytesHeader, fmt.Sprintf("%d", c.acceptedMaxResponse))
	resp, err := c.inner.Do(req)
	if err != nil {
		var irohErr *IrohTransportError
		if errors.As(err, &irohErr) {
			return clientHTTPResponse{}, irohErr
		}
		return clientHTTPResponse{}, &RpcError{Type: "TransportError", Message: fmt.Sprintf("HTTP request failed: %v", err)}
	}
	defer resp.Body.Close()
	c.observeSessionHeaders(resp.Header)
	caps, err := ParseHTTPServerCapabilities(resp.Header)
	if err != nil {
		return clientHTTPResponse{}, &RpcError{Type: "ProtocolError", Message: err.Error()}
	}
	if !caps.AcceptMaxResponseBytesSupport {
		return clientHTTPResponse{}, &RpcError{Type: "ProtocolError", Message: fmt.Sprintf(
			"server must advertise %s: true on every RPC response", acceptMaxResponseBytesSupportHeader)}
	}
	// Cached only now, below the support check: an intermediary's 502 page or
	// a gateway's 413 carries no VGI headers at all, and caching the all-zero
	// snapshot it parses as would erase what the server actually advertised --
	// which is read later to decide whether a body can be externalised.
	c.observeCapabilities(caps)
	c.responseBudgetMu.Lock()
	if caps.MaxResponseBytes > 0 {
		c.serverMaxResponse = caps.MaxResponseBytes
	}
	serverMaxResponse := c.serverMaxResponse
	c.responseBudgetMu.Unlock()
	encoding := strings.TrimSpace(resp.Header.Get(contentEncodingHeader))
	if encoding == "" {
		encoding = strings.TrimSpace(resp.Header.Get(customContentEncodingHeader))
	}
	if err := validateClientContentEncoding(encoding); err != nil {
		return clientHTTPResponse{}, err
	}
	decodedLimit := minPositive(c.maxDecoded, c.acceptedMaxResponse, serverMaxResponse)
	encodedReadLimit := c.maxEncoded
	if encoding == "" || strings.EqualFold(encoding, identityEncoding) {
		// An identity response is already decoded. Bound the streaming read by
		// the accepted response budget so an untrusted peer cannot force an
		// allocation up to the larger encoded transport cap first.
		encodedReadLimit = minPositive(encodedReadLimit, decodedLimit)
	}
	if resp.ContentLength > encodedReadLimit {
		return clientHTTPResponse{}, &RpcError{Type: "TransportError", Message: fmt.Sprintf("encoded HTTP response exceeds client limit (%d > %d bytes)", resp.ContentLength, encodedReadLimit)}
	}
	encoded, err := io.ReadAll(io.LimitReader(resp.Body, encodedReadLimit+1))
	if err != nil {
		return clientHTTPResponse{}, &RpcError{Type: "TransportError", Message: fmt.Sprintf("read HTTP response: %v", err)}
	}
	if int64(len(encoded)) > encodedReadLimit {
		return clientHTTPResponse{}, &RpcError{Type: "TransportError", Message: fmt.Sprintf("encoded HTTP response exceeds client limit (%d bytes)", encodedReadLimit)}
	}
	decoded := encoded
	if encoding != "" && !strings.EqualFold(encoding, identityEncoding) {
		decoded, err = DecodeContentEncoding(encoded, encoding, decodedLimit)
		if err != nil {
			return clientHTTPResponse{}, &RpcError{Type: "TransportError", Message: fmt.Sprintf("decode HTTP response: %v", err)}
		}
	}
	if int64(len(decoded)) > decodedLimit {
		return clientHTTPResponse{}, &RpcError{Type: "TransportError", Message: fmt.Sprintf("decoded HTTP response exceeds client limit (%d > %d bytes)", len(decoded), decodedLimit)}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		envelope := decodeErrorEnvelope(decoded)
		// 413 keeps its status even when the body explains itself, because
		// the status is the actionable half: it is how a client knows to
		// re-send through the upload-URL route rather than to give up. Every
		// other non-2xx is better described by the envelope the peer wrote.
		if envelope != nil && resp.StatusCode != http.StatusRequestEntityTooLarge {
			if envelope.RequestID == "" {
				envelope.RequestID = resp.Header.Get(requestIDHeader)
			}
			return clientHTTPResponse{}, envelope
		}
		detail := boundedText(decoded)
		if envelope != nil {
			detail = envelope.Error()
		}
		return clientHTTPResponse{}, &HTTPStatusError{
			StatusCode: resp.StatusCode,
			Detail:     detail,
			RequestID:  resp.Header.Get(requestIDHeader),
		}
	}
	return clientHTTPResponse{
		status:   resp.StatusCode,
		body:     decoded,
		rpcError: strings.EqualFold(resp.Header.Get(rpcErrorHeader), "true"),
	}, nil
}

// encodeRequestBody applies the configured request Content-Encoding.
//
// Returns the body unchanged, and an empty encoding, when compression is off --
// the default -- so the common path copies nothing.
func (c *HttpClient) encodeRequestBody(body []byte) ([]byte, string, error) {
	if !c.compressRequests || len(body) == 0 {
		return body, "", nil
	}
	var buf bytes.Buffer
	writer, err := newCompressWriter("zstd", &buf, c.compressionLevel)
	if err != nil {
		return nil, "", fmt.Errorf("vgirpc: compress request body: %w", err)
	}
	if _, err := writer.Write(body); err != nil {
		_ = writer.Close()
		return nil, "", fmt.Errorf("vgirpc: compress request body: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, "", fmt.Errorf("vgirpc: compress request body: %w", err)
	}
	return buf.Bytes(), "zstd", nil
}

// decodeErrorEnvelope recovers the RPC error a non-2xx response carries, or
// nil when the body is not an Arrow exception envelope.
//
// A server answers a bad request with the status *and* the envelope: the status
// is for the intermediaries, the envelope is for the caller. Reading only the
// status throws the caller's half away -- a parameter-validation failure
// arrives as HTTP 400 whose detail is the raw Arrow bytes, so the error type
// the peer named is gone and the message is binary noise. That went unnoticed
// because this client had only ever been run against a server that pairs with
// it; the Python reference returns 400 for every parameter and schema
// rejection, and so does this port's own server.
//
// Only an Arrow body is reinterpreted. A 401's JSON envelope, a proxy's HTML
// error page, and a bodyless 404 stay [HTTPStatusError], because for those the
// status really is the whole answer.
func decodeErrorEnvelope(body []byte) *RpcError {
	if len(body) == 0 {
		return nil
	}
	reader, err := ipc.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil
	}
	defer reader.Release()
	for reader.Next() {
		record := reader.RecordBatch()
		metadata := recordMetadata(record)
		if record.NumRows() == 0 && metadata[MetaLogLevel] == string(LogException) {
			return rpcErrorFromMetadata(metadata)
		}
	}
	return nil
}

func (c *HttpClient) ensureResponseBudgetSupport(ctx context.Context) error {
	c.responseBudgetMu.Lock()
	defer c.responseBudgetMu.Unlock()
	if c.responseBudgetVerified {
		return nil
	}
	caps, err := c.DiscoverCapabilities(ctx)
	if err != nil {
		return err
	}
	if !caps.AcceptMaxResponseBytesSupport {
		return &RpcError{Type: "ProtocolError", Message: fmt.Sprintf(
			"server does not advertise %s: true", acceptMaxResponseBytesSupportHeader)}
	}
	c.serverMaxResponse = caps.MaxResponseBytes
	c.responseBudgetVerified = true
	return nil
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

// Token returns the opaque continuation token the server last minted for this
// stream, or "" once the stream has ended.
//
// Read after [HttpClientStream.Next] it is the cursor that resumes *after* the
// batch just returned, which is what a caller check-pointing a long producer
// persists alongside it.
func (s *HttpClientStream) Token() string {
	if s == nil {
		return ""
	}
	return s.token
}

// Next returns the next producer batch. ok is false at end-of-stream. The
// caller owns a returned batch and must Release it.
func (s *HttpClientStream) Next(ctx context.Context) (batch *ClientBatch, ok bool, err error) {
	return s.NextWithMetadata(ctx, nil)
}

// NextWithMetadata is [HttpClientStream.Next] with Arrow custom metadata sent
// upstream on the continuation request.
//
// A producer turn carries no data from the client, but it does carry metadata:
// this is how a caller passes per-turn direction (a resume hint, a trace
// context, a budget) to a stream it is pulling rather than pushing.
func (s *HttpClientStream) NextWithMetadata(ctx context.Context, custom map[string]string) (batch *ClientBatch, ok bool, err error) {
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
		body, err := s.continuationBody(false, nil, custom)
		if err != nil {
			return nil, false, err
		}
		response, err := s.client.post(ctx, s.client.rpcPath(s.method, "/exchange"), body)
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
	if s.schemas.Input != nil && !clientSchemasEqual(input.Schema(), s.schemas.Input) {
		return nil, &RpcError{Type: "TypeError", Message: fmt.Sprintf("exchange input schema mismatch: expected %s, got %s", s.schemas.Input, input.Schema())}
	}
	body, err := s.continuationBody(false, input, nil)
	if err != nil {
		return nil, err
	}
	// An exchange is non-idempotent: once bytes may reach the server, the old
	// cursor must never be sent again after a timeout, reset, malformed body,
	// or any other ambiguous outcome. Only a completely parsed response can
	// reactivate the session with its newly minted cursor.
	s.token = ""
	s.finished = true
	response, err := s.client.post(ctx, s.client.rpcPath(s.method, "/exchange"), body)
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
	body, err := s.continuationBody(true, nil, nil)
	if err == nil {
		var response clientHTTPResponse
		response, err = s.client.post(ctx, s.client.rpcPath(s.method, "/exchange"), body)
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

func (s *HttpClientStream) continuationBody(cancel bool, input arrow.RecordBatch, custom map[string]string) ([]byte, error) {
	requestID, err := clientRequestID()
	if err != nil {
		return nil, err
	}
	if input == nil {
		input = emptyBatch(arrow.NewSchema(nil, nil))
		defer input.Release()
	}
	metadata := recordMetadata(input)
	for key, value := range custom {
		metadata[key] = value
	}
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

func (c *HttpClient) parseIPCStream(raw io.Reader, expected *arrow.Schema, tokenIsData bool) (*parsedClientStream, error) {
	parsed, _, err := c.parseIPCStreamDrained(raw, expected, tokenIsData)
	return parsed, err
}

// parseIPCStreamDrained is [HttpClient.parseIPCStream] reporting whether the
// response stream was read to its end.
//
// That distinction only matters where the stream *is* the connection. Over
// HTTP the body is a finished buffer and a caller can stop reading wherever it
// likes. Over a raw socket, stopping early leaves the rest of the response in
// the byte stream, so the next call reads the tail of the last one -- which is
// why a remote handler error used to poison a raw connection: refusing to
// reuse it was the only safe thing left to do, and the conformance suite
// requires it to stay usable. Draining first makes reuse correct, and the flag
// is how the caller knows it may.
func (c *HttpClient) parseIPCStreamDrained(raw io.Reader, expected *arrow.Schema, tokenIsData bool) (*parsedClientStream, bool, error) {
	reader, err := ipc.NewReader(raw)
	if err != nil {
		return nil, false, &RpcError{Type: "ProtocolError", Message: fmt.Sprintf("read Arrow IPC response: %v", err)}
	}
	defer reader.Release()
	if expected != nil && !clientSchemasEqual(reader.Schema(), expected) {
		return nil, false, &RpcError{Type: "TypeError", Message: fmt.Sprintf("response schema mismatch: expected %s, got %s", expected, reader.Schema())}
	}
	parsed := &parsedClientStream{}
	var remote *RpcError
	cursorOnly := -1
	for reader.Next() {
		record := reader.RecordBatch()
		record.Retain()
		metadata := recordMetadata(record)
		if level := metadata[MetaLogLevel]; record.NumRows() == 0 && level != "" {
			record.Release()
			if level == string(LogException) {
				// Keep reading. The error is the answer, but the messages
				// behind it still belong to this response, and on a raw
				// transport they are still in the socket.
				if remote == nil {
					remote = rpcErrorFromMetadata(metadata)
				}
				continue
			}
			if c.onLog != nil {
				c.onLog(logMessageFromMetadata(metadata))
			}
			continue
		}
		if remote != nil {
			record.Release()
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
			resolved, resolvedMeta, resolveErr := resolveExternalBatch(c.external, c.onLog, record, metadata)
			if resolveErr != nil {
				parsed.release()
				return nil, false, resolveErr
			}
			record, metadata = resolved, resolvedMeta
			// The cursor rides the data batch, so when that batch is
			// externalised the cursor goes up to storage with it and reaches
			// the client only inside the fetched stream. Reading the pointer
			// batch alone finds no cursor and reports a stream that ended
			// mid-exchange -- a failure that cannot occur against a peer which
			// never externalises, which is every peer this client had met.
			if resumed := metadata[MetaStreamState]; resumed != "" {
				token = resumed
				parsed.token = resumed
				delete(metadata, MetaStreamState)
			}
			if call := metadata[MetaCallState]; call != "" {
				parsed.callToken = call
				delete(metadata, MetaCallState)
			}
		}
		if token != "" && record.NumRows() == 0 && !tokenIsData {
			record.Release()
			continue
		}
		if token != "" && record.NumRows() == 0 {
			// A zero-row batch that carried the cursor *might* be a bare
			// sentinel rather than data. It cannot be told apart yet: a server
			// that merges the cursor onto an empty data batch produces the
			// same shape, and against a zero-column schema even the column
			// count does not separate them. Note it and decide once the whole
			// response is in hand.
			cursorOnly = len(parsed.batches)
		}
		parsed.batches = append(parsed.batches, &ClientBatch{Batch: record, Metadata: metadata})
	}
	if err := reader.Err(); err != nil {
		parsed.release()
		return nil, false, &RpcError{Type: "ProtocolError", Message: fmt.Sprintf("read Arrow IPC response batch: %v", err)}
	}
	if remote != nil {
		parsed.release()
		return nil, true, remote
	}
	// Now it can be told apart: a cursor batch beside another batch was a bare
	// sentinel, because a stream turn carries at most one data batch. A cursor
	// batch that is the only one was the data batch with the cursor on it.
	// (Only a stream turn mints a cursor, so this cannot misread the
	// multi-batch upload-URL reply, which has none.)
	if cursorOnly >= 0 && len(parsed.batches) > 1 {
		parsed.batches[cursorOnly].Release()
		parsed.batches = append(parsed.batches[:cursorOnly], parsed.batches[cursorOnly+1:]...)
	}
	return parsed, true, nil
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
