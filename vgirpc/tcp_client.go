// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

const defaultTCPClientConnectTimeout = 10 * time.Second

type tcpClientConfig struct {
	connectTimeout  time.Duration
	proxy           string
	protocolVersion string
	maxRequest      int64
	maxResponse     int64
	onLog           ClientLogHandler
}

// TcpClientOption configures NewTcpClient.
type TcpClientOption func(*tcpClientConfig) error

// WithTcpClientProxy routes the connection through an explicit credential-free
// SOCKS5h proxy. The target hostname is resolved by the proxy and failure never
// falls back to a direct connection.
func WithTcpClientProxy(proxyURL string) TcpClientOption {
	return func(config *tcpClientConfig) error {
		if _, err := newSOCKS5HDialer(proxyURL); err != nil {
			return err
		}
		config.proxy = proxyURL
		return nil
	}
}

// WithTcpClientConnectTimeout bounds direct dialing or the complete SOCKS5h
// connection setup. The timeout must be positive.
func WithTcpClientConnectTimeout(timeout time.Duration) TcpClientOption {
	return func(config *tcpClientConfig) error {
		if timeout <= 0 {
			return errors.New("vgirpc: TCP client connect timeout must be positive")
		}
		config.connectTimeout = timeout
		return nil
	}
}

// WithTcpClientProtocolVersion stamps an application protocol version on each request.
func WithTcpClientProtocolVersion(version string) TcpClientOption {
	return func(config *tcpClientConfig) error {
		if version != "" {
			if _, _, _, err := parseSemver(version); err != nil {
				return err
			}
		}
		config.protocolVersion = version
		return nil
	}
}

// WithTcpClientLimits sets mandatory encoded request and response bounds.
func WithTcpClientLimits(maxRequestBytes, maxResponseBytes int64) TcpClientOption {
	return func(config *tcpClientConfig) error {
		if maxRequestBytes <= 0 || maxResponseBytes <= 0 {
			return errors.New("vgirpc: TCP client limits must be positive")
		}
		config.maxRequest = maxRequestBytes
		config.maxResponse = maxResponseBytes
		return nil
	}
}

// WithTcpClientLogHandler receives client-directed log batches.
func WithTcpClientLogHandler(handler ClientLogHandler) TcpClientOption {
	return func(config *tcpClientConfig) error {
		config.onLog = handler
		return nil
	}
}

// TcpClient is a blocking, stateful client for VGI's raw Arrow IPC protocol.
// Calls are serialized because one ordered byte stream carries both requests
// and responses. A transport or protocol failure poisons and closes the
// connection rather than risking response/request desynchronization.
type TcpClient struct {
	mu       sync.Mutex
	conn     net.Conn
	codec    *HttpClient
	closed   bool
	poisoned error
}

// NewTcpClient connects to a raw VGI TCP worker. The same context bounds direct
// dialing or the entire SOCKS5h handshake; a configured connect timeout is
// applied when the context has no earlier deadline.
func NewTcpClient(ctx context.Context, host string, port int, options ...TcpClientOption) (*TcpClient, error) {
	if ctx == nil {
		return nil, errors.New("vgirpc: TCP client context must not be nil")
	}
	if host == "" || port < 1 || port > 65535 {
		return nil, errors.New("vgirpc: TCP client requires a host and port between 1 and 65535")
	}
	config := tcpClientConfig{
		connectTimeout: defaultTCPClientConnectTimeout,
		maxRequest:     defaultClientMaxRequestBytes,
		maxResponse:    defaultClientMaxDecodedResponseBytes,
	}
	for _, option := range options {
		if option != nil {
			if err := option(&config); err != nil {
				return nil, err
			}
		}
	}
	dialCtx := ctx
	if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > config.connectTimeout {
		var cancel context.CancelFunc
		dialCtx, cancel = context.WithTimeout(ctx, config.connectTimeout)
		defer cancel()
	}
	target := net.JoinHostPort(host, strconv.Itoa(port))
	var conn net.Conn
	var err error
	if config.proxy != "" {
		dialer, dialErr := newSOCKS5HDialer(config.proxy)
		if dialErr != nil {
			return nil, dialErr
		}
		conn, err = dialer.DialContext(dialCtx, "tcp", target)
	} else {
		conn, err = (&net.Dialer{}).DialContext(dialCtx, "tcp", target)
	}
	if err != nil {
		return nil, fmt.Errorf("vgirpc: connect TCP worker: %w", err)
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		if err := tcp.SetNoDelay(true); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("vgirpc: set TCP_NODELAY: %w", err)
		}
	}
	return newTcpClientFromConn(conn, config), nil
}

func newTcpClientFromConn(conn net.Conn, config tcpClientConfig) *TcpClient {
	return &TcpClient{
		conn: conn,
		codec: &HttpClient{
			protocolVersion: config.protocolVersion,
			maxRequest:      config.maxRequest,
			maxDecoded:      config.maxResponse,
			onLog:           config.onLog,
		},
	}
}

// CallUnary performs one raw unary RPC. The caller owns the returned batch and
// must call Release. Independent calls may be issued concurrently; they are
// serialized on the single stateful connection.
func (client *TcpClient) CallUnary(ctx context.Context, method string, params arrow.RecordBatch,
	expected *arrow.Schema) (*ClientBatch, error) {
	if client == nil {
		return nil, errors.New("vgirpc: TCP client is nil")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closed {
		if client.poisoned != nil {
			return nil, fmt.Errorf("vgirpc: TCP client is closed after a failed call: %w", client.poisoned)
		}
		return nil, errors.New("vgirpc: TCP client is closed")
	}
	if ctx == nil {
		return nil, errors.New("vgirpc: TCP call context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	body, err := client.codec.initialBody(method, params)
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = client.conn.SetDeadline(time.Now()) })
	if deadline, ok := ctx.Deadline(); ok {
		if err := client.conn.SetDeadline(deadline); err != nil {
			stop()
			return nil, client.poison(err)
		}
	}
	defer func() {
		stop()
		if !client.closed {
			_ = client.conn.SetDeadline(time.Time{})
		}
	}()
	if err := writeAll(client.conn, body); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = ctxErr
		}
		return nil, client.poison(err)
	}
	parsed, err := client.codec.parseIPCStream(
		&cappedTCPReader{reader: client.conn, limit: client.codec.maxDecoded}, expected, true)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = ctxErr
		}
		return nil, client.poison(err)
	}
	defer parsed.releaseExceptFirst()
	if len(parsed.batches) != 1 {
		return nil, client.poison(&RpcError{Type: "ProtocolError", Message: fmt.Sprintf(
			"unary response contained %d data batches", len(parsed.batches))})
	}
	result := parsed.batches[0]
	parsed.batches[0] = nil
	return result, nil
}

// OpenProducer starts one stateful producer stream on the raw connection. The
// stream exclusively owns the connection until Close, Cancel, or Abort.
func (client *TcpClient) OpenProducer(ctx context.Context, method string, params arrow.RecordBatch,
	schemas ClientStreamSchema) (*TcpClientStream, error) {
	if schemas.Input != nil {
		return nil, errors.New("vgirpc: producer input schema must be nil")
	}
	return client.openStream(ctx, method, params, schemas, false)
}

// OpenExchange starts one stateful lockstep exchange stream. The stream
// exclusively owns the connection until Close, Cancel, or Abort.
func (client *TcpClient) OpenExchange(ctx context.Context, method string, params arrow.RecordBatch,
	schemas ClientStreamSchema) (*TcpClientStream, error) {
	if schemas.Input == nil {
		return nil, errors.New("vgirpc: exchange input schema is required")
	}
	return client.openStream(ctx, method, params, schemas, true)
}

func (client *TcpClient) openStream(ctx context.Context, method string, params arrow.RecordBatch,
	schemas ClientStreamSchema, exchange bool) (*TcpClientStream, error) {
	if client == nil {
		return nil, errors.New("vgirpc: TCP client is nil")
	}
	if schemas.Output == nil {
		return nil, errors.New("vgirpc: stream output schema is required")
	}
	client.mu.Lock()
	release := true
	defer func() {
		if release {
			client.mu.Unlock()
		}
	}()
	if client.closed {
		if client.poisoned != nil {
			return nil, fmt.Errorf("vgirpc: TCP client is closed after a failed call: %w", client.poisoned)
		}
		return nil, errors.New("vgirpc: TCP client is closed")
	}
	if ctx == nil {
		return nil, errors.New("vgirpc: TCP stream context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	body, err := client.codec.initialBody(method, params)
	if err != nil {
		return nil, err
	}
	cleanup, err := client.setOperationDeadline(ctx)
	if err != nil {
		return nil, client.poison(err)
	}
	defer cleanup()
	if err = writeAll(client.conn, body); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = ctxErr
		}
		return nil, client.poison(err)
	}
	var header *ClientBatch
	if schemas.Header != nil {
		parsed, parseErr := client.codec.parseIPCStream(
			&cappedTCPReader{reader: client.conn, limit: client.codec.maxDecoded}, schemas.Header, true)
		if parseErr != nil {
			return nil, client.poison(parseErr)
		}
		defer parsed.releaseExceptFirst()
		if len(parsed.batches) != 1 {
			return nil, client.poison(&RpcError{Type: "ProtocolError", Message: "stream header did not contain exactly one data batch"})
		}
		header = parsed.batches[0]
		parsed.batches[0] = nil
	}
	release = false
	return &TcpClientStream{
		client:   client,
		schemas:  schemas,
		exchange: exchange,
		header:   header,
	}, nil
}

func (client *TcpClient) setOperationDeadline(ctx context.Context) (func(), error) {
	stop := context.AfterFunc(ctx, func() { _ = client.conn.SetDeadline(time.Now()) })
	if deadline, ok := ctx.Deadline(); ok {
		if err := client.conn.SetDeadline(deadline); err != nil {
			stop()
			return func() {}, err
		}
	}
	return func() {
		stop()
		if !client.closed {
			_ = client.conn.SetDeadline(time.Time{})
		}
	}, nil
}

// TcpClientStream is one producer or exchange lifecycle over a stateful raw
// connection. It is not safe for concurrent use. Call Close or Cancel; Abort
// is the non-blocking escape hatch when a peer is no longer responsive.
type TcpClientStream struct {
	client       *TcpClient
	schemas      ClientStreamSchema
	exchange     bool
	header       *ClientBatch
	inputWriter  *ipc.Writer
	outputReader *ipc.Reader
	outputCount  *cappedTCPReader
	finished     bool
	closed       bool
	released     bool
}

// Header returns an owned retained copy of the optional stream header.
func (stream *TcpClientStream) Header() *ClientBatch {
	if stream == nil || stream.header == nil {
		return nil
	}
	stream.header.Batch.Retain()
	metadata := make(map[string]string, len(stream.header.Metadata))
	for key, value := range stream.header.Metadata {
		metadata[key] = value
	}
	return &ClientBatch{Batch: stream.header.Batch, Metadata: metadata}
}

// Finished reports whether the worker has closed the output stream.
func (stream *TcpClientStream) Finished() bool { return stream != nil && stream.finished }

// Next sends one producer tick and returns its data batch, or ok=false when
// the producer finishes without data.
func (stream *TcpClientStream) Next(ctx context.Context) (*ClientBatch, bool, error) {
	if err := stream.requireOpen(false); err != nil {
		return nil, false, err
	}
	if stream.finished {
		return nil, false, nil
	}
	tick := emptyBatch(arrow.NewSchema(nil, nil))
	defer tick.Release()
	if err := stream.writeTurn(ctx, tick, nil, false); err != nil {
		return nil, false, err
	}
	batch, err := stream.readNextData(ctx)
	return batch, batch != nil, err
}

// Exchange sends one input and returns exactly one lockstep output batch.
func (stream *TcpClientStream) Exchange(ctx context.Context, input arrow.RecordBatch) (*ClientBatch, error) {
	if err := stream.requireOpen(true); err != nil {
		return nil, err
	}
	if stream.finished {
		return nil, &RpcError{Type: "ProtocolError", Message: "exchange stream is finished"}
	}
	if input == nil || !clientSchemasEqual(input.Schema(), stream.schemas.Input) {
		return nil, &RpcError{Type: "TypeError", Message: fmt.Sprintf(
			"exchange input schema mismatch: expected %s", stream.schemas.Input)}
	}
	if err := stream.writeTurn(ctx, input, recordMetadata(input), false); err != nil {
		return nil, err
	}
	batch, err := stream.readNextData(ctx)
	if err != nil {
		return nil, err
	}
	if batch == nil {
		return nil, stream.fail(&RpcError{Type: "ProtocolError", Message: "exchange response ended without a data batch"})
	}
	return batch, nil
}

func (stream *TcpClientStream) requireOpen(exchange bool) error {
	if stream == nil || stream.client == nil {
		return errors.New("vgirpc: TCP stream is nil")
	}
	if stream.closed {
		return errors.New("vgirpc: TCP stream is closed")
	}
	if stream.exchange != exchange {
		return errors.New("vgirpc: operation is invalid for this TCP stream kind")
	}
	return nil
}

func (stream *TcpClientStream) writeTurn(ctx context.Context, batch arrow.RecordBatch,
	metadata map[string]string, cancel bool) error {
	if ctx == nil {
		return errors.New("vgirpc: TCP stream context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	clean := make(map[string]string, len(metadata))
	for key, value := range metadata {
		clean[key] = value
	}
	stripClientControlMetadata(clean)
	if cancel {
		clean[MetaCancel] = "1"
	}
	if _, err := encodeClientBatch(batch, clean, stream.client.codec.maxRequest); err != nil {
		return err
	}
	cleanup, err := stream.client.setOperationDeadline(ctx)
	if err != nil {
		return stream.fail(err)
	}
	defer cleanup()
	if stream.inputWriter == nil {
		stream.inputWriter = ipc.NewWriter(stream.client.conn, ipc.WithSchema(batch.Schema()))
	} else if !clientSchemasEqual(stream.schemas.Input, batch.Schema()) && stream.exchange {
		return &RpcError{Type: "TypeError", Message: "exchange input schema changed within one stream"}
	}
	var outbound arrow.RecordBatch = batch
	if len(clean) != 0 {
		keys := make([]string, 0, len(clean))
		values := make([]string, 0, len(clean))
		for key, value := range clean {
			keys = append(keys, key)
			values = append(values, value)
		}
		annotated := array.NewRecordBatchWithMetadata(
			batch.Schema(), batch.Columns(), batch.NumRows(), arrow.NewMetadata(keys, values))
		defer annotated.Release()
		outbound = annotated
	}
	if err := stream.inputWriter.Write(outbound); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = ctxErr
		}
		return stream.fail(err)
	}
	return nil
}

func (stream *TcpClientStream) ensureOutputReader() error {
	if stream.outputReader != nil {
		return nil
	}
	stream.outputCount = &cappedTCPReader{reader: stream.client.conn, limit: stream.client.codec.maxDecoded}
	reader, err := ipc.NewReader(stream.outputCount)
	if err != nil {
		return fmt.Errorf("read raw stream response: %w", err)
	}
	if !clientSchemasEqual(reader.Schema(), stream.schemas.Output) {
		reader.Release()
		return &RpcError{Type: "TypeError", Message: fmt.Sprintf(
			"response schema mismatch: expected %s, got %s", stream.schemas.Output, reader.Schema())}
	}
	stream.outputReader = reader
	return nil
}

func (stream *TcpClientStream) readNextData(ctx context.Context) (*ClientBatch, error) {
	if ctx == nil {
		return nil, errors.New("vgirpc: TCP stream context must not be nil")
	}
	cleanup, err := stream.client.setOperationDeadline(ctx)
	if err != nil {
		return nil, stream.fail(err)
	}
	defer cleanup()
	if err := stream.ensureOutputReader(); err != nil {
		return nil, stream.fail(err)
	}
	for stream.outputReader.Next() {
		record := stream.outputReader.RecordBatch()
		record.Retain()
		metadata := recordMetadata(record)
		if level := metadata[MetaLogLevel]; record.NumRows() == 0 && level != "" {
			record.Release()
			if level == string(LogException) {
				stream.finished = true
				return nil, rpcErrorFromMetadata(metadata)
			}
			if stream.client.codec.onLog != nil {
				stream.client.codec.onLog(logMessageFromMetadata(metadata))
			}
			continue
		}
		if metadata[MetaLocation] != "" {
			record.Release()
			return nil, stream.fail(&RpcError{Type: "ProtocolError", Message: "external-location responses require an external resolver"})
		}
		return &ClientBatch{Batch: record, Metadata: metadata}, nil
	}
	if err := stream.outputReader.Err(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = ctxErr
		}
		return nil, stream.fail(err)
	}
	stream.finished = true
	return nil, nil
}

// Cancel sends the raw cancellation sentinel, drains the response, and
// releases the connection for subsequent calls.
func (stream *TcpClientStream) Cancel(ctx context.Context) error {
	if stream == nil || stream.closed {
		return nil
	}
	if stream.finished {
		return stream.finish(ctx)
	}
	schema := arrow.NewSchema(nil, nil)
	if stream.exchange {
		schema = stream.schemas.Input
	}
	batch := emptyBatch(schema)
	defer batch.Release()
	if err := stream.writeTurn(ctx, batch, nil, true); err != nil {
		return err
	}
	return stream.finish(ctx)
}

// Close closes the client input stream, drains the worker output, and makes
// the raw connection reusable. The context must bound an unresponsive peer.
func (stream *TcpClientStream) Close(ctx context.Context) error {
	if stream == nil || stream.closed {
		return nil
	}
	return stream.finish(ctx)
}

func (stream *TcpClientStream) finish(ctx context.Context) error {
	if ctx == nil {
		return errors.New("vgirpc: TCP stream context must not be nil")
	}
	cleanup, err := stream.client.setOperationDeadline(ctx)
	if err != nil {
		return stream.fail(err)
	}
	defer cleanup()
	if stream.inputWriter == nil {
		schema := arrow.NewSchema(nil, nil)
		if stream.exchange {
			schema = stream.schemas.Input
		}
		stream.inputWriter = ipc.NewWriter(stream.client.conn, ipc.WithSchema(schema))
	}
	if err := stream.inputWriter.Close(); err != nil {
		return stream.fail(err)
	}
	if stream.outputReader == nil {
		if err := stream.ensureOutputReader(); err != nil {
			return stream.fail(err)
		}
	}
	for stream.outputReader.Next() {
		record := stream.outputReader.RecordBatch()
		metadata := recordMetadata(record)
		if level := metadata[MetaLogLevel]; record.NumRows() == 0 && level != "" {
			if level == string(LogException) {
				err = rpcErrorFromMetadata(metadata)
			} else if stream.client.codec.onLog != nil {
				stream.client.codec.onLog(logMessageFromMetadata(metadata))
			}
		}
	}
	if readErr := stream.outputReader.Err(); readErr != nil && err == nil {
		err = readErr
	}
	stream.finished = true
	stream.closed = true
	stream.release()
	return err
}

// Abort closes and poisons the underlying connection without blocking on
// protocol cleanup. Use it when Close cannot safely wait for the peer.
func (stream *TcpClientStream) Abort() {
	if stream == nil || stream.closed {
		return
	}
	stream.closed = true
	_ = stream.client.poison(errors.New("raw stream was aborted"))
	stream.release()
}

func (stream *TcpClientStream) fail(err error) error {
	stream.closed = true
	stream.client.poison(err)
	stream.release()
	return err
}

func (stream *TcpClientStream) release() {
	if stream.released {
		return
	}
	stream.released = true
	if stream.outputReader != nil {
		stream.outputReader.Release()
	}
	if stream.header != nil {
		stream.header.Release()
		stream.header = nil
	}
	stream.client.mu.Unlock()
}

type cappedTCPReader struct {
	reader io.Reader
	limit  int64
	read   int64
}

func (reader *cappedTCPReader) Read(buffer []byte) (int, error) {
	if reader.read >= reader.limit {
		return 0, &RpcError{Type: "TransportError", Message: fmt.Sprintf(
			"raw response exceeds client limit (%d bytes)", reader.limit)}
	}
	remaining := reader.limit - reader.read
	if int64(len(buffer)) > remaining {
		buffer = buffer[:remaining]
	}
	count, err := reader.reader.Read(buffer)
	reader.read += int64(count)
	return count, err
}

func (client *TcpClient) poison(err error) error {
	client.closed = true
	client.poisoned = err
	_ = client.conn.Close()
	return err
}

// Close closes the stateful connection. It is idempotent.
func (client *TcpClient) Close() error {
	if client == nil {
		return nil
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closed {
		return nil
	}
	client.closed = true
	return client.conn.Close()
}
