// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/klauspost/compress/zstd"
)

const (
	arrowContentType   = "application/vnd.apache.arrow.stream"
	hmacLen            = 32
	defaultTokenTTL    = 5 * time.Minute
	defaultMaxBodySize = 64 << 20 // 64 MB
)

// RegisterStateType registers a concrete type for gob encoding so that it can
// be serialized into HTTP state tokens. Each [ProducerState] and
// [ExchangeState] implementation (and any types they embed) must be
// registered before the first HTTP stream request. Typically this is done
// in a package init() function.
func RegisterStateType(v interface{}) {
	gob.Register(v)
}

// HttpServer serves RPC requests over HTTP. It wraps a [Server] and exposes
// URL routes under a configurable prefix (default "/vgi"):
//
//	POST /vgi/{method}           — unary RPC call
//	POST /vgi/{method}/init      — stream initialization (producer or exchange)
//	POST /vgi/{method}/exchange  — exchange continuation with state token
//	GET  /vgi                    — landing page (HTML)
//	GET  /vgi/describe           — API reference page (HTML)
//
// HttpServer implements [http.Handler] and can be used directly with
// [http.ListenAndServe] or mounted on an existing [http.ServeMux].
type HttpServer struct {
	server      *Server
	signingKey  []byte
	tokenTTL    time.Duration
	maxBodySize int64
	prefix      string
	mux         *http.ServeMux
	zstdEncoder *zstd.Encoder // non-nil when response compression is enabled

	// Pre-rendered HTML pages (built by initPages)
	landingHTML  []byte
	describeHTML []byte
	notFoundHTML []byte

	// Page configuration
	protocolName      string
	repoURL           string
	enableLandingPage  bool
	enableDescribePage bool
	enableNotFoundPage bool
}

// NewHttpServer creates a new HTTP server wrapping an RPC server.
func NewHttpServer(server *Server) *HttpServer {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		panic(fmt.Sprintf("vgirpc: failed to generate signing key: %v", err))
	}
	h := &HttpServer{
		server:      server,
		signingKey:  key,
		tokenTTL:    defaultTokenTTL,
		maxBodySize: defaultMaxBodySize,
		prefix:      "/vgi",

		enableLandingPage:  true,
		enableDescribePage: true,
		enableNotFoundPage: true,
	}
	h.initRoutes()
	return h
}

// NewHttpServerWithKey creates a new HTTP server with a caller-provided signing key.
// The key must be at least 16 bytes long.
func NewHttpServerWithKey(server *Server, signingKey []byte) *HttpServer {
	if len(signingKey) < 16 {
		panic("vgirpc: signing key must be at least 16 bytes")
	}
	h := &HttpServer{
		server:      server,
		signingKey:  signingKey,
		tokenTTL:    defaultTokenTTL,
		maxBodySize: defaultMaxBodySize,
		prefix:      "/vgi",

		enableLandingPage:  true,
		enableDescribePage: true,
		enableNotFoundPage: true,
	}
	h.initRoutes()
	return h
}

// SetProtocolName sets the protocol name displayed on HTML pages.
// If not set, the server's service name is used, falling back to "vgi-rpc Service".
func (h *HttpServer) SetProtocolName(name string) {
	h.protocolName = name
}

// SetRepoURL sets a source repository URL shown on the landing and describe pages.
func (h *HttpServer) SetRepoURL(url string) {
	h.repoURL = url
}

// SetEnableLandingPage controls whether GET requests to the prefix serve an
// HTML landing page. Enabled by default.
func (h *HttpServer) SetEnableLandingPage(enabled bool) {
	h.enableLandingPage = enabled
}

// SetEnableDescribePage controls whether GET {prefix}/describe serves an
// HTML API reference page. Enabled by default.
func (h *HttpServer) SetEnableDescribePage(enabled bool) {
	h.enableDescribePage = enabled
}

// SetEnableNotFoundPage controls whether unmatched routes return a friendly
// HTML 404 page. Enabled by default.
func (h *HttpServer) SetEnableNotFoundPage(enabled bool) {
	h.enableNotFoundPage = enabled
}

// initRoutes registers POST routes for RPC and builds the mux. Call once
// from the constructor. HTML GET routes are added lazily by initPages.
func (h *HttpServer) initRoutes() {
	h.mux = http.NewServeMux()
	h.mux.HandleFunc(fmt.Sprintf("POST %s/{method}/init", h.prefix), h.handleStreamInit)
	h.mux.HandleFunc(fmt.Sprintf("POST %s/{method}/exchange", h.prefix), h.handleStreamExchange)
	h.mux.HandleFunc(fmt.Sprintf("POST %s/{method}", h.prefix), h.handleUnary)
}

// InitPages pre-renders the HTML pages and registers GET routes. This must be
// called after all methods are registered on the server and after any
// Set* configuration calls (SetProtocolName, SetRepoURL, etc.).
//
// If not called explicitly, pages are initialized automatically on the first
// HTTP request.
func (h *HttpServer) InitPages() {
	name := h.protocolName
	if name == "" {
		name = h.server.serviceName
	}
	if name == "" {
		name = "vgi-rpc Service"
	}

	serverID := h.server.serverID

	if h.enableDescribePage {
		h.describeHTML = buildDescribeHTML(h.server, h.prefix, name, h.repoURL)
		h.mux.HandleFunc(fmt.Sprintf("GET %s/describe", h.prefix), h.handleDescribePage)
	}

	if h.enableLandingPage {
		describePath := ""
		if h.enableDescribePage {
			describePath = h.prefix + "/describe"
		}
		h.landingHTML = buildLandingHTML(h.prefix, name, serverID, describePath, h.repoURL)
		h.mux.HandleFunc(fmt.Sprintf("GET %s", h.prefix), h.handleLandingPage)
	}

	if h.enableNotFoundPage {
		h.notFoundHTML = buildNotFoundHTML(h.prefix, name)
		h.mux.HandleFunc("/", h.handleNotFound)
	}
}

// SetTokenTTL sets the maximum age for state tokens.
func (h *HttpServer) SetTokenTTL(d time.Duration) {
	h.tokenTTL = d
}

// SetMaxBodySize sets the maximum allowed HTTP request body size in bytes.
// The limit applies to both raw and decompressed bodies. Set to 0 to disable
// the limit (not recommended for production).
func (h *HttpServer) SetMaxBodySize(n int64) {
	h.maxBodySize = n
}

// SetCompressionLevel enables zstd compression of response bodies at the
// given level (1–11). When enabled, responses are compressed if the client
// sends an Accept-Encoding header containing "zstd". Pass 0 to disable
// response compression (the default).
func (h *HttpServer) SetCompressionLevel(level int) {
	if level <= 0 {
		h.zstdEncoder = nil
		return
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevel(level)))
	if err != nil {
		panic(fmt.Sprintf("vgirpc: failed to create zstd encoder: %v", err))
	}
	h.zstdEncoder = enc
}

// ServeHTTP implements http.Handler.
func (h *HttpServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Auto-initialize pages on first request if not done explicitly.
	if h.landingHTML == nil && h.describeHTML == nil && h.notFoundHTML == nil {
		h.InitPages()
	}

	if h.zstdEncoder != nil && strings.Contains(r.Header.Get("Accept-Encoding"), "zstd") {
		cw := &compressResponseWriter{ResponseWriter: w, encoder: h.zstdEncoder}
		defer cw.finish()
		h.mux.ServeHTTP(cw, r)
		return
	}
	h.mux.ServeHTTP(w, r)
}

// compressResponseWriter buffers the response body and compresses it with zstd
// when the response has an Arrow content type.
type compressResponseWriter struct {
	http.ResponseWriter
	encoder    *zstd.Encoder
	buf        bytes.Buffer
	statusCode int
}

func (cw *compressResponseWriter) WriteHeader(code int) {
	cw.statusCode = code
}

func (cw *compressResponseWriter) Write(data []byte) (int, error) {
	return cw.buf.Write(data)
}

func (cw *compressResponseWriter) finish() {
	if cw.statusCode == 0 {
		cw.statusCode = http.StatusOK
	}
	data := cw.buf.Bytes()
	if cw.ResponseWriter.Header().Get("Content-Type") == arrowContentType && len(data) > 0 {
		compressed := cw.encoder.EncodeAll(data, make([]byte, 0, len(data)))
		cw.ResponseWriter.Header().Set("Content-Encoding", "zstd")
		cw.ResponseWriter.WriteHeader(cw.statusCode)
		_, _ = cw.ResponseWriter.Write(compressed)
	} else {
		cw.ResponseWriter.WriteHeader(cw.statusCode)
		_, _ = cw.ResponseWriter.Write(data)
	}
}

// handleUnary dispatches a unary RPC call.
func (h *HttpServer) handleUnary(w http.ResponseWriter, r *http.Request) {
	method := r.PathValue("method")

	if ct := r.Header.Get("Content-Type"); ct != arrowContentType {
		h.writeHttpError(w, http.StatusUnsupportedMediaType,
			fmt.Errorf("unsupported content type: %s", ct), nil)
		return
	}

	if method == "__describe__" {
		h.handleDescribe(w, r)
		return
	}

	info, ok := h.server.methods[method]
	if !ok {
		h.writeHttpError(w, http.StatusNotFound,
			&RpcError{Type: "AttributeError", Message: fmt.Sprintf("Unknown method: '%s'", method)}, nil)
		return
	}

	if info.Type != MethodUnary {
		h.writeHttpError(w, http.StatusBadRequest,
			&RpcError{Type: "TypeError", Message: fmt.Sprintf("Method '%s' is a stream; use /init endpoint", method)}, nil)
		return
	}

	body, err := h.readHTTPBody(r)
	if err != nil {
		h.writeHttpError(w, http.StatusBadRequest, err, nil)
		return
	}

	req, err := ReadRequest(bytes.NewReader(body))
	if err != nil {
		h.writeHttpError(w, http.StatusBadRequest, err, nil)
		return
	}
	defer req.Batch.Release()

	var handlerErr error
	stats := &CallStatistics{}

	transportMeta := buildHTTPTransportMeta(req.Metadata, r)
	dispatchInfo := DispatchInfo{
		Method:            method,
		MethodType:        DispatchMethodUnary,
		ServerID:          h.server.serverID,
		RequestID:         req.RequestID,
		TransportMetadata: transportMeta,
	}

	ctx, hookCleanup := h.startDispatchHook(r.Context(), dispatchInfo, stats, &handlerErr)
	defer hookCleanup()

	params, err := deserializeParams(req.Batch, info.ParamsType)
	if err != nil {
		handlerErr = &RpcError{Type: "TypeError", Message: fmt.Sprintf("parameter deserialization: %v", err)}
		h.writeHttpError(w, http.StatusBadRequest, handlerErr, info.ResultSchema)
		return
	}

	// Record input stats
	stats.RecordInput(req.Batch.NumRows(), batchBufferSize(req.Batch))

	callCtx := &CallContext{
		Ctx:       ctx,
		RequestID: req.RequestID,
		ServerID:  h.server.serverID,
		Method:    method,
		LogLevel:  LogLevel(req.LogLevel),
	}
	if callCtx.LogLevel == "" {
		callCtx.LogLevel = LogTrace
	}

	// Call handler
	var resultVal reflect.Value
	var callErr error

	if info.ResultType == nil {
		results := info.Handler.Call([]reflect.Value{
			reflect.ValueOf(ctx), reflect.ValueOf(callCtx), params,
		})
		if !results[0].IsNil() {
			callErr = results[0].Interface().(error)
		}
	} else {
		results := info.Handler.Call([]reflect.Value{
			reflect.ValueOf(ctx), reflect.ValueOf(callCtx), params,
		})
		resultVal = results[0]
		if !results[1].IsNil() {
			callErr = results[1].Interface().(error)
		}
	}

	logs := callCtx.drainLogs()

	// Write response
	var buf bytes.Buffer
	if callErr != nil {
		handlerErr = callErr
		ipcW := ipc.NewWriter(&buf, ipc.WithSchema(info.ResultSchema))
		for _, logMsg := range logs {
			_ = writeLogBatch(ipcW, info.ResultSchema, logMsg, h.server.serverID, req.RequestID)
		}
		_ = writeErrorBatch(ipcW, info.ResultSchema, callErr, h.server.serverID, req.RequestID, h.server.debugErrors)
		_ = ipcW.Close()
		statusCode := http.StatusInternalServerError
		if rpcErr, ok := callErr.(*RpcError); ok {
			if rpcErr.Type == "TypeError" || rpcErr.Type == "ValueError" {
				statusCode = http.StatusBadRequest
			}
		}
		h.writeArrow(w, statusCode, buf.Bytes())
		return
	}

	if info.ResultType == nil {
		_ = WriteVoidResponse(&buf, logs, h.server.serverID, req.RequestID)
		h.writeArrow(w, http.StatusOK, buf.Bytes())
		return
	}

	resultBatch, err := serializeResult(info.ResultSchema, resultVal.Interface())
	if err != nil {
		handlerErr = &RpcError{Type: "SerializationError", Message: err.Error()}
		h.writeHttpError(w, http.StatusInternalServerError, handlerErr, info.ResultSchema)
		return
	}
	defer resultBatch.Release()

	// Record output stats
	stats.RecordOutput(resultBatch.NumRows(), batchBufferSize(resultBatch))

	_ = WriteUnaryResponse(&buf, info.ResultSchema, logs, resultBatch, h.server.serverID, req.RequestID)
	h.writeArrow(w, http.StatusOK, buf.Bytes())
}

// handleStreamInit dispatches stream initialization.
func (h *HttpServer) handleStreamInit(w http.ResponseWriter, r *http.Request) {
	method := r.PathValue("method")

	if ct := r.Header.Get("Content-Type"); ct != arrowContentType {
		h.writeHttpError(w, http.StatusUnsupportedMediaType,
			fmt.Errorf("unsupported content type: %s", ct), nil)
		return
	}

	info, ok := h.server.methods[method]
	if !ok {
		h.writeHttpError(w, http.StatusNotFound,
			&RpcError{Type: "AttributeError", Message: fmt.Sprintf("Unknown method: '%s'", method)}, nil)
		return
	}

	if info.Type == MethodUnary {
		h.writeHttpError(w, http.StatusBadRequest,
			&RpcError{Type: "TypeError", Message: fmt.Sprintf("Method '%s' is unary; use base endpoint", method)}, nil)
		return
	}

	body, err := h.readHTTPBody(r)
	if err != nil {
		h.writeHttpError(w, http.StatusBadRequest, err, nil)
		return
	}

	req, err := ReadRequest(bytes.NewReader(body))
	if err != nil {
		h.writeHttpError(w, http.StatusBadRequest, err, nil)
		return
	}
	defer req.Batch.Release()

	var handlerErr error
	stats := &CallStatistics{}

	transportMeta := buildHTTPTransportMeta(req.Metadata, r)
	dispatchInfo := DispatchInfo{
		Method:            method,
		MethodType:        DispatchMethodStream,
		ServerID:          h.server.serverID,
		RequestID:         req.RequestID,
		TransportMetadata: transportMeta,
	}

	ctx, hookCleanup := h.startDispatchHook(r.Context(), dispatchInfo, stats, &handlerErr)
	defer hookCleanup()

	params, err := deserializeParams(req.Batch, info.ParamsType)
	if err != nil {
		handlerErr = &RpcError{Type: "TypeError", Message: err.Error()}
		h.writeHttpError(w, http.StatusBadRequest, handlerErr, nil)
		return
	}

	// Record input stats for init params
	stats.RecordInput(req.Batch.NumRows(), batchBufferSize(req.Batch))

	callCtx := &CallContext{
		Ctx:       ctx,
		RequestID: req.RequestID,
		ServerID:  h.server.serverID,
		Method:    method,
		LogLevel:  LogLevel(req.LogLevel),
	}
	if callCtx.LogLevel == "" {
		callCtx.LogLevel = LogTrace
	}

	// Call stream handler
	results := info.Handler.Call([]reflect.Value{
		reflect.ValueOf(ctx), reflect.ValueOf(callCtx), params,
	})

	if !results[1].IsNil() {
		handlerErr = results[1].Interface().(error)
		statusCode := http.StatusInternalServerError
		if _, ok := handlerErr.(*RpcError); ok {
			statusCode = http.StatusInternalServerError
		}
		h.writeHttpError(w, statusCode, handlerErr, nil)
		return
	}

	streamResult := results[0].Interface().(*StreamResult)
	outputSchema := streamResult.OutputSchema
	state := streamResult.State

	// Determine mode: for MethodDynamic, check the concrete state type
	var isProducer bool
	if info.Type == MethodDynamic {
		if _, ok := state.(ProducerState); ok {
			isProducer = true
		} else if _, ok := state.(ExchangeState); ok {
			isProducer = false
		} else {
			handlerErr = &RpcError{
				Type:    "RuntimeError",
				Message: fmt.Sprintf("dynamic stream state %T does not implement ProducerState or ExchangeState", state),
			}
			h.writeHttpError(w, http.StatusInternalServerError, handlerErr, nil)
			return
		}
	} else {
		isProducer = info.Type == MethodProducer
	}

	var buf bytes.Buffer

	// Write header if present
	if info.HasHeader && streamResult.Header != nil {
		initLogs := callCtx.drainLogs()
		if err := h.server.writeStreamHeader(&buf, streamResult.Header, initLogs); err != nil {
			h.writeHttpError(w, http.StatusInternalServerError, err, nil)
			return
		}
	}

	if isProducer {
		// Run produce loop to completion
		writer := ipc.NewWriter(&buf, ipc.WithSchema(outputSchema))

		// Write any buffered init logs
		initLogs := callCtx.drainLogs()
		for _, logMsg := range initLogs {
			_ = writeLogBatch(writer, outputSchema, logMsg, h.server.serverID, "")
		}

		handlerErr = h.runProduceLoop(ctx, writer, outputSchema, state.(ProducerState), info, stats)
		_ = writer.Close()
	} else {
		// Exchange init — return state token (carry schema for dynamic methods)
		token, err := h.packStateToken(state, outputSchema)
		if err != nil {
			h.writeHttpError(w, http.StatusInternalServerError, err, nil)
			return
		}

		writer := ipc.NewWriter(&buf, ipc.WithSchema(outputSchema))

		// Write any buffered init logs
		initLogs := callCtx.drainLogs()
		for _, logMsg := range initLogs {
			_ = writeLogBatch(writer, outputSchema, logMsg, h.server.serverID, "")
		}

		// Write zero-row batch with state token
		stateMeta := arrow.NewMetadata(
			[]string{MetaStreamState}, []string{string(token)})
		zeroBatch := emptyBatch(outputSchema)
		batchWithMeta := array.NewRecordBatchWithMetadata(
			outputSchema, zeroBatch.Columns(), zeroBatch.NumRows(), stateMeta)
		_ = writer.Write(batchWithMeta)
		batchWithMeta.Release()
		zeroBatch.Release()
		_ = writer.Close()
	}

	h.writeArrow(w, http.StatusOK, buf.Bytes())
}

// handleStreamExchange dispatches stream exchange or producer continuation.
func (h *HttpServer) handleStreamExchange(w http.ResponseWriter, r *http.Request) {
	method := r.PathValue("method")

	if ct := r.Header.Get("Content-Type"); ct != arrowContentType {
		h.writeHttpError(w, http.StatusUnsupportedMediaType,
			fmt.Errorf("unsupported content type: %s", ct), nil)
		return
	}

	info, ok := h.server.methods[method]
	if !ok {
		h.writeHttpError(w, http.StatusNotFound,
			&RpcError{Type: "AttributeError", Message: fmt.Sprintf("Unknown method: '%s'", method)}, nil)
		return
	}

	body, err := h.readHTTPBody(r)
	if err != nil {
		h.writeHttpError(w, http.StatusBadRequest, err, nil)
		return
	}

	// Read the input batch and extract state token from custom metadata
	inputReader, err := ipc.NewReader(bytes.NewReader(body))
	if err != nil {
		h.writeHttpError(w, http.StatusBadRequest, err, nil)
		return
	}
	defer inputReader.Release()

	if !inputReader.Next() {
		h.writeHttpError(w, http.StatusBadRequest,
			fmt.Errorf("no batch in exchange request"), nil)
		return
	}
	inputBatch := inputReader.RecordBatch()

	// Extract state token from custom metadata
	var tokenBytes []byte
	if bwm, ok := inputBatch.(arrow.RecordBatchWithMetadata); ok {
		meta := bwm.Metadata()
		if v, found := meta.GetValue(MetaStreamState); found {
			tokenBytes = []byte(v)
		}
	}

	if tokenBytes == nil {
		h.writeHttpError(w, http.StatusBadRequest,
			&RpcError{Type: "RuntimeError", Message: "Missing state token in exchange request"}, nil)
		return
	}

	tokenData, err := h.unpackStateToken(tokenBytes)
	if err != nil {
		h.writeHttpError(w, http.StatusBadRequest, err, nil)
		return
	}

	var handlerErr error
	stats := &CallStatistics{}

	transportMeta := buildHTTPTransportMeta(nil, r)
	dispatchInfo := DispatchInfo{
		Method:            method,
		MethodType:        DispatchMethodStream,
		ServerID:          h.server.serverID,
		TransportMetadata: transportMeta,
	}

	ctx, hookCleanup := h.startDispatchHook(r.Context(), dispatchInfo, stats, &handlerErr)
	defer hookCleanup()

	// Determine mode: for MethodDynamic, check the concrete state type
	var isProducer bool
	if info.Type == MethodDynamic {
		if _, ok := tokenData.State.(ProducerState); ok {
			isProducer = true
		} else {
			isProducer = false
		}
	} else {
		isProducer = info.Type == MethodProducer
	}

	// For dynamic methods, OutputSchema is not set at registration time —
	// recover it from the serialized schema stored in the state token.
	var outputSchema *arrow.Schema
	if info.Type == MethodDynamic && len(tokenData.SchemaIPC) > 0 {
		var schemaErr error
		outputSchema, schemaErr = deserializeSchema(tokenData.SchemaIPC)
		if schemaErr != nil {
			h.writeHttpError(w, http.StatusBadRequest,
				&RpcError{Type: "RuntimeError", Message: fmt.Sprintf("failed to recover output schema: %v", schemaErr)}, nil)
			return
		}
	} else {
		outputSchema = info.OutputSchema
	}

	if isProducer {
		handlerErr = h.handleProducerContinuation(ctx, w, outputSchema, tokenData.State.(ProducerState), info, stats)
	} else {
		handlerErr = h.handleExchangeCall(ctx, w, inputBatch, outputSchema, tokenData.State.(ExchangeState), info, stats)
	}
}

// handleProducerContinuation runs the produce loop for a continuation request.
// Returns the handler error (if any) for hook reporting.
func (h *HttpServer) handleProducerContinuation(ctx context.Context, w http.ResponseWriter, schema *arrow.Schema,
	state ProducerState, info *methodInfo, stats *CallStatistics) error {

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	handlerErr := h.runProduceLoop(ctx, writer, schema, state, info, stats)
	_ = writer.Close()
	h.writeArrow(w, http.StatusOK, buf.Bytes())
	return handlerErr
}

// handleExchangeCall processes one exchange and returns the result with updated token.
// Returns the handler error (if any) for hook reporting.
func (h *HttpServer) handleExchangeCall(ctx context.Context, w http.ResponseWriter, inputBatch arrow.RecordBatch,
	schema *arrow.Schema, state ExchangeState, info *methodInfo, stats *CallStatistics) error {

	// Record input stats
	stats.RecordInput(inputBatch.NumRows(), batchBufferSize(inputBatch))

	out := newOutputCollector(schema, h.server.serverID, false)
	callCtx := &CallContext{
		Ctx:      ctx,
		ServerID: h.server.serverID,
		Method:   info.Name,
		LogLevel: LogTrace,
	}

	var exchangeErr error
	func() {
		defer func() {
			if rv := recover(); rv != nil {
				exchangeErr = &RpcError{Type: "RuntimeError", Message: fmt.Sprintf("%v", rv)}
			}
		}()
		if err := state.Exchange(ctx, inputBatch, out, callCtx); err != nil {
			exchangeErr = err
		}
	}()

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))

	if exchangeErr != nil {
		_ = writeErrorBatch(writer, schema, exchangeErr, h.server.serverID, "", h.server.debugErrors)
		_ = writer.Close()
		h.writeArrow(w, http.StatusInternalServerError, buf.Bytes())
		return exchangeErr
	}

	if err := out.validate(); err != nil {
		_ = writeErrorBatch(writer, schema, err, h.server.serverID, "", h.server.debugErrors)
		_ = writer.Close()
		h.writeArrow(w, http.StatusInternalServerError, buf.Bytes())
		return err
	}

	// Serialize updated state into new token (carry schema for dynamic methods)
	newToken, err := h.packStateToken(state, schema)
	if err != nil {
		_ = writeErrorBatch(writer, schema, err, h.server.serverID, "", h.server.debugErrors)
		_ = writer.Close()
		h.writeArrow(w, http.StatusInternalServerError, buf.Bytes())
		return err
	}

	// Flush output batches, merging state token into the data batch metadata
	for i, ab := range out.batches {
		isDataBatch := (i == out.dataBatchIdx)
		if ab.meta != nil {
			// Log batch — write as-is
			batchWithMeta := array.NewRecordBatchWithMetadata(
				schema, ab.batch.Columns(), ab.batch.NumRows(), *ab.meta)
			_ = writer.Write(batchWithMeta)
			batchWithMeta.Release()
		} else if isDataBatch {
			// Data batch — record output stats and merge state token
			stats.RecordOutput(ab.batch.NumRows(), batchBufferSize(ab.batch))
			stateMeta := arrow.NewMetadata(
				[]string{MetaStreamState}, []string{string(newToken)})
			batchWithMeta := array.NewRecordBatchWithMetadata(
				schema, ab.batch.Columns(), ab.batch.NumRows(), stateMeta)
			_ = writer.Write(batchWithMeta)
			batchWithMeta.Release()
		} else {
			_ = writer.Write(ab.batch)
		}
		ab.batch.Release()
	}

	_ = writer.Close()
	h.writeArrow(w, http.StatusOK, buf.Bytes())
	return nil
}

// runProduceLoop runs the producer state machine to completion.
// Returns the handler error (if any) for hook reporting.
func (h *HttpServer) runProduceLoop(ctx context.Context, writer *ipc.Writer, schema *arrow.Schema,
	state ProducerState, info *methodInfo, stats *CallStatistics) error {

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		out := newOutputCollector(schema, h.server.serverID, true)
		callCtx := &CallContext{
			Ctx:      ctx,
			ServerID: h.server.serverID,
			Method:   info.Name,
			LogLevel: LogTrace,
		}

		var produceErr error
		func() {
			defer func() {
				if rv := recover(); rv != nil {
					produceErr = &RpcError{Type: "RuntimeError", Message: fmt.Sprintf("%v", rv)}
				}
			}()
			if err := state.Produce(ctx, out, callCtx); err != nil {
				produceErr = err
			}
		}()

		if produceErr != nil {
			_ = writeErrorBatch(writer, schema, produceErr, h.server.serverID, "", h.server.debugErrors)
			return produceErr
		}

		if !out.Finished() {
			if err := out.validate(); err != nil {
				_ = writeErrorBatch(writer, schema, err, h.server.serverID, "", h.server.debugErrors)
				return err
			}
		}

		// Flush output
		for _, ab := range out.batches {
			if ab.meta != nil {
				batchWithMeta := array.NewRecordBatchWithMetadata(
					schema, ab.batch.Columns(), ab.batch.NumRows(), *ab.meta)
				_ = writer.Write(batchWithMeta)
				batchWithMeta.Release()
			} else {
				// Data batch — record output stats
				stats.RecordOutput(ab.batch.NumRows(), batchBufferSize(ab.batch))
				_ = writer.Write(ab.batch)
			}
			ab.batch.Release()
		}

		if out.Finished() {
			return nil
		}
	}
}

// startDispatchHook runs OnDispatchStart (if a hook is configured) and returns
// the (possibly enriched) context plus a cleanup function that must be deferred
// by the caller. The cleanup calls OnDispatchEnd with the current *handlerErr.
func (h *HttpServer) startDispatchHook(ctx context.Context, info DispatchInfo, stats *CallStatistics, handlerErr *error) (context.Context, func()) {
	if h.server.dispatchHook == nil {
		return ctx, func() {}
	}

	var hookToken HookToken
	var active bool
	func() {
		defer func() {
			if rv := recover(); rv != nil {
				slog.Error("dispatch hook start panic", "err", rv)
			}
		}()
		var hookCtx context.Context
		hookCtx, hookToken = h.server.dispatchHook.OnDispatchStart(ctx, info)
		if hookCtx != nil {
			ctx = hookCtx
		}
		active = true
	}()

	cleanup := func() {
		if !active {
			return
		}
		func() {
			defer func() {
				if rv := recover(); rv != nil {
					slog.Error("dispatch hook end panic", "err", rv)
				}
			}()
			h.server.dispatchHook.OnDispatchEnd(ctx, hookToken, info, stats, *handlerErr)
		}()
	}
	return ctx, cleanup
}

// buildHTTPTransportMeta builds transport metadata from IPC metadata and HTTP headers.
func buildHTTPTransportMeta(ipcMeta map[string]string, r *http.Request) map[string]string {
	meta := make(map[string]string, len(ipcMeta)+4)
	for k, v := range ipcMeta {
		meta[k] = v
	}
	if tp := r.Header.Get("Traceparent"); tp != "" {
		meta[MetaTraceparent] = tp
	}
	if ts := r.Header.Get("Tracestate"); ts != "" {
		meta[MetaTracestate] = ts
	}
	meta["remote_addr"] = r.RemoteAddr
	meta["user_agent"] = r.UserAgent()
	return meta
}

// handleDescribe handles the __describe__ introspection endpoint.
func (h *HttpServer) handleDescribe(w http.ResponseWriter, r *http.Request) {
	body, err := h.readHTTPBody(r)
	if err != nil {
		h.writeHttpError(w, http.StatusBadRequest, err, nil)
		return
	}

	req, err := ReadRequest(bytes.NewReader(body))
	if err != nil {
		h.writeHttpError(w, http.StatusBadRequest, err, nil)
		return
	}
	defer req.Batch.Release()

	batch, meta := h.server.buildDescribeBatch()
	defer batch.Release()

	batchWithMeta := array.NewRecordBatchWithMetadata(
		describeSchema, batch.Columns(), batch.NumRows(), meta)
	defer batchWithMeta.Release()

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(describeSchema))
	_ = writer.Write(batchWithMeta)
	_ = writer.Close()

	h.writeArrow(w, http.StatusOK, buf.Bytes())
}

// --- State Token ---

type stateTokenData struct {
	CreatedAt int64
	State     interface{}
	SchemaIPC []byte // serialized output schema for dynamic methods; nil for static
}

func (h *HttpServer) packStateToken(state interface{}, outputSchema *arrow.Schema) ([]byte, error) {
	data := stateTokenData{
		CreatedAt: time.Now().Unix(),
		State:     state,
	}
	if outputSchema != nil {
		data.SchemaIPC = serializeSchema(outputSchema)
	}
	var payload bytes.Buffer
	enc := gob.NewEncoder(&payload)
	if err := enc.Encode(&data); err != nil {
		return nil, fmt.Errorf("state token encode: %w", err)
	}

	payloadBytes := payload.Bytes()
	mac := hmac.New(sha256.New, h.signingKey)
	mac.Write(payloadBytes)
	sig := mac.Sum(nil)

	raw := append(payloadBytes, sig...)
	encoded := make([]byte, base64.StdEncoding.EncodedLen(len(raw)))
	base64.StdEncoding.Encode(encoded, raw)
	return encoded, nil
}

func (h *HttpServer) unpackStateToken(token []byte) (*stateTokenData, error) {
	raw, err := base64.StdEncoding.DecodeString(string(token))
	if err != nil {
		return nil, &RpcError{Type: "RuntimeError", Message: "Malformed state token"}
	}
	token = raw

	if len(token) < hmacLen {
		return nil, &RpcError{Type: "RuntimeError", Message: "Malformed state token"}
	}

	payloadBytes := token[:len(token)-hmacLen]
	receivedSig := token[len(token)-hmacLen:]

	mac := hmac.New(sha256.New, h.signingKey)
	mac.Write(payloadBytes)
	expectedSig := mac.Sum(nil)

	if !hmac.Equal(receivedSig, expectedSig) {
		return nil, &RpcError{Type: "RuntimeError", Message: "State token signature verification failed"}
	}

	// NOTE: gob is not designed for untrusted input and could panic or cause
	// type confusion with attacker-crafted payloads. This is acceptable here
	// because the HMAC is verified above — an attacker cannot reach the gob
	// decoder without knowing the signing key. If the threat model changes
	// (e.g. shared keys across trust boundaries), consider switching to a
	// safer serializer (JSON, protobuf).
	var data stateTokenData
	dec := gob.NewDecoder(bytes.NewReader(payloadBytes))
	if err := dec.Decode(&data); err != nil {
		return nil, fmt.Errorf("state token decode: %w", err)
	}

	age := time.Since(time.Unix(data.CreatedAt, 0))
	if age > h.tokenTTL {
		return nil, &RpcError{Type: "RuntimeError",
			Message: fmt.Sprintf("State token expired (age: %v, ttl: %v)", age, h.tokenTTL)}
	}

	return &data, nil
}

// deserializeSchema recovers an Arrow schema from IPC-serialized bytes.
func deserializeSchema(data []byte) (*arrow.Schema, error) {
	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("schema deserialization: %w", err)
	}
	defer reader.Release()
	return reader.Schema(), nil
}

// --- Helpers ---

// readHTTPBody reads the request body, decompressing it if the Content-Encoding
// header indicates zstd compression. Both the raw and decompressed body are
// limited to h.maxBodySize bytes (0 = unlimited).
func (h *HttpServer) readHTTPBody(r *http.Request) ([]byte, error) {
	limit := h.maxBodySize

	var body []byte
	var err error
	if limit > 0 {
		body, err = io.ReadAll(io.LimitReader(r.Body, limit+1))
	} else {
		body, err = io.ReadAll(r.Body)
	}
	if err != nil {
		return nil, err
	}
	if limit > 0 && int64(len(body)) > limit {
		return nil, &RpcError{Type: "ValueError", Message: fmt.Sprintf("Request body exceeds maximum size of %d bytes", limit)}
	}

	if r.Header.Get("Content-Encoding") == "zstd" {
		reader, err := zstd.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("zstd decompression init: %w", err)
		}
		defer reader.Close()

		if limit > 0 {
			body, err = io.ReadAll(io.LimitReader(reader, limit+1))
		} else {
			body, err = io.ReadAll(reader)
		}
		if err != nil {
			return nil, fmt.Errorf("zstd decompression: %w", err)
		}
		if limit > 0 && int64(len(body)) > limit {
			return nil, &RpcError{Type: "ValueError", Message: fmt.Sprintf("Decompressed body exceeds maximum size of %d bytes", limit)}
		}
	}
	return body, nil
}

func (h *HttpServer) writeHttpError(w http.ResponseWriter, statusCode int, err error, schema *arrow.Schema) {
	if schema == nil {
		schema = arrow.NewSchema(nil, nil)
	}
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	_ = writeErrorBatch(writer, schema, err, h.server.serverID, "", h.server.debugErrors)
	_ = writer.Close()
	h.writeArrow(w, statusCode, buf.Bytes())
}

func (h *HttpServer) writeArrow(w http.ResponseWriter, statusCode int, data []byte) {
	w.Header().Set("Content-Type", arrowContentType)
	w.WriteHeader(statusCode)
	_, _ = w.Write(data)
}
