// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"reflect"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// handleStreamInit dispatches stream initialization.
func (h *HttpServer) handleStreamInit(w http.ResponseWriter, r *http.Request) {
	identity := h.authenticateIdentity(w, r)
	if identity == nil {
		return
	}
	auth, peerEvidence := identity.auth, identity.evidence

	method := r.PathValue("method")

	if ct := r.Header.Get("Content-Type"); ct != arrowContentType {
		h.writeHttpError(w, http.StatusUnsupportedMediaType,
			fmt.Errorf("unsupported content type: %s", ct), nil)
		return
	}

	info, ok := h.server.methods[method]
	if !ok {
		h.writeHttpError(w, http.StatusNotFound,
			&MethodNotImplementedError{Method: method}, nil)
		return
	}

	if info.Type == MethodUnary {
		h.writeHttpError(w, http.StatusBadRequest,
			&RpcError{Type: "TypeError", Message: fmt.Sprintf("Method '%s' is unary; use base endpoint", method)}, nil)
		return
	}

	body, err := h.readHTTPBody(r)
	if err != nil {
		h.writeBodyReadError(w, err, nil)
		return
	}

	req, err := ReadRequest(bytes.NewReader(body))
	if err != nil {
		h.writeHttpError(w, http.StatusBadRequest, err, nil)
		return
	}
	defer func() { req.Batch.Release() }()
	if req.Method != method {
		h.writeHttpError(w, http.StatusBadRequest, &RpcError{
			Type:    "ProtocolError",
			Message: fmt.Sprintf("Method mismatch: route names %q, request metadata names %q", method, req.Method),
		}, nil)
		return
	}

	// Stream init accepts the same externally uploaded request shape as unary.
	// The routing metadata remains on req.Metadata; only the parameter batch is
	// replaced by the fetched, materialized batch.
	if h.server.externalConfig != nil {
		var outerMeta arrow.Metadata
		if rb, ok := req.Batch.(arrow.RecordBatchWithMetadata); ok {
			outerMeta = rb.Metadata()
		}
		if IsExternalLocationBatch(req.Batch, outerMeta) {
			resolved, _, resolveErr := ResolveExternalLocation(req.Batch, outerMeta, h.server.externalConfig)
			if resolveErr != nil {
				h.writeHttpError(w, http.StatusInternalServerError, &RpcError{
					Type:    "ValueError",
					Message: fmt.Sprintf("resolving external request: %v", resolveErr),
				}, nil)
				return
			}
			req.Batch.Release()
			req.Batch = resolved
		}
	}

	var handlerErr error
	stats := &CallStatistics{}

	// Mint a stable stream_id at /init; round-trip it through the state
	// token so every continuation log record reports the same value.
	streamID := RandomStreamID()
	// Mint the stream's call id and its call token here, once. Everything the
	// call token carries is fixed for the life of the stream, so this is the
	// only point at which any of it is serialized or sealed; continuations
	// echo the token back and resolve it from cache.
	callID, callIDErr := newCallID()
	if callIDErr != nil {
		h.writeHttpError(w, http.StatusInternalServerError, callIDErr, nil)
		return
	}

	// Capture self-contained IPC bytes of the request batch for observability
	// hooks. This re-encodes the whole request payload, so only pay for it when
	// a hook is actually installed to consume DispatchInfo.RequestData.
	// Best-effort: a serialization failure here must not fail dispatch.
	var reqBytes []byte
	if h.server.dispatchHook != nil {
		if rb, serErr := SerializeRequestBatch(req.Batch); serErr == nil {
			reqBytes = rb
		}
	}

	transportMeta := buildHTTPTransportMeta(req.Metadata, r)
	dispatchInfo := DispatchInfo{
		Method:            method,
		MethodType:        DispatchMethodStream,
		ServerID:          h.server.serverID,
		Protocol:          h.server.serviceName,
		ProtocolHash:      h.server.ProtocolHash(),
		ProtocolVersion:   h.server.protocolVersion,
		RequestID:         req.RequestID,
		TransportMetadata: transportMeta,
		Auth:              auth,
		RemoteAddr:        r.RemoteAddr,
		RequestData:       reqBytes,
		StreamID:          streamID,
		Implementation:    h.server.implementation,
	}

	ctx, hookCleanup := h.startDispatchHook(r.Context(), dispatchInfo, stats, &handlerErr)
	defer hookCleanup()

	// Application-protocol-version gate. Same as the HTTP unary path —
	// stream init dispatches directly here without going through
	// server.serveOne. Stream methods never include ``__describe__``,
	// but the exemption is kept for symmetry with the unary path.
	if h.server.protocolVersionSet {
		clientVersion, present := req.Metadata[MetaProtocolVersion]
		if pverr := h.server.checkProtocolVersion(clientVersion, present); pverr != nil {
			handlerErr = pverr
			h.writeHttpError(w, http.StatusBadRequest, pverr, nil)
			return
		}
	}

	params, err := deserializeParams(req.Batch, info.ParamsType)
	if err != nil {
		handlerErr = &RpcError{Type: "TypeError", Message: err.Error()}
		h.writeHttpError(w, http.StatusBadRequest, handlerErr, nil)
		return
	}

	// Record input stats for init params
	stats.RecordInput(req.Batch.NumRows(), batchBufferSize(req.Batch))

	callCtx := &CallContext{
		Ctx:               ctx,
		RequestID:         req.RequestID,
		ServerID:          h.server.serverID,
		Method:            method,
		LogLevel:          LogLevel(req.LogLevel),
		Auth:              auth,
		PeerEvidence:      peerEvidence,
		TransportMetadata: transportMeta,
		Cookies:           buildHTTPCookies(r),
		Kind:              TransportKindHTTP,
		Implementation:    h.server.implementation,
	}
	if callCtx.LogLevel == "" {
		callCtx.LogLevel = LogTrace
	}

	// Resolve sticky session token (when present) before the handler runs.
	// Producer / exchange streams open their session on /init and resume
	// it on every /exchange turn — see handleStreamExchange for the resume
	// path. Wrapping the writer flushes VGI-Session / VGI-Session-Close on
	// the response; ReleaseLock fires after the handler returns.
	stickyCleanup, stickyErr := h.installStickyOnRequest(r, callCtx, auth)
	defer stickyCleanup.ReleaseLock()
	w = stickyCleanup.Wrap(w)
	if stickyErr != nil {
		handlerErr = stickyErr
		h.writeHttpError(w, http.StatusInternalServerError, stickyErr, nil)
		return
	}

	// Call stream handler. Keep panics inside the RPC boundary so sticky
	// cleanup and dispatch-end hooks still run and the client receives a
	// RuntimeError batch.
	var streamResult *StreamResult
	func() {
		defer func() {
			if rv := recover(); rv != nil {
				handlerErr = &RpcError{
					Type:    "RuntimeError",
					Message: fmt.Sprintf("handler panicked: %v", rv),
				}
			}
		}()
		results := info.Handler.Call([]reflect.Value{
			reflect.ValueOf(ctx), reflect.ValueOf(callCtx), params,
		})
		if !results[1].IsNil() {
			handlerErr = results[1].Interface().(error)
			return
		}
		streamResult = results[0].Interface().(*StreamResult)
	}()

	if handlerErr != nil {
		// The 500 is what writeArrow rewrites to 200 + X-VGI-RPC-Error: true —
		// the method ran and failed, so the failure is an application result
		// (same contract as the unary path).
		h.writeHttpError(w, http.StatusInternalServerError, handlerErr, nil)
		return
	}
	if streamResult == nil {
		handlerErr = &RpcError{Type: "RuntimeError", Message: "stream handler returned a nil result"}
		h.writeHttpError(w, http.StatusInternalServerError, handlerErr, nil)
		return
	}
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
		if isProducer {
			if _, ok := state.(ProducerState); !ok {
				handlerErr = &RpcError{
					Type:    "RuntimeError",
					Message: fmt.Sprintf("stream state %T does not implement ProducerState", state),
				}
				h.writeHttpError(w, http.StatusInternalServerError, handlerErr, nil)
				return
			}
		} else if _, ok := state.(ExchangeState); !ok {
			handlerErr = &RpcError{
				Type:    "RuntimeError",
				Message: fmt.Sprintf("stream state %T does not implement ExchangeState", state),
			}
			h.writeHttpError(w, http.StatusInternalServerError, handlerErr, nil)
			return
		}
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
		// Run exactly one producer transition for this init tick.
		writer := ipc.NewWriter(&buf, ipc.WithSchema(outputSchema))

		// Write any buffered init logs
		initLogs := callCtx.drainLogs()
		for _, logMsg := range initLogs {
			h.logIPCWriteErr("log-batch", info.Name, writeLogBatch(writer, outputSchema, logMsg, h.server.serverID, ""))
		}

		// The producer's first turn folds into this /init request, so the init
		// request's custom metadata is what the pipe transports would have
		// delivered on the first tick batch.
		finished, err := h.runProduceTurn(ctx, writer, outputSchema, state.(ProducerState), info, stats, auth, peerEvidence, transportMeta, callCtx.Cookies, callCtx.stickySink, requestMetadata(req))
		handlerErr = err
		if err == nil && !finished {
			// The producer remains active — append a continuation token.
			token, tokenErr := h.packCursorToken(callID, state, auth)
			callToken, callErr := h.packCallToken(callID, outputSchema, auth, streamID)
			if tokenErr != nil {
				handlerErr = tokenErr
			} else if callErr != nil {
				handlerErr = callErr
			} else if werr := writeStateTokenBatch(writer, outputSchema, token, callToken); werr != nil {
				h.logIPCWriteErr("state-token-batch", info.Name, werr)
				handlerErr = werr
			}
		}
		if cerr := writer.Close(); cerr != nil {
			h.logIPCWriteErr("close", info.Name, cerr)
			if handlerErr == nil {
				handlerErr = cerr
			}
		}
	} else {
		// Exchange init — return state token (carry schema for dynamic methods)
		token, err := h.packCursorToken(callID, state, auth)
		if err != nil {
			h.writeHttpError(w, http.StatusInternalServerError, err, nil)
			return
		}
		callToken, err := h.packCallToken(callID, outputSchema, auth, streamID)
		if err != nil {
			h.writeHttpError(w, http.StatusInternalServerError, err, nil)
			return
		}

		writer := ipc.NewWriter(&buf, ipc.WithSchema(outputSchema))

		// Write any buffered init logs
		initLogs := callCtx.drainLogs()
		for _, logMsg := range initLogs {
			h.logIPCWriteErr("log-batch", info.Name, writeLogBatch(writer, outputSchema, logMsg, h.server.serverID, ""))
		}

		// Write zero-row batch with state token
		if werr := writeStateTokenBatch(writer, outputSchema, token, callToken); werr != nil {
			h.logIPCWriteErr("state-token-batch", info.Name, werr)
			handlerErr = werr
		}
		if cerr := writer.Close(); cerr != nil {
			h.logIPCWriteErr("close", info.Name, cerr)
			if handlerErr == nil {
				handlerErr = cerr
			}
		}
	}

	h.writeArrow(w, streamResponseStatus(handlerErr), buf.Bytes())
}

// streamResponseStatus picks the status for a stream response that already
// carries its outcome in the body.
//
// An in-handler stream error stays a plain 200 with an EXCEPTION batch —
// the method ran, and the client learns the outcome by decoding the stream.
// An operator cap refusal is different in kind: nothing the method did is
// being delivered, so it answers with the error-signalling status that
// writeArrow rewrites to 200 + X-VGI-RPC-Error: true, which is what lets a
// proxy see the failure without parsing Arrow. Mirrors the Python
// reference, which flips the response status only on cap overshoot.
func streamResponseStatus(handlerErr error) int {
	var capErr *externalCapError
	if errors.As(handlerErr, &capErr) {
		return http.StatusInternalServerError
	}
	return http.StatusOK
}

// handleStreamExchange dispatches stream exchange or producer continuation.
func (h *HttpServer) handleStreamExchange(w http.ResponseWriter, r *http.Request) {
	identity := h.authenticateIdentity(w, r)
	if identity == nil {
		return
	}
	auth, peerEvidence := identity.auth, identity.evidence

	method := r.PathValue("method")

	if ct := r.Header.Get("Content-Type"); ct != arrowContentType {
		h.writeHttpError(w, http.StatusUnsupportedMediaType,
			fmt.Errorf("unsupported content type: %s", ct), nil)
		return
	}

	info, ok := h.server.methods[method]
	if !ok {
		h.writeHttpError(w, http.StatusNotFound,
			&MethodNotImplementedError{Method: method}, nil)
		return
	}

	body, err := h.readHTTPBody(r)
	if err != nil {
		h.writeBodyReadError(w, err, nil)
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

	// Extract state token and cancel signal from custom metadata BEFORE
	// attempting any schema cast: cancel batches carry an empty schema that
	// would fail the cast, but the server must observe them regardless.
	// Keep the full metadata: it becomes CallContext.InputMetadata for the
	// exchange handler (parity with the pipe transports; the cast below
	// drops it from the batch).
	var tokenBytes []byte
	var callTokenBytes []byte
	var cancelled bool
	var inputMeta arrow.Metadata
	if bwm, ok := inputBatch.(arrow.RecordBatchWithMetadata); ok {
		meta := bwm.Metadata()
		inputMeta = meta
		if v, found := meta.GetValue(MetaStreamState); found {
			tokenBytes = []byte(v)
		}
		if v, found := meta.GetValue(MetaCallState); found {
			callTokenBytes = []byte(v)
		}
		if _, found := meta.GetValue(MetaCancel); found {
			cancelled = true
		}
	}

	// Materialize an externally uploaded exchange batch before schema casting
	// and application dispatch. Tokens found on the small pointer batch remain
	// valid as a compatibility fallback; metadata surfaced to the handler comes
	// from the fetched data batch.
	var resolvedInput arrow.RecordBatch
	if !cancelled && h.server.externalConfig != nil && IsExternalLocationBatch(inputBatch, inputMeta) {
		resolved, resolvedMeta, resolveErr := ResolveExternalLocation(inputBatch, inputMeta, h.server.externalConfig)
		if resolveErr != nil {
			h.writeHttpError(w, http.StatusInternalServerError, &RpcError{
				Type:    "ValueError",
				Message: fmt.Sprintf("resolving external request: %v", resolveErr),
			}, nil)
			return
		}
		resolvedInput = resolved
		defer resolvedInput.Release()
		inputBatch = resolvedInput
		inputMeta = resolvedMeta
		if bwm, ok := inputBatch.(arrow.RecordBatchWithMetadata); ok {
			meta := bwm.Metadata()
			inputMeta = meta
			if v, found := meta.GetValue(MetaStreamState); found {
				tokenBytes = []byte(v)
			}
			if v, found := meta.GetValue(MetaCallState); found {
				callTokenBytes = []byte(v)
			}
		}
	}

	// Cast compatible input types if schema doesn't match exactly.
	// Skip the cast when cancelled: the cancel batch is always empty schema.
	if !cancelled && info.InputSchema != nil && !inputBatch.Schema().Equal(info.InputSchema) {
		castBatch, castErr := castRecordBatch(inputBatch, info.InputSchema)
		if castErr != nil {
			h.writeHttpError(w, http.StatusBadRequest, castErr, nil)
			return
		}
		defer castBatch.Release()
		inputBatch = castBatch
	}

	if tokenBytes == nil {
		h.writeHttpError(w, http.StatusBadRequest,
			&RpcError{Type: "RuntimeError", Message: "Missing state token in exchange request"}, nil)
		return
	}

	// Open the cursor FIRST: its AEAD tag covers the call id and its AAD
	// covers the caller, so the id is authenticated before it is used to
	// resolve anything. See resolveCall for why that ordering is the whole
	// security argument for the cache.
	tokenData, err := h.openCursorToken(tokenBytes, auth)
	if err != nil {
		h.writeHttpError(w, http.StatusBadRequest, err, nil)
		return
	}
	call, err := h.resolveCall(tokenData, callTokenBytes, auth)
	if err != nil {
		h.writeHttpError(w, http.StatusBadRequest, err, nil)
		return
	}

	// Rehydrate non-serializable fields if a callback is registered
	if h.rehydrateFunc != nil {
		if err := h.rehydrateFunc(tokenData.State, method); err != nil {
			h.writeHttpError(w, http.StatusInternalServerError,
				&RpcError{Type: "RuntimeError", Message: fmt.Sprintf("state rehydration failed: %v", err)}, nil)
			return
		}
	}

	var handlerErr error
	stats := &CallStatistics{}

	transportMeta := buildHTTPTransportMeta(nil, r)
	streamID := call.StreamID
	if streamID == "" {
		// Tokens minted before the stream_id field was added — mint fresh.
		streamID = RandomStreamID()
	}
	dispatchInfo := DispatchInfo{
		Method:            method,
		MethodType:        DispatchMethodStream,
		ServerID:          h.server.serverID,
		Protocol:          h.server.serviceName,
		ProtocolHash:      h.server.ProtocolHash(),
		ProtocolVersion:   h.server.protocolVersion,
		TransportMetadata: transportMeta,
		Auth:              auth,
		RemoteAddr:        r.RemoteAddr,
		StreamID:          streamID,
		Cancelled:         cancelled,
		Implementation:    h.server.implementation,
	}

	ctx, hookCleanup := h.startDispatchHook(r.Context(), dispatchInfo, stats, &handlerErr)
	defer hookCleanup()

	// Resolve sticky session token for the continuation/exchange request.
	// Each /exchange turn re-enters the middleware so the per-session lock
	// is acquired fresh — matches Python which runs sticky middleware on
	// every HTTP request. Pre-built sink is threaded down to the three
	// downstream dispatch paths so each CallContext sees the same view.
	stickyCleanup, stickyErr := h.installStickyOnRequestNoCtx(r, auth)
	defer stickyCleanup.ReleaseLock()
	w = stickyCleanup.Wrap(w)
	if stickyErr != nil {
		handlerErr = stickyErr
		h.writeHttpError(w, http.StatusInternalServerError, stickyErr, nil)
		return
	}
	stickySinkForCtx := stickyCleanup.sink

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
	if info.Type == MethodDynamic && len(call.SchemaIPC) > 0 {
		var schemaErr error
		outputSchema, schemaErr = deserializeSchema(call.SchemaIPC)
		if schemaErr != nil {
			h.writeHttpError(w, http.StatusBadRequest,
				&RpcError{Type: "RuntimeError", Message: fmt.Sprintf("failed to recover output schema: %v", schemaErr)}, nil)
			return
		}
	} else {
		outputSchema = info.OutputSchema
	}

	cookies := buildHTTPCookies(r)

	if cancelled {
		handlerErr = h.handleStreamCancel(ctx, w, outputSchema, tokenData.State, info, auth, peerEvidence, transportMeta, cookies, stickySinkForCtx)
		return
	}

	if isProducer {
		handlerErr = h.handleProducerContinuation(ctx, w, outputSchema, tokenData.State.(ProducerState), info, stats, auth, peerEvidence, transportMeta, cookies, streamID, tokenData.CallID, stickySinkForCtx, inputMeta)
	} else {
		handlerErr = h.handleExchangeCall(ctx, w, inputBatch, inputMeta, outputSchema, tokenData.State.(ExchangeState), info, stats, auth, peerEvidence, transportMeta, cookies, streamID, tokenData.CallID, stickySinkForCtx)
	}
}

// handleStreamCancel processes a client-initiated cancel exchange. It invokes
// the optional StreamCanceller hook on the state and writes an empty IPC
// stream (no state token) so the client knows the stream is finished.
func (h *HttpServer) handleStreamCancel(ctx context.Context, w http.ResponseWriter, schema *arrow.Schema,
	state interface{}, info *methodInfo, auth *AuthContext, peerEvidence *PeerEvidenceSet, transportMeta map[string]string, cookies map[string]string, sink *stickySink) error {
	if canceller, ok := state.(StreamCanceller); ok {
		callCtx := &CallContext{
			Ctx:               ctx,
			ServerID:          h.server.serverID,
			Method:            info.Name,
			LogLevel:          LogTrace,
			Auth:              auth,
			PeerEvidence:      peerEvidence,
			TransportMetadata: transportMeta,
			Cookies:           cookies,
			Kind:              TransportKindHTTP,
			Implementation:    h.server.implementation,
			stickySink:        sink,
		}
		func() {
			defer func() {
				if rv := recover(); rv != nil {
					slog.Debug("stream cancel: OnCancel panic", "method", info.Name, "panic", rv)
				}
			}()
			if err := canceller.OnCancel(ctx, callCtx); err != nil {
				slog.Debug("stream cancel: OnCancel error", "method", info.Name, "err", err)
			}
		}()
	}
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	h.logIPCWriteErr("close", info.Name, writer.Close())
	h.writeArrow(w, http.StatusOK, buf.Bytes())
	return nil
}

// handleProducerContinuation runs one producer transition for a continuation request.
// Returns the handler error (if any) for hook reporting.
func (h *HttpServer) handleProducerContinuation(ctx context.Context, w http.ResponseWriter, schema *arrow.Schema,
	state ProducerState, info *methodInfo, stats *CallStatistics, auth *AuthContext, peerEvidence *PeerEvidenceSet, transportMeta map[string]string, cookies map[string]string, streamID string, callID string, sink *stickySink, requestMeta arrow.Metadata) error {

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	// The continuation request's custom metadata is this turn's tick metadata:
	// on the pipe transports every producer turn is a tick batch whose metadata
	// reaches the worker, and DuckDB uses that to push updated dynamic filters
	// (vgi_pushdown_filters — Top-N boundary tightening, join-key IN sets)
	// between ticks. Over HTTP a turn is a continuation POST, so its metadata
	// has to be forwarded here or those updates are silently dropped. The
	// framework's own transport keys are stripped first — the pipe transports
	// never put them on a tick, and the stream-state value is a sealed cursor
	// token that must not surface to user code.
	finished, err := h.runProduceTurn(ctx, writer, schema, state, info, stats, auth, peerEvidence, transportMeta, cookies, sink, stripFrameworkTickMetadata(requestMeta))
	if err == nil && !finished {
		// The producer remains active — append a continuation token.
		token, tokenErr := h.packCursorToken(callID, state, auth)
		if tokenErr != nil {
			err = tokenErr
		} else if werr := writeStateTokenBatch(writer, schema, token, nil); werr != nil {
			h.logIPCWriteErr("state-token-batch", info.Name, werr)
			err = werr
		}
	}
	if cerr := writer.Close(); cerr != nil {
		h.logIPCWriteErr("close", info.Name, cerr)
		if err == nil {
			err = cerr
		}
	}
	h.writeArrow(w, streamResponseStatus(err), buf.Bytes())
	return err
}

// handleExchangeCall processes one exchange and returns the result with updated token.
// Returns the handler error (if any) for hook reporting.
func (h *HttpServer) handleExchangeCall(ctx context.Context, w http.ResponseWriter, inputBatch arrow.RecordBatch,
	inputMeta arrow.Metadata, schema *arrow.Schema, state ExchangeState, info *methodInfo, stats *CallStatistics, auth *AuthContext, peerEvidence *PeerEvidenceSet, transportMeta map[string]string, cookies map[string]string, streamID string, callID string, sink *stickySink) error {

	// Record input stats
	stats.RecordInput(inputBatch.NumRows(), batchBufferSize(inputBatch))

	out := newOutputCollector(schema, h.server.serverID, false)
	out.setBudgets(h.maxResponseBytes, h.maxExternalizedResponseBytes, h.server.externalConfig != nil)
	callCtx := &CallContext{
		Ctx:               ctx,
		ServerID:          h.server.serverID,
		Method:            info.Name,
		LogLevel:          LogTrace,
		Auth:              auth,
		PeerEvidence:      peerEvidence,
		TransportMetadata: transportMeta,
		Cookies:           cookies,
		Kind:              TransportKindHTTP,
		Implementation:    h.server.implementation,
		stickySink:        sink,
		// Surface the request batch's custom metadata to the handler, matching
		// the pipe transports (server_stream.go sets it from the input batch).
		// The framework's own transport keys are stripped first, exactly as on
		// the producer continuation turn: the pipe transports keep stream/call
		// state in the connection and never on a batch, so without this an
		// identical worker sees clean user metadata over subprocess and
		// framework internals over HTTP. MetaStreamState in particular is a
		// sealed cursor token that must not reach application code.
		InputMetadata: stripFrameworkTickMetadata(inputMeta),
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
		out.releaseBatches()
		h.logIPCWriteErr("error-batch", info.Name, writeErrorBatch(writer, schema, exchangeErr, h.server.serverID, "", h.server.debugErrors))
		h.logIPCWriteErr("close", info.Name, writer.Close())
		h.writeArrow(w, http.StatusInternalServerError, buf.Bytes())
		return exchangeErr
	}

	if err := out.validate(); err != nil {
		out.releaseBatches()
		h.logIPCWriteErr("error-batch", info.Name, writeErrorBatch(writer, schema, err, h.server.serverID, "", h.server.debugErrors))
		h.logIPCWriteErr("close", info.Name, writer.Close())
		h.writeArrow(w, http.StatusInternalServerError, buf.Bytes())
		return err
	}

	// Serialize updated state into new token (carry schema for dynamic methods)
	newToken, err := h.packCursorToken(callID, state, auth)
	if err != nil {
		out.releaseBatches()
		h.logIPCWriteErr("error-batch", info.Name, writeErrorBatch(writer, schema, err, h.server.serverID, "", h.server.debugErrors))
		h.logIPCWriteErr("close", info.Name, writer.Close())
		h.writeArrow(w, http.StatusInternalServerError, buf.Bytes())
		return err
	}

	// Refuse an over-budget upload before flushing — exchange is lockstep,
	// so the whole external budget belongs to this single emit and there is
	// no prior accumulation to carry.
	if capErr := h.checkExternalBudget(out, info.Name); capErr != nil {
		out.releaseBatches()
		h.writeExchangeCapError(w, schema, info.Name, capErr)
		return capErr
	}

	// Flush output batches, merging state token into the data batch metadata
	var writeErr error
	var externalBytes int64
	for i, ab := range out.batches {
		isDataBatch := (i == out.dataBatchIdx)
		if isDataBatch {
			// Data batch — record output stats and merge the state token ON
			// TOP of any per-emit metadata (vgi.cache.*, vgi_batch_index, …).
			// The token must ride the data batch even when the emit carried
			// metadata or produced zero rows, or the client sees end-of-stream
			// mid-exchange (mirrors Python's merge_data_metadata).
			// Stats stay on the logical batch — externalizing changes where
			// the bytes travel, not how many the call produced.
			stats.RecordOutput(ab.batch.NumRows(), batchBufferSize(ab.batch))
			bschema := schema
			var keys, values []string
			if ab.meta != nil {
				// Use the batch's own (possibly projection-narrowed) schema
				// and keep the per-emit metadata underneath the token.
				bschema = ab.batch.Schema()
				keys = append(keys, ab.meta.Keys()...)
				values = append(values, ab.meta.Values()...)
			}
			keys = append(keys, MetaStreamState)
			values = append(values, string(newToken))
			merged := arrow.NewMetadata(keys, values)
			toWrite := array.NewRecordBatchWithMetadata(
				bschema, ab.batch.Columns(), ab.batch.NumRows(), merged)
			// Externalize the token-bearing batch, not the bare one: the
			// client recovers the continuation token from the *fetched*
			// stream's metadata, so it has to be inside the upload.
			if extBatch, rawBytes, replaced := h.externalizeStreamDataBatch(ctx, toWrite); replaced {
				toWrite.Release()
				toWrite = extBatch
				externalBytes += rawBytes
			}
			if werr := writer.Write(toWrite); werr != nil {
				h.logIPCWriteErr("data-batch", info.Name, werr)
				if writeErr == nil {
					writeErr = werr
				}
			}
			toWrite.Release()
		} else if ab.meta != nil {
			// Use the batch's own (possibly projection-narrowed) schema and
			// attach the custom metadata on top.
			batchWithMeta := array.NewRecordBatchWithMetadata(
				ab.batch.Schema(), ab.batch.Columns(), ab.batch.NumRows(), *ab.meta)
			if werr := writer.Write(batchWithMeta); werr != nil {
				h.logIPCWriteErr("log-batch", info.Name, werr)
				if writeErr == nil {
					writeErr = werr
				}
			}
			batchWithMeta.Release()
		} else {
			if werr := writer.Write(ab.batch); werr != nil {
				h.logIPCWriteErr("batch", info.Name, werr)
				if writeErr == nil {
					writeErr = werr
				}
			}
		}
		ab.batch.Release()
	}

	if cerr := writer.Close(); cerr != nil {
		h.logIPCWriteErr("close", info.Name, cerr)
		if writeErr == nil {
			writeErr = cerr
		}
	}

	// Hard cap enforcement for exchange, both channels: if the IPC body
	// exceeded max_response_bytes, or the upload exceeded
	// max_externalized_response_bytes, replace the response with a fresh
	// EXCEPTION-only stream so the client surfaces RpcError. The external
	// arm is a backstop — the pre-flight above refuses the upload before it
	// happens — and catches the case where the predicted size understated
	// what actually went out.
	if capErr := enforceResponseBudgets(info.Name, int64(buf.Len()), externalBytes,
		h.maxResponseBytes, h.maxExternalizedResponseBytes); capErr != nil {
		h.writeExchangeCapError(w, schema, info.Name, capErr)
		return capErr
	}

	h.writeArrow(w, http.StatusOK, buf.Bytes())
	return writeErr
}

// writeExchangeCapError discards whatever the exchange turn produced and
// answers with a fresh IPC stream carrying only the cap-overshoot error.
// Goes out at the error-signalling status so writeArrow rewrites it to
// 200 + X-VGI-RPC-Error: true.
func (h *HttpServer) writeExchangeCapError(w http.ResponseWriter, schema *arrow.Schema, method string, capErr error) {
	var errBuf bytes.Buffer
	errW := ipc.NewWriter(&errBuf, ipc.WithSchema(schema))
	h.logIPCWriteErr("cap-error-batch", method, writeErrorBatch(errW, schema, capErr, h.server.serverID, "", h.server.debugErrors))
	h.logIPCWriteErr("close", method, errW.Close())
	h.writeArrow(w, http.StatusInternalServerError, errBuf.Bytes())
}

// externalizeStreamDataBatch offloads a stream's data batch to external
// storage when it clears the configured threshold. It returns the batch to
// write, the raw (pre-compression) byte count to charge against
// max_externalized_response_bytes, and whether the swap happened. The
// caller owns the returned batch only when the last result is true;
// otherwise the batch travels inline and the original is returned as-is.
//
// `batch` must already carry every piece of custom metadata the client is
// meant to see — per-emit annotations and, on the exchange path, the
// continuation token. The upload serializes the batch *with* its metadata
// and the client reads that metadata back off the fetched inner stream, so
// anything left on the pointer batch instead would be silently dropped when
// the location resolves. (An exchange whose token rode the pointer would
// end the stream mid-conversation.) This mirrors Python's
// maybe_externalize_collector, which uploads the whole cycle before
// replacing it with the pointer.
//
// An upload failure is logged and the batch travels inline: externalization
// is an optimization, not a correctness requirement.
func (h *HttpServer) externalizeStreamDataBatch(ctx context.Context, batch arrow.RecordBatch) (arrow.RecordBatch, int64, bool) {
	if h.server.externalConfig == nil || batch.NumRows() == 0 {
		return batch, 0, false
	}
	extBatch, extMeta, rawBytes, err := externalizeBatchCtx(ctx, batch, arrow.Metadata{}, h.server.externalConfig)
	if err != nil {
		slog.Error("failed to externalize stream batch", "err", err)
		return batch, 0, false
	}
	if extBatch == batch {
		return batch, 0, false
	}
	// Wrap the pointer batch with the location metadata so the IPC writer
	// surfaces it on the wire (same step the unary path takes).
	withMeta := array.NewRecordBatchWithMetadata(extBatch.Schema(), extBatch.Columns(), extBatch.NumRows(), extMeta)
	extBatch.Release()
	return withMeta, rawBytes, true
}

// checkExternalBudget pre-flights the external cap for one collector turn.
// It returns a non-nil error when uploading this turn's data batch would
// push the call past max_externalized_response_bytes.
//
// Pre-flight rather than post-flush because the operator's intent in
// setting the cap is "don't emit data beyond this per call", not "emit and
// then complain" — predicting the upload size from the data batch's buffer
// size refuses a violating upload without paying for the storage
// round-trip. This is also why the cap is *hard* for producer streams,
// which get a soft wire cap: by the time a continuation token could carry
// the overshoot to the next turn, the bytes are already in object storage
// and the egress is already billed.
func (h *HttpServer) checkExternalBudget(out *OutputCollector, method string) error {
	if h.server.externalConfig == nil || h.maxExternalizedResponseBytes <= 0 || out.dataBatchIdx < 0 {
		return nil
	}
	predicted := predictExternalizeBytes(out.batches[out.dataBatchIdx].batch, h.server.externalConfig)
	if predicted == 0 {
		return nil
	}
	if predicted > h.maxExternalizedResponseBytes {
		return newExternalCapError(method, predicted, h.maxExternalizedResponseBytes)
	}
	return nil
}

// requestMetadata renders a decoded request's custom metadata as arrow.Metadata,
// the shape CallContext.InputMetadata takes on the pipe transports. Keys are
// sorted so the result is deterministic.
func requestMetadata(req *Request) arrow.Metadata {
	if req == nil || len(req.Metadata) == 0 {
		return arrow.Metadata{}
	}
	keys := make([]string, 0, len(req.Metadata))
	for k := range req.Metadata {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	values := make([]string, len(keys))
	for i, k := range keys {
		values[i] = req.Metadata[k]
	}
	return arrow.NewMetadata(keys, values)
}

// frameworkTickMetadataKeys are the metadata keys the HTTP transport itself
// puts on a stream continuation request. They are transport plumbing, not
// application metadata: the pipe transports carry the equivalent state in the
// connection rather than on the batch, so a worker must never see them on a
// tick. MetaStreamState in particular is a sealed cursor token.
var frameworkTickMetadataKeys = map[string]struct{}{
	MetaStreamState: {},
	MetaCallState:   {},
	MetaCancel:      {},
}

// stripFrameworkTickMetadata returns meta with the framework's transport keys
// removed, preserving the relative order of the remaining keys.
func stripFrameworkTickMetadata(meta arrow.Metadata) arrow.Metadata {
	if meta.Len() == 0 {
		return arrow.Metadata{}
	}
	srcKeys := meta.Keys()
	srcValues := meta.Values()
	keys := make([]string, 0, len(srcKeys))
	values := make([]string, 0, len(srcValues))
	for i, k := range srcKeys {
		if _, framework := frameworkTickMetadataKeys[k]; framework {
			continue
		}
		keys = append(keys, k)
		values = append(values, srcValues[i])
	}
	if len(keys) == 0 {
		return arrow.Metadata{}
	}
	return arrow.NewMetadata(keys, values)
}

// runProduceTurn drives exactly one lock-step producer transition. Returns
// (true, nil) when the producer has finished, (false, nil) when the caller
// should emit a continuation token, or (false, err) on error.
//
// firstTickMeta is surfaced as CallContext.InputMetadata on the Produce
// call of this HTTP turn. On the pipe transports every producer turn is a
// distinct tick batch whose custom metadata reaches the worker; over HTTP the
// first turn folds into the /init request and later turns are continuation
// POSTs, so callers pass the corresponding request's metadata (framework
// transport keys stripped).
func (h *HttpServer) runProduceTurn(ctx context.Context, writer *ipc.Writer, schema *arrow.Schema,
	state ProducerState, info *methodInfo, stats *CallStatistics, auth *AuthContext, peerEvidence *PeerEvidenceSet, transportMeta map[string]string, cookies map[string]string, sink *stickySink, firstTickMeta arrow.Metadata) (bool, error) {

	if err := ctx.Err(); err != nil {
		return true, nil
	}
	out := newOutputCollector(schema, h.server.serverID, true)
	remainingExternal := h.maxExternalizedResponseBytes
	out.setBudgets(h.maxResponseBytes, remainingExternal, h.server.externalConfig != nil)
	callCtx := &CallContext{
		Ctx:               ctx,
		ServerID:          h.server.serverID,
		Method:            info.Name,
		LogLevel:          LogTrace,
		Auth:              auth,
		PeerEvidence:      peerEvidence,
		TransportMetadata: transportMeta,
		Cookies:           cookies,
		Kind:              TransportKindHTTP,
		Implementation:    h.server.implementation,
		stickySink:        sink,
	}
	callCtx.InputMetadata = firstTickMeta

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
		out.releaseBatches()
		h.logIPCWriteErr("error-batch", info.Name, writeErrorBatch(writer, schema, produceErr, h.server.serverID, "", h.server.debugErrors))
		return false, produceErr
	}

	if err := out.validate(); err != nil {
		out.releaseBatches()
		h.logIPCWriteErr("error-batch", info.Name, writeErrorBatch(writer, schema, err, h.server.serverID, "", h.server.debugErrors))
		return false, err
	}

	// Refuse an over-budget upload before flushing anything from this
	// cycle — see checkExternalBudget for why the external cap gets no
	// continuation-token escape.
	if capErr := h.checkExternalBudget(out, info.Name); capErr != nil {
		out.releaseBatches()
		h.logIPCWriteErr("cap-error-batch", info.Name, writeErrorBatch(writer, schema, capErr, h.server.serverID, "", h.server.debugErrors))
		return false, capErr
	}

	// Flush output. The data batch is the one at out.dataBatchIdx; it may
	// carry per-batch metadata (e.g. vgi_batch_index, vgi_partition_values).
	// Classifying purely by "has metadata" would mis-file an annotated data
	// batch as a log batch.
	for i, ab := range out.batches {
		isDataBatch := i == out.dataBatchIdx
		if isDataBatch {
			// Stats stay on the *logical* batch: output_bytes answers "how
			// much Arrow data did this call produce", which externalizing
			// does not change — only where the bytes travel. The upload
			// itself is reported separately as externalized_bytes.
			stats.RecordOutput(ab.batch.NumRows(), batchBufferSize(ab.batch))
			// Attach per-emit metadata BEFORE offering the batch for
			// externalization: the uploaded stream is where the client
			// reads that metadata back from.
			toWrite := ab.batch
			owned := false
			if ab.meta != nil {
				toWrite = array.NewRecordBatchWithMetadata(
					schema, ab.batch.Columns(), ab.batch.NumRows(), *ab.meta)
				owned = true
			}
			if extBatch, _, replaced := h.externalizeStreamDataBatch(ctx, toWrite); replaced {
				if owned {
					toWrite.Release()
				}
				toWrite = extBatch
				owned = true
			}
			werr := writer.Write(toWrite)
			if owned {
				toWrite.Release()
			}
			if werr != nil {
				h.logIPCWriteErr("data-batch", info.Name, werr)
				ab.batch.Release()
				return false, werr
			}
		} else if ab.meta != nil {
			batchWithMeta := array.NewRecordBatchWithMetadata(
				schema, ab.batch.Columns(), ab.batch.NumRows(), *ab.meta)
			if werr := writer.Write(batchWithMeta); werr != nil {
				h.logIPCWriteErr("log-batch", info.Name, werr)
				batchWithMeta.Release()
				ab.batch.Release()
				return false, werr
			}
			batchWithMeta.Release()
		} else {
			if werr := writer.Write(ab.batch); werr != nil {
				h.logIPCWriteErr("batch", info.Name, werr)
				ab.batch.Release()
				return false, werr
			}
		}
		ab.batch.Release()
	}

	if out.Finished() {
		return true, nil
	}
	return false, nil
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
