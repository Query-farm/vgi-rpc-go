// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// handleStreamInit dispatches stream initialization.
func (h *HttpServer) handleStreamInit(w http.ResponseWriter, r *http.Request) {
	auth := h.authenticate(w, r)
	if auth == nil {
		return
	}

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
		Auth:              auth,
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
		Ctx:               ctx,
		RequestID:         req.RequestID,
		ServerID:          h.server.serverID,
		Method:            method,
		LogLevel:          LogLevel(req.LogLevel),
		Auth:              auth,
		TransportMetadata: transportMeta,
		Cookies:           buildHTTPCookies(r),
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
		// Run produce loop (may be limited by producerBatchLimit)
		writer := ipc.NewWriter(&buf, ipc.WithSchema(outputSchema))

		// Write any buffered init logs
		initLogs := callCtx.drainLogs()
		for _, logMsg := range initLogs {
			h.logIPCWriteErr("log-batch", info.Name, writeLogBatch(writer, outputSchema, logMsg, h.server.serverID, ""))
		}

		finished, err := h.runProduceLoop(ctx, writer, outputSchema, state.(ProducerState), info, stats, auth, transportMeta, callCtx.Cookies)
		handlerErr = err
		if err == nil && !finished {
			// Batch limit reached — append continuation token
			token, tokenErr := h.packStateToken(state, outputSchema)
			if tokenErr != nil {
				handlerErr = tokenErr
			} else if werr := writeStateTokenBatch(writer, outputSchema, token); werr != nil {
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
		token, err := h.packStateToken(state, outputSchema)
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
		if werr := writeStateTokenBatch(writer, outputSchema, token); werr != nil {
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

	h.writeArrow(w, http.StatusOK, buf.Bytes())
}

// handleStreamExchange dispatches stream exchange or producer continuation.
func (h *HttpServer) handleStreamExchange(w http.ResponseWriter, r *http.Request) {
	auth := h.authenticate(w, r)
	if auth == nil {
		return
	}

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

	// Extract state token and cancel signal from custom metadata BEFORE
	// attempting any schema cast: cancel batches carry an empty schema that
	// would fail the cast, but the server must observe them regardless.
	var tokenBytes []byte
	var cancelled bool
	if bwm, ok := inputBatch.(arrow.RecordBatchWithMetadata); ok {
		meta := bwm.Metadata()
		if v, found := meta.GetValue(MetaStreamState); found {
			tokenBytes = []byte(v)
		}
		if _, found := meta.GetValue(MetaCancel); found {
			cancelled = true
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

	tokenData, err := h.unpackStateToken(tokenBytes)
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
	dispatchInfo := DispatchInfo{
		Method:            method,
		MethodType:        DispatchMethodStream,
		ServerID:          h.server.serverID,
		TransportMetadata: transportMeta,
		Auth:              auth,
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

	cookies := buildHTTPCookies(r)

	if cancelled {
		handlerErr = h.handleStreamCancel(ctx, w, outputSchema, tokenData.State, info, auth, transportMeta, cookies)
		return
	}

	if isProducer {
		handlerErr = h.handleProducerContinuation(ctx, w, outputSchema, tokenData.State.(ProducerState), info, stats, auth, transportMeta, cookies)
	} else {
		handlerErr = h.handleExchangeCall(ctx, w, inputBatch, outputSchema, tokenData.State.(ExchangeState), info, stats, auth, transportMeta, cookies)
	}
}

// handleStreamCancel processes a client-initiated cancel exchange. It invokes
// the optional StreamCanceller hook on the state and writes an empty IPC
// stream (no state token) so the client knows the stream is finished.
func (h *HttpServer) handleStreamCancel(ctx context.Context, w http.ResponseWriter, schema *arrow.Schema,
	state interface{}, info *methodInfo, auth *AuthContext, transportMeta map[string]string, cookies map[string]string) error {
	if canceller, ok := state.(StreamCanceller); ok {
		callCtx := &CallContext{
			Ctx:               ctx,
			ServerID:          h.server.serverID,
			Method:            info.Name,
			LogLevel:          LogTrace,
			Auth:              auth,
			TransportMetadata: transportMeta,
			Cookies:           cookies,
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

// handleProducerContinuation runs the produce loop for a continuation request.
// Returns the handler error (if any) for hook reporting.
func (h *HttpServer) handleProducerContinuation(ctx context.Context, w http.ResponseWriter, schema *arrow.Schema,
	state ProducerState, info *methodInfo, stats *CallStatistics, auth *AuthContext, transportMeta map[string]string, cookies map[string]string) error {

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	finished, err := h.runProduceLoop(ctx, writer, schema, state, info, stats, auth, transportMeta, cookies)
	if err == nil && !finished {
		// Batch limit reached — append continuation token
		token, tokenErr := h.packStateToken(state, schema)
		if tokenErr != nil {
			err = tokenErr
		} else if werr := writeStateTokenBatch(writer, schema, token); werr != nil {
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
	h.writeArrow(w, http.StatusOK, buf.Bytes())
	return err
}

// handleExchangeCall processes one exchange and returns the result with updated token.
// Returns the handler error (if any) for hook reporting.
func (h *HttpServer) handleExchangeCall(ctx context.Context, w http.ResponseWriter, inputBatch arrow.RecordBatch,
	schema *arrow.Schema, state ExchangeState, info *methodInfo, stats *CallStatistics, auth *AuthContext, transportMeta map[string]string, cookies map[string]string) error {

	// Record input stats
	stats.RecordInput(inputBatch.NumRows(), batchBufferSize(inputBatch))

	out := newOutputCollector(schema, h.server.serverID, false)
	callCtx := &CallContext{
		Ctx:               ctx,
		ServerID:          h.server.serverID,
		Method:            info.Name,
		LogLevel:          LogTrace,
		Auth:              auth,
		TransportMetadata: transportMeta,
		Cookies:           cookies,
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
		h.logIPCWriteErr("error-batch", info.Name, writeErrorBatch(writer, schema, exchangeErr, h.server.serverID, "", h.server.debugErrors))
		h.logIPCWriteErr("close", info.Name, writer.Close())
		h.writeArrow(w, http.StatusInternalServerError, buf.Bytes())
		return exchangeErr
	}

	if err := out.validate(); err != nil {
		h.logIPCWriteErr("error-batch", info.Name, writeErrorBatch(writer, schema, err, h.server.serverID, "", h.server.debugErrors))
		h.logIPCWriteErr("close", info.Name, writer.Close())
		h.writeArrow(w, http.StatusInternalServerError, buf.Bytes())
		return err
	}

	// Serialize updated state into new token (carry schema for dynamic methods)
	newToken, err := h.packStateToken(state, schema)
	if err != nil {
		h.logIPCWriteErr("error-batch", info.Name, writeErrorBatch(writer, schema, err, h.server.serverID, "", h.server.debugErrors))
		h.logIPCWriteErr("close", info.Name, writer.Close())
		h.writeArrow(w, http.StatusInternalServerError, buf.Bytes())
		return err
	}

	// Flush output batches, merging state token into the data batch metadata
	var writeErr error
	for i, ab := range out.batches {
		isDataBatch := (i == out.dataBatchIdx)
		if ab.meta != nil {
			// Log batch — write as-is
			batchWithMeta := array.NewRecordBatchWithMetadata(
				schema, ab.batch.Columns(), ab.batch.NumRows(), *ab.meta)
			if werr := writer.Write(batchWithMeta); werr != nil {
				h.logIPCWriteErr("log-batch", info.Name, werr)
				if writeErr == nil {
					writeErr = werr
				}
			}
			batchWithMeta.Release()
		} else if isDataBatch {
			// Data batch — record output stats and merge state token
			stats.RecordOutput(ab.batch.NumRows(), batchBufferSize(ab.batch))
			stateMeta := arrow.NewMetadata(
				[]string{MetaStreamState}, []string{string(newToken)})
			batchWithMeta := array.NewRecordBatchWithMetadata(
				schema, ab.batch.Columns(), ab.batch.NumRows(), stateMeta)
			if werr := writer.Write(batchWithMeta); werr != nil {
				h.logIPCWriteErr("data-batch", info.Name, werr)
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
	h.writeArrow(w, http.StatusOK, buf.Bytes())
	return writeErr
}

// runProduceLoop runs the producer state machine until completion or the batch
// limit is reached. Returns (true, nil) when the producer has finished,
// (false, nil) when the batch limit was reached (caller should emit a
// continuation token), or (false, err) on error.
func (h *HttpServer) runProduceLoop(ctx context.Context, writer *ipc.Writer, schema *arrow.Schema,
	state ProducerState, info *methodInfo, stats *CallStatistics, auth *AuthContext, transportMeta map[string]string, cookies map[string]string) (bool, error) {

	dataBatches := 0
	for {
		if err := ctx.Err(); err != nil {
			return true, nil
		}
		out := newOutputCollector(schema, h.server.serverID, true)
		callCtx := &CallContext{
			Ctx:               ctx,
			ServerID:          h.server.serverID,
			Method:            info.Name,
			LogLevel:          LogTrace,
			Auth:              auth,
			TransportMetadata: transportMeta,
			Cookies:           cookies,
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
			h.logIPCWriteErr("error-batch", info.Name, writeErrorBatch(writer, schema, produceErr, h.server.serverID, "", h.server.debugErrors))
			return false, produceErr
		}

		if !out.Finished() {
			if err := out.validate(); err != nil {
				h.logIPCWriteErr("error-batch", info.Name, writeErrorBatch(writer, schema, err, h.server.serverID, "", h.server.debugErrors))
				return false, err
			}
		}

		// Flush output
		for _, ab := range out.batches {
			if ab.meta != nil {
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
				// Data batch — record output stats
				stats.RecordOutput(ab.batch.NumRows(), batchBufferSize(ab.batch))
				if werr := writer.Write(ab.batch); werr != nil {
					h.logIPCWriteErr("data-batch", info.Name, werr)
					ab.batch.Release()
					return false, werr
				}
				dataBatches++
			}
			ab.batch.Release()
		}

		if out.Finished() {
			return true, nil
		}

		// Check batch limit
		if h.producerBatchLimit > 0 && dataBatches >= h.producerBatchLimit {
			return false, nil
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
