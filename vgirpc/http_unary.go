// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// handleUnary dispatches a unary RPC call.
func (h *HttpServer) handleUnary(w http.ResponseWriter, r *http.Request) {
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
	defer func() { req.Batch.Release() }()

	// If the client externalized the parameters via __upload_url__/init,
	// the request batch is a zero-row pointer batch carrying
	// vgi_rpc.location. Fetch the referenced IPC stream and replace the
	// batch with the inner (real) batch. Dispatch metadata
	// (vgi_rpc.method, vgi_rpc.request_version) is taken from the outer
	// batch (req.Method already populated above).
	if h.server.externalConfig != nil {
		var outerMeta arrow.Metadata
		if rb, ok := req.Batch.(arrow.RecordBatchWithMetadata); ok {
			outerMeta = rb.Metadata()
		}
		if IsExternalLocationBatch(req.Batch, outerMeta) {
			resolved, _, rerr := ResolveExternalLocation(req.Batch, outerMeta, h.server.externalConfig)
			if rerr != nil {
				h.writeHttpError(w, http.StatusBadRequest, &RpcError{
					Type:    "ValueError",
					Message: fmt.Sprintf("resolving external request: %v", rerr),
				}, nil)
				return
			}
			req.Batch.Release()
			req.Batch = resolved
		}
	}

	var handlerErr error
	stats := &CallStatistics{}

	// Capture self-contained IPC bytes of the request batch for the access log.
	var reqBytes []byte
	if rb, serErr := SerializeRequestBatch(req.Batch); serErr == nil {
		reqBytes = rb
	}

	transportMeta := buildHTTPTransportMeta(req.Metadata, r)
	dispatchInfo := DispatchInfo{
		Method:            method,
		MethodType:        DispatchMethodUnary,
		ServerID:          h.server.serverID,
		Protocol:          h.server.serviceName,
		ProtocolHash:      h.server.ProtocolHash(),
		ProtocolVersion:   h.server.protocolVersion,
		RequestID:         req.RequestID,
		TransportMetadata: transportMeta,
		Auth:              auth,
		RemoteAddr:        r.RemoteAddr,
		RequestData:       reqBytes,
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
	callCtx.enableCookieSink()

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
	responseCookies := callCtx.drainCookies()
	applyResponseCookies(w, responseCookies)

	// Write response
	var buf bytes.Buffer
	if callErr != nil {
		handlerErr = callErr
		ipcW := ipc.NewWriter(&buf, ipc.WithSchema(info.ResultSchema))
		for _, logMsg := range logs {
			h.logIPCWriteErr("log-batch", info.Name, writeLogBatch(ipcW, info.ResultSchema, logMsg, h.server.serverID, req.RequestID))
		}
		h.logIPCWriteErr("error-batch", info.Name, writeErrorBatch(ipcW, info.ResultSchema, callErr, h.server.serverID, req.RequestID, h.server.debugErrors))
		h.logIPCWriteErr("close", info.Name, ipcW.Close())
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
		if err := WriteVoidResponse(&buf, logs, h.server.serverID, req.RequestID); err != nil {
			h.logIPCWriteErr("void-response", info.Name, err)
			handlerErr = err
		}
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

	// Externalize the result batch if it exceeds the configured threshold.
	if h.server.externalConfig != nil {
		extBatch, extMeta, extErr := MaybeExternalizeBatch(resultBatch, arrow.Metadata{}, h.server.externalConfig)
		if extErr != nil {
			slog.Error("failed to externalize unary result", "method", info.Name, "err", extErr)
		} else if extBatch != resultBatch {
			// Wrap the pointer batch with the location metadata so the
			// IPC writer surfaces it on the wire.
			withMeta := array.NewRecordBatchWithMetadata(extBatch.Schema(), extBatch.Columns(), extBatch.NumRows(), extMeta)
			resultBatch.Release()
			extBatch.Release()
			resultBatch = withMeta
		}
	}

	// Record output stats
	stats.RecordOutput(resultBatch.NumRows(), batchBufferSize(resultBatch))

	if err := WriteUnaryResponse(&buf, info.ResultSchema, logs, resultBatch, h.server.serverID, req.RequestID); err != nil {
		h.logIPCWriteErr("unary-response", info.Name, err)
		handlerErr = err
	}
	h.writeArrow(w, http.StatusOK, buf.Bytes())
}
