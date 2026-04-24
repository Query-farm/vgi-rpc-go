// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// Returns handlerErr (application error reported to hook) and transportErr (I/O error for serve loop).
func (s *Server) serveUnary(ctx context.Context, w io.Writer, req *Request, info *methodInfo, stats *CallStatistics) (handlerErr, transportErr error) {
	// Deserialize parameters
	params, err := deserializeParams(req.Batch, info.ParamsType)
	if err != nil {
		handlerErr = &RpcError{Type: "TypeError", Message: fmt.Sprintf("parameter deserialization: %v", err)}
		s.logIPCWriteErr("error-response", req.Method, writeErrorResponse(w, info.ResultSchema, handlerErr, s.serverID, req.RequestID, s.debugErrors))
		return handlerErr, nil
	}

	// Record input stats
	stats.RecordInput(req.Batch.NumRows(), batchBufferSize(req.Batch))

	// Build call context
	callCtx := &CallContext{
		Ctx:               ctx,
		RequestID:         req.RequestID,
		ServerID:          s.serverID,
		Method:            req.Method,
		LogLevel:          LogLevel(req.LogLevel),
		Auth:              Anonymous(),
		TransportMetadata: req.Metadata,
	}
	if callCtx.LogLevel == "" {
		callCtx.LogLevel = LogTrace // default: allow all, client filters
	}

	// Call handler
	var resultVal reflect.Value
	var callErr error

	if info.ResultType == nil {
		// Void handler: func(context.Context, *CallContext, P) error
		results := info.Handler.Call([]reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(callCtx),
			params,
		})
		if !results[0].IsNil() {
			callErr = results[0].Interface().(error)
		}
	} else {
		// Valued handler: func(context.Context, *CallContext, P) (R, error)
		results := info.Handler.Call([]reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(callCtx),
			params,
		})
		resultVal = results[0]
		if !results[1].IsNil() {
			callErr = results[1].Interface().(error)
		}
	}

	logs := callCtx.drainLogs()

	// Handle error
	if callErr != nil {
		// Write error response with logs in a single IPC stream
		ipcW := ipc.NewWriter(w, ipc.WithSchema(info.ResultSchema))
		for _, logMsg := range logs {
			s.logIPCWriteErr("log-batch", req.Method, writeLogBatch(ipcW, info.ResultSchema, logMsg, s.serverID, req.RequestID))
		}
		s.logIPCWriteErr("error-batch", req.Method, writeErrorBatch(ipcW, info.ResultSchema, callErr, s.serverID, req.RequestID, s.debugErrors))
		s.logIPCWriteErr("close", req.Method, ipcW.Close())
		return callErr, nil
	}

	// Handle void result
	if info.ResultType == nil {
		return nil, WriteVoidResponse(w, logs, s.serverID, req.RequestID)
	}

	// Serialize result
	resultBatch, err := serializeResult(info.ResultSchema, resultVal.Interface())
	if err != nil {
		handlerErr = &RpcError{Type: "SerializationError", Message: fmt.Sprintf("result serialization: %v", err)}
		s.logIPCWriteErr("error-response", req.Method, writeErrorResponse(w, info.ResultSchema, handlerErr, s.serverID, req.RequestID, s.debugErrors))
		return handlerErr, nil
	}
	defer resultBatch.Release()

	// Maybe externalize large result batch
	if s.externalConfig != nil {
		extBatch, _, extErr := MaybeExternalizeBatch(resultBatch, arrow.Metadata{}, s.externalConfig)
		if extErr != nil {
			slog.Error("failed to externalize result batch", "err", extErr)
		} else if extBatch != resultBatch {
			resultBatch.Release()
			resultBatch = extBatch
		}
	}

	// Record output stats
	stats.RecordOutput(resultBatch.NumRows(), batchBufferSize(resultBatch))

	return nil, WriteUnaryResponse(w, info.ResultSchema, logs, resultBatch, s.serverID, req.RequestID)
}

// serveStream dispatches a producer or exchange stream method.
// Returns handlerErr (application error reported to hook) and transportErr (I/O error for serve loop).
