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
	"github.com/apache/arrow-go/v18/arrow/array"
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
	if stats != nil {
		stats.RecordInput(req.Batch.NumRows(), batchBufferSize(req.Batch))
	}

	// Build call context
	callCtx := &CallContext{
		Ctx:               ctx,
		RequestID:         req.RequestID,
		ServerID:          s.serverID,
		Method:            req.Method,
		LogLevel:          LogLevel(req.LogLevel),
		Auth:              Anonymous(),
		TransportMetadata: req.Metadata,
		Kind:              s.TransportKind(),
		Implementation:    s.implementation,
	}
	if callCtx.LogLevel == "" {
		callCtx.LogLevel = LogTrace // default: allow all, client filters
	}

	// Call handler. A user panic is an application failure, not a reason to
	// terminate the serving process (and skip the dispatch-end hook).
	var resultVal reflect.Value
	var callErr error
	func() {
		defer func() {
			if rv := recover(); rv != nil {
				callErr = &RpcError{
					Type:    "RuntimeError",
					Message: fmt.Sprintf("handler panicked: %v", rv),
				}
			}
		}()
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
	}()

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
	// Use a closure so the final owner is released. A deferred method call
	// captures its receiver immediately; that would retain the original batch
	// here, double-release it after replacement, and leak the pointer wrapper.
	defer func() { resultBatch.Release() }()

	// Maybe externalize large result batch
	if s.externalConfig != nil {
		extBatch, extMeta, extErr := maybeExternalizeBatchCtx(ctx, resultBatch, arrow.Metadata{}, s.externalConfig)
		if extErr != nil {
			slog.Error("failed to externalize result batch", "err", extErr)
		} else if extBatch != resultBatch {
			withMeta := array.NewRecordBatchWithMetadata(extBatch.Schema(), extBatch.Columns(), extBatch.NumRows(), extMeta)
			resultBatch.Release()
			extBatch.Release()
			resultBatch = withMeta
		}
	}

	// Record output stats
	if stats != nil {
		stats.RecordOutput(resultBatch.NumRows(), batchBufferSize(resultBatch))
	}

	// If the client advertised a shared-memory segment, try to ship the
	// result through it. On success the pipe carries only a small pointer
	// batch (zero rows + offset/length metadata) instead of the full IPC
	// stream.
	wireBatch := resultBatch
	if req.Shm != nil {
		shmBatch, replaced, shmErr := MaybeWriteToShm(resultBatch, req.Shm)
		if shmErr != nil {
			slog.Debug("shm write failed; falling back to pipe", "method", req.Method, "err", shmErr)
		} else if replaced {
			defer shmBatch.Release()
			wireBatch = shmBatch
		}
	}

	return nil, WriteUnaryResponse(w, info.ResultSchema, logs, wireBatch, s.serverID, req.RequestID)
}

// serveStream dispatches a producer or exchange stream method.
// Returns handlerErr (application error reported to hook) and transportErr (I/O error for serve loop).
