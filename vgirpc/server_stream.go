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

func (s *Server) serveStream(ctx context.Context, r io.Reader, w io.Writer, req *Request, info *methodInfo, stats *CallStatistics) (handlerErr, transportErr error) {
	// Deserialize parameters
	params, err := deserializeParams(req.Batch, info.ParamsType)
	if err != nil {
		handlerErr = &RpcError{
			Type:    "TypeError",
			Message: fmt.Sprintf("parameter deserialization: %v", err),
		}
		emptySchema := arrow.NewSchema(nil, nil)
		s.logIPCWriteErr("error-response", req.Method, writeErrorResponse(w, emptySchema, handlerErr, s.serverID, req.RequestID, s.debugErrors))
		return handlerErr, nil
	}

	// Build call context for init
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
		callCtx.LogLevel = LogTrace
	}

	// Call handler to get StreamResult
	results := info.Handler.Call([]reflect.Value{
		reflect.ValueOf(ctx),
		reflect.ValueOf(callCtx),
		params,
	})

	// Check for init error
	if !results[1].IsNil() {
		callErr := results[1].Interface().(error)

		// Write the error inside the expected output stream format (not a
		// standalone error stream) so the client can read it during the
		// normal streaming protocol.  Then drain the client's input stream
		// so the transport is clean for the next request.
		outputSchema := info.OutputSchema
		if outputSchema == nil {
			outputSchema = arrow.NewSchema(nil, nil)
		}
		s.logIPCWriteErr("error-response", req.Method, writeErrorResponse(w, outputSchema, callErr, s.serverID, req.RequestID, s.debugErrors))

		// Drain the client's input (ticks / exchange batches).
		// The client writes before reading, so this won't deadlock.
		drainInputStream(r)
		return callErr, nil
	}

	streamResult := results[0].Interface().(*StreamResult)
	outputSchema := streamResult.OutputSchema
	state := streamResult.State

	// Validate that State implements the expected interface.
	// For MethodDynamic, determine mode at runtime from the state type.
	var isProducer bool
	if info.Type == MethodDynamic {
		// Runtime dispatch: check which interface the state implements
		if _, ok := state.(ProducerState); ok {
			isProducer = true
		} else if _, ok := state.(ExchangeState); ok {
			isProducer = false
		} else {
			stateErr := &RpcError{
				Type:    "RuntimeError",
				Message: fmt.Sprintf("dynamic stream state %T does not implement ProducerState or ExchangeState", state),
			}
			s.logIPCWriteErr("error-response", req.Method, writeErrorResponse(w, outputSchema, stateErr, s.serverID, req.RequestID, s.debugErrors))
			drainInputStream(r)
			return stateErr, nil
		}
	} else {
		isProducer = info.Type == MethodProducer
		if isProducer {
			if _, ok := state.(ProducerState); !ok {
				stateErr := &RpcError{
					Type:    "RuntimeError",
					Message: fmt.Sprintf("stream state %T does not implement ProducerState", state),
				}
				s.logIPCWriteErr("error-response", req.Method, writeErrorResponse(w, outputSchema, stateErr, s.serverID, req.RequestID, s.debugErrors))
				drainInputStream(r)
				return stateErr, nil
			}
		} else {
			if _, ok := state.(ExchangeState); !ok {
				stateErr := &RpcError{
					Type:    "RuntimeError",
					Message: fmt.Sprintf("stream state %T does not implement ExchangeState", state),
				}
				s.logIPCWriteErr("error-response", req.Method, writeErrorResponse(w, outputSchema, stateErr, s.serverID, req.RequestID, s.debugErrors))
				drainInputStream(r)
				return stateErr, nil
			}
		}
	}

	// Write header IPC stream if method declares a header type
	if info.HasHeader && streamResult.Header != nil {
		slog.Debug("stream: writing header", "method", info.Name, "type", fmt.Sprintf("%T", streamResult.Header))
		if err := s.writeStreamHeader(w, streamResult.Header, callCtx.drainLogs()); err != nil {
			slog.Debug("stream: header write error", "method", info.Name, "err", err)
			return nil, nil // transport error during header, bail out
		}
		slog.Debug("stream: header written", "method", info.Name)
	}

	// Open input reader for client ticks/data
	slog.Debug("stream: opening input reader", "method", info.Name)
	inputReader, err := ipc.NewReader(r)
	if err != nil {
		slog.Debug("stream: input reader error", "method", info.Name, "err", err)
		return nil, nil // transport error
	}
	defer inputReader.Release()
	slog.Debug("stream: input reader opened", "method", info.Name)

	// Open output IPC writer
	outputWriter := ipc.NewWriter(w, ipc.WithSchema(outputSchema))
	slog.Debug("stream: output writer opened", "method", info.Name, "is_producer", isProducer)

	// Write any buffered init logs
	initLogs := callCtx.drainLogs()
	for _, logMsg := range initLogs {
		s.logIPCWriteErr("init-log-batch", req.Method, writeLogBatch(outputWriter, outputSchema, logMsg, s.serverID, req.RequestID))
	}

	// Determine input schema for casting (exchange methods only)
	var inputSchema *arrow.Schema
	if !isProducer {
		if info.InputSchema != nil {
			inputSchema = info.InputSchema
		} else if streamResult.InputSchema != nil {
			inputSchema = streamResult.InputSchema
		}
	}

	// Lockstep loop
	var streamErr error

	slog.Debug("stream: entering lockstep loop", "method", info.Name)
	for {
		// Read one input batch (tick for producer, real data for exchange)
		slog.Debug("stream: waiting for input batch", "method", info.Name)
		if !inputReader.Next() {
			// Client closed the stream (StopIteration equivalent)
			slog.Debug("stream: input reader done", "method", info.Name)
			break
		}
		inputBatch := inputReader.RecordBatch()
		slog.Debug("stream: got input batch", "method", info.Name, "rows", inputBatch.NumRows(), "cols", inputBatch.NumCols())

		// Client cancellation signal: a batch carrying vgi_rpc.cancel metadata
		// ends the stream without invoking Produce/Exchange. Invoke the
		// optional StreamCanceller hook before breaking so the state object
		// can release resources.
		if bwm, ok := inputBatch.(arrow.RecordBatchWithMetadata); ok {
			if _, cancelled := bwm.Metadata().GetValue(MetaCancel); cancelled {
				slog.Debug("stream: cancel received", "method", info.Name)
				if canceller, ok := state.(StreamCanceller); ok {
					cancelCtx := &CallContext{
						Ctx:               ctx,
						RequestID:         req.RequestID,
						ServerID:          s.serverID,
						Method:            req.Method,
						LogLevel:          LogLevel(req.LogLevel),
						Auth:              Anonymous(),
						TransportMetadata: req.Metadata,
					}
					if cancelCtx.LogLevel == "" {
						cancelCtx.LogLevel = LogTrace
					}
					func() {
						defer func() {
							if rv := recover(); rv != nil {
								slog.Debug("stream: OnCancel panic", "method", info.Name, "panic", rv)
							}
						}()
						if err := canceller.OnCancel(ctx, cancelCtx); err != nil {
							slog.Debug("stream: OnCancel error", "method", info.Name, "err", err)
						}
					}()
				}
				break
			}
		}

		// Resolve external input batches (exchange streams may receive pointer batches)
		if s.externalConfig != nil {
			var inputMeta arrow.Metadata
			if bwm, ok := inputBatch.(arrow.RecordBatchWithMetadata); ok {
				inputMeta = bwm.Metadata()
			}
			resolvedBatch, _, resolveErr := ResolveExternalLocation(inputBatch, inputMeta, s.externalConfig)
			if resolveErr != nil {
				slog.Error("failed to resolve external input", "err", resolveErr)
			} else {
				inputBatch = resolvedBatch
			}
		}

		// Cast compatible input types if schema doesn't match exactly
		if inputSchema != nil && !inputBatch.Schema().Equal(inputSchema) {
			castBatch, castErr := castRecordBatch(inputBatch, inputSchema)
			if castErr != nil {
				streamErr = castErr
				s.logIPCWriteErr("cast-error-batch", req.Method, writeErrorBatch(outputWriter, outputSchema, castErr, s.serverID, req.RequestID, s.debugErrors))
				break
			}
			defer castBatch.Release()
			inputBatch = castBatch
		}

		// Record input stats per streaming batch
		stats.RecordInput(inputBatch.NumRows(), batchBufferSize(inputBatch))

		// Create OutputCollector for this iteration
		out := newOutputCollector(outputSchema, s.serverID, isProducer)

		// Build per-iteration call context
		iterCtx := &CallContext{
			Ctx:               ctx,
			RequestID:         req.RequestID,
			ServerID:          s.serverID,
			Method:            req.Method,
			LogLevel:          LogLevel(req.LogLevel),
			Auth:              Anonymous(),
			TransportMetadata: req.Metadata,
		}
		if iterCtx.LogLevel == "" {
			iterCtx.LogLevel = LogTrace
		}
		if bwm, ok := inputBatch.(arrow.RecordBatchWithMetadata); ok {
			iterCtx.InputMetadata = bwm.Metadata()
		}

		// Dispatch to state
		func() {
			defer func() {
				if rv := recover(); rv != nil {
					streamErr = &RpcError{
						Type:    "RuntimeError",
						Message: fmt.Sprintf("%v", rv),
					}
				}
			}()
			if isProducer {
				if err := state.(ProducerState).Produce(ctx, out, iterCtx); err != nil {
					streamErr = err
				}
			} else {
				if err := state.(ExchangeState).Exchange(ctx, inputBatch, out, iterCtx); err != nil {
					streamErr = err
				}
			}
		}()

		if streamErr != nil {
			s.logIPCWriteErr("stream-error-batch", req.Method, writeErrorBatch(outputWriter, outputSchema, streamErr, s.serverID, req.RequestID, s.debugErrors))
			break
		}

		// Validate (unless finished)
		if !out.Finished() {
			if err := out.validate(); err != nil {
				streamErr = err
				s.logIPCWriteErr("validate-error-batch", req.Method, writeErrorBatch(outputWriter, outputSchema, err, s.serverID, req.RequestID, s.debugErrors))
				break
			}
		}

		// Flush all accumulated batches to output writer
		for i, ab := range out.batches {
			var writeErr error
			if ab.meta != nil {
				batchWithMeta := array.NewRecordBatchWithMetadata(
					outputSchema, ab.batch.Columns(), ab.batch.NumRows(), *ab.meta)
				writeErr = outputWriter.Write(batchWithMeta)
				batchWithMeta.Release()
				ab.batch.Release()
			} else {
				// Maybe externalize large data batches
				dataBatch := ab.batch
				if s.externalConfig != nil && dataBatch.NumRows() > 0 {
					extBatch, _, extErr := MaybeExternalizeBatch(dataBatch, arrow.Metadata{}, s.externalConfig)
					if extErr != nil {
						slog.Error("failed to externalize stream batch", "err", extErr)
					} else if extBatch != dataBatch {
						dataBatch.Release()
						dataBatch = extBatch
					}
				}
				// Data batch — record output stats
				stats.RecordOutput(dataBatch.NumRows(), batchBufferSize(dataBatch))
				writeErr = outputWriter.Write(dataBatch)
				dataBatch.Release()
			}
			if writeErr != nil {
				// Release remaining batches and break
				for _, remaining := range out.batches[i+1:] {
					remaining.batch.Release()
				}
				transportErr = fmt.Errorf("writing output batch: %w", writeErr)
				break
			}
		}

		if transportErr != nil {
			break
		}

		if out.Finished() {
			break
		}

	}

	// Close output writer (sends EOS)
	s.logIPCWriteErr("close", req.Method, outputWriter.Close())

	// Drain remaining input so transport is clean for next request
	for inputReader.Next() {
		// discard
	}

	return streamErr, transportErr
}

// writeStreamHeader writes a stream header as a separate complete IPC stream.
