// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"reflect"
	"sort"
	"strings"
	"syscall"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// MethodType identifies how a registered method should be dispatched.
type MethodType int

const (
	// MethodUnary identifies a request-response method with a single result.
	MethodUnary MethodType = iota
	// MethodProducer identifies a server-driven streaming method.
	MethodProducer
	// MethodExchange identifies a bidirectional streaming method.
	MethodExchange
	// MethodDynamic identifies a stream method where the state type (ProducerState
	// or ExchangeState) is determined at runtime by the handler's return value.
	MethodDynamic
)

// methodInfo stores the registration details for one RPC method.
type methodInfo struct {
	Name          string
	Type          MethodType
	ParamsType    reflect.Type      // Go struct type for parameters
	ResultType    reflect.Type      // Go type for result (nil for void)
	ParamsSchema  *arrow.Schema     // Arrow schema for parameter deserialization
	ResultSchema  *arrow.Schema     // Arrow schema for result serialization
	Handler       reflect.Value     // func(context.Context, *CallContext, P) (R, error) or similar
	ParamDefaults map[string]string // parameter defaults from struct tags
	OutputSchema  *arrow.Schema     // for streaming methods: output batch schema
	InputSchema   *arrow.Schema     // for exchange methods: input batch schema (nil for producer)
	HasHeader     bool              // whether the method returns a stream with header
	HeaderSchema  *arrow.Schema     // Arrow schema for header type (if HasHeader)
}

// Server is the RPC server that dispatches incoming requests to registered methods.
type Server struct {
	methods      map[string]*methodInfo
	serverID     string
	serviceName  string
	dispatchHook DispatchHook
	debugErrors  bool
}

// NewServer creates a new RPC server.
func NewServer() *Server {
	return &Server{
		methods: make(map[string]*methodInfo),
	}
}

// SetServerID sets a server identifier included in response metadata.
func (s *Server) SetServerID(id string) {
	s.serverID = id
}

// SetServiceName sets a logical service name used by observability hooks.
func (s *Server) SetServiceName(name string) {
	s.serviceName = name
}

// ServiceName returns the logical service name, or empty string if not set.
func (s *Server) ServiceName() string {
	return s.serviceName
}

// SetDispatchHook registers a hook that is called around each RPC dispatch.
func (s *Server) SetDispatchHook(hook DispatchHook) {
	s.dispatchHook = hook
}

// SetDebugErrors controls whether error responses include full stack traces
// with file paths and function names. When false (the default), error responses
// contain only the error type and message. Enable this for development or
// internal services; disable it for public-facing deployments to avoid leaking
// implementation details.
func (s *Server) SetDebugErrors(enabled bool) {
	s.debugErrors = enabled
}

// Unary registers a unary RPC method with typed parameters and return value.
// P must be a struct with `vgirpc` tags. R is the return type.
func Unary[P any, R any](s *Server, name string, handler func(context.Context, *CallContext, P) (R, error)) {
	var p P
	var r R
	paramsType := reflect.TypeOf(p)
	resultType := reflect.TypeOf(r)

	paramsSchema, err := structToSchema(paramsType)
	if err != nil {
		panic(fmt.Sprintf("vgirpc: registering %q: invalid params type %T: %v", name, p, err))
	}

	resultSchema, err := resultSchema(resultType)
	if err != nil {
		panic(fmt.Sprintf("vgirpc: registering %q: invalid result type %T: %v", name, r, err))
	}

	s.methods[name] = &methodInfo{
		Name:          name,
		Type:          MethodUnary,
		ParamsType:    paramsType,
		ResultType:    resultType,
		ParamsSchema:  paramsSchema,
		ResultSchema:  resultSchema,
		Handler:       reflect.ValueOf(handler),
		ParamDefaults: extractDefaults(paramsType),
	}
}

// UnaryVoid registers a unary RPC method that returns no value.
func UnaryVoid[P any](s *Server, name string, handler func(context.Context, *CallContext, P) error) {
	var p P
	paramsType := reflect.TypeOf(p)

	paramsSchema, err := structToSchema(paramsType)
	if err != nil {
		panic(fmt.Sprintf("vgirpc: registering %q: invalid params type %T: %v", name, p, err))
	}

	resultSchema := arrow.NewSchema(nil, nil) // empty schema for void

	s.methods[name] = &methodInfo{
		Name:          name,
		Type:          MethodUnary,
		ParamsType:    paramsType,
		ResultType:    nil,
		ParamsSchema:  paramsSchema,
		ResultSchema:  resultSchema,
		Handler:       reflect.ValueOf(handler),
		ParamDefaults: extractDefaults(paramsType),
	}
}

// Producer registers a producer stream method.
// The handler returns a StreamResult containing the ProducerState.
func Producer[P any](s *Server, name string, outputSchema *arrow.Schema,
	handler func(context.Context, *CallContext, P) (*StreamResult, error)) {
	if outputSchema == nil {
		panic(fmt.Sprintf("vgirpc: registering %q: outputSchema must not be nil", name))
	}
	var p P
	paramsType := reflect.TypeOf(p)
	paramsSchema, err := structToSchema(paramsType)
	if err != nil {
		panic(fmt.Sprintf("vgirpc: registering %q: invalid params type %T: %v", name, p, err))
	}

	s.methods[name] = &methodInfo{
		Name:          name,
		Type:          MethodProducer,
		ParamsType:    paramsType,
		ParamsSchema:  paramsSchema,
		ResultSchema:  arrow.NewSchema(nil, nil), // empty for streams
		Handler:       reflect.ValueOf(handler),
		ParamDefaults: extractDefaults(paramsType),
		OutputSchema:  outputSchema,
	}
}

// ProducerWithHeader registers a producer stream method that returns a header.
func ProducerWithHeader[P any](s *Server, name string, outputSchema *arrow.Schema,
	headerSchema *arrow.Schema, handler func(context.Context, *CallContext, P) (*StreamResult, error)) {
	if outputSchema == nil {
		panic(fmt.Sprintf("vgirpc: registering %q: outputSchema must not be nil", name))
	}
	var p P
	paramsType := reflect.TypeOf(p)
	paramsSchema, err := structToSchema(paramsType)
	if err != nil {
		panic(fmt.Sprintf("vgirpc: registering %q: invalid params type %T: %v", name, p, err))
	}

	s.methods[name] = &methodInfo{
		Name:          name,
		Type:          MethodProducer,
		ParamsType:    paramsType,
		ParamsSchema:  paramsSchema,
		ResultSchema:  arrow.NewSchema(nil, nil),
		Handler:       reflect.ValueOf(handler),
		ParamDefaults: extractDefaults(paramsType),
		OutputSchema:  outputSchema,
		HasHeader:     true,
		HeaderSchema:  headerSchema,
	}
}

// Exchange registers an exchange stream method.
func Exchange[P any](s *Server, name string, outputSchema, inputSchema *arrow.Schema,
	handler func(context.Context, *CallContext, P) (*StreamResult, error)) {
	if outputSchema == nil {
		panic(fmt.Sprintf("vgirpc: registering %q: outputSchema must not be nil", name))
	}
	if inputSchema == nil {
		panic(fmt.Sprintf("vgirpc: registering %q: inputSchema must not be nil", name))
	}
	var p P
	paramsType := reflect.TypeOf(p)
	paramsSchema, err := structToSchema(paramsType)
	if err != nil {
		panic(fmt.Sprintf("vgirpc: registering %q: invalid params type %T: %v", name, p, err))
	}

	s.methods[name] = &methodInfo{
		Name:          name,
		Type:          MethodExchange,
		ParamsType:    paramsType,
		ParamsSchema:  paramsSchema,
		ResultSchema:  arrow.NewSchema(nil, nil),
		Handler:       reflect.ValueOf(handler),
		ParamDefaults: extractDefaults(paramsType),
		OutputSchema:  outputSchema,
		InputSchema:   inputSchema,
	}
}

// ExchangeWithHeader registers an exchange stream method that returns a header.
func ExchangeWithHeader[P any](s *Server, name string, outputSchema, inputSchema *arrow.Schema,
	headerSchema *arrow.Schema, handler func(context.Context, *CallContext, P) (*StreamResult, error)) {
	if outputSchema == nil {
		panic(fmt.Sprintf("vgirpc: registering %q: outputSchema must not be nil", name))
	}
	if inputSchema == nil {
		panic(fmt.Sprintf("vgirpc: registering %q: inputSchema must not be nil", name))
	}
	var p P
	paramsType := reflect.TypeOf(p)
	paramsSchema, err := structToSchema(paramsType)
	if err != nil {
		panic(fmt.Sprintf("vgirpc: registering %q: invalid params type %T: %v", name, p, err))
	}

	s.methods[name] = &methodInfo{
		Name:          name,
		Type:          MethodExchange,
		ParamsType:    paramsType,
		ParamsSchema:  paramsSchema,
		ResultSchema:  arrow.NewSchema(nil, nil),
		Handler:       reflect.ValueOf(handler),
		ParamDefaults: extractDefaults(paramsType),
		OutputSchema:  outputSchema,
		InputSchema:   inputSchema,
		HasHeader:     true,
		HeaderSchema:  headerSchema,
	}
}

// DynamicStreamWithHeader registers a stream method where the state type
// (ProducerState or ExchangeState) is determined at runtime based on the
// StreamResult returned by the handler. The handler must return a StreamResult
// whose State field implements either ProducerState or ExchangeState.
// OutputSchema and InputSchema are taken from the StreamResult at runtime.
func DynamicStreamWithHeader[P any](s *Server, name string,
	headerSchema *arrow.Schema, handler func(context.Context, *CallContext, P) (*StreamResult, error)) {
	var p P
	paramsType := reflect.TypeOf(p)
	paramsSchema, err := structToSchema(paramsType)
	if err != nil {
		panic(fmt.Sprintf("vgirpc: registering %q: invalid params type %T: %v", name, p, err))
	}

	s.methods[name] = &methodInfo{
		Name:         name,
		Type:         MethodDynamic,
		ParamsType:   paramsType,
		ParamsSchema: paramsSchema,
		ResultSchema: arrow.NewSchema(nil, nil),
		Handler:      reflect.ValueOf(handler),
		ParamDefaults: extractDefaults(paramsType),
		HasHeader:    true,
		HeaderSchema: headerSchema,
	}
}

// RunStdio runs the server loop reading from stdin and writing to stdout.
// If stdin or stdout is connected to a terminal, a warning is printed to
// stderr (matching the Python vgi-rpc behaviour).
func (s *Server) RunStdio() {
	// Ignore SIGPIPE so writes to closed pipes (stderr logging, stdout IPC)
	// return errors instead of killing the process. Transport errors are
	// already handled by isTransportClosed() in the serve loop.
	signal.Ignore(syscall.SIGPIPE)

	if isTerminal(os.Stdin) || isTerminal(os.Stdout) {
		fmt.Fprintln(os.Stderr,
			"WARNING: This process communicates via Arrow IPC on stdin/stdout "+
				"and is not intended to be run interactively.\n"+
				"It should be launched as a subprocess by an RPC client "+
				"(e.g. vgi_rpc.connect()).")
	}
	s.Serve(os.Stdin, os.Stdout)
}

// isTerminal reports whether f is connected to a terminal.
func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

// Serve runs the server loop on the given reader/writer pair.
func (s *Server) Serve(r io.Reader, w io.Writer) {
	s.ServeWithContext(context.Background(), r, w)
}

// ServeWithContext runs the server loop on the given reader/writer pair with a context.
func (s *Server) ServeWithContext(ctx context.Context, r io.Reader, w io.Writer) {
	for {
		err := s.serveOne(ctx, r, w)
		if err != nil {
			if err == io.EOF {
				return
			}
			// Only log unexpected errors (not broken pipe / connection reset)
			if !isTransportClosed(err) {
				slog.Error("serve loop error", "err", err)
			}
			return
		}
	}
}

// serveOne handles one complete RPC request-response cycle.
func (s *Server) serveOne(ctx context.Context, r io.Reader, w io.Writer) error {
	req, err := ReadRequest(r)
	if err != nil {
		if err == io.EOF {
			return io.EOF
		}
		// Try to write error response
		if rpcErr, ok := err.(*RpcError); ok {
			emptySchema := arrow.NewSchema(nil, nil)
			_ = writeErrorResponse(w, emptySchema, rpcErr, s.serverID, "", s.debugErrors)
			return nil // continue serving
		}
		return err // transport error, stop serving
	}
	defer req.Batch.Release()

	// Handle __describe__ introspection
	if req.Method == "__describe__" {
		return s.serveDescribe(w, req)
	}

	// Look up method
	info, ok := s.methods[req.Method]
	if !ok {
		available := s.availableMethods()
		errMsg := fmt.Sprintf("Unknown method: '%s'. Available methods: %v", req.Method, available)
		emptySchema := arrow.NewSchema(nil, nil)
		_ = writeErrorResponse(w, emptySchema, &RpcError{
			Type:    "AttributeError",
			Message: errMsg,
		}, s.serverID, req.RequestID, s.debugErrors)
		return nil
	}

	// Build dispatch info and stats for hooks
	dispatchInfo := DispatchInfo{
		Method:            req.Method,
		MethodType:        methodTypeString(info.Type),
		ServerID:          s.serverID,
		RequestID:         req.RequestID,
		TransportMetadata: req.Metadata,
	}

	var hookToken HookToken
	var hookActive bool
	stats := &CallStatistics{}

	if s.dispatchHook != nil {
		func() {
			defer func() {
				if rv := recover(); rv != nil {
					slog.Error("dispatch hook start panic", "err", rv)
				}
			}()
			var hookCtx context.Context
			hookCtx, hookToken = s.dispatchHook.OnDispatchStart(ctx, dispatchInfo)
			if hookCtx != nil {
				ctx = hookCtx
			}
			hookActive = true
		}()
	}

	// Dispatch based on method type
	var handlerErr error
	var transportErr error
	switch info.Type {
	case MethodUnary:
		handlerErr, transportErr = s.serveUnary(ctx, w, req, info, stats)
	case MethodProducer, MethodExchange, MethodDynamic:
		handlerErr, transportErr = s.serveStream(ctx, r, w, req, info, stats)
	default:
		_ = writeErrorResponse(w, info.ResultSchema,
			fmt.Errorf("method type %d not yet implemented", info.Type),
			s.serverID, req.RequestID, s.debugErrors)
	}

	// Hook end (panic-safe)
	if hookActive {
		func() {
			defer func() {
				if rv := recover(); rv != nil {
					slog.Error("dispatch hook end panic", "err", rv)
				}
			}()
			s.dispatchHook.OnDispatchEnd(ctx, hookToken, dispatchInfo, stats, handlerErr)
		}()
	}

	return transportErr
}

// serveUnary dispatches a unary method call.
// Returns handlerErr (application error reported to hook) and transportErr (I/O error for serve loop).
func (s *Server) serveUnary(ctx context.Context, w io.Writer, req *Request, info *methodInfo, stats *CallStatistics) (handlerErr, transportErr error) {
	// Deserialize parameters
	params, err := deserializeParams(req.Batch, info.ParamsType)
	if err != nil {
		handlerErr = &RpcError{Type: "TypeError", Message: fmt.Sprintf("parameter deserialization: %v", err)}
		_ = writeErrorResponse(w, info.ResultSchema, handlerErr, s.serverID, req.RequestID, s.debugErrors)
		return handlerErr, nil
	}

	// Record input stats
	stats.RecordInput(req.Batch.NumRows(), batchBufferSize(req.Batch))

	// Build call context
	callCtx := &CallContext{
		Ctx:       ctx,
		RequestID: req.RequestID,
		ServerID:  s.serverID,
		Method:    req.Method,
		LogLevel:  LogLevel(req.LogLevel),
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
			if err := writeLogBatch(ipcW, info.ResultSchema, logMsg, s.serverID, req.RequestID); err != nil {
				slog.Error("failed to write log batch", "err", err)
			}
		}
		if err := writeErrorBatch(ipcW, info.ResultSchema, callErr, s.serverID, req.RequestID, s.debugErrors); err != nil {
			slog.Error("failed to write error batch", "err", err)
		}
		if err := ipcW.Close(); err != nil {
			slog.Error("failed to close IPC writer", "err", err)
		}
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
		_ = writeErrorResponse(w, info.ResultSchema, handlerErr, s.serverID, req.RequestID, s.debugErrors)
		return handlerErr, nil
	}
	defer resultBatch.Release()

	// Record output stats
	stats.RecordOutput(resultBatch.NumRows(), batchBufferSize(resultBatch))

	return nil, WriteUnaryResponse(w, info.ResultSchema, logs, resultBatch, s.serverID, req.RequestID)
}

// serveStream dispatches a producer or exchange stream method.
// Returns handlerErr (application error reported to hook) and transportErr (I/O error for serve loop).
func (s *Server) serveStream(ctx context.Context, r io.Reader, w io.Writer, req *Request, info *methodInfo, stats *CallStatistics) (handlerErr, transportErr error) {
	// Deserialize parameters
	params, err := deserializeParams(req.Batch, info.ParamsType)
	if err != nil {
		handlerErr = &RpcError{
			Type:    "TypeError",
			Message: fmt.Sprintf("parameter deserialization: %v", err),
		}
		emptySchema := arrow.NewSchema(nil, nil)
		_ = writeErrorResponse(w, emptySchema, handlerErr, s.serverID, req.RequestID, s.debugErrors)
		return handlerErr, nil
	}

	// Build call context for init
	callCtx := &CallContext{
		Ctx:       ctx,
		RequestID: req.RequestID,
		ServerID:  s.serverID,
		Method:    req.Method,
		LogLevel:  LogLevel(req.LogLevel),
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
		outputWriter := ipc.NewWriter(w, ipc.WithSchema(outputSchema))
		if err := writeErrorBatch(outputWriter, outputSchema, callErr, s.serverID, req.RequestID, s.debugErrors); err != nil {
			slog.Error("failed to write error batch", "err", err)
		}
		if err := outputWriter.Close(); err != nil {
			slog.Error("failed to close output writer", "err", err)
		}

		// Drain the client's input (ticks / exchange batches).
		// The client writes before reading, so this won't deadlock.
		if inputReader, err := ipc.NewReader(r); err == nil {
			for inputReader.Next() {
				// discard
			}
			inputReader.Release()
		}
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
			outputWriter := ipc.NewWriter(w, ipc.WithSchema(outputSchema))
			_ = writeErrorBatch(outputWriter, outputSchema, stateErr, s.serverID, req.RequestID, s.debugErrors)
			_ = outputWriter.Close()
			if inputReader, err := ipc.NewReader(r); err == nil {
				for inputReader.Next() {
				}
				inputReader.Release()
			}
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
				outputWriter := ipc.NewWriter(w, ipc.WithSchema(outputSchema))
				_ = writeErrorBatch(outputWriter, outputSchema, stateErr, s.serverID, req.RequestID, s.debugErrors)
				_ = outputWriter.Close()
				if inputReader, err := ipc.NewReader(r); err == nil {
					for inputReader.Next() {
					}
					inputReader.Release()
				}
				return stateErr, nil
			}
		} else {
			if _, ok := state.(ExchangeState); !ok {
				stateErr := &RpcError{
					Type:    "RuntimeError",
					Message: fmt.Sprintf("stream state %T does not implement ExchangeState", state),
				}
				outputWriter := ipc.NewWriter(w, ipc.WithSchema(outputSchema))
				_ = writeErrorBatch(outputWriter, outputSchema, stateErr, s.serverID, req.RequestID, s.debugErrors)
				_ = outputWriter.Close()
				if inputReader, err := ipc.NewReader(r); err == nil {
					for inputReader.Next() {
					}
					inputReader.Release()
				}
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
		if err := writeLogBatch(outputWriter, outputSchema, logMsg, s.serverID, req.RequestID); err != nil {
			slog.Error("failed to write init log batch", "err", err)
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

		// Record input stats per streaming batch
		stats.RecordInput(inputBatch.NumRows(), batchBufferSize(inputBatch))

		// Create OutputCollector for this iteration
		out := newOutputCollector(outputSchema, s.serverID, isProducer)

		// Build per-iteration call context
		iterCtx := &CallContext{
			Ctx:       ctx,
			RequestID: req.RequestID,
			ServerID:  s.serverID,
			Method:    req.Method,
			LogLevel:  LogLevel(req.LogLevel),
		}
		if iterCtx.LogLevel == "" {
			iterCtx.LogLevel = LogTrace
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
			// Write error batch to output stream
			if err := writeErrorBatch(outputWriter, outputSchema, streamErr, s.serverID, req.RequestID, s.debugErrors); err != nil {
				slog.Error("failed to write stream error batch", "err", err)
			}
			break
		}

		// Validate (unless finished)
		if !out.Finished() {
			if err := out.validate(); err != nil {
				streamErr = err
				if writeErr := writeErrorBatch(outputWriter, outputSchema, err, s.serverID, req.RequestID, s.debugErrors); writeErr != nil {
					slog.Error("failed to write validation error batch", "err", writeErr)
				}
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
			} else {
				// Data batch — record output stats
				stats.RecordOutput(ab.batch.NumRows(), batchBufferSize(ab.batch))
				writeErr = outputWriter.Write(ab.batch)
			}
			ab.batch.Release()
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
	if err := outputWriter.Close(); err != nil {
		slog.Error("failed to close output writer", "err", err)
	}

	// Drain remaining input so transport is clean for next request
	for inputReader.Next() {
		// discard
	}

	return streamErr, transportErr
}

// writeStreamHeader writes a stream header as a separate complete IPC stream.
func (s *Server) writeStreamHeader(w io.Writer, header ArrowSerializable, logs []LogMessage) error {
	// Serialize header to a 1-row batch using IPC bytes
	data, err := serializeArrowSerializable(header)
	if err != nil {
		return err
	}

	// Read the batch back to get the header schema and batch
	headerReader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer headerReader.Release()

	if !headerReader.Next() {
		return fmt.Errorf("no batch in header IPC")
	}
	headerBatch := headerReader.RecordBatch()
	headerSchema := headerBatch.Schema()

	// Write header as a complete IPC stream (schema + optional logs + header batch + EOS)
	headerWriter := ipc.NewWriter(w, ipc.WithSchema(headerSchema))

	// Write any buffered logs into header stream
	for _, logMsg := range logs {
		_ = writeLogBatch(headerWriter, headerSchema, logMsg, s.serverID, "")
	}

	_ = headerWriter.Write(headerBatch)
	return headerWriter.Close()
}

// serveDescribe handles the __describe__ introspection request.
func (s *Server) serveDescribe(w io.Writer, req *Request) error {
	batch, meta := s.buildDescribeBatch()
	defer batch.Release()

	batchWithMeta := array.NewRecordBatchWithMetadata(
		describeSchema, batch.Columns(), batch.NumRows(), meta)
	defer batchWithMeta.Release()

	writer := ipc.NewWriter(w, ipc.WithSchema(describeSchema))
	defer writer.Close()

	return writer.Write(batchWithMeta)
}

// extractDefaults extracts default values from struct vgirpc tags.
func extractDefaults(t reflect.Type) map[string]string {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}
	defaults := make(map[string]string)
	for i := range t.NumField() {
		f := t.Field(i)
		tag := f.Tag.Get("vgirpc")
		if tag == "" || tag == "-" {
			continue
		}
		info := parseTag(tag)
		if info.Default != nil {
			defaults[info.Name] = *info.Default
		}
	}
	if len(defaults) == 0 {
		return nil
	}
	return defaults
}

// isTransportClosed returns true for errors that indicate the transport was closed normally.
func isTransportClosed(err error) bool {
	if err == io.EOF {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "EOF")
}

func (s *Server) availableMethods() []string {
	names := make([]string, 0, len(s.methods))
	for name := range s.methods {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

