// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

func serverDebugLog(format string, args ...interface{}) {
	f, err := os.OpenFile("/tmp/vgi-rpc-server.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, format+"\n", args...)
}

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
	methods  map[string]*methodInfo
	serverID string
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
func (s *Server) RunStdio() {
	s.Serve(os.Stdin, os.Stdout)
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
				log.Printf("vgirpc: serve loop error: %v", err)
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
			_ = WriteErrorResponse(w, emptySchema, rpcErr, s.serverID, "")
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
		_ = WriteErrorResponse(w, emptySchema, &RpcError{
			Type:    "AttributeError",
			Message: errMsg,
		}, s.serverID, req.RequestID)
		return nil
	}

	// Dispatch based on method type
	switch info.Type {
	case MethodUnary:
		return s.serveUnary(ctx, w, req, info)
	case MethodProducer, MethodExchange, MethodDynamic:
		return s.serveStream(ctx, r, w, req, info)
	default:
		_ = WriteErrorResponse(w, info.ResultSchema,
			fmt.Errorf("method type %d not yet implemented", info.Type),
			s.serverID, req.RequestID)
		return nil
	}
}

// serveUnary dispatches a unary method call.
func (s *Server) serveUnary(ctx context.Context, w io.Writer, req *Request, info *methodInfo) error {
	// Deserialize parameters
	params, err := deserializeParams(req.Batch, info.ParamsType)
	if err != nil {
		_ = WriteErrorResponse(w, info.ResultSchema,
			&RpcError{Type: "TypeError", Message: fmt.Sprintf("parameter deserialization: %v", err)},
			s.serverID, req.RequestID)
		return nil
	}

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
				log.Printf("vgirpc: failed to write log batch: %v", err)
			}
		}
		if err := writeErrorBatch(ipcW, info.ResultSchema, callErr, s.serverID, req.RequestID); err != nil {
			log.Printf("vgirpc: failed to write error batch: %v", err)
		}
		if err := ipcW.Close(); err != nil {
			log.Printf("vgirpc: failed to close IPC writer: %v", err)
		}
		return nil
	}

	// Handle void result
	if info.ResultType == nil {
		return WriteVoidResponse(w, logs, s.serverID, req.RequestID)
	}

	// Serialize result
	resultBatch, err := serializeResult(info.ResultSchema, resultVal.Interface())
	if err != nil {
		_ = WriteErrorResponse(w, info.ResultSchema,
			&RpcError{Type: "SerializationError", Message: fmt.Sprintf("result serialization: %v", err)},
			s.serverID, req.RequestID)
		return nil
	}
	defer resultBatch.Release()

	return WriteUnaryResponse(w, info.ResultSchema, logs, resultBatch, s.serverID, req.RequestID)
}

// serveStream dispatches a producer or exchange stream method.
func (s *Server) serveStream(ctx context.Context, r io.Reader, w io.Writer, req *Request, info *methodInfo) error {
	// Deserialize parameters
	params, err := deserializeParams(req.Batch, info.ParamsType)
	if err != nil {
		emptySchema := arrow.NewSchema(nil, nil)
		_ = WriteErrorResponse(w, emptySchema, &RpcError{
			Type:    "TypeError",
			Message: fmt.Sprintf("parameter deserialization: %v", err),
		}, s.serverID, req.RequestID)
		return nil
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
		if err := writeErrorBatch(outputWriter, outputSchema, callErr, s.serverID, req.RequestID); err != nil {
			log.Printf("vgirpc: failed to write error batch: %v", err)
		}
		if err := outputWriter.Close(); err != nil {
			log.Printf("vgirpc: failed to close output writer: %v", err)
		}

		// Drain the client's input (ticks / exchange batches).
		// The client writes before reading, so this won't deadlock.
		if inputReader, err := ipc.NewReader(r); err == nil {
			for inputReader.Next() {
				// discard
			}
			inputReader.Release()
		}
		return nil
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
			_ = writeErrorBatch(outputWriter, outputSchema, stateErr, s.serverID, req.RequestID)
			_ = outputWriter.Close()
			if inputReader, err := ipc.NewReader(r); err == nil {
				for inputReader.Next() {
				}
				inputReader.Release()
			}
			return nil
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
				_ = writeErrorBatch(outputWriter, outputSchema, stateErr, s.serverID, req.RequestID)
				_ = outputWriter.Close()
				if inputReader, err := ipc.NewReader(r); err == nil {
					for inputReader.Next() {
					}
					inputReader.Release()
				}
				return nil
			}
		} else {
			if _, ok := state.(ExchangeState); !ok {
				stateErr := &RpcError{
					Type:    "RuntimeError",
					Message: fmt.Sprintf("stream state %T does not implement ExchangeState", state),
				}
				outputWriter := ipc.NewWriter(w, ipc.WithSchema(outputSchema))
				_ = writeErrorBatch(outputWriter, outputSchema, stateErr, s.serverID, req.RequestID)
				_ = outputWriter.Close()
				if inputReader, err := ipc.NewReader(r); err == nil {
					for inputReader.Next() {
					}
					inputReader.Release()
				}
				return nil
			}
		}
	}

	// Write header IPC stream if method declares a header type
	if info.HasHeader && streamResult.Header != nil {
		serverDebugLog("serveStream[%s]: writing header (type=%T)", info.Name, streamResult.Header)
		if err := s.writeStreamHeader(w, streamResult.Header, callCtx.drainLogs()); err != nil {
			serverDebugLog("serveStream[%s]: header write error: %v", info.Name, err)
			return nil // transport error during header, bail out
		}
		serverDebugLog("serveStream[%s]: header written ok", info.Name)
	}

	// Open input reader for client ticks/data
	serverDebugLog("serveStream[%s]: opening input reader", info.Name)
	inputReader, err := ipc.NewReader(r)
	if err != nil {
		serverDebugLog("serveStream[%s]: input reader error: %v", info.Name, err)
		return nil // transport error
	}
	defer inputReader.Release()
	serverDebugLog("serveStream[%s]: input reader opened", info.Name)

	// Open output IPC writer
	outputWriter := ipc.NewWriter(w, ipc.WithSchema(outputSchema))
	serverDebugLog("serveStream[%s]: output writer opened, isProducer=%v", info.Name, isProducer)

	// Write any buffered init logs
	initLogs := callCtx.drainLogs()
	for _, logMsg := range initLogs {
		if err := writeLogBatch(outputWriter, outputSchema, logMsg, s.serverID, req.RequestID); err != nil {
			log.Printf("vgirpc: failed to write init log batch: %v", err)
		}
	}

	// Lockstep loop
	var streamErr error

	serverDebugLog("serveStream[%s]: entering lockstep loop", info.Name)
	for {
		// Read one input batch (tick for producer, real data for exchange)
		serverDebugLog("serveStream[%s]: waiting for input batch", info.Name)
		if !inputReader.Next() {
			// Client closed the stream (StopIteration equivalent)
			serverDebugLog("serveStream[%s]: input reader done (client closed)", info.Name)
			break
		}
		inputBatch := inputReader.RecordBatch()
		serverDebugLog("serveStream[%s]: got input batch rows=%d cols=%d", info.Name, inputBatch.NumRows(), inputBatch.NumCols())

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
				if r := recover(); r != nil {
					streamErr = &RpcError{
						Type:    "RuntimeError",
						Message: fmt.Sprintf("%v", r),
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
			if err := writeErrorBatch(outputWriter, outputSchema, streamErr, s.serverID, req.RequestID); err != nil {
				log.Printf("vgirpc: failed to write stream error batch: %v", err)
			}
			break
		}

		// Validate (unless finished)
		if !out.Finished() {
			if err := out.validate(); err != nil {
				if writeErr := writeErrorBatch(outputWriter, outputSchema, err, s.serverID, req.RequestID); writeErr != nil {
					log.Printf("vgirpc: failed to write validation error batch: %v", writeErr)
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
				writeErr = outputWriter.Write(ab.batch)
			}
			ab.batch.Release()
			if writeErr != nil {
				// Release remaining batches and break
				for _, remaining := range out.batches[i+1:] {
					remaining.batch.Release()
				}
				streamErr = fmt.Errorf("writing output batch: %w", writeErr)
				break
			}
		}

		if out.Finished() {
			break
		}

	}

	// Close output writer (sends EOS)
	if err := outputWriter.Close(); err != nil {
		log.Printf("vgirpc: failed to close output writer: %v", err)
	}

	// Drain remaining input so transport is clean for next request
	for inputReader.Next() {
		// discard
	}

	return nil
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

