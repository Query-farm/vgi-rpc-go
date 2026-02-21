package vgirpc

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

const (
	arrowContentType = "application/vnd.apache.arrow.stream"
	hmacLen          = 32
	defaultTokenTTL  = 5 * time.Minute
)

// RegisterStateType registers a state type for gob serialization.
// Must be called before using HTTP transport with stateful streams.
func RegisterStateType(v interface{}) {
	gob.Register(v)
}

// HttpServer serves RPC requests over HTTP.
type HttpServer struct {
	server     *Server
	signingKey []byte
	tokenTTL   time.Duration
	prefix     string
	mux        *http.ServeMux
}

// NewHttpServer creates a new HTTP server wrapping an RPC server.
func NewHttpServer(server *Server) *HttpServer {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	h := &HttpServer{
		server:     server,
		signingKey: key,
		tokenTTL:   defaultTokenTTL,
		prefix:     "/vgi",
	}
	h.mux = http.NewServeMux()
	h.mux.HandleFunc(fmt.Sprintf("POST %s/{method}/init", h.prefix), h.handleStreamInit)
	h.mux.HandleFunc(fmt.Sprintf("POST %s/{method}/exchange", h.prefix), h.handleStreamExchange)
	h.mux.HandleFunc(fmt.Sprintf("POST %s/{method}", h.prefix), h.handleUnary)
	return h
}

// NewHttpServerWithKey creates a new HTTP server with a caller-provided signing key.
// The key must be at least 16 bytes long.
func NewHttpServerWithKey(server *Server, signingKey []byte) *HttpServer {
	if len(signingKey) < 16 {
		panic("vgirpc: signing key must be at least 16 bytes")
	}
	h := &HttpServer{
		server:     server,
		signingKey: signingKey,
		tokenTTL:   defaultTokenTTL,
		prefix:     "/vgi",
	}
	h.mux = http.NewServeMux()
	h.mux.HandleFunc(fmt.Sprintf("POST %s/{method}/init", h.prefix), h.handleStreamInit)
	h.mux.HandleFunc(fmt.Sprintf("POST %s/{method}/exchange", h.prefix), h.handleStreamExchange)
	h.mux.HandleFunc(fmt.Sprintf("POST %s/{method}", h.prefix), h.handleUnary)
	return h
}

// SetTokenTTL sets the maximum age for state tokens.
func (h *HttpServer) SetTokenTTL(d time.Duration) {
	h.tokenTTL = d
}

// ServeHTTP implements http.Handler.
func (h *HttpServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
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

	body, err := io.ReadAll(r.Body)
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

	params, err := deserializeParams(req.Batch, info.ParamsType)
	if err != nil {
		h.writeHttpError(w, http.StatusBadRequest,
			&RpcError{Type: "TypeError", Message: fmt.Sprintf("parameter deserialization: %v", err)},
			info.ResultSchema)
		return
	}

	callCtx := &CallContext{
		Ctx:       r.Context(),
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
			reflect.ValueOf(r.Context()), reflect.ValueOf(callCtx), params,
		})
		if !results[0].IsNil() {
			callErr = results[0].Interface().(error)
		}
	} else {
		results := info.Handler.Call([]reflect.Value{
			reflect.ValueOf(r.Context()), reflect.ValueOf(callCtx), params,
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
		ipcW := ipc.NewWriter(&buf, ipc.WithSchema(info.ResultSchema))
		for _, logMsg := range logs {
			_ = writeLogBatch(ipcW, info.ResultSchema, logMsg, h.server.serverID, req.RequestID)
		}
		_ = writeErrorBatch(ipcW, info.ResultSchema, callErr, h.server.serverID, req.RequestID)
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
		h.writeHttpError(w, http.StatusInternalServerError,
			&RpcError{Type: "SerializationError", Message: err.Error()}, info.ResultSchema)
		return
	}
	defer resultBatch.Release()

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

	body, err := io.ReadAll(r.Body)
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

	params, err := deserializeParams(req.Batch, info.ParamsType)
	if err != nil {
		h.writeHttpError(w, http.StatusBadRequest,
			&RpcError{Type: "TypeError", Message: err.Error()}, nil)
		return
	}

	callCtx := &CallContext{
		Ctx:       r.Context(),
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
		reflect.ValueOf(r.Context()), reflect.ValueOf(callCtx), params,
	})

	if !results[1].IsNil() {
		callErr := results[1].Interface().(error)
		statusCode := http.StatusInternalServerError
		if _, ok := callErr.(*RpcError); ok {
			statusCode = http.StatusInternalServerError
		}
		h.writeHttpError(w, statusCode, callErr, nil)
		return
	}

	streamResult := results[0].Interface().(*StreamResult)
	outputSchema := streamResult.OutputSchema
	state := streamResult.State
	isProducer := info.Type == MethodProducer

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

		h.runProduceLoop(r.Context(), writer, outputSchema, state.(ProducerState), info)
		_ = writer.Close()
	} else {
		// Exchange init — return state token
		token, err := h.packStateToken(state)
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

	body, err := io.ReadAll(r.Body)
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

	isProducer := info.Type == MethodProducer
	outputSchema := info.OutputSchema

	if isProducer {
		h.handleProducerContinuation(r.Context(), w, outputSchema, tokenData.State.(ProducerState), info)
	} else {
		h.handleExchangeCall(r.Context(), w, inputBatch, outputSchema, tokenData.State.(ExchangeState), info)
	}
}

// handleProducerContinuation runs the produce loop for a continuation request.
func (h *HttpServer) handleProducerContinuation(ctx context.Context, w http.ResponseWriter, schema *arrow.Schema,
	state ProducerState, info *methodInfo) {

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	h.runProduceLoop(ctx, writer, schema, state, info)
	_ = writer.Close()
	h.writeArrow(w, http.StatusOK, buf.Bytes())
}

// handleExchangeCall processes one exchange and returns the result with updated token.
func (h *HttpServer) handleExchangeCall(ctx context.Context, w http.ResponseWriter, inputBatch arrow.RecordBatch,
	schema *arrow.Schema, state ExchangeState, info *methodInfo) {

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
			if r := recover(); r != nil {
				exchangeErr = &RpcError{Type: "RuntimeError", Message: fmt.Sprintf("%v", r)}
			}
		}()
		if err := state.Exchange(ctx, inputBatch, out, callCtx); err != nil {
			exchangeErr = err
		}
	}()

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))

	if exchangeErr != nil {
		_ = writeErrorBatch(writer, schema, exchangeErr, h.server.serverID, "")
		_ = writer.Close()
		h.writeArrow(w, http.StatusInternalServerError, buf.Bytes())
		return
	}

	if err := out.validate(); err != nil {
		_ = writeErrorBatch(writer, schema, err, h.server.serverID, "")
		_ = writer.Close()
		h.writeArrow(w, http.StatusInternalServerError, buf.Bytes())
		return
	}

	// Serialize updated state into new token
	newToken, err := h.packStateToken(state)
	if err != nil {
		_ = writeErrorBatch(writer, schema, err, h.server.serverID, "")
		_ = writer.Close()
		h.writeArrow(w, http.StatusInternalServerError, buf.Bytes())
		return
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
			// Data batch — merge state token
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
}

// runProduceLoop runs the producer state machine to completion.
func (h *HttpServer) runProduceLoop(ctx context.Context, writer *ipc.Writer, schema *arrow.Schema,
	state ProducerState, info *methodInfo) {

	for {
		if err := ctx.Err(); err != nil {
			return
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
				if r := recover(); r != nil {
					produceErr = &RpcError{Type: "RuntimeError", Message: fmt.Sprintf("%v", r)}
				}
			}()
			if err := state.Produce(ctx, out, callCtx); err != nil {
				produceErr = err
			}
		}()

		if produceErr != nil {
			_ = writeErrorBatch(writer, schema, produceErr, h.server.serverID, "")
			return
		}

		if !out.Finished() {
			if err := out.validate(); err != nil {
				_ = writeErrorBatch(writer, schema, err, h.server.serverID, "")
				return
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
				_ = writer.Write(ab.batch)
			}
			ab.batch.Release()
		}

		if out.Finished() {
			return
		}
	}
}

// handleDescribe handles the __describe__ introspection endpoint.
func (h *HttpServer) handleDescribe(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
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
}

func (h *HttpServer) packStateToken(state interface{}) ([]byte, error) {
	data := stateTokenData{
		CreatedAt: time.Now().Unix(),
		State:     state,
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

	return append(payloadBytes, sig...), nil
}

func (h *HttpServer) unpackStateToken(token []byte) (*stateTokenData, error) {
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

// --- Helpers ---

func (h *HttpServer) writeHttpError(w http.ResponseWriter, statusCode int, err error, schema *arrow.Schema) {
	if schema == nil {
		schema = arrow.NewSchema(nil, nil)
	}
	var buf bytes.Buffer
	_ = WriteErrorResponse(&buf, schema, err, h.server.serverID, "")
	h.writeArrow(w, statusCode, buf.Bytes())
}

func (h *HttpServer) writeArrow(w http.ResponseWriter, statusCode int, data []byte) {
	w.Header().Set("Content-Type", arrowContentType)
	w.WriteHeader(statusCode)
	_, _ = w.Write(data)
}
