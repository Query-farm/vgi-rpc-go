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
	"strings"
	"syscall"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

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
			s.logIPCWriteErr("error-response", "", writeErrorResponse(w, emptySchema, rpcErr, s.serverID, "", s.debugErrors))
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
		s.logIPCWriteErr("error-response", req.Method, writeErrorResponse(w, emptySchema, &RpcError{
			Type:    "AttributeError",
			Message: errMsg,
		}, s.serverID, req.RequestID, s.debugErrors))
		return nil
	}

	// Build dispatch info and stats for hooks
	dispatchInfo := DispatchInfo{
		Method:            req.Method,
		MethodType:        methodTypeString(info.Type),
		ServerID:          s.serverID,
		RequestID:         req.RequestID,
		TransportMetadata: req.Metadata,
		Auth:              Anonymous(),
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
		s.logIPCWriteErr("error-response", req.Method, writeErrorResponse(w, info.ResultSchema,
			fmt.Errorf("method type %d not yet implemented", info.Type),
			s.serverID, req.RequestID, s.debugErrors))
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
		if werr := writeLogBatch(headerWriter, headerSchema, logMsg, s.serverID, ""); werr != nil {
			s.logIPCWriteErr("log-batch", "", werr)
			return werr
		}
	}

	if werr := headerWriter.Write(headerBatch); werr != nil {
		s.logIPCWriteErr("header-batch", "", werr)
		return werr
	}
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
