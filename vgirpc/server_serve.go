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
	"strconv"
	"strings"
	"syscall"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// shmConnState caches the shared-memory segment for one pipe/stdio connection.
// The client advertises (segment_name, segment_size) once on init requests and
// then references the segment by offset on later data requests (e.g.
// table_buffering_process), so the worker attaches the segment the first time it
// is advertised and reuses it for the rest of the connection, closing it when
// the serve loop ends. A no-op on connections that never use shm.
type shmConnState struct {
	seg  *ShmSegment
	name string
	size int
}

// ensure attaches the segment advertised in reqMeta (if any) or returns the
// already-attached segment. Returns nil when no segment has been advertised on
// this connection yet (the caller treats a pointer batch with no segment as a
// negotiation mismatch).
func (c *shmConnState) ensure(reqMeta map[string]string) *ShmSegment {
	name, hasName := reqMeta[MetaShmSegmentName]
	sizeStr, hasSize := reqMeta[MetaShmSegmentSize]
	if !hasName || !hasSize {
		return c.seg
	}
	size, err := strconv.Atoi(sizeStr)
	if err != nil || size <= ShmHeaderSize {
		return c.seg
	}
	if c.seg != nil && c.name == name && c.size == size {
		return c.seg
	}
	// A new or changed segment was advertised — (re)attach and cache it.
	c.close()
	seg, err := ShmAttach(name, size, false /* track */)
	if err != nil {
		return nil
	}
	c.seg, c.name, c.size = seg, name, size
	return c.seg
}

func (c *shmConnState) close() {
	if c.seg != nil {
		_ = c.seg.Close()
		c.seg = nil
	}
}

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
// Returns when the context is cancelled, the transport returns io.EOF, or a fatal
// transport error occurs. A blocking read on r cannot be interrupted by ctx alone —
// callers wanting prompt shutdown should also close r (or its underlying file
// descriptor) when cancelling the context.
func (s *Server) ServeWithContext(ctx context.Context, r io.Reader, w io.Writer) {
	if err := s.notifyTransport(TransportKindPipe, nil); err != nil {
		// Hook refused the binding; abort the serve. The error has
		// already been logged inside notifyTransport.
		return
	}
	// The shared-memory segment is advertised once (on init requests) and then
	// referenced by offset on later data requests, so it is attached once and
	// cached for the lifetime of this connection rather than per request.
	shmConn := &shmConnState{}
	defer shmConn.close()
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		err := s.serveOne(ctx, r, w, shmConn)
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
func (s *Server) serveOne(ctx context.Context, r io.Reader, w io.Writer, shmConn *shmConnState) error {
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

	// Attach (or reuse) the shared-memory segment for this connection. The
	// client advertises (segment_name, segment_size) once on init requests and
	// then references the segment by offset on subsequent data requests, so the
	// segment is cached on shmConn across calls rather than attached per request.
	// Resolve the request batch through it when it is a pointer batch carrying
	// the real parameters in the segment.
	if seg := shmConn.ensure(req.Metadata); seg != nil {
		reqWasPointer := IsShmPointerBatch(req.Batch)
		if reqWasPointer {
			resolved, releaseOff, release, rerr := ResolveShmBatch(req.Batch, seg)
			if rerr != nil {
				// Unrecoverable: the pointer batch carries no usable
				// row data on its own, so dispatching it would silently
				// fail parameter deserialization. Surface the resolve
				// error directly to the client. The method may not
				// have been looked up yet at this point, so use an
				// empty schema for the error response (matches the
				// other early-error paths in this function).
				rpcErr := &RpcError{
					Type:    "IOError",
					Message: fmt.Sprintf("shm resolve failed: %v", rerr),
				}
				emptySchema := arrow.NewSchema(nil, nil)
				s.logIPCWriteErr("error-response", req.Method,
					writeErrorResponse(w, emptySchema, rpcErr, s.serverID, req.RequestID, s.debugErrors))
				return nil
			}
			req.Batch.Release()
			req.Batch = resolved
			if release {
				_ = seg.FreeOffset(releaseOff)
			}
		}
		// Expose the segment to the dispatch (for response shm-write and
		// streaming continuation-input resolution) only when the client engaged
		// shm on THIS request — it advertised the segment (init) or sent a
		// pointer batch (data exchange). Control RPCs that neither advertise nor
		// reference the segment keep req.Shm nil so their small responses are
		// written inline, matching the per-request behaviour clients expect.
		_, hasSegName := req.Metadata[MetaShmSegmentName]
		if hasSegName || reqWasPointer {
			req.Shm = seg
		}
	}

	// Defensive: a pointer batch the worker could not resolve (no segment was
	// ever advertised on this connection) means the client used shm without
	// honoring the __transport_options__ negotiation. Fail loudly rather than
	// silently mis-deserializing the zero-row pointer. (A resolved pointer is no
	// longer a pointer batch here, so this only fires when resolution was
	// impossible.)
	if IsShmPointerBatch(req.Batch) {
		rpcErr := &RpcError{
			Type:    "IOError",
			Message: "received shm pointer batch but no segment is attached (transport negotiation mismatch)",
		}
		emptySchema := arrow.NewSchema(nil, nil)
		s.logIPCWriteErr("error-response", req.Method,
			writeErrorResponse(w, emptySchema, rpcErr, s.serverID, req.RequestID, s.debugErrors))
		return nil
	}

	// Handle __describe__ introspection
	if req.Method == "__describe__" {
		return s.serveDescribe(w, req)
	}

	// Handle __transport_options__ transport capability negotiation
	if req.Method == "__transport_options__" {
		return s.serveTransportOptions(w)
	}

	// Look up method
	info, ok := s.methods[req.Method]
	if !ok {
		available := s.availableMethods()
		errMsg := fmt.Sprintf("Unknown method: '%s'. Available methods: %v", req.Method, available)
		emptySchema := arrow.NewSchema(nil, nil)
		s.logIPCWriteErr("error-response", req.Method, writeErrorResponse(w, emptySchema,
			&MethodNotImplementedError{Method: req.Method, Message: errMsg},
			s.serverID, req.RequestID, s.debugErrors))
		return nil
	}

	// Capture self-contained IPC bytes of the request batch for observability hooks.
	// Best-effort: a serialization failure here must not fail dispatch.
	var reqBytes []byte
	if rb, serErr := SerializeRequestBatch(req.Batch); serErr == nil {
		reqBytes = rb
	}

	// Build dispatch info and stats for hooks. For stream methods, mint a
	// stable stream_id up front; pipe transport processes the entire stream
	// lifetime within this one serveOne call, so a single ID covers init
	// and all continuations.
	var streamID string
	if methodTypeString(info.Type) == DispatchMethodStream {
		streamID = RandomStreamID()
	}
	dispatchInfo := DispatchInfo{
		Method:            req.Method,
		MethodType:        methodTypeString(info.Type),
		ServerID:          s.serverID,
		Protocol:          s.serviceName,
		ProtocolHash:      s.ProtocolHash(),
		ProtocolVersion:   s.protocolVersion,
		RequestID:         req.RequestID,
		TransportMetadata: req.Metadata,
		Auth:              Anonymous(),
		RequestData:       reqBytes,
		StreamID:          streamID,
		Implementation:    s.implementation,
	}

	// Application-protocol-version gate. Fires only when the operator
	// declared a protocol_version via [Server.SetProtocolVersion].
	// ``__describe__`` is exempt (it's the diagnostic path a mismatched
	// client uses to introspect the server's expected version, and that
	// short-circuit already ran above). Mirrors Python's
	// ``RpcServer.serve_one`` check at the same point.
	if s.protocolVersionSet {
		clientVersion, present := req.Metadata[MetaProtocolVersion]
		if pverr := s.checkProtocolVersion(clientVersion, present); pverr != nil {
			errSchema := info.ResultSchema
			if errSchema == nil || methodTypeString(info.Type) != DispatchMethodUnary {
				errSchema = arrow.NewSchema(nil, nil)
			}
			s.logIPCWriteErr("error-response", req.Method,
				writeErrorResponse(w, errSchema, pverr, s.serverID, req.RequestID, s.debugErrors))
			return nil
		}
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

// serveTransportOptions handles the __transport_options__ capability handshake:
// a framework-level negotiation (parallel to __describe__) through which the
// client discovers whether the shared-memory side-channel may be used. The
// worker's capabilities ride as response metadata under vgi_rpc.transport.*;
// the response batch is empty. shm is offered only when this build supports it.
func (s *Server) serveTransportOptions(w io.Writer) error {
	emptySchema := arrow.NewSchema(nil, nil)
	shmVal := "false"
	if shmSupported {
		shmVal = "true"
	}
	keys := []string{MetaTransportShm, MetaRequestVersion}
	vals := []string{shmVal, ProtocolVersion}
	if s.serverID != "" {
		keys = append(keys, MetaServerID)
		vals = append(vals, s.serverID)
	}
	meta := arrow.NewMetadata(keys, vals)

	rec := array.NewRecordBatchWithMetadata(emptySchema, nil, 0, meta)
	defer rec.Release()

	writer := ipc.NewWriter(w, ipc.WithSchema(emptySchema))
	defer writer.Close()

	return writer.Write(rec)
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
