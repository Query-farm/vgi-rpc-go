// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"encoding/json"
	"fmt"
	"runtime"
)

// ErrRpc is a sentinel for use with errors.Is to check whether any error in a
// chain is an *RpcError.
var ErrRpc = &RpcError{}

// RpcError represents an error in the vgi_rpc protocol.
type RpcError struct {
	// Type is the error category (e.g. "ValueError", "RuntimeError",
	// "TypeError") matching Python exception class names.
	Type string
	// Message is the human-readable error description.
	Message string
	// Traceback is an optional stack trace string, populated automatically
	// when the error is serialized to the wire.
	Traceback string
	// RequestID is the client-supplied request identifier, set when the
	// error is written to a response batch.
	RequestID string
	// Kind is an optional, stable, machine-readable tag emitted as
	// vgi_rpc.error_kind on the wire. Used by Python clients to
	// distinguish typed sub-errors (e.g. MethodNotImplementedError)
	// from generic AttributeError instances without substring matching.
	Kind string
}

// Error returns a string of the form "Type: Message".
func (e *RpcError) Error() string {
	return fmt.Sprintf("%s: %s", e.Type, e.Message)
}

// Is supports errors.Is by matching any *RpcError target.
func (e *RpcError) Is(target error) bool {
	_, ok := target.(*RpcError)
	return ok
}

// ErrorKind returns the value to advertise as vgi_rpc.error_kind on the
// wire when this error becomes an EXCEPTION batch. Empty means "no
// classification" — the metadata key is omitted in that case so older
// clients that don't recognise the key see no change. Mirrors Python's
// error_kind class attribute mechanism.
func (e *RpcError) ErrorKind() string {
	return e.Kind
}

// errorKindCarrier is satisfied by errors that want to advertise a
// machine-readable kind on the wire. Both *RpcError and the typed
// MethodNotImplementedError sentinel implement it.
type errorKindCarrier interface {
	ErrorKind() string
}

// MethodNotImplementedError marks a request for a method the service
// does not expose. The framework writes vgi_rpc.error_kind =
// "MethodNotImplementedError" so callers can distinguish "method gone"
// from other AttributeError-class failures without parsing the message.
type MethodNotImplementedError struct {
	Method  string
	Message string
}

func (e *MethodNotImplementedError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("Unknown method: '%s'", e.Method)
}

// ErrorKind returns the stable tag emitted on the wire.
func (e *MethodNotImplementedError) ErrorKind() string {
	return "MethodNotImplementedError"
}

// ErrorType is the "exception_type" string lifted into the error envelope
// — kept as "AttributeError" so existing Python clients that match on it
// continue to work. The new error_kind metadata key is the typed sibling.
func (e *MethodNotImplementedError) ErrorType() string {
	return "AttributeError"
}

// ProtocolVersionError surfaces when the client's declared
// ``vgi_rpc.protocol_version`` is incompatible with the server's
// (or absent / malformed). The framework writes
// ``vgi_rpc.error_kind = "protocol_version_mismatch"`` and the
// message text is directional — it tells the reader which side to
// upgrade. Mirrors Python's ProtocolVersionError (subclass of
// VersionError). Wraps as HTTP 400 on the HTTP transport.
type ProtocolVersionError struct {
	Message string
}

func (e *ProtocolVersionError) Error() string {
	return e.Message
}

// ErrorKind returns the wire-stable kind for ProtocolVersionError.
func (e *ProtocolVersionError) ErrorKind() string { return "protocol_version_mismatch" }

// ErrorType is the exception type name surfaced to Python clients.
func (e *ProtocolVersionError) ErrorType() string { return "ProtocolVersionError" }

// SessionLostError surfaces from the sticky session machinery when a
// presented VGI-Session token cannot be resolved to a live registry
// entry — malformed token, AAD mismatch (cross-principal replay),
// server_id mismatch (wrong worker), registry miss, TTL expiry. Wire
// shape mirrors Python: 200 + X-VGI-RPC-Error + EXCEPTION batch with
// vgi_rpc.error_kind = "session_lost" so cross-language clients can
// match on the metadata key.
type SessionLostError struct {
	Reason string
}

func (e *SessionLostError) Error() string {
	if e.Reason != "" {
		return e.Reason
	}
	return "session lost"
}

// ErrorKind returns the wire-stable kind for SessionLostError.
func (e *SessionLostError) ErrorKind() string { return "session_lost" }

// ErrorType is the exception type name that Python's typed exception
// class surfaces as.
func (e *SessionLostError) ErrorType() string { return "SessionLostError" }

// ServerDrainingError surfaces from ctx.OpenSession when the server
// is in drain mode and refusing new sessions. Existing-session calls
// continue to serve until TTL or explicit close.
type ServerDrainingError struct{}

func (e *ServerDrainingError) Error() string {
	return "server is draining — new sessions are rejected"
}

// ErrorKind returns the wire-stable kind for ServerDrainingError.
func (e *ServerDrainingError) ErrorKind() string { return "server_draining" }

// ErrorType is the exception type name that Python's typed exception
// class surfaces as.
func (e *ServerDrainingError) ErrorType() string { return "ServerDrainingError" }

// stackFrame represents a single frame in a Go stack trace,
// matching the Python wire format for error batch log_extra.
type stackFrame struct {
	File     string `json:"file"`
	Line     int    `json:"line"`
	Function string `json:"function"`
}

// errorExtra is the JSON structure written to vgi_rpc.log_extra
// for EXCEPTION-level log batches.
type errorExtra struct {
	ExceptionType    string       `json:"exception_type"`
	ExceptionMessage string       `json:"exception_message"`
	Traceback        string       `json:"traceback"`
	Frames           []stackFrame `json:"frames"`
}

// buildErrorExtra creates the JSON string for vgi_rpc.log_extra from an error.
// When debug is false, stack traces and file paths are omitted to avoid leaking
// implementation details to clients.
func buildErrorExtra(err error, debug bool) string {
	errType := fmt.Sprintf("%T", err)

	// Prefer the wire-stable class name for typed errors.
	switch e := err.(type) {
	case *RpcError:
		errType = e.Type
	case *MethodNotImplementedError:
		errType = e.ErrorType()
	case *SessionLostError:
		errType = e.ErrorType()
	case *ServerDrainingError:
		errType = e.ErrorType()
	case *ProtocolVersionError:
		errType = e.ErrorType()
	}

	extra := errorExtra{
		ExceptionType:    errType,
		ExceptionMessage: err.Error(),
	}

	if debug {
		// Capture Go stack trace
		buf := make([]byte, 4096)
		n := runtime.Stack(buf, false)
		extra.Traceback = string(buf[:n])

		// Extract frames from runtime callers
		pcs := make([]uintptr, 10)
		n = runtime.Callers(2, pcs)
		if n > 0 {
			callersFrames := runtime.CallersFrames(pcs[:n])
			count := 0
			for {
				frame, more := callersFrames.Next()
				if count >= 5 {
					break
				}
				extra.Frames = append(extra.Frames, stackFrame{
					File:     frame.File,
					Line:     frame.Line,
					Function: frame.Function,
				})
				count++
				if !more {
					break
				}
			}
		}
	}

	data, _ := json.Marshal(extra)
	return string(data)
}
