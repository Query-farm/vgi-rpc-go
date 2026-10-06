// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"encoding/json"
	"errors"
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
	// Kind is the error's reason, vgi_rpc.error_kind on the wire: an open,
	// stable, machine-readable token a client branches on. "" when absent.
	Kind string
	// Code is the canonical code's name, vgi_rpc.error_code on the wire
	// (WIRE_PROTOCOL.md §8). On a decoded error, "" means the server sent none
	// -- a server older than the error model -- which is a different answer
	// from "UNKNOWN". [RpcError.ErrorCode] reads it as a [Code].
	Code string
	// Details is vgi_rpc.error_details as received: every object, in wire
	// order, unknown types included. The typed accessors ([RpcError.RetryInfo]
	// and friends) return the catalog entry of their type and skip the rest.
	Details []map[string]any
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

// ErrorCode returns the canonical code; [CodeUnknown] when absent or not one of
// the sixteen. Raised from a handler, an *RpcError is emitted with this code.
func (e *RpcError) ErrorCode() Code {
	return ParseCode(e.Code)
}

// ErrorDetails returns the details as [RawDetail] values, so an *RpcError
// relayed by an intermediary carries them on unchanged.
func (e *RpcError) ErrorDetails() []ErrorDetail {
	if len(e.Details) == 0 {
		return nil
	}
	out := make([]ErrorDetail, len(e.Details))
	for i, d := range e.Details {
		out[i] = RawDetail(d)
	}
	return out
}

// IsRetryable reports whether retrying this call is warranted by the rule in
// WIRE_PROTOCOL.md §8: UNAVAILABLE always, RESOURCE_EXHAUSTED only with
// RetryInfo. When [RpcError.RetryInfo] is present a retry waits at least that
// long. The client never retries an RPC error itself: a method may not be
// idempotent, so retry is the caller's decision.
func (e *RpcError) IsRetryable() bool {
	return IsRetryable(e.ErrorCode(), e.Details)
}

// TypedDetails returns the details this client understands, in wire order;
// unknown types and malformed objects are skipped.
func (e *RpcError) TypedDetails() []ErrorDetail {
	var out []ErrorDetail
	for _, d := range e.Details {
		if typed := ParseErrorDetail(d); typed != nil {
			out = append(out, typed)
		}
	}
	return out
}

func rpcErrorDetail[T ErrorDetail](e *RpcError) (T, bool) {
	for _, d := range e.Details {
		if typed, ok := ParseErrorDetail(d).(T); ok {
			return typed, true
		}
	}
	var zero T
	return zero, false
}

// ErrorInfo returns the vgi_rpc.ErrorInfo detail, if present.
func (e *RpcError) ErrorInfo() (ErrorInfo, bool) { return rpcErrorDetail[ErrorInfo](e) }

// RetryInfo returns the vgi_rpc.RetryInfo detail, if present.
func (e *RpcError) RetryInfo() (RetryInfo, bool) { return rpcErrorDetail[RetryInfo](e) }

// BadRequest returns the vgi_rpc.BadRequest detail, if present.
func (e *RpcError) BadRequest() (BadRequest, bool) { return rpcErrorDetail[BadRequest](e) }

// PreconditionFailure returns the vgi_rpc.PreconditionFailure detail, if present.
func (e *RpcError) PreconditionFailure() (PreconditionFailure, bool) {
	return rpcErrorDetail[PreconditionFailure](e)
}

// QuotaFailure returns the vgi_rpc.QuotaFailure detail, if present.
func (e *RpcError) QuotaFailure() (QuotaFailure, bool) { return rpcErrorDetail[QuotaFailure](e) }

// ResourceInfo returns the vgi_rpc.ResourceInfo detail, if present.
func (e *RpcError) ResourceInfo() (ResourceInfo, bool) { return rpcErrorDetail[ResourceInfo](e) }

// Help returns the vgi_rpc.Help detail, if present.
func (e *RpcError) Help() (Help, bool) { return rpcErrorDetail[Help](e) }

// LocalizedMessage returns the vgi_rpc.LocalizedMessage detail, if present.
func (e *RpcError) LocalizedMessage() (LocalizedMessage, bool) {
	return rpcErrorDetail[LocalizedMessage](e)
}

// errorKindCarrier is satisfied by errors that want to advertise a
// machine-readable kind on the wire. Both *RpcError and the typed
// MethodNotImplementedError sentinel implement it.
type errorKindCarrier interface {
	ErrorKind() string
}

// errorTypeCarrier is satisfied by errors that name the wire-stable exception
// class they should surface as, so a Go type name never reaches a client in a
// field every other port fills with a shared class name.
type errorTypeCarrier interface {
	ErrorType() string
}

// MethodNotImplementedError marks a request for a method the service
// does not expose. The framework writes vgi_rpc.error_kind =
// "method_not_implemented" (code UNIMPLEMENTED) so callers can distinguish
// "method gone" from other AttributeError-class failures without parsing the
// message -- the documented capability-probe signal.
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
	return "method_not_implemented"
}

// ErrorCode returns UNIMPLEMENTED.
func (e *MethodNotImplementedError) ErrorCode() Code { return CodeUnimplemented }

// ErrorType is the "exception_type" string lifted into the error envelope
// — kept as "AttributeError" so existing Python clients that match on it
// continue to work. The new error_kind metadata key is the typed sibling.
func (e *MethodNotImplementedError) ErrorType() string {
	return "AttributeError"
}

// ProtocolVersionError surfaces when the client's declared
// “vgi_rpc.protocol_version“ is incompatible with the server's
// (or absent / malformed). The framework writes
// “vgi_rpc.error_kind = "protocol_version_mismatch"“ and the
// message text is directional — it tells the reader which side to
// upgrade. Mirrors Python's ProtocolVersionError (subclass of
// VersionError). Wraps as HTTP 400 on the HTTP transport.
type ProtocolVersionError struct {
	Message string
	// Protocol names the gated protocol. With several bindings, "Server:
	// 2.0.0" alone does not say which server, so the PreconditionFailure
	// detail names it.
	Protocol string
}

func (e *ProtocolVersionError) Error() string {
	return e.Message
}

// ErrorKind returns the wire-stable kind for ProtocolVersionError.
func (e *ProtocolVersionError) ErrorKind() string { return "protocol_version_mismatch" }

// ErrorCode returns FAILED_PRECONDITION.
func (e *ProtocolVersionError) ErrorCode() Code { return CodeFailedPrecondition }

// ErrorDetails returns one PreconditionFailure naming the gated protocol.
func (e *ProtocolVersionError) ErrorDetails() []ErrorDetail {
	return []ErrorDetail{PreconditionFailure{Violations: []PreconditionViolation{{
		Type:        "protocol_version",
		Subject:     e.Protocol,
		Description: "the client's protocol_version is incompatible with the server's",
	}}}}
}

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

// ErrorCode returns ABORTED: retry the whole session, not the call.
func (e *SessionLostError) ErrorCode() Code { return CodeAborted }

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

// ErrorCode returns UNAVAILABLE.
func (e *ServerDrainingError) ErrorCode() Code { return CodeUnavailable }

// ErrorDetails returns the default one-second RetryInfo.
func (e *ServerDrainingError) ErrorDetails() []ErrorDetail {
	return []ErrorDetail{RetryInfo{RetryDelaySeconds: 1}}
}

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
	ExceptionType    string          `json:"exception_type"`
	ExceptionMessage string          `json:"exception_message"`
	ErrorCode        Code            `json:"error_code"`
	ErrorKind        string          `json:"error_kind,omitempty"`
	ErrorDetails     json.RawMessage `json:"error_details,omitempty"`
	// Absent -- not empty -- when the operator turned tracebacks off
	// (Server.SetIncludeTracebacks); present by default (WIRE_PROTOCOL.md §8).
	Traceback string       `json:"traceback,omitempty"`
	Frames    []stackFrame `json:"frames,omitempty"`
}

// errorModel is the error model's three layers, read off a server-side error
// once so the top-level keys and the log_extra mirror cannot disagree.
type errorModel struct {
	code Code
	kind string
	// details is the encoded array, or nil when the error declares none or
	// they were dropped (rule violation, or over the 4 KiB cap).
	details json.RawMessage
}

func errorModelOf(err error) errorModel {
	m := errorModel{code: ErrorCodeOf(err), kind: ErrorKindOf(err)}
	if encoded, ok := encodeErrorDetails(ErrorDetailsOf(err)); ok {
		m.details = encoded
	}
	return m
}

// buildErrorExtra creates the JSON string for vgi_rpc.log_extra from an error.
// When includeTraceback is false, stack traces and file paths are omitted.
func buildErrorExtra(err error, model errorModel, includeTraceback bool) string {
	errType := fmt.Sprintf("%T", err)

	// Prefer the wire-stable class name for typed errors. Asked of the error
	// itself rather than enumerated in a switch: a switch has to be edited
	// every time a typed error is added, the edit is easy to forget, and
	// forgetting it is silent -- the class name reaching the client is then a
	// Go type name no other port has ever spelled. errors.As, so a handler
	// wrapping a typed error with %w keeps its class name.
	var rpcErr *RpcError
	var carrier errorTypeCarrier
	if errors.As(err, &rpcErr) {
		errType = rpcErr.Type
	} else if errors.As(err, &carrier) {
		errType = carrier.ErrorType()
	}

	extra := errorExtra{
		ExceptionType:    errType,
		ExceptionMessage: err.Error(),
		ErrorCode:        model.code,
		ErrorKind:        model.kind,
		ErrorDetails:     model.details,
	}

	if includeTraceback {
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

	data, _ := marshalCompact(extra)
	return string(data)
}
