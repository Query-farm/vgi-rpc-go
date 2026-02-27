// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

// Well-known metadata keys used in the vgi_rpc wire protocol.
// These appear as custom_metadata on Arrow IPC RecordBatch messages.
const (
	// MetaMethod identifies the RPC method name in a request batch.
	MetaMethod = "vgi_rpc.method"
	// MetaRequestVersion carries the protocol version in a request batch.
	MetaRequestVersion = "vgi_rpc.request_version"
	// MetaRequestID carries the client-supplied request identifier, echoed
	// in all response batches for correlation.
	MetaRequestID = "vgi_rpc.request_id"
	// MetaLogLevel carries the log severity level for log and error batches,
	// and the client-requested minimum level in request batches.
	MetaLogLevel = "vgi_rpc.log_level"
	// MetaLogMessage carries the log message text in log and error batches.
	MetaLogMessage = "vgi_rpc.log_message"
	// MetaLogExtra carries a JSON object with structured log data (e.g.
	// exception details, key-value extras).
	MetaLogExtra = "vgi_rpc.log_extra"
	// MetaServerID carries the server identifier set via [Server.SetServerID].
	MetaServerID = "vgi_rpc.server_id"
	// MetaStreamState carries the HMAC-signed state token for HTTP stateful
	// exchange streams. The "#b64" suffix signals that the value is
	// base64-encoded binary data, ensuring UTF-8 validity in Arrow IPC metadata.
	MetaStreamState = "vgi_rpc.stream_state#b64"
	// MetaShmOffset carries the byte offset for shared memory pointers.
	MetaShmOffset = "vgi_rpc.shm_offset"
	// MetaShmLength carries the byte length for shared memory pointers.
	MetaShmLength = "vgi_rpc.shm_length"
	// MetaLocation carries a URI for external pointer references.
	MetaLocation = "vgi_rpc.location"

	// MetaTraceparent carries the W3C traceparent header for distributed tracing.
	MetaTraceparent = "traceparent"
	// MetaTracestate carries the W3C tracestate header for distributed tracing.
	MetaTracestate = "tracestate"

	// ProtocolVersion is the current protocol version string.
	ProtocolVersion = "1"
)
