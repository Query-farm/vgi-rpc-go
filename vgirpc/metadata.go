// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"fmt"
	"regexp"
	"strconv"
)

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
	// MetaCancel signals client-initiated cancellation of a streaming RPC.
	// When present on an input batch the server ends the stream cleanly
	// without invoking Produce/Exchange (optionally running StreamCanceller.OnCancel).
	MetaCancel = "vgi_rpc.cancel"
	// MetaShmOffset carries the byte offset for shared memory pointers.
	MetaShmOffset = "vgi_rpc.shm_offset"
	// MetaShmLength carries the byte length for shared memory pointers.
	MetaShmLength = "vgi_rpc.shm_length"
	// MetaShmSegmentName carries the POSIX shared-memory segment name on
	// the request batch's custom_metadata. Advertised by the client; the
	// server attaches per-request.
	MetaShmSegmentName = "vgi_rpc.shm_segment_name"
	// MetaShmSegmentSize carries the segment size (decimal-encoded bytes,
	// including the 64 KB header) on the request batch's custom_metadata.
	MetaShmSegmentSize = "vgi_rpc.shm_segment_size"
	// MetaShmSource is set on a materialized batch after a pointer batch
	// has been resolved, carrying the segment name. Diagnostic only.
	MetaShmSource = "vgi_rpc.shm_source"
	// MetaLocation carries a URI for external pointer references.
	MetaLocation = "vgi_rpc.location"
	// MetaLocationSHA256 carries a hex-encoded SHA-256 checksum of the raw
	// (pre-compression) IPC bytes for integrity verification.
	MetaLocationSHA256 = "vgi_rpc.location.sha256"
	// MetaErrorKind carries a stable, machine-readable tag for the error
	// category (e.g. "MethodNotImplementedError") so callers can match
	// without substring-searching the human-readable message. Optional —
	// absent for unclassified errors.
	MetaErrorKind = "vgi_rpc.error_kind"
	// MetaProtocolVersion carries the application protocol surface version
	// declared by the Protocol class on every request batch. Format:
	// canonical semver MAJOR.MINOR.PATCH (see [semverRegex]). The server
	// enforces an exact major+minor match at the dispatch boundary; patch
	// is ignored. Mismatches surface as ProtocolVersionError with a
	// directional message. Distinct from [MetaRequestVersion] (wire framing
	// version, currently "1") — this layer versions the *protocol surface*.
	MetaProtocolVersion = "vgi_rpc.protocol_version"

	// MetaTraceparent carries the W3C traceparent header for distributed tracing.
	MetaTraceparent = "traceparent"
	// MetaTracestate carries the W3C tracestate header for distributed tracing.
	MetaTracestate = "tracestate"

	// ProtocolVersion is the current protocol version string.
	ProtocolVersion = "1"
)

// semverRegex matches canonical semver MAJOR.MINOR.PATCH with non-negative
// integers and no leading zeros (except literal "0"). No prerelease tags
// (1.0.0-rc1) and no build metadata (1.0.0+foo) — mirrors Python's
// vgi_rpc.metadata.SEMVER_REGEX byte-for-byte.
var semverRegex = regexp.MustCompile(`^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$`)

// parseSemver parses a canonical semver string into (major, minor, patch).
// Returns an error for any input that isn't ``MAJOR.MINOR.PATCH`` with
// non-negative integers and no leading zeros (except literal ``0``). No
// prereleases (``1.0.0-rc1``) and no build metadata (``1.0.0+foo``).
// Mirrors Python's vgi_rpc.metadata.parse_version.
func parseSemver(value string) (major, minor, patch int, err error) {
	m := semverRegex.FindStringSubmatch(value)
	if m == nil {
		//lint:ignore ST1005 message text mirrors Python's parse_version() verbatim for cross-language parity
		return 0, 0, 0, fmt.Errorf(
			"Invalid protocol version %q: expected canonical semver "+
				"MAJOR.MINOR.PATCH with non-negative integers and no leading zeros "+
				"(no prereleases or build metadata).",
			value)
	}
	major, _ = strconv.Atoi(m[1])
	minor, _ = strconv.Atoi(m[2])
	patch, _ = strconv.Atoi(m[3])
	return major, minor, patch, nil
}
