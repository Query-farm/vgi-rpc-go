// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"io"
	"log/slog"
	"reflect"
	"sort"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// TransportKind is a coarse identifier of the transport binding a [Server].
//
// Workers (RPC method implementations) read this via [Server.TransportKind]
// or the [ServeStartHook] lifecycle hook to tailor startup behaviour
// (skip HTTP-only caching, enable transport-specific metrics, etc.).
// Per-call branching is available via [CallContext.Kind].
type TransportKind string

const (
	// TransportKindPipe identifies stdio / pipe / shared-memory pipe transports.
	TransportKindPipe TransportKind = "pipe"
	// TransportKindHTTP identifies the HTTP server transport.
	TransportKindHTTP TransportKind = "http"
	// TransportKindUnix identifies AF_UNIX socket transports.
	TransportKindUnix TransportKind = "unix"
)

// TransportCapabilityShm signals the transport supports zero-copy
// shared-memory pointer batches. Currently only set for pipe transports
// bound through a shared-memory segment.
const TransportCapabilityShm = "shm"

// ServeStartHook is a lifecycle callback fired once per process before
// the first request is dispatched. For HTTP it fires on the first request
// handled in the current process (lazy / fork-safe). For pipe transports
// it fires when [Server.Serve] begins.
//
// A hook that returns an error propagates out of the serve path; the
// transport binding is NOT committed when the hook fails, so a retry
// re-fires the hook rather than silently skipping it.
type ServeStartHook func(kind TransportKind, capabilities map[string]bool) error

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
	methods              map[string]*methodInfo
	serverID             string
	serviceName          string
	protocolVersion      string // canonical semver MAJOR.MINOR.PATCH, or "" when opted out
	protocolVersionParts [3]int // parsed (major, minor, patch); used when protocolVersion != ""
	protocolVersionSet   bool   // true when SetProtocolVersion was called with a non-empty value
	protocolHash         string
	dispatchHook         DispatchHook
	debugErrors          bool
	externalConfig       *ExternalLocationConfig
	implementation       any

	// Transport binding state, set lazily by notifyTransport.
	transportMu           sync.Mutex
	transportKind         TransportKind
	transportCapabilities map[string]bool
	serveStartHook        ServeStartHook
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

// SetServiceName sets a logical service name used by observability hooks
// and as the default protocol name on HTTP pages.
func (s *Server) SetServiceName(name string) {
	s.serviceName = name
}

// ServiceName returns the logical service name, or empty string if not set.
func (s *Server) ServiceName() string {
	return s.serviceName
}

// ServerID returns the server identifier, or empty string if not set.
func (s *Server) ServerID() string {
	return s.serverID
}

// SetImplementation stores an opaque reference to the service implementation
// so framework callbacks (CallContext.Implementation, DispatchInfo.Implementation)
// can hand it to dispatch hooks and stream-state lifecycle callbacks.
// Mirrors Python's `RpcServer(..., implementation=impl)` parameter. The value
// is not consulted by the framework itself; it's a pass-through for callers.
func (s *Server) SetImplementation(impl any) {
	s.implementation = impl
}

// Implementation returns the value passed to [Server.SetImplementation],
// or nil if none was set.
func (s *Server) Implementation() any {
	return s.implementation
}

// SetExternalLocation configures external storage for large batches.
// When set, batches exceeding the threshold are uploaded to storage and
// replaced with pointer batches containing a download URL.
func (s *Server) SetExternalLocation(config *ExternalLocationConfig) {
	s.externalConfig = config
}

// SetDispatchHook registers a hook that is called around each RPC dispatch.
func (s *Server) SetDispatchHook(hook DispatchHook) {
	s.dispatchHook = hook
}

// SetServeStartHook registers a lifecycle callback fired once per process
// before the first request is dispatched. Use this to run worker-side
// setup that depends on the transport binding (warm caches differently
// for HTTP vs. pipe, attach metrics labels, etc.).
//
// The hook is fired lazily on first dispatch for HTTP (so pre-fork
// servers wire each child correctly) and eagerly on [Server.Serve] for
// pipe transports.
func (s *Server) SetServeStartHook(hook ServeStartHook) {
	s.transportMu.Lock()
	defer s.transportMu.Unlock()
	s.serveStartHook = hook
}

// TransportKind returns the kind of transport the server is bound to,
// or empty string before the first request is dispatched.
func (s *Server) TransportKind() TransportKind {
	s.transportMu.Lock()
	defer s.transportMu.Unlock()
	return s.transportKind
}

// TransportCapabilities returns the capability set advertised by the
// bound transport. Currently includes "shm" when a shared-memory pipe
// is in use. Returns nil before a transport is bound.
func (s *Server) TransportCapabilities() map[string]bool {
	s.transportMu.Lock()
	defer s.transportMu.Unlock()
	if s.transportCapabilities == nil {
		return nil
	}
	out := make(map[string]bool, len(s.transportCapabilities))
	for k, v := range s.transportCapabilities {
		out[k] = v
	}
	return out
}

// notifyTransport binds the server to a transport and fires
// [ServeStartHook] once. Idempotent for the same (kind, capabilities).
// Bind state is committed only after the hook returns successfully, so a
// transient hook failure leaves transportKind unset and the next request
// re-fires the hook rather than silently skipping it.
func (s *Server) notifyTransport(kind TransportKind, capabilities map[string]bool) error {
	s.transportMu.Lock()
	if s.transportKind == kind && capabilitiesEqual(s.transportCapabilities, capabilities) {
		s.transportMu.Unlock()
		return nil
	}
	hook := s.serveStartHook
	s.transportMu.Unlock()

	if hook != nil {
		// Defensive copy so the hook can't mutate our internal state.
		capsCopy := make(map[string]bool, len(capabilities))
		for k, v := range capabilities {
			capsCopy[k] = v
		}
		if err := hook(kind, capsCopy); err != nil {
			slog.Error("vgirpc: on_serve_start hook failed; not committing transport binding",
				"transport_kind", string(kind), "err", err)
			return err
		}
	}

	s.transportMu.Lock()
	s.transportKind = kind
	s.transportCapabilities = capabilities
	s.transportMu.Unlock()
	return nil
}

func capabilitiesEqual(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// SetProtocolVersion declares the application protocol surface version
// the server advertises and enforces. “v“ must be canonical semver
// MAJOR.MINOR.PATCH (e.g. “"1.0.0"“); the empty string opts back out.
//
// When set, the server:
//   - surfaces “v“ under “vgi_rpc.protocol_version“ in the
//     “__describe__“ response custom_metadata (so a mismatched client
//     can introspect the expected version), and
//   - enforces an exact major+minor match against the client's
//     “vgi_rpc.protocol_version“ request metadata at the dispatch
//     boundary. Patch is ignored. Mismatch raises
//     [ProtocolVersionError] (vgi_rpc.error_kind =
//     “protocol_version_mismatch“) with a directional message.
//
// “__describe__“ requests are exempt from the dispatch check so a
// mismatched client can discover the server's version. Mirrors Python's
// “RpcServer(protocol)“ reading “Protocol.protocol_version“.
//
// Panics if “v“ is non-empty and not canonical semver. Pre-flight
// validation here keeps the malformed-config failure mode close to the
// configuration call site rather than surfacing on first dispatch.
func (s *Server) SetProtocolVersion(v string) {
	if v == "" {
		s.protocolVersion = ""
		s.protocolVersionSet = false
		s.protocolVersionParts = [3]int{}
		return
	}
	major, minor, patch, err := parseSemver(v)
	if err != nil {
		panic(err)
	}
	s.protocolVersion = v
	s.protocolVersionParts = [3]int{major, minor, patch}
	s.protocolVersionSet = true
}

// ProtocolVersion returns the application protocol surface version the
// server advertises and enforces, or the empty string when none is set
// (opt-out — no dispatch-boundary check fires).
func (s *Server) ProtocolVersion() string {
	return s.protocolVersion
}

// checkProtocolVersion validates a client's declared protocol_version
// against the server's. Returns a *ProtocolVersionError on mismatch
// (including missing / malformed / undecodable) or nil on match. Caller
// is responsible for invoking only when “s.protocolVersionSet“ is true.
// Mirrors Python's RpcServer._check_protocol_version directional-message
// format byte-for-byte.
func (s *Server) checkProtocolVersion(clientVersion string, present bool) *ProtocolVersionError {
	if !present {
		return &ProtocolVersionError{
			Message: "VGI client/worker protocol_version mismatch.\n" +
				"  Client: <not declared>\n" +
				"  Server: " + s.protocolVersion + "\n" +
				"  Direction: the client did not send a vgi_rpc.protocol_version " +
				"metadata key. This is either a vgi-rpc framework bug or a " +
				"non-VGI client connecting to a VGI worker.",
		}
	}
	major, minor, _, err := parseSemver(clientVersion)
	if err != nil {
		return &ProtocolVersionError{
			Message: "VGI client/worker protocol_version mismatch.\n" +
				"  Client: " + clientVersion + "\n" +
				"  Server: " + s.protocolVersion + "\n" +
				"  Direction: client sent a malformed protocol_version. " +
				"Expected canonical semver MAJOR.MINOR.PATCH.",
		}
	}
	serverMajor, serverMinor := s.protocolVersionParts[0], s.protocolVersionParts[1]
	if major == serverMajor && minor == serverMinor {
		return nil
	}
	var direction string
	if major < serverMajor || (major == serverMajor && minor < serverMinor) {
		direction = "client is too old; upgrade the VGI extension/client to a " +
			"version supporting protocol_version " + s.protocolVersion + "."
	} else {
		direction = "server is too old; upgrade the VGI worker to a version " +
			"supporting protocol_version " + clientVersion + "."
	}
	return &ProtocolVersionError{
		Message: "VGI client/worker protocol_version mismatch.\n" +
			"  Client: " + clientVersion + "\n" +
			"  Server: " + s.protocolVersion + "\n" +
			"  Direction: " + direction,
	}
}

// ProtocolHash returns the SHA-256 hex digest of the canonical __describe__
// payload. Computed lazily on first call and cached.
func (s *Server) ProtocolHash() string {
	if s.protocolHash == "" {
		batch, meta := s.buildDescribeBatch()
		batch.Release()
		if v, ok := meta.GetValue(MetaProtocolHash); ok {
			s.protocolHash = v
		}
	}
	return s.protocolHash
}

// SetDebugErrors controls whether error responses include full stack traces
// with file paths and function names. When false (the default), error responses
// contain only the error type and message. Enable this for development or
// internal services; disable it for public-facing deployments to avoid leaking
// implementation details.
func (s *Server) SetDebugErrors(enabled bool) {
	s.debugErrors = enabled
}

// drainInputStream reads and discards any batches remaining on the input
// IPC stream so the underlying transport is left clean for the next
// request. Safe to call in error paths before any batches have been read.
func drainInputStream(r io.Reader) {
	inputReader, err := ipc.NewReader(r)
	if err != nil {
		return
	}
	for inputReader.Next() {
		// discard
	}
	inputReader.Release()
}

// logIPCWriteErr reports a non-nil error encountered while writing an Arrow
// IPC stream on the serve-loop transport (stdio/pipe/unix socket). The writer
// is the live transport, so errors here indicate real I/O failures — either
// the peer went away or the stream was corrupted. Logs at error level.
func (s *Server) logIPCWriteErr(op, method string, err error) {
	if err == nil {
		return
	}
	slog.Error("ipc write failed", "op", op, "method", method, "err", err)
}

// Unary registers a unary RPC method with typed parameters and return value.
// P must be a struct with `vgirpc` tags. R is the return type.

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

func (s *Server) availableMethods() []string {
	names := make([]string, 0, len(s.methods))
	for name := range s.methods {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
