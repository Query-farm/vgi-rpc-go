// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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
	// TransportKindTcp identifies raw AF_INET (TCP) socket transports.
	// This speaks the same raw Arrow-IPC framing as the Unix transport over
	// a bare TCP socket — there is NO authentication or TLS, so it is for
	// trusted networks only (use TransportKindHTTP for untrusted networks).
	TransportKindTcp TransportKind = "tcp"
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
	// DeclaredStreamKind is "producer" or "exchange" when the registration
	// stated it, and "" when it did not.
	//
	// Separate from Type because the two are independent axes that the
	// registration helpers used to conflate: a method whose *output schema* is
	// decided at runtime must register as MethodDynamic, which said nothing
	// about whether it is a producer or an exchange -- and stream_kind is the
	// only field in a description that tells a client whether a stream accepts
	// input. A dynamic schema should not make the kind undiscoverable.
	DeclaredStreamKind string
	HeaderSchema       *arrow.Schema // Arrow schema for header type (if HasHeader)
	// ResultMetadata, when set, supplies custom metadata for the unary result
	// batch. Reflection uses it to carry vgi_rpc.protocol_versions on every
	// transport (PROTOCOL_VERSIONS_V1 §4.1).
	ResultMetadata func() map[string]string
}

// resultMetadata is info's extra unary result-batch metadata, empty when none.
func (info *methodInfo) resultMetadata() arrow.Metadata {
	if info.ResultMetadata == nil {
		return arrow.Metadata{}
	}
	md := info.ResultMetadata()
	if len(md) == 0 {
		return arrow.Metadata{}
	}
	keys, vals := appendExtraMetadata(nil, nil, md)
	return arrow.NewMetadata(keys, vals)
}

// withResultMetadata returns batch carrying info's extra result metadata,
// releasing batch when it is replaced.
func (info *methodInfo) withResultMetadata(batch arrow.RecordBatch) arrow.RecordBatch {
	md := info.resultMetadata()
	if md.Len() == 0 {
		return batch
	}
	out := array.NewRecordBatchWithMetadata(batch.Schema(), batch.Columns(), batch.NumRows(), md)
	batch.Release()
	return out
}

// Server is the RPC server that dispatches incoming requests to registered methods.
type Server struct {
	methods              map[string]*methodInfo
	serverID             string
	serviceName          string
	protocolVersion      string // canonical semver MAJOR.MINOR.PATCH, or "" when opted out
	protocolVersionParts [3]int // parsed (major, minor, patch); used when protocolVersion != ""
	protocolVersionSet   bool   // true when SetProtocolVersion was called with a non-empty value
	// protocolVersionRange widens the primary's accepted window downward
	// (SetProtocolVersionRange); nil means exactly protocolVersion's major.minor.
	protocolVersionRange *VersionRange
	// protocolVersionRanges overrides the window of hosted protocols by wire
	// name (SetProtocolVersionRanges) -- how a fixture impersonates an older
	// server.
	protocolVersionRanges map[string]VersionRange
	// protocolVersionsCache is the advertisement once the server is sealed.
	protocolVersionsCache atomic.Pointer[string]
	canonicalHashValue    string
	canonicalHashOnce     sync.Once

	// extraBindings holds protocols beyond the primary. The primary's methods
	// stay in `methods` so every existing registration path is untouched; it is
	// projected into a binding on demand by bindings().
	extraBindings map[string]*protocolBinding
	// extraOrder is extraBindings' keys in registration order, which is the
	// order reflection lists application protocols in (WIRE_PROTOCOL.md §3.1).
	extraOrder   []string
	dispatchHook DispatchHook
	// identity is the vgi_rpc.Identity.v2 implementation RegisterIdentity
	// hosted, or nil; an HttpServer reads its grant keys and resolver to
	// accept identity credentials as bearers.
	identity *IdentityImpl
	// omitTracebacks is the operator's switch turning tracebacks off on every
	// transport; false (the default) includes them everywhere.
	omitTracebacks bool
	// sealed is set when the server first serves anything, on any transport;
	// from then on the hosted protocol set is fixed (WIRE_PROTOCOL.md §3.1).
	sealed         atomic.Bool
	externalConfig *ExternalLocationConfig
	implementation any

	// Transport binding state, set lazily by notifyTransport.
	transportNotifyMu     sync.Mutex
	transportMu           sync.Mutex
	transportKind         TransportKind
	transportKindFast     atomic.Value // stores TransportKind after a successful binding
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

// SetDispatchHook registers a hook that is called around each RPC dispatch,
// replacing any hook already installed. To run several hooks, use
// [Server.AddDispatchHook] or [MultiDispatchHook].
func (s *Server) SetDispatchHook(hook DispatchHook) {
	s.dispatchHook = hook
}

// AddDispatchHook installs hook alongside any already registered, rather than
// replacing it as [Server.SetDispatchHook] does. Hooks start in the order they
// were added and end in reverse; see [MultiDispatchHook]. Like
// SetDispatchHook, call it before the server starts dispatching.
func (s *Server) AddDispatchHook(hook DispatchHook) {
	s.dispatchHook = MultiDispatchHook(s.dispatchHook, hook)
}

// DispatchHook returns the hook installed on the server, or nil if none is.
// It lets a caller pick up a hook that an instrumentation package installed
// with SetDispatchHook before something else replaces it.
func (s *Server) DispatchHook() DispatchHook {
	return s.dispatchHook
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
	if kind := s.transportKindFast.Load(); kind != nil {
		return kind.(TransportKind)
	}
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
	// Seal before anything else -- including a serve-start hook that refuses
	// the binding -- so "the server has started serving" is never undone.
	s.sealed.Store(true)
	// Serialize the check/hook/commit transaction. The state mutex cannot be
	// held while the hook runs because hooks may inspect TransportKind(), which
	// also takes that mutex. Without this separate gate, concurrent first HTTP
	// requests can all observe an unbound server and fire the one-shot hook.
	s.transportNotifyMu.Lock()
	defer s.transportNotifyMu.Unlock()

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
	s.transportKindFast.Store(kind)
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
//   - surfaces “v“ as protocol_version on the vgi_rpc.Reflection.v1
//     description (so a mismatched client can introspect the expected
//     version), and
//   - accepts a client “vgi_rpc.protocol_version“ whose major.minor lies
//     in the protocol's window -- exactly v's major.minor unless widened by
//     [Server.SetProtocolVersionRange] or overridden by
//     [Server.SetProtocolVersionRanges]. Patch is ignored. A refusal raises
//     [ProtocolVersionError] (vgi_rpc.error_kind =
//     “protocol_version_unsupported“) naming both windows, and
//   - advertises the window (PROTOCOL_VERSIONS_V1): the VGI-Protocol-Versions
//     HTTP header and the vgi_rpc.protocol_versions metadata key.
//
// The vgi_rpc.Reflection.v1 binding is exempt from the dispatch check so
// a mismatched client can discover the server's version. Mirrors Python's
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
		s.protocolVersionRange = nil
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

// SetProtocolVersionRange widens the window the server's own protocol accepts
// and advertises downward from its declared version (PROTOCOL_VERSIONS_V1 §5;
// Python's Protocol.protocol_version_range). r is "MIN-MAX" in major.minor;
// MAX must equal the declared version's major.minor, because the declared
// version is the newest spoken. "" restores the default (exactly the declared
// version's major.minor).
//
// Call after [Server.SetProtocolVersion]. Panics on a malformed or
// inconsistent range, close to the configuration call site.
func (s *Server) SetProtocolVersionRange(r string) {
	if r == "" {
		s.protocolVersionRange = nil
		return
	}
	if !s.protocolVersionSet {
		panic(fmt.Errorf("vgirpc: SetProtocolVersionRange(%q) requires SetProtocolVersion first", r))
	}
	rng, err := ParseVersionRange(r)
	if err != nil {
		panic(err)
	}
	top := MajorMinor{s.protocolVersionParts[0], s.protocolVersionParts[1]}
	if rng.Max != top {
		panic(fmt.Errorf(
			"vgirpc: protocol version range %q must end at protocol_version %s's major.minor (%s): "+
				"protocol_version is the newest version spoken, and the range widens it downward only",
			r, s.protocolVersion, top))
	}
	s.protocolVersionRange = &rng
}

// SetProtocolVersionRanges overrides the window hosted protocols accept and
// advertise, keyed by wire name ({"vgi.v3": "3.3-3.4"}; PROTOCOL_VERSIONS_V1
// §5, Python's RpcServer(protocol_version_ranges=...)). Unconstrained: it is
// how a fixture worker impersonates an older server. Call after every
// protocol is hosted and before serving; naming a protocol that is not
// hosted, is version-exempt, or declares no version is an error.
func (s *Server) SetProtocolVersionRanges(ranges map[string]string) error {
	if s.sealed.Load() {
		return fmt.Errorf("vgirpc: cannot change protocol version ranges: the server is already serving")
	}
	all := s.bindings()
	parsed := make(map[string]VersionRange, len(ranges))
	for name, text := range ranges {
		b, ok := all[name]
		if !ok || !b.VersionSet || b.VersionExempt {
			var versioned []string
			for _, n := range s.orderedBindingNames() {
				if bb := all[n]; bb.VersionSet && !bb.VersionExempt {
					versioned = append(versioned, n)
				}
			}
			return fmt.Errorf(
				"vgirpc: protocol version ranges name %q, which is not a hosted protocol that declares a "+
					"protocol_version. Versioned protocols here: %v", name, versioned)
		}
		rng, err := ParseVersionRange(text)
		if err != nil {
			return fmt.Errorf("vgirpc: protocol version range for %q: %w", name, err)
		}
		parsed[name] = rng
	}
	if len(parsed) == 0 {
		s.protocolVersionRanges = nil
	} else {
		s.protocolVersionRanges = parsed
	}
	return nil
}

// bindingWindow returns the major.minor window binding b accepts and
// advertises, and false when b is not gated (no version, or exempt).
func (s *Server) bindingWindow(b *protocolBinding) (VersionRange, bool) {
	if b == nil || !b.VersionSet || b.VersionExempt {
		return VersionRange{}, false
	}
	if rng, ok := s.protocolVersionRanges[b.Name]; ok {
		return rng, true
	}
	if b.DeclaredRange != nil {
		return *b.DeclaredRange, true
	}
	mm := MajorMinor{b.VersionParts[0], b.VersionParts[1]}
	return VersionRange{Min: mm, Max: mm}, true
}

// ProtocolVersions returns the VGI-Protocol-Versions advertisement: every
// gated protocol's window, "name=MIN-MAX[, name=MIN-MAX]..." in hosting order,
// primary first. Lists only protocols this server gates -- those declaring a
// protocol_version -- so reflection (exempt) and unversioned protocols are
// absent. Empty when nothing is gated. Advertised on every HTTP response and
// as vgi_rpc.protocol_versions metadata on reflection results,
// __transport_options__ replies and refusals.
func (s *Server) ProtocolVersions() string {
	if cached := s.protocolVersionsCache.Load(); cached != nil {
		return *cached
	}
	all := s.bindings()
	var windows []ProtocolWindow
	for _, name := range s.orderedBindingNames() {
		if rng, ok := s.bindingWindow(all[name]); ok {
			windows = append(windows, ProtocolWindow{Name: name, Range: rng})
		}
	}
	value := FormatProtocolVersions(windows)
	// The hosted set and every version are fixed once the server serves, so
	// the advertisement is computed once from then on.
	if s.sealed.Load() {
		s.protocolVersionsCache.Store(&value)
	}
	return value
}

// gate enforces binding's supported version window against one request's
// metadata. nil when the binding is not gated or the version is accepted.
//
// The client's own advertised windows (vgi_rpc.protocol_versions_client) are
// logged at debug and quoted in a refusal; they never cause one.
func (s *Server) gate(binding *protocolBinding, md map[string]string) *ProtocolVersionError {
	if binding == nil || !binding.VersionSet || binding.VersionExempt {
		return nil
	}
	clientVersion, present := md[MetaProtocolVersion]
	window, _ := s.bindingWindow(binding)
	// Exact equality with the declared version is the common case. Skip the
	// parsing and logging on every RPC then -- unless an override moved the
	// window off the declared version (a fixture impersonating an older
	// server), when the declared version itself is refused.
	if present && clientVersion == binding.Version &&
		window.Contains(MajorMinor{binding.VersionParts[0], binding.VersionParts[1]}) {
		return nil
	}
	clientVersions := md[MetaClientProtocolVersions]
	if clientVersions != "" && slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		slog.Debug("client protocol versions", "protocol", binding.Name, "client_versions", clientVersions,
			"server_id", s.serverID)
	}
	return gateVersion(binding.Name, window, clientVersion, present, clientVersions, s.ProtocolVersions())
}

// gateVersion checks one request's declared version against a binding's
// window, returning the refusal or nil.
//
// Takes the binding's name and window rather than reading the server's,
// because a server hosting several protocols has a window per binding and no
// single "server version". Every message names the protocol for the same
// reason. Message texts mirror the Python reference exactly
// (PROTOCOL_VERSIONS_V1, "Decisions" 6).
func gateVersion(
	protocolName string,
	window VersionRange,
	clientVersion string,
	present bool,
	clientVersions string,
	advertisement string,
) *ProtocolVersionError {
	serverWindow := "this server supports " + protocolName + " " + window.String()
	refusal := func(message, declared string) *ProtocolVersionError {
		return &ProtocolVersionError{
			Message:        message,
			Protocol:       protocolName,
			ClientVersion:  declared,
			ServerVersion:  window.String(),
			ServerVersions: advertisement,
			ClientVersions: clientVersions,
		}
	}
	if !present {
		return refusal(ErrorKindProtocolVersionUnsupported+": the client did not send vgi_rpc.protocol_version for "+
			protocolName+"; "+serverWindow+". This is either a vgi-rpc framework bug or a non-VGI "+
			"client connecting to a VGI worker.", "")
	}
	if !utf8.ValidString(clientVersion) {
		return refusal(ErrorKindProtocolVersionUnsupported+": the client sent a non-UTF-8 protocol_version for "+
			protocolName+"; "+serverWindow+".", "<undecodable>")
	}
	major, minor, _, err := parseSemver(clientVersion)
	if err != nil {
		return refusal(ErrorKindProtocolVersionUnsupported+": the client sent a malformed protocol_version "+
			pyRepr(clientVersion)+" for "+protocolName+" (expected canonical semver MAJOR.MINOR.PATCH); "+
			serverWindow+".", clientVersion)
	}
	client := MajorMinor{major, minor}
	if window.Contains(client) {
		return nil
	}
	speaks := "client speaks " + protocolName + " " + client.String()
	if supports, ok := ParseProtocolVersions(clientVersions)[protocolName]; ok {
		speaks += " (supports " + supports.String() + ")"
	}
	var direction string
	if client.Less(window.Min) {
		direction = "The client is too old (this server is too new for it). Upgrade the client, or use a " +
			"worker that supports " + protocolName + " " + client.String() + "."
	} else {
		direction = "The server is too old (the client is too new for it). Upgrade the worker, or use a " +
			"client that supports " + protocolName + " " + window.Max.String() + "."
	}
	return refusal(ErrorKindProtocolVersionUnsupported+": "+speaks+"; "+serverWindow+". "+direction, clientVersion)
}

// pyRepr quotes s the way Python's repr() quotes a str without quotes or
// backslashes in it, so refusal texts match the reference byte for byte.
func pyRepr(s string) string {
	if !strings.ContainsAny(s, "'\\") {
		return "'" + s + "'"
	}
	return strconv.Quote(s)
}

// SetIncludeTracebacks decides whether EXCEPTION batches carry the remote
// stack trace (log_extra traceback and frames).
//
// On by default, on every transport (WIRE_PROTOCOL.md §8): the DuckDB extension
// puts the remote traceback into the error its user sees, and omitting it hid
// chained causes. An operator who does not want stack traces to leave the
// process turns them off here, for the whole server -- every transport it
// answers on, never per transport. The exception type, message, code, kind and
// details are sent either way.
func (s *Server) SetIncludeTracebacks(enabled bool) {
	s.omitTracebacks = !enabled
}

// SetDebugErrors is [Server.SetIncludeTracebacks] under its older name.
func (s *Server) SetDebugErrors(enabled bool) {
	s.SetIncludeTracebacks(enabled)
}

// traceFor is the traceback setting for an error raised by method: included
// always, unless the operator turned them off. method names the call in the
// synthesized trace written for an error that carries no stack of its own.
func (s *Server) traceFor(method string) traceOpts {
	return traceOpts{include: !s.omitTracebacks, protocol: s.serviceName, method: method}
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
