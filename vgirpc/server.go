// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"io"
	"log/slog"
	"reflect"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

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
	methods         map[string]*methodInfo
	serverID        string
	serviceName     string
	protocolVersion string
	protocolHash    string
	dispatchHook    DispatchHook
	debugErrors     bool
	externalConfig  *ExternalLocationConfig
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

// SetProtocolVersion stores an operator-supplied free-form protocol-contract
// version string. Reported in access-log records as ``protocol_version``;
// complementary to (build) ``server_version``.
func (s *Server) SetProtocolVersion(v string) {
	s.protocolVersion = v
}

// ProtocolVersion returns the operator-supplied protocol version string.
func (s *Server) ProtocolVersion() string {
	return s.protocolVersion
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
