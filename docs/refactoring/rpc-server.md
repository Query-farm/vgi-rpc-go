# RPC Server Interfaces

## Purpose

This document defines the server-side contract for the `rpc` package only:

- public server registration API
- middleware model
- request and response writer contracts
- optional describe capability interfaces
- `__describe__` payload rules

Client-side APIs will be documented separately.

## Server

The naming is intentionally aligned with common Go `http.ServeMux` style.

```go
type Server struct { ... }

func NewServer(opts ...ServerOption) *Server

func (s *Server) Use(mw ...Middleware)
func (s *Server) HandleUnary(name string, h UnaryHandler)
func (s *Server) HandleProducer(name string, h StreamHandler)
func (s *Server) HandleExchange(name string, h StreamHandler)
func (s *Server) RegisterPlugin(p Plugin)

func (s *Server) HttpHandler() http.Handler
func (s *Server) StdioHandler() StdioHandler
```

Notes:

- The method name is defined only by `Handle...`.
- The method kind is defined only by the registration method.
- `Use(...)` appends RPC-aware middleware in registration order.
- Middleware order should follow the usual Go router mental model: first registered, first entered, last exited.
- `Server` is transport-neutral dispatch core.
- `HttpHandler()` exposes the HTTP transport binding.
- `StdioHandler()` exposes the stdio transport binding.
- The server owns these protocol endpoints:
  - `__describe__`
  - `/{method}`
  - `/{method}/init`
  - `/{method}/exchange`
  - `/{method}/cancel`
  - `__upload_url__/init` when externalized request uploads are enabled
- `/{method}/cancel` is best-effort cancellation for active producer and exchange streams.

## Protocol Version

The `rpc` package should expose a package-level protocol version constant.

```go
const ProtocolVersion = "..."

func RequestIDFromContext(ctx context.Context) (string, bool)
```

Notes:

- This constant is the authoritative protocol version string for the package.
- Incoming request batches must carry `vgi_rpc.request_version`.
- Server runtime must reject requests whose `vgi_rpc.request_version` does not match `ProtocolVersion`.
- `__describe__` metadata must report this same version through `vgi_rpc.request_version`.
- `RequestIDFromContext(...)` returns the effective request identifier previously injected by runtime into the request context.

## Middleware Model

Server extensibility should use a middleware chain.

```go
type Middleware func(BaseHandler) BaseHandler
type BaseHandler func(req *Request, w ResponseWriter) error
```

Notes:

- Middleware wraps VGI-aware dispatch, not generic raw HTTP composition.
- Authentication, tracing, metrics, logging, policy, rate limiting, and audit all fit naturally as middleware.
- Middleware must not consume the Arrow payload directly. The payload remains owned by `Request.Reader()`.

### `type StdioHandler`

```go
type StdioHandler interface {
    ServeStdio(ctx context.Context, rw io.ReadWriter) error
}
```

Notes:

- This is the transport adapter returned by `Server.StdioHandler()`.
- The exact stdio adapter shape can still evolve, but the core point is that stdio is a first-class transport binding, not an HTTP special case.

### `type ServerOption`

```go
type ServerOption interface {
    applyServer(*Server)
}

func WithExternalStorage(storage ExternalBatchService) ServerOption
```

Notes:

- Transport middleware is installed through `Server.Use(...)`, not constructor options.
- Constructor options remain for static server configuration only.
- Raw HTTP middleware belongs outside `rpc.Server`, at the outer `http.Handler` or mux layer where `rpc.Server` is mounted.
- `WithExternalStorage(...)` installs the optional external batch URL service used by transport bindings.

### `type Plugin`

```go
type Plugin interface {
    Register(server *Server)
}
```

Notes:

- A plugin configures the server directly.
- The common case is that `Register(...)` installs middleware via `server.Use(...)`.

## Core Handler Interfaces

All optional describe-related methods also receive `ctx`. This code is user-provided and may perform slow or cancelable work.

### `type UnaryHandler interface`

```go
type UnaryHandler interface {
    Unary(req *Request, w ResponseWriter) error
}
```

Notes:

- Unary is a single request/response operation.
- The request payload is read from `req.Reader()`.
- Static unary output schema is provided through `StaticOutputSchemaHandler` when available.

### `type StreamHandler interface`

```go
type StreamHandler interface {
    Init(req *Request, w ResponseWriter) error
    Stream(req *Request, w ResponseWriter) error
}
```

Notes:

- `StreamHandler` is used for both producer and exchange methods.
- The registration path (`HandleProducer` vs `HandleExchange`) determines stream kind semantics.
- `Init(...)` handles the init phase.
- `Stream(...)` handles continuation steps.
- For exchange methods, input payload is read from `req.Reader()`.
- For producer methods, continuation requests usually carry no business input batch.

### `type Canceler interface`

```go
type Canceler interface {
    Cancel(req *Request) error
}
```

Notes:

- Optional for `StreamHandler`.
- Invoked for best-effort `/{method}/cancel`.
- `req.State` identifies the active continuation chain.
- If the handler does not implement `Canceler`, runtime may acknowledge cancel and rely on timeout or TTL cleanup.

## Request Model

```go
type Request struct {
    Method    string
    Phase     CallPhase
    RequestID string
    LogLevel  string

    Metadata Metadata
    Header   arrow.RecordBatch
    State    []byte

    httpHeader  http.Header
    httpRequest *http.Request
    ctx         context.Context
    reader      arrow.RecordReader
}

func (r *Request) Context() context.Context
func (r *Request) Reader() arrow.RecordReader
func (r *Request) HTTPHeader() http.Header
func (r *Request) HTTPRequest() *http.Request
func (r *Request) IsFinal() bool
```

Notes:

- `Metadata`, `Header`, and `State` are transport-level VGI data.
- `Phase` is assigned by runtime and describes the current dispatch phase.
- `HTTPHeader()` exposes the original HTTP request headers.
- `HTTPRequest()` exposes the original HTTP request object for peer info, TLS info, auth, audit, URL, method, and similar transport concerns.
- Handlers must treat `HTTPRequest().Body` as runtime-owned and potentially already consumed.
- `HTTPRequest()` is therefore for inspection, not for reading the payload again.
- `Reader()` is the only supported way to consume Arrow request data.
- Method-specific payloads such as bind or init protocol requests are decoded from `Reader()` by the concrete handler implementation, not projected into generic `Request` fields.
- If the incoming payload is an external pointer batch, runtime resolves it through `ExternalBatchService.Read(...)` before exposing it through `Reader()`.
- `IsFinal()` is meaningful only for stream continuation phases where runtime has already determined final versus non-final step state.

### Wire invariants for incoming requests

- Every RPC request batch must carry:
  - `vgi_rpc.method`
  - `vgi_rpc.request_version`
- `vgi_rpc.request_id` and `vgi_rpc.log_level` are optional but should be preserved when present.
- Unary and init request payloads are expected to use a single-row batch when the schema is non-empty.
- Stream continuation requests may use zero-row control batches when the active binding requires them.
- Runtime should preserve all incoming custom metadata in `Request.Metadata`, even when only a subset is interpreted by the core transport layer.

## Response Writer

```go
type ResponseWriter interface {
    arrow.RecordWriter

    SetMetadata(md Metadata) error
    SetOutputSchema(schema *arrow.Schema) error
    SetHeader(record arrow.RecordBatch) error

    Log(level LogLevel, message string, extra Metadata) error

    NextState(state []byte) error
    Finalize() error
}
```

Notes:

- This is a unified writer surface for unary, init, and stream phases.
- Not every method is valid in every phase.
- Invalid phase usage must return an error.

Phase expectations:

- `SetOutputSchema(...)`
  - valid in `PhaseInit`
  - used to finalize dynamic output schema during init
- `SetHeader(...)`
  - valid in `PhaseInit`
  - runtime may return an error in other phases unless explicit protocol support is added later
- `SetMetadata(...)`
  - valid in all phases
- `Log(...)`
  - valid in all phases
  - runtime decides whether the log is emitted locally, sent to the client as a VGI log batch, or both
- `NextState(...)`
  - valid in `PhaseProduce` and `PhaseExchange`
- `Finalize()`
  - valid in stream phases

## Supporting Types

### `type Metadata`

```go
type Metadata map[string]string
```

### `type LogLevel`

```go
type LogLevel string

const (
    LogDebug     LogLevel = "DEBUG"
    LogInfo      LogLevel = "INFO"
    LogWarning   LogLevel = "WARNING"
    LogError     LogLevel = "ERROR"
    LogException LogLevel = "EXCEPTION"
)
```

### `type CallPhase`

```go
type CallPhase uint8

const (
    PhaseDescribe CallPhase = iota + 1
    PhaseUnary
    PhaseInit
    PhaseProduce
    PhaseExchange
    PhaseCancel
)
```

## External Batch URLs

The server should support an optional transport service for externalized Arrow IPC payloads.

This service has two responsibilities:

- vend upload and download URLs for large outbound or inbound payloads
- resolve incoming pointer batches into `arrow.RecordReader` values when a request references an external location

### `type ExternalBatchService`

```go
type ExternalBatchService interface {
    Capabilities(ctx context.Context) ExternalBatchCapabilities
    CreateUpload(ctx context.Context, req ExternalUploadRequest) (*ExternalUploadTarget, error)
    Read(ctx context.Context, locationURL string) (arrow.RecordReader, error)
}
```

Notes:

- `CreateUpload(...)` is used by the HTTP binding to implement `__upload_url__/init`.
- `Read(...)` is used when an incoming request contains a pointer batch instead of inline IPC bytes.
- `Read(...)` is transport-neutral. It may support signed HTTPS URLs, object storage URLs, or any other implementation-defined location scheme.
- `Read(...)` returns the resolved Arrow payload directly at Arrow abstraction level.
- A server without external batch support may leave this service unset.

### `type ExternalBatchCapabilities`

```go
type ExternalBatchCapabilities struct {
    Enabled          bool
    MaxRequestBytes  int64
    MaxUploadBytes   int64
    UploadURL        bool

    ExternalInput    bool
    ExternalOutput   bool
    Cancel           bool
    PointerSHA256    bool

    ContentEncodings []string
}
```

Notes:

- This is the server-side source of truth for HTTP capability discovery.
- `UploadURL` indicates whether `CreateUpload(...)` is supported.
- `ExternalInput` indicates whether the runtime can resolve incoming pointer batches through `Read(...)`.
- `ExternalOutput` indicates whether the runtime may externalize outgoing Arrow IPC payloads.
- `Cancel` indicates whether the HTTP binding exposes `/{method}/cancel`.
- `PointerSHA256` indicates whether pointer-batch checksum metadata is emitted and/or validated.
- `ContentEncodings` describes the encodings accepted for uploaded or fetched payloads, for example `identity` or `zstd`.

### `type ExternalUploadRequest`

```go
type ExternalUploadRequest struct {
    Count int
}
```

Notes:

- This matches the current VGI HTTP upload URL negotiation pattern, which asks for one or more upload targets up front.
- The shape can grow later if upload negotiation needs schema, content type, expiry hints, or auth scopes.

### `type ExternalUploadTarget`

```go
type ExternalUploadTarget struct {
    UploadURL   string
    DownloadURL string
    Headers     Metadata
    ExpiresAt   time.Time
}
```

Notes:

- `UploadURL` is where the client uploads raw IPC bytes.
- `DownloadURL` is what later appears in the pointer batch metadata as `vgi_rpc.location`.
- `Headers` allows the service to require signed upload headers if needed.
- The current VGI DuckDB HTTP client only consumes URLs, but this shape should preserve room for explicit headers.

## HTTP Capability Binding

`__capabilities__` is part of the HTTP transport binding, not the transport-neutral core server API.

It should describe at least:

- inline request size limits
- upload URL support
- external input pointer support
- external output pointer support
- accepted content encodings
- explicit cancel support
- pointer checksum support

Current DuckDB VGI HTTP clients are known to consume at least:

- `VGI-Max-Request-Bytes`
- `VGI-Upload-URL-Support`
- `VGI-Max-Upload-Bytes`

The richer capability model above is still worth defining now so that new clients do not need to guess which transport features are actually enabled on a given server.

### Current HTTP capability discovery

Current DuckDB VGI HTTP clients discover capabilities by sending `HEAD /` to the server base URL and reading response headers.

This means:

- capability discovery is part of the HTTP binding
- the current client does not require a dedicated `__capabilities__` route
- the HTTP binding should emit capability headers on the base route used for discovery

The server may still expose a dedicated `__capabilities__` route later as an additional convenience surface, but the compatibility requirement for DuckDB today is `HEAD /` plus headers.

### Current HTTP capability headers

The current DuckDB HTTP client is known to consume these headers:

- `VGI-Max-Request-Bytes`
- `VGI-Upload-URL-Support`
- `VGI-Max-Upload-Bytes`

Future clients may also consume additional headers derived from `ExternalBatchCapabilities`, but the three headers above are the current compatibility baseline.

## HTTP Binding Details

### Compression

The current DuckDB HTTP client expects the following HTTP compression behavior:

- Arrow IPC request and response bodies should use:
  - `Content-Type: application/vnd.apache.arrow.stream`
- request bodies may be sent with:
  - `Content-Encoding: zstd`
- the client may advertise response compression with:
  - `X-VGI-Accept-Encoding: zstd`
- compressed responses are identified with:
  - `X-VGI-Content-Encoding: zstd`

Notes:

- The HTTP binding should support `zstd` request decompression when enabled.
- The HTTP binding may compress Arrow IPC responses with `zstd` when the client advertises support.
- `ExternalBatchCapabilities.ContentEncodings` describes supported upload and download encodings, but the HTTP binding must also define the concrete header contract above.

### HTTP Arrow IPC error responses

When an HTTP request has already committed to an Arrow IPC response body, the HTTP binding may report an RPC failure by returning:

- `HTTP 200`
- `X-VGI-RPC-Error: true`
- Arrow IPC body containing the protocol error batch

This path exists because some HTTP clients discard or special-case 5xx response bodies.

Before Arrow IPC response handling begins, the binding may still return ordinary HTTP status failures.

### Request ID propagation

Request correlation should exist at both protocol and HTTP levels.

Rules:

- Incoming Arrow request metadata may carry `vgi_rpc.request_id`.
- Runtime should expose this value as `Request.RequestID`.
- The HTTP binding should echo the effective request identifier as:
  - `X-Request-ID`
- When the incoming request already carries `vgi_rpc.request_id`, the echoed `X-Request-ID` value should match it.
- When the server generates a request identifier locally, runtime should:
  - expose it as `Request.RequestID`
  - write it to outgoing Arrow response metadata as `vgi_rpc.request_id`
  - echo it through `X-Request-ID` on the HTTP response

Notes:

- This rule makes request correlation available to generic HTTP tooling, middleware, and tracing plugins.
- Middleware such as an OTEL plugin may use `Request.RequestID`, `traceparent`, and `tracestate` together for correlation and observability.

### Trace context propagation

The HTTP binding should preserve W3C trace context.

Rules:

- Incoming HTTP requests may carry:
  - `traceparent`
  - `tracestate`
- Runtime should preserve these values in:
  - `Request.Metadata`
  - `Request.Context()`
- Transport-aware middleware and plugins may extract them from either location.
- When the binding emits outgoing protocol-visible metadata, it should use:
  - `traceparent`
  - `tracestate`

Notes:

- This keeps tracing usable for both HTTP-aware middleware and transport-neutral runtime code.
- The tracing plugin model should not require direct access to raw HTTP headers once the request has been normalized.

### Stream init header path

The current DuckDB runtime has a dedicated parsing path for stream-init responses.

The HTTP and stdio bindings should preserve this model:

- init responses may emit a typed header batch before normal stream data begins
- init responses may emit log batches before the header batch
- an init failure may be represented as an error stream rather than ordinary data
- runtime must preserve enough phase information so that the client can distinguish stream-init header handling from normal unary or continuation response handling

`SetHeader(...)` is the server-side API surface for this capability, but the transport binding is responsible for serializing it in the init response shape expected by the client.

Additional rules:

- The init header batch is the first non-log data batch in the init response stream.
- If init fails after Arrow IPC response handling has begun, the transport may emit an error stream instead of a normal header batch.
- Clients must not confuse init-header parsing with normal data-batch parsing for unary or continuation phases.

## Optional Describe Capability Interfaces

### `type ArgumentedHandler interface`

```go
type ArgumentedHandler interface {
    Arguments(ctx context.Context) *arrow.Schema
}
```

Notes:

- Optional for unary, producer, and exchange handlers.
- Provides the parameter schema used by request decoding and `__describe__`.

### `type StaticInputSchemaHandler interface`

```go
type StaticInputSchemaHandler interface {
    InputSchema(ctx context.Context) *arrow.Schema
}
```

Notes:

- Optional for exchange handlers.
- Used only to enrich `__describe__` when exchange input schema is stable ahead of time.
- Runtime source of truth still comes from the client bind or request payload.

### `type StaticOutputSchemaHandler interface`

```go
type StaticOutputSchemaHandler interface {
    OutputSchema(ctx context.Context) *arrow.Schema
}
```

Notes:

- Optional for unary, producer, and exchange handlers.
- Used when output schema is stable and known before invocation or init.
- If not implemented, output schema may be finalized dynamically during init.

### `type HandlerDescriber interface`

```go
type HandlerDescriber interface {
    Doc(ctx context.Context) string
    HasHeader(ctx context.Context) bool
    HeaderSchema(ctx context.Context) *arrow.Schema
    IsVoid(ctx context.Context) bool
}
```

Notes:

- Optional for unary, producer, and exchange handlers.
- Consolidates describe-only metadata in one place.
- Does not control runtime request decoding or execution semantics.
- `HeaderSchema(...)` may return nil when the header schema is not statically known.
- `IsVoid(ctx)` is meaningful for both unary and stream methods, because some stream methods may consume input without returning data rows.

## Server-side Describe Rules

Server `__describe__` generation should use:

- registration name from `HandleUnary`, `HandleProducer`, and `HandleExchange`
- method kind from the registration path
- `ArgumentedHandler.Arguments(ctx)` when implemented
- `StaticInputSchemaHandler.InputSchema(ctx)` when implemented
- `StaticOutputSchemaHandler.OutputSchema(ctx)` when implemented
- `HandlerDescriber` methods when implemented
- otherwise the runtime fills missing values from method-kind defaults

`__describe__` describes all registered methods in a single response, not one method per request.

## `__describe__` Contract

### Top-level response

`__describe__` returns an Arrow IPC stream whose first data batch contains one row per registered method.

Response metadata must include:

- `vgi_rpc.protocol_name`
- `vgi_rpc.request_version`
- `vgi_rpc.describe_version`
- `vgi_rpc.server_id` when configured

### Describe batch fields

The protocol-level describe record should contain the following fields for every method:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `name` | `string` | yes | registered method name |
| `method_type` | `string` | yes | `unary` or `stream` |
| `doc` | `string` nullable | yes | human-readable method documentation |
| `has_return` | `bool` | yes | whether the method yields a value or result stream |
| `params_schema_ipc` | `binary` | yes | IPC-serialized Arrow schema for unary or init parameters |
| `input_schema_ipc` | `binary` nullable | yes | IPC-serialized Arrow schema for exchange input when statically describable |
| `result_schema_ipc` | `binary` nullable | yes | IPC-serialized Arrow schema for output when statically describable |
| `param_types_json` | `string` nullable | yes | JSON object of parameter names to logical types |
| `param_defaults_json` | `string` nullable | yes | JSON object of parameter defaults |
| `has_header` | `bool` | yes | whether the method may emit a typed init header batch |
| `header_schema_ipc` | `binary` nullable | yes | IPC-serialized Arrow schema for header batch |
| `is_exchange` | `bool` nullable | yes | `true` for exchange, `false` for producer, `null` for unary |
| `param_docs_json` | `string` nullable | yes | JSON object of parameter documentation |

Notes:

- `input_schema_ipc` is part of the protocol contract even if some legacy implementations still omit it.
- `result_schema_ipc` and `input_schema_ipc` may be null when the schema is only known after init.
- `method_type` stays coarse (`unary` or `stream`), while `is_exchange` disambiguates producer versus exchange inside stream methods.
- `param_defaults_json` and `param_docs_json` should be derived from `arrow.Field.Metadata` on the argument schema.
- The current DuckDB VGI runtime does not appear to depend on `__describe__` for its core execution flow. At present it is mainly an introspection and tooling surface.

### Parameter metadata rules

When `ArgumentedHandler.Arguments(ctx)` returns a schema, the server should use field metadata to enrich `__describe__`.

Reserved parameter field metadata keys:

- `vgi.param_type`
- `vgi.param_default`
- `vgi.param_doc`

Rules:

- `param_types_json`
  - uses `field.Metadata["vgi.param_type"]` when present
  - otherwise falls back to a type string derived from the Arrow field type
- `param_defaults_json`
  - built from `field.Metadata["vgi.param_default"]`
  - values are interpreted as JSON literals, not plain strings
  - include only fields that actually define the metadata key
- `param_docs_json`
  - built from `field.Metadata["vgi.param_doc"]`
  - include only fields that actually define the metadata key
- partial field coverage is allowed
- clients must treat these JSON objects as advisory overlays
- `params_schema_ipc` remains the authoritative full parameter contract

### Fill rules by method kind

#### Unary

- `method_type = "unary"`
- `doc` comes from `HandlerDescriber.Doc(ctx)` when implemented; otherwise null
- `has_return = false` only when `HandlerDescriber.IsVoid(ctx) == true`
- otherwise `has_return = true`
- `params_schema_ipc` comes from `ArgumentedHandler.Arguments(ctx)` when implemented, otherwise zero-field schema
- `input_schema_ipc = null`
- `result_schema_ipc` comes from `StaticOutputSchemaHandler.OutputSchema(ctx)` when implemented; otherwise null
- `is_exchange = null`
- `has_header` comes from `HandlerDescriber.HasHeader(ctx)` when implemented; otherwise false
- `header_schema_ipc` comes from `HandlerDescriber.HeaderSchema(ctx)` when implemented; otherwise null

#### Producer

- `method_type = "stream"`
- `doc` comes from `HandlerDescriber.Doc(ctx)` when implemented; otherwise null
- `has_return = false` only when `HandlerDescriber.IsVoid(ctx) == true`
- otherwise `has_return = true`
- `params_schema_ipc` comes from `ArgumentedHandler.Arguments(ctx)` when implemented, otherwise zero-field schema
- `input_schema_ipc = null`
- `result_schema_ipc` comes from `StaticOutputSchemaHandler.OutputSchema(ctx)` when implemented; otherwise null because final output schema is init-time dynamic
- `is_exchange = false`
- `has_header` comes from `HandlerDescriber.HasHeader(ctx)` when implemented; otherwise false
- `header_schema_ipc` comes from `HandlerDescriber.HeaderSchema(ctx)` when implemented; otherwise null

#### Exchange

- `method_type = "stream"`
- `doc` comes from `HandlerDescriber.Doc(ctx)` when implemented; otherwise null
- `has_return = false` only when `HandlerDescriber.IsVoid(ctx) == true`
- otherwise `has_return = true`
- `params_schema_ipc` comes from `ArgumentedHandler.Arguments(ctx)` when implemented, otherwise zero-field schema
- `input_schema_ipc` comes from `StaticInputSchemaHandler.InputSchema(ctx)` when implemented; otherwise null
- `result_schema_ipc` comes from `StaticOutputSchemaHandler.OutputSchema(ctx)` when implemented; otherwise null because final output schema is init-time dynamic
- `is_exchange = true`
- `has_header` comes from `HandlerDescriber.HasHeader(ctx)` when implemented; otherwise false
- `header_schema_ipc` comes from `HandlerDescriber.HeaderSchema(ctx)` when implemented; otherwise null

### Dynamic schema rules

- If output schema is only known after `Init(...)`, `result_schema_ipc` should be null in `__describe__`.
- If exchange input schema is client-defined and only known from the incoming init or bind request, `input_schema_ipc` should be null in `__describe__`.
- `SetOutputSchema(...)` remains the runtime mechanism that finalizes dynamic output schema.
- Absence of static schema in `__describe__` must not block method invocation.

## Log Batch Wire Rules

Server-side logging is exposed through `ResponseWriter.Log(...)`, but transport bindings must serialize these events using VGI log-batch rules.

Current DuckDB VGI clients recognize a log batch as:

- a zero-row Arrow batch
- carrying at least:
  - `vgi_rpc.log_level`
  - `vgi_rpc.log_message`

Additional metadata:

- `vgi_rpc.log_extra`
  - optional JSON object
  - for `EXCEPTION`, current DuckDB code expects fields such as:
    - `traceback`
    - `exception_type`

Rules:

- `Log(...)` should produce a protocol-visible log batch when the active transport forwards logs to the client.
- `LogException` is a log severity only; it does not replace returning an `error`.
- Bindings may also record logs locally in addition to emitting protocol log batches.
- `EXCEPTION` log batches should use `vgi_rpc.log_extra` for structured exception details such as traceback and exception type.

## Pointer Batch Wire Rules

The current protocol uses zero-row pointer batches to externalize Arrow IPC payloads.

Current pointer-batch metadata keys:

- `vgi_rpc.location`
  - required for external payload references
- `vgi_rpc.stream_state#b64`
  - optional continuation state token
- `vgi_rpc.location.sha256`
  - optional payload checksum

Rules:

- When an incoming request batch carries `vgi_rpc.location`, runtime should resolve the external Arrow IPC payload through `ExternalBatchService.Read(...)` before exposing `Request.Reader()`.
- When `vgi_rpc.stream_state#b64` is present, runtime must preserve it as the continuation state for the active request.
- When `ExternalBatchCapabilities.PointerSHA256` is enabled and `vgi_rpc.location.sha256` is present, runtime should validate the referenced payload before exposing it to the handler.
- When checksum validation is not enabled, the field may still be preserved in metadata for downstream tooling.
- Pointer batches are transport control batches. Handlers should normally see the resolved Arrow reader, not the unresolved pointer representation.

## Externalized Response Rules

Servers may externalize outgoing Arrow IPC payloads as pointer batches when the active binding and capabilities allow it.

Rules:

- Externalized responses may be used for unary, producer, or exchange results.
- When a response payload is externalized, the transport emits a pointer batch instead of the inline Arrow payload.
- The pointer batch should use:
  - `vgi_rpc.location`
  - optionally `vgi_rpc.location.sha256`
  - optionally `vgi_rpc.stream_state#b64` for continuation-aware stream responses
- `ExternalBatchCapabilities.ExternalOutput` indicates whether this behavior is enabled.
- The decision to externalize may depend on response size, transport limits, and server policy.

Notes:

- Handlers still write normal Arrow records through `ResponseWriter`.
- Externalization is a transport concern performed after handler output has been produced.
- This keeps handler code Arrow-first while allowing large results to move through object storage or signed URLs.

## Upload URL Negotiation Rules

`__upload_url__/init` is part of the HTTP binding for large request payloads.

Current DuckDB HTTP clients are known to consume response rows containing:

- `upload_url`
- `download_url`
- `expires_at`

Rules:

- These three fields are the compatibility baseline.
- `ExternalUploadTarget.Headers` is a future-compatible extension for clients that can honor explicit upload headers later.
- The current DuckDB client does not require explicit upload headers today.
- Uploaded payloads are raw Arrow IPC bytes, optionally compressed according to the active HTTP binding and capability rules.

## Runtime Notes

- In the current DuckDB VGI runtime, stream headers are used at init time and read through dedicated stream-header parsing paths.
- `has_header` in `__describe__` is descriptive metadata, not the mechanism that enables runtime header parsing.
- In the current DuckDB VGI runtime, DML and write-like flows may either return rows (`RETURNING`) or behave effectively as no-row streams, so `IsVoid(ctx)` is meaningful for stream methods as well.
- Producer continuation requests commonly use zero-row control batches.
- Exchange continuation requests may use inline data batches or zero-row pointer batches that resolve to external Arrow IPC payloads.

## Error Semantics

Handlers may both emit logs through `ResponseWriter.Log(...)` and return an `error`.

These are distinct mechanisms:

- `Log(...)` records protocol-visible log events
- `error` terminates the current call or phase with failure

### `LogException`

`LogException` is a log severity, not a substitute for returning an error.

Rules:

- `Log(LogException, ...)` may be used to emit an exception-level diagnostic event
- it does not by itself guarantee that the current call fails
- a handler that wants the call to fail must still return an `error`

This keeps the server-side contract explicit and avoids hidden failure semantics inside logging.

### Returned `error`

When a handler returns a non-nil `error`, runtime should terminate the current call and map the failure to the active transport binding.

Transport-neutral rule:

- unary/init/stream phases fail by returning `error`
- runtime is responsible for serializing that failure into the protocol form expected by the current binding

### Protocol error batches versus transport-level HTTP errors

For VGI bindings that already have an active Arrow IPC response body:

- runtime should prefer protocol error batches
- this is the normal path for unary and streaming RPC failures after Arrow response setup has begun

For HTTP requests that fail before an Arrow IPC response can be started:

- runtime may still return a plain HTTP error status
- examples:
  - malformed HTTP request
  - authentication failure
  - body too large
  - unsupported media type
  - invalid route or unknown method

Practical rule:

- once runtime has committed to a VGI Arrow IPC response, failures should be encoded as protocol error batches
- before that point, transport-native HTTP errors are still acceptable

### Middleware failures

- middleware may return an `error` before the target handler is invoked
- the same transport mapping rules apply
- auth and policy middleware may still choose transport-native HTTP status codes when failure happens before VGI response handling begins

## Cancellation Rules

- The client may call `POST /{method}/cancel` with the current continuation state.
- Runtime constructs a continuation `Request`.
- If the handler implements `Canceler`, runtime calls `Cancel(req)`.
- Cancel remains best-effort:
  - delivery is not guaranteed
  - handler cleanup on timeout or TTL is still required

## Server-side Audit

### Covered

- transport-neutral server core with HTTP and stdio bindings
- unary, producer, and exchange registration
- middleware chain
- unified request model
- unified response writer model
- package-level protocol version constant
- mandatory request metadata invariants:
  - `vgi_rpc.method`
  - `vgi_rpc.request_version`
- request ID propagation:
  - `vgi_rpc.request_id`
  - `Request.RequestID`
  - `RequestIDFromContext(...)`
  - `X-Request-ID`
- trace context propagation:
  - `traceparent`
  - `tracestate`
- `__describe__` payload model
- log batch wire rules
- pointer batch wire rules
- upload URL negotiation baseline
- best-effort cancellation
- externalized request and response payload support
- HTTP compression binding:
  - `Content-Type: application/vnd.apache.arrow.stream`
  - `Content-Encoding: zstd`
  - `X-VGI-Accept-Encoding: zstd`
  - `X-VGI-Content-Encoding: zstd`
- HTTP Arrow IPC error binding:
  - `HTTP 200`
  - `X-VGI-RPC-Error: true`

### Covered With Current-Client Compatibility Rules

- HTTP capability discovery through `HEAD /`
- capability headers:
  - `VGI-Max-Request-Bytes`
  - `VGI-Upload-URL-Support`
  - `VGI-Max-Upload-Bytes`
- stream init header path
- zero-row control batches for producer continuation
- zero-row pointer batches for externalized exchange continuation

### Intentionally Outside `rpc` Core

- CORS policy and `OPTIONS` browser middleware behavior
- outer HTTP mux composition
- catalog semantics
- bind/init payload schemas for DuckDB-specific higher-level APIs
- retry policy, circuit breaking, and opinionated resiliency behavior
