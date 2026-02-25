# API Reference

Full Go documentation is available on [pkg.go.dev](https://pkg.go.dev/github.com/Query-farm/vgi-rpc/vgirpc). For the protocol specification, see the [wire protocol](https://vgi-rpc.query.farm/wire-protocol) page.

## Registration Functions

These generic functions register RPC methods on a `Server`:

| Function | Description |
|---|---|
| `Unary[P, R](s, name, handler)` | Register a unary method returning a result |
| `UnaryVoid[P](s, name, handler)` | Register a unary method with no result |
| `Producer[P](s, name, outputSchema, handler)` | Register a producer stream |
| `ProducerWithHeader[P](s, name, outputSchema, headerSchema, handler)` | Register a producer with a header |
| `Exchange[P](s, name, outputSchema, inputSchema, handler)` | Register an exchange stream |
| `ExchangeWithHeader[P](s, name, outputSchema, inputSchema, headerSchema, handler)` | Register an exchange with a header |

Handler signatures:

```go
// Unary
func(ctx context.Context, callCtx *CallContext, params P) (R, error)

// UnaryVoid
func(ctx context.Context, callCtx *CallContext, params P) error

// Producer / Exchange
func(ctx context.Context, callCtx *CallContext, params P) (*StreamResult, error)
```

## Server

```go
func NewServer() *Server
```

| Method | Description |
|---|---|
| `SetServerID(id string)` | Set the server identifier included in response metadata |
| `RunStdio()` | Run the server loop on stdin/stdout |
| `Serve(r io.Reader, w io.Writer)` | Run the server on any reader/writer pair |
| `ServeWithContext(ctx context.Context, r io.Reader, w io.Writer)` | Run the server with a context for cancellation |

## HttpServer

```go
func NewHttpServer(server *Server) *HttpServer
func NewHttpServerWithKey(server *Server, signingKey []byte) *HttpServer
func RegisterStateType(v interface{})
```

| Method | Description |
|---|---|
| `SetTokenTTL(d time.Duration)` | Set state token maximum age |
| `ServeHTTP(w http.ResponseWriter, r *http.Request)` | Implements `http.Handler` |

## Stream Interfaces

### ProducerState

```go
type ProducerState interface {
    Produce(ctx context.Context, out *OutputCollector, callCtx *CallContext) error
}
```

### ExchangeState

```go
type ExchangeState interface {
    Exchange(ctx context.Context, input arrow.RecordBatch, out *OutputCollector, callCtx *CallContext) error
}
```

## StreamResult

Returned by producer/exchange init handlers:

```go
type StreamResult struct {
    OutputSchema *arrow.Schema
    State        interface{}      // ProducerState or ExchangeState
    InputSchema  *arrow.Schema    // exchange only; nil for producers
    Header       ArrowSerializable // optional header sent before data
}
```

## OutputCollector

| Method | Description |
|---|---|
| `Emit(batch arrow.RecordBatch) error` | Emit a pre-built RecordBatch |
| `EmitArrays(arrays []arrow.Array, numRows int64) error` | Build and emit a batch from arrays |
| `EmitMap(data map[string][]interface{}) error` | Build and emit a batch from column maps |
| `Finish() error` | Signal end-of-stream (producer only) |
| `Finished() bool` | Whether `Finish()` has been called |
| `ClientLog(level LogLevel, message string, extras ...KV) error` | Emit a log batch to the client |

## ArrowSerializable

```go
type ArrowSerializable interface {
    ArrowSchema() *arrow.Schema
}
```

## CallContext

```go
type CallContext struct {
    Ctx       context.Context
    RequestID string
    ServerID  string
    Method    string
    LogLevel  LogLevel
}
```

| Method | Description |
|---|---|
| `ClientLog(level LogLevel, msg string, extras ...KV)` | Record a log message for the client |

## RpcError

```go
type RpcError struct {
    Type      string
    Message   string
    Traceback string
    RequestID string
}
```

| Method | Description |
|---|---|
| `Error() string` | Returns error string |
| `Is(target error) bool` | Supports `errors.Is` |

**Sentinel:** `ErrRpc` — use with `errors.Is(err, vgirpc.ErrRpc)`

## Request

```go
type Request struct {
    Method    string
    Version   string
    RequestID string
    LogLevel  string
    Batch     arrow.RecordBatch
    Metadata  map[string]string
}
```

## Logging

### LogLevel

```go
type LogLevel string

const (
    LogException LogLevel = "exception"
    LogError     LogLevel = "error"
    LogWarn      LogLevel = "warn"
    LogInfo      LogLevel = "info"
    LogDebug     LogLevel = "debug"
    LogTrace     LogLevel = "trace"
)
```

### KV

```go
type KV struct {
    Key   string
    Value string
}
```

## Method Types

```go
type MethodType int

const (
    MethodUnary    MethodType = iota
    MethodProducer
    MethodExchange
)
```

## Batch Kinds

```go
type BatchKind int

const (
    BatchData            BatchKind = iota
    BatchLog
    BatchError
    BatchExternalPointer
    BatchShmPointer
    BatchStateToken
)
```

## Metadata Keys

| Constant | Value |
|---|---|
| `MetaMethod` | `vgi_rpc.method` |
| `MetaRequestVersion` | `vgi_rpc.version` |
| `MetaRequestID` | `vgi_rpc.request_id` |
| `MetaLogLevel` | `vgi_rpc.log_level` |
| `MetaLogMessage` | `vgi_rpc.log_message` |
| `MetaLogExtra` | `vgi_rpc.log_extra` |
| `MetaServerID` | `vgi_rpc.server_id` |
| `MetaStreamState` | `vgi_rpc.stream_state` |
| `MetaProtocolName` | `vgi_rpc.protocol_name` |
| `MetaDescribeVersion` | `vgi_rpc.describe_version` |
| `ProtocolVersion` | `"1"` |
| `DescribeVersion` | `"2"` |

## Wire Functions

| Function | Description |
|---|---|
| `ReadRequest(r io.Reader) (*Request, error)` | Read one IPC stream and parse the request |
| `WriteUnaryResponse(w, schema, logs, result, serverID, requestID)` | Write a unary response |
| `WriteErrorResponse(w, schema, err, serverID, requestID)` | Write an error response |
| `WriteVoidResponse(w, logs, serverID, requestID)` | Write a void response |
