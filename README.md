# vgi-rpc-go

Go implementation of the [vgi_rpc](https://github.com/Query-farm/vgi-rpc) framework -- an Apache Arrow IPC-based RPC protocol for high-performance data services.

## Install

```bash
go get github.com/Query-farm/vgi-rpc/vgirpc
```

## Quick Start

```go
package main

import (
    "context"
    "github.com/Query-farm/vgi-rpc/vgirpc"
)

type GreetParams struct {
    Name string `vgirpc:"name"`
}

func main() {
    server := vgirpc.NewServer()

    vgirpc.Unary(server, "greet", func(_ context.Context, ctx *vgirpc.CallContext, p GreetParams) (string, error) {
        return "Hello, " + p.Name + "!", nil
    })

    server.RunStdio()
}
```

## Features

- **Unary RPCs** with typed parameters and results via struct tags
- **Producer streams** for server-initiated data flows
- **Exchange streams** for bidirectional batch processing
- **Dynamic streams** with runtime-determined producer/exchange mode
- **Stream headers** for metadata before the first data batch
- **Client-directed logging** at configurable levels
- **`context.Context` support** for cancellation and deadlines
- **HTTP transport** with signed state tokens and zstd decompression
- **ArrowSerializable** interface for complex nested types
- **OpenTelemetry support** via optional `vgirpc/otel` module (tracing + metrics)

## API Overview

### Registration

```go
vgirpc.Unary[P, R](server, name, handler)
vgirpc.UnaryVoid[P](server, name, handler)
vgirpc.Producer[P](server, name, outputSchema, handler)
vgirpc.ProducerWithHeader[P](server, name, outputSchema, headerSchema, handler)
vgirpc.Exchange[P](server, name, outputSchema, inputSchema, handler)
vgirpc.ExchangeWithHeader[P](server, name, outputSchema, inputSchema, headerSchema, handler)
vgirpc.DynamicStreamWithHeader[P](server, name, headerSchema, handler)
```

### Transports

```go
server.RunStdio()                          // stdin/stdout
server.Serve(reader, writer)               // any io.Reader/Writer
server.ServeWithContext(ctx, reader, writer) // with context

httpServer := vgirpc.NewHttpServer(server) // HTTP
http.ListenAndServe(":8080", httpServer)
```

## Struct Tags

Method parameters are Go structs annotated with `vgirpc` struct tags. The tag format is:

```
`vgirpc:"wire_name[,option[,option...]]"`
```

### Options

| Option | Effect | Example |
|---|---|---|
| *(none)* | Field mapped by name, default Arrow type | `vgirpc:"name"` |
| `default=VALUE` | Use VALUE when the client omits the parameter | `vgirpc:"sep,default=-"` |
| `enum` | Arrow Dictionary (categorical string) | `vgirpc:"status,enum"` |
| `int32` | Arrow Int32 instead of Int64 | `vgirpc:"value,int32"` |
| `float32` | Arrow Float32 instead of Float64 | `vgirpc:"value,float32"` |
| `binary` | Serialize an `ArrowSerializable` as IPC bytes | `vgirpc:"point,binary"` |

### Nullable fields

Pointer types become nullable Arrow columns. A nil pointer serializes as an Arrow null:

```go
type Params struct {
    Name  *string `vgirpc:"name"`   // nullable string
    Count *int64  `vgirpc:"count"`  // nullable int
}
```

### Examples

```go
type ConcatenateParams struct {
    Prefix    string `vgirpc:"prefix"`
    Suffix    string `vgirpc:"suffix"`
    Separator string `vgirpc:"separator,default=-"`
}

type EchoEnumParams struct {
    Status Status `vgirpc:"status,enum"`  // Status is a string type
}

type EchoPointParams struct {
    Point Point `vgirpc:"point,binary"`   // Point implements ArrowSerializable
}
```

## Streaming

### Producer streams

A producer stream is a server-driven flow of output batches initiated by a single request. The handler returns a `*StreamResult` containing a `ProducerState`:

```go
type counterState struct {
    Count   int
    Current int
}

func (s *counterState) Produce(ctx context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
    if s.Current >= s.Count {
        return out.Finish()  // signal end-of-stream
    }
    // ... build arrays ...
    if err := out.EmitArrays(arrays, numRows); err != nil {
        return err
    }
    s.Current++
    return nil
}
```

The server calls `Produce` in a lockstep loop: read one tick from the client, call `Produce`, flush all output, repeat. Each call must either emit exactly one data batch or call `out.Finish()`.

Register with `vgirpc.Producer` or `vgirpc.ProducerWithHeader`.

### Exchange streams

An exchange stream processes client-sent input batches one at a time. The handler returns a `*StreamResult` containing an `ExchangeState`:

```go
type scaleState struct {
    Factor float64
}

func (s *scaleState) Exchange(ctx context.Context, input arrow.RecordBatch, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
    // ... process input, build output arrays ...
    return out.EmitArrays(arrays, numRows)
}
```

Each `Exchange` call must emit exactly one data batch. It must NOT call `out.Finish()` — the client controls stream termination.

Register with `vgirpc.Exchange` or `vgirpc.ExchangeWithHeader`.

### OutputCollector

The `OutputCollector` enforces the one-data-batch-per-call rule and supports:

- `Emit(batch)` — emit a pre-built RecordBatch
- `EmitArrays(arrays, numRows)` — build a batch from arrays using the output schema
- `EmitMap(data)` — build a batch from column name/value pairs
- `Finish()` — signal end-of-stream (producer only)
- `ClientLog(level, message, extras...)` — emit a log batch

### StreamResult

The `StreamResult` returned by init handlers carries:

- `OutputSchema` — the Arrow schema for output batches
- `State` — a `ProducerState` or `ExchangeState`
- `InputSchema` — for exchange methods; nil for producers
- `Header` — an optional `ArrowSerializable` value sent before data

### Stream headers

Both producer and exchange methods can return a header — an `ArrowSerializable` value sent as a separate IPC stream before the main data stream. Use `ProducerWithHeader` or `ExchangeWithHeader` to register:

```go
type MyHeader struct {
    TotalExpected int64  `arrow:"total_expected"`
    Description   string `arrow:"description"`
}

func (h MyHeader) ArrowSchema() *arrow.Schema { /* ... */ }
```

## ArrowSerializable

Types that implement the `ArrowSerializable` interface provide their own Arrow schema:

```go
type ArrowSerializable interface {
    ArrowSchema() *arrow.Schema
}
```

Fields are mapped to Arrow columns using `arrow` struct tags:

```go
type Point struct {
    X float64 `arrow:"x"`
    Y float64 `arrow:"y"`
}

func (p Point) ArrowSchema() *arrow.Schema {
    return arrow.NewSchema([]arrow.Field{
        {Name: "x", Type: arrow.PrimitiveTypes.Float64},
        {Name: "y", Type: arrow.PrimitiveTypes.Float64},
    }, nil)
}
```

**Serialization context matters:**

- At the method parameter/result level (with `vgirpc:"name,binary"` tag), the value is serialized as binary — an embedded IPC stream.
- When nested inside another `ArrowSerializable`, the value becomes an Arrow struct column.

## Observability

### Dispatch hooks

Register a `DispatchHook` to observe every RPC call (tracing, metrics, logging):

```go
server.SetDispatchHook(myHook)
```

### OpenTelemetry

The optional `vgirpc/otel` module provides a ready-made hook with W3C trace propagation, spans, and metrics:

```go
import vgiotel "github.com/Query-farm/vgi-rpc/vgirpc/otel"

server := vgirpc.NewServer()
// ... register methods ...
vgiotel.InstrumentServer(server, vgiotel.DefaultConfig())
```

Install separately:

```bash
go get github.com/Query-farm/vgi-rpc/vgirpc/otel
```

## HTTP Transport

`HttpServer` wraps a `Server` and serves RPC over HTTP. Request bodies may be zstd-compressed (`Content-Encoding: zstd`):

```go
httpServer := vgirpc.NewHttpServer(server)
http.ListenAndServe(":8080", httpServer)
```

### URL routing

All routes use the prefix `/vgi` (configurable):

| Route | Purpose |
|---|---|
| `POST /vgi/{method}` | Unary RPC call |
| `POST /vgi/{method}/init` | Stream initialization |
| `POST /vgi/{method}/exchange` | Exchange continuation |
| `POST /vgi/__describe__` | Introspection |

All request and response bodies use `Content-Type: application/vnd.apache.arrow.stream`.

### State tokens

HTTP is stateless, so exchange streams carry an HMAC-signed state token in batch custom metadata (`vgi_rpc.stream_state`). The server serializes the `ExchangeState` via `encoding/gob`, signs it, and returns it to the client. The client sends the token back with each exchange request.

**Important:** Call `vgirpc.RegisterStateType` for every concrete type used in your state (and any types they embed) before the first HTTP stream request:

```go
func init() {
    vgirpc.RegisterStateType(&myExchangeState{})
}
```

### Signing key

By default, `NewHttpServer` generates a random 32-byte signing key. For multi-instance deployments, use `NewHttpServerWithKey` with a shared key:

```go
httpServer := vgirpc.NewHttpServerWithKey(server, sharedKey)
```

## Error Handling

### RpcError

`RpcError` represents a protocol-level error with a type and message:

```go
return "", &vgirpc.RpcError{
    Type:    "ValueError",
    Message: "parameter out of range",
}
```

The `Type` field uses Python exception class names by convention (e.g. `"ValueError"`, `"RuntimeError"`, `"TypeError"`).

### ErrRpc sentinel

Use `errors.Is(err, vgirpc.ErrRpc)` to check whether any error in a chain is an `*RpcError`.

### Error types

| Type | Typical use |
|---|---|
| `ValueError` | Invalid parameter value |
| `TypeError` | Wrong parameter type or method type mismatch |
| `RuntimeError` | General server-side error |
| `AttributeError` | Unknown method name |
| `VersionError` | Protocol version mismatch |
| `SerializationError` | Failed to serialize result |

## Introspection

The `__describe__` endpoint returns a RecordBatch describing all registered methods. It is called automatically by the Python client's `describe()` method:

```python
from vgi_rpc import Client
client = Client(["./my-server"])
info = client.describe()
```

The response includes method names, types (unary/stream), parameter schemas, result schemas, defaults, and header information. The describe schema version is tracked by `vgirpc.DescribeVersion`.

## Reference

- Python reference implementation: [github.com/Query-farm/vgi-rpc](https://github.com/Query-farm/vgi-rpc)
- Go package documentation: `go doc github.com/Query-farm/vgi-rpc/vgirpc`
