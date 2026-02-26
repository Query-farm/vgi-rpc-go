# Observability

vgi-rpc-go provides a `DispatchHook` interface for observing every RPC call. The optional `vgirpc/otel` module supplies a ready-made hook for [OpenTelemetry](https://opentelemetry.io/) tracing and metrics.

## Dispatch Hooks

Register a hook on the server to be notified at the start and end of each dispatch:

```go
server.SetDispatchHook(myHook)
```

The hook interface:

```go
type DispatchHook interface {
    OnDispatchStart(ctx context.Context, info DispatchInfo) (context.Context, HookToken)
    OnDispatchEnd(ctx context.Context, token HookToken, info DispatchInfo, stats *CallStatistics, err error)
}
```

- `OnDispatchStart` is called before the handler runs. It may return a modified context (e.g. with a trace span) and an opaque `HookToken` that is passed to `OnDispatchEnd`.
- `OnDispatchEnd` is called after the handler completes with I/O statistics and any handler error.

Both calls are wrapped in `recover` — a panicking hook never crashes the server.

### DispatchInfo

`DispatchInfo` carries per-call metadata:

| Field | Description |
|---|---|
| `Method` | RPC method name |
| `MethodType` | `"unary"` or `"stream"` |
| `ServerID` | Server identifier from `SetServerID` |
| `RequestID` | Client-supplied request ID |
| `TransportMetadata` | All transport-level metadata (IPC custom metadata keys or HTTP headers including `traceparent`, `tracestate`, `remote_addr`, `user_agent`) |

### CallStatistics

`CallStatistics` tracks per-call I/O:

| Field | Description |
|---|---|
| `InputBatches` / `OutputBatches` | Number of Arrow batches |
| `InputRows` / `OutputRows` | Total row count |
| `InputBytes` / `OutputBytes` | Total buffer bytes |

## OpenTelemetry

The `vgirpc/otel` module implements `DispatchHook` with W3C trace context propagation, server spans, and RPC metrics.

### Installation

```bash
go get github.com/Query-farm/vgi-rpc/vgirpc/otel
```

### Usage

```go
import vgiotel "github.com/Query-farm/vgi-rpc/vgirpc/otel"

server := vgirpc.NewServer()
server.SetServiceName("my-service")
// ... register methods ...

vgiotel.InstrumentServer(server, vgiotel.DefaultConfig())
```

`DefaultConfig()` enables tracing, metrics, and exception recording using the global OTel SDK providers. Customize with `OtelConfig`:

```go
cfg := vgiotel.OtelConfig{
    TracerProvider:   myTracerProvider,
    MeterProvider:    myMeterProvider,
    EnableTracing:    true,
    EnableMetrics:    true,
    RecordExceptions: true,
    ServiceName:      "my-service",
    CustomAttributes: []attribute.KeyValue{
        attribute.String("deployment.environment", "production"),
    },
}
vgiotel.InstrumentServer(server, cfg)
```

### Trace Context Propagation

The hook automatically extracts `traceparent` and `tracestate` from `TransportMetadata` using the configured `propagation.TextMapPropagator`:

- **Stdio/pipe transport**: The Python client injects W3C trace headers as IPC batch custom metadata. These are passed through as `TransportMetadata`.
- **HTTP transport**: Standard `Traceparent` and `Tracestate` HTTP headers are captured into `TransportMetadata` by the server.

### Spans

Each RPC call produces a span named `vgi_rpc/{method}` with `SpanKindServer`. Span attributes:

| Attribute | Value |
|---|---|
| `rpc.system` | `vgi_rpc` |
| `rpc.service` | `OtelConfig.ServiceName` (defaults to `Server.ServiceName()` or `"GoRpcServer"`) |
| `rpc.method` | Method name |
| `rpc.vgi_rpc.method_type` | `"unary"` or `"stream"` |
| `rpc.vgi_rpc.server_id` | Server ID |
| `rpc.vgi_rpc.input_batches` | Input batch count |
| `rpc.vgi_rpc.output_batches` | Output batch count |
| `rpc.vgi_rpc.input_rows` | Input row count |
| `rpc.vgi_rpc.output_rows` | Output row count |
| `rpc.vgi_rpc.input_bytes` | Input buffer bytes |
| `rpc.vgi_rpc.output_bytes` | Output buffer bytes |
| `rpc.vgi_rpc.error_type` | Error type (on failure) |
| `net.peer.ip` | Remote address (HTTP only) |
| `user_agent.original` | User agent (HTTP only) |

### Metrics

| Metric | Type | Unit | Description |
|---|---|---|---|
| `rpc.server.requests` | Counter | `{request}` | Number of RPC requests |
| `rpc.server.duration` | Histogram | `s` | Duration of RPC requests |

Both metrics carry attributes: `rpc.system`, `rpc.service`, `rpc.method`, `rpc.vgi_rpc.method_type`, `status` (`"ok"` or `"error"`).
