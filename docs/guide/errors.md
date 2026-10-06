# Error Handling

## RpcError

`RpcError` represents a protocol-level error with a type and message:

```go
return "", &vgirpc.RpcError{
    Type:    "ValueError",
    Message: "parameter out of range",
}
```

The `Type` field uses Python exception class names by convention (e.g. `"ValueError"`, `"RuntimeError"`, `"TypeError"`). This ensures compatibility with Python clients that map error types to exception classes.

## ErrRpc Sentinel

Use `errors.Is(err, vgirpc.ErrRpc)` to check whether any error in a chain is an `*RpcError`:

```go
if errors.Is(err, vgirpc.ErrRpc) {
    // handle RPC-level error
}
```

## Error Types

| Type | Typical use |
|---|---|
| `ValueError` | Invalid parameter value |
| `TypeError` | Wrong parameter type or method type mismatch |
| `RuntimeError` | General server-side error |
| `AttributeError` | Unknown method name |
| `VersionError` | Protocol version mismatch |
| `SerializationError` | Failed to serialize result |

## Returning Errors from Handlers

Any handler can return an `*RpcError` to send a typed error to the client:

```go
vgirpc.Unary(server, "divide", func(_ context.Context, _ *vgirpc.CallContext, p DivideParams) (float64, error) {
    if p.B == 0 {
        return 0, &vgirpc.RpcError{
            Type:    "ValueError",
            Message: "division by zero",
        }
    }
    return p.A / p.B, nil
})
```

Non-`RpcError` errors returned from handlers are wrapped as `RuntimeError` automatically.

## The error model: code, kind, details

Every EXCEPTION batch carries three layers, adopted from gRPC's
`google.rpc.Status` (`WIRE_PROTOCOL.md` §8):

| Layer | Wire key | Go |
|---|---|---|
| Code | `vgi_rpc.error_code` | `vgirpc.Code` -- one of the sixteen gRPC codes minus `OK`, sent by name (`"UNAVAILABLE"`) |
| Reason | `vgi_rpc.error_kind` | an open, stable token a client branches on |
| Details | `vgi_rpc.error_details` | a JSON array from a fixed catalog: `ErrorInfo`, `RetryInfo`, `BadRequest`, `PreconditionFailure`, `QuotaFailure`, `ResourceInfo`, `Help`, `LocalizedMessage` |

The code is sent on every error -- `UNKNOWN` when the error is unclassified --
and all three are mirrored in `log_extra`. Return a `*vgirpc.StatusError` to
choose them:

```go
return &vgirpc.StatusError{
    Code:    vgirpc.CodeUnavailable,
    Kind:    "report_rebuilding",
    Message: "report is being rebuilt",
    Details: []vgirpc.ErrorDetail{vgirpc.RetryInfo{RetryDelaySeconds: 30}},
}
```

Any error type may instead implement `ErrorCode() vgirpc.Code`,
`ErrorKind() string` and `ErrorDetails() []vgirpc.ErrorDetail`; they are found
with `errors.As`, so wrapping with `%w` keeps the classification. A
protocol-defined detail goes in a `vgirpc.RawDetail` whose `@type` lives under
the protocol's own name. Each type may appear once, and the serialized array is
capped at 4 KiB: an array that breaks a rule or exceeds the cap is dropped
**whole** (code and kind are still sent). `vgirpc.ValidateErrorDetails` checks
an array up front.

Details never carry credentials, tokens or user data.

### On the client

A decoded `*RpcError` carries `Code` (`""` when the server predates the model,
which is not the same as `"UNKNOWN"`), `Kind` and `Details` (every object as
received, unknown types included), plus typed accessors that skip what they do
not know:

```go
var rpcErr *vgirpc.RpcError
if errors.As(err, &rpcErr) && rpcErr.IsRetryable() {
    wait := time.Second
    if ri, ok := rpcErr.RetryInfo(); ok {
        wait = time.Duration(ri.RetryDelaySeconds * float64(time.Second))
    }
    _ = wait // the caller decides whether to retry
}
```

`IsRetryable` follows the code: `UNAVAILABLE`, and `RESOURCE_EXHAUSTED` when it
carries `RetryInfo`. `ABORTED` means retry the whole operation at a higher
level. The client never retries an RPC error itself, because a method may not
be idempotent.

### Tracebacks

`Server.SetIncludeTracebacks` decides whether errors carry the Go stack trace.
They are included by default on every transport -- the DuckDB extension shows
the remote traceback to its user -- and `SetIncludeTracebacks(false)` turns
them off for the whole server, on every transport it serves. The type,
message, code, kind and details are sent either way. (`SetDebugErrors` is the
older name for the same setting.)
