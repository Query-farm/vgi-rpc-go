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
