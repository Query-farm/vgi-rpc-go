# Native HTTP client and conformance

`HttpClient` is the public blocking client for VGI-RPC over HTTP. It supports
unary calls, producer streams, and typed exchange streams. Exchange schemas are
declared explicitly, so zero-row and all-null batches keep their exact Arrow
types, child fields, and nullability.

```go
client, err := vgirpc.NewHttpClient("http://127.0.0.1:8080")
if err != nil {
    log.Fatal(err)
}
defer client.Close()

stream, err := client.OpenExchange(
    ctx,
    "typed_exchange",
    params,
    vgirpc.ClientStreamSchema{Input: schema, Output: schema},
)
if err != nil {
    log.Fatal(err)
}
defer stream.Close()

result, err := stream.Exchange(ctx, input)
if err != nil {
    log.Fatal(err)
}
defer result.Release()
```

The caller owns request batches and retains ownership after a call. Returned
`ClientBatch` values and batches returned by `Header` are caller-owned and must
be released. `Close` is local and idempotent. Use `Cancel(ctx)` when the server
must be notified; cancellation is best effort and marks the session finished.
Call `Close` afterward to release any locally buffered batches.

An exchange is poisoned as soon as a continuation request begins. If the
request or response fails, it cannot safely be retried with the old state token;
open a new stream instead. Request bodies and encoded and decoded response
bodies have independent configurable size limits.

## Reference-worker regression

The native-client CI test launches the Python reference worker and exercises
the public client API:

```bash
VGI_RPC_PYTHON=python go test ./vgirpc \
  -run '^TestPythonNativeClientTypedExchange$' -count=1 -v
```

It verifies all-null one-row and exact-schema zero-row batches, plus populated
dictionary, timestamp, decimal, nullable-list, and nested-struct values. It
also verifies that a plain-text HTTP 400 schema rejection is drained and a new
stream can be opened with the same client.
