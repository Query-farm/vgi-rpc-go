# Native HTTP client and conformance

`HttpClient` is the public blocking client for VGI-RPC over HTTP. It supports
unary calls, producer streams, and typed exchange streams. Exchange schemas are
declared explicitly, so zero-row and all-null batches keep their exact Arrow
types, child fields, and nullability.

```go
client, err := vgirpc.NewHttpClient("http://127.0.0.1:8080",
    vgirpc.WithClientProtocol("my.Service.v1"))
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

## Conformance in client role

The regression above is one test. The shared conformance suite is ~1400, and
until recently every one of them pointed the Python client at the **Go
server**. That leaves the Go client covered only by the server it ships with,
which is the one configuration that cannot validate a client: a server accepts
its own client's habits, and every accommodation it makes is invisible to
exactly that pair. A sibling port shipped a client that sent bare URL paths
carrying no routing key; it passed its own suite for weeks and produced 730
failures the first time it met the strict Python reference.

`conformance/cmd/vgi-rpc-conformance-client-driver` closes that gap. It is a
small executable speaking a newline-delimited JSON control protocol on
stdin/stdout — specified in the Python reference at
`tools/cross-port/specs/CLIENT_DRIVER_PROTOCOL.md` — which lets the Python
suite drive this port's client against any server.

```bash
make test-client-python   # Go client vs the Python REFERENCE server -- the gate
make test-client          # Go client vs the Go server -- a triage tool
```

Both run in CI. Only the first is the conformance claim; the second earns its
place differentially, because a client that passes its own server and fails the
reference has a client bug, while failing both means something more basic.

`make test-client-python` needs a `vgi-rpc-python` checkout: the reference
servers live in that repository's `tests/` directory, not in the published
wheel. Point `VGI_RPC_PYTHON_REPO` at one.

The driver reaches the transports this port's client has: `http`, `unix` and
`tcp`. There is no `stdio` or `shm` client here — the port serves both and
dials neither — so those are skipped rather than quietly mapped onto another
transport. Little wire coverage is lost: `unix` and `tcp` carry the same raw
Arrow IPC framing that `stdio` does, over a socket instead of a pipe.
