# Native-client conformance status

vgi-rpc-go currently implements RPC servers and low-level intermediary wire
helpers. It does **not** expose a native RPC client, an HTTP client connection,
or a producer/exchange session API. Consequently, the reverse conformance lane
cannot honestly run here yet: manually assembling `POST /typed_exchange/init`
and `/exchange` requests with `net/http` would test a bespoke test client, not
the Go SDK's client serialization.

`WriteRequest`, `FindStreamTokens`, and the other helpers in
`wire_intermediary.go` do not close this gap. They intentionally provide
framing primitives for proxies and gateways and do not own HTTP capability
negotiation, stream lifecycle, retries, or typed request construction.

## Gate for a future native client

The first native HTTP exchange client must add a CI test against the Python
reference worker:

```bash
python -m vgi_rpc.conformance.client_worker --http 0
```

That test must use the client's public declared-schema API—not runtime value
inference—to open `typed_exchange`, send, and verify:

1. One row with every field null.
2. A zero-row batch retaining the exact declared schema.
3. A populated row containing `dictionary<int16, utf8>`,
   `timestamp[us, UTC]`, `decimal128(18, 4)`, nullable lists, and the nullable
   nested struct defined by the reference worker.

The Python worker rejects schema drift before its normal cast path, so a green
test proves the client put the declared field order, types, child fields, and
nullability on the wire. Until the native client exists, the repository's CI
continues to cover the supported direction: the Python client drives the Go
server through the shared conformance suite.
