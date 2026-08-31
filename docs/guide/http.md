# HTTP Transport

`HttpServer` wraps a `Server` and serves RPC over HTTP. For background on available transports, see the [transports overview](https://vgi-rpc.query.farm/#transports) on the main vgi-rpc site.

```go
httpServer := vgirpc.NewHttpServer(server)
http.ListenAndServe(":8080", httpServer)
```

## URL Routing

Routes use an empty prefix by default:

| Route | Purpose |
|---|---|
| `POST /{method}` | Unary RPC call |
| `POST /{method}/init` | Stream initialization |
| `POST /{method}/exchange` | Exchange continuation |
| `POST /__describe__` | Introspection |

All request and response bodies use `Content-Type: application/vnd.apache.arrow.stream`.

## Native Go client

`HttpClient` provides unary, producer, and typed exchange calls over these
routes. The stream schema is declared when a producer or exchange is opened;
each exchange input is checked against that schema before dispatch.

```go
client, err := vgirpc.NewHttpClient("https://rpc.example.com")
if err != nil {
    log.Fatal(err)
}
defer client.Close()

stream, err := client.OpenExchange(ctx, "transform", params,
    vgirpc.ClientStreamSchema{Input: inputSchema, Output: outputSchema})
if err != nil {
    log.Fatal(err)
}
defer stream.Close()
```

`Close` is local. Call `stream.Cancel(ctx)` for explicit best-effort remote
cancellation. A failed exchange continuation cannot be retried because the
server may already have advanced its opaque state token.

### Tailscale userspace networking

Route every client connection through a tailscaled SOCKS5 server with
proxy-side hostname resolution:

```go
client, err := vgirpc.NewHttpClient(
    "https://worker.example-tailnet.ts.net:9400",
    vgirpc.WithClientTCPProxy("socks5h://127.0.0.1:1055"),
)
```

Only SOCKS5 `NO AUTH` is supported. The proxy URL cannot contain credentials,
and this option cannot be combined with `WithClientHTTPClient`. The client does
not resolve the target hostname locally and never falls back to a direct
connection if proxy negotiation fails. One request context covers proxy
connection, negotiation, target connection, and the HTTP request.

## Request Compression

The server transparently decompresses request bodies sent with `Content-Encoding: zstd` or `gzip`. The Python vgi-rpc client compresses by default (level 3), picking zstd or gzip depending on whether `zstandard` is installed, so this is handled automatically.

## Response Compression

**Response compression is on by default, at zstd level 1.** Nothing needs to be called to get it; `SetCompressionLevel` on the `HttpServer` only changes the level, and `SetCompressionLevel(0)` turns compression off.

Level 1 rather than the more common level 3, and that is not a size/speed tradeoff: measured on an 8.41 MB Arrow response body, level 1 was **4.7× faster than level 3 and produced a smaller body**. It wins on both axes, so there is nothing to give up. Raise it only if a measurement on your own payloads says so.

The codec is negotiated per request from the client's stated preference order, reading `X-VGI-Accept-Encoding` first and then anything `Accept-Encoding` adds; the first codec the server can produce wins. VGI's own custom header leads because HTTP stacks routinely inject their own `Accept-Encoding` (cpp-httplib, the DuckDB engine's client, sends `deflate, gzip, br, zstd`), and because browser `fetch()` cannot set `Accept-Encoding` at all — it is a forbidden header name, so a WASM/browser client has no other way to ask for a codec.

`identity` is a recognised token in either header: list it ahead of the compressed codecs to explicitly ask for an uncompressed response (useful for benchmarking or for a proxy that must see the raw body). q-values are parsed off and ignored — order alone decides, so `identity;q=0` still opts out.

Which response header carries the result:

| Outcome | Response header |
|---|---|
| Codec offered on both accept headers, or on `Accept-Encoding` only | `Content-Encoding: <codec>` |
| Codec offered *only* on `X-VGI-Accept-Encoding` | `X-VGI-Content-Encoding: <codec>` |
| `identity` won, or no producible codec was offered | *(none — the body is uncompressed)* |

The custom response header exists for the same reason as the custom request header: a client that had to ask via `X-VGI-Accept-Encoding` is one whose fetch or proxy layer would auto-decode or mangle a standard `Content-Encoding`, so the response must not claim one.

## Capability Advertisement

`VGI-Supported-Encodings` lists the codecs the server can do in **both** directions — decode on requests and produce on responses — in server-preference order, excluding `identity`. It is emitted on every response, so `OPTIONS /health` works as a discovery probe.

The header is always present, and an **empty value is meaningful**: it means "this server speaks no compression", which is distinct from an absent header (a legacy server, which clients read as zstd-capable). A stock server advertises `VGI-Supported-Encodings: zstd, gzip`, because compression is on by default; only an explicit `SetCompressionLevel(0)` produces the present-but-empty value.

Note that one level gates both codecs: `SetCompressionLevel` is zstd-named but gzip reuses the same level (clamped into gzip's 1–9 range), so the producible set is all-or-nothing.

## State Tokens

HTTP is stateless, so exchange streams carry an HMAC-signed state token in batch custom metadata (`vgi_rpc.stream_state#b64`). The server serializes the `ExchangeState` via `encoding/gob`, signs it, and returns it to the client. The client sends the token back with each exchange request.

!!! important
    Call `vgirpc.RegisterStateType` for every concrete type used in your state (and any types they embed) before the first HTTP stream request:

    ```go
    func init() {
        vgirpc.RegisterStateType(&myExchangeState{})
    }
    ```

## Signing Key

By default, `NewHttpServer` generates a random 32-byte signing key. For multi-instance deployments, use `NewHttpServerWithKey` with a shared key:

```go
httpServer, err := vgirpc.NewHttpServerWithKey(server, sharedKey)
if err != nil {
    log.Fatal(err)
}
```

## Token TTL

State tokens have a configurable time-to-live. Use `SetTokenTTL` to adjust:

```go
httpServer.SetTokenTTL(30 * time.Minute)
```

## Concurrency Limits

`HttpServer` does not cap the number of concurrent streams or connections. Each producer request drives one lock-step transition, but a long-running `Produce` callback still holds its HTTP connection until that transition returns. An unbounded client population combined with slow callbacks can therefore pin batch buffers and connections.

vgi-rpc deliberately delegates this concern to your reverse proxy or load balancer. Recommended controls:

- **nginx**: `limit_conn` (per-IP or global), `limit_req` (request rate), `proxy_read_timeout` (cap producer duration).
- **Envoy**: `circuit_breakers.max_connections` and `max_pending_requests` on the upstream cluster, `route.timeout` for per-request deadlines.
- **HAProxy**: `maxconn` (frontend) and `rate-limit sessions`.
- **AWS ALB / GCP HTTPS LB**: target group connection limits and idle timeouts.

Configure the proxy timeout to accommodate the longest individual producer transition. Active producers automatically issue a fresh `/exchange` request after each transition.

If you cannot place a proxy in front of the server, wrap `HttpServer` with a middleware `http.Handler` that maintains a `chan struct{}` semaphore and rejects new requests with `503 Service Unavailable` once the cap is reached.

## Graceful Shutdown

`HttpServer` is an `http.Handler`, so the standard library's `http.Server.Shutdown(ctx)` is the right primitive. It stops accepting new connections and cancels the context of in-flight handlers; vgi-rpc's streaming handlers check the context between produce iterations and exit cleanly when it's cancelled.

```go
srv := &http.Server{Addr: ":8080", Handler: httpServer}

go func() {
    if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
        log.Fatal(err)
    }
}()

// Wait for SIGINT / SIGTERM
sigCh := make(chan os.Signal, 1)
signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
<-sigCh

// Give in-flight streams up to 30s to finish.
shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
if err := srv.Shutdown(shutdownCtx); err != nil {
    log.Printf("shutdown: %v", err)
}
```

A long-running producer transition that ignores its context will not exit until the shutdown deadline expires, at which point the connection is closed and the goroutine is left running until it returns. Handlers that perform their own long-running work should observe `ctx.Done()` themselves.

## Full Example

```go
package main

import (
    "context"
    "net/http"

    "github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// Stream state must be registered so the HTTP transport can round-trip it
// through a signed state token.
type myState struct {
    Remaining int
}

func (s *myState) Produce(ctx context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
    if s.Remaining <= 0 {
        return out.Finish()
    }
    s.Remaining--
    // EmitMap takes columns: each value is a slice, one entry per row.
    return out.EmitMap(map[string][]interface{}{"value": {int64(s.Remaining)}})
}

func init() {
    vgirpc.RegisterStateType(&myState{})
}

func main() {
    server := vgirpc.NewServer()
    // ... register methods ...

    httpServer := vgirpc.NewHttpServer(server)
    http.ListenAndServe(":8080", httpServer)
}
```
