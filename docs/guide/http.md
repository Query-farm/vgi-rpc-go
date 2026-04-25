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

## Request Compression

The server transparently decompresses request bodies sent with `Content-Encoding: zstd`. The Python vgi-rpc client enables zstd compression by default (level 3), so this is handled automatically.

## State Tokens

HTTP is stateless, so exchange streams carry an HMAC-signed state token in batch custom metadata (`vgi_rpc.stream_state`). The server serializes the `ExchangeState` via `encoding/gob`, signs it, and returns it to the client. The client sends the token back with each exchange request.

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

`HttpServer` does not cap the number of concurrent streams or connections. A long-running producer holds an HTTP connection open until it finishes (or the batch limit is reached), so an unbounded client population combined with slow producers can pin batch buffers and connections.

vgi-rpc deliberately delegates this concern to your reverse proxy or load balancer. Recommended controls:

- **nginx**: `limit_conn` (per-IP or global), `limit_req` (request rate), `proxy_read_timeout` (cap producer duration).
- **Envoy**: `circuit_breakers.max_connections` and `max_pending_requests` on the upstream cluster, `route.timeout` for per-request deadlines.
- **HAProxy**: `maxconn` (frontend) and `rate-limit sessions`.
- **AWS ALB / GCP HTTPS LB**: target group connection limits and idle timeouts.

When stream methods may run for minutes, configure the proxy timeout accordingly, or use `SetProducerBatchLimit` to force the client to issue fresh `/exchange` requests inside the proxy's per-request timeout window.

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

A long-running producer that ignores its context will not exit until the shutdown deadline expires, at which point the connection is closed and the goroutine is left running until it returns. Handlers that loop independently of vgi-rpc's produce loop should observe `ctx.Done()` themselves.

## Full Example

```go
package main

import (
    "net/http"
    "github.com/Query-farm/vgi-rpc/vgirpc"
)

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
