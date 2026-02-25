# HTTP Transport

`HttpServer` wraps a `Server` and serves RPC over HTTP. For background on available transports, see the [transports overview](https://vgi-rpc.query.farm/#transports) on the main vgi-rpc site.

```go
httpServer := vgirpc.NewHttpServer(server)
http.ListenAndServe(":8080", httpServer)
```

## URL Routing

All routes use the prefix `/vgi` (configurable):

| Route | Purpose |
|---|---|
| `POST /vgi/{method}` | Unary RPC call |
| `POST /vgi/{method}/init` | Stream initialization |
| `POST /vgi/{method}/exchange` | Exchange continuation |
| `POST /vgi/__describe__` | Introspection |

All request and response bodies use `Content-Type: application/vnd.apache.arrow.stream`.

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
httpServer := vgirpc.NewHttpServerWithKey(server, sharedKey)
```

## Token TTL

State tokens have a configurable time-to-live. Use `SetTokenTTL` to adjust:

```go
httpServer.SetTokenTTL(30 * time.Minute)
```

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
