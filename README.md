# vgi-rpc-go

Go implementation of the [vgi_rpc](https://github.com/Query-farm/vgi-rpc) framework -- an Apache Arrow IPC-based RPC protocol for high-performance data services.

## Install

```bash
go get github.com/Query-farm/vgi-rpc-go/vgirpc
```

## Quick Start

```go
package main

import (
    "context"
    "github.com/Query-farm/vgi-rpc-go/vgirpc"
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
- **Stream headers** for metadata before the first data batch
- **Client-directed logging** at configurable levels
- **`context.Context` support** for cancellation and deadlines
- **HTTP transport** with signed state tokens for stateless exchange
- **ArrowSerializable** interface for complex nested types

## API Overview

### Registration

```go
vgirpc.Unary[P, R](server, name, handler)
vgirpc.UnaryVoid[P](server, name, handler)
vgirpc.Producer[P](server, name, outputSchema, handler)
vgirpc.ProducerWithHeader[P](server, name, outputSchema, headerSchema, handler)
vgirpc.Exchange[P](server, name, outputSchema, inputSchema, handler)
vgirpc.ExchangeWithHeader[P](server, name, outputSchema, inputSchema, headerSchema, handler)
```

### Transports

```go
server.RunStdio()                          // stdin/stdout
server.Serve(reader, writer)               // any io.Reader/Writer
server.ServeWithContext(ctx, reader, writer) // with context

httpServer := vgirpc.NewHttpServer(server) // HTTP
http.ListenAndServe(":8080", httpServer)
```

