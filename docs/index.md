---
description: "vgi-rpc-go: Go implementation of the vgi_rpc framework — Apache Arrow IPC-based RPC for high-performance data services."
hide:
  - navigation
  - toc
---

<div class="hero" markdown>

<div class="hero-logo" markdown>
![vgi-rpc-go logo](assets/logo-hero.png){ .hero-logo-img }
</div>

# vgi-rpc-go

Go implementation of the [vgi_rpc](https://vgi-rpc.query.farm/) framework — Apache Arrow IPC-based RPC for high-performance data services.

<p class="built-by">Built by <a href="https://query.farm">Query.Farm</a></p>

</div>

Define RPC methods with typed Go structs annotated with `vgirpc` struct tags. The framework derives [Apache Arrow](https://arrow.apache.org/) schemas from struct fields and provides server dispatch with automatic serialization/deserialization.

## Key Features

- **Unary RPCs** with typed parameters and results via struct tags
- **Producer streams** for server-initiated data flows
- **Exchange streams** for bidirectional batch processing
- **Dynamic streams** with runtime-determined producer/exchange mode
- **Stream headers** for metadata before the first data batch
- **Client-directed logging** at configurable levels
- **`context.Context` support** for cancellation and deadlines
- **HTTP transport** with signed state tokens and zstd decompression
- **ArrowSerializable** interface for complex nested types
- **OpenTelemetry support** via optional `vgirpc/otel` module (tracing + metrics)

## Three Method Types

### Unary

A single request produces a single response. The client sends parameters, the server returns a result.

```
Client  ──  add(a=2, b=3)  ──▸  Server
Client  ◂──     5.0         ──  Server
```

### Producer

The server pushes batches to the client until calling `out.Finish()`:

```
Client  ──  countdown(n=3)  ──▸  Server
Client  ◂──  {value: [3]}   ──  Server
Client  ◂──  {value: [2]}   ──  Server
Client  ◂──  {value: [1]}   ──  Server
Client  ◂──    [finish]     ──  Server
```

### Exchange

Lockstep bidirectional streaming — one request, one response, repeat:

```
Client  ──  transform(factor=2)  ──▸  Server
Client  ──    {value: [10]}      ──▸  Server
Client  ◂──   {result: [20]}     ──  Server
Client  ──    {value: [5]}       ──▸  Server
Client  ◂──   {result: [10]}     ──  Server
Client  ──      [close]          ──▸  Server
```

## Installation

```bash
go get github.com/Query-farm/vgi-rpc/vgirpc
```

## Quick Start

```go
package main

import (
    "context"
    "github.com/Query-farm/vgi-rpc/vgirpc"
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

## Next Steps

- Read the [Guide](guide/index.md) for struct tags, streaming, HTTP transport, and more
- Browse the [API Reference](api.md) for all exported types and functions
- Check out the [Examples](examples.md) for runnable programs
- Learn about the [wire protocol](https://vgi-rpc.query.farm/wire-protocol) and [benchmarks](https://vgi-rpc.query.farm/benchmarks) on the main vgi-rpc site
- See all [language implementations](https://vgi-rpc.query.farm/#languages) — Python, Go, TypeScript, C++

---

<p style="text-align: center; opacity: 0.7;">
  <a href="https://vgi-rpc.query.farm">vgi-rpc</a> &middot; <a href="https://query.farm">Query.Farm</a>
</p>
