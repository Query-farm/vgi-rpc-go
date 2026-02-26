# Guide

This guide covers the core concepts of `vgi-rpc-go` — from struct tags and streaming to HTTP transport and error handling. For protocol-level details, see the [wire protocol specification](https://vgi-rpc.query.farm/wire-protocol) on the main vgi-rpc site.

## Topics

- **[Struct Tags](struct-tags.md)** — map Go struct fields to Arrow columns with options for defaults, enums, and type overrides
- **[Streaming](streaming.md)** — producer and exchange patterns, OutputCollector, StreamResult, and stream headers
- **[ArrowSerializable](arrow-serializable.md)** — implement custom Arrow schemas for complex types
- **[HTTP Transport](http.md)** — serve RPC over HTTP with signed state tokens
- **[Error Handling](errors.md)** — RpcError, ErrRpc sentinel, and standard error types
- **[Observability](observability.md)** — dispatch hooks and OpenTelemetry instrumentation
- **[Introspection](introspection.md)** — the `__describe__` endpoint for service discovery

## Quick Overview

### Registration

```go
vgirpc.Unary[P, R](server, name, handler)
vgirpc.UnaryVoid[P](server, name, handler)
vgirpc.Producer[P](server, name, outputSchema, handler)
vgirpc.ProducerWithHeader[P](server, name, outputSchema, headerSchema, handler)
vgirpc.Exchange[P](server, name, outputSchema, inputSchema, handler)
vgirpc.ExchangeWithHeader[P](server, name, outputSchema, inputSchema, headerSchema, handler)
vgirpc.DynamicStreamWithHeader[P](server, name, headerSchema, handler)
```

### Transports

```go
server.RunStdio()                          // stdin/stdout
server.Serve(reader, writer)               // any io.Reader/Writer
server.ServeWithContext(ctx, reader, writer) // with context

httpServer := vgirpc.NewHttpServer(server) // HTTP
http.ListenAndServe(":8080", httpServer)
```
