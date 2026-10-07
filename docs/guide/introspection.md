# Introspection

Introspection is `vgi_rpc.Reflection.v1`, a protocol co-hosted alongside the
application's own rather than a hardcoded method name. It is registered like
anything else:

```go
if err := vgirpc.RegisterReflection(server); err != nil {
	log.Fatal(err)
}
```

Two calls, both on that protocol:

- `list_protocols` — what this server hosts, with each protocol's version and
  hash. Cheap enough to decide with: a client that already holds a hash can skip
  the second call entirely.
- `describe(protocol)` — one protocol's full method list.

Reflection is **opt-in**: a server that never calls `RegisterReflection` does
not host it (the Python reference's `enable_describe` likewise defaults to
false), and a client asking such a server gets `ReflectionNotSupportedError`.

## Asking from a client

`vgirpc.ListProtocols` and `vgirpc.DescribeProtocol` ask over a connection the
caller already holds. The target is any client this package hands out —
`*HttpClient` (`NewHttpClient`, `NewIrohHTTPClient`) or `*TcpClient`
(`NewTcpClient`, `NewUnixClient`, `NewIrohClient`) — bound to *any* protocol
the server hosts. The client's own connection is reused and never closed:
over HTTP the calls share its `net/http` client, prefix, headers and auth;
over TCP, Unix and Iroh they share its one stateful connection, which the
server demultiplexes by each request's protocol key. Do not call them while a
stream holds that connection.

```go
hosted, err := vgirpc.ListProtocols(ctx, client) // or client.ListProtocols(ctx)
var noReflection *vgirpc.ReflectionNotSupportedError
switch {
case errors.As(err, &noReflection):
	// The server does not host vgi_rpc.Reflection.v1. The client still works.
case err != nil:
	return err
}
for _, p := range hosted {
	fmt.Println(p.Name, p.Version, p.Hash) // server order: primary first
}

desc, err := vgirpc.DescribeProtocol(ctx, client, "acme.App.v1")
```

- `ListProtocols(ctx, target) ([]HostedProtocol, error)` — one round trip.
  `HostedProtocol` is a value: `Name`, `Version`, `Hash`, `Deprecated`
  (default `false`), `DeprecationMessage` (default `""`) and `Features`
  (empty, never nil), in the server's order: application protocols in
  registration order, primary first, then the framework's own.
- `DescribeProtocol(ctx, target, name) (*ClientServiceDescription, error)` —
  lists first, then describes, so "no reflection" and "no such protocol" stay
  distinct. An unknown name is an ordinary `*RpcError` with
  `Kind == "protocol_not_supported"`.
- A server without reflection returns `*ReflectionNotSupportedError`. It embeds
  the server's `*RpcError` (so `Kind`, `Code`, `Message`, `RequestID` and
  `Details` are readable on it, and `errors.As` to `*RpcError` still matches)
  and covers `protocol_not_supported`, `method_not_implemented`,
  `UNIMPLEMENTED` and an older HTTP server's bare 404. No listing is ever
  inferred, and the connection stays usable.

The same calls exist as methods, `client.ListProtocols(ctx)` and
`client.DescribeProtocol(ctx, name)`. `client.Describe(ctx)` describes the
first application protocol without naming it.

## Response Contents

A description carries, per method:

- Method name and type (unary or stream, and for a stream its kind when the
  registration can state one)
- Parameter, result and header schemas, as serialized Arrow IPC
- Idempotency and deprecation metadata

Server identity (`server_id`, `server_version`) rides on `list_protocols`
instead: two processes serving one protocol must describe it identically, or the
description is not a property of the protocol.

## Protocol hash

Each protocol carries a `protocol_hash` — the SHA-256 of canonical JSON over
what Arrow *decodes to*, not over serialized IPC bytes, so the same protocol
hashes the same in every port. See `vgirpc.ComputeProtocolHash` and
`vgirpc.CanonicalDescription`, whose preimage bytes are what to diff when two
ports disagree.

## HTTP Access

Reflection is reached like any other co-hosted protocol — there is no reserved
route for it:

```
POST /vgi_rpc.Reflection.v1/list_protocols
POST /vgi_rpc.Reflection.v1/describe
```

## `__describe__` is retired

The reserved `POST /__describe__` endpoint is gone. The name is still
recognised, only to refuse it with a message naming this protocol and both of
its entry points — a client told merely "no such method" cannot tell "retired"
from "this server was built without introspection", and those need opposite
fixes.
