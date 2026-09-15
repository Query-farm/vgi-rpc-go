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

```python
from vgi_rpc.introspect import introspect
info = introspect(transport)          # list_protocols, then describe
```

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
