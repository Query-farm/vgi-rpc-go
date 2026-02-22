# Introspection

The `__describe__` endpoint returns a RecordBatch describing all registered methods. It is called automatically by the Python client's `describe()` method:

```python
from vgi_rpc import Client
client = Client(["./my-server"])
info = client.describe()
```

## Response Contents

The describe response includes:

- Method names
- Method types (unary, producer, exchange)
- Parameter schemas (field names, Arrow types, defaults)
- Result schemas
- Header schemas (for streaming methods with headers)
- Server implementation metadata

## Schema Version

The describe schema version is tracked by `vgirpc.DescribeVersion` (currently `"2"`). The response metadata also includes:

| Metadata key | Value |
|---|---|
| `vgi_rpc.protocol_name` | Server implementation name (e.g. `"vgi-rpc-go"`) |
| `vgi_rpc.describe_version` | Schema version for the describe format |

## HTTP Access

Over HTTP, the describe endpoint is available at:

```
POST /vgi/__describe__
```

The request body should be an Arrow IPC stream with empty metadata (or the standard describe parameters).
