# Streaming

vgi-rpc-go supports two streaming patterns: **producer** (server pushes data) and **exchange** (lockstep bidirectional). Both use the `OutputCollector` to emit batches and the `StreamResult` to carry state. For a high-level comparison with other RPC frameworks, see the [comparison table](https://vgi-rpc.query.farm/#comparison) on the main vgi-rpc site.

## Producer Streams

A producer stream is a server-driven flow of output batches initiated by a single request. The handler returns a `*StreamResult` containing a `ProducerState`:

```go
type counterState struct {
    Count   int
    Current int
}

func (s *counterState) Produce(ctx context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
    if s.Current >= s.Count {
        return out.Finish()  // signal end-of-stream
    }
    // ... build arrays ...
    if err := out.EmitArrays(arrays, numRows); err != nil {
        return err
    }
    s.Current++
    return nil
}
```

The server calls `Produce` in a lockstep loop: read one tick from the client, call `Produce`, flush all output, repeat. Each call must either emit exactly one data batch or call `out.Finish()`.

Register with `vgirpc.Producer` or `vgirpc.ProducerWithHeader`.

## Exchange Streams

An exchange stream processes client-sent input batches one at a time. The handler returns a `*StreamResult` containing an `ExchangeState`:

```go
type scaleState struct {
    Factor float64
}

func (s *scaleState) Exchange(ctx context.Context, input arrow.RecordBatch, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
    // ... process input, build output arrays ...
    return out.EmitArrays(arrays, numRows)
}
```

Each `Exchange` call must emit exactly one data batch. It must NOT call `out.Finish()` — the client controls stream termination.

Register with `vgirpc.Exchange` or `vgirpc.ExchangeWithHeader`.

## Dynamic Streams

A dynamic stream defers the choice between producer and exchange mode to runtime. The handler returns a `*StreamResult` whose `State` field implements either `ProducerState` or `ExchangeState` — the server inspects the concrete type to determine the mode.

```go
vgirpc.DynamicStreamWithHeader[P](server, "my_method", headerSchema,
    func(ctx context.Context, callCtx *vgirpc.CallContext, p P) (*vgirpc.StreamResult, error) {
        if someCondition {
            return &vgirpc.StreamResult{
                OutputSchema: outputSchema,
                State:        &myProducerState{},
                Header:       myHeader,
            }, nil
        }
        return &vgirpc.StreamResult{
            OutputSchema: outputSchema,
            State:        &myExchangeState{},
            Header:       myHeader,
        }, nil
    })
```

Register with `vgirpc.DynamicStreamWithHeader`. The `OutputSchema` and `InputSchema` are taken from the `StreamResult` at runtime rather than at registration time.

## OutputCollector

The `OutputCollector` enforces the one-data-batch-per-call rule and supports:

| Method | Description |
|---|---|
| `Emit(batch)` | Emit a pre-built RecordBatch |
| `EmitArrays(arrays, numRows)` | Build a batch from arrays using the output schema |
| `EmitMap(data)` | Build a batch from column name/value pairs |
| `Finish()` | Signal end-of-stream (producer only) |
| `ClientLog(level, message, extras...)` | Emit a log batch |

## StreamResult

The `StreamResult` returned by init handlers carries:

| Field | Description |
|---|---|
| `OutputSchema` | The Arrow schema for output batches |
| `State` | A `ProducerState` or `ExchangeState` |
| `InputSchema` | For exchange methods; nil for producers |
| `Header` | An optional `ArrowSerializable` value sent before data |

## Stream Headers

Both producer and exchange methods can return a header — an `ArrowSerializable` value sent as a separate IPC stream before the main data stream. Use `ProducerWithHeader` or `ExchangeWithHeader` to register:

```go
type MyHeader struct {
    TotalExpected int64  `arrow:"total_expected"`
    Description   string `arrow:"description"`
}

func (h MyHeader) ArrowSchema() *arrow.Schema { /* ... */ }
```

Set the `Header` field on `StreamResult` in your init handler:

```go
return &vgirpc.StreamResult{
    OutputSchema: outputSchema,
    State:        &myState{},
    Header:       MyHeader{TotalExpected: 100, Description: "batch export"},
}, nil
```
