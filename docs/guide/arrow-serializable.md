# ArrowSerializable

Types that implement the `ArrowSerializable` interface provide their own Arrow schema for custom serialization:

```go
type ArrowSerializable interface {
    ArrowSchema() *arrow.Schema
}
```

## Defining a Type

Fields are mapped to Arrow columns using `arrow` struct tags:

```go
type Point struct {
    X float64 `arrow:"x"`
    Y float64 `arrow:"y"`
}

func (p Point) ArrowSchema() *arrow.Schema {
    return arrow.NewSchema([]arrow.Field{
        {Name: "x", Type: arrow.PrimitiveTypes.Float64},
        {Name: "y", Type: arrow.PrimitiveTypes.Float64},
    }, nil)
}
```

## Serialization Context

The serialization format depends on where the type appears:

| Context | Serialization | Example tag |
|---|---|---|
| Method parameter/result (top-level) | Binary — embedded IPC stream | `vgirpc:"point,binary"` |
| Nested inside another `ArrowSerializable` | Arrow struct column | `arrow:"point"` |

### As a Method Parameter

When used as a method parameter with the `binary` tag option, the value is serialized as an embedded Arrow IPC byte stream:

```go
type EchoPointParams struct {
    Point Point `vgirpc:"point,binary"`
}
```

### Nested Inside Another Type

When one `ArrowSerializable` is a field of another, it becomes an Arrow struct column — no binary wrapping:

```go
type Line struct {
    Start Point `arrow:"start"`
    End   Point `arrow:"end"`
}

func (l Line) ArrowSchema() *arrow.Schema {
    pointFields := []arrow.Field{
        {Name: "x", Type: arrow.PrimitiveTypes.Float64},
        {Name: "y", Type: arrow.PrimitiveTypes.Float64},
    }
    return arrow.NewSchema([]arrow.Field{
        {Name: "start", Type: arrow.StructOf(pointFields...)},
        {Name: "end", Type: arrow.StructOf(pointFields...)},
    }, nil)
}
```
