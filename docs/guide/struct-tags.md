# Struct Tags

Method parameters are Go structs annotated with `vgirpc` struct tags. The tag format is:

```
`vgirpc:"wire_name[,option[,option...]]"`
```

## Options

| Option | Effect | Example |
|---|---|---|
| *(none)* | Field mapped by name, default Arrow type | `vgirpc:"name"` |
| `default=VALUE` | Use VALUE when the client omits the parameter | `vgirpc:"sep,default=-"` |
| `enum` | Arrow Dictionary (categorical string) | `vgirpc:"status,enum"` |
| `int32` | Arrow Int32 instead of Int64 | `vgirpc:"value,int32"` |
| `float32` | Arrow Float32 instead of Float64 | `vgirpc:"value,float32"` |
| `binary` | Serialize an `ArrowSerializable` as IPC bytes | `vgirpc:"point,binary"` |

## Type Mapping

Go types map to Arrow types as follows:

| Go type | Arrow type | Notes |
|---|---|---|
| `string` | `Utf8` | |
| `bool` | `Boolean` | |
| `int64` | `Int64` | default for integers |
| `int64` with `int32` | `Int32` | via tag option |
| `float64` | `Float64` | default for floats |
| `float64` with `float32` | `Float32` | via tag option |
| `string` with `enum` | `Dictionary(Utf8)` | categorical |
| `ArrowSerializable` with `binary` | `Binary` | embedded IPC stream |

## Nullable Fields

Pointer types become nullable Arrow columns. A nil pointer serializes as an Arrow null:

```go
type Params struct {
    Name  *string `vgirpc:"name"`   // nullable string
    Count *int64  `vgirpc:"count"`  // nullable int
}
```

## Examples

```go
type ConcatenateParams struct {
    Prefix    string `vgirpc:"prefix"`
    Suffix    string `vgirpc:"suffix"`
    Separator string `vgirpc:"separator,default=-"`
}

type EchoEnumParams struct {
    Status Status `vgirpc:"status,enum"`  // Status is a string type
}

type EchoPointParams struct {
    Point Point `vgirpc:"point,binary"`   // Point implements ArrowSerializable
}
```
