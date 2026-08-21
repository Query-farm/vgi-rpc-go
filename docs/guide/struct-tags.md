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
| `struct` | Inline Arrow struct column, derived from the field's own struct | `vgirpc:"copy_from,struct"` |

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
| `struct` with `struct` | `Struct<...>` | inline nested column |

## Nullable Fields

Pointer types become nullable Arrow columns. A nil pointer serializes as an Arrow null:

```go
type Params struct {
    Name  *string `vgirpc:"name"`   // nullable string
    Count *int64  `vgirpc:"count"`  // nullable int
}
```

## Inline Structs

`struct` makes a field an **inline Arrow struct column** whose children are
derived from the field's own Go struct by these same tag rules, recursively:

```go
type CopyFromContext struct {
    Format         string `vgirpc:"format"`
    FilePath       string `vgirpc:"file_path"`
    ExpectedSchema []byte `vgirpc:"expected_schema"`
}

type BindRequest struct {
    // struct<format: utf8 not null, file_path: utf8 not null,
    //        expected_schema: binary not null>, nullable
    CopyFrom *CopyFromContext `vgirpc:"copy_from,struct"`
}
```

This is the opposite of `binary`/`ArrowSerializable`, which carries a nested
value as IPC stream *bytes* in a binary column. Use `struct` when the protocol
declares the field inline — a peer that validates its parameter contract with
`Schema.Equal` rejects `binary` where the protocol says `struct`.

A pointer field is a nullable struct column; a nil pointer serializes as an
Arrow null and decodes back to `nil`, so "absent" stays distinguishable from a
present-but-empty struct. Nesting is allowed up to 8 levels (a self-referential
type is an error, not a hang). The tag applies to the field's own type only: a
slice or map *of* structs is not supported.

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
