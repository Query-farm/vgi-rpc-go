# Examples

## Hello World

A minimal server with two unary methods — `greet` and `add`. Supports both stdio and HTTP transports.

```go
--8<-- "examples/hello_world/main.go"
```

Run it:

```bash
# Build and run with stdio transport
go build -o hello-world ./examples/hello_world/
./hello-world

# Or with HTTP transport
./hello-world --http
```

## Math Service

A more complete example with all four method types: unary (`add`, `multiply`), producer (`countdown`), and exchange (`running_sum`).

```go
--8<-- "examples/math_service/main.go"
```

### Python Client

The math service includes a Python client that demonstrates calling all method types across both transports:

```python
--8<-- "examples/math_service/client.py"
```

Run the demo:

```bash
# Build the server
go build -o math-service ./examples/math_service/

# Run the Python client (requires vgi_rpc package)
source /path/to/vgi-rpc/.venv/bin/activate
python examples/math_service/client.py
```
