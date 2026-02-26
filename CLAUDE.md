# CLAUDE.md

## Build & Test

All common tasks are available via `make`:

```bash
make build     # build all packages (root + vgirpc/otel sub-module)
make lint      # go build + go vet + staticcheck (root + otel)
make test      # build conformance worker, run Python conformance tests
make coverage  # run tests with Go coverage instrumentation
```

The Python venv at `/Users/rusty/Development/vgi-rpc/.venv` must be activated before running `make test` or `make coverage`:

```bash
source /Users/rusty/Development/vgi-rpc/.venv/bin/activate
```

## Testing Policy

Do not add Go unit tests (`_test.go` files) to this module. The canonical test suite lives in the Python `vgi_rpc` package at `/Users/rusty/Development/vgi-rpc`. All correctness validation is done through the conformance test harness (`make test`).
