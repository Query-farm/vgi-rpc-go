# CLAUDE.md

## Build & Test

All common tasks are available via `make`:

```bash
make build     # build all packages (root + vgirpc/otel sub-module)
make lint      # go build + go vet + staticcheck (root + otel)
make test      # build conformance worker, run Python conformance tests
make coverage  # run tests with Go coverage instrumentation
```

### Python dependency

The conformance tests require the `vgi-rpc` package from PyPI:

```bash
pip install "vgi-rpc[http,cli]>=0.1.13" pytest pytest-timeout
```

The Makefile defaults to `python3`. Override with `PYTHON=/path/to/python make test` if your `vgi-rpc` install lives in a different environment.

## Testing Policy

Do not add Go unit tests (`_test.go` files) to this module. The canonical test suite lives in the `vgi-rpc` PyPI package (`vgi_rpc.conformance._pytest_suite`). All correctness validation is done through the conformance test harness (`make test`).

## CI

CI installs `vgi-rpc` from PyPI (not a local checkout) for repeatable builds. See `.github/workflows/ci.yml`.
