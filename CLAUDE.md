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

## Cross-language wire alignment

This port tracks `vgi-rpc-python` for wire compatibility. Two surfaces matter:

- **`__describe__`** — `DescribeVersion = "4"`. The response batch is the slim 8-column schema (`name`, `method_type`, `has_return`, `params_schema_ipc`, `result_schema_ipc`, `has_header`, `header_schema_ipc`, `is_exchange`). Python-flavoured columns (`doc`, `param_types_json`, `param_defaults_json`, `param_docs_json`) are not on the wire. The response's `arrow.Metadata` carries `vgi_rpc.protocol_hash` — a SHA-256 hex digest over the canonical describe payload, computed by `computeProtocolHash` to mirror Python's `compute_protocol_hash` byte-for-byte. Within-port stable; cross-port byte equality is *not* guaranteed because Arrow IPC schema bytes vary across language Arrow libraries.
- **Access log** — every dispatch fires `AccessLogHook` (when installed), writing one JSONL record per call. The record shape conforms to `vgi_rpc/access_log.schema.json` in the Python repo and validates under `vgi-rpc-test --access-log <path>`. `DispatchInfo` carries `Protocol`, `ProtocolHash`, `ProtocolVersion`, `RemoteAddr`, `RequestData`, `StreamID`, `Cancelled`, and `HTTPStatus`; the access-log emitter maps these to the spec field names. Configure protocol-version via `Server.SetProtocolVersion(...)`.

The conformance worker accepts `--access-log <path>` anywhere on the CLI to enable JSONL emission.
