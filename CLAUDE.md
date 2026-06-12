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
pip install "vgi-rpc[http,cli,external]>=0.20.0" pytest pytest-timeout
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

### Access-log rotation

Unlike the Python reference (which builds rotation and record truncation into `vgi_rpc/logging_utils.py`), Go's `AccessLogHook` writes to any `io.Writer` and leaves rotation to the caller. The recommended pattern wraps `lumberjack.Logger`:

```go
import "gopkg.in/natefinch/lumberjack.v2"

writer := &lumberjack.Logger{
    Filename:   "/var/log/vgi-rpc/access.jsonl",
    MaxSize:    100,  // MB
    MaxBackups: 10,
    MaxAge:     14,   // days
    Compress:   true,
}
hook := vgirpc.NewAccessLogHook(writer, serverVersion)
server.SetDispatchHook(hook)
```

`AccessLogHook` serializes writes through an internal mutex, so wrapping a non-thread-safe writer is safe. For high-volume workloads, call `hook.SetDebug(true)` only when replay/audit needs the full base64 `request_data` field — at INFO the field is replaced with `original_request_bytes` + `truncated: true`, which typically halves record size.

### Sentry integration

`vgirpc/sentry/` is a separate Go module wrapping `getsentry/sentry-go`. It mirrors Python's `vgi_rpc/sentry.py` surface (error capture, scope tags, user mapping, optional transactions) and installs as a `DispatchHook`. Operators initialise the SDK themselves and then call `Instrument`:

```go
import (
    "github.com/getsentry/sentry-go"
    vgisentry "github.com/Query-farm/vgi-rpc/vgirpc/sentry"
)

sentry.Init(sentry.ClientOptions{Dsn: "https://..."})
server := vgirpc.NewServer()
vgisentry.Instrument(server, nil) // default config
```

Limitations vs Python:
- No auto-attach on server construction — call `Instrument` explicitly.
- No `record_params` / `tag_params` (per-call kwarg recording): vgi-rpc-go fires `OnDispatchStart` before parameter deserialisation, so the typed params struct isn't visible to the hook. The remaining surface (auth, claims, custom tags, error capture, transactions) is fully supported.
- `SetDispatchHook` holds at most one hook in core; `Instrument` replaces it. For composite usage (AccessLog + Sentry + OTel) wrap them in a caller-side multiplexing `DispatchHook` before registering.

### Race-detector pass

`make race` builds the conformance worker with `go build -race` and runs the full 866-test suite under it (~5 minutes; ~3-5× slower than `make test`). The pytest-timeout plugin is disabled for this target because the upstream `_pytest_suite.py` declares `pytestmark = pytest.mark.timeout(5)` at module scope, which fires on the slower instrumented worker even when individual tests pass. Use `make race` before cutting releases.
