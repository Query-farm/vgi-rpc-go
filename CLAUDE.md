# CLAUDE.md

## Build & Test

All common tasks are available via `make`:

```bash
make build     # build all packages (root + otel, sentry, jwtauth, s3, gcs submodules)
make lint      # go build + go vet + staticcheck (root + otel, sentry, jwtauth)
make test      # build conformance worker, run Python conformance tests
make coverage  # run tests with Go coverage instrumentation
```

### Python dependency

The conformance tests are driven by the released `vgi-rpc` package from PyPI.
`make test` (and `coverage` / `leakcheck` / `race`) bootstraps a repo-local
`.venv` and installs it automatically, so a fresh clone needs no manual setup:

```bash
make test          # creates .venv on first run, then runs the suite
make venv          # create/refresh .venv without running tests
```

To install by hand, or to point at a checkout of `vgi-rpc-python` instead:

```bash
pip install "vgi-rpc[http,cli,external]>=0.20.0" pytest pytest-timeout
```

Override `PYTHON` to use an interpreter you manage yourself — an editable install of `vgi-rpc-python`, say. Supplying it on the command line or in the environment skips the `.venv` bootstrap entirely:

```bash
PYTHON=/path/to/python make test
```

`VGI_RPC_SPEC` overrides the installed requirement (default `vgi-rpc[http,cli,external]>=0.20.0`); the suite is verified against 0.25.0.

## Testing Policy

Do not add Go unit tests (`_test.go` files) to this module. The canonical test suite lives in the `vgi-rpc` PyPI package (`vgi_rpc.conformance._pytest_suite`). All correctness validation is done through the conformance test harness (`make test`).

**Exception — benchmarks.** `vgirpc/bench_test.go` and `vgirpc/bench_http_test.go` hold `Benchmark*` functions only, no `Test*` correctness tests. Go benchmarks can only live in `_test.go` files, and the Python harness cannot report `B/op` / `allocs/op`. Run them with:

```bash
go test -run XXX -bench . -benchmem -count=6 ./vgirpc/
```

Compare runs with `benchstat` (`go install golang.org/x/perf/cmd/benchstat@latest`). Timings on a loaded machine are unreliable — a contended run once showed a phantom 93% regression that an interleaved old-vs-new re-run proved was an 18% improvement. `allocs/op` and `B/op` are deterministic and trustworthy regardless of load; trust those first and only believe `sec/op` deltas from low-variance runs.

## CI

CI clones `vgi-rpc-python` at HEAD and installs from that checkout, so the Go
port is tested against unreleased upstream changes and regressions surface
before they ship. Local `make test` instead uses the released PyPI package —
so the two can legitimately disagree, and a CI-only failure usually means
upstream changed something not yet released. See `.github/workflows/ci.yml`.

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
    vgisentry "github.com/Query-farm/vgi-rpc-go/vgirpc/sentry"
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

`make race` builds the conformance worker with `go build -race` and runs the full 1049-test suite under it (~5 minutes; ~3-5× slower than `make test`). The pytest-timeout plugin is disabled for this target because the upstream `_pytest_suite.py` declares `pytestmark = pytest.mark.timeout(5)` at module scope, which fires on the slower instrumented worker even when individual tests pass. Use `make race` before cutting releases.

## Hot-path performance invariants

These are load-bearing: undoing them silently regresses throughput, and the
allocation counts are covered by the benchmarks above.

### Per-type reflection memoization

Deriving a struct's Arrow schema and parsing its `vgirpc` tags are pure
functions of the `reflect.Type`, so `vgirpc/types_cache.go` memoizes both in a
`sync.Map` keyed by type (`describeStruct`). `deserializeParams` runs on every
inbound request; before memoization it re-split every tag string and matched
columns with an O(fields × columns) scan.

**Schema pointer identity.** `structToSchema` returns the *shared* cached
`*arrow.Schema` rather than a fresh one. `arrow.Schema` is immutable, so this
is safe, and it is deliberate: the Arrow IPC writer requires an exact schema
pointer match, so sharing the pointer keeps that fast path hit. Do not "fix"
this by returning a copy.

Column lookup uses `resolveColumn`, which tries the field's own ordinal
position before falling back to a scan. Both paths are allocation-free.

### Pooled response codec writers

`vgirpc/http_compression.go` checks zstd/gzip writers out of a `sync.Pool`
keyed by (codec, level) and returns them on `Close`. Constructing a zstd
encoder per response allocates level-sized window tables and, at default
concurrency, spawns `GOMAXPROCS` goroutines — on the order of 21 MB per
request. Encoder concurrency is pinned to 1 because response bodies are fully
buffered before compression, so extra workers buy nothing and make each pooled
encoder far more expensive to hold. klauspost's `Encoder` is not
goroutine-safe; a request owns one for the duration of its write and never
shares it.

### Lazy state must be `sync.Once`

`HttpServer.InitPages` and `Server.ProtocolHash` are both reached from the
dispatch path and are guarded by `sync.Once`. Both previously used an
unsynchronized check-then-act: concurrent first requests raced, and
`InitPages` additionally panicked because `mux.HandleFunc` rejects a duplicate
pattern. `InitPages` is idempotent as a result. Any new lazily-initialized
field reachable from a handler needs the same treatment — the conformance
suite does not exercise concurrent first requests, so this class of bug does
not show up there.

### `DispatchInfo.RequestData` is conditional

`SerializeRequestBatch` re-encodes the whole request payload, so it only runs
when a `DispatchHook` is actually registered. With no hook installed,
`RequestData` is nil. See `http_unary.go`, `http_stream.go`, `server_serve.go`.

## Documentation verification

`make docs-verify` runs `tools/docverify`, which checks README.md, CLAUDE.md
and the whole `docs/` tree:

- **modules** — every `github.com/Query-farm/...` path resolves to a real
  package in this repo. This exists because every example once imported
<!-- docverify:ignore -->
  `github.com/Query-farm/vgi-rpc/vgirpc`, the *Python* reference repo, which
  is not a Go module — so every documented `go get` failed. (A line preceded
  by a `docverify:ignore` HTML comment is skipped, as that one is.)
- **compile** — every fenced `go` block that is a complete program builds
  against the local working tree, not the published module.
- **symbols** — every `vgirpc.X` / `vgiotel.X` reference in a `go` block is an
  exported symbol of that package. This covers the ~50 fragment blocks that
  are not complete programs and so cannot be compiled.
- **links** — every relative link and image path resolves on disk.

Run `make docs-verify` after changing any exported API or any documentation.
