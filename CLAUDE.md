# CLAUDE.md

## Build & Test

All common tasks are available via `make`:

```bash
make build      # build all packages (root + otel, sentry, jwtauth, s3, gcs submodules)
make lint       # go build + go vet + staticcheck (root + otel, sentry, jwtauth)
make go-test    # Go unit tests (language-local only — see Testing Policy)
make test       # go-test, then build conformance worker + run Python conformance tests
make test-client        # the same suite driving THIS port's client, vs the Go server
make test-client-python # ... vs the Python reference server -- the client-role gate
make coverage   # run tests with Go coverage instrumentation
make cross-port # compare this port to the Python reference (see below)
make ci         # every gate in .github/workflows/ci.yml — run this before pushing
```

`make ci` exists because the other targets, run together, are still not the CI
gate list. Four gates are reachable through no other make target —
`staticcheck` at the version CI pins (`STATICCHECK_VERSION`, since releases add
and retire checks), the runner-driven `vgi-rpc-test` suite (the only place
`large_payload.echo_binary_over_int32_max` runs — pytest carries no
`large_payload` cases), the access-log spec check, and `cross-port`. A fifth
passes by *skipping*: `TestPythonNativeClientTypedExchange` no-ops unless
`VGI_RPC_PYTHON` names an interpreter, so `make go-test` has never run it. The
two client-role legs have their own targets, but nothing else runs them
together with the server-role suite they are meant to be read against.

### Client role (`make test-client-python`)

`make test` and everything below it point the *Python* client at the *Go*
server. That leaves the Go client covered by exactly one peer: the server it
ships with. Which is the one configuration that cannot validate a client, since
a server accepts its own client's habits and every accommodation it makes is
invisible to precisely that pair. The Rust port shipped a client that sent bare
URL paths carrying no routing key; green against its own server for weeks, 730
failures the first time it met the strict Python reference.

`conformance/cmd/vgi-rpc-conformance-client-driver` is how the shared suite
drives this port's client instead. It speaks the JSONL control protocol
specified in the reference's `tools/cross-port/specs/CLIENT_DRIVER_PROTOCOL.md`
and relays -- it decodes no values, resolves no external pointers, retries
nothing, and defaults neither the routing key nor the method name, because
anything it repaired on the client's behalf would turn a client defect into a
passing run.

The gate is `make test-client-python`, against the reference. `make test-client`
runs the same suite against the Go server and is a triage tool rather than a
gate: its value is differential, because a client that passes its own server and
fails the reference has a client bug, while failing both means something more
basic. Both need a `vgi-rpc-python` checkout -- the reference servers live in
that repository's `tests/`, not in the published wheel.

Transports: `http`, `unix` and `tcp`. This port serves `stdio` and `shm` and
dials neither, so the driver refuses them by name instead of substituting
another transport; little wire coverage is lost, since `unix` and `tcp` carry
the same raw Arrow IPC framing over a socket instead of a pipe.

### Cross-port drift (`make cross-port`)

Every other gate asks whether this port is self-consistent. This one asks the
question no gate inside this repository can: does this port still agree with
the Python reference? It runs the reference's own two tools —
`describe_diff.py`, which spawns `./conformance-worker`, asks it over
`vgi_rpc.Reflection.v1` for every protocol it hosts and compares
`protocol_hash` and then every field of every method against Python's; and
`identity_consistency.py`, which greps this port's source for the
`vgi_rpc.Identity.v1` constants and error kinds.

They ran only by hand until they were wired into CI, and in one day of running
them properly they found four ports describing `vgi_rpc.Reflection.v1` as
hosting zero methods and six ports logging six different wrong `protocol_hash`
values — with every one of those ports' own CI green the whole time.

Both need a `vgi-rpc-python` checkout, resolved through `VGI_RPC_PYTHON_REPO`
like the rest of the Python dependency below; `scripts/cross-port-check.sh`
builds the sibling layout the tools expect out of symlinks, so the checkout can
live anywhere. It refuses rather than skipping when there is no checkout, and
refuses when `identity_consistency` audited no files — that tool exits 0 when
it cannot find the repository at all, and an empty matrix must not read as
agreement.

### Python dependency

The conformance tests ship inside `vgi-rpc` itself, so **which `vgi-rpc` the
venv holds decides what this port is measured against**. That is not a detail.
A released wheel is the wrong reference whenever the wire is moving: PyPI's
v0.25.0 predates the multiservice work entirely, and running this suite against
it reports several hundred failures that say nothing about this port. Its
version number is also *higher* than some trees that do have the work, so the
number is no guide.

The canonical reference is a checkout of `vgi-rpc-python` on the branch carrying
the work under test. `make test` (and `coverage` / `leakcheck` / `race`)
bootstraps a repo-local `.venv` from `VGI_RPC_PYTHON_REPO`, installed editable
so the venv tracks whatever branch that tree is on — the same reference CI uses
(it clones `vgi-rpc-python` at HEAD).

```bash
make test          # creates .venv on first run, then runs the suite
make venv          # create/refresh .venv without running tests
```

`VGI_RPC_PYTHON_REPO` defaults to `~/Development/vgi-rpc-python` **if that
directory exists** — a `wildcard`, not a hardcoded path, so a machine without it
falls back to `VGI_RPC_SPEC` from PyPI and says so rather than failing. Point it
elsewhere, or empty it, from the environment or the command line:

```bash
VGI_RPC_PYTHON_REPO=/path/to/vgi-rpc-python make test   # another checkout
VGI_RPC_PYTHON_REPO= make test                          # released wheel
```

To install by hand, matching what the bootstrap and CI do:

```bash
pip install -e "/path/to/vgi-rpc-python[http,cli,external,conformance]" \
    pytest pytest-timeout "httpx2==2.9.1"
```

Override `PYTHON` to use an interpreter you manage yourself. Supplying it on the command line or in the environment skips the `.venv` bootstrap entirely:

```bash
PYTHON=/path/to/python make test
```

`VGI_RPC_SPEC` overrides the PyPI fallback requirement (default `vgi-rpc[http,cli,external]>=0.20.0`).

## Testing Policy

**Cross-language behaviour belongs in the shared conformance suite; Go tests cover what that suite cannot reach.**

The canonical correctness suite is the cross-language one in the `vgi-rpc` PyPI package (`vgi_rpc.conformance._pytest_suite`), run by `make test`. It is canonical for a reason: it drives *this* worker and the Python, Java, TypeScript and Rust ones from one set of assertions, so the ports cannot silently drift. Anything observable on the wire — dispatch semantics, framing, headers, error mapping, stream and transport behaviour — is validated there, and a Go-local test asserting the same thing is worse than useless: it passes while this port drifts away from the others.

So: **if a behaviour is reachable through RPC dispatch, test it in the conformance suite, not here.** When the suite can't reach it yet, extend the worker (`conformance/cmd/vgi-rpc-conformance-go`) so it can — `--http-pkce` was added exactly that way.

Go `_test.go` files are appropriate for the things a subprocess-driven, dispatch-only harness structurally cannot touch:

- **Library entry points for intermediaries** — `DecodeContentEncoding`, `WriteRequest` / `ReadRequest`, `FindStateToken`, `ReadUnaryResult`. These are called by proxies and gateways, never by a dispatching server, so no conformance run exercises them.
- **Client-side helpers** — the harness drives a *server* worker; `FetchOAuthResourceMetadata` and friends have no server surface at all.
- **Unexported internals** — `isExternalLocationBatch`, `maybeExternalizeBatch`, `resolveExternalLocation`, `buildHTTPCookies`. Reachable only in-package.
- **Pure language-local functions** — header/token parsing, codec negotiation, cookie construction, URL derivation.
- **Go-API contract guards** — "this symbol is still exported with this value" (`vgirpc/http_upload_url_test.go`). The *value* is cross-language; "is it exported from the Go package" is not.
- **Concurrency and allocation properties** — races, `sync.Once` init, pooling. `make race` covers the conformance workload; targeted races need a Go test.
- **Benchmarks.** `vgirpc/bench_test.go` and `vgirpc/bench_http_test.go` hold `Benchmark*` functions only, no `Test*`. Go benchmarks can only live in `_test.go` files, and the Python harness cannot report `B/op` / `allocs/op`.

A Go test that ports one of the *Python package's own unit tests* (`tests/test_wire.py`) is in scope. One that duplicates the *conformance* suite is not — delete it and rely on `make test`.

Run them with `make go-test` (also run by `make test` and by CI's lint job). Benchmarks:

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

- **Introspection** — `vgi_rpc.Reflection.v1`, an ordinary co-hosted protocol: `list_protocols` for what the server hosts, then `describe` for one protocol's methods. `__describe__` is retired and is refused with a message naming that replacement (see `retiredDescribeError`) rather than with a bare "no such method", which a stale client cannot tell from "this server was built without introspection". Each protocol's `protocol_hash` is the **canonical** digest (`ComputeProtocolHash`, `protocolhash.go`), taken over what Arrow decodes to rather than over serialized IPC bytes, and so comparable across ports — `WIRE_PROTOCOL.md §14`. The legacy byte-based digest taken over the old describe payload is gone with the payload.
- **Access log** — every dispatch fires `AccessLogHook` (when installed), writing one JSONL record per call. The record shape conforms to `vgi_rpc/access_log.schema.json` in the Python repo and validates under `vgi-rpc-test --access-log <path>`. `DispatchInfo` carries `Protocol`, `ProtocolHash`, `ProtocolVersion`, `RemoteAddr`, `RequestData`, `StreamID`, `Cancelled`, and `HTTPStatus`; the access-log emitter maps these to the spec field names. `Protocol` and `ProtocolHash` name the protocol that **owns the dispatched method**, not the server's primary, and both come from `Server.dispatchLabel(binding)` at every emit site — a record naming one protocol while carrying another's digest is well-formed, passes the schema, and decodes against the wrong description. Configure protocol-version via `Server.SetProtocolVersion(...)`.

The conformance worker accepts `--access-log <path>` anywhere on the CLI to enable JSONL emission, plus `--access-log-sample <rate>`, `--access-log-async`, `--access-log-queue-size <n>` and `--access-log-debug` (the CLI face of `AccessLogHook.SetDebug`).

Verify a change against the spec with the standalone runner, which validates every emitted record against the schema and exits non-zero if any fails:

```bash
make conformance-worker
~/…/vgi-rpc/.venv/bin/vgi-rpc-test \
  --cmd "$PWD/conformance-worker --access-log /tmp/go-al.jsonl --access-log-debug" \
  --access-log /tmp/go-al.jsonl --require-request-data
```

`--access-log-debug` + `--require-request-data` belong together and are not optional here: at INFO the worker omits `request_data`, and a log that never carries the field satisfies every rule governing it trivially, so the payload contract goes unchecked. CI runs exactly this command (`.github/workflows/ci.yml`, "Verify access log against the spec") — do not fall back to checking it by hand, which is how it drifted before.

`--cmd` only exercises the pipe path. The HTTP-only fields (`request_id`, `request_bytes`, `response_bytes`, `externalized_bytes`) need the worker started with `--http` / `--http-with-storage` and the runner pointed at it with `--url`.

#### Egress accounting

Three byte figures answer three different questions and must not be conflated: `request_bytes`/`response_bytes` are what crossed the wire (post-compression), `input_bytes`/`output_bytes` are logical Arrow buffers, and `externalized_bytes` never touches the HTTP body at all. A compressible result routinely shows a ~1000x gap between the first pair and the second.

`response_bytes` cannot be measured where a record is assembled — compression runs afterwards, in `compressResponseWriter.finish`. `HttpServer.ServeHTTP` therefore installs an `egressRecorder` (`accesslog_egress.go`) in the request context; `OnDispatchEnd` appends to it instead of writing, and the recorder emits once the body exists. A transport that installs no recorder keeps logging inline, so the immediate-vs-deferred choice is made in exactly one place. Externalised bytes are counted inside `maybeExternalizeBatchCtx`, the one function every upload passes through.

#### Trace correlation

`trace_id`/`span_id` come from `SetTraceContextProvider`, a pluggable accessor, because core carries no OpenTelemetry dependency. Wire it up once at startup with `vgirpc.SetTraceContextProvider(vgiotel.TraceContext)`. The accessor reads whatever span is *current* in the dispatch context, so an application-opened span correlates as readily as a framework-opened one. Malformed values (a dashed UUID, uppercase hex, one half of the pair) are dropped rather than emitted — a record carrying only one of the two fails the cross-language schema.

#### Claim redaction, sampling, async emission

- `RedactClaims` is the default policy: key-based, replaces values rather than dropping keys (which claims a credential carried is what an audit log is for), and covers credential-shaped names plus standard OIDC PII. `SetClaimRedactor` replaces it; `NoClaimRedaction` opts out. A redactor that panics **fails closed** — the claims are dropped, never emitted unredacted.
- `AccessLogHook.SetSampleRate` never samples out errors, decides deterministically per call (keyed on `stream_id`, then `request_id`, so every record of one stream shares its init's fate), and stamps `sample_rate` on every kept record. An out-of-range rate is rejected where it is configured, not at the first request.
- `AccessLogHook.SetAsync` moves writes to a goroutine behind a bounded queue that never blocks. Full means drop, and the next record through carries `dropped_records`. Opt-in: it trades the guarantee that a record on disk means the call completed. Call `Close()` at shutdown to drain.

#### Correlation id

`HttpServer.ServeHTTP` echoes the caller's `X-Request-ID` (bounded at 128 chars) or mints a 16-char hex one, set before dispatch so it rides every exit path including 401s and 404s. The shared suite only asserts the header is listed in `Access-Control-Expose-Headers`, which this port passed for a while without emitting anything — `vgirpc/accesslog_test.go` guards emission.

### Large payloads (>2 GiB)

The Python reference wraps every unbuffered transport writer in `_ExactWriter` (vgi-rpc 0.41.0), which loops on the returned count *and* clamps each call to 1 GiB. **This port needs no counterpart, and none was added.** Arrow IPC hands a whole column buffer to a single `Write` (`writeIPCPayload` in arrow-go), but the writers this package hands to `Serve` — `*os.File` for stdio/subprocess, `net.Conn` for unix and TCP — already loop and clamp at `maxRW = 1 << 30` inside `internal/poll`. Nothing here buffers, re-chunks, or trusts a returned count on the way there; the only custom `io.Writer`s (`shmSliceWriter`, `compressResponseWriter`, the HTTP wrappers) are in-memory.

That is a claim about the platform, so it was measured rather than assumed, on darwin/arm64:

- one raw `syscall.Write` of `1<<31 + 1` bytes fails with `EINVAL` on a pipe *and* on a TCP socket — so the hazard is real here and the test below is not vacuous;
- the same buffer through `(*os.File).Write`, a unix `net.Conn` and a TCP `net.Conn` returns `n == 2147483649, err == nil` with the peer receiving all of it.

The shared suite is what guards this going forward. `large_payload.echo_binary_4mib` catches a writer that never loops; `large_payload.echo_binary_over_int32_max` crosses `INT_MAX`, and is the only test reaching the size where the syscall stops accepting a whole buffer. Both are **required** as of vgi-rpc 0.42.0 — the earlier `VGI_RPC_CONFORMANCE_HUGE` opt-in is gone, on the grounds that a conformance test nobody runs enforces nothing, least of all one guarding a failure that presents as a hung process rather than an error.

```bash
vgi-rpc-test --cmd "$PWD/conformance-worker"   # also --unix / --tcp
```

Run it on macOS. Linux caps a single transfer at `0x7ffff000` and returns a short count that any correct loop absorbs, so a Linux-only CI cannot tell you whether this still holds. The test is scoped to pipe/unix/tcp; HTTP bodies take a different path with their own caps.

Two CI steps run `vgi-rpc-test`, and the split is deliberate. `_pytest_suite.py` has no `large_payload` tests, so the category is reachable only through the runner; "Run conformance suite (large payloads included)" is where the >2 GiB test executes, with no access log and a 600s budget. The access-log step excludes that one test by name, because at `--access-log-debug` the emitter base64s the whole request batch into the record — a 4 MiB payload already produces a 5,593,736-byte record, so 2 GiB would produce a ~2.9 GB JSONL line for Python to schema-validate. Do not merge the two steps back together.

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

`AccessLogHook` serializes writes through an internal mutex, so wrapping a non-thread-safe writer is safe. For high-volume workloads, call `hook.SetDebug(true)` only when replay/audit needs the full base64 `request_data` field — at INFO the field is replaced with `original_request_bytes` + `truncated: "payload_omitted"`, which typically halves record size.

`"payload_omitted"` is deliberately not `true`. `true` means genuine size-driven shedding; `"payload_omitted"` means nothing was lost to a cap, the emitter simply is not logging payloads at this level. Sharing one marker made it fire on essentially every record and left a consumer scanning for real data loss with nothing to filter on. This port enforces no per-record byte cap (rotation and truncation are the caller's, per the `lumberjack` pattern above), so it never emits `true`.

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

`HttpServer.InitPages` and `Server.canonicalHash` are both reached from the
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
