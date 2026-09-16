"""Run Python conformance tests against the Go conformance worker."""
import contextlib
import os
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, Protocol

try:
    # vgi-rpc 0.40.1 moved its HTTP client dependency from httpx to httpx2,
    # so a checkout of the reference no longer drags httpx in. Only the
    # readiness poll below uses it; both spellings serve.
    import httpx2 as httpx
except ModuleNotFoundError:  # pragma: no cover - older reference / PyPI build
    import httpx
import pytest

from vgi_rpc.conformance import ConformanceService
from vgi_rpc.http import http_connect
from vgi_rpc.introspect import ServiceDescription
from vgi_rpc.log import Message
from vgi_rpc.rpc import SubprocessTransport, _RpcProxy, tcp_connect, unix_connect

GO_WORKER = os.environ.get(
    "GO_CONFORMANCE_WORKER",
    str(Path(__file__).parent / "conformance-worker"),
)

# Two axes, and only one combination of them is the conformance claim.
#
# ROLE   "server" (default) points the *Python* client at the *Go* server --
#        what this file has always done.  "client" turns it around and drives
#        the Go client through conformance/cmd/vgi-rpc-conformance-client-driver.
# SERVER which conformance server the suite talks to: "go" (default) or the
#        Python reference.
#
# ROLE=client SERVER=python is the gate.  A green client run against the Go
# server proves only that the two halves of this port agree with each other:
# a server accepts its own client's habits, so every accommodation it makes is
# invisible to exactly that pair.  The Rust port shipped a client sending bare
# URL paths with no routing key, green against its own server for weeks and 730
# failures the first time it met the reference.
ROLE = os.environ.get("VGI_CONFORMANCE_ROLE", "server")
SERVER = os.environ.get("VGI_CONFORMANCE_SERVER", "go")

# The Python reference, when SERVER=python.  Its conformance servers live in
# the *repository's* tests/ directory, not in the published wheel, so this
# needs a checkout rather than an install.  Same env-var names the Rust port
# uses, so one CI recipe configures either.
_REF_REPO = Path(os.environ.get("VGI_RPC_PYTHON_REPO") or Path.home() / "Development" / "vgi-rpc-python")
_REF_PYTHON = os.environ.get("VGI_RPC_PYTHON") or sys.executable
_PY_TESTS = Path(os.environ.get("VGI_PY_TESTS_DIR") or _REF_REPO / "tests")
_PY_SERVE_HTTP = str(_PY_TESTS / "serve_conformance_http.py")
_PY_SERVE_STRICT = str(_PY_TESTS / "serve_conformance_http_strict.py")
_PY_SERVE_AUTH = str(_PY_TESTS / "serve_conformance_http_auth.py")
_PY_SERVE_PIPE = str(_PY_TESTS / "serve_conformance_pipe.py")
_PY_SERVE_UNIX = str(_PY_TESTS / "serve_conformance_unix.py")
_PY_SERVE_TCP = str(_PY_TESTS / "serve_conformance_tcp.py")

if SERVER == "python" and not _PY_TESTS.is_dir():
    pytest.skip(
        f"VGI_CONFORMANCE_SERVER=python needs a vgi-rpc-python checkout; {_PY_TESTS} does not exist. "
        "The reference conformance servers ship in that repository's tests/ directory, not in the wheel. "
        "Point VGI_RPC_PYTHON_REPO at a checkout.",
        allow_module_level=True,
    )


# Under ROLE=client, re-bind the module-level ``vgi_rpc.http`` entry points to
# the driver.  The HTTP feature groups -- external location, sticky sessions,
# response caps, upload URLs -- import ``http_connect`` / ``http_capabilities``
# / ``request_upload_urls`` *inside the test body*, so without this they
# quietly exercise the Python client and prove nothing about this port.
if ROLE == "client":
    import go_client_proxy as _shim

    _shim.DRIVER.install_http_overrides()


#: Transports the current role can drive.
#:
#: Under ROLE=client this is what the Go *client* can dial, which is narrower
#: than what the Go *server* serves: the port has no stdio or shm client, so
#: ``pipe``, ``subprocess`` and ``shm`` are absent.  Left absent rather than
#: mapped onto another transport -- a substitution would report a pass for a
#: call the client cannot make.  ``unix`` and ``tcp`` carry the same raw Arrow
#: IPC framing that stdio does, so little wire coverage is lost.
_ALL_CONN_TRANSPORTS = ("pipe", "subprocess", "shm", "http", "http_externalize_always", "unix", "tcp")
_ALL_RAW_TRANSPORTS = ("pipe", "subprocess", "shm", "unix", "tcp")


def _transports(candidates: tuple[str, ...]) -> list[str]:
    """Narrow a transport matrix to this role, honouring ``VGI_TRANSPORTS``."""
    if ROLE == "client":
        from go_client_proxy import CLIENT_TRANSPORTS

        candidates = tuple(name for name in candidates if name in CLIENT_TRANSPORTS)
    requested = os.environ.get("VGI_TRANSPORTS")
    if requested:
        wanted = {name.strip() for name in requested.split(",") if name.strip()}
        candidates = tuple(name for name in candidates if name in wanted)
    # An empty params list makes pytest collect nothing at all, which reads as
    # a green run over zero tests. Fail the collection instead.
    if not candidates:
        raise RuntimeError(f"no conformance transports selected (ROLE={ROLE}, VGI_TRANSPORTS={requested!r})")
    return list(candidates)


_CONN_TRANSPORTS = _transports(_ALL_CONN_TRANSPORTS)


def _free_port() -> int:
    """Reserve a loopback port for a server that cannot bind zero itself."""
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    return port


def _pipe_worker_cmd() -> list[str]:
    """Argv for a stdio conformance worker on the current SERVER."""
    if SERVER == "python":
        # The reference CLI needs reflection turned on explicitly; the Go
        # worker registers it unconditionally.
        return [_REF_PYTHON, _PY_SERVE_PIPE, "--describe"]
    return [GO_WORKER]


def _translate_http_args(args: tuple[str, ...]) -> list[str]:
    """Map this worker's HTTP flag vector onto the Python reference's.

    Translating the flags -- rather than giving every fixture a per-server
    branch -- keeps each fixture's comment describing *what it configures*
    instead of two spellings of how.  A flag vector with no reference
    equivalent skips rather than silently starting a differently configured
    server, because a fixture quietly standing in for another one is how a
    group passes while testing nothing.
    """
    rest = list(args)
    script = _PY_SERVE_HTTP
    out: list[str] = []
    storage_mode = False

    if rest and rest[0] == "--http-auth":
        if rest[1:]:
            pytest.skip(f"reference auth server takes no extra flags: {args!r}")
        # This one binds the port it is told to and echoes it back, rather than
        # binding zero and reporting what it got, so the port has to be picked
        # here. Racy in principle; the reference's own harness does the same.
        return [_REF_PYTHON, _PY_SERVE_AUTH, "--port", str(_free_port())]
    if rest and rest[0] == "--http-proof":
        pytest.skip("proxy-proof workers are not wired for SERVER=python")
    if rest and rest[0] == "--http-strict":
        script, rest, storage_mode = _PY_SERVE_STRICT, rest[1:], True
    elif rest and rest[0] == "--http-with-storage":
        out += ["--fake-storage", rest[1]]
        rest, storage_mode = rest[2:], True
    elif rest and rest[0] == "--http-with-zstd-storage":
        out += ["--fake-storage", rest[1], "--compression", "zstd"]
        rest, storage_mode = rest[2:], True
    elif rest and rest[0] == "--http-external-security":
        # The reference spells this configuration out; the Go worker bundles
        # it behind one flag. The numbers are the shared suite's.
        out += [
            "--fake-storage", rest[1],
            "--max-request-bytes", "1048576",
            "--max-fetch-bytes", "4096",
            "--max-decompressed-fetch-bytes", "8192",
            "--reject-localhost-redirects",
        ]
        rest, storage_mode = rest[2:], True
    elif rest and rest[0] == "--http":
        rest = rest[1:]

    index = 0
    while index < len(rest):
        flag = rest[index]
        if flag in (
            "--no-compression",
            "--no-call-state-cache",
            "--sticky-auth",
            "--introspect",
            "--fail-serve-start-once",
            "--reject-localhost-redirects",
        ):
            out.append(flag)
            index += 1
        elif flag in (
            "--max-request-bytes",
            "--sticky-ttl",
            "--token-key",
            "--identity",
            "--cors-origin",
            "--access-log",
            "--fake-storage",
            "--externalize-threshold",
            "--max-response-bytes",
            "--max-externalized-response-bytes",
        ):
            out += [flag, rest[index + 1]]
            storage_mode = storage_mode or flag in ("--fake-storage", "--externalize-threshold")
            index += 2
        elif flag == "--server-id":
            # The reference mints a fresh server id per process, so the sticky
            # peer pair differs without being told to. Dropping the flag is
            # what the Rust harness does, for the same reason.
            index += 2
        else:
            pytest.skip(f"no reference equivalent for worker flag {flag!r} (from {args!r})")

    if script is _PY_SERVE_STRICT:
        return [_REF_PYTHON, script, "--port", "0", "--describe", *out]
    if storage_mode:
        # The reference's externalisation branch is selected by the *absence*
        # of --http; it binds --port and prints PORT: just the same.
        return [_REF_PYTHON, script, "--port", "0", "--describe", *out]
    return [_REF_PYTHON, script, "--http", "--describe", *out]


def _http_worker_argv(args: tuple[str, ...]) -> list[str]:
    """Argv for an HTTP conformance worker on the current SERVER."""
    if SERVER == "python":
        return _translate_http_args(args)
    return [GO_WORKER, *args]


@pytest.fixture(scope="session")
def go_transport() -> Iterator[SubprocessTransport]:
    transport = SubprocessTransport(_pipe_worker_cmd())
    yield transport
    transport.close()


# Environment knob so `make race` (which builds the worker with -race and
# slows it 3-5x) can bump teardown timeouts without changing call sites.
_WORKER_TEARDOWN_TIMEOUT = float(os.environ.get("VGI_GO_WORKER_TEARDOWN_TIMEOUT", "5"))


def _wait_for_http(port: int, timeout: float = 5.0) -> None:
    """Poll until the HTTP server is accepting connections."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            _ = httpx.get(f"http://127.0.0.1:{port}/", timeout=5.0)
            return
        except (httpx.ConnectError, httpx.ConnectTimeout):
            time.sleep(0.1)
    raise TimeoutError(f"HTTP server on port {port} did not start within {timeout}s")


def _start_http_worker(*extra_args: str, tcp_only_ready: bool = False) -> Iterator[int]:
    """Spawn an HTTP conformance worker and yield its TCP port."""
    proc = subprocess.Popen(
        _http_worker_argv(extra_args),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("PORT:"), f"Expected PORT:<n>, got: {line!r}"
        port = int(line.split(":", 1)[1])

        if tcp_only_ready:
            _wait_for_tcp("127.0.0.1", port)
        else:
            _wait_for_http(port)

        yield port
    finally:
        proc.terminate()
        proc.wait(timeout=_WORKER_TEARDOWN_TIMEOUT)


@pytest.fixture(scope="session")
def go_http_port() -> Iterator[int]:
    """Start Go conformance HTTP server."""
    yield from _start_http_worker("--http")


# Aliases expected by upstream conformance suite (vgi_rpc.conformance._pytest_suite).
@pytest.fixture(scope="session")
def conformance_http_port(go_http_port: int) -> int:
    return go_http_port


@pytest.fixture
def conformance_resource_soak_target() -> Iterator[Any]:
    """Expose one isolated Go HTTP worker to the shared resource soak.

    The dedicated process is important: sampling the session-wide worker would
    mix unrelated conformance activity into the descriptor, thread, and RSS
    measurements.  stderr is inherited so a failed soak cannot deadlock on an
    unread pipe and leaves useful diagnostics in the pytest log.
    """
    from vgi_rpc.conformance._resource_soak_pytest import (
        ResourceSoakLimits,
        ResourceSoakTarget,
    )

    proc = subprocess.Popen(
        _http_worker_argv(("--http",)),
        stdout=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("PORT:"), f"Expected PORT:<n>, got: {line!r}"
        port = int(line.split(":", 1)[1])
        _wait_for_http(port)

        def connect() -> contextlib.AbstractContextManager[Any]:
            return http_connect(ConformanceService, f"http://127.0.0.1:{port}")

        yield ResourceSoakTarget(
            name="go-http",
            pid=proc.pid,
            connect=connect,
            limits=ResourceSoakLimits(
                rss_growth_bytes=32 * 1024 * 1024,
                rss_slope_bytes_per_epoch=2 * 1024 * 1024,
                descriptor_growth=3,
                thread_growth=4,
                child_growth=0,
            ),
            # Go Arrow's allocator reaches a stable reserved-arena plateau only
            # after several equivalent workloads. Keep the measured budget
            # strict, but establish the baseline after that legitimate warm-up.
            warmup_multiplier=8,
        )
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=_WORKER_TEARDOWN_TIMEOUT)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=_WORKER_TEARDOWN_TIMEOUT)


@pytest.fixture(scope="session")
def conformance_http_no_compression_port() -> Iterator[int]:
    """Go HTTP worker with response compression disabled.

    Backs the shared ``test_empty_advertisement_means_never_compressed``
    case.  It needs its own server because the state under test is a
    *server configuration* -- "I can produce no codecs" -- which no client
    request can induce.  ``identity`` covers the client-side ability to
    demand an uncompressed body; only a server booted this way emits the
    present-but-empty ``VGI-Supported-Encodings`` that distinguishes
    "speaks no compression" from an absent header on a legacy server.
    """
    yield from _start_http_worker("--http", "--no-compression")


@pytest.fixture(scope="session")
def conformance_http_small_request_cap_port() -> Iterator[int]:
    """Go HTTP worker with the shared suite's canonical 4 KiB request cap."""
    yield from _start_http_worker("--http", "--max-request-bytes", "4096")


@pytest.fixture(scope="class")
def conformance_http_serve_start_fail_once_port() -> Iterator[int]:
    """Worker whose first HTTP transport notification fails, then retries.

    Readiness is TCP-only because an HTTP probe would consume the injected
    first-request failure before the shared lifecycle test can observe it.
    """
    yield from _start_http_worker(
        "--http",
        "--fail-serve-start-once",
        tcp_only_ready=True,
    )


@pytest.fixture(scope="session")
def conformance_http_auth_port() -> Iterator[int]:
    """Start a Go HTTP server that rejects every RPC call with 401."""
    yield from _start_http_worker("--http-auth")


@pytest.fixture(scope="session")
def conformance_http_auth_reason_port(conformance_http_auth_port: int) -> int:
    """Port of a worker that honours ``X-Conformance-Auth-Reason``.

    Backs the shared ``TestUnauthorized`` reason-code tests. Membership in
    the closed set is not enough on its own — a server that answers every
    401 with ``unauthorized`` satisfies that. These tests prove the codes
    are *discriminated*, which is what makes them worth branching on.

    The Go worker's ``--http-auth`` mode already reads the header, so this
    is the same worker under the name the suite looks up.
    """
    return conformance_http_auth_port


@pytest.fixture(scope="session")
def conformance_http_cors_port(conformance_fake_storage: str) -> Iterator[int]:
    """Start a Go HTTP worker that allows the conformance origin.

    Backs the shared ``TestCors`` group, which is the only place the suite
    can check what a *browser* may read: every other test drives the server
    with a client that ignores CORS entirely.  Needs its own worker because
    the companion ``TestCorsOffMode`` requires the default one to grant no
    origin at all.  The origin is fixed by the suite (``_CORS_ORIGIN``).

    Storage mode is deliberate, not incidental: the derived exposure check
    can only catch a missing entry for a header the worker actually
    advertises, so a *plain* worker here would silently skip the whole
    conditional half of the capability set -- the size caps and the
    upload-URL trio -- which are exactly the exposures a port is most likely
    to miss.  ``test_worker_advertises_the_optional_capabilities`` guards
    this fixture against being pointed back at a bare worker.
    """
    yield from _start_http_worker(
        "--http-with-storage",
        conformance_fake_storage,
        "--cors-origin",
        "https://conformance.example",
    )


@pytest.fixture(scope="session")
def conformance_http_introspect_port() -> Iterator[int]:
    """Start a Go HTTP worker with token introspection enabled.

    Backs the shared ``TestTokenIntrospection`` group.  It needs its own
    worker because the endpoint resolves nothing unless explicitly enabled --
    which the ungated ``TestTokenIntrospectionOffMode`` asserts against the
    default worker.  The introspector principal, subject credential and JWS
    trap token are fixed by the suite; the worker configures exactly those.
    """
    yield from _start_http_worker("--http", "--introspect")


@pytest.fixture(scope="session")
def conformance_http_identity_port() -> Iterator[int]:
    """A Go HTTP worker hosting ``vgi_rpc.Identity.v1`` with both hooks.

    Backs the shared identity group.  The protocol is nearly all guards and
    every guard reads deployment policy -- who may introspect, what a
    credential resolves to, whether a grant is minted, how recently the caller
    authenticated -- so no cross-port assertion exists against a worker whose
    allowlist and hooks are unknown.  The policy is pinned by
    ``IDENTITY_CONFORMANCE_FIXTURE.md`` and configured in
    ``conformance/identity_fixture.go``.

    It needs its own worker because ``TestIdentityAbsentByDefault`` asserts
    against the *plain* one that a deployment configuring no hook hosts no
    identity protocol at all -- so ``--identity`` must stay off there.
    """
    yield from _start_http_worker("--http", "--identity", "both")


@pytest.fixture(scope="session")
def conformance_http_identity_introspect_only_port() -> Iterator[int]:
    """The same binary with the mint hook left out.

    Method-level narrowing -- that an unconfigured hook makes its method
    *absent* rather than hosted-and-refusing, and shrinks the ``protocol_hash``
    with it -- is only observable against a second worker configured with one
    hook.  A client compares that hash to decide whether its cached description
    is still valid, so two different method sets sharing one hash means a
    client keeps calling a method that is no longer there.
    """
    yield from _start_http_worker("--http", "--identity", "introspect-only")


@pytest.fixture(scope="session")
def conformance_http_cold_call_cache_port() -> Iterator[int]:
    """Start a Go HTTP server with the call-state cache disabled.

    Backs the shared ``TestColdCallStateCache`` group. With the cache warm a
    client that never echoes the call token still works, and only breaks
    once a continuation lands on a process with no cached entry. Disabling
    the cache makes every turn take that path.
    """
    yield from _start_http_worker("--http", "--no-call-state-cache")


@pytest.fixture(scope="session")
def conformance_http_access_log(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[tuple[int, Path]]:
    """Go HTTP worker writing JSONL access records, yielding ``(port, path)``.

    Backs the shared ``TestRequestId`` correlation case, which asserts that
    the ``X-Request-ID`` on a response and the ``request_id`` in the record
    name the same request. That is the whole value of the field, and nothing
    observable on the wire can stand in for it: the check has to read back
    what the server logged for a request the suite itself made.

    The worker needs no new flag — ``--access-log <path>`` is already
    scanned out of ``os.Args`` (``conformance/cmd/vgi-rpc-conformance-go``)
    and installs an ``AccessLogHook`` emitting the spec's JSONL, ``logger``
    field included.
    """
    log_path = tmp_path_factory.mktemp("accesslog") / "conformance.log"
    gen = _start_http_worker("--http", "--access-log", str(log_path))
    port = next(gen)
    try:
        yield port, log_path
    finally:
        next(gen, None)


# ---------------------------------------------------------------------------
# Sticky failure-path fixtures (upstream TestSticky; see the reference repo's
# docs/sticky-sessions-spec.md §9.1)
# ---------------------------------------------------------------------------

# Shared AEAD key for the peer pair. Both workers can open each other's session
# tokens, which is the point: the rejection under test has to come from the
# server_id comparison, not from a decrypt failure.
_STICKY_PEER_TOKEN_KEY = "5f" * 32


@pytest.fixture(scope="session")
def conformance_http_sticky_short_ttl_port() -> Iterator[int]:
    """A sticky worker whose default session TTL is short enough to outwait.

    Backs ``TestSticky::test_expired_session_surfaces_session_lost``; the main
    worker's 300s default is not something a test can sit out.
    """
    yield from _start_http_worker("--http", "--sticky-ttl", "1")


@pytest.fixture(scope="session")
def conformance_http_sticky_peer_ports() -> Iterator[tuple[int, int]]:
    """Two sticky workers sharing one AEAD key but reporting distinct server ids.

    Backs ``TestSticky::test_token_from_other_worker_rejected``. The Go worker
    otherwise hardcodes ``conformance-go`` as its server id, so without the
    explicit ``--server-id`` both peers would look like the same worker and the
    test would have nothing to reject.
    """
    gen_a = _start_http_worker(
        "--http", "--token-key", _STICKY_PEER_TOKEN_KEY, "--server-id", "conformance-go-peer-a"
    )
    gen_b = _start_http_worker(
        "--http", "--token-key", _STICKY_PEER_TOKEN_KEY, "--server-id", "conformance-go-peer-b"
    )
    port_a = next(gen_a)
    try:
        port_b = next(gen_b)
        try:
            yield port_a, port_b
        finally:
            next(gen_b, None)
    finally:
        next(gen_a, None)


@pytest.fixture(scope="session")
def conformance_http_sticky_auth_port() -> Iterator[int]:
    """A sticky worker that authenticates the ``X-Conformance-Principal`` header.

    Backs ``TestSticky::test_cross_principal_replay_rejected``, which needs one
    worker reachable as two identities.
    """
    yield from _start_http_worker("--http", "--sticky-auth")


@pytest.fixture(scope="session")
def proof_worker_factory() -> Iterator[Callable[..., Any]]:
    """Spawn Go workers gated on proxy proof, for the shared TestProxyProof group.

    The shared suite owns the matrix; this only has to know how to start one
    worker for a given configuration.
    """
    if SERVER != "go":
        pytest.skip(f"proof worker not wired for SERVER={SERVER}")

    from vgi_rpc.conformance.proof_harness import ProofWorker, ProofWorkerConfig

    @contextlib.contextmanager
    def spawn(config: ProofWorkerConfig) -> Iterator[ProofWorker]:
        args = [
            "--http-proof",
            "--proof-mode", config.mode,
            "--proof-origin-id", config.origin_id,
            "--proof-secrets", config.secrets,
            "--proof-skew", str(config.skew_seconds),
        ]
        if not config.replay_cache:
            args.append("--proof-no-replay-cache")
        gen = _start_http_worker(*args)
        port = next(gen)
        try:
            # The Go worker mounts proof mode under /vgi, mirroring its auth mode.
            yield ProofWorker(port=port, prefix="/vgi", config=config)
        finally:
            with contextlib.suppress(StopIteration):
                next(gen)

    yield spawn


@pytest.fixture(scope="session")
def conformance_fake_storage() -> Iterator[str]:
    """Run the Python fake-storage WSGI app in a background thread."""
    from vgi_rpc.conformance.fake_storage import serve_in_thread

    base_url, shutdown = serve_in_thread()
    try:
        yield base_url
    finally:
        shutdown()


@pytest.fixture(scope="session")
def conformance_http_with_storage_port(conformance_fake_storage: str) -> Iterator[int]:
    """Go HTTP worker configured to externalize large batches via fake storage."""
    yield from _start_http_worker("--http-with-storage", conformance_fake_storage)


@pytest.fixture(scope="session")
def conformance_http_with_zstd_storage_port(conformance_fake_storage: str) -> Iterator[int]:
    """Go HTTP worker with externalization + zstd compression enabled."""
    yield from _start_http_worker("--http-with-zstd-storage", conformance_fake_storage)


@pytest.fixture(scope="session")
def conformance_http_external_security_port(conformance_fake_storage: str) -> Iterator[int]:
    """Go worker with independent fetch caps and per-hop URL validation."""
    yield from _start_http_worker("--http-external-security", conformance_fake_storage)


@pytest.fixture(scope="session")
def conformance_http_strict_cap_port() -> Iterator[int]:
    """Go HTTP worker with strict response caps (matches Python's --http-strict).

    The worker installs max_response_bytes + max_externalized_response_bytes
    (defaulting to 1 MiB each). The conformance suite's
    ``TestHttpResponseCap`` / ``TestHttpResponseCapSoftWire`` classes probe
    the capability headers at runtime and tailor expectations to whichever
    caps the server advertises.
    """
    yield from _start_http_worker("--http-strict")


@pytest.fixture(scope="session")
def conformance_http_externalized_cap_port(conformance_fake_storage: str) -> Iterator[int]:
    """Go HTTP worker whose *external-channel* cap is the one that bites.

    Backs the shared ``TestExternalizedResponseCap`` group. Two settings
    make this fixture mean what it says:

    * ``--max-externalized-response-bytes`` is tight (64 KiB) so an
      externalised response overshoots it.
    * ``--max-response-bytes`` is deliberately *generous* (8 MiB). An
      externalised payload leaves only a pointer batch on the wire, so the
      body cap should never be what fails here -- if it were tight too,
      the group would pass while proving nothing about the external cap.

    ``--externalize-threshold`` stays at the strict worker's 4 KiB default
    so a modest payload still externalises, which is what lets the
    under-cap control exercise the same channel without tripping the cap.
    """
    yield from _start_http_worker(
        "--http-strict",
        "--fake-storage",
        conformance_fake_storage,
        "--max-externalized-response-bytes",
        str(64 * 1024),
        "--max-response-bytes",
        str(8 * 1024 * 1024),
    )


@pytest.fixture(scope="session")
def conformance_http_externalize_always_port(conformance_fake_storage: str) -> Iterator[int]:
    """Go HTTP worker that externalizes EVERY non-empty response batch.

    Sets ``--externalize-threshold 1`` so every data-bearing batch (any
    batch with > 0 rows) goes through the upload-URL flow.  Keeps the
    inline-request cap loose (1 MiB) so normal client-vended request
    bodies aren't 413-rejected — this variant exercises *response*-side
    externalization across the full conformance method matrix.
    """
    yield from _start_http_worker(
        "--http-with-storage",
        conformance_fake_storage,
        "--externalize-threshold",
        "1",
        "--max-request-bytes",
        "1048576",
    )


def _short_unix_path(name: str) -> str:
    """Return a short /tmp path for a Unix domain socket (macOS 104-byte limit)."""
    fd, path = tempfile.mkstemp(prefix=f"vgi-go-{name}-", suffix=".sock", dir="/tmp")
    os.close(fd)
    os.unlink(path)
    return path


def _wait_for_unix(path: str, timeout: float = 5.0) -> None:
    """Poll until a Unix domain socket is accepting connections."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                sock.connect(path)
                return
            finally:
                sock.close()
        except (FileNotFoundError, ConnectionRefusedError, OSError):
            time.sleep(0.1)
    raise TimeoutError(f"Unix socket at {path} did not start within {timeout}s")


@pytest.fixture(scope="session")
def go_unix_path() -> Iterator[str]:
    """Start Go conformance Unix socket server."""
    path = _short_unix_path("conf")
    argv = (
        [_REF_PYTHON, _PY_SERVE_UNIX, path]
        if SERVER == "python"
        else [GO_WORKER, "--unix", path]
    )
    proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line == f"UNIX:{path}", f"Expected UNIX:{path}, got: {line!r}"
        _wait_for_unix(path)
        yield path
    finally:
        proc.terminate()
        proc.wait(timeout=_WORKER_TEARDOWN_TIMEOUT)


def _wait_for_tcp(host: str, port: int, timeout: float = 5.0) -> None:
    """Poll until a TCP socket is accepting connections."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            sock = socket.create_connection((host, port), timeout=1.0)
            sock.close()
            return
        except (ConnectionRefusedError, OSError):
            time.sleep(0.1)
    raise TimeoutError(f"TCP socket at {host}:{port} did not start within {timeout}s")


@pytest.fixture(scope="session")
def go_tcp_addr() -> Iterator[tuple[str, int]]:
    """Start Go conformance raw-TCP server on a loopback auto-selected port."""
    argv = (
        [_REF_PYTHON, _PY_SERVE_TCP, "127.0.0.1", "0"]
        if SERVER == "python"
        else [GO_WORKER, "--tcp", "127.0.0.1:0"]
    )
    proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("TCP:"), f"Expected TCP:<host>:<port>, got: {line!r}"
        host_part, _, port_part = line[len("TCP:") :].rpartition(":")
        host = host_part or "127.0.0.1"
        port = int(port_part)
        _wait_for_tcp(host, port)
        yield (host, port)
    finally:
        proc.terminate()
        proc.wait(timeout=_WORKER_TEARDOWN_TIMEOUT)


class _KindProbe(Protocol):
    # The routing key is the protocol's *name*, and the Go worker registers
    # this probe as "TransportKindProbe" (SetServiceName, main.go). Without
    # the override the key is derived from this class's own name and the
    # server refuses the call as addressed to a protocol it does not host --
    # a rejection about naming that reads as one about transport kinds.
    protocol_name = "TransportKindProbe"

    def report_transport_kind(self) -> str: ...


@pytest.fixture(scope="class")
def conformance_transport_kind_probes() -> tuple[tuple[str, Callable[[], str]], ...]:
    """Real Go-worker probes for every transport kind the port supports.

    Go-worker only: the probe is a protocol this port's conformance binary
    registers (``--transport-kind-probe``), and the reference server hosts no
    such thing. There is nothing here for a foreign server to answer.
    """
    if SERVER != "go":
        pytest.skip(f"transport-kind probe is a Go-worker fixture; SERVER={SERVER}")

    def probe_pipe() -> str:
        transport = SubprocessTransport([GO_WORKER, "--transport-kind-probe"])
        try:
            return str(_RpcProxy(_KindProbe, transport, None).report_transport_kind())
        finally:
            transport.close()

    def probe_http() -> str:
        worker = _start_http_worker("--http", "--transport-kind-probe")
        port = next(worker)
        try:
            with http_connect(_KindProbe, f"http://127.0.0.1:{port}") as proxy:
                return str(proxy.report_transport_kind())
        finally:
            next(worker, None)

    def probe_unix() -> str:
        path = _short_unix_path("kind")
        proc = subprocess.Popen(
            [GO_WORKER, "--unix", path, "--transport-kind-probe"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            assert proc.stdout is not None
            line = proc.stdout.readline().decode().strip()
            assert line == f"UNIX:{path}", f"Expected UNIX:{path}, got: {line!r}"
            _wait_for_unix(path)
            with unix_connect(_KindProbe, path) as proxy:
                return str(proxy.report_transport_kind())
        finally:
            proc.terminate()
            proc.wait(timeout=_WORKER_TEARDOWN_TIMEOUT)

    def probe_tcp() -> str:
        proc = subprocess.Popen(
            [GO_WORKER, "--tcp", "127.0.0.1:0", "--transport-kind-probe"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            assert proc.stdout is not None
            line = proc.stdout.readline().decode().strip()
            assert line.startswith("TCP:"), f"Expected TCP:<host>:<port>, got: {line!r}"
            host, _, raw_port = line[len("TCP:") :].rpartition(":")
            port = int(raw_port)
            _wait_for_tcp(host, port)
            with tcp_connect(_KindProbe, host, port) as proxy:
                return str(proxy.report_transport_kind())
        finally:
            proc.terminate()
            proc.wait(timeout=_WORKER_TEARDOWN_TIMEOUT)

    return (
        ("pipe", probe_pipe),
        ("http", probe_http),
        ("unix", probe_unix),
        ("tcp", probe_tcp),
    )


ConnFactory = Callable[..., contextlib.AbstractContextManager[Any]]


class _ShmAdapter:
    """Wraps a SubprocessTransport with a shared-memory side-channel.

    Mirrors ``vgi_rpc.rpc.ShmPipeTransport`` — exposes the inner pipe's
    reader/writer plus a ``.shm`` property the proxy uses to redirect
    large batch payloads through the segment. Client owns the segment
    lifetime; server attaches per-request.
    """

    __slots__ = ("_inner", "_shm")

    def __init__(self, inner: SubprocessTransport, shm: Any) -> None:
        self._inner = inner
        self._shm = shm

    @property
    def reader(self) -> Any:
        return self._inner.reader

    @property
    def writer(self) -> Any:
        return self._inner.writer

    @property
    def shm(self) -> Any:
        return self._shm

    def close(self) -> None:
        self._inner.close()


def _client_factory(
    param: str,
    on_log: Callable[[Message], None] | None,
    http_port: int | None,
    unix_path: str | None,
    tcp_addr: tuple[str, int] | None,
    ext_port: int | None,
) -> contextlib.AbstractContextManager[Any]:
    """Yield a Go-client-backed proxy (``VGI_CONFORMANCE_ROLE=client``)."""
    from go_client_proxy import GoClientProxy

    external_config = None
    if param == "http":
        transport, target = "http", f"http://127.0.0.1:{http_port}"
    elif param == "http_externalize_always":
        from vgi_rpc.external import ExternalLocationConfig

        transport, target = "http", f"http://127.0.0.1:{ext_port}"
        # Resolution happens in the client under test, never here: doing it on
        # this side would make the external-location group pass without the
        # client ever performing a fetch. The validator is opted out because
        # the fake storage vends http:// loopback URLs.
        external_config = ExternalLocationConfig(url_validator=None)
    elif param == "unix":
        transport, target = "unix", unix_path
    elif param == "tcp":
        assert tcp_addr is not None
        transport, target = "tcp", f"{tcp_addr[0]}:{tcp_addr[1]}"
    else:
        raise AssertionError(f"transport {param!r} has no client in this port")

    @contextlib.contextmanager
    def _conn() -> Iterator[Any]:
        proxy = GoClientProxy(transport, target, on_log, external_config=external_config)
        try:
            yield proxy
        finally:
            proxy.close()

    return _conn()


@pytest.fixture(params=_CONN_TRANSPORTS)
def conformance_conn(
    request: pytest.FixtureRequest,
    go_transport: SubprocessTransport,
    go_http_port: int,
    go_unix_path: str,
    go_tcp_addr: tuple[str, int],
) -> ConnFactory:
    def factory(
        on_log: Callable[[Message], None] | None = None,
    ) -> contextlib.AbstractContextManager[Any]:
        if ROLE == "client":
            return _client_factory(
                request.param,
                on_log,
                go_http_port if request.param == "http" else None,
                go_unix_path if request.param == "unix" else None,
                go_tcp_addr if request.param == "tcp" else None,
                (
                    request.getfixturevalue("conformance_http_externalize_always_port")
                    if request.param == "http_externalize_always"
                    else None
                ),
            )
        if request.param == "pipe":

            @contextlib.contextmanager
            def _pipe_conn() -> Iterator[_RpcProxy]:
                transport = SubprocessTransport(_pipe_worker_cmd())
                try:
                    yield _RpcProxy(ConformanceService, transport, on_log)
                finally:
                    transport.close()

            return _pipe_conn()
        elif request.param == "shm":

            @contextlib.contextmanager
            def _shm_conn() -> Iterator[_RpcProxy]:
                from vgi_rpc.shm import ShmSegment

                segment = ShmSegment.create(8 * 1024 * 1024)
                transport = SubprocessTransport(_pipe_worker_cmd())
                wrapped = _ShmAdapter(transport, segment)
                try:
                    yield _RpcProxy(ConformanceService, wrapped, on_log)
                finally:
                    transport.close()
                    with contextlib.suppress(BufferError):
                        segment.close()
                    segment.unlink()

            return _shm_conn()
        elif request.param == "http":
            return http_connect(
                ConformanceService,
                f"http://127.0.0.1:{go_http_port}",
                on_log=on_log,
            )
        elif request.param == "http_externalize_always":
            from vgi_rpc.external import ExternalLocationConfig

            ext_port: int = request.getfixturevalue("conformance_http_externalize_always_port")
            return http_connect(
                ConformanceService,
                f"http://127.0.0.1:{ext_port}",
                on_log=on_log,
                # Server uses http://127.0.0.1 download URLs from the
                # in-process fake storage; disable the HTTPS-only validator.
                external_location=ExternalLocationConfig(url_validator=None),
            )
        elif request.param == "unix":
            return unix_connect(
                ConformanceService,
                go_unix_path,
                on_log=on_log,
            )
        elif request.param == "tcp":
            return tcp_connect(
                ConformanceService,
                go_tcp_addr[0],
                go_tcp_addr[1],
                on_log=on_log,
            )
        else:
            # "subprocess" — shared transport
            @contextlib.contextmanager
            def _conn() -> Iterator[_RpcProxy]:
                yield _RpcProxy(ConformanceService, go_transport, on_log)

            return _conn()

    return factory


@pytest.fixture(params=_ALL_RAW_TRANSPORTS)
def conformance_raw_conn(
    request: pytest.FixtureRequest,
    go_transport: SubprocessTransport,
    go_unix_path: str,
    go_tcp_addr: tuple[str, int],
) -> ConnFactory:
    """Connect only through transports exposing a persistent byte stream.

    Always the Python raw proxy, in either role. The group behind this fixture
    writes deliberately malformed frames onto the socket and then reuses it, so
    it is a *server* contract probe that bypasses whatever client is under
    test -- and there is no client API for emitting a mutated frame, which is
    the point of testing it this way.
    """

    def factory(
        on_log: Callable[[Message], None] | None = None,
    ) -> contextlib.AbstractContextManager[Any]:
        if request.param == "pipe":

            @contextlib.contextmanager
            def _pipe_conn() -> Iterator[_RpcProxy]:
                transport = SubprocessTransport(_pipe_worker_cmd())
                try:
                    yield _RpcProxy(ConformanceService, transport, on_log)
                finally:
                    transport.close()

            return _pipe_conn()
        if request.param == "subprocess":

            @contextlib.contextmanager
            def _shared_conn() -> Iterator[_RpcProxy]:
                yield _RpcProxy(ConformanceService, go_transport, on_log)

            return _shared_conn()
        if request.param == "shm":

            @contextlib.contextmanager
            def _shm_conn() -> Iterator[_RpcProxy]:
                from vgi_rpc.shm import ShmSegment

                segment = ShmSegment.create(8 * 1024 * 1024)
                transport = SubprocessTransport(_pipe_worker_cmd())
                wrapped = _ShmAdapter(transport, segment)
                try:
                    yield _RpcProxy(ConformanceService, wrapped, on_log)
                finally:
                    transport.close()
                    with contextlib.suppress(BufferError):
                        segment.close()
                    segment.unlink()

            return _shm_conn()
        if request.param == "unix":
            return unix_connect(ConformanceService, go_unix_path, on_log=on_log)
        if request.param == "tcp":
            return tcp_connect(
                ConformanceService,
                go_tcp_addr[0],
                go_tcp_addr[1],
                on_log=on_log,
            )
        raise AssertionError(f"non-raw conformance transport: {request.param}")

    return factory


@pytest.fixture(params=_CONN_TRANSPORTS)
def conformance_describe(
    request: pytest.FixtureRequest,
    go_transport: SubprocessTransport,
    go_http_port: int,
    go_unix_path: str,
    go_tcp_addr: tuple[str, int],
) -> ServiceDescription:
    """Return a ``ServiceDescription`` from real reflection calls over the wire.

    Parallels ``conformance_conn`` — same transport matrix — but instead of a
    proxy it drives ``vgi_rpc.Reflection.v1`` against the Go worker under test
    (``list_protocols``, then ``describe``) and adapts the reply, so
    ``TestDescribeConformance`` validates introspection against the running Go
    server (not a throwaway in-process Python one).  The Go worker registers
    reflection on every transport; ``__describe__``, which this fixture used to
    call, is retired and answers only with a refusal naming its replacement.
    """
    from vgi_rpc.http import http_introspect
    from vgi_rpc.introspect import introspect
    from vgi_rpc.rpc import TcpTransport, UnixTransport

    param = request.param
    if ROLE == "client":
        # The description the *client under test* decoded, relayed as JSON.
        # Reflection's reply is two nested payloads rather than one flat batch,
        # so this is the single op the control protocol relays decoded -- see
        # the driver spec's section 4.3.
        with _client_factory(
            param,
            None,
            go_http_port if param == "http" else None,
            go_unix_path if param == "unix" else None,
            go_tcp_addr if param == "tcp" else None,
            (
                request.getfixturevalue("conformance_http_externalize_always_port")
                if param == "http_externalize_always"
                else None
            ),
        ) as proxy:
            return proxy.describe()
    if param in ("pipe", "shm"):
        # No describe-specific side channel needed; a fresh stdio worker is the
        # faithful equivalent of Python's fresh in-process pipe server.
        transport = SubprocessTransport(_pipe_worker_cmd())
        try:
            return introspect(transport)
        finally:
            transport.close()
    if param == "subprocess":
        return introspect(go_transport)
    if param == "unix":
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.connect(go_unix_path)
        except BaseException:
            sock.close()
            raise
        transport = UnixTransport(sock)
        try:
            return introspect(transport)
        finally:
            transport.close()
    if param == "tcp":
        tcp_sock = socket.create_connection(go_tcp_addr)
        transport = TcpTransport(tcp_sock)
        try:
            return introspect(transport)
        finally:
            transport.close()
    if param == "http_externalize_always":
        from vgi_rpc.external import ExternalLocationConfig

        ext_port: int = request.getfixturevalue("conformance_http_externalize_always_port")
        # Reflection is an ordinary co-hosted protocol, so its replies are
        # externalized like any other method's.  Without a resolver the client
        # reads the pointer batch itself -- an empty ``result`` column, which
        # surfaces as an IndexError from pyarrow rather than as anything about
        # describe.  Same url_validator opt-out as ``conformance_conn``: the
        # download URLs are http://127.0.0.1 from the in-process fake storage.
        return http_introspect(
            base_url=f"http://127.0.0.1:{ext_port}",
            external_location=ExternalLocationConfig(url_validator=None),
        )
    return http_introspect(base_url=f"http://127.0.0.1:{go_http_port}")


# Import all tests from the conformance test module (PyPI package)
from vgi_rpc.conformance._pytest_suite import *  # noqa: F401,F403,E402


# Override: allow TestLargeData on all transports (the upstream suite skips
# non-pipe transports, but the Go worker handles them fine).
class TestLargeData(TestLargeData):  # type: ignore[no-redef]  # noqa: F811
    @pytest.fixture(autouse=True)
    def _skip_non_pipe(self) -> None:
        pass
