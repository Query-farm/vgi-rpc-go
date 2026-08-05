"""Run Python conformance tests against the Go conformance worker."""
import contextlib
import os
import socket
import subprocess
import tempfile
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

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


@pytest.fixture(scope="session")
def go_transport() -> Iterator[SubprocessTransport]:
    transport = SubprocessTransport([GO_WORKER])
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


def _start_http_worker(*extra_args: str) -> Iterator[int]:
    """Spawn the Go HTTP conformance worker and yield its TCP port."""
    proc = subprocess.Popen(
        [GO_WORKER, *extra_args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("PORT:"), f"Expected PORT:<n>, got: {line!r}"
        port = int(line.split(":", 1)[1])

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
    proc = subprocess.Popen(
        [GO_WORKER, "--unix", path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
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
    proc = subprocess.Popen(
        [GO_WORKER, "--tcp", "127.0.0.1:0"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
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


@pytest.fixture(params=["pipe", "subprocess", "shm", "http", "http_externalize_always", "unix", "tcp"])
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
        if request.param == "pipe":

            @contextlib.contextmanager
            def _pipe_conn() -> Iterator[_RpcProxy]:
                transport = SubprocessTransport([GO_WORKER])
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
                transport = SubprocessTransport([GO_WORKER])
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


@pytest.fixture(params=["pipe", "subprocess", "shm", "http", "http_externalize_always", "unix", "tcp"])
def conformance_describe(
    request: pytest.FixtureRequest,
    go_transport: SubprocessTransport,
    go_http_port: int,
    go_unix_path: str,
    go_tcp_addr: tuple[str, int],
) -> ServiceDescription:
    """Return a ``ServiceDescription`` from a real ``__describe__`` over the wire.

    Parallels ``conformance_conn`` — same transport matrix — but instead of a
    proxy it sends an actual ``__describe__`` request to the Go worker under
    test and parses the response, so ``TestDescribeConformance`` validates
    introspection against the running Go server (not a throwaway in-process
    Python one).  The Go server always exposes ``__describe__``.
    """
    from vgi_rpc.http import http_introspect
    from vgi_rpc.introspect import introspect
    from vgi_rpc.rpc import TcpTransport, UnixTransport

    param = request.param
    if param in ("pipe", "shm"):
        # No describe-specific side channel needed; a fresh stdio worker is the
        # faithful equivalent of Python's fresh in-process pipe server.
        transport = SubprocessTransport([GO_WORKER])
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
        ext_port: int = request.getfixturevalue("conformance_http_externalize_always_port")
        return http_introspect(base_url=f"http://127.0.0.1:{ext_port}")
    return http_introspect(base_url=f"http://127.0.0.1:{go_http_port}")


# Import all tests from the conformance test module (PyPI package)
from vgi_rpc.conformance._pytest_suite import *  # noqa: F401,F403,E402


from vgi_rpc.rpc import AnnotatedBatch, RpcError  # noqa: E402


# Override: allow TestLargeData on all transports (the upstream suite skips
# non-pipe transports, but the Go worker handles them fine).
class TestLargeData(TestLargeData):  # type: ignore[no-redef]  # noqa: F811
    @pytest.fixture(autouse=True)
    def _skip_non_pipe(self) -> None:
        pass


# Override: the Go server drains client input after stream init errors, so
# these tests work on all transports (the upstream suite skips them).
class TestProducerStream(TestProducerStream):  # type: ignore[no-redef]  # noqa: F811
    def test_produce_error_on_init(self, conformance_conn: ConnFactory) -> None:
        with conformance_conn() as proxy, pytest.raises(RpcError, match="intentional init error"):
            list(proxy.produce_error_on_init())


class TestExchangeStream(TestExchangeStream):  # type: ignore[no-redef]  # noqa: F811
    def test_error_on_init(self, conformance_conn: ConnFactory) -> None:
        with conformance_conn() as proxy:
            with pytest.raises(RpcError, match="intentional exchange init error"):
                session = proxy.exchange_error_on_init()
                # HTTP raises during init; pipe/subprocess raises on first exchange.
                session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
