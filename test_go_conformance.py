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
def conformance_http_auth_port() -> Iterator[int]:
    """Start a Go HTTP server that rejects every RPC call with 401."""
    yield from _start_http_worker("--http-auth")


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
