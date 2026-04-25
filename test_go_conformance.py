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
from vgi_rpc.log import Message
from vgi_rpc.rpc import SubprocessTransport, _RpcProxy, unix_connect

GO_WORKER = os.environ.get(
    "GO_CONFORMANCE_WORKER",
    str(Path(__file__).parent / "conformance-worker"),
)


@pytest.fixture(scope="session")
def go_transport() -> Iterator[SubprocessTransport]:
    transport = SubprocessTransport([GO_WORKER])
    yield transport
    transport.close()


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
        proc.wait(timeout=5)


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
        proc.wait(timeout=5)


ConnFactory = Callable[..., contextlib.AbstractContextManager[Any]]


@pytest.fixture(params=["pipe", "subprocess", "http", "unix"])
def conformance_conn(
    request: pytest.FixtureRequest,
    go_transport: SubprocessTransport,
    go_http_port: int,
    go_unix_path: str,
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
        elif request.param == "http":
            return http_connect(
                ConformanceService,
                f"http://127.0.0.1:{go_http_port}",
                on_log=on_log,
            )
        elif request.param == "unix":
            return unix_connect(
                ConformanceService,
                go_unix_path,
                on_log=on_log,
            )
        else:
            # "subprocess" — shared transport
            @contextlib.contextmanager
            def _conn() -> Iterator[_RpcProxy]:
                yield _RpcProxy(ConformanceService, go_transport, on_log)

            return _conn()

    return factory


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
