"""Run Python conformance tests against the Go conformance worker."""
import contextlib
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from typing import Any

import httpx
import pytest

sys.path.insert(0, "/Users/rusty/Development/vgi-rpc")

from vgi_rpc.conformance import ConformanceService
from vgi_rpc.http import http_connect
from vgi_rpc.log import Message
from vgi_rpc.rpc import SubprocessTransport, _RpcProxy

GO_WORKER = "/Users/rusty/Development/vgi-rpc-go/conformance-worker"


@pytest.fixture(scope="session")
def go_transport() -> Iterator[SubprocessTransport]:
    transport = SubprocessTransport([GO_WORKER])
    yield transport
    transport.close()


@pytest.fixture(scope="session")
def go_http_port() -> Iterator[int]:
    """Start Go conformance HTTP server."""
    proc = subprocess.Popen(
        [GO_WORKER, "--http"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("PORT:"), f"Expected PORT:<n>, got: {line!r}"
        port = int(line.split(":", 1)[1])

        # Wait for server to be ready
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            try:
                httpx.get(f"http://127.0.0.1:{port}/", timeout=1.0)
                break
            except (httpx.ConnectError, httpx.ConnectTimeout):
                time.sleep(0.1)
            except httpx.HTTPStatusError:
                break  # Server is up, just returned an error status

        yield port
    finally:
        proc.terminate()
        proc.wait(timeout=5)


ConnFactory = Callable[..., contextlib.AbstractContextManager[Any]]


@pytest.fixture(params=["pipe", "subprocess", "http"])
def conformance_conn(
    request: pytest.FixtureRequest,
    go_transport: SubprocessTransport,
    go_http_port: int,
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
        else:
            # "subprocess" — shared transport
            @contextlib.contextmanager
            def _conn() -> Iterator[_RpcProxy]:
                yield _RpcProxy(ConformanceService, go_transport, on_log)

            return _conn()

    return factory


# Import all tests from the conformance test module
from tests.test_conformance import *  # noqa: F401,F403,E402


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
