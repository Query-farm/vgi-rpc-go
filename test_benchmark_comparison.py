"""Benchmark comparison: Python vgi-rpc vs Go vgi-rpc-go.

Runs identical RPC benchmarks against both the Python and Go subprocess workers
using pytest-benchmark. The Go worker must be pre-built at ./benchmark-worker.

Run with:
    source /Users/rusty/Development/vgi-rpc/.venv/bin/activate
    python -m pytest test_benchmark_comparison.py --benchmark-enable --benchmark-only \
        -o "addopts=" --timeout=300 -v
"""

from __future__ import annotations

import contextlib
import os
import sys
from collections.abc import Callable, Iterator
from enum import Enum
from typing import Any, Protocol

import pyarrow as pa
import pytest

# Add the vgi-rpc package to the path
sys.path.insert(0, "/Users/rusty/Development/vgi-rpc")

from pytest_benchmark.fixture import BenchmarkFixture
from vgi_rpc.rpc import (
    AnnotatedBatch,
    Stream,
    StreamState,
    SubprocessTransport,
    _RpcProxy,
)


# ---------------------------------------------------------------------------
# Service Protocol (subset matching both Python and Go workers)
# ---------------------------------------------------------------------------


class Color(Enum):
    RED = "red"
    GREEN = "green"
    BLUE = "blue"


class BenchmarkService(Protocol):
    """Service protocol for benchmark methods — implemented by both workers."""

    def noop(self) -> None: ...
    def add(self, a: float, b: float) -> float: ...
    def greet(self, name: str) -> str: ...
    def roundtrip_types(
        self, color: Color, mapping: dict[str, int], tags: frozenset[int]
    ) -> str: ...
    def generate(self, count: int) -> Stream[StreamState]: ...
    def transform(self, factor: float) -> Stream[StreamState]: ...


# ---------------------------------------------------------------------------
# Worker commands
# ---------------------------------------------------------------------------

_GO_WORKER = os.path.join(os.path.dirname(__file__), "benchmark-worker")
_PYTHON_WORKER = [
    sys.executable,
    os.path.join(
        os.path.dirname(__file__),
        "serve_benchmark_fixture.py",
    ),
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

ConnFactory = Callable[..., contextlib.AbstractContextManager[Any]]


@pytest.fixture(scope="session")
def go_transport() -> Iterator[SubprocessTransport]:
    """Session-scoped Go subprocess transport."""
    transport = SubprocessTransport([_GO_WORKER])
    yield transport
    transport.close()


@pytest.fixture(scope="session")
def python_transport() -> Iterator[SubprocessTransport]:
    """Session-scoped Python subprocess transport."""
    transport = SubprocessTransport(_PYTHON_WORKER)
    yield transport
    transport.close()


@pytest.fixture(params=["go", "python"])
def make_conn(
    request: pytest.FixtureRequest,
    go_transport: SubprocessTransport,
    python_transport: SubprocessTransport,
) -> ConnFactory:
    """Return a factory that creates an RPC connection context manager."""

    def factory() -> contextlib.AbstractContextManager[Any]:
        if request.param == "go":
            transport = go_transport
        else:
            transport = python_transport

        @contextlib.contextmanager
        def _conn() -> Iterator[_RpcProxy]:
            yield _RpcProxy(BenchmarkService, transport)

        return _conn()

    return factory


# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------


class TestUnaryBenchmarks:
    """Unary RPC benchmarks."""

    def test_noop(
        self, benchmark: BenchmarkFixture, make_conn: ConnFactory
    ) -> None:
        """Minimum framework overhead — noop call."""
        with make_conn() as proxy:
            benchmark(proxy.noop)

    def test_add(
        self, benchmark: BenchmarkFixture, make_conn: ConnFactory
    ) -> None:
        """Primitive params + return — add(a, b)."""
        with make_conn() as proxy:
            benchmark(proxy.add, a=1.0, b=2.0)

    def test_greet(
        self, benchmark: BenchmarkFixture, make_conn: ConnFactory
    ) -> None:
        """String params — greet(name)."""
        with make_conn() as proxy:
            benchmark(proxy.greet, name="benchmark")

    def test_roundtrip_types(
        self, benchmark: BenchmarkFixture, make_conn: ConnFactory
    ) -> None:
        """Complex types — roundtrip_types(color, mapping, tags)."""
        with make_conn() as proxy:
            benchmark(
                proxy.roundtrip_types,
                color=Color.GREEN,
                mapping={"x": 1},
                tags=frozenset({7}),
            )


class TestStreamBenchmarks:
    """Streaming RPC benchmarks."""

    def test_producer(
        self, benchmark: BenchmarkFixture, make_conn: ConnFactory
    ) -> None:
        """Producer stream throughput — generate(count=50)."""

        def run() -> list[Any]:
            with make_conn() as proxy:
                return list(proxy.generate(count=50))

        benchmark(run)

    def test_exchange(
        self, benchmark: BenchmarkFixture, make_conn: ConnFactory
    ) -> None:
        """Exchange throughput — 20 exchanges via transform."""

        def run() -> list[Any]:
            with make_conn() as proxy:
                session = proxy.transform(factor=2.0)
                results = []
                for i in range(20):
                    batch = AnnotatedBatch(
                        batch=pa.RecordBatch.from_pydict({"value": [float(i)]})
                    )
                    results.append(session.exchange(batch))
                session.close()
                return results

        benchmark(run)
