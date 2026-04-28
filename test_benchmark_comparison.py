"""Benchmark comparison: Python vgi-rpc vs Go vgi-rpc-go.

Runs identical RPC benchmarks against both the Python and Go subprocess workers
using pytest-benchmark. The Go worker must be pre-built at ./benchmark-worker.

Requires: pip install "vgi-rpc" pytest pytest-benchmark

Run with:
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
_RUST_WORKER = os.environ.get(
    "RUST_BENCH_WORKER",
    os.path.expanduser("~/Development/vgi-rpc-rust/target/release/vgi-rpc-benchmark-rust"),
)


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


@pytest.fixture(scope="session")
def rust_transport() -> Iterator[SubprocessTransport]:
    """Session-scoped Rust subprocess transport."""
    if not os.path.exists(_RUST_WORKER):
        pytest.skip(f"Rust worker not built at {_RUST_WORKER}")
    transport = SubprocessTransport([_RUST_WORKER])
    yield transport
    transport.close()


class _ShmAdapter:
    """Wraps a SubprocessTransport with a shared memory side-channel.

    Mirrors ``scripts/bench_shm.py`` in vgi-rpc-rust. The proxy auto-uses
    shm whenever ``transport.shm`` is non-None and the worker speaks the
    shm metadata protocol (Rust + Python; Go does not).
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


@pytest.fixture(params=["go", "python", "rust", "go_shm", "python_shm", "rust_shm"])
def make_conn(
    request: pytest.FixtureRequest,
    go_transport: SubprocessTransport,
    python_transport: SubprocessTransport,
) -> ConnFactory:
    """Return a factory that creates an RPC connection context manager."""

    def factory() -> contextlib.AbstractContextManager[Any]:
        from vgi_rpc.shm import ShmSegment

        param = request.param
        shm_segment = None
        if param == "go":
            transport: Any = go_transport
        elif param == "rust":
            transport = request.getfixturevalue("rust_transport")
        elif param == "python":
            transport = python_transport
        elif param == "go_shm":
            shm_segment = ShmSegment.create(32 * 1024 * 1024)
            transport = _ShmAdapter(go_transport, shm_segment)
        elif param == "python_shm":
            shm_segment = ShmSegment.create(32 * 1024 * 1024)
            transport = _ShmAdapter(python_transport, shm_segment)
        elif param == "rust_shm":
            shm_segment = ShmSegment.create(32 * 1024 * 1024)
            transport = _ShmAdapter(request.getfixturevalue("rust_transport"), shm_segment)
        else:
            raise ValueError(f"unknown param {param!r}")

        @contextlib.contextmanager
        def _conn() -> Iterator[_RpcProxy]:
            try:
                yield _RpcProxy(BenchmarkService, transport)
            finally:
                if shm_segment is not None:
                    with contextlib.suppress(BufferError):
                        shm_segment.close()
                    shm_segment.unlink()

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
        self, benchmark: BenchmarkFixture, make_conn: ConnFactory, request: pytest.FixtureRequest
    ) -> None:
        """Complex types — roundtrip_types(color, mapping, tags)."""
        if "rust" in request.node.name:
            pytest.skip("Rust benchmark worker omits roundtrip_types (no dict/set param support)")
        if "go" not in request.node.name and "python" not in request.node.name:
            pytest.skip("only Go and Python expose roundtrip_types")
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


class TestLargePayloadBenchmarks:
    """Large-batch round-trip benchmarks — the regime where shm should pay off.

    Uses ``transform`` with single Float64 batches sized 10k / 100k / 1M
    rows (= 80 KB / 800 KB / 8 MB). The batch reuses one pyarrow array
    across iterations so the timing isolates RPC + IPC + (optional) shm
    transport, not Python-side batch construction.
    """

    @pytest.mark.parametrize("rows", [10_000, 100_000, 1_000_000])
    def test_exchange_large(
        self,
        benchmark: BenchmarkFixture,
        make_conn: ConnFactory,
        rows: int,
        request: pytest.FixtureRequest,
    ) -> None:
        # Skip 1M-row case for plain pipe variants on Python — pure-Python
        # IPC framing is slow enough at that size to dominate the suite.
        # Keep it for the others so we can see shm vs pipe at 8 MB.
        if rows >= 1_000_000 and "shm" not in request.node.name and "python" in request.node.name:
            pytest.skip("skipping 1M-row Python pipe (too slow without shm)")

        sample = AnnotatedBatch(
            batch=pa.RecordBatch.from_pydict(
                {"value": pa.array([float(i) for i in range(rows)], type=pa.float64())}
            )
        )

        with make_conn() as proxy:
            session = proxy.transform(factor=1.0)
            try:
                # Warmup: prime allocators, attach shm segment if applicable.
                for _ in range(3):
                    session.exchange(sample)

                def run() -> Any:
                    return session.exchange(sample)

                benchmark(run)
            finally:
                session.close()

    def test_producer_large(
        self,
        benchmark: BenchmarkFixture,
        make_conn: ConnFactory,
    ) -> None:
        """Producer stream of many small batches — generate(count=2000).

        Stresses per-batch framing overhead (no shm benefit at 1 row/batch);
        included as a contrast to the wide single-batch exchange tests.
        """

        def run() -> int:
            with make_conn() as proxy:
                return sum(1 for _ in proxy.generate(count=2000))

        benchmark(run)
