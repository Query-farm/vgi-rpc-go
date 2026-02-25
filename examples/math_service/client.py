"""Demo client for the math_service Go server.

Demonstrates all four RPC method types (unary, producer, exchange) across
both subprocess (stdio) and HTTP transports.

Requires the vgi_rpc package (activate the venv first):
    source /Users/rusty/Development/vgi-rpc/.venv/bin/activate
    python examples/math_service/client.py
"""

from __future__ import annotations

import subprocess
import sys
import time
from typing import Protocol

sys.path.insert(0, "/Users/rusty/Development/vgi-rpc")

from vgi_rpc.http import http_connect
from vgi_rpc.rpc import AnnotatedBatch, RpcConnection, Stream, StreamState, SubprocessTransport

BINARY = "./math-service"


# ---------------------------------------------------------------------------
# Protocol definition — matches the Go server's registered methods
# ---------------------------------------------------------------------------


class MathService(Protocol):
    def add(self, a: float, b: float) -> float: ...
    def multiply(self, a: float, b: float) -> float: ...
    def countdown(self, start: int) -> Stream[StreamState]: ...
    def running_sum(self, initial: float = 0.0) -> Stream[StreamState]: ...


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def demo_all_methods(proxy: MathService, transport_name: str) -> None:
    """Exercise every method on the given proxy."""
    print(f"\n{'=' * 50}")
    print(f"  {transport_name} transport")
    print(f"{'=' * 50}")

    # --- Unary: add ---
    result = proxy.add(a=3.0, b=4.0)
    print(f"\nadd(3, 4) = {result}")
    assert result == 7.0

    # --- Unary: multiply ---
    result = proxy.multiply(a=6.0, b=7.0)
    print(f"multiply(6, 7) = {result}")
    assert result == 42.0

    # --- Producer: countdown ---
    print("\ncountdown(start=5):")
    batches = list(proxy.countdown(start=5))
    for ab in batches:
        val = ab.batch.column("value")[0].as_py()
        print(f"  value={val}")
    assert len(batches) == 6  # 5, 4, 3, 2, 1, 0
    assert batches[0].batch.column("value")[0].as_py() == 5
    assert batches[-1].batch.column("value")[0].as_py() == 0

    # --- Exchange: running_sum ---
    print("\nrunning_sum(initial=0):")
    with proxy.running_sum(initial=0.0) as session:
        for values in [[1.0, 2.0], [3.0], [4.0, 5.0]]:
            inp = AnnotatedBatch.from_pydict({"value": values})
            out = session.exchange(inp)
            running = out.batch.column("sum")[0].as_py()
            print(f"  sent {values} -> sum={running}")

    print("\nAll assertions passed!")


# ---------------------------------------------------------------------------
# Subprocess transport demo
# ---------------------------------------------------------------------------


def demo_subprocess() -> None:
    transport = SubprocessTransport([BINARY])
    try:
        with RpcConnection(MathService, transport) as proxy:  # type: ignore[type-abstract]
            demo_all_methods(proxy, "subprocess (stdio)")
    finally:
        transport.close()


# ---------------------------------------------------------------------------
# HTTP transport demo
# ---------------------------------------------------------------------------


def demo_http() -> None:
    proc = subprocess.Popen(
        [BINARY, "--http"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("PORT:"), f"Expected PORT:<n>, got: {line!r}"
        port = int(line.split(":", 1)[1])

        # Wait for server readiness
        import httpx

        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            try:
                httpx.get(f"http://127.0.0.1:{port}/", timeout=1.0)
                break
            except (httpx.ConnectError, httpx.ConnectTimeout):
                time.sleep(0.1)
            except httpx.HTTPStatusError:
                break

        with http_connect(MathService, f"http://127.0.0.1:{port}") as proxy:  # type: ignore[type-abstract]
            demo_all_methods(proxy, "HTTP")
    finally:
        proc.terminate()
        proc.wait(timeout=5)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Build the server binary
    print("Building math-service binary...")
    subprocess.run(
        ["go", "build", "-o", BINARY, "./examples/math_service/"],
        check=True,
        cwd="/Users/rusty/Development/vgi-rpc-go",
    )
    print("Build complete.")

    demo_subprocess()
    demo_http()

    print("\nAll demos completed successfully.")
