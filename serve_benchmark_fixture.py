"""Python subprocess worker implementing the same benchmark fixture methods as the Go worker.

Serves: noop, add, greet, roundtrip_types, generate, transform
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol

import pyarrow as pa
import pyarrow.compute as pc

from vgi_rpc.rpc import (
    AnnotatedBatch,
    CallContext,
    OutputCollector,
    Stream,
    StreamState,
    run_server,
)


class Color(Enum):
    RED = "red"
    GREEN = "green"
    BLUE = "blue"


@dataclass
class GenerateState(StreamState):
    count: int
    current: int = 0

    def process(
        self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext
    ) -> None:
        if self.current >= self.count:
            out.finish()
            return
        out.emit_pydict({"i": [self.current], "value": [self.current * 10]})
        self.current += 1


@dataclass
class TransformState(StreamState):
    factor: float

    def process(
        self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext
    ) -> None:
        scaled = pc.multiply(input.batch.column("value"), self.factor)
        out.emit_arrays([scaled])


class BenchmarkService(Protocol):
    def noop(self) -> None: ...
    def add(self, a: float, b: float) -> float: ...
    def greet(self, name: str) -> str: ...
    def roundtrip_types(
        self, color: Color, mapping: dict[str, int], tags: frozenset[int]
    ) -> str: ...
    def generate(self, count: int) -> Stream[StreamState]: ...
    def transform(self, factor: float) -> Stream[StreamState]: ...


class BenchmarkServiceImpl:
    def noop(self) -> None:
        return None

    def add(self, a: float, b: float) -> float:
        return a + b

    def greet(self, name: str) -> str:
        return f"Hello, {name}!"

    def roundtrip_types(
        self, color: Color, mapping: dict[str, int], tags: frozenset[int]
    ) -> str:
        return f"{color.name}:true:{dict(sorted(mapping.items()))}:{sorted(tags)}"

    def generate(self, count: int) -> Stream[GenerateState]:
        schema = pa.schema(
            [pa.field("i", pa.int64()), pa.field("value", pa.int64())]
        )
        return Stream(output_schema=schema, state=GenerateState(count=count))

    def transform(self, factor: float) -> Stream[TransformState]:
        schema = pa.schema([pa.field("value", pa.float64())])
        return Stream(
            output_schema=schema,
            state=TransformState(factor=factor),
            input_schema=schema,
        )


if __name__ == "__main__":
    run_server(BenchmarkService, BenchmarkServiceImpl())
