"""Emit a synthesized stream at a configurable speed-up factor."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from aml_sentinel.models import Transaction


class Sink(Protocol):
    """Destination for replayed transactions (detector, Kafka producer, file...)."""

    async def emit(self, tx: Transaction) -> None: ...


class CollectingSink:
    """In-process sink that buffers everything; the `--direct` mode default."""

    def __init__(self) -> None:
        self.transactions: list[Transaction] = []

    async def emit(self, tx: Transaction) -> None:
        self.transactions.append(tx)


@dataclass(frozen=True, slots=True)
class ReplayStats:
    count: int
    wall_seconds: float
    simulated_span_seconds: float


async def replay(stream: Sequence[Transaction], sink: Sink, *, speed: float = 0) -> ReplayStats:
    """Push *stream* (already time-ordered) into *sink*.

    ``speed`` is simulated seconds per wall-clock second: 3600 replays an hour
    of activity per second. ``speed <= 0`` emits as fast as possible.
    """
    started = time.perf_counter()
    prev_event = stream[0].event_time if stream else None

    for tx in stream:
        assert prev_event is not None
        if tx.event_time < prev_event:
            raise ValueError(f"stream not time-ordered at {tx.tx_id}")
        if speed > 0:
            delay = (tx.event_time - prev_event).total_seconds() / speed
            if delay > 0:
                await asyncio.sleep(delay)
        prev_event = tx.event_time
        await sink.emit(tx)

    wall = time.perf_counter() - started
    span = (
        (stream[-1].event_time - stream[0].event_time).total_seconds() if len(stream) > 1 else 0.0
    )
    return ReplayStats(count=len(stream), wall_seconds=wall, simulated_span_seconds=span)
