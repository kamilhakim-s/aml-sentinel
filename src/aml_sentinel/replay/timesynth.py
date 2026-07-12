"""Synthesize event times for the generated dataset.

gen-fraud-graph writes constant placeholder timestamps (2024-01-01T10:00:00
for normal traffic, 12:00:00 for fraud), so the replay engine assigns its own:

* normal transactions land uniformly across a simulated horizon;
* each fraud ring gets a random start, with consecutive hops separated by
  short random gaps so the whole cycle closes well inside a detection window.

All randomness comes from one seeded ``random.Random`` for reproducible runs.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from aml_sentinel.models import RingGroundTruth, Transaction
from aml_sentinel.replay.loader import Dataset, RawTransaction


@dataclass(frozen=True, slots=True)
class SynthesisConfig:
    start: datetime = field(default_factory=lambda: datetime(2024, 1, 1))
    horizon: timedelta = timedelta(days=30)
    min_hop_gap: timedelta = timedelta(minutes=1)
    max_hop_gap: timedelta = timedelta(minutes=30)
    seed: int = 42

    @property
    def max_ring_span(self) -> timedelta:
        """Upper bound on first-to-last hop spacing for the deepest ring (depth 7)."""
        return 6 * self.max_hop_gap


def _uniform_offset(rng: random.Random, span: timedelta) -> timedelta:
    return timedelta(seconds=rng.uniform(0, span.total_seconds()))


def _with_time(raw: RawTransaction, event_time: datetime) -> Transaction:
    return Transaction(
        tx_id=raw.tx_id,
        src=raw.src,
        dst=raw.dst,
        amount=raw.amount,
        event_time=event_time,
        description=raw.description,
    )


def synthesize(
    dataset: Dataset, config: SynthesisConfig | None = None
) -> tuple[list[Transaction], list[RingGroundTruth]]:
    """Assign event times and merge into one time-ordered stream.

    Returns the sorted stream (fraud and normal indistinguishable) and the
    per-ring ground truth, which must only ever reach the scoring module.
    """
    config = config or SynthesisConfig()
    if config.horizon <= config.max_ring_span:
        raise ValueError(
            f"horizon {config.horizon} must exceed the max ring span {config.max_ring_span}"
        )
    rng = random.Random(config.seed)

    stream: list[Transaction] = [
        _with_time(raw, config.start + _uniform_offset(rng, config.horizon))
        for raw in dataset.normal
    ]

    ground_truth: list[RingGroundTruth] = []
    ring_window = config.horizon - config.max_ring_span
    for ring in dataset.rings:
        hop_time = config.start + _uniform_offset(rng, ring_window)
        first_hop = hop_time
        hop_txs: list[Transaction] = []
        for i, raw in enumerate(ring.hops):
            if i > 0:
                gap_s = rng.uniform(
                    config.min_hop_gap.total_seconds(), config.max_hop_gap.total_seconds()
                )
                hop_time += timedelta(seconds=gap_s)
            hop_txs.append(_with_time(raw, hop_time))
        stream.extend(hop_txs)
        ground_truth.append(
            RingGroundTruth(
                ring_id=ring.ring_id,
                accounts=ring.accounts,
                tx_ids=tuple(tx.tx_id for tx in hop_txs),
                first_hop_time=first_hop,
                last_hop_time=hop_time,
            )
        )

    stream.sort(key=lambda tx: (tx.event_time, tx.tx_id))
    return stream, ground_truth
