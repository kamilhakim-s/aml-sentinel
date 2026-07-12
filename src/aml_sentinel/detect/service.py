"""Detection service: consume the stream, keep the rolling graph, emit alerts."""

from __future__ import annotations

import statistics
import time
from collections import Counter
from dataclasses import dataclass
from datetime import timedelta

from aml_sentinel.detect.graph import WindowedGraph
from aml_sentinel.detect.typologies import Typology, default_typologies
from aml_sentinel.models import Alert, Edge, Transaction

DEFAULT_WINDOW = timedelta(hours=72)


@dataclass(frozen=True, slots=True)
class DetectionStats:
    transactions: int
    alerts_by_typology: dict[str, int]
    latency_p50_us: float
    latency_p99_us: float
    latency_max_us: float


class DetectionService:
    """Feeds each transaction into the graph, then evaluates every typology."""

    def __init__(
        self,
        window: timedelta = DEFAULT_WINDOW,
        typologies: list[Typology] | None = None,
    ) -> None:
        self.graph = WindowedGraph(window)
        self.typologies = default_typologies() if typologies is None else typologies
        self.alerts: list[Alert] = []
        self._latencies_us: list[float] = []
        self._counter = 0

    def process(self, tx: Transaction) -> list[Alert]:
        """Process one transaction; returns alerts fired by it."""
        started = time.perf_counter()
        self.graph.add(Edge.from_transaction(tx))
        fired: list[Alert] = []
        for typology in self.typologies:
            for finding in typology.evaluate(self.graph, tx):
                self._counter += 1
                fired.append(
                    Alert(
                        alert_id=f"AL-{self._counter:06d}",
                        typology=finding.typology,
                        fired_at=finding.fired_at,
                        accounts=finding.accounts,
                        evidence_edges=finding.evidence_edges,
                        score=finding.score,
                    )
                )
        self.alerts.extend(fired)
        self._latencies_us.append((time.perf_counter() - started) * 1e6)
        return fired

    def stats(self) -> DetectionStats:
        counts = Counter(alert.typology for alert in self.alerts)
        lat = sorted(self._latencies_us)
        if not lat:
            return DetectionStats(0, dict(counts), 0.0, 0.0, 0.0)
        return DetectionStats(
            transactions=len(lat),
            alerts_by_typology=dict(counts),
            latency_p50_us=statistics.median(lat),
            latency_p99_us=lat[min(len(lat) - 1, int(len(lat) * 0.99))],
            latency_max_us=lat[-1],
        )


class DetectorSink:
    """Replay `Sink` adapter so the replay engine can stream straight into detection."""

    def __init__(self, service: DetectionService) -> None:
        self.service = service

    async def emit(self, tx: Transaction) -> None:
        self.service.process(tx)
