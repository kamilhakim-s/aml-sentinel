"""Pluggable typology rules evaluated per transaction against the rolling graph.

Each rule sees only the windowed graph and the incoming transaction — never
ground truth. Rules return evidence-bearing findings; the detection service
assigns alert ids.
"""

from __future__ import annotations

import statistics
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta

from aml_sentinel.detect.graph import WindowedGraph
from aml_sentinel.models import Edge, Transaction


@dataclass(frozen=True, slots=True)
class Finding:
    """An alert minus its id: what a typology hands back to the service."""

    typology: str
    fired_at: datetime
    accounts: tuple[str, ...]
    evidence_edges: tuple[Edge, ...]
    score: float


class Typology(ABC):
    """A detection rule. Stateful subclasses must keep event-time-bounded state."""

    name: str

    @abstractmethod
    def evaluate(self, graph: WindowedGraph, tx: Transaction) -> list[Finding]: ...


class CycleDetection(Typology):
    """Fires when a new edge closes a directed cycle inside the window.

    On edge src->dst, runs a bounded DFS over out-edges from dst looking for
    src. Depth is capped at ``max_depth`` edges per cycle (upstream rings are
    4-7 hops) and total node expansions at ``budget`` so a pathological hub
    cannot stall the stream.
    """

    name = "cycle"

    def __init__(self, max_depth: int = 7, budget: int = 50_000) -> None:
        self.max_depth = max_depth
        self.budget = budget

    def evaluate(self, graph: WindowedGraph, tx: Transaction) -> list[Finding]:
        if tx.src == tx.dst:
            return []
        closing = Edge.from_transaction(tx)
        cycles = self._find_cycles(graph, closing)
        findings = []
        for cycle in cycles:
            accounts = tuple(e.src for e in cycle)
            findings.append(
                Finding(
                    typology=self.name,
                    fired_at=tx.event_time,
                    accounts=accounts,
                    evidence_edges=tuple(cycle),
                    score=self._score(cycle),
                )
            )
        return findings

    def _find_cycles(self, graph: WindowedGraph, closing: Edge) -> list[list[Edge]]:
        """Paths dst ->* src of at most max_depth-1 edges, each closed by *closing*."""
        target = closing.src
        cycles: list[list[Edge]] = []
        seen_account_sets: set[frozenset[str]] = set()
        expansions = 0
        # Path state: list of edges walked so far from closing.dst.
        stack: list[tuple[str, list[Edge]]] = [(closing.dst, [])]
        while stack:
            node, path = stack.pop()
            expansions += 1
            if expansions > self.budget:
                break
            on_path = {closing.dst, *(e.dst for e in path)}
            for edge in graph.out_edges(node):
                if edge.dst == target:
                    cycle = [closing, *path, edge]
                    key = frozenset(e.src for e in cycle)
                    if key not in seen_account_sets:
                        seen_account_sets.add(key)
                        cycles.append(cycle)
                elif len(path) + 2 < self.max_depth and edge.dst not in on_path:
                    stack.append((edge.dst, [*path, edge]))
        return cycles

    @staticmethod
    def _score(cycle: list[Edge]) -> float:
        """Uniform round-trip amounts are the classic layering signature."""
        amounts = [e.amount for e in cycle]
        mean = statistics.fmean(amounts)
        if mean == 0:
            return 0.5
        cv = statistics.pstdev(amounts) / mean
        return round(1.0 / (1.0 + cv), 4)


class Structuring(Typology):
    """N transactions just under the reporting threshold touching one account in 24h."""

    name = "structuring"

    def __init__(
        self,
        band: tuple[float, float] = (9_000.0, 10_000.0),
        min_count: int = 3,
        window: timedelta = timedelta(hours=24),
    ) -> None:
        self.band = band
        self.min_count = min_count
        self.window = window
        self._recent: defaultdict[str, deque[Edge]] = defaultdict(deque)
        self._last_fired: dict[str, datetime] = {}

    def evaluate(self, graph: WindowedGraph, tx: Transaction) -> list[Finding]:
        lo, hi = self.band
        if not (lo <= tx.amount < hi):
            return []
        edge = Edge.from_transaction(tx)
        findings = []
        for account in (tx.src, tx.dst):
            recent = self._recent[account]
            recent.append(edge)
            cutoff = tx.event_time - self.window
            while recent and recent[0].event_time <= cutoff:
                recent.popleft()
            if len(recent) < self.min_count:
                continue
            # One alert per account per window, or every band tx re-fires.
            last = self._last_fired.get(account)
            if last is not None and tx.event_time - last < self.window:
                continue
            self._last_fired[account] = tx.event_time
            evidence = tuple(recent)
            accounts = tuple(dict.fromkeys(a for e in evidence for a in (e.src, e.dst)))
            findings.append(
                Finding(
                    typology=self.name,
                    fired_at=tx.event_time,
                    accounts=accounts,
                    evidence_edges=evidence,
                    score=round(min(1.0, 0.5 + 0.1 * (len(recent) - self.min_count)), 4),
                )
            )
        return findings


class HighValueDegreeOutlier(Typology):
    """High-value transaction hitting an account whose rolling degree is anomalous.

    A simple statistical rule: the account's total degree in the window is
    compared against the population of active accounts via z-score. Catches
    fan-in/fan-out mule hubs without pattern-matching any injected typology.
    """

    name = "high_value_degree_outlier"

    def __init__(
        self,
        min_amount: float = 5_000.0,
        z_threshold: float = 3.0,
        min_population: int = 50,
        max_evidence_edges: int = 20,
    ) -> None:
        self.min_amount = min_amount
        self.z_threshold = z_threshold
        self.min_population = min_population
        self.max_evidence_edges = max_evidence_edges

    def evaluate(self, graph: WindowedGraph, tx: Transaction) -> list[Finding]:
        if tx.amount < self.min_amount:
            return []
        stats = graph.degree_stats()
        if stats.n_nodes < self.min_population:
            return []
        findings = []
        for account in dict.fromkeys((tx.src, tx.dst)):
            z = stats.z_score(graph.degree(account))
            if z < self.z_threshold:
                continue
            edges = (*graph.in_edges(account), *graph.out_edges(account))
            evidence = tuple(
                sorted(edges, key=lambda e: e.event_time, reverse=True)[: self.max_evidence_edges]
            )
            findings.append(
                Finding(
                    typology=self.name,
                    fired_at=tx.event_time,
                    accounts=(account,),
                    evidence_edges=evidence,
                    score=round(min(1.0, z / (2 * self.z_threshold)), 4),
                )
            )
        return findings


def default_typologies() -> list[Typology]:
    return [CycleDetection(), Structuring(), HighValueDegreeOutlier()]
