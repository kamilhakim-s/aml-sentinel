"""Rolling transaction graph with event-time window eviction.

Edges arrive in nondecreasing event-time order (the replay engine sorts the
stream), so eviction is a matter of popping from the front of time-ordered
deques: one global deque drives expiry, and because per-node adjacency deques
are appended in the same global order, the expired edge is always at the front
of its endpoints' deques too — eviction is O(1) per edge.

The graph also maintains aggregate degree statistics (sum and sum of squares
of total per-node degree) incrementally, so statistical rules can ask for a
population mean/std in O(1) instead of scanning all accounts per transaction.
"""

from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta

from aml_sentinel.models import Edge


@dataclass(frozen=True, slots=True)
class DegreeStats:
    """Population statistics over total degree of accounts active in the window."""

    n_nodes: int
    mean: float
    std: float

    def z_score(self, degree: int) -> float:
        if self.std == 0:
            return 0.0
        return (degree - self.mean) / self.std


class WindowedGraph:
    """Directed multigraph of transactions inside a rolling event-time window."""

    def __init__(self, window: timedelta) -> None:
        if window <= timedelta(0):
            raise ValueError("window must be positive")
        self.window = window
        self._edges: deque[Edge] = deque()
        self._out: defaultdict[str, deque[Edge]] = defaultdict(deque)
        self._in: defaultdict[str, deque[Edge]] = defaultdict(deque)
        self._degree: defaultdict[str, int] = defaultdict(int)
        self._deg_sum = 0
        self._deg_sumsq = 0
        self._watermark: datetime | None = None

    def __len__(self) -> int:
        return len(self._edges)

    @property
    def watermark(self) -> datetime | None:
        return self._watermark

    def add(self, edge: Edge) -> None:
        """Insert *edge*, advancing the watermark and evicting expired edges first."""
        if self._watermark is not None and edge.event_time < self._watermark:
            raise ValueError(
                f"out-of-order edge {edge.tx_id}: {edge.event_time} < watermark {self._watermark}"
            )
        self._watermark = edge.event_time
        self._evict(edge.event_time - self.window)
        self._edges.append(edge)
        self._out[edge.src].append(edge)
        self._in[edge.dst].append(edge)
        self._bump_degree(edge.src, +1)
        self._bump_degree(edge.dst, +1)

    def _evict(self, horizon: datetime) -> None:
        """Drop every edge with event_time <= *horizon* (strictly older than the window)."""
        while self._edges and self._edges[0].event_time <= horizon:
            edge = self._edges.popleft()
            out_q = self._out[edge.src]
            assert out_q[0] is edge
            out_q.popleft()
            if not out_q:
                del self._out[edge.src]
            in_q = self._in[edge.dst]
            assert in_q[0] is edge
            in_q.popleft()
            if not in_q:
                del self._in[edge.dst]
            self._bump_degree(edge.src, -1)
            self._bump_degree(edge.dst, -1)

    def _bump_degree(self, node: str, delta: int) -> None:
        old = self._degree[node]
        new = old + delta
        self._deg_sum += delta
        self._deg_sumsq += new * new - old * old
        if new == 0:
            del self._degree[node]
        else:
            self._degree[node] = new

    def out_edges(self, node: str) -> tuple[Edge, ...]:
        return tuple(self._out.get(node, ()))

    def in_edges(self, node: str) -> tuple[Edge, ...]:
        return tuple(self._in.get(node, ()))

    def out_degree(self, node: str) -> int:
        return len(self._out.get(node, ()))

    def in_degree(self, node: str) -> int:
        return len(self._in.get(node, ()))

    def degree(self, node: str) -> int:
        return self._degree.get(node, 0)

    def degree_stats(self) -> DegreeStats:
        n = len(self._degree)
        if n == 0:
            return DegreeStats(n_nodes=0, mean=0.0, std=0.0)
        mean = self._deg_sum / n
        var = max(self._deg_sumsq / n - mean * mean, 0.0)
        return DegreeStats(n_nodes=n, mean=mean, std=math.sqrt(var))
