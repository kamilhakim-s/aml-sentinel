from datetime import datetime, timedelta

import pytest

from aml_sentinel.detect import WindowedGraph
from aml_sentinel.models import Edge

T0 = datetime(2024, 1, 1)


def edge(tx_id: str, src: str, dst: str, *, hours: float, amount: float = 100.0) -> Edge:
    return Edge(
        tx_id=tx_id, src=src, dst=dst, amount=amount, event_time=T0 + timedelta(hours=hours)
    )


def test_add_and_adjacency() -> None:
    g = WindowedGraph(timedelta(hours=72))
    e1 = edge("t1", "a", "b", hours=0)
    e2 = edge("t2", "b", "c", hours=1)
    g.add(e1)
    g.add(e2)
    assert len(g) == 2
    assert g.out_edges("a") == (e1,)
    assert g.in_edges("c") == (e2,)
    assert g.degree("b") == 2
    assert g.out_degree("b") == 1 and g.in_degree("b") == 1


def test_eviction_by_watermark() -> None:
    g = WindowedGraph(timedelta(hours=72))
    g.add(edge("t1", "a", "b", hours=0))
    g.add(edge("t2", "c", "d", hours=71))
    assert len(g) == 2
    g.add(edge("t3", "e", "f", hours=73))  # t1 is now 73h old -> expired
    assert len(g) == 2
    assert g.out_edges("a") == ()
    assert g.degree("a") == 0
    assert g.degree("c") == 1


def test_eviction_is_exclusive_of_window_edge() -> None:
    g = WindowedGraph(timedelta(hours=72))
    g.add(edge("t1", "a", "b", hours=0))
    g.add(edge("t2", "c", "d", hours=72))  # exactly window-old: still evicted (<=)
    assert g.out_edges("a") == ()
    assert len(g) == 1


def test_out_of_order_edge_rejected() -> None:
    g = WindowedGraph(timedelta(hours=72))
    g.add(edge("t1", "a", "b", hours=5))
    with pytest.raises(ValueError, match="out-of-order"):
        g.add(edge("t2", "b", "c", hours=4))


def test_degree_stats_track_evictions() -> None:
    g = WindowedGraph(timedelta(hours=10))
    g.add(edge("t1", "a", "b", hours=0))
    g.add(edge("t2", "a", "c", hours=1))
    stats = g.degree_stats()
    assert stats.n_nodes == 3
    assert stats.mean == pytest.approx(4 / 3)
    g.add(edge("t3", "x", "y", hours=20))  # evicts both earlier edges
    stats = g.degree_stats()
    assert stats.n_nodes == 2
    assert stats.mean == pytest.approx(1.0)
    assert stats.std == pytest.approx(0.0)


def test_window_must_be_positive() -> None:
    with pytest.raises(ValueError):
        WindowedGraph(timedelta(0))
