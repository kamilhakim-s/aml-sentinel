"""End-to-end detection over the fixture stream, plus the fraud-free property test."""

import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from aml_sentinel.cli import main
from aml_sentinel.detect import CycleDetection, DetectionService, DetectorSink
from aml_sentinel.models import Transaction
from aml_sentinel.replay import Dataset, SynthesisConfig, replay, synthesize

from .conftest import FIXTURE_DIR


def run_detection(dataset: Dataset, seed: int = 42) -> DetectionService:
    stream, _ = synthesize(dataset, SynthesisConfig(seed=seed))
    service = DetectionService()
    asyncio.run(replay(stream, DetectorSink(service), speed=0))
    return service


def test_every_injected_ring_produces_a_cycle_alert(dataset: Dataset) -> None:
    """Exit criterion: each ring in the fixture yields a cycle alert matching its accounts."""
    stream, ground_truth = synthesize(dataset, SynthesisConfig(seed=42))
    service = DetectionService()
    asyncio.run(replay(stream, DetectorSink(service), speed=0))

    cycle_account_sets = [frozenset(a.accounts) for a in service.alerts if a.typology == "cycle"]
    for gt in ground_truth:
        assert frozenset(gt.accounts) in cycle_account_sets, f"ring {gt.ring_id} missed"


def test_cycle_alert_evidence_matches_ring_transactions(dataset: Dataset) -> None:
    stream, ground_truth = synthesize(dataset, SynthesisConfig(seed=42))
    service = DetectionService()
    asyncio.run(replay(stream, DetectorSink(service), speed=0))

    by_accounts = {frozenset(a.accounts): a for a in service.alerts if a.typology == "cycle"}
    for gt in ground_truth:
        alert = by_accounts[frozenset(gt.accounts)]
        assert {e.tx_id for e in alert.evidence_edges} == set(gt.tx_ids)
        # the alert fires exactly when the ring's closing hop arrives
        assert alert.fired_at == gt.last_hop_time


@settings(max_examples=50, deadline=None)
@given(
    edges=st.lists(
        st.tuples(st.integers(0, 30), st.integers(0, 30)).filter(lambda p: p[0] < p[1]),
        max_size=120,
    ),
    gaps=st.lists(st.integers(1, 3600), min_size=120, max_size=120),
)
def test_no_cycle_alert_on_fraud_free_stream(edges: list[tuple[int, int]], gaps: list[int]) -> None:
    """Property: edges always go low->high account id, so no directed cycle can exist."""
    service = DetectionService(typologies=[CycleDetection()])
    t = datetime(2024, 1, 1)
    for i, (src, dst) in enumerate(edges):
        t += timedelta(seconds=gaps[i])
        service.process(
            Transaction(
                tx_id=f"tx_{i}",
                src=f"acc_{src}",
                dst=f"acc_{dst}",
                amount=9999.0,
                event_time=t,
                description="fraud-free",
            )
        )
    assert [a for a in service.alerts if a.typology == "cycle"] == []


def test_cli_detect(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    alerts_out = tmp_path / "alerts.json"
    rc = main(
        [
            "detect",
            "--data-dir",
            str(FIXTURE_DIR),
            "--alerts-out",
            str(alerts_out),
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "processed 189 transactions" in out
    assert "per-tx latency" in out
    alerts = json.loads(alerts_out.read_text())
    assert sum(1 for a in alerts if a["typology"] == "cycle") >= 2
    assert all(
        {"alert_id", "typology", "fired_at", "accounts", "evidence_edges", "score"} <= set(a)
        for a in alerts
    )
