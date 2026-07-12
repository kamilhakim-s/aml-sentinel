import asyncio
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from aml_sentinel.cli import main
from aml_sentinel.detect import DetectionService, DetectorSink
from aml_sentinel.models import Alert, Edge, RingGroundTruth
from aml_sentinel.replay import Dataset, SynthesisConfig, replay, synthesize
from aml_sentinel.scoring import score_alerts

from .conftest import FIXTURE_DIR

T0 = datetime(2024, 1, 1)


def edge(tx_id: str, *, hours: float = 0.0) -> Edge:
    return Edge(
        tx_id=tx_id, src="a", dst="b", amount=9999.0, event_time=T0 + timedelta(hours=hours)
    )


def alert(alert_id: str, typology: str, tx_ids: list[str], *, fired_hours: float = 1.0) -> Alert:
    return Alert(
        alert_id=alert_id,
        typology=typology,
        fired_at=T0 + timedelta(hours=fired_hours),
        accounts=("a", "b"),
        evidence_edges=tuple(edge(t) for t in tx_ids),
        score=1.0,
    )


def ring(ring_id: str, tx_ids: list[str], *, last_hop_hours: float = 1.0) -> RingGroundTruth:
    return RingGroundTruth(
        ring_id=ring_id,
        accounts=("a", "b"),
        tx_ids=tuple(tx_ids),
        first_hop_time=T0,
        last_hop_time=T0 + timedelta(hours=last_hop_hours),
    )


class TestScoreAlerts:
    def test_perfect_detection(self) -> None:
        alerts = [alert("A1", "cycle", ["f1", "f2"], fired_hours=1.0)]
        truth = [ring("r1", ["f1", "f2"], last_hop_hours=1.0)]
        report = score_alerts(alerts, truth)
        m = report.per_typology[0]
        assert m.typology == "cycle"
        assert m.precision == 1.0
        assert m.recall == 1.0
        assert m.mean_latency_s == 0.0

    def test_false_positive_lowers_precision_not_recall(self) -> None:
        alerts = [
            alert("A1", "cycle", ["f1"]),
            alert("A2", "cycle", ["benign_tx"]),
        ]
        truth = [ring("r1", ["f1", "f2"])]
        m = score_alerts(alerts, truth).per_typology[0]
        assert m.precision == 0.5
        assert m.false_positives == 1
        assert m.recall == 1.0

    def test_missed_ring_lowers_recall(self) -> None:
        alerts = [alert("A1", "cycle", ["f1"])]
        truth = [ring("r1", ["f1"]), ring("r2", ["g1"])]
        m = score_alerts(alerts, truth).per_typology[0]
        assert m.recall == 0.5
        assert m.rings_detected == 1

    def test_latency_uses_earliest_matching_alert(self) -> None:
        alerts = [
            alert("A1", "cycle", ["f1"], fired_hours=3.0),
            alert("A2", "cycle", ["f1"], fired_hours=2.0),
        ]
        truth = [ring("r1", ["f1"], last_hop_hours=1.0)]
        m = score_alerts(alerts, truth).per_typology[0]
        assert m.mean_latency_s == 3600.0  # 2h alert minus 1h last hop

    def test_no_alerts_gives_null_precision(self) -> None:
        report = score_alerts([], [ring("r1", ["f1"])])
        assert report.overall.precision is None
        assert report.overall.recall == 0.0
        assert report.overall.mean_latency_s is None

    def test_markdown_table_shape(self) -> None:
        report = score_alerts([alert("A1", "cycle", ["f1"])], [ring("r1", ["f1"])])
        md = report.to_markdown()
        lines = md.splitlines()
        assert lines[0].startswith("| Typology |")
        assert any(line.startswith("| cycle |") for line in lines)
        assert any(line.startswith("| overall |") for line in lines)


def test_fixture_pipeline_scores_full_cycle_recall(dataset: Dataset) -> None:
    """Both fixture rings must be caught by cycle alerts with zero detection latency."""
    stream, ground_truth = synthesize(dataset, SynthesisConfig(seed=42))
    service = DetectionService()
    asyncio.run(replay(stream, DetectorSink(service), speed=0))

    report = score_alerts(service.alerts, ground_truth)
    cycle = next(m for m in report.per_typology if m.typology == "cycle")
    assert cycle.recall == 1.0
    assert cycle.rings_detected == 2
    assert cycle.median_latency_s == 0.0  # cycle fires on the closing hop
    assert report.overall.recall == 1.0


def test_cli_score(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    report_out = tmp_path / "report.md"
    rc = main(["score", "--data-dir", str(FIXTURE_DIR), "--report-out", str(report_out)])
    assert rc == 0
    out = capsys.readouterr().out
    assert "| Typology |" in out
    assert "| overall |" in out
    assert report_out.is_file()
    assert (tmp_path / "report.json").is_file()
