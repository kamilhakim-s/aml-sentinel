import asyncio
import json
from pathlib import Path

import pytest

from aml_sentinel.cli import main
from aml_sentinel.models import Transaction
from aml_sentinel.replay import CollectingSink, Dataset, SynthesisConfig, replay, synthesize

from .conftest import FIXTURE_DIR


def test_replay_full_speed_preserves_order_and_count(dataset: Dataset) -> None:
    stream, _ = synthesize(dataset, SynthesisConfig(seed=1))
    sink = CollectingSink()
    stats = asyncio.run(replay(stream, sink, speed=0))
    assert stats.count == len(stream)
    assert sink.transactions == stream
    assert stats.simulated_span_seconds > 0


def test_replay_rejects_unordered_stream(dataset: Dataset) -> None:
    stream, _ = synthesize(dataset, SynthesisConfig(seed=1))
    shuffled = [stream[5], stream[0]]
    with pytest.raises(ValueError, match="not time-ordered"):
        asyncio.run(replay(shuffled, CollectingSink(), speed=0))


def test_replay_empty_stream() -> None:
    stats = asyncio.run(replay([], CollectingSink(), speed=0))
    assert stats.count == 0


def test_replay_speed_paces_emission(dataset: Dataset) -> None:
    stream, _ = synthesize(dataset, SynthesisConfig(seed=1))
    sim_span = (stream[-1].event_time - stream[0].event_time).total_seconds()
    # pick a speed that should stretch the replay to ~0.2s of wall time
    speed = sim_span / 0.2
    stats = asyncio.run(replay(stream, CollectingSink(), speed=speed))
    assert stats.wall_seconds >= 0.15


def test_cli_replay_direct(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    gt_out = tmp_path / "gt.json"
    rc = main(
        [
            "replay",
            "--data-dir",
            str(FIXTURE_DIR),
            "--direct",
            "--seed",
            "42",
            "--ground-truth-out",
            str(gt_out),
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "replayed 189 transactions" in out
    ground_truth = json.loads(gt_out.read_text())
    assert [gt["ring_id"] for gt in ground_truth] == ["pat_0", "pat_1"]
    assert all(gt["first_hop_time"] <= gt["last_hop_time"] for gt in ground_truth)


def test_cli_requires_direct(capsys: pytest.CaptureFixture[str]) -> None:
    rc = main(["replay", "--data-dir", str(FIXTURE_DIR)])
    assert rc == 2
    assert "--direct" in capsys.readouterr().err


def test_transaction_to_dict_roundtrip(dataset: Dataset) -> None:
    stream, _ = synthesize(dataset, SynthesisConfig(seed=1))
    tx = stream[0]
    d = tx.to_dict()
    assert isinstance(tx, Transaction)
    assert d["tx_id"] == tx.tx_id
    assert d["event_time"] == tx.event_time.isoformat()
