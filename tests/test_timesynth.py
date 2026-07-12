import itertools
from datetime import timedelta

import pytest

from aml_sentinel.replay import Dataset, SynthesisConfig, synthesize

CONFIG = SynthesisConfig(seed=123)


def test_deterministic_given_seed(dataset: Dataset) -> None:
    stream_a, gt_a = synthesize(dataset, CONFIG)
    stream_b, gt_b = synthesize(dataset, CONFIG)
    assert stream_a == stream_b
    assert gt_a == gt_b


def test_different_seed_changes_times(dataset: Dataset) -> None:
    stream_a, _ = synthesize(dataset, CONFIG)
    stream_b, _ = synthesize(dataset, SynthesisConfig(seed=124))
    assert stream_a != stream_b


def test_stream_sorted_and_complete(dataset: Dataset) -> None:
    stream, _ = synthesize(dataset, CONFIG)
    assert len(stream) == len(dataset.normal) + sum(r.depth for r in dataset.rings)
    times = [tx.event_time for tx in stream]
    assert times == sorted(times)
    assert len({tx.tx_id for tx in stream}) == len(stream)


def test_all_times_within_horizon(dataset: Dataset) -> None:
    stream, _ = synthesize(dataset, CONFIG)
    end = CONFIG.start + CONFIG.horizon
    assert all(CONFIG.start <= tx.event_time < end for tx in stream)


def test_ring_hops_clustered_and_ordered(dataset: Dataset) -> None:
    _, ground_truth = synthesize(dataset, CONFIG)
    stream, _ = synthesize(dataset, CONFIG)
    by_id = {tx.tx_id: tx for tx in stream}
    for gt in ground_truth:
        hop_times = [by_id[tx_id].event_time for tx_id in gt.tx_ids]
        assert hop_times == sorted(hop_times)
        assert hop_times[0] == gt.first_hop_time
        assert hop_times[-1] == gt.last_hop_time
        span = gt.last_hop_time - gt.first_hop_time
        assert span <= CONFIG.max_ring_span
        gaps = [b - a for a, b in itertools.pairwise(hop_times)]
        assert all(CONFIG.min_hop_gap <= g <= CONFIG.max_hop_gap for g in gaps)


def test_no_fraud_label_on_stream(dataset: Dataset) -> None:
    """The emitted Transaction type must not leak ground truth to the detector."""
    stream, _ = synthesize(dataset, CONFIG)
    fields = set(stream[0].to_dict())
    assert fields == {"tx_id", "src", "dst", "amount", "event_time", "description"}


def test_horizon_too_small_rejected(dataset: Dataset) -> None:
    with pytest.raises(ValueError, match="horizon"):
        synthesize(dataset, SynthesisConfig(horizon=timedelta(hours=2)))
