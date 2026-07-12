import pytest

from aml_sentinel.replay import Dataset, load_dataset
from aml_sentinel.replay.loader import DatasetError

from .conftest import FIXTURE_DIR


def test_counts(dataset: Dataset) -> None:
    assert len(dataset.normal) == 180
    assert len(dataset.rings) == 2
    assert [r.depth for r in dataset.rings] == [4, 5]


def test_rings_grouped_correctly(dataset: Dataset) -> None:
    for ring in dataset.rings:
        assert len(ring.hops) == ring.depth
        assert len(ring.accounts) == ring.depth
        # hops form a closed cycle over exactly the involved accounts
        for i, hop in enumerate(ring.hops):
            assert hop.src == ring.accounts[i]
            assert hop.dst == ring.accounts[(i + 1) % ring.depth]
        assert all(hop.amount == 9999.0 for hop in ring.hops)


def test_ring_ids_and_pattern(dataset: Dataset) -> None:
    assert [r.ring_id for r in dataset.rings] == ["pat_0", "pat_1"]
    assert all(r.pattern_type == "cycle" for r in dataset.rings)


def test_missing_dir_raises(tmp_path) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(DatasetError):
        load_dataset(tmp_path)


def test_fixture_dir_exists() -> None:
    assert (FIXTURE_DIR / "fraud" / "fraud_cases.csv").is_file()
