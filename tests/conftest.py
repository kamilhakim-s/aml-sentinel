from pathlib import Path

import pytest

from aml_sentinel.replay import Dataset, load_dataset

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "data"


@pytest.fixture(scope="session")
def dataset() -> Dataset:
    return load_dataset(FIXTURE_DIR)
