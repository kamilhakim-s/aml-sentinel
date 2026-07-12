"""Load gen-fraud-graph CSV output into memory.

Expected layout under the data directory (as written by gen-fraud-graph):

    transactions/transactions_<worker>_<batch>.csv   normal traffic
    fraud/transactions_fraud.csv                     injected ring hops
    fraud/fraud_cases.csv                            one row per ring (ground truth)

Transaction CSVs share the header
``tx_id,src_id,dst_id,amount,timestamp,description,embedding``; the upstream
timestamp is a constant placeholder and is discarded here (see timesynth).
The embedding column is never read.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

_TX_COLUMNS = ("tx_id", "src_id", "dst_id", "amount", "timestamp", "description")
_CASE_COLUMNS = ("pattern_id", "start_acc_id", "pattern_type", "depth", "involved_accounts")


@dataclass(frozen=True, slots=True)
class RawTransaction:
    """A transaction row before event-time synthesis."""

    tx_id: str
    src: str
    dst: str
    amount: float
    description: str


@dataclass(frozen=True, slots=True)
class FraudRing:
    """One injected ring: its case metadata plus its hop transactions in order."""

    ring_id: str
    pattern_type: str
    depth: int
    accounts: tuple[str, ...]
    hops: tuple[RawTransaction, ...]


@dataclass(frozen=True, slots=True)
class Dataset:
    normal: tuple[RawTransaction, ...]
    rings: tuple[FraudRing, ...]


class DatasetError(ValueError):
    """Raised when the data directory does not match the expected layout."""


def _read_transactions(path: Path) -> list[RawTransaction]:
    with path.open(newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if header is None or tuple(header[: len(_TX_COLUMNS)]) != _TX_COLUMNS:
            raise DatasetError(f"{path}: unexpected header {header!r}")
        return [
            RawTransaction(
                tx_id=row[0], src=row[1], dst=row[2], amount=float(row[3]), description=row[5]
            )
            for row in reader
        ]


def _read_cases(path: Path) -> list[tuple[str, str, int, tuple[str, ...]]]:
    with path.open(newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if header is None or tuple(header) != _CASE_COLUMNS:
            raise DatasetError(f"{path}: unexpected header {header!r}")
        return [(row[0], row[2], int(row[3]), tuple(row[4].split("|"))) for row in reader]


def load_dataset(data_dir: Path) -> Dataset:
    """Load normal transactions and fraud rings from *data_dir*."""
    tx_dir = data_dir / "transactions"
    normal_files = sorted(tx_dir.glob("transactions_*.csv"))
    if not normal_files:
        raise DatasetError(f"no transaction CSVs found under {tx_dir}")

    normal: list[RawTransaction] = []
    for path in normal_files:
        normal.extend(_read_transactions(path))

    fraud_tx = _read_transactions(data_dir / "fraud" / "transactions_fraud.csv")
    cases = _read_cases(data_dir / "fraud" / "fraud_cases.csv")

    # Fraud hops are written ring by ring, so consecutive runs of `depth`
    # rows belong to one case. Cross-check against involved_accounts.
    if sum(depth for _, _, depth, _ in cases) != len(fraud_tx):
        raise DatasetError(
            f"fraud_cases depths sum to {sum(d for _, _, d, _ in cases)} "
            f"but transactions_fraud.csv has {len(fraud_tx)} rows"
        )

    rings: list[FraudRing] = []
    offset = 0
    for ring_id, pattern_type, depth, accounts in cases:
        hops = tuple(fraud_tx[offset : offset + depth])
        offset += depth
        account_set = set(accounts)
        for hop in hops:
            if hop.src not in account_set or hop.dst not in account_set:
                raise DatasetError(
                    f"ring {ring_id}: hop {hop.tx_id} touches account outside "
                    f"involved_accounts ({hop.src} -> {hop.dst})"
                )
        rings.append(
            FraudRing(
                ring_id=ring_id,
                pattern_type=pattern_type,
                depth=depth,
                accounts=accounts,
                hops=hops,
            )
        )

    return Dataset(normal=tuple(normal), rings=tuple(rings))
