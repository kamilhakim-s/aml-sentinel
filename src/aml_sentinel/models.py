"""Core domain models shared across replay, detection, and scoring."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class Transaction:
    """A single transaction event as seen by the detector.

    Carries no fraud label: ground truth flows only to the scoring module.
    """

    tx_id: str
    src: str
    dst: str
    amount: float
    event_time: datetime
    description: str

    def to_dict(self) -> dict[str, str | float]:
        return {
            "tx_id": self.tx_id,
            "src": self.src,
            "dst": self.dst,
            "amount": self.amount,
            "event_time": self.event_time.isoformat(),
            "description": self.description,
        }


@dataclass(frozen=True, slots=True)
class RingGroundTruth:
    """Ground truth for one injected fraud ring, for scoring only."""

    ring_id: str
    accounts: tuple[str, ...]
    tx_ids: tuple[str, ...]
    first_hop_time: datetime
    last_hop_time: datetime

    def to_dict(self) -> dict[str, object]:
        return {
            "ring_id": self.ring_id,
            "accounts": list(self.accounts),
            "tx_ids": list(self.tx_ids),
            "first_hop_time": self.first_hop_time.isoformat(),
            "last_hop_time": self.last_hop_time.isoformat(),
        }
