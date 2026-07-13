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

    @classmethod
    def from_dict(cls, data: dict[str, str | float]) -> Transaction:
        """Inverse of to_dict; used by the Kafka consumer."""
        return cls(
            tx_id=str(data["tx_id"]),
            src=str(data["src"]),
            dst=str(data["dst"]),
            amount=float(data["amount"]),
            event_time=datetime.fromisoformat(str(data["event_time"])),
            description=str(data["description"]),
        )


@dataclass(frozen=True, slots=True)
class Edge:
    """A transaction viewed as a directed graph edge (also used as alert evidence)."""

    tx_id: str
    src: str
    dst: str
    amount: float
    event_time: datetime

    @classmethod
    def from_transaction(cls, tx: Transaction) -> Edge:
        return cls(
            tx_id=tx.tx_id, src=tx.src, dst=tx.dst, amount=tx.amount, event_time=tx.event_time
        )

    def to_dict(self) -> dict[str, str | float]:
        return {
            "tx_id": self.tx_id,
            "src": self.src,
            "dst": self.dst,
            "amount": self.amount,
            "event_time": self.event_time.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class Alert:
    """A typology hit: the accounts involved plus the evidence subgraph edges."""

    alert_id: str
    typology: str
    fired_at: datetime
    accounts: tuple[str, ...]
    evidence_edges: tuple[Edge, ...]
    score: float

    def to_dict(self) -> dict[str, object]:
        return {
            "alert_id": self.alert_id,
            "typology": self.typology,
            "fired_at": self.fired_at.isoformat(),
            "accounts": list(self.accounts),
            "evidence_edges": [e.to_dict() for e in self.evidence_edges],
            "score": self.score,
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
