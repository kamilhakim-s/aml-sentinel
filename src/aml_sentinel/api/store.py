"""Case persistence: one row per ingested alert, SQLite by default.

SQLAlchemy 2.0 declarative models plus a thin repository so the FastAPI
endpoints stay free of session plumbing. Postgres works by swapping the URL
(docker compose does this in Phase 4).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import JSON, DateTime, Engine, Float, String, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

OPEN = "open"
DISPOSED = "disposed"
DISPOSITIONS = ("true_positive", "false_positive")


class Base(DeclarativeBase):
    pass


class CaseRow(Base):
    __tablename__ = "cases"

    case_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    alert_id: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    typology: Mapped[str] = mapped_column(String(64), index=True)
    fired_at: Mapped[datetime] = mapped_column(DateTime)
    score: Mapped[float] = mapped_column(Float)
    accounts: Mapped[list[str]] = mapped_column(JSON)
    evidence_edges: Mapped[list[dict[str, Any]]] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(16), default=OPEN, index=True)
    disposition: Mapped[str | None] = mapped_column(String(16), default=None)
    disposition_notes: Mapped[str | None] = mapped_column(String(2000), default=None)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(UTC).replace(tzinfo=None)
    )
    disposed_at: Mapped[datetime | None] = mapped_column(DateTime, default=None)


class DuplicateAlertError(ValueError):
    """An alert with this alert_id has already been ingested."""


class CaseStore:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        Base.metadata.create_all(engine)

    def create_case(
        self,
        *,
        alert_id: str,
        typology: str,
        fired_at: datetime,
        score: float,
        accounts: list[str],
        evidence_edges: list[dict[str, Any]],
    ) -> CaseRow:
        with Session(self.engine) as session:
            existing = session.scalar(select(CaseRow).where(CaseRow.alert_id == alert_id))
            if existing is not None:
                raise DuplicateAlertError(alert_id)
            row = CaseRow(
                alert_id=alert_id,
                typology=typology,
                fired_at=fired_at,
                score=score,
                accounts=accounts,
                evidence_edges=evidence_edges,
            )
            session.add(row)
            session.commit()
            session.refresh(row)
            return row

    def get_case(self, case_id: int) -> CaseRow | None:
        with Session(self.engine) as session:
            return session.get(CaseRow, case_id)

    def list_cases(
        self,
        *,
        typology: str | None = None,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[CaseRow]:
        stmt = select(CaseRow).order_by(CaseRow.fired_at.desc(), CaseRow.case_id.desc())
        if typology is not None:
            stmt = stmt.where(CaseRow.typology == typology)
        if status is not None:
            stmt = stmt.where(CaseRow.status == status)
        stmt = stmt.limit(limit).offset(offset)
        with Session(self.engine) as session:
            return list(session.scalars(stmt))

    def stats(self) -> dict[str, Any]:
        """Case-queue counters for the UI metrics panel."""
        with Session(self.engine) as session:
            rows = session.scalars(select(CaseRow)).all()
        by_typology: dict[str, int] = {}
        counters = {"total": 0, "open": 0, "disposed": 0, "true_positive": 0, "false_positive": 0}
        for row in rows:
            counters["total"] += 1
            counters[row.status] += 1
            if row.disposition is not None:
                counters[row.disposition] += 1
            by_typology[row.typology] = by_typology.get(row.typology, 0) + 1
        return {**counters, "by_typology": by_typology}

    def dispose_case(self, case_id: int, disposition: str, notes: str | None) -> CaseRow | None:
        if disposition not in DISPOSITIONS:
            raise ValueError(f"invalid disposition {disposition!r}")
        with Session(self.engine) as session:
            row = session.get(CaseRow, case_id)
            if row is None:
                return None
            row.status = DISPOSED
            row.disposition = disposition
            row.disposition_notes = notes
            row.disposed_at = datetime.now(UTC).replace(tzinfo=None)
            session.commit()
            session.refresh(row)
            return row


def make_engine(url: str = "sqlite:///cases.db") -> Engine:
    return create_engine(url)
