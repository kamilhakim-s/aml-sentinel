"""Pydantic request/response models for the case API."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class EdgeModel(BaseModel):
    tx_id: str
    src: str
    dst: str
    amount: float
    event_time: datetime


class AlertIn(BaseModel):
    """Alert as emitted by the detection service (mirrors models.Alert.to_dict)."""

    alert_id: str = Field(min_length=1, max_length=32)
    typology: str = Field(min_length=1, max_length=64)
    fired_at: datetime
    accounts: list[str] = Field(min_length=1)
    evidence_edges: list[EdgeModel] = Field(min_length=1)
    score: float = Field(ge=0.0, le=1.0)


class Subgraph(BaseModel):
    """Evidence subgraph for UI rendering: distinct accounts + the evidence edges."""

    nodes: list[str]
    edges: list[EdgeModel]


class CaseSummary(BaseModel):
    case_id: int
    alert_id: str
    typology: str
    fired_at: datetime
    score: float
    accounts: list[str]
    status: str
    disposition: str | None


class CaseDetail(CaseSummary):
    evidence: Subgraph
    disposition_notes: str | None
    created_at: datetime
    disposed_at: datetime | None


class DispositionIn(BaseModel):
    disposition: Literal["true_positive", "false_positive"]
    notes: str | None = Field(default=None, max_length=2000)


class CaseCreated(BaseModel):
    case_id: int
    alert_id: str
