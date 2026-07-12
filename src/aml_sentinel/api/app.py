"""FastAPI case-management service.

Endpoints:
    POST /alerts                   internal alert ingestion (detector -> case)
    GET  /alerts                   alert/case queue, filterable
    GET  /cases/{case_id}          case detail + evidence subgraph
    POST /cases/{case_id}/disposition   analyst true/false-positive call
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import Engine

from aml_sentinel import __version__
from aml_sentinel.api.schemas import (
    AlertIn,
    CaseCreated,
    CaseDetail,
    CaseSummary,
    DispositionIn,
    EdgeModel,
    Subgraph,
)
from aml_sentinel.api.store import CaseRow, CaseStore, DuplicateAlertError


def _summary(row: CaseRow) -> CaseSummary:
    return CaseSummary(
        case_id=row.case_id,
        alert_id=row.alert_id,
        typology=row.typology,
        fired_at=row.fired_at,
        score=row.score,
        accounts=list(row.accounts),
        status=row.status,
        disposition=row.disposition,
    )


def _detail(row: CaseRow) -> CaseDetail:
    edges = [EdgeModel.model_validate(e) for e in row.evidence_edges]
    nodes = list(dict.fromkeys(a for e in edges for a in (e.src, e.dst)))
    return CaseDetail(
        **_summary(row).model_dump(),
        evidence=Subgraph(nodes=nodes, edges=edges),
        disposition_notes=row.disposition_notes,
        created_at=row.created_at,
        disposed_at=row.disposed_at,
    )


def create_app(engine: Engine) -> FastAPI:
    store = CaseStore(engine)
    app = FastAPI(title="AML Sentinel Case API", version=__version__)

    @app.post("/alerts", response_model=CaseCreated, status_code=201)
    def ingest_alert(alert: AlertIn) -> CaseCreated:
        try:
            row = store.create_case(
                alert_id=alert.alert_id,
                typology=alert.typology,
                fired_at=alert.fired_at,
                score=alert.score,
                accounts=alert.accounts,
                evidence_edges=[e.model_dump(mode="json") for e in alert.evidence_edges],
            )
        except DuplicateAlertError as exc:
            raise HTTPException(
                status_code=409, detail=f"alert {alert.alert_id} already ingested"
            ) from exc
        return CaseCreated(case_id=row.case_id, alert_id=row.alert_id)

    @app.get("/alerts", response_model=list[CaseSummary])
    def list_alerts(
        typology: str | None = None,
        status: str | None = Query(default=None, pattern="^(open|disposed)$"),
        limit: int = Query(default=100, ge=1, le=1000),
        offset: int = Query(default=0, ge=0),
    ) -> list[CaseSummary]:
        rows = store.list_cases(typology=typology, status=status, limit=limit, offset=offset)
        return [_summary(row) for row in rows]

    @app.get("/cases/{case_id}", response_model=CaseDetail)
    def get_case(case_id: int) -> CaseDetail:
        row = store.get_case(case_id)
        if row is None:
            raise HTTPException(status_code=404, detail=f"case {case_id} not found")
        return _detail(row)

    @app.post("/cases/{case_id}/disposition", response_model=CaseDetail)
    def dispose(case_id: int, body: DispositionIn) -> CaseDetail:
        row = store.dispose_case(case_id, body.disposition, body.notes)
        if row is None:
            raise HTTPException(status_code=404, detail=f"case {case_id} not found")
        return _detail(row)

    return app
