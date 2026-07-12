"""FastAPI case-management service (plus the demo UI).

Endpoints:
    POST /alerts                   internal alert ingestion (detector -> case)
    GET  /alerts                   alert/case queue, filterable
    GET  /cases/{case_id}          case detail + evidence subgraph
    POST /cases/{case_id}/disposition   analyst true/false-positive call
    GET  /stats                    case-queue counters for the metrics panel
    WS   /ws/alerts                pushes each newly ingested case summary
    GET  /                         single-page UI (static, no build step)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
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

_STATIC_DIR = Path(__file__).parent / "static"


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


class AlertBroadcaster:
    """Fans newly ingested cases out to connected websocket clients."""

    def __init__(self) -> None:
        self._clients: set[WebSocket] = set()

    def add(self, ws: WebSocket) -> None:
        self._clients.add(ws)

    def remove(self, ws: WebSocket) -> None:
        self._clients.discard(ws)

    async def broadcast(self, payload: dict[str, Any]) -> None:
        for ws in list(self._clients):
            try:
                await ws.send_json(payload)
            except Exception:
                self._clients.discard(ws)


def create_app(engine: Engine) -> FastAPI:
    store = CaseStore(engine)
    broadcaster = AlertBroadcaster()
    app = FastAPI(title="AML Sentinel Case API", version=__version__)

    @app.post("/alerts", response_model=CaseCreated, status_code=201)
    async def ingest_alert(alert: AlertIn) -> CaseCreated:
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
        await broadcaster.broadcast(_summary(row).model_dump(mode="json"))
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

    @app.get("/stats")
    def stats() -> dict[str, Any]:
        return store.stats()

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.websocket("/ws/alerts")
    async def ws_alerts(ws: WebSocket) -> None:
        await ws.accept()
        broadcaster.add(ws)
        try:
            while True:  # the client never sends; this blocks until disconnect
                await ws.receive_text()
        except WebSocketDisconnect:
            broadcaster.remove(ws)

    app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(_STATIC_DIR / "index.html")

    return app
