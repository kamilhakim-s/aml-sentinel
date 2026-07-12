"""Websocket feed, stats endpoint, and static UI serving."""

from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from aml_sentinel.api import create_app

from .test_api import alert_payload


@pytest.fixture()
def client() -> Iterator[TestClient]:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    with TestClient(create_app(engine)) as c:
        yield c


def test_websocket_receives_ingested_alert(client: TestClient) -> None:
    with client.websocket_connect("/ws/alerts") as ws:
        client.post("/alerts", json=alert_payload("AL-000042"))
        message: dict[str, Any] = ws.receive_json()
    assert message["alert_id"] == "AL-000042"
    assert message["typology"] == "cycle"
    assert message["status"] == "open"


def test_stats_counts_and_dispositions(client: TestClient) -> None:
    client.post("/alerts", json=alert_payload("AL-000001", "cycle"))
    client.post("/alerts", json=alert_payload("AL-000002", "structuring"))
    case_id = client.post("/alerts", json=alert_payload("AL-000003", "cycle")).json()["case_id"]
    client.post(f"/cases/{case_id}/disposition", json={"disposition": "true_positive"})

    stats = client.get("/stats").json()
    assert stats["total"] == 3
    assert stats["open"] == 2
    assert stats["disposed"] == 1
    assert stats["true_positive"] == 1
    assert stats["false_positive"] == 0
    assert stats["by_typology"] == {"cycle": 2, "structuring": 1}


def test_ui_served_at_root(client: TestClient) -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert "AML Sentinel" in response.text
    assert client.get("/static/app.js").status_code == 200
    assert client.get("/static/style.css").status_code == 200


def test_healthz(client: TestClient) -> None:
    assert client.get("/healthz").json() == {"status": "ok"}
