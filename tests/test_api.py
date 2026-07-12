from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from aml_sentinel.api import create_app


@pytest.fixture()
def client() -> Iterator[TestClient]:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    with TestClient(create_app(engine)) as c:
        yield c


def alert_payload(alert_id: str = "AL-000001", typology: str = "cycle") -> dict[str, Any]:
    return {
        "alert_id": alert_id,
        "typology": typology,
        "fired_at": "2024-01-05T12:30:00",
        "accounts": ["acc_1", "acc_2", "acc_3"],
        "evidence_edges": [
            {
                "tx_id": "tx_1",
                "src": "acc_1",
                "dst": "acc_2",
                "amount": 9999.0,
                "event_time": "2024-01-05T12:00:00",
            },
            {
                "tx_id": "tx_2",
                "src": "acc_2",
                "dst": "acc_3",
                "amount": 9999.0,
                "event_time": "2024-01-05T12:10:00",
            },
            {
                "tx_id": "tx_3",
                "src": "acc_3",
                "dst": "acc_1",
                "amount": 9999.0,
                "event_time": "2024-01-05T12:30:00",
            },
        ],
        "score": 1.0,
    }


def test_ingest_and_get_case(client: TestClient) -> None:
    r = client.post("/alerts", json=alert_payload())
    assert r.status_code == 201
    created = r.json()
    assert created["alert_id"] == "AL-000001"

    r = client.get(f"/cases/{created['case_id']}")
    assert r.status_code == 200
    case = r.json()
    assert case["typology"] == "cycle"
    assert case["status"] == "open"
    assert case["disposition"] is None
    assert set(case["evidence"]["nodes"]) == {"acc_1", "acc_2", "acc_3"}
    assert [e["tx_id"] for e in case["evidence"]["edges"]] == ["tx_1", "tx_2", "tx_3"]


def test_duplicate_alert_conflicts(client: TestClient) -> None:
    assert client.post("/alerts", json=alert_payload()).status_code == 201
    assert client.post("/alerts", json=alert_payload()).status_code == 409


def test_invalid_alert_rejected(client: TestClient) -> None:
    bad = alert_payload()
    bad["score"] = 3.0
    assert client.post("/alerts", json=bad).status_code == 422
    bad = alert_payload()
    bad["evidence_edges"] = []
    assert client.post("/alerts", json=bad).status_code == 422


def test_list_alerts_with_filters(client: TestClient) -> None:
    client.post("/alerts", json=alert_payload("AL-000001", "cycle"))
    client.post("/alerts", json=alert_payload("AL-000002", "structuring"))
    client.post("/alerts", json=alert_payload("AL-000003", "cycle"))

    assert len(client.get("/alerts").json()) == 3
    cycles = client.get("/alerts", params={"typology": "cycle"}).json()
    assert {a["alert_id"] for a in cycles} == {"AL-000001", "AL-000003"}
    assert len(client.get("/alerts", params={"limit": 1}).json()) == 1
    assert client.get("/alerts", params={"status": "bogus"}).status_code == 422


def test_disposition_flow(client: TestClient) -> None:
    case_id = client.post("/alerts", json=alert_payload()).json()["case_id"]

    r = client.post(
        f"/cases/{case_id}/disposition",
        json={"disposition": "true_positive", "notes": "matches ring pat_0"},
    )
    assert r.status_code == 200
    case = r.json()
    assert case["status"] == "disposed"
    assert case["disposition"] == "true_positive"
    assert case["disposition_notes"] == "matches ring pat_0"
    assert case["disposed_at"] is not None

    open_cases = client.get("/alerts", params={"status": "open"}).json()
    assert open_cases == []


def test_disposition_validation_and_404(client: TestClient) -> None:
    case_id = client.post("/alerts", json=alert_payload()).json()["case_id"]
    r = client.post(f"/cases/{case_id}/disposition", json={"disposition": "maybe"})
    assert r.status_code == 422
    r = client.post("/cases/9999/disposition", json={"disposition": "false_positive"})
    assert r.status_code == 404
    assert client.get("/cases/9999").status_code == 404
