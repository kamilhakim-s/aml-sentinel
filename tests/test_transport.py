"""Kafka codec round-trip and alert posting (no broker required)."""

import asyncio
import json
from datetime import datetime
from typing import Any

import pytest

from aml_sentinel.detect import DetectionService
from aml_sentinel.detect.poster import AlertPoster, PostingDetectorSink
from aml_sentinel.models import Transaction
from aml_sentinel.replay.kafka import decode_transaction, encode_transaction


def make_tx(tx_id: str = "tx_1") -> Transaction:
    return Transaction(
        tx_id=tx_id,
        src="acc_1",
        dst="acc_2",
        amount=9999.0,
        event_time=datetime(2024, 1, 5, 12, 30),
        description="layered transfer via intermediary",
    )


def test_codec_round_trip() -> None:
    tx = make_tx()
    assert decode_transaction(encode_transaction(tx)) == tx


def test_encode_is_plain_json() -> None:
    payload = json.loads(encode_transaction(make_tx()))
    assert payload["tx_id"] == "tx_1"
    assert payload["event_time"] == "2024-01-05T12:30:00"
    assert "is_fraud" not in payload  # no label leakage on the wire


def test_posting_sink_posts_fired_alerts(monkeypatch: pytest.MonkeyPatch) -> None:
    posted: list[dict[str, Any]] = []
    poster = AlertPoster("http://api.test")
    monkeypatch.setattr(poster, "post", lambda alert: posted.append(alert.to_dict()))

    service = DetectionService()
    sink = PostingDetectorSink(service, poster)

    async def run() -> None:
        t0 = datetime(2024, 1, 1)
        for i, (src, dst) in enumerate([("a", "b"), ("b", "c"), ("c", "a")]):
            await sink.emit(
                Transaction(
                    tx_id=f"t{i}",
                    src=src,
                    dst=dst,
                    amount=9999.0,
                    event_time=t0.replace(minute=i),
                    description="hop",
                )
            )

    asyncio.run(run())
    assert [p["typology"] for p in posted] == ["cycle"]
    assert posted[0]["alert_id"] == service.alerts[0].alert_id


def test_poster_treats_409_as_duplicate(monkeypatch: pytest.MonkeyPatch) -> None:
    import urllib.error

    poster = AlertPoster("http://api.test")

    def raise_409(request: Any, timeout: float) -> Any:
        raise urllib.error.HTTPError(request.full_url, 409, "conflict", None, None)  # type: ignore[arg-type]

    monkeypatch.setattr("urllib.request.urlopen", raise_409)
    service = DetectionService()
    # fire a real alert to have something to post
    t0 = datetime(2024, 1, 1)
    for i, (src, dst) in enumerate([("a", "b"), ("b", "a")]):
        service.process(
            Transaction(
                tx_id=f"t{i}",
                src=src,
                dst=dst,
                amount=1.0,
                event_time=t0.replace(minute=i),
                description="x",
            )
        )
    poster.post(service.alerts[0])
    assert poster.duplicates == 1
    assert poster.posted == 0
