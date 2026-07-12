"""Deliver fired alerts to the case API."""

from __future__ import annotations

import json
import urllib.error
import urllib.request

from aml_sentinel.detect.service import DetectionService
from aml_sentinel.models import Alert, Transaction


class AlertPoster:
    """POSTs alerts to the case API; duplicate ingestion (409) is not an error."""

    def __init__(self, api_url: str, timeout: float = 5.0) -> None:
        self.endpoint = api_url.rstrip("/") + "/alerts"
        self.timeout = timeout
        self.posted = 0
        self.duplicates = 0

    def post(self, alert: Alert) -> None:
        request = urllib.request.Request(
            self.endpoint,
            data=json.dumps(alert.to_dict()).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout):
                self.posted += 1
        except urllib.error.HTTPError as exc:
            if exc.code == 409:
                self.duplicates += 1
            else:
                raise


class PostingDetectorSink:
    """Replay sink: detect, then push whatever fired to the case API."""

    def __init__(self, service: DetectionService, poster: AlertPoster) -> None:
        self.service = service
        self.poster = poster

    async def emit(self, tx: Transaction) -> None:
        for alert in self.service.process(tx):
            self.poster.post(alert)
