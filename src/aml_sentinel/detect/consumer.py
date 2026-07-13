"""Kafka consumer: transactions topic -> detection service -> case API."""

from __future__ import annotations

import logging
from datetime import timedelta

from aiokafka import AIOKafkaConsumer

from aml_sentinel.detect.poster import AlertPoster
from aml_sentinel.detect.service import DEFAULT_WINDOW, DetectionService
from aml_sentinel.replay.kafka import decode_transaction

logger = logging.getLogger(__name__)


async def consume_transactions(
    brokers: str,
    topic: str,
    api_url: str,
    *,
    window: timedelta = DEFAULT_WINDOW,
    group_id: str = "aml-sentinel-detector",
    max_messages: int | None = None,
) -> DetectionService:
    """Consume until cancelled (or *max_messages*, for tests); returns the service."""
    service = DetectionService(window=window)
    poster = AlertPoster(api_url)
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=brokers,
        group_id=group_id,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    processed = 0
    try:
        async for message in consumer:
            tx = decode_transaction(message.value)
            for alert in service.process(tx):
                poster.post(alert)
                logger.info("alert %s (%s) -> %s", alert.alert_id, alert.typology, api_url)
            processed += 1
            if processed % 10_000 == 0:
                logger.info("processed %d transactions", processed)
            if max_messages is not None and processed >= max_messages:
                break
    finally:
        await consumer.stop()
    return service
