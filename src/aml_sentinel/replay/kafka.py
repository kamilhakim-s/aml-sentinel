"""Kafka (Redpanda) transport for the replay engine."""

from __future__ import annotations

import json

from aiokafka import AIOKafkaProducer

from aml_sentinel.models import Transaction


def encode_transaction(tx: Transaction) -> bytes:
    return json.dumps(tx.to_dict()).encode()


def decode_transaction(payload: bytes) -> Transaction:
    return Transaction.from_dict(json.loads(payload))


class KafkaSink:
    """Replay sink that produces each transaction to a topic, keyed by src account."""

    def __init__(self, brokers: str, topic: str) -> None:
        self.topic = topic
        self._producer = AIOKafkaProducer(bootstrap_servers=brokers)

    async def __aenter__(self) -> KafkaSink:
        await self._producer.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self._producer.stop()

    async def emit(self, tx: Transaction) -> None:
        await self._producer.send_and_wait(self.topic, encode_transaction(tx), key=tx.src.encode())
