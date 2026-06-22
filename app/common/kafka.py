from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import ConsumerRecord

from app.common.config import Settings
from app.common.serialization import json_dumps_bytes, json_loads_bytes

logger = logging.getLogger(__name__)


@asynccontextmanager
async def kafka_producer(settings: Settings) -> AsyncIterator[AIOKafkaProducer]:
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=json_dumps_bytes,
        key_serializer=lambda key: key.encode("utf-8") if isinstance(key, str) else key,
        enable_idempotence=True,
    )
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()


@asynccontextmanager
async def kafka_consumer(
    settings: Settings,
    *,
    topic: str,
    group_suffix: str,
) -> AsyncIterator[AIOKafkaConsumer]:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"{settings.kafka_group_prefix}-{group_suffix}",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=json_loads_bytes,
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
    )
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.stop()


async def publish_json(
    producer: AIOKafkaProducer,
    topic: str,
    payload: dict[str, Any],
    *,
    key: str | None = None,
) -> None:
    await producer.send_and_wait(topic, payload, key=key)
    logger.debug("published kafka message", extra={"topic": topic, "key": key})


async def commit_after(record: ConsumerRecord, consumer: AIOKafkaConsumer) -> None:
    await consumer.commit()
    logger.debug(
        "committed kafka message",
        extra={"topic": record.topic, "partition": record.partition, "offset": record.offset},
    )
