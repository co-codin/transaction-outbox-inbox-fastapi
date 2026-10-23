import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from faststream.rabbit import RabbitBroker

from app.core.config import settings
from app.core.logging import log_event
from app.db.session import async_session_maker
from app.messaging.kafka import KafkaOutboxPublisher
from app.messaging.outbox import OutboxPublisher
from app.messaging.topology import declare_topology

logger = logging.getLogger(__name__)


@asynccontextmanager
async def outbox_runtime() -> AsyncIterator[tuple[RabbitBroker, OutboxPublisher]]:
    broker = RabbitBroker(settings.rabbitmq_url)
    await broker.start()
    await declare_topology(broker)

    kafka_publisher: KafkaOutboxPublisher | None = None
    if settings.kafka_enabled:
        kafka_publisher = KafkaOutboxPublisher.from_settings(settings)
        try:
            await kafka_publisher.start()
        except Exception:
            logger.exception("kafka_publisher_start_failed")
        else:
            try:
                await kafka_publisher.ensure_topics()
            except Exception:
                logger.exception("kafka_topic_setup_failed")

    outbox = OutboxPublisher(
        broker,
        async_session_maker,
        poll_interval_seconds=settings.outbox_poll_interval_seconds,
        batch_size=settings.outbox_batch_size,
        retention_seconds=settings.outbox_retention_seconds,
        cleanup_interval_seconds=settings.outbox_cleanup_interval_seconds,
        kafka_publisher=kafka_publisher,
    )
    outbox.start()
    log_event(
        logger,
        logging.INFO,
        "outbox_runtime_started",
        kafka_enabled=settings.kafka_enabled,
    )

    try:
        yield broker, outbox
    finally:
        await outbox.stop()
        if kafka_publisher is not None:
            await kafka_publisher.stop()
        await broker.stop()
