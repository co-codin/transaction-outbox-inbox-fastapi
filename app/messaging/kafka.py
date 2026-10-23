import asyncio
import json
import logging
from typing import Any, Protocol

from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError, for_code

from app.core.config import Settings
from app.core.logging import log_event
from app.schemas.inbox import INBOX_POISON_EVENT_TYPE, KafkaEventEnvelope

logger = logging.getLogger(__name__)
_DLQ_RETENTION_MS = "604800000"


class KafkaEventPublisher(Protocol):
    async def publish_payment_event(
        self,
        *,
        message_id: str,
        event_type: str,
        aggregate_id: str,
        payload: dict[str, Any],
    ) -> None:
        """Publish one outbox event to the payments topic."""

    async def publish_poison_event(self, *, message_id: str, payload: dict[str, Any]) -> None:
        """Publish one poison inbox message to the DLQ topic."""


class KafkaOutboxPublisher:
    """Idempotent Kafka producer used by the transactional outbox relay."""

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        client_id: str,
        payments_topic: str,
        dlq_topic: str,
        request_timeout_ms: int,
        startup_timeout_seconds: float,
        partitions: int,
        replication_factor: int,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id
        self._payments_topic = payments_topic
        self._dlq_topic = dlq_topic
        self._request_timeout_ms = request_timeout_ms
        self._startup_timeout_seconds = startup_timeout_seconds
        self._partitions = partitions
        self._replication_factor = replication_factor
        self._producer: AIOKafkaProducer | None = None
        self._ready = False

    @classmethod
    def from_settings(cls, settings: Settings) -> "KafkaOutboxPublisher":
        return cls(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id=settings.kafka_client_id,
            payments_topic=settings.kafka_payments_topic,
            dlq_topic=settings.kafka_inbox_dlq_topic,
            request_timeout_ms=settings.kafka_request_timeout_ms,
            startup_timeout_seconds=settings.kafka_startup_timeout_seconds,
            partitions=settings.kafka_topic_partitions,
            replication_factor=settings.kafka_topic_replication_factor,
        )

    async def start(self) -> None:
        await self._ensure_started()

    async def stop(self) -> None:
        await self._stop_producer()

    async def publish_payment_event(
        self,
        *,
        message_id: str,
        event_type: str,
        aggregate_id: str,
        payload: dict[str, Any],
    ) -> None:
        envelope = KafkaEventEnvelope(
            message_id=message_id,
            event_type=event_type,
            aggregate_id=aggregate_id,
            payload=payload,
        )
        await self._send(
            self._payments_topic,
            key=message_id,
            value=envelope.model_dump(),
            headers={
                "message_id": message_id,
                "event_type": event_type,
                "aggregate_id": aggregate_id,
            },
        )

    async def publish_poison_event(self, *, message_id: str, payload: dict[str, Any]) -> None:
        await self._send(
            self._dlq_topic,
            key=message_id,
            value=payload,
            headers={
                "message_id": message_id,
                "event_type": INBOX_POISON_EVENT_TYPE,
            },
        )

    async def ensure_topics(self) -> None:
        await self._ensure_started()
        admin = AIOKafkaAdminClient(
            bootstrap_servers=self._bootstrap_servers,
            client_id=f"{self._client_id}-admin",
            request_timeout_ms=self._request_timeout_ms,
        )
        await admin.start()
        try:
            existing = set(await admin.list_topics())
            missing = [topic for topic in self._topic_specs() if topic.name not in existing]
            if not missing:
                return
            response = await admin.create_topics(missing)
            for item in response.topic_errors:
                code = item[1]
                if code in (0, TopicAlreadyExistsError.errno):
                    continue
                raise for_code(code)(f"Could not create topic {item[0]}")
        finally:
            await admin.close()
        log_event(
            logger,
            logging.INFO,
            "kafka_topics_ready",
            payments_topic=self._payments_topic,
            dlq_topic=self._dlq_topic,
        )

    def _topic_specs(self) -> tuple[NewTopic, NewTopic]:
        return (
            NewTopic(
                name=self._payments_topic,
                num_partitions=self._partitions,
                replication_factor=self._replication_factor,
            ),
            NewTopic(
                name=self._dlq_topic,
                num_partitions=self._partitions,
                replication_factor=self._replication_factor,
                topic_configs={"retention.ms": _DLQ_RETENTION_MS},
            ),
        )

    async def _send(
        self,
        topic: str,
        *,
        key: str,
        value: dict[str, Any],
        headers: dict[str, str],
    ) -> None:
        await self._ensure_started()
        producer = self._producer
        if producer is None:
            raise RuntimeError("Kafka producer is not started")
        encoded_headers = [(name, raw.encode("utf-8")) for name, raw in headers.items()]
        try:
            await producer.send_and_wait(
                topic,
                key=key.encode("utf-8"),
                value=json.dumps(value, separators=(",", ":"), default=str).encode("utf-8"),
                headers=encoded_headers,
            )
        except Exception:
            self._ready = False
            raise

    async def _ensure_started(self) -> None:
        if self._ready and self._producer is not None:
            return
        await self._stop_producer()
        producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            client_id=f"{self._client_id}-outbox",
            acks="all",
            enable_idempotence=True,
            linger_ms=20,
            request_timeout_ms=self._request_timeout_ms,
        )
        self._producer = producer
        try:
            await asyncio.wait_for(producer.start(), timeout=self._startup_timeout_seconds)
        except Exception:
            await self._stop_producer()
            raise
        self._ready = True
        log_event(
            logger,
            logging.INFO,
            "kafka_producer_started",
            bootstrap_servers=self._bootstrap_servers,
        )

    async def _stop_producer(self) -> None:
        producer = self._producer
        self._producer = None
        self._ready = False
        if producer is None:
            return
        try:
            await producer.stop()
        except Exception:
            logger.exception("kafka_producer_stop_failed")
