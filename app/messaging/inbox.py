import asyncio
import hashlib
import json
import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Protocol

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.structs import OffsetAndMetadata

from app.core.config import Settings
from app.core.logging import log_event
from app.schemas.inbox import MESSAGE_ID_MAX_LENGTH
from app.services.inbox import (
    HandlerRegistry,
    IncomingInboxMessage,
    InboxDeliveryResult,
    ingest_message,
)

logger = logging.getLogger(__name__)


class KafkaMessageConsumer(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def getmany(
        self,
        *partitions: Any,
        timeout_ms: int = 0,
        max_records: int | None = None,
    ) -> Mapping[Any, list[Any]]: ...

    async def commit(self, offsets: Mapping[Any, Any] | None = None) -> None: ...

    def seek(self, partition: Any, offset: int) -> None: ...


BeforeCommit = Callable[[InboxDeliveryResult], Awaitable[None]]


class InboxConsumer:
    """Consume Kafka records into the inbox and commit offsets after the DB commit.

    A retryable handler failure leaves the offset uncommitted and seeks back to
    that record so later records in the same partition cannot be acknowledged
    ahead of it. Terminal rows (processed or dead-lettered) commit the offset.
    """

    def __init__(
        self,
        *,
        consumer: KafkaMessageConsumer,
        session_factory: Callable[[], Any],
        registry: HandlerRegistry,
        max_attempts: int,
        inline: bool,
        retry_backoff_seconds: float,
        max_records: int,
        poll_timeout_ms: int,
        before_commit: BeforeCommit | None = None,
    ) -> None:
        self._consumer = consumer
        self._session_factory = session_factory
        self._registry = registry
        self._max_attempts = max_attempts
        self._inline = inline
        self._retry_backoff_seconds = retry_backoff_seconds
        self._max_records = max_records
        self._poll_timeout_ms = poll_timeout_ms
        self._before_commit = before_commit

    async def run(self, stop: Any) -> None:
        await self._consumer.start()
        log_event(
            logger,
            logging.INFO,
            "inbox_consumer_started",
            inline=self._inline,
            max_attempts=self._max_attempts,
        )
        try:
            while not stop.is_set():
                batches = await self._consumer.getmany(
                    timeout_ms=self._poll_timeout_ms,
                    max_records=self._max_records,
                )
                for records in batches.values():
                    if stop.is_set():
                        break
                    await self._process_partition(records, stop)
        finally:
            await self._consumer.stop()
            log_event(logger, logging.INFO, "inbox_consumer_stopped")

    async def _process_partition(self, records: list[Any], stop: Any) -> None:
        for record in records:
            if stop.is_set():
                return
            should_continue = await self._handle_record(record, stop)
            if not should_continue:
                return

    async def _handle_record(self, record: Any, stop: Any) -> bool:
        incoming = parse_consumer_record(record)
        try:
            async with self._session_factory() as session:
                async with session.begin():
                    result = await ingest_message(
                        session,
                        incoming,
                        registry=self._registry,
                        max_attempts=self._max_attempts,
                        inline=self._inline,
                    )
                    if self._before_commit is not None:
                        await self._before_commit(result)
        except Exception:
            logger.exception(
                "inbox_db_transaction_failed",
                extra={
                    "event": "inbox_db_transaction_failed",
                    "message_id": incoming.message_id,
                    "topic": incoming.topic,
                    "partition": incoming.partition,
                    "offset": incoming.offset,
                },
            )
            await wait_for_stop(stop, self._retry_backoff_seconds)
            self._seek(record)
            return False

        if not result.commit_offset:
            log_event(
                logger,
                logging.INFO,
                "inbox_offset_held",
                message_id=result.message_id,
                event_type=result.event_type,
                topic=result.topic,
                partition=result.partition,
                offset=result.offset,
                attempts=result.attempts,
                status=result.status,
            )
            await wait_for_stop(stop, self._retry_backoff_seconds)
            self._seek(record)
            return False

        await self._commit(record)
        log_event(
            logger,
            logging.INFO,
            "inbox_offset_committed" if not result.duplicate else "inbox_duplicate_skipped",
            message_id=result.message_id,
            event_type=result.event_type,
            topic=result.topic,
            partition=result.partition,
            offset=result.offset,
            attempts=result.attempts,
            status=result.status,
            handler_invoked=result.handler_invoked,
        )
        return True

    async def _commit(self, record: Any) -> None:
        partition = TopicPartition(record.topic, record.partition)
        await self._consumer.commit(
            {partition: OffsetAndMetadata(record.offset + 1, "")},
        )

    def _seek(self, record: Any) -> None:
        self._consumer.seek(TopicPartition(record.topic, record.partition), record.offset)


def build_kafka_consumer(settings: Settings) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        settings.kafka_payments_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id=f"{settings.kafka_client_id}-inbox",
        group_id=settings.kafka_inbox_group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        request_timeout_ms=settings.kafka_request_timeout_ms,
        max_poll_records=settings.inbox_consumer_max_records,
    )


def build_inbox_consumer(
    settings: Settings,
    *,
    session_factory: Callable[[], Any],
    registry: HandlerRegistry,
    consumer: KafkaMessageConsumer | None = None,
    before_commit: BeforeCommit | None = None,
) -> InboxConsumer:
    return InboxConsumer(
        consumer=consumer if consumer is not None else build_kafka_consumer(settings),
        session_factory=session_factory,
        registry=registry,
        max_attempts=settings.inbox_max_attempts,
        inline=settings.inbox_processing_mode == "inline",
        retry_backoff_seconds=settings.inbox_retry_backoff_seconds,
        max_records=settings.inbox_consumer_max_records,
        poll_timeout_ms=settings.inbox_poll_timeout_ms,
        before_commit=before_commit,
    )


def parse_consumer_record(record: Any) -> IncomingInboxMessage:
    topic = str(record.topic)
    partition = int(record.partition)
    offset = int(record.offset)
    headers = _decode_headers(getattr(record, "headers", None))
    key = _decode_bytes(getattr(record, "key", None))
    body, decode_error = _load_json(getattr(record, "value", None))

    message_id = headers.get("message_id") or _string_field(body, "message_id") or key
    event_type = headers.get("event_type") or _string_field(body, "event_type") or ""
    payload = _payload_from_body(body)
    fallback_id = f"{topic}:{partition}:{offset}"

    if decode_error is not None:
        return IncomingInboxMessage(
            message_id=_fit_message_id(message_id or fallback_id),
            topic=topic,
            partition=partition,
            offset=offset,
            event_type=event_type or "unknown",
            payload={"raw": _decode_bytes(getattr(record, "value", None)) or ""},
            malformed=True,
            error=decode_error,
        )
    if not message_id:
        return IncomingInboxMessage(
            message_id=_fit_message_id(fallback_id),
            topic=topic,
            partition=partition,
            offset=offset,
            event_type=event_type or "unknown",
            payload=payload,
            malformed=True,
            error="message_id is missing",
        )
    if not event_type:
        return IncomingInboxMessage(
            message_id=_fit_message_id(message_id),
            topic=topic,
            partition=partition,
            offset=offset,
            event_type="unknown",
            payload=payload,
            malformed=True,
            error="event_type is missing",
        )
    return IncomingInboxMessage(
        message_id=_fit_message_id(message_id),
        topic=topic,
        partition=partition,
        offset=offset,
        event_type=event_type,
        payload=payload,
    )


async def wait_for_stop(stop: Any, seconds: float) -> None:
    if seconds <= 0 or stop.is_set():
        return
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        return


def _decode_headers(headers: Any) -> dict[str, str]:
    decoded: dict[str, str] = {}
    if not headers:
        return decoded
    for key, value in headers:
        text = _decode_bytes(value)
        if text is not None:
            decoded[str(key)] = text
    return decoded


def _load_json(value: Any) -> tuple[Any, str | None]:
    if value is None:
        return {}, "kafka record value is empty"
    if isinstance(value, bytes | bytearray):
        try:
            text = bytes(value).decode("utf-8")
        except UnicodeDecodeError as exc:
            return {}, f"kafka record is not utf-8: {exc}"
    elif isinstance(value, str):
        text = value
    else:
        return value, None
    try:
        return json.loads(text), None
    except json.JSONDecodeError as exc:
        return {}, f"kafka record is not json: {exc}"


def _payload_from_body(body: Any) -> dict[str, Any]:
    if isinstance(body, dict) and isinstance(body.get("payload"), dict) and "event_type" in body:
        return body["payload"]
    if isinstance(body, dict):
        return body
    if body == {}:
        return {}
    return {"value": body}


def _string_field(body: Any, name: str) -> str | None:
    if not isinstance(body, dict):
        return None
    value = body.get(name)
    if isinstance(value, str) and value:
        return value
    return None


def _decode_bytes(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value or None
    if isinstance(value, bytes | bytearray):
        try:
            text = bytes(value).decode("utf-8")
        except UnicodeDecodeError:
            return None
        return text or None
    return None


def _fit_message_id(value: str) -> str:
    if len(value) <= MESSAGE_ID_MAX_LENGTH:
        return value
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return f"{value[:190]}:{digest[:32]}"
