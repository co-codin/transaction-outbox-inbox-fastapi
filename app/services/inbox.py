import logging
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from sqlalchemy import and_, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.inbox import InboxEffect, InboxMessage, InboxStatus
from app.models.outbox import OutboxEvent
from app.schemas.inbox import INBOX_POISON_EVENT_TYPE
from app.schemas.messages import PAYMENT_CREATED_EVENT_TYPE

logger = logging.getLogger(__name__)
_ERROR_LIMIT = 2000


class InboxProcessingError(Exception):
    """A handler failed and the delivery should be retried or dead-lettered."""


class UnknownInboxEventError(InboxProcessingError):
    def __init__(self, event_type: str) -> None:
        self.event_type = event_type
        super().__init__(f"No inbox handler registered for event type {event_type}")


class InboxHandler(Protocol):
    event_type: str

    async def handle(self, session: AsyncSession, message: InboxMessage) -> None:
        """Apply the side effect inside the caller's database transaction."""


@dataclass(frozen=True)
class IncomingInboxMessage:
    message_id: str
    topic: str
    partition: int
    offset: int
    event_type: str
    payload: dict[str, Any]
    malformed: bool = False
    error: str | None = None


@dataclass(frozen=True)
class InboxDeliveryResult:
    message_id: str
    event_type: str
    topic: str
    partition: int
    offset: int
    status: str
    attempts: int
    duplicate: bool
    commit_offset: bool
    retryable: bool
    handler_invoked: bool


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, InboxHandler] = {}

    def register(self, handler: InboxHandler) -> None:
        if handler.event_type in self._handlers:
            raise ValueError(f"Handler already registered for {handler.event_type}")
        self._handlers[handler.event_type] = handler

    def get(self, event_type: str) -> InboxHandler:
        try:
            return self._handlers[event_type]
        except KeyError as exc:
            raise UnknownInboxEventError(event_type) from exc

    def event_types(self) -> tuple[str, ...]:
        return tuple(self._handlers)


def build_default_registry() -> HandlerRegistry:
    registry = HandlerRegistry()
    registry.register(PaymentCreatedEffectHandler())
    return registry


class PaymentCreatedEffectHandler:
    """Records one ``inbox_effects`` row per payment-created message."""

    event_type = PAYMENT_CREATED_EVENT_TYPE

    async def handle(self, session: AsyncSession, message: InboxMessage) -> None:
        payment_id = message.payload.get("payment_id")
        if not isinstance(payment_id, str) or not payment_id:
            raise InboxProcessingError("payment_id is required")

        existing = await session.execute(
            select(InboxEffect.id).where(InboxEffect.message_id == message.message_id),
        )
        if existing.scalar_one_or_none() is not None:
            return

        session.add(
            InboxEffect(
                inbox_id=message.id,
                message_id=message.message_id,
                event_type=message.event_type,
                payload={
                    "payment_id": payment_id,
                    "event_type": message.event_type,
                },
            ),
        )


def claim_received_statement(batch_size: int):
    return (
        select(InboxMessage)
        .where(InboxMessage.status == InboxStatus.RECEIVED.value)
        .order_by(InboxMessage.received_at, InboxMessage.id)
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )


async def ingest_message(
    session: AsyncSession,
    incoming: IncomingInboxMessage,
    *,
    registry: HandlerRegistry,
    max_attempts: int,
    inline: bool,
) -> InboxDeliveryResult:
    """Insert or lock the inbox row and, in inline mode, run the handler.

    The caller owns the database transaction. Kafka offsets must be committed
    only after that transaction commits, and only when ``commit_offset`` is set.
    """

    row, inserted = await _load_or_insert(session, incoming)
    if row.status == InboxStatus.PROCESSED.value:
        return _result(
            row,
            duplicate=True,
            commit_offset=True,
            retryable=False,
            handler_invoked=False,
        )
    if row.status == InboxStatus.FAILED.value:
        return _result(
            row,
            duplicate=True,
            commit_offset=True,
            retryable=False,
            handler_invoked=False,
        )
    if incoming.malformed:
        return _fail_terminal(
            session,
            row,
            error=incoming.error or "malformed kafka message",
            attempts=max(row.attempts, max_attempts),
        )
    if not inline:
        return _result(
            row,
            duplicate=not inserted,
            commit_offset=True,
            retryable=False,
            handler_invoked=False,
        )
    return await _execute_handler(session, row, registry=registry, max_attempts=max_attempts)


async def process_available_batch(
    session: AsyncSession,
    *,
    registry: HandlerRegistry,
    max_attempts: int,
    batch_size: int,
) -> list[InboxDeliveryResult]:
    """Claim received rows with ``FOR UPDATE SKIP LOCKED`` and run handlers."""

    result = await session.execute(claim_received_statement(batch_size))
    rows: Sequence[InboxMessage] = list(result.scalars())
    outcomes: list[InboxDeliveryResult] = []
    for row in rows:
        outcomes.append(
            await _execute_handler(
                session,
                row,
                registry=registry,
                max_attempts=max_attempts,
            ),
        )
    return outcomes


async def _load_or_insert(
    session: AsyncSession,
    incoming: IncomingInboxMessage,
) -> tuple[InboxMessage, bool]:
    existing = await _lock_inbox_message(session, incoming)
    if existing is not None:
        return existing, False

    row = InboxMessage(
        id=uuid.uuid4(),
        message_id=incoming.message_id,
        topic=incoming.topic,
        partition=incoming.partition,
        offset=incoming.offset,
        event_type=incoming.event_type or "unknown",
        payload=incoming.payload,
        status=InboxStatus.RECEIVED.value,
        attempts=0,
        received_at=datetime.now(UTC),
    )
    try:
        async with session.begin_nested():
            session.add(row)
            await session.flush()
    except IntegrityError:
        if row in session:
            session.expunge(row)
        existing = await _lock_inbox_message(session, incoming)
        if existing is None:
            raise
        return existing, False
    return row, True


async def _lock_inbox_message(
    session: AsyncSession,
    incoming: IncomingInboxMessage,
) -> InboxMessage | None:
    stmt = (
        select(InboxMessage)
        .where(
            or_(
                InboxMessage.message_id == incoming.message_id,
                and_(
                    InboxMessage.topic == incoming.topic,
                    InboxMessage.partition == incoming.partition,
                    InboxMessage.offset == incoming.offset,
                ),
            ),
        )
        .order_by(InboxMessage.received_at)
        .limit(1)
        .with_for_update()
    )
    result = await session.execute(stmt)
    return result.scalars().first()


async def _execute_handler(
    session: AsyncSession,
    row: InboxMessage,
    *,
    registry: HandlerRegistry,
    max_attempts: int,
) -> InboxDeliveryResult:
    # Count the try before the savepoint. A handler failure rolls the side
    # effect back and leaves this increment in the outer transaction.
    row.attempts += 1
    try:
        handler = registry.get(row.event_type)
    except UnknownInboxEventError as exc:
        return _record_failure(
            session,
            row,
            exc,
            max_attempts=max_attempts,
            handler_invoked=False,
        )

    try:
        async with session.begin_nested():
            await handler.handle(session, row)
            await session.flush()
    except Exception as exc:
        logger.warning(
            "inbox_handler_failed",
            extra={
                "event": "inbox_handler_failed",
                "message_id": row.message_id,
                "event_type": row.event_type,
                "attempts": row.attempts,
                "error": _error_text(exc),
            },
        )
        return _record_failure(
            session,
            row,
            exc,
            max_attempts=max_attempts,
            handler_invoked=True,
        )

    row.status = InboxStatus.PROCESSED.value
    row.processed_at = datetime.now(UTC)
    row.last_error = None
    return _result(
        row,
        duplicate=False,
        commit_offset=True,
        retryable=False,
        handler_invoked=True,
    )


def _record_failure(
    session: AsyncSession,
    row: InboxMessage,
    exc: Exception,
    *,
    max_attempts: int,
    handler_invoked: bool,
) -> InboxDeliveryResult:
    row.last_error = _error_text(exc)
    if row.attempts >= max_attempts:
        return _fail_terminal(
            session,
            row,
            error=row.last_error,
            attempts=row.attempts,
            handler_invoked=handler_invoked,
        )
    row.status = InboxStatus.RECEIVED.value
    return _result(
        row,
        duplicate=False,
        commit_offset=False,
        retryable=True,
        handler_invoked=handler_invoked,
    )


def _fail_terminal(
    session: AsyncSession,
    row: InboxMessage,
    *,
    error: str,
    attempts: int,
    handler_invoked: bool = False,
) -> InboxDeliveryResult:
    row.attempts = attempts
    row.last_error = error[:_ERROR_LIMIT]
    row.status = InboxStatus.FAILED.value
    row.processed_at = None
    _enqueue_poison(session, row)
    return _result(
        row,
        duplicate=False,
        commit_offset=True,
        retryable=False,
        handler_invoked=handler_invoked,
    )


def _enqueue_poison(session: AsyncSession, row: InboxMessage) -> None:
    if row.id is None:
        raise RuntimeError("inbox row must have an id before poison enqueue")
    failed_at = datetime.now(UTC).isoformat()
    session.add(
        OutboxEvent(
            aggregate_id=row.id,
            event_type=INBOX_POISON_EVENT_TYPE,
            payload={
                "message_id": row.message_id,
                "event_type": row.event_type,
                "topic": row.topic,
                "partition": row.partition,
                "offset": row.offset,
                "attempts": row.attempts,
                "last_error": row.last_error,
                "payload": row.payload,
                "failed_at": failed_at,
            },
        ),
    )


def _result(
    row: InboxMessage,
    *,
    duplicate: bool,
    commit_offset: bool,
    retryable: bool,
    handler_invoked: bool,
) -> InboxDeliveryResult:
    return InboxDeliveryResult(
        message_id=row.message_id,
        event_type=row.event_type,
        topic=row.topic,
        partition=row.partition,
        offset=row.offset,
        status=row.status,
        attempts=row.attempts,
        duplicate=duplicate,
        commit_offset=commit_offset,
        retryable=retryable,
        handler_invoked=handler_invoked,
    )


def _error_text(exc: BaseException) -> str:
    text = str(exc).strip() or exc.__class__.__name__
    return text[:_ERROR_LIMIT]
