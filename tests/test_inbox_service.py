import importlib.util
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import IntegrityError

from app.models.inbox import InboxEffect, InboxMessage, InboxStatus
from app.models.outbox import OutboxEvent
from app.schemas.inbox import INBOX_POISON_EVENT_TYPE
from app.schemas.messages import PAYMENT_CREATED_EVENT_TYPE
from app.services.inbox import (
    HandlerRegistry,
    IncomingInboxMessage,
    InboxProcessingError,
    PaymentCreatedEffectHandler,
    UnknownInboxEventError,
    build_default_registry,
    claim_received_statement,
    ingest_message,
    process_available_batch,
)


class Savepoint:
    def __init__(self, session: "FakeSession") -> None:
        self._session = session
        self._mark = len(session.added)

    async def __aenter__(self) -> "Savepoint":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        if exc_type is not None:
            del self._session.added[self._mark :]
        return False


class ScalarResult:
    def __init__(self, rows) -> None:
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None


class FakeResult:
    def __init__(self, rows) -> None:
        self._rows = rows

    def scalars(self) -> ScalarResult:
        return ScalarResult(self._rows)


class FakeSession:
    def __init__(self, *, rows=None, flush_error: Exception | None = None) -> None:
        self.added = []
        self.rows = list(rows or [])
        self.flush_error = flush_error

    def begin_nested(self) -> Savepoint:
        return Savepoint(self)

    def add(self, obj) -> None:
        self.added.append(obj)

    async def flush(self) -> None:
        if self.flush_error is not None:
            error = self.flush_error
            self.flush_error = None
            raise error

    async def execute(self, statement):  # noqa: ARG002
        return FakeResult(self.rows)

    def expunge(self, obj) -> None:
        if obj in self.added:
            self.added.remove(obj)

    def __contains__(self, obj) -> bool:
        return obj in self.added


class RecordingHandler:
    def __init__(self, event_type: str = PAYMENT_CREATED_EVENT_TYPE, *, fail: bool = False) -> None:
        self.event_type = event_type
        self.fail = fail
        self.messages = []

    async def handle(self, session, message) -> None:
        self.messages.append(message)
        if self.fail:
            session.add(InboxEffect(
                inbox_id=message.id,
                message_id=message.message_id,
                event_type=message.event_type,
                payload={"payment_id": "rolled-back"},
            ))
            raise InboxProcessingError("handler failed")


class LockSequence:
    def __init__(self, rows) -> None:
        self.rows = list(rows)

    async def __call__(self, session, incoming):  # noqa: ARG002
        return self.rows.pop(0)


def _incoming(**overrides) -> IncomingInboxMessage:
    fields = {
        "message_id": "msg-1",
        "topic": "payments.events",
        "partition": 0,
        "offset": 4,
        "event_type": PAYMENT_CREATED_EVENT_TYPE,
        "payload": {"payment_id": "pay-1", "attempt": 1},
    }
    fields.update(overrides)
    return IncomingInboxMessage(**fields)


def _row(**overrides) -> InboxMessage:
    fields = {
        "id": uuid4(),
        "message_id": "msg-1",
        "topic": "payments.events",
        "partition": 0,
        "offset": 4,
        "event_type": PAYMENT_CREATED_EVENT_TYPE,
        "payload": {"payment_id": "pay-1", "attempt": 1},
        "status": InboxStatus.RECEIVED.value,
        "attempts": 0,
        "received_at": datetime(2026, 10, 23, tzinfo=UTC),
    }
    fields.update(overrides)
    return InboxMessage(**fields)


def _registry(*handlers) -> HandlerRegistry:
    registry = HandlerRegistry()
    for handler in handlers:
        registry.register(handler)
    return registry


def test_inbox_migration_revises_the_initial_schema() -> None:
    path = Path("alembic/versions/20261023_0002_inbox.py")
    spec = importlib.util.spec_from_file_location("inbox_migration", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.revision == "20261023_0002"
    assert module.down_revision == "20260610_0001"


def test_inbox_message_id_and_kafka_position_are_unique() -> None:
    names = {constraint.name for constraint in InboxMessage.__table__.constraints}
    assert "uq_inbox_message_id" in names
    assert "uq_inbox_topic_partition_offset" in names
    effect_names = {constraint.name for constraint in InboxEffect.__table__.constraints}
    assert "uq_inbox_effects_message_id" in effect_names


def test_claim_uses_skip_locked() -> None:
    compiled = str(
        claim_received_statement(10).compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        ),
    )
    assert "FOR UPDATE SKIP LOCKED" in compiled.upper()
    assert "status = 'received'" in compiled


def test_registry_dispatches_by_event_type_and_rejects_duplicates() -> None:
    handler = RecordingHandler()
    registry = _registry(handler)

    assert registry.get(PAYMENT_CREATED_EVENT_TYPE) is handler
    with pytest.raises(UnknownInboxEventError, match="unknown.event"):
        registry.get("unknown.event")
    with pytest.raises(ValueError, match="already registered"):
        registry.register(RecordingHandler())


def test_default_registry_handles_payment_created_events() -> None:
    registry = build_default_registry()
    assert isinstance(registry.get(PAYMENT_CREATED_EVENT_TYPE), PaymentCreatedEffectHandler)


@pytest.mark.asyncio
async def test_processed_redelivery_skips_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    row = _row(status=InboxStatus.PROCESSED.value, attempts=1)
    monkeypatch.setattr("app.services.inbox._lock_inbox_message", LockSequence([row]))
    handler = RecordingHandler()

    result = await ingest_message(
        FakeSession(),
        _incoming(),
        registry=_registry(handler),
        max_attempts=3,
        inline=True,
    )

    assert result.duplicate is True
    assert result.commit_offset is True
    assert result.retryable is False
    assert result.handler_invoked is False
    assert handler.messages == []
    assert row.attempts == 1


@pytest.mark.asyncio
async def test_failed_redelivery_does_not_enqueue_another_poison(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    row = _row(status=InboxStatus.FAILED.value, attempts=3, last_error="poison")
    session = FakeSession()
    monkeypatch.setattr("app.services.inbox._lock_inbox_message", LockSequence([row]))

    result = await ingest_message(
        session,
        _incoming(),
        registry=_registry(RecordingHandler()),
        max_attempts=3,
        inline=True,
    )

    assert result.commit_offset is True
    assert result.duplicate is True
    assert session.added == []


@pytest.mark.asyncio
async def test_inline_dispatch_marks_message_processed(monkeypatch: pytest.MonkeyPatch) -> None:
    session = FakeSession()
    monkeypatch.setattr("app.services.inbox._lock_inbox_message", LockSequence([None]))
    handler = RecordingHandler()

    result = await ingest_message(
        session,
        _incoming(),
        registry=_registry(handler),
        max_attempts=3,
        inline=True,
    )

    row = handler.messages[0]
    assert result.status == InboxStatus.PROCESSED.value
    assert result.commit_offset is True
    assert result.handler_invoked is True
    assert result.attempts == 1
    assert row.status == InboxStatus.PROCESSED.value
    assert row.processed_at is not None
    assert row in session.added


@pytest.mark.asyncio
async def test_duplicate_insert_race_uses_the_winning_row(monkeypatch: pytest.MonkeyPatch) -> None:
    winner = _row(status=InboxStatus.PROCESSED.value, attempts=1)
    session = FakeSession(flush_error=IntegrityError("INSERT", {}, Exception("unique")))
    monkeypatch.setattr("app.services.inbox._lock_inbox_message", LockSequence([None, winner]))
    handler = RecordingHandler()

    result = await ingest_message(
        session,
        _incoming(),
        registry=_registry(handler),
        max_attempts=3,
        inline=True,
    )

    assert result.duplicate is True
    assert result.commit_offset is True
    assert handler.messages == []
    assert session.added == []


@pytest.mark.asyncio
async def test_handler_failure_retries_without_keeping_the_side_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = FakeSession()
    monkeypatch.setattr("app.services.inbox._lock_inbox_message", LockSequence([None]))
    handler = RecordingHandler(fail=True)

    result = await ingest_message(
        session,
        _incoming(),
        registry=_registry(handler),
        max_attempts=3,
        inline=True,
    )

    row = next(obj for obj in session.added if isinstance(obj, InboxMessage))
    assert result.retryable is True
    assert result.commit_offset is False
    assert result.attempts == 1
    assert row.status == InboxStatus.RECEIVED.value
    assert row.last_error == "handler failed"
    assert not any(isinstance(obj, InboxEffect) for obj in session.added)
    assert not any(isinstance(obj, OutboxEvent) for obj in session.added)


@pytest.mark.asyncio
async def test_unknown_event_retries_then_dead_letters(monkeypatch: pytest.MonkeyPatch) -> None:
    session = FakeSession()
    monkeypatch.setattr("app.services.inbox._lock_inbox_message", LockSequence([None]))

    first = await ingest_message(
        session,
        _incoming(event_type="unknown.event"),
        registry=HandlerRegistry(),
        max_attempts=2,
        inline=True,
    )
    row = next(obj for obj in session.added if isinstance(obj, InboxMessage))
    assert first.retryable is True
    assert row.attempts == 1

    monkeypatch.setattr("app.services.inbox._lock_inbox_message", LockSequence([row]))
    second = await ingest_message(
        session,
        _incoming(event_type="unknown.event"),
        registry=HandlerRegistry(),
        max_attempts=2,
        inline=True,
    )

    assert second.status == InboxStatus.FAILED.value
    assert second.commit_offset is True
    assert second.attempts == 2
    poison = next(obj for obj in session.added if isinstance(obj, OutboxEvent))
    assert poison.event_type == INBOX_POISON_EVENT_TYPE
    assert poison.payload["message_id"] == "msg-1"
    assert poison.payload["attempts"] == 2
    assert poison.aggregate_id == row.id


@pytest.mark.asyncio
async def test_worker_ingest_stores_the_row_without_running_a_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = FakeSession()
    monkeypatch.setattr("app.services.inbox._lock_inbox_message", LockSequence([None]))
    handler = RecordingHandler()

    result = await ingest_message(
        session,
        _incoming(),
        registry=_registry(handler),
        max_attempts=3,
        inline=False,
    )

    assert result.commit_offset is True
    assert result.handler_invoked is False
    assert result.status == InboxStatus.RECEIVED.value
    assert handler.messages == []


@pytest.mark.asyncio
async def test_worker_claims_received_rows_and_dead_letters_poison() -> None:
    ready = _row(message_id="ready")
    poison = _row(message_id="poison", attempts=2)
    session = FakeSession(rows=[ready, poison])
    handler = RecordingHandler(fail=True)

    results = await process_available_batch(
        session,
        registry=_registry(handler),
        max_attempts=3,
        batch_size=10,
    )

    assert [result.message_id for result in results] == ["ready", "poison"]
    assert ready.status == InboxStatus.RECEIVED.value
    assert ready.attempts == 1
    assert poison.status == InboxStatus.FAILED.value
    assert poison.attempts == 3
    assert any(
        isinstance(obj, OutboxEvent) and obj.payload["message_id"] == "poison"
        for obj in session.added
    )
    assert not any(isinstance(obj, InboxEffect) for obj in session.added)


class EffectQuery:
    def __init__(self, found: bool) -> None:
        self.found = found

    def scalar_one_or_none(self):
        return uuid4() if self.found else None


class EffectSession(FakeSession):
    def __init__(self, found: bool) -> None:
        super().__init__()
        self.found = found

    async def execute(self, statement):  # noqa: ARG002
        return EffectQuery(self.found)


@pytest.mark.asyncio
async def test_payment_handler_is_idempotent_when_the_effect_exists() -> None:
    handler = PaymentCreatedEffectHandler()
    message = _row()
    session = EffectSession(found=True)

    await handler.handle(session, message)

    assert session.added == []

    empty = EffectSession(found=False)
    await handler.handle(empty, message)

    effect = empty.added[0]
    assert isinstance(effect, InboxEffect)
    assert effect.message_id == message.message_id
    assert effect.payload == {"payment_id": "pay-1", "event_type": PAYMENT_CREATED_EVENT_TYPE}


@pytest.mark.asyncio
async def test_payment_handler_rejects_a_payload_without_payment_id() -> None:
    with pytest.raises(InboxProcessingError, match="payment_id"):
        await PaymentCreatedEffectHandler().handle(EffectSession(found=False), _row(payload={}))


@pytest.mark.asyncio
async def test_malformed_message_is_poisoned_immediately(monkeypatch: pytest.MonkeyPatch) -> None:
    session = FakeSession()
    monkeypatch.setattr("app.services.inbox._lock_inbox_message", LockSequence([None]))

    result = await ingest_message(
        session,
        _incoming(malformed=True, error="kafka record is not json"),
        registry=_registry(RecordingHandler()),
        max_attempts=3,
        inline=True,
    )

    assert result.status == InboxStatus.FAILED.value
    assert result.attempts == 3
    assert result.commit_offset is True
    poison = next(obj for obj in session.added if isinstance(obj, OutboxEvent))
    assert poison.payload["last_error"] == "kafka record is not json"
