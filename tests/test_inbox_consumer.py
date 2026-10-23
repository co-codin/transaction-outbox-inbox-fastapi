import asyncio
import json
from types import SimpleNamespace

import pytest

from app.messaging.inbox import InboxConsumer, parse_consumer_record
from app.schemas.messages import PAYMENT_CREATED_EVENT_TYPE
from app.services.inbox import HandlerRegistry, InboxDeliveryResult


class FakeKafka:
    def __init__(self, records, events: list[str]) -> None:
        self._records = list(records)
        self.events = events
        self.commits = []
        self.seeks = []
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def getmany(self, *partitions, timeout_ms: int = 0, max_records: int | None = None):  # noqa: ARG002
        if not self._records:
            await asyncio.sleep(0.01)
            return {}
        batch = self._records[: max_records or len(self._records)]
        self._records = self._records[len(batch) :]
        return {("payments.events", 0): batch}

    async def commit(self, offsets=None) -> None:
        self.events.append("kafka-commit")
        self.commits.append(offsets)

    def seek(self, partition, offset: int) -> None:
        self.seeks.append((partition.topic, partition.partition, offset))


class Begin:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        self._events.append("db-rollback" if exc_type else "db-commit")
        return False


class FakeSession:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False

    def begin(self) -> Begin:
        return Begin(self._events)


def _record(offset: int, message_id: str = "msg-1", event_type: str = PAYMENT_CREATED_EVENT_TYPE):
    body = {
        "message_id": message_id,
        "event_type": event_type,
        "aggregate_id": "agg-1",
        "payload": {"payment_id": "pay-1"},
    }
    return SimpleNamespace(
        topic="payments.events",
        partition=0,
        offset=offset,
        key=message_id.encode(),
        value=json.dumps(body).encode(),
        headers=[
            ("message_id", message_id.encode()),
            ("event_type", event_type.encode()),
        ],
    )


def _result(offset: int, *, commit_offset: bool, duplicate: bool = False) -> InboxDeliveryResult:
    return InboxDeliveryResult(
        message_id="msg-1",
        event_type=PAYMENT_CREATED_EVENT_TYPE,
        topic="payments.events",
        partition=0,
        offset=offset,
        status="processed" if commit_offset else "received",
        attempts=1,
        duplicate=duplicate,
        commit_offset=commit_offset,
        retryable=not commit_offset,
        handler_invoked=not duplicate,
    )


def _consumer(records, events, ingest, *, before_commit=None) -> tuple[InboxConsumer, FakeKafka]:
    kafka = FakeKafka(records, events)

    def factory():
        return FakeSession(events)

    consumer = InboxConsumer(
        consumer=kafka,
        session_factory=factory,
        registry=HandlerRegistry(),
        max_attempts=3,
        inline=True,
        retry_backoff_seconds=0,
        max_records=10,
        poll_timeout_ms=1,
        before_commit=before_commit,
    )
    return consumer, kafka


@pytest.mark.asyncio
async def test_offset_is_committed_only_after_the_database_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    stop = asyncio.Event()

    async def ingest(session, incoming, **kwargs):  # noqa: ARG001
        events.append(f"ingest:{incoming.offset}")
        if incoming.offset == 1:
            stop.set()
        return _result(incoming.offset, commit_offset=True)

    monkeypatch.setattr("app.messaging.inbox.ingest_message", ingest)
    consumer, kafka = _consumer([_record(0), _record(1, message_id="msg-2")], events, ingest)

    await asyncio.wait_for(consumer.run(stop), timeout=2)

    assert events == [
        "ingest:0",
        "db-commit",
        "kafka-commit",
        "ingest:1",
        "db-commit",
        "kafka-commit",
    ]
    assert [next(iter(commit.values())).offset for commit in kafka.commits] == [1, 2]
    assert kafka.seeks == []
    assert kafka.stopped is True


@pytest.mark.asyncio
async def test_crash_before_commit_does_not_commit_the_offset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    stop = asyncio.Event()

    async def ingest(session, incoming, **kwargs):  # noqa: ARG001
        events.append(f"ingest:{incoming.offset}")
        return _result(incoming.offset, commit_offset=True)

    async def crash(result) -> None:  # noqa: ARG001
        stop.set()
        raise RuntimeError("crash before commit")

    monkeypatch.setattr("app.messaging.inbox.ingest_message", ingest)
    consumer, kafka = _consumer([_record(7)], events, ingest, before_commit=crash)

    await asyncio.wait_for(consumer.run(stop), timeout=2)

    assert events == ["ingest:7", "db-rollback"]
    assert kafka.commits == []
    assert kafka.seeks == [("payments.events", 0, 7)]


@pytest.mark.asyncio
async def test_retryable_failure_holds_the_partition_offset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    seen: list[int] = []
    stop = asyncio.Event()

    async def ingest(session, incoming, **kwargs):  # noqa: ARG001
        seen.append(incoming.offset)
        events.append(f"ingest:{incoming.offset}")
        stop.set()
        return _result(incoming.offset, commit_offset=False)

    monkeypatch.setattr("app.messaging.inbox.ingest_message", ingest)
    consumer, kafka = _consumer([_record(3), _record(4)], events, ingest)

    await asyncio.wait_for(consumer.run(stop), timeout=2)

    assert seen == [3]
    assert events == ["ingest:3", "db-commit"]
    assert kafka.commits == []
    assert kafka.seeks == [("payments.events", 0, 3)]


def test_parser_reads_the_outbox_envelope_and_headers() -> None:
    parsed = parse_consumer_record(_record(9, message_id="outbox-1"))

    assert parsed.message_id == "outbox-1"
    assert parsed.event_type == PAYMENT_CREATED_EVENT_TYPE
    assert parsed.payload == {"payment_id": "pay-1"}
    assert parsed.offset == 9
    assert parsed.malformed is False


def test_parser_marks_invalid_json_and_missing_identity_as_malformed() -> None:
    invalid = SimpleNamespace(
        topic="payments.events",
        partition=1,
        offset=2,
        key=None,
        value=b"not-json",
        headers=[],
    )
    parsed = parse_consumer_record(invalid)
    assert parsed.malformed is True
    assert parsed.message_id == "payments.events:1:2"
    assert "not json" in (parsed.error or "")

    missing_type = SimpleNamespace(
        topic="payments.events",
        partition=0,
        offset=1,
        key=b"only-key",
        value=b'{"payload":{"payment_id":"pay-1"}}',
        headers=[],
    )
    parsed = parse_consumer_record(missing_type)
    assert parsed.malformed is True
    assert parsed.message_id == "only-key"
    assert parsed.error == "event_type is missing"


def test_parser_fits_oversized_message_ids() -> None:
    huge = "m" * 400
    record = _record(0, message_id=huge)
    parsed = parse_consumer_record(record)
    assert len(parsed.message_id) <= 255
    assert parsed.message_id != huge
