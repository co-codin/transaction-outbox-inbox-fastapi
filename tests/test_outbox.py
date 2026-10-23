from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID

import pytest

from app.messaging.outbox import OutboxPublisher
from app.models.outbox import OutboxStatus
from app.schemas.inbox import INBOX_POISON_EVENT_TYPE
from app.schemas.messages import PAYMENT_CREATED_EVENT_TYPE


class FakeResult:
    def __init__(self, events=None, rowcount: int = 0) -> None:
        self._events = events or []
        self.rowcount = rowcount

    def scalars(self):
        return self._events


class FakeSession:
    def __init__(self, events) -> None:
        self._events = events

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    def begin(self):
        return self

    async def execute(self, statement):  # noqa: ARG002
        return FakeResult(self._events)


class FailingBroker:
    async def publish(self, message, **kwargs) -> None:  # noqa: ARG002
        raise ConnectionError("rabbitmq unavailable")


class RecordingBroker:
    def __init__(self) -> None:
        self.published = []

    async def publish(self, message, **kwargs) -> None:
        self.published.append({"message": message, **kwargs})


def _event(event_type: str = PAYMENT_CREATED_EVENT_TYPE) -> SimpleNamespace:
    return SimpleNamespace(
        id=UUID("a1163e5d-8f5e-432b-96dd-48ff68204948"),
        aggregate_id=UUID("b2163e5d-8f5e-432b-96dd-48ff68204948"),
        event_type=event_type,
        payload={"payment_id": "b2163e5d-8f5e-432b-96dd-48ff68204948", "attempt": 1},
        status=OutboxStatus.PENDING.value,
        attempts=0,
        last_error=None,
        created_at=datetime(2026, 6, 10, tzinfo=UTC),
        published_at=None,
    )


class RecordingKafka:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.payments = []
        self.poisons = []

    async def publish_payment_event(self, **kwargs) -> None:
        if self.fail:
            raise ConnectionError("kafka unavailable")
        self.payments.append(kwargs)

    async def publish_poison_event(self, *, message_id: str, payload) -> None:
        if self.fail:
            raise ConnectionError("kafka unavailable")
        self.poisons.append({"message_id": message_id, "payload": payload})


def _publisher(broker, events, kafka=None) -> OutboxPublisher:
    return OutboxPublisher(
        broker,
        lambda: FakeSession(events),
        poll_interval_seconds=1.0,
        batch_size=50,
        retention_seconds=86400,
        cleanup_interval_seconds=300,
        kafka_publisher=kafka,
    )


@pytest.mark.asyncio
async def test_outbox_publish_failures_remain_pending_for_later_retry() -> None:
    event = _event()
    publisher = _publisher(FailingBroker(), [event])

    for _ in range(3):
        published_count = await publisher.publish_once()

    assert published_count == 1
    assert event.attempts == 3
    assert event.status == OutboxStatus.PENDING.value
    assert event.last_error == "rabbitmq unavailable"
    assert event.published_at is None


@pytest.mark.asyncio
async def test_outbox_success_marks_event_published() -> None:
    event = _event()
    broker = RecordingBroker()
    publisher = _publisher(broker, [event])

    published_count = await publisher.publish_once()

    assert published_count == 1
    assert event.status == OutboxStatus.PUBLISHED.value
    assert event.last_error is None
    assert isinstance(event.published_at, datetime)
    assert broker.published[0]["message"] == event.payload


@pytest.mark.asyncio
async def test_outbox_publishes_payment_events_to_kafka_and_rabbit() -> None:
    event = _event()
    broker = RecordingBroker()
    kafka = RecordingKafka()
    publisher = _publisher(broker, [event], kafka)

    await publisher.publish_once()

    assert event.status == OutboxStatus.PUBLISHED.value
    assert kafka.payments[0]["message_id"] == str(event.id)
    assert kafka.payments[0]["event_type"] == event.event_type
    assert kafka.payments[0]["payload"] == event.payload
    assert len(broker.published) == 1
    assert kafka.poisons == []


@pytest.mark.asyncio
async def test_outbox_kafka_failure_keeps_event_pending() -> None:
    event = _event()
    broker = RecordingBroker()
    publisher = _publisher(broker, [event], RecordingKafka(fail=True))

    await publisher.publish_once()

    assert event.status == OutboxStatus.PENDING.value
    assert event.attempts == 1
    assert event.last_error == "kafka unavailable"
    assert len(broker.published) == 1


@pytest.mark.asyncio
async def test_outbox_poison_events_publish_only_to_the_dlq_topic() -> None:
    event = _event(INBOX_POISON_EVENT_TYPE)
    event.payload = {"message_id": "inbox-1", "event_type": "payments.new", "attempts": 3}
    broker = RecordingBroker()
    kafka = RecordingKafka()
    publisher = _publisher(broker, [event], kafka)

    await publisher.publish_once()

    assert event.status == OutboxStatus.PUBLISHED.value
    assert broker.published == []
    assert kafka.poisons == [{"message_id": "inbox-1", "payload": event.payload}]
