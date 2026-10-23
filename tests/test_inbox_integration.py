"""Kafka and Postgres proof of the transactional inbox.

These tests start disposable containers and are skipped when Docker is unavailable.
"""

import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.messaging.inbox import InboxConsumer
from app.messaging.kafka import KafkaOutboxPublisher
from app.messaging.outbox import OutboxPublisher
from app.models.inbox import InboxEffect, InboxMessage, InboxStatus
from app.models.outbox import OutboxEvent, OutboxStatus
from app.schemas.messages import PAYMENT_CREATED_EVENT_TYPE
from app.services.inbox import (
    HandlerRegistry,
    InboxProcessingError,
    PaymentCreatedEffectHandler,
)

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.integration


def _docker_available() -> bool:
    if os.getenv("INBOX_INTEGRATION") == "0":
        return False
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=15,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0


@pytest.fixture(scope="module")
def inbox_stack():
    if not _docker_available():
        pytest.skip("Docker is required for inbox integration tests")

    from testcontainers.community.kafka import KafkaContainer
    from testcontainers.community.postgres import PostgresContainer

    postgres = PostgresContainer(
        "postgres:16-alpine",
        username="payments",
        password="payments",
        dbname="payments",
    )
    kafka = KafkaContainer()
    postgres.start()
    try:
        kafka.start(timeout=120)
    except Exception:
        postgres.stop()
        raise
    database_url = postgres.get_connection_url(driver="asyncpg")
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env["PYTHONPATH"] = str(ROOT)
    migrated = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if migrated.returncode != 0:
        kafka.stop()
        postgres.stop()
        raise RuntimeError(migrated.stderr or migrated.stdout)

    stack = {
        "bootstrap": kafka.get_bootstrap_server(),
        "database_url": database_url,
    }
    try:
        yield stack
    finally:
        kafka.stop()
        postgres.stop()


@pytest.fixture
async def sessions(inbox_stack):
    engine = create_async_engine(inbox_stack["database_url"], pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        async with session.begin():
            await session.execute(
                text("TRUNCATE inbox_effects, inbox, outbox RESTART IDENTITY CASCADE"),
            )
    try:
        yield session_factory
    finally:
        await engine.dispose()


def _topics() -> tuple[str, str]:
    suffix = uuid.uuid4().hex[:8]
    return f"payments.events.{suffix}", f"payments.inbox.dlq.{suffix}"


async def _produce(bootstrap: str, topic: str, message_id: str, payload: dict) -> None:
    from aiokafka import AIOKafkaProducer

    producer = AIOKafkaProducer(bootstrap_servers=bootstrap, acks="all")
    await producer.start()
    try:
        body = {
            "message_id": message_id,
            "event_type": PAYMENT_CREATED_EVENT_TYPE,
            "aggregate_id": message_id,
            "payload": payload,
        }
        await producer.send_and_wait(
            topic,
            key=message_id.encode(),
            value=json.dumps(body).encode(),
            headers=[
                ("message_id", message_id.encode()),
                ("event_type", PAYMENT_CREATED_EVENT_TYPE.encode()),
            ],
        )
    finally:
        await producer.stop()


async def _ensure(bootstrap: str, payments: str, dlq: str) -> KafkaOutboxPublisher:
    publisher = KafkaOutboxPublisher(
        bootstrap_servers=bootstrap,
        client_id=f"inbox-it-{uuid.uuid4().hex[:6]}",
        payments_topic=payments,
        dlq_topic=dlq,
        request_timeout_ms=10_000,
        startup_timeout_seconds=20,
        partitions=1,
        replication_factor=1,
    )
    await publisher.start()
    await publisher.ensure_topics()
    return publisher


def _kafka_consumer(bootstrap: str, topic: str, group: str):
    from aiokafka import AIOKafkaConsumer

    return AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        request_timeout_ms=10_000,
    )


class ObservingConsumer:
    def __init__(self, inner, session_factory) -> None:
        self.inner = inner
        self.session_factory = session_factory
        self.effect_counts: list[int] = []

    async def start(self) -> None:
        await self.inner.start()

    async def stop(self) -> None:
        await self.inner.stop()

    async def getmany(self, *partitions, timeout_ms: int = 0, max_records: int | None = None):
        return await self.inner.getmany(
            *partitions,
            timeout_ms=timeout_ms,
            max_records=max_records,
        )

    async def commit(self, offsets=None) -> None:
        async with self.session_factory() as session:
            count = await session.scalar(select(func.count()).select_from(InboxEffect))
        self.effect_counts.append(int(count or 0))
        await self.inner.commit(offsets)

    def seek(self, partition, offset: int) -> None:
        self.inner.seek(partition, offset)


class CountingHandler:
    event_type = PAYMENT_CREATED_EVENT_TYPE

    def __init__(self) -> None:
        self.calls = 0
        self._inner = PaymentCreatedEffectHandler()

    async def handle(self, session, message) -> None:
        self.calls += 1
        await self._inner.handle(session, message)


class PoisonHandler:
    event_type = PAYMENT_CREATED_EVENT_TYPE

    async def handle(self, session, message) -> None:  # noqa: ARG002
        raise InboxProcessingError("poison payload")


async def _run_until(consumer: InboxConsumer, session_factory, predicate, timeout: float = 40) -> None:
    stop = asyncio.Event()
    task = asyncio.create_task(consumer.run(stop))

    async def watch() -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            async with session_factory() as session:
                if await predicate(session):
                    stop.set()
                    return
            await asyncio.sleep(0.05)
        stop.set()
        raise AssertionError("timed out waiting for inbox condition")

    try:
        await watch()
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=10)


async def _effect_count(session) -> int:
    return int(await session.scalar(select(func.count()).select_from(InboxEffect)) or 0)


@pytest.mark.asyncio
async def test_duplicate_messages_are_processed_once_and_offset_commits_after_db(
    inbox_stack,
    sessions,
) -> None:
    payments, dlq = _topics()
    publisher = await _ensure(inbox_stack["bootstrap"], payments, dlq)
    message_id = f"dup-{uuid.uuid4()}"
    try:
        await _produce(inbox_stack["bootstrap"], payments, message_id, {"payment_id": "pay-dup"})
        await _produce(inbox_stack["bootstrap"], payments, message_id, {"payment_id": "pay-dup"})
    finally:
        await publisher.stop()

    handler = CountingHandler()
    registry = HandlerRegistry()
    registry.register(handler)
    inner = _kafka_consumer(inbox_stack["bootstrap"], payments, f"dup-{uuid.uuid4()}")
    observed = ObservingConsumer(inner, sessions)
    consumer = InboxConsumer(
        consumer=observed,
        session_factory=sessions,
        registry=registry,
        max_attempts=3,
        inline=True,
        retry_backoff_seconds=0,
        max_records=10,
        poll_timeout_ms=500,
    )

    async def done(session) -> bool:
        count = await _effect_count(session)
        rows = (await session.scalars(select(InboxMessage).where(InboxMessage.message_id == message_id))).all()
        return count == 1 and len(rows) == 1 and len(observed.effect_counts) >= 2

    await _run_until(consumer, sessions, done)

    assert handler.calls == 1
    assert observed.effect_counts == [1, 1]
    async with sessions() as session:
        row = await session.scalar(select(InboxMessage).where(InboxMessage.message_id == message_id))
        assert row is not None
        assert row.status == InboxStatus.PROCESSED.value
        assert await _effect_count(session) == 1


@pytest.mark.asyncio
async def test_crash_before_commit_redelivers_without_a_second_effect(
    inbox_stack,
    sessions,
) -> None:
    payments, dlq = _topics()
    publisher = await _ensure(inbox_stack["bootstrap"], payments, dlq)
    message_id = f"crash-{uuid.uuid4()}"
    try:
        await _produce(
            inbox_stack["bootstrap"],
            payments,
            message_id,
            {"payment_id": "pay-crash"},
        )
    finally:
        await publisher.stop()

    handler = CountingHandler()
    registry = HandlerRegistry()
    registry.register(handler)
    crashes = {"count": 0}

    async def before_commit(result) -> None:  # noqa: ARG001
        if crashes["count"] == 0:
            crashes["count"] += 1
            raise RuntimeError("crash before commit")

    inner = _kafka_consumer(inbox_stack["bootstrap"], payments, f"crash-{uuid.uuid4()}")
    observed = ObservingConsumer(inner, sessions)
    consumer = InboxConsumer(
        consumer=observed,
        session_factory=sessions,
        registry=registry,
        max_attempts=3,
        inline=True,
        retry_backoff_seconds=0,
        max_records=10,
        poll_timeout_ms=500,
        before_commit=before_commit,
    )

    async def done(session) -> bool:
        return await _effect_count(session) == 1 and handler.calls >= 2

    await _run_until(consumer, sessions, done)

    assert crashes["count"] == 1
    assert handler.calls == 2
    assert observed.effect_counts == [1]
    async with sessions() as session:
        assert await _effect_count(session) == 1


@pytest.mark.asyncio
async def test_poison_message_is_marked_failed_and_published_to_the_dlq(
    inbox_stack,
    sessions,
) -> None:
    from aiokafka import AIOKafkaConsumer

    payments, dlq = _topics()
    kafka_publisher = await _ensure(inbox_stack["bootstrap"], payments, dlq)
    message_id = f"poison-{uuid.uuid4()}"
    try:
        await _produce(
            inbox_stack["bootstrap"],
            payments,
            message_id,
            {"payment_id": "pay-poison"},
        )
    finally:
        await kafka_publisher.stop()

    registry = HandlerRegistry()
    registry.register(PoisonHandler())
    consumer = InboxConsumer(
        consumer=_kafka_consumer(inbox_stack["bootstrap"], payments, f"poison-{uuid.uuid4()}"),
        session_factory=sessions,
        registry=registry,
        max_attempts=2,
        inline=True,
        retry_backoff_seconds=0,
        max_records=10,
        poll_timeout_ms=200,
    )

    async def failed(session) -> bool:
        row = await session.scalar(select(InboxMessage).where(InboxMessage.message_id == message_id))
        return row is not None and row.status == InboxStatus.FAILED.value

    await _run_until(consumer, sessions, failed)

    async with sessions() as session:
        row = await session.scalar(select(InboxMessage).where(InboxMessage.message_id == message_id))
        assert row is not None
        assert row.attempts == 2
        assert row.last_error == "poison payload"
        assert await _effect_count(session) == 0
        poison = await session.scalar(
            select(OutboxEvent).where(OutboxEvent.aggregate_id == row.id),
        )
        assert poison is not None
        assert poison.status == OutboxStatus.PENDING.value

    relay_kafka = await _ensure(inbox_stack["bootstrap"], payments, dlq)
    relay = OutboxPublisher(
        _Broker(),
        sessions,
        poll_interval_seconds=0.1,
        batch_size=10,
        retention_seconds=3600,
        cleanup_interval_seconds=3600,
        kafka_publisher=relay_kafka,
    )
    try:
        published = await relay.publish_once()
        assert published == 1
    finally:
        await relay_kafka.stop()

    reader = AIOKafkaConsumer(
        dlq,
        bootstrap_servers=inbox_stack["bootstrap"],
        group_id=f"dlq-{uuid.uuid4()}",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await reader.start()
    try:
        deadline = time.monotonic() + 20
        found = None
        while time.monotonic() < deadline and found is None:
            batches = await reader.getmany(timeout_ms=500, max_records=10)
            for records in batches.values():
                for record in records:
                    body = json.loads(record.value.decode())
                    if body.get("message_id") == message_id:
                        found = body
        assert found is not None
        assert found["attempts"] == 2
        assert found["last_error"] == "poison payload"
        assert found["payload"]["payment_id"] == "pay-poison"
    finally:
        await reader.stop()


@pytest.mark.asyncio
async def test_outbox_relay_reaches_the_inbox_handler(inbox_stack, sessions) -> None:
    payments, dlq = _topics()
    kafka_publisher = await _ensure(inbox_stack["bootstrap"], payments, dlq)
    payment_id = uuid.uuid4()
    async with sessions() as session:
        async with session.begin():
            session.add(
                OutboxEvent(
                    aggregate_id=payment_id,
                    event_type=PAYMENT_CREATED_EVENT_TYPE,
                    payload={"payment_id": str(payment_id), "attempt": 1},
                ),
            )
    relay = OutboxPublisher(
        _Broker(),
        sessions,
        poll_interval_seconds=0.1,
        batch_size=10,
        retention_seconds=3600,
        cleanup_interval_seconds=3600,
        kafka_publisher=kafka_publisher,
    )
    try:
        assert await relay.publish_once() == 1
    finally:
        await kafka_publisher.stop()

    consumer = InboxConsumer(
        consumer=_kafka_consumer(inbox_stack["bootstrap"], payments, f"relay-{uuid.uuid4()}"),
        session_factory=sessions,
        registry=_default_registry(),
        max_attempts=3,
        inline=True,
        retry_backoff_seconds=0,
        max_records=10,
        poll_timeout_ms=500,
    )

    async def done(session) -> bool:
        return await _effect_count(session) == 1

    await _run_until(consumer, sessions, done)
    async with sessions() as session:
        effect = await session.scalar(select(InboxEffect))
        assert effect is not None
        assert effect.payload["payment_id"] == str(payment_id)


@pytest.mark.asyncio
async def test_skip_locked_prevents_two_workers_from_claiming_one_row(inbox_stack, sessions) -> None:
    from app.services.inbox import claim_received_statement

    async with sessions() as session:
        async with session.begin():
            session.add(
                InboxMessage(
                    id=uuid.uuid4(),
                    message_id=f"lock-{uuid.uuid4()}",
                    topic="payments.events",
                    partition=0,
                    offset=0,
                    event_type=PAYMENT_CREATED_EVENT_TYPE,
                    payload={"payment_id": "pay-lock"},
                    status=InboxStatus.RECEIVED.value,
                    attempts=0,
                ),
            )

    async with sessions() as session_one, sessions() as session_two:
        async with session_one.begin():
            claimed = list((await session_one.execute(claim_received_statement(10))).scalars())
            assert len(claimed) == 1
            async with session_two.begin():
                other = list((await session_two.execute(claim_received_statement(10))).scalars())
                assert other == []


class _Broker:
    async def publish(self, message, **kwargs) -> None:  # noqa: ARG002
        return None


def _default_registry() -> HandlerRegistry:
    registry = HandlerRegistry()
    registry.register(PaymentCreatedEffectHandler())
    return registry
