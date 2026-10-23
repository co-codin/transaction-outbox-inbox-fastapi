import pytest

from app.messaging.topology import (
    KAFKA_INBOX_DLQ_TOPIC,
    KAFKA_PAYMENTS_TOPIC,
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
    PAYMENTS_NEW_ROUTING_KEY,
    PAYMENTS_RETRY_QUEUES,
    route_for_event_type,
)
from app.schemas.messages import PAYMENT_CREATED_EVENT_TYPE


def test_payments_queue_declares_dead_letter_routing() -> None:
    assert PAYMENTS_NEW_QUEUE.arguments["x-dead-letter-exchange"] == "payments.dead"
    assert PAYMENTS_NEW_QUEUE.arguments["x-dead-letter-routing-key"] == "payments.dlq"


def test_retry_queues_use_exponential_ttl_backoff() -> None:
    assert [queue.arguments["x-message-ttl"] for queue in PAYMENTS_RETRY_QUEUES] == [
        2000,
        4000,
    ]
    assert all(
        queue.arguments["x-dead-letter-exchange"] == "payments"
        and queue.arguments["x-dead-letter-routing-key"] == "payments.new"
        for queue in PAYMENTS_RETRY_QUEUES
    )


def test_outbox_routes_payment_events() -> None:
    route = route_for_event_type(PAYMENT_CREATED_EVENT_TYPE)

    assert route.exchange == PAYMENTS_EXCHANGE
    assert route.queue == PAYMENTS_NEW_QUEUE
    assert route.routing_key == PAYMENTS_NEW_ROUTING_KEY


def test_kafka_topics_are_centralized() -> None:
    assert KAFKA_PAYMENTS_TOPIC == "payments.events"
    assert KAFKA_INBOX_DLQ_TOPIC == "payments.inbox.dlq"


def test_outbox_route_rejects_unknown_event_type() -> None:
    with pytest.raises(ValueError, match="Unsupported outbox event type"):
        route_for_event_type("unknown.event")
