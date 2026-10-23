from dataclasses import dataclass

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.schemas.messages import PAYMENT_CREATED_EVENT_TYPE

KAFKA_PAYMENTS_TOPIC = "payments.events"
KAFKA_INBOX_DLQ_TOPIC = "payments.inbox.dlq"

PAYMENTS_EXCHANGE = RabbitExchange(
    "payments",
    type=ExchangeType.DIRECT,
    durable=True,
)
PAYMENTS_NEW_ROUTING_KEY = "payments.new"
PAYMENTS_NEW_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key=PAYMENTS_NEW_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": "payments.dead",
        "x-dead-letter-routing-key": "payments.dlq",
    },
)

PAYMENTS_DEAD_EXCHANGE = RabbitExchange(
    "payments.dead",
    type=ExchangeType.DIRECT,
    durable=True,
)
PAYMENTS_DLQ_ROUTING_KEY = "payments.dlq"
PAYMENTS_DLQ_QUEUE = RabbitQueue(
    "payments.dlq",
    durable=True,
    routing_key=PAYMENTS_DLQ_ROUTING_KEY,
)

RETRY_DELAYS_MS = (2000, 4000)
PAYMENTS_RETRY_QUEUES = tuple(
    RabbitQueue(
        f"payments.retry.{index}",
        durable=True,
        arguments={
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
            "x-dead-letter-routing-key": PAYMENTS_NEW_ROUTING_KEY,
        },
    )
    for index, delay_ms in enumerate(RETRY_DELAYS_MS, start=1)
)


@dataclass(frozen=True)
class OutboxRoute:
    exchange: RabbitExchange
    queue: RabbitQueue
    routing_key: str


def route_for_event_type(event_type: str) -> OutboxRoute:
    if event_type == PAYMENT_CREATED_EVENT_TYPE:
        return OutboxRoute(PAYMENTS_EXCHANGE, PAYMENTS_NEW_QUEUE, PAYMENTS_NEW_ROUTING_KEY)
    raise ValueError(f"Unsupported outbox event type: {event_type}")


async def declare_topology(broker: RabbitBroker) -> None:
    payments_exchange = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    dead_exchange = await broker.declare_exchange(PAYMENTS_DEAD_EXCHANGE)

    payments_queue = await broker.declare_queue(PAYMENTS_NEW_QUEUE)
    await payments_queue.bind(payments_exchange, routing_key=PAYMENTS_NEW_ROUTING_KEY)

    dlq = await broker.declare_queue(PAYMENTS_DLQ_QUEUE)
    await dlq.bind(dead_exchange, routing_key=PAYMENTS_DLQ_ROUTING_KEY)

    for retry_queue in PAYMENTS_RETRY_QUEUES:
        await broker.declare_queue(retry_queue)
