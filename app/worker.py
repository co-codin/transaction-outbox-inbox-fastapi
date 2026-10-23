from faststream import FastStream
from faststream.rabbit import Channel, RabbitBroker

from app.core.config import settings
from app.core.logging import configure_logging
from app.messaging.topology import PAYMENTS_EXCHANGE, PAYMENTS_NEW_QUEUE, declare_topology
from app.schemas.messages import PaymentEvent
from app.services.processor import process_payment_event

configure_logging(level=settings.log_level, json_logs=settings.log_json)

broker = RabbitBroker(settings.rabbitmq_url)
app = FastStream(broker)


@app.after_startup
async def setup_topology() -> None:
    await declare_topology(broker)


@broker.subscriber(
    PAYMENTS_NEW_QUEUE,
    PAYMENTS_EXCHANGE,
    channel=Channel(prefetch_count=settings.consumer_prefetch_count),
)
async def handle_payment(event: PaymentEvent) -> None:
    await process_payment_event(event, broker)
