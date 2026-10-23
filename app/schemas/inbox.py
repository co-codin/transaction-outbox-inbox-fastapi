from typing import Any

from pydantic import BaseModel, Field

INBOX_POISON_EVENT_TYPE = "inbox.message.failed"
MESSAGE_ID_MAX_LENGTH = 255


class KafkaEventEnvelope(BaseModel):
    """Wire format the outbox relay writes to the payments Kafka topic."""

    message_id: str = Field(min_length=1, max_length=MESSAGE_ID_MAX_LENGTH)
    event_type: str = Field(min_length=1, max_length=100)
    aggregate_id: str
    payload: dict[str, Any]


class InboxPoisonEvent(BaseModel):
    """Payload stored in the outbox and published to the inbox DLQ topic."""

    message_id: str
    event_type: str
    topic: str
    partition: int
    offset: int
    attempts: int = Field(ge=1)
    last_error: str | None
    payload: dict[str, Any]
    failed_at: str
