import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class InboxStatus(str, enum.Enum):
    RECEIVED = "received"
    PROCESSED = "processed"
    FAILED = "failed"


class InboxMessage(Base):
    """Durable record of a Kafka delivery.

    ``message_id`` is the idempotency key. A redelivery of the same logical
    event resolves to this row and does not apply the handler side effect again
    once the row is terminal.
    """

    __tablename__ = "inbox"
    __table_args__ = (
        UniqueConstraint("message_id", name="uq_inbox_message_id"),
        UniqueConstraint(
            "topic",
            "partition",
            "offset",
            name="uq_inbox_topic_partition_offset",
        ),
        CheckConstraint(
            "status in ('received', 'processed', 'failed')",
            name="ck_inbox_status",
        ),
        CheckConstraint("attempts >= 0", name="ck_inbox_attempts_non_negative"),
        Index("ix_inbox_status_received_at", "status", "received_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    message_id: Mapped[str] = mapped_column(String(255), nullable=False)
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    partition: Mapped[int] = mapped_column(Integer, nullable=False)
    offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default=InboxStatus.RECEIVED.value,
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class InboxEffect(Base):
    """Example handler side effect. ``message_id`` makes the write idempotent."""

    __tablename__ = "inbox_effects"
    __table_args__ = (
        UniqueConstraint("message_id", name="uq_inbox_effects_message_id"),
        Index("ix_inbox_effects_inbox_id", "inbox_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    inbox_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("inbox.id", name="fk_inbox_effects_inbox_id", ondelete="CASCADE"),
        nullable=False,
    )
    message_id: Mapped[str] = mapped_column(String(255), nullable=False)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
