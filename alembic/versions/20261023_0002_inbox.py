"""create inbox and inbox_effects tables

Revision ID: 20261023_0002
Revises: 20260610_0001
Create Date: 2026-10-23 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20261023_0002"
down_revision: str | None = "20260610_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "inbox",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("message_id", sa.String(length=255), nullable=False),
        sa.Column("topic", sa.String(length=255), nullable=False),
        sa.Column("partition", sa.Integer(), nullable=False),
        sa.Column("offset", sa.BigInteger(), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status in ('received', 'processed', 'failed')",
            name="ck_inbox_status",
        ),
        sa.CheckConstraint("attempts >= 0", name="ck_inbox_attempts_non_negative"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("message_id", name="uq_inbox_message_id"),
        sa.UniqueConstraint(
            "topic",
            "partition",
            "offset",
            name="uq_inbox_topic_partition_offset",
        ),
    )
    op.create_index(
        "ix_inbox_status_received_at",
        "inbox",
        ["status", "received_at"],
        unique=False,
    )

    op.create_table(
        "inbox_effects",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("inbox_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("message_id", sa.String(length=255), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["inbox_id"],
            ["inbox.id"],
            name="fk_inbox_effects_inbox_id",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("message_id", name="uq_inbox_effects_message_id"),
    )
    op.create_index(
        "ix_inbox_effects_inbox_id",
        "inbox_effects",
        ["inbox_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_inbox_effects_inbox_id", table_name="inbox_effects")
    op.drop_table("inbox_effects")
    op.drop_index("ix_inbox_status_received_at", table_name="inbox")
    op.drop_table("inbox")
