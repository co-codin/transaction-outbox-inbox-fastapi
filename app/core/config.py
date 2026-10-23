from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.messaging.topology import KAFKA_INBOX_DLQ_TOPIC, KAFKA_PAYMENTS_TOPIC


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    api_key: str = Field(default="change-me", alias="API_KEY")
    database_url: str = Field(
        default="postgresql+asyncpg://payments:payments@localhost:5432/payments",
        alias="DATABASE_URL",
    )
    rabbitmq_url: str = Field(
        default="amqp://guest:guest@localhost:5672/",
        alias="RABBITMQ_URL",
    )
    grpc_port: int = Field(default=50051, alias="GRPC_PORT", ge=1, le=65535)

    outbox_poll_interval_seconds: float = Field(
        default=1.0,
        alias="OUTBOX_POLL_INTERVAL_SECONDS",
        gt=0,
    )
    outbox_batch_size: int = Field(default=50, alias="OUTBOX_BATCH_SIZE", gt=0)
    outbox_retention_seconds: float = Field(
        default=86400.0,
        alias="OUTBOX_RETENTION_SECONDS",
        ge=0,
    )
    outbox_cleanup_interval_seconds: float = Field(
        default=300.0,
        alias="OUTBOX_CLEANUP_INTERVAL_SECONDS",
        gt=0,
    )

    consumer_prefetch_count: int = Field(
        default=10,
        alias="CONSUMER_PREFETCH_COUNT",
        ge=1,
    )

    payment_processing_min_seconds: float = Field(
        default=2.0,
        alias="PAYMENT_PROCESSING_MIN_SECONDS",
        ge=0,
    )
    payment_processing_max_seconds: float = Field(
        default=5.0,
        alias="PAYMENT_PROCESSING_MAX_SECONDS",
        ge=0,
    )
    payment_success_rate: float = Field(
        default=0.9,
        alias="PAYMENT_SUCCESS_RATE",
        ge=0,
        le=1,
    )

    webhook_timeout_seconds: float = Field(
        default=5.0,
        alias="WEBHOOK_TIMEOUT_SECONDS",
        gt=0,
    )

    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_json: bool = Field(default=True, alias="LOG_JSON")

    kafka_enabled: bool = Field(default=True, alias="KAFKA_ENABLED")
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        alias="KAFKA_BOOTSTRAP_SERVERS",
    )
    kafka_client_id: str = Field(
        default="transaction-outbox-inbox",
        alias="KAFKA_CLIENT_ID",
    )
    kafka_payments_topic: str = Field(
        default=KAFKA_PAYMENTS_TOPIC,
        alias="KAFKA_PAYMENTS_TOPIC",
    )
    kafka_inbox_dlq_topic: str = Field(
        default=KAFKA_INBOX_DLQ_TOPIC,
        alias="KAFKA_INBOX_DLQ_TOPIC",
    )
    kafka_inbox_group_id: str = Field(
        default="payments-inbox",
        alias="KAFKA_INBOX_GROUP_ID",
    )
    kafka_request_timeout_ms: int = Field(
        default=10_000,
        alias="KAFKA_REQUEST_TIMEOUT_MS",
        gt=0,
    )
    kafka_startup_timeout_seconds: float = Field(
        default=10.0,
        alias="KAFKA_STARTUP_TIMEOUT_SECONDS",
        gt=0,
    )
    kafka_topic_partitions: int = Field(default=1, alias="KAFKA_TOPIC_PARTITIONS", ge=1)
    kafka_topic_replication_factor: int = Field(
        default=1,
        alias="KAFKA_TOPIC_REPLICATION_FACTOR",
        ge=1,
    )

    inbox_processing_mode: Literal["inline", "worker"] = Field(
        default="inline",
        alias="INBOX_PROCESSING_MODE",
    )
    inbox_max_attempts: int = Field(default=3, alias="INBOX_MAX_ATTEMPTS", ge=1)
    inbox_batch_size: int = Field(default=50, alias="INBOX_BATCH_SIZE", gt=0)
    inbox_poll_interval_seconds: float = Field(
        default=1.0,
        alias="INBOX_POLL_INTERVAL_SECONDS",
        gt=0,
    )
    inbox_retry_backoff_seconds: float = Field(
        default=1.0,
        alias="INBOX_RETRY_BACKOFF_SECONDS",
        ge=0,
    )
    inbox_consumer_max_records: int = Field(
        default=20,
        alias="INBOX_CONSUMER_MAX_RECORDS",
        ge=1,
    )
    inbox_poll_timeout_ms: int = Field(
        default=1_000,
        alias="INBOX_POLL_TIMEOUT_MS",
        ge=0,
    )

    @property
    def payment_processing_delay_range(self) -> tuple[float, float]:
        low = self.payment_processing_min_seconds
        high = max(low, self.payment_processing_max_seconds)
        return low, high


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
