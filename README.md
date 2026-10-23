# transaction-outbox-inbox-fastapi

A FastAPI service that demonstrates the **transactional outbox** and
**transactional inbox** patterns on one payment flow. The repository was
renamed from `transaction-outbox-fastapi`.

The API accepts a payment, persists it with an outbox event in a single
database transaction, and a relay publishes that event to RabbitMQ and Kafka.
A FastStream consumer processes the RabbitMQ copy and sends one webhook. A
Kafka inbox consumer records the same event idempotently and applies an
example side effect exactly once.

## Contents

- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Quick Start (Docker)](#quick-start-docker)
- [API Reference](#api-reference)
- [Usage Examples](#usage-examples)
- [Local Development](#local-development)
- [Configuration](#configuration)
- [Delivery Guarantees](#delivery-guarantees)

## Architecture

```
client ──POST /payments──▶  API  ──┐  (one DB transaction)
                                   ├─▶ payments table  (status=pending)
                                   └─▶ outbox  table   (status=pending)
                                            │
                       outbox relay ────────┘  SELECT … FOR UPDATE SKIP LOCKED
                                            │
                         ┌──────────────────┴───────────────────┐
                         ▼                                      ▼
           RabbitMQ payments exchange                 Kafka payments.events
           routing key: payments.new                  key = outbox id
                         │                                      │
                         ▼                                      ▼
              [ payments.new ] consumer              inbox consumer (aiokafka)
                         │                              one DB transaction:
                         │                              INSERT inbox (unique message_id)
                         │                              + handler side effect
                         │                                      │
                         │                              commit DB, then commit offset
                         │                                      │
                         │                                      ▼
                         │                              inbox_effects (exactly once)
                         │
                         │  on poison: outbox row inbox.message.failed
                         │                                      │
                         │                                      ▼
                         │                              Kafka payments.inbox.dlq
                         ▼
              gateway + webhook (once)
              retries 2s, 4s, then payments.dlq
```

## Outbox and inbox

| | Transactional outbox | Transactional inbox |
| --- | --- | --- |
| Problem | The database commit and the broker publish are not one atomic operation. | A Kafka redelivery can apply a side effect more than once. |
| Write | `payments` and `outbox` commit together. | The inbox row and the handler side effect commit together. |
| Publish / consume | The relay publishes only after that commit. | The Kafka offset is committed only after the database commit. |
| Idempotency key | `payments.idempotency_key` for intake. | `inbox.message_id` for delivery. |
| Duplicates | The payment consumer skips terminal rows. `webhook_sent_at` blocks a second webhook. | A redelivery of a processed or failed `message_id` does not run the handler again. |
| Failure | RabbitMQ TTL retries, then `payments.dlq`. Kafka publish failure leaves the outbox row pending. | Handler failures increment `attempts`. After the max, status becomes `failed` and one `inbox.message.failed` outbox row is relayed to `payments.inbox.dlq`. |

**Flow:**

1. The API validates the request, then inserts the `payment` and an `outbox`
   event in the **same transaction**, and returns `202 Accepted`.
2. The outbox relay in the API process claims pending rows
   (`FOR UPDATE SKIP LOCKED`). Payment events are published to the RabbitMQ
   `payments` exchange and to the Kafka topic `payments.events`. The outbox row
   is marked published only after both publishes succeed. The Kafka record key
   and `message_id` header are the outbox id.
3. The RabbitMQ consumer reads `payments.new`, locks the payment row, emulates
   the gateway (2–5 s; 90% `succeeded` / 10% `failed`), persists the result,
   then sends the webhook and records `webhook_sent_at` or `webhook_last_error`.
4. If payment processing or webhook delivery fails, the event is routed through
   TTL retry queues (2 s, then 4 s) that dead-letter back to `payments.new`.
   A terminal status skips reprocessing, and `webhook_sent_at` skips resending.
   After 3 total attempts the event lands in `payments.dlq`.
5. The inbox worker reads `payments.events` with auto-commit disabled. For each
   record it opens one database transaction, inserts an `inbox` row keyed by
   `message_id`, and in the default `inline` mode runs the handler in that same
   transaction. The handler's example side effect is one `inbox_effects` row,
   also unique on `message_id`. The Kafka offset is committed only after the
   database transaction commits.
6. A crash before that database commit rolls the insert and the side effect
   back. The offset stays uncommitted, Kafka redelivers, and the handler runs
   again without a leftover effect. A crash after the database commit
   redelivers too, but the unique key finds the terminal row and skips the
   handler.
7. A retryable handler error commits the attempt count and does **not** commit
   the offset, then seeks back to that record so a later offset in the same
   partition cannot be acknowledged first. After `INBOX_MAX_ATTEMPTS` (default
   3) the row becomes `failed` and a poison outbox event is written in the same
   transaction. The relay publishes it to `payments.inbox.dlq`. A redelivery
   after that does not write a second poison event.
8. `INBOX_PROCESSING_MODE=worker` stores the inbox row and commits the offset
   immediately. A separate loop claims `received` rows with
   `SELECT … FOR UPDATE SKIP LOCKED`, runs the same handlers, and retries or
   dead-letters them. Run only one mode per consumer group.

## Tech Stack

FastAPI · gRPC · Pydantic v2 · SQLAlchemy 2.0 (async) · PostgreSQL · RabbitMQ
via FastStream · Kafka (KRaft) via aiokafka · Alembic · Docker Compose.

## Quick Start (Docker)

```bash
cp .env.example .env
API_KEY=dev-api-key docker compose up --build
```

The API container runs Alembic migrations before Uvicorn starts. The consumer
waits for the API healthcheck, so the two services never race on the migration
table.

| Service                | URL / Address                               |
| ---------------------- | ------------------------------------------- |
| API                    | http://localhost:8000                       |
| gRPC                   | localhost:50051                             |
| RabbitMQ management UI | http://localhost:15672 (`guest` / `guest`)  |
| Kafka (KRaft)          | `localhost:9092` (inside Compose: `kafka:19092`) |
| PostgreSQL             | `localhost:5433` (override `POSTGRES_PORT`) |

Kafka topics `payments.events` and `payments.inbox.dlq` are created on startup.
The `inbox` service is the Kafka consumer. The `consumer` service remains the
RabbitMQ payment processor.

## API Reference

All endpoints require the `X-API-Key` header.

| Method | Path                    | Required headers               | Success        |
| ------ | ----------------------- | ------------------------------ | -------------- |
| `POST` | `/api/v1/payments`      | `X-API-Key`, `Idempotency-Key` | `202 Accepted` |
| `GET`  | `/api/v1/payments/{id}` | `X-API-Key`                    | `200 OK`       |

The generated OpenAPI schema is committed at `openapi.json`. Regenerate it
after API/schema changes with:

```bash
PYTHONPATH=. python -c "import json; from pathlib import Path; from app.main import app; Path('openapi.json').write_text(json.dumps(app.openapi(), indent=2, sort_keys=True) + '\n')"
```

**Request body** (`POST /api/v1/payments`):

| Field         | Type           | Notes                                |
| ------------- | -------------- | ------------------------------------ |
| `amount`      | decimal string | `> 0`, up to 2 decimal places        |
| `currency`    | string         | `RUB`, `USD`, or `EUR`               |
| `description` | string         | 1–1000 chars                         |
| `metadata`    | object         | optional, arbitrary JSON             |
| `webhook_url` | URL            | called with the final payment status |

The `Idempotency-Key` header is required, trimmed of surrounding whitespace, and
limited to 255 characters. Replaying the same key returns the original payment
instead of creating a duplicate.

### gRPC

The gRPC service exposes the same create-payment workflow as
`POST /api/v1/payments`:

| Service method                         | Metadata                       | Success |
| -------------------------------------- | ------------------------------ | ------- |
| `payments.v1.Payments/CreatePayment`   | `x-api-key`, `idempotency-key` | accepted response |

The protobuf contract lives at `app/grpc/payment.proto`. Request fields mirror
the HTTP POST body: `amount`, `currency`, `description`, `metadata`, and
`webhook_url`. The response contains `payment_id`, `status`, and `created_at`.
Conflicting idempotency keys are returned as gRPC `ALREADY_EXISTS`.

## Usage Examples

Optionally run a local webhook receiver in another terminal to inspect callbacks:

```bash
python -m http.server 9000
```

Create a payment:

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-api-key" \
  -H "Idempotency-Key: order-1001" \
  -d '{
    "amount": "1500.00",
    "currency": "RUB",
    "description": "Order 1001",
    "metadata": {"order_id": "1001"},
    "webhook_url": "http://host.docker.internal:9000/webhook"
  }'
```

Response — `202 Accepted`:

```json
{
  "payment_id": "2bb1c9a9-04c9-48f9-99f5-3d6a7f2dcd29",
  "status": "pending",
  "created_at": "2026-06-10T12:00:00Z"
}
```

Fetch payment details:

```bash
curl -s http://localhost:8000/api/v1/payments/<payment_id> \
  -H "X-API-Key: dev-api-key"
```

Repeating the `POST` with the same `Idempotency-Key` returns the original payment
rather than creating a new one.

Create a payment through gRPC with `grpcurl`:

```bash
grpcurl -plaintext \
  -import-path . \
  -proto app/grpc/payment.proto \
  -H "x-api-key: dev-api-key" \
  -H "idempotency-key: order-1002" \
  -d '{
    "amount": "1500.00",
    "currency": "RUB",
    "description": "Order 1002",
    "metadata": {"order_id": "1002"},
    "webhook_url": "http://host.docker.internal:9000/webhook"
  }' \
  localhost:50051 payments.v1.Payments/CreatePayment
```

## Local Development

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements-dev.txt
pre-commit install
alembic upgrade head
uvicorn app.main:app --reload
```

Run the RabbitMQ consumer and the Kafka inbox worker in separate terminals:

```bash
faststream run app.worker:app --reload
python -m app.inbox_worker
```

Run the gRPC server in another terminal:

```bash
python -m app.grpc.server
```

Regenerate gRPC stubs after editing `app/grpc/payment.proto`:

```bash
make grpc-generate
```

Run the tests:

```bash
pytest
```

Run lint and the local check bundle:

```bash
ruff check .
pre-commit run --all-files
make check
```

With the Docker stack running, run the end-to-end suite (creates a real payment,
verifies idempotent replay, the 409 conflict on body mismatch, processing, and
webhook delivery to a local receiver):

```bash
E2E=1 pytest tests/test_e2e.py
```

Run the broker outage recovery test against the Docker stack:

```bash
E2E_BROKER_OUTAGE=1 pytest tests/test_broker_outage.py
```

## Configuration

All settings are read from the environment; see `.env.example` for the full list
and defaults. The most relevant:

| Variable                         | Description                                    |
| -------------------------------- | ---------------------------------------------- |
| `API_KEY`                        | Static key required in `X-API-Key`             |
| `DATABASE_URL`                   | Async PostgreSQL DSN (`postgresql+asyncpg://`) |
| `RABBITMQ_URL`                   | RabbitMQ connection URL                        |
| `GRPC_PORT`                      | gRPC server listen port (default `50051`)      |
| `PAYMENT_PROCESSING_MIN_SECONDS` | Lower bound of the emulated gateway delay      |
| `PAYMENT_PROCESSING_MAX_SECONDS` | Upper bound of the emulated gateway delay      |
| `PAYMENT_SUCCESS_RATE`           | Probability of `succeeded` (default `0.9`)     |
| `LOG_LEVEL` / `LOG_JSON`         | Log level and JSON structured logs             |
| `KAFKA_ENABLED`                  | Publish outbox events to Kafka (default true) |
| `KAFKA_BOOTSTRAP_SERVERS`        | Kafka bootstrap servers                        |
| `KAFKA_PAYMENTS_TOPIC`           | Topic for payment events (default `payments.events`) |
| `KAFKA_INBOX_DLQ_TOPIC`          | Poison-message topic (default `payments.inbox.dlq`) |
| `KAFKA_INBOX_GROUP_ID`           | Inbox consumer group                           |
| `INBOX_PROCESSING_MODE`          | `inline` (default) or `worker`                 |
| `INBOX_MAX_ATTEMPTS`             | Handler attempts before the inbox DLQ          |
| `INBOX_RETRY_BACKOFF_SECONDS`    | Pause before redelivering a retryable record   |

## Delivery Guarantees

- **Transactional outbox.** The payment and its outbox event are committed in one
  transaction. If RabbitMQ is unavailable, the outbox row stays `pending` and the
  publisher keeps retrying — no event is lost.
- **Idempotent intake.** A unique constraint on `idempotency_key`, plus an
  `IntegrityError` fallback, ensures concurrent duplicate requests resolve to the
  same payment.
- **Idempotent processing.** The payment consumer locks the payment row
  (`FOR UPDATE`) and skips any payment already in a terminal state, so duplicate
  deliveries never reprocess the gateway result.
- **Idempotent webhook delivery.** The webhook is sent under the same row lock
  with a `webhook_sent_at` check-and-set, so duplicate deliveries and retries
  never resend a webhook that already went out.
- **Retries.** Any failure — processing or webhook delivery — routes the event
  through TTL retry queues with exponential delays of 2 s and 4 s; completed
  steps are skipped on redelivery.
- **Dead-letter queue.** After 3 failed attempts the event goes to
  `payments.dlq` for manual inspection.
- **Transactional inbox.** The inbox insert and the handler side effect commit
  in one database transaction. The Kafka offset is committed only after that.
  `inbox.message_id` is unique, so a redelivery cannot apply the effect twice.
- **Inbox retries and DLQ.** A failed handler keeps the offset uncommitted until
  the attempt budget is exhausted. The failed row and one poison outbox event
  commit together; the relay then publishes `payments.inbox.dlq`.
- **Structured logs.** Process entrypoints emit one JSON object per line
  (`LOG_JSON=true`) with fields such as `event`, `message_id`, `topic`,
  `partition`, and `offset`. Set `LOG_JSON=false` for plain text.

Inbox integration tests use Postgres and Kafka containers and are skipped when
Docker is not available:

```bash
pytest tests/test_inbox_integration.py
```

Set `INBOX_INTEGRATION=0` to skip them even when Docker is installed. The
RabbitMQ end-to-end suites above stay opt-in as well.
