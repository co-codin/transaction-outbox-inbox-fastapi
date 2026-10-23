---
name: payment-fastapi
description: Work on the transaction-outbox-inbox-fastapi payment processor (renamed from transaction-outbox-fastapi). Use when Codex needs to inspect, change, review, test, or document this FastAPI + SQLAlchemy + PostgreSQL + RabbitMQ/FastStream + Kafka inbox service, especially around payment API behavior, transactional outbox publishing, the Kafka inbox, idempotency, webhook delivery, retry queues, DLQ handling, Alembic migrations, Docker Compose, or task.pdf requirement alignment.
---

# Payment FastAPI

## Overview

Use this skill to keep changes aligned with the service contract: accept
payments through FastAPI, persist payment plus outbox atomically, publish
`payments.new` to RabbitMQ and Kafka, process the RabbitMQ copy once in
FastStream, send a webhook once, and retry or dead-letter failures. The Kafka
copy is consumed by the transactional inbox, which commits offsets only after
the database transaction and applies `inbox_effects` once per `message_id`.

## Repo Map

- `app/api/routes.py`: payment endpoints and request header validation.
- `app/api/deps.py`: static `X-API-Key` dependency.
- `app/services/payments.py`: create/get payment logic and idempotency conflict
  behavior.
- `app/services/processor.py`: consumer workflow, gateway simulation,
  webhook-once guard, retry and DLQ publishing.
- `app/services/webhooks.py`: webhook payload construction and HTTP POST.
- `app/messaging/topology.py`: exchanges, queues, retry TTLs, routing keys, and
  Kafka topic names.
- `app/messaging/outbox.py`: transactional outbox relay (RabbitMQ and Kafka).
- `app/messaging/kafka.py`: idempotent Kafka producer and topic setup.
- `app/messaging/inbox.py`: Kafka inbox consumer. Offset commits happen after
  the database commit.
- `app/inbox_worker.py`: inbox process entrypoint.
- `app/services/inbox.py`: dedup, handler registry, retries, and poison outbox
  rows.
- `app/models/inbox.py`: `inbox` and `inbox_effects`.
- `app/models`: SQLAlchemy ORM models.
- `app/schemas`: Pydantic v2 API and message models.
- `alembic/versions`: database schema migrations.
- `tests`: unit and opt-in end-to-end coverage.

## Change Workflow

1. Read the relevant code path before editing. For requirement questions,
   compare against `task.pdf` first.
2. Preserve the core delivery guarantees unless the user explicitly changes
   them: outbox atomicity, intake idempotency, terminal-status idempotency,
   webhook-once delivery, retry queues, and DLQ.
3. Keep public API schemas separate from operational bookkeeping. Do not expose
   `webhook_sent_at`, `webhook_last_error`, or outbox columns by accident.
4. If a model column changes, update the ORM model, Alembic migration, schemas
   if applicable, and tests together.
5. Prefer deleting unused code over adding abstractions. Keep queue names,
   routing keys, and Kafka topic names in `app/messaging/topology.py`.

## Behavior Contracts

- `POST /api/v1/payments` requires `X-API-Key` and `Idempotency-Key`, returns
  `202`, and creates an outbox event in the same transaction.
- Reusing the same idempotency key with the same body returns the original
  payment; reusing it with a different body returns `409`.
- The consumer locks the payment row, skips terminal statuses, simulates 2-5
  seconds of processing, and uses the configured success rate.
- `webhook_sent_at` is the duplicate-send guard. `webhook_last_error` records
  the last webhook failure and is cleared on success.
- Retry behavior is three total attempts: retry queue 1, retry queue 2, then
  `payments.dlq`.
- Inbox deliveries use `inbox.message_id` as the unique idempotency key. The
  default handler writes one `inbox_effects` row in the same transaction. The
  Kafka offset is committed only after that transaction commits. Exhausted
  attempts write `inbox.message.failed` to the outbox, and the relay publishes
  it to `payments.inbox.dlq`.

## Verification Commands

Use targeted tests first:

```bash
PYTHONPATH=. pytest -q tests/test_api_payments.py
PYTHONPATH=. pytest -q tests/test_outbox.py tests/test_topology.py
PYTHONPATH=. pytest -q tests/test_inbox_service.py tests/test_inbox_consumer.py tests/test_logging.py
PYTHONPATH=. pytest -q tests/test_processor_processing.py tests/test_processor_retry.py tests/test_webhooks.py
```

Use broader checks before finalizing standard code changes:

```bash
PYTHONPATH=. pytest -q
ruff check .
docker compose config >/dev/null
```

Use `python3` for direct Python commands if `python` is unavailable. If plain
`pytest` cannot import `app`, retry with `PYTHONPATH=.` before diagnosing the
application.

## End-to-End Checks

Run these only when the Docker stack is available and the task needs real broker
or webhook proof:

```bash
E2E=1 pytest -q tests/test_e2e.py
E2E_BROKER_OUTAGE=1 pytest -q tests/test_broker_outage.py
pytest -q tests/test_inbox_integration.py
```

`tests/test_inbox_integration.py` starts its own Postgres and Kafka containers
and skips when Docker is unavailable. Report skipped E2E coverage clearly when
not run.
