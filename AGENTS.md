# Repository Guidance

This repository implements an async payment processor with both sides of
reliable messaging: a transactional outbox (PostgreSQL, RabbitMQ, and Kafka)
and a transactional inbox (Kafka). The service still fulfills the original
payment task in `task.pdf`: FastAPI intake, PostgreSQL persistence, outbox
publication, RabbitMQ/FastStream processing, webhook delivery, retries, and
DLQ routing. The repository was renamed from `transaction-outbox-fastapi` to
`transaction-outbox-inbox-fastapi`.

## Architecture

- Keep API code under `app/api`, business operations under `app/services`,
  broker and outbox/inbox runtime code under `app/messaging`, database models
  under `app/models`, and Pydantic schemas under `app/schemas`.
- `POST /api/v1/payments` must create the `payments` row and corresponding
  `outbox` row in one database transaction.
- The outbox relay is owned by the API lifespan in `app/main.py`. It publishes
  payment events to RabbitMQ and, when Kafka is enabled, to `payments.events`.
  The RabbitMQ consumer entrypoint stays `app/worker.py`. The Kafka inbox
  entrypoint is `python -m app.inbox_worker`.
- The RabbitMQ consumer must remain one handler for `payments.new`: process the
  payment, persist the terminal status, send the webhook, and route failed
  attempts to retry queues or `payments.dlq`.
- The inbox consumer commits a Kafka offset only after the database transaction
  commits. `inbox.message_id` is the idempotency key. Handler side effects for
  `payments.new` go to `inbox_effects` in the same transaction.
- Inline mode runs the handler inside the consume transaction. Worker mode
  stores the row, commits the offset, and a separate loop claims `received`
  rows with `SELECT … FOR UPDATE SKIP LOCKED`. Do not run both modes in one
  consumer group.
- Poison inbox messages become `failed` and enqueue one `inbox.message.failed`
  outbox row. The relay publishes that row to `payments.inbox.dlq`.
- Preserve idempotency at the existing boundaries too: `Idempotency-Key` for
  payment intake and `webhook_sent_at` for webhook delivery.

## Code Rules

- Use async SQLAlchemy 2.0 patterns with `AsyncSession`.
- Keep Pydantic v2 models in schema modules; avoid returning ORM models directly
  from API routes.
- Do not add dependencies unless the task clearly requires one. `aiokafka` is
  the Kafka client. `testcontainers` is a test dependency.
- When model fields change, update the Alembic migration and tests in the same
  pass.
- Keep RabbitMQ queue names, routing keys, and Kafka topic names centralized in
  `app/messaging/topology.py`.
- Do not expose internal delivery bookkeeping fields such as
  `webhook_sent_at`, `webhook_last_error`, inbox rows, or outbox internals
  through the public payment API unless explicitly requested.

## Verification

Use the smallest check that proves the change, then broaden when shared behavior
is touched.

- API/schema changes: `PYTHONPATH=. pytest -q tests/test_api_payments.py`
- Processor/retry/webhook changes:
  `PYTHONPATH=. pytest -q tests/test_processor_processing.py tests/test_processor_retry.py tests/test_webhooks.py`
- Outbox/topology changes:
  `PYTHONPATH=. pytest -q tests/test_outbox.py tests/test_topology.py`
- Inbox unit coverage:
  `PYTHONPATH=. pytest -q tests/test_inbox_service.py tests/test_inbox_consumer.py tests/test_logging.py`
- Inbox integration (Docker): `PYTHONPATH=. pytest -q tests/test_inbox_integration.py`
- General verification: `PYTHONPATH=. pytest -q`
- Lint: `ruff check .`
- Docker config check: `docker compose config >/dev/null`

The local shell may not expose `python`; use `python3` for direct Python
commands. If plain `pytest` cannot import `app`, rerun with `PYTHONPATH=.`
before treating it as a code failure.

## Delivery Notes

- Keep diffs small and behavior-preserving unless the user asks for a feature.
- Report skipped end-to-end tests explicitly. `tests/test_e2e.py` and
  `tests/test_broker_outage.py` require opt-in environment variables and a
  running Docker stack. `tests/test_inbox_integration.py` skips itself when
  Docker is unavailable; set `INBOX_INTEGRATION=0` to force that skip.
- With Kafka enabled, a payment outbox row stays pending until both RabbitMQ
  and Kafka accept the publish. RabbitMQ can observe a duplicate if Kafka fails
  after the RabbitMQ publish; payment processing and the inbox both tolerate
  that duplicate.
- Leave unrelated untracked or user-owned files alone.
