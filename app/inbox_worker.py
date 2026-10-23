import asyncio
import contextlib
import logging
import signal

from app.core.config import settings
from app.core.logging import configure_logging, log_event
from app.db.session import async_session_maker
from app.messaging.inbox import build_inbox_consumer, wait_for_stop
from app.messaging.kafka import KafkaOutboxPublisher
from app.services.inbox import HandlerRegistry, build_default_registry, process_available_batch

logger = logging.getLogger(__name__)


async def main() -> None:
    configure_logging(level=settings.log_level, json_logs=settings.log_json)
    if not settings.kafka_enabled:
        raise SystemExit("KAFKA_ENABLED must be true to run the inbox worker")

    stop = asyncio.Event()
    _install_signal_handlers(stop)
    publisher = KafkaOutboxPublisher.from_settings(settings)
    await publisher.start()
    try:
        await publisher.ensure_topics()
    except Exception:
        logger.exception("kafka_topic_setup_failed")

    registry = build_default_registry()
    consumer = build_inbox_consumer(
        settings,
        session_factory=async_session_maker,
        registry=registry,
    )
    tasks = [asyncio.create_task(consumer.run(stop), name="inbox-consumer")]
    if settings.inbox_processing_mode == "worker":
        tasks.append(
            asyncio.create_task(_process_loop(stop, registry), name="inbox-processor"),
        )
    log_event(
        logger,
        logging.INFO,
        "inbox_worker_started",
        mode=settings.inbox_processing_mode,
        group_id=settings.kafka_inbox_group_id,
        topic=settings.kafka_payments_topic,
    )
    try:
        await _supervise(stop, tasks)
    finally:
        stop.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await publisher.stop()
        log_event(logger, logging.INFO, "inbox_worker_stopped")


async def _process_loop(stop: asyncio.Event, registry: HandlerRegistry) -> None:
    while not stop.is_set():
        results = []
        retryable = False
        try:
            async with async_session_maker() as session:
                async with session.begin():
                    results = await process_available_batch(
                        session,
                        registry=registry,
                        max_attempts=settings.inbox_max_attempts,
                        batch_size=settings.inbox_batch_size,
                    )
            retryable = any(result.retryable for result in results)
            if results:
                log_event(
                    logger,
                    logging.INFO,
                    "inbox_batch_processed",
                    count=len(results),
                    retryable=retryable,
                )
        except Exception:
            logger.exception("inbox_worker_iteration_failed")
            retryable = True
        if retryable:
            delay = max(settings.inbox_poll_interval_seconds, settings.inbox_retry_backoff_seconds)
        elif results:
            delay = 0
        else:
            delay = settings.inbox_poll_interval_seconds
        await wait_for_stop(stop, delay)


async def _supervise(stop: asyncio.Event, tasks: list[asyncio.Task[None]]) -> None:
    stop_task = asyncio.create_task(stop.wait())
    try:
        done, _pending = await asyncio.wait(
            {*tasks, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_task not in done:
            stop.set()
            failed = [task for task in done if task in tasks and task.exception() is not None]
            if failed:
                raise failed[0].exception()
        await asyncio.wait(tasks, timeout=30)
    finally:
        stop_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await stop_task


def _install_signal_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_args: stop.set())


if __name__ == "__main__":
    asyncio.run(main())
