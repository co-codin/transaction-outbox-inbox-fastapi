import json
import logging

from app.core.logging import JsonFormatter, log_event


def test_json_formatter_includes_structured_fields() -> None:
    record = logging.LogRecord(
        name="app.messaging.inbox",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="inbox_offset_committed",
        args=(),
        exc_info=None,
    )
    record.event = "inbox_offset_committed"
    record.message_id = "msg-1"
    record.offset = 4

    payload = json.loads(JsonFormatter().format(record))

    assert payload["message"] == "inbox_offset_committed"
    assert payload["event"] == "inbox_offset_committed"
    assert payload["message_id"] == "msg-1"
    assert payload["offset"] == 4
    assert payload["level"] == "INFO"
    assert payload["logger"] == "app.messaging.inbox"


def test_log_event_passes_fields_through_the_formatter() -> None:
    logger = logging.getLogger("tests.inbox.logging")
    handler = logging.StreamHandler()
    formatter = JsonFormatter()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    try:
        with _capture_handler(handler) as lines:
            log_event(logger, logging.INFO, "inbox_duplicate_skipped", message_id="msg-9", attempts=2)
    finally:
        logger.removeHandler(handler)

    payload = json.loads(lines[0])
    assert payload["event"] == "inbox_duplicate_skipped"
    assert payload["message_id"] == "msg-9"
    assert payload["attempts"] == 2


class _capture_handler:
    def __init__(self, handler: logging.Handler) -> None:
        self._handler = handler
        self.lines: list[str] = []
        self._original = handler.emit

    def __enter__(self) -> list[str]:
        def emit(record: logging.LogRecord) -> None:
            self.lines.append(self._handler.format(record))

        self._handler.emit = emit
        return self.lines

    def __exit__(self, exc_type, exc, tb) -> None:
        self._handler.emit = self._original
