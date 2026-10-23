import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

import grpc
from google.protobuf.json_format import MessageToDict
from pydantic import ValidationError

from app.core.config import settings
from app.core.logging import configure_logging
from app.db.session import async_session_maker
from app.grpc import payment_pb2, payment_pb2_grpc
from app.messaging.runtime import outbox_runtime
from app.schemas.payments import PaymentAccepted, PaymentCreate
from app.services.idempotency import (
    IdempotencyKeyValidationError,
    normalize_idempotency_key,
)
from app.services.payments import IdempotencyKeyConflictError, create_payment

logger = logging.getLogger(__name__)


def _metadata_value(context: grpc.aio.ServicerContext, name: str) -> str | None:
    target = name.lower()
    for key, value in context.invocation_metadata():
        if key.lower() == target and isinstance(value, str):
            return value
    return None


def _validation_detail(exc: ValidationError) -> str:
    messages = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error["loc"])
        messages.append(f"{location}: {error['msg']}")
    return "; ".join(messages)


def _timestamp(value: datetime) -> str:
    serialized = value.isoformat()
    if value.tzinfo is not None and value.utcoffset() == UTC.utcoffset(value):
        return serialized.replace("+00:00", "Z")
    return serialized


class Payments(payment_pb2_grpc.PaymentsServicer):
    def __init__(self, session_factory: Any = async_session_maker) -> None:
        self._session_factory = session_factory

    async def CreatePayment(
        self,
        request: payment_pb2.CreatePaymentRequest,
        context: grpc.aio.ServicerContext,
    ) -> payment_pb2.PaymentAcceptedResponse:
        if _metadata_value(context, "x-api-key") != settings.api_key:
            await context.abort(
                grpc.StatusCode.UNAUTHENTICATED,
                "Invalid or missing API key",
            )
            raise RuntimeError("unreachable")

        try:
            idempotency_key = normalize_idempotency_key(
                _metadata_value(context, "idempotency-key"),
            )
        except IdempotencyKeyValidationError as exc:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
            raise RuntimeError("unreachable")

        metadata = MessageToDict(request.metadata, preserving_proto_field_name=True)
        try:
            payload = PaymentCreate.model_validate(
                {
                    "amount": request.amount,
                    "currency": request.currency,
                    "description": request.description,
                    "metadata": metadata,
                    "webhook_url": request.webhook_url,
                },
            )
        except ValidationError as exc:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, _validation_detail(exc))
            raise RuntimeError("unreachable")

        try:
            async with self._session_factory() as session:
                payment = await create_payment(session, payload, idempotency_key)
        except IdempotencyKeyConflictError:
            await context.abort(
                grpc.StatusCode.ALREADY_EXISTS,
                "Idempotency-Key already used with a different request body",
            )
            raise RuntimeError("unreachable")

        accepted = PaymentAccepted.model_validate(payment)
        return payment_pb2.PaymentAcceptedResponse(
            payment_id=str(accepted.payment_id),
            status=accepted.status.value,
            created_at=_timestamp(accepted.created_at),
        )


async def serve() -> None:
    server = grpc.aio.server()
    payment_pb2_grpc.add_PaymentsServicer_to_server(Payments(), server)
    listen_addr = f"[::]:{settings.grpc_port}"
    server.add_insecure_port(listen_addr)

    async with outbox_runtime():
        await server.start()
        logger.info("gRPC payment server listening on %s", listen_addr)
        try:
            await server.wait_for_termination()
        finally:
            await server.stop(grace=5)


def main() -> None:
    configure_logging(level=settings.log_level, json_logs=settings.log_json)
    asyncio.run(serve())


if __name__ == "__main__":
    main()
