"""Talks to the user service through Kafka (request / reply)."""
from uuid import uuid4

from fastapi import HTTPException

from app import kafka, payment_pb2, settings


async def _ask_user_service(**fields) -> payment_pb2.User:
    correlation_id = uuid4().hex
    request = payment_pb2.User(
        service=payment_pb2.SelectService.PAYMENT,
        correlation_id=correlation_id,
        **fields,
    )
    reply = await kafka.request_reply(
        settings.KAFKA_TOPIC_REQUEST_TO_USER,
        request,
        settings.KAFKA_TOPIC_RESPONSE_FROM_USER_TO_PAYMENT,
        payment_pb2.User,
        correlation_id,
    )
    if reply is None:
        raise HTTPException(status_code=504, detail="User service did not answer")
    if reply.error_message or reply.http_status_code:
        raise HTTPException(
            status_code=reply.http_status_code or 400, detail=reply.error_message
        )
    return reply


async def login(username: str, password: str) -> payment_pb2.User:
    return await _ask_user_service(
        username=username, password=password, option=payment_pb2.SelectOption.LOGIN
    )


async def get_user(username: str) -> payment_pb2.User:
    return await _ask_user_service(
        username=username, option=payment_pb2.SelectOption.CURRENT_USER
    )


async def refresh(email: str) -> payment_pb2.User:
    return await _ask_user_service(
        email=email, option=payment_pb2.SelectOption.REFRESH_TOKEN
    )
