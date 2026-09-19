"""Talks to the user service through Kafka (request / reply)."""
from uuid import uuid4

from fastapi import HTTPException

from app import kafka, order_pb2, settings


async def _ask_user_service(**fields) -> order_pb2.User:
    correlation_id = uuid4().hex
    request = order_pb2.User(
        service=order_pb2.SelectService.ORDER,
        correlation_id=correlation_id,
        **fields,
    )
    reply = await kafka.request_reply(
        settings.KAFKA_TOPIC_REQUEST_TO_USER,
        request,
        settings.KAFKA_TOPIC_RESPONSE_FROM_USER_TO_ORDER,
        order_pb2.User,
        correlation_id,
    )
    if reply is None:
        raise HTTPException(status_code=504, detail="User service did not answer")
    if reply.error_message or reply.http_status_code:
        raise HTTPException(
            status_code=reply.http_status_code or 400, detail=reply.error_message
        )
    return reply


async def login(username: str, password: str) -> order_pb2.User:
    return await _ask_user_service(
        username=username, password=password, option=order_pb2.SelectOption.LOGIN
    )


async def get_user(username: str) -> order_pb2.User:
    return await _ask_user_service(
        username=username, option=order_pb2.SelectOption.CURRENT_USER
    )


async def refresh(email: str) -> order_pb2.User:
    return await _ask_user_service(
        email=email, option=order_pb2.SelectOption.REFRESH_TOKEN
    )
