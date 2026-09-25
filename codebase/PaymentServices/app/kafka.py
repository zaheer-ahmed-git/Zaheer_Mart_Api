import asyncio
import logging
from typing import Optional, Type, TypeVar

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # type: ignore
from aiokafka.admin import AIOKafkaAdminClient, NewTopic  # type: ignore

from app import payment_pb2, settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

T = TypeVar("T")


async def retry_async(func, retries=5, delay=2):
    for attempt in range(retries):
        try:
            return await func()
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(delay)
            else:
                raise


async def create_topic():
    admin_client = AIOKafkaAdminClient(bootstrap_servers=f"{settings.BOOTSTRAP_SERVER}")
    await retry_async(admin_client.start)
    names = {
        settings.KAFKA_TOPIC_PAYMENT,
        settings.KAFKA_TOPIC_GET_FROM_ORDER,
        settings.KAFKA_TOPIC_REQUEST_TO_USER,
        settings.KAFKA_TOPIC_RESPONSE_FROM_USER_TO_PAYMENT,
    }
    topic_list = [
        NewTopic(name=name.strip(), num_partitions=2, replication_factor=1)
        for name in names
    ]
    try:
        await admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except Exception as e:
        # TopicAlreadyExists is expected when another service created it first.
        logger.warning(f"Creating topics: {e}")
    finally:
        await admin_client.close()


async def send_kafka_message(topic: str, message: bytes):
    producer = AIOKafkaProducer(bootstrap_servers=settings.BOOTSTRAP_SERVER)
    await retry_async(producer.start)
    try:
        await producer.send_and_wait(topic, message)
    finally:
        await producer.stop()


async def request_reply(
    request_topic: str,
    request_message,
    reply_topic: str,
    reply_class: Type[T],
    correlation_id: str,
    timeout: Optional[float] = None,
) -> Optional[T]:
    """Send a request and wait for the reply with the same correlation id.
    The reply consumer has no group and starts at the end of the topic BEFORE the
    request is sent: concurrent requests never steal each other's answers.
    Returns None on timeout."""
    timeout = timeout or settings.REQUEST_TIMEOUT_SECONDS
    consumer = AIOKafkaConsumer(
        reply_topic,
        bootstrap_servers=settings.BOOTSTRAP_SERVER,
        group_id=None,
        auto_offset_reset="latest",
        enable_auto_commit=False,
    )
    await retry_async(consumer.start)
    try:
        await send_kafka_message(request_topic, request_message.SerializeToString())

        async def _wait():
            async for msg in consumer:
                reply = reply_class()  # type: ignore[call-arg]
                try:
                    reply.ParseFromString(msg.value)  # type: ignore[attr-defined]
                except Exception:
                    continue
                if reply.correlation_id == correlation_id:  # type: ignore[attr-defined]
                    return reply

        try:
            return await asyncio.wait_for(_wait(), timeout)
        except asyncio.TimeoutError:
            logger.error(f"No reply on {reply_topic} within {timeout}s")
            return None
    finally:
        await consumer.stop()


async def consume_messages_order(topic: str, group_id: str):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.BOOTSTRAP_SERVER,
        group_id=group_id,
        auto_offset_reset="earliest",
    )
    await retry_async(consumer.start)
    try:
        async for msg in consumer:
            try:
                new_msg = payment_pb2.Order()
                new_msg.ParseFromString(msg.value)
                yield new_msg
            except Exception:
                logger.exception("Could not parse order message")
    finally:
        await consumer.stop()
