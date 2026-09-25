import logging

from app import kafka, payment_pb2, settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def publish_payment_result(
    order_id: str,
    user_id: str,
    username: str,
    email: str,
    status: int,
):
    """Result of a payment, for the order and notification services.
    It never contains card data."""
    payment_proto = payment_pb2.Payment(
        order_id=order_id,
        user_id=user_id,
        username=username,
        email=email,
        payment_status=status,
    )
    await kafka.send_kafka_message(
        settings.KAFKA_TOPIC_PAYMENT, payment_proto.SerializeToString()
    )
