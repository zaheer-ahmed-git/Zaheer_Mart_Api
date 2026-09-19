import logging

from app import kafka, models, order_pb2, settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def publish_order_created(
    order_id: str, item: models.OrdersCreate, user_proto: order_pb2.User
):
    order_proto = order_pb2.Order(
        order_id=order_id,
        user_id=str(user_proto.user_id),
        username=user_proto.username,
        email=user_proto.email,
        product_id=item.product_id,
        quantity=item.quantity,
        shipping_address=item.shipping_address,
        customer_notes=item.customer_notes,
        option=order_pb2.SelectOption.CREATE,
    )
    await kafka.send_message_producer(
        settings.KAFKA_TOPIC_ORDER, order_proto.SerializeToString()
    )


async def publish_order_update(
    order_id: str, order: models.OrdersUpdate, user_id: str
):
    # Empty string / 0 mean "not changed" for the consumer.
    order_proto = order_pb2.Order(
        order_id=order_id,
        user_id=user_id,
        quantity=order.quantity or 0,
        shipping_address=order.shipping_address or "",
        customer_notes=order.customer_notes or "",
        option=order_pb2.SelectOption.UPDATE,
    )
    await kafka.send_message_producer(
        settings.KAFKA_TOPIC_ORDER, order_proto.SerializeToString()
    )
    logger.info(f"Order update message sent for order ID: {order_id}")


async def publish_order_deleted(order_id: str, user_id: str):
    order_proto = order_pb2.Order(
        order_id=order_id,
        user_id=user_id,
        option=order_pb2.SelectOption.DELETE,
    )
    await kafka.send_message_producer(
        settings.KAFKA_TOPIC_ORDER, order_proto.SerializeToString()
    )
    logger.info(f"Order delete message sent for order ID: {order_id}")


async def publish_order_event(order: models.Orders, option, username: str = "", email: str = ""):
    """Event for the payment and notification services (order accepted / deleted)."""
    event = order_pb2.Order(
        order_id=order.order_id,
        user_id=order.user_id,
        username=username,
        email=email,
        product_id=order.product_id,
        quantity=order.quantity,
        shipping_address=order.shipping_address,
        customer_notes=order.customer_notes,
        option=option,
    )
    await kafka.send_message_producer(
        settings.KAFKA_TOPIC_ORDER_EVENTS, event.SerializeToString()
    )
