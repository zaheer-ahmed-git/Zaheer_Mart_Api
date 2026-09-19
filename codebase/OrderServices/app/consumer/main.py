import logging
from typing import Optional
from uuid import uuid4

from sqlmodel import Session

from app import crud, db, kafka, models, order_pb2, settings
from app.producer import producer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def inventory_check(
    product_id: str, quantity: int, option
) -> Optional[order_pb2.Order]:
    """Ask the inventory service and wait for the answer of THIS request."""
    correlation_id = uuid4().hex
    request = order_pb2.Order(
        product_id=product_id,
        quantity=quantity,
        option=option,
        correlation_id=correlation_id,
    )
    return await kafka.request_reply(
        settings.KAFKA_TOPIC_INVENTORY_CHECK_REQUEST,
        request,
        settings.KAFKA_TOPIC_INVENTORY_CHECK_RESPONSE,
        order_pb2.Order,
        correlation_id,
    )


async def handle_create_order(new_msg: order_pb2.Order):
    if new_msg.quantity <= 0:
        logger.error(f"Invalid quantity: {new_msg.quantity}")
        return
    with Session(db.engine) as session:
        if await crud.get_order(session, new_msg.order_id):
            logger.info("Order already exists, message ignored")
            return
        check = await inventory_check(
            new_msg.product_id, new_msg.quantity, order_pb2.SelectOption.CREATE
        )
        accepted = bool(
            check and check.is_product_available and check.is_stock_available
        )
        order = await crud.create_order(
            session,
            order_id=new_msg.order_id,
            user_id=new_msg.user_id,
            product_id=new_msg.product_id,
            quantity=new_msg.quantity,
            shipping_address=new_msg.shipping_address,
            customer_notes=new_msg.customer_notes,
            order_status=models.ORDER_IN_PROGRESS if accepted else models.ORDER_REJECTED,
            payment_status=models.PAYMENT_PENDING
            if accepted
            else models.PAYMENT_NOT_REQUIRED,
        )
        if not accepted:
            logger.warning(f"Order {order.order_id} rejected: no product or stock")
            return
        await producer.publish_order_event(
            order, order_pb2.SelectOption.CREATE, new_msg.username, new_msg.email
        )


async def handle_update_order(new_msg: order_pb2.Order):
    with Session(db.engine) as session:
        order = await crud.get_order(session, new_msg.order_id)
        if order is None or order.user_id != new_msg.user_id:
            logger.error("Order not found for this user.")
            return
        if order.payment_status == models.PAYMENT_DONE or order.order_status in (
            models.ORDER_REJECTED,
        ):
            logger.info("Order can not be updated in its current state")
            return
        changes: dict = {}
        if new_msg.quantity and new_msg.quantity != order.quantity:
            difference = new_msg.quantity - order.quantity
            check = await inventory_check(
                order.product_id, difference, order_pb2.SelectOption.UPDATE
            )
            if check and check.is_stock_available:
                changes["quantity"] = new_msg.quantity
            else:
                logger.warning("Insufficient stock, quantity not changed")
        if new_msg.shipping_address:
            changes["shipping_address"] = new_msg.shipping_address
        if new_msg.customer_notes:
            changes["customer_notes"] = new_msg.customer_notes
        if changes:
            await crud.update_order(session, order.order_id, changes)


async def handle_delete_order(new_msg: order_pb2.Order):
    with Session(db.engine) as session:
        order = await crud.get_order(session, new_msg.order_id)
        if order is None or order.user_id != new_msg.user_id:
            logger.info("Order not found for this user")
            return
        if order.payment_status == models.PAYMENT_DONE:
            logger.info("Order cannot be deleted as payment is done")
            return
        if order.order_status != models.ORDER_REJECTED:
            # give the reserved stock back
            await inventory_check(
                order.product_id, order.quantity, order_pb2.SelectOption.DELETE
            )
        deleted = await crud.delete_order(session, order.order_id)
        if deleted:
            await producer.publish_order_event(deleted, order_pb2.SelectOption.DELETE)


async def process_message(order: order_pb2.Order):
    try:
        if order.option == order_pb2.SelectOption.CREATE:
            await handle_create_order(order)
        elif order.option == order_pb2.SelectOption.UPDATE:
            await handle_update_order(order)
        elif order.option == order_pb2.SelectOption.DELETE:
            await handle_delete_order(order)
        else:
            logger.warning(f"Unknown option: {order.option}")
    except Exception:
        logger.exception("Error processing order message")


async def start_consuming():
    try:
        async for message in kafka.consume_messages_order(
            settings.KAFKA_TOPIC_ORDER, settings.KAFKA_CONSUMER_GROUP_ID_FOR_ORDER
        ):
            await process_message(message)
    except Exception:
        logger.exception("Error in order consumer")


async def handle_order_payment(session: Session, new_msg: order_pb2.Payment):
    order = await crud.get_order(session, new_msg.order_id)
    if order is None or order.user_id != new_msg.user_id:
        logger.warning(f"Order not found for order_id: {new_msg.order_id}")
        return
    if order.payment_status == models.PAYMENT_DONE:
        logger.info("Payment already registered for this order")
        return
    await inventory_check(
        order.product_id, order.quantity, order_pb2.SelectOption.PAYMENT_DONE
    )
    await crud.update_order(
        session,
        order.order_id,
        {
            "order_status": models.ORDER_COMPLETED,
            "payment_status": models.PAYMENT_DONE,
        },
    )


async def process_message_payment(msg: order_pb2.Payment):
    if msg.payment_status == order_pb2.PaymentStatus.PAID:
        with Session(db.engine) as session:
            await handle_order_payment(session, msg)


async def start_consuming_payment():
    try:
        async for message in kafka.consume_messages_payment(
            settings.KAFKA_TOPIC_PAYMENT_DONE_FROM_PAYMENT,
            settings.KAFKA_CONSUMER_GROUP_ID_FOR_RESPONSE_FROM_PAYMENT,
        ):
            try:
                await process_message_payment(message)
            except Exception:
                logger.exception("Error processing payment message")
    except Exception:
        logger.exception("Error in payment consumer")
