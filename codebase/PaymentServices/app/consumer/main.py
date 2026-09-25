import logging

from sqlmodel import Session

from app import crud, db, kafka, models, payment_pb2, settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def process_order_message(message: payment_pb2.Order):
    """Events published by the order service once an order is accepted / deleted."""
    try:
        if message.option == payment_pb2.SelectOption.CREATE:
            await handle_create_order(message)
        elif message.option == payment_pb2.SelectOption.DELETE:
            await handle_delete_order(message)
    except Exception:
        logger.exception("Error processing order event")


async def handle_create_order(message: payment_pb2.Order):
    with Session(db.engine) as session:
        if await crud.get_order(session, message.order_id):
            return
        session.add(
            models.Payment(order_id=message.order_id, user_id=message.user_id)
        )
        session.commit()


async def handle_delete_order(message: payment_pb2.Order):
    with Session(db.engine) as session:
        payment = await crud.get_order(session, message.order_id)
        if payment and payment.payment_status != models.PAYMENT_DONE:
            session.delete(payment)
            session.commit()


async def start_order_consumer():
    try:
        async for message in kafka.consume_messages_order(
            settings.KAFKA_TOPIC_GET_FROM_ORDER,
            settings.KAFKA_CONSUMER_GROUP_ID_FOR_ORDER,
        ):
            await process_order_message(message)
    except Exception:
        logger.exception("Error in order consumer")
