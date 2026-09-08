import logging

from sqlmodel import Session

from app import crud, db, kafka, product_pb2, settings
from app.models import ProductBase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def handle_create_product(product: product_pb2.Product):
    with Session(db.engine) as session:
        if await crud.get_product(session, product.product_id):
            logger.info("Product already exists, message ignored")
            return None
        data = ProductBase(
            product_id=product.product_id,
            name=product.name,
            description=product.description,
            price=product.price,
            is_available=product.is_available if product.HasField("is_available") else True,
            category=product.category or None,
        )
        return await crud.create_product_crud(session, data)


def _changes_from_message(product: product_pb2.Product) -> dict:
    changes: dict = {}
    if product.name:
        changes["name"] = product.name
    if product.description:
        changes["description"] = product.description
    if product.HasField("price"):
        changes["price"] = product.price
    if product.HasField("is_available"):
        changes["is_available"] = product.is_available
    if product.category:
        changes["category"] = product.category
    return changes


async def handle_update_product(product: product_pb2.Product):
    changes = _changes_from_message(product)
    if not changes:
        return None
    with Session(db.engine) as session:
        return await crud.update_product(session, product.product_id, changes)


async def handle_delete_product(product_id: str):
    with Session(db.engine) as session:
        return await crud.delete_product(session, product_id)


async def process_message(product: product_pb2.Product):
    try:
        if product.option == product_pb2.SelectOption.CREATE:
            await handle_create_product(product)
        elif product.option == product_pb2.SelectOption.UPDATE:
            await handle_update_product(product)
        elif product.option == product_pb2.SelectOption.DELETE:
            await handle_delete_product(product.product_id)
        else:
            logger.warning(f"Unknown option: {product.option}")
    except Exception:
        logger.exception("Error processing product message")


async def process_message_stock(new_msg: product_pb2.Inventory):
    try:
        with Session(db.engine) as session:
            product = await crud.get_product(session, new_msg.product_id)
            if product is not None:
                product.is_available = new_msg.stock_level > 0
                session.add(product)
                session.commit()
    except Exception:
        logger.exception("Error processing stock message")


async def start_consuming():
    try:
        async for message in kafka.consume_messages(
            settings.KAFKA_TOPIC_PRODUCT, settings.KAFKA_CONSUMER_GROUP_ID_FOR_PRODUCT
        ):
            await process_message(message)
    except Exception:
        logger.exception("Error in consumer")


async def start_consuming_stock():
    try:
        async for message in kafka.consume_messages_stock(
            settings.KAFKA_TOPIC_STOCK_LEVEL_CHECK,
            settings.KAFKA_CONSUMER_GROUP_ID_FOR_STOCK_LEVEL_CHECK,
        ):
            await process_message_stock(message)
    except Exception:
        logger.exception("Error in stock consumer")
