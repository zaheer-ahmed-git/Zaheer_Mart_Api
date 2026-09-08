import logging

from app import kafka, models, product_pb2, settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def publish_product_created(product: models.ProductBase):
    message = kafka.product_to_proto(product)
    message.option = product_pb2.SelectOption.CREATE
    await kafka.send_kafka_message(
        settings.KAFKA_TOPIC_PRODUCT, message.SerializeToString()
    )
    logger.info(f"Product created message sent for product ID: {product.product_id}")


async def publish_product_updated(product: models.ProductUpdate, product_id: str):
    # Only the fields sent by the client are set: the others stay "unset" in the
    # message, so the consumer leaves them unchanged.
    message = product_pb2.Product(
        product_id=product_id, option=product_pb2.SelectOption.UPDATE
    )
    if product.name is not None:
        message.name = product.name
    if product.description is not None:
        message.description = product.description
    if product.price is not None:
        message.price = product.price
    if product.is_available is not None:
        message.is_available = product.is_available
    if product.category is not None:
        message.category = product.category
    await kafka.send_kafka_message(
        settings.KAFKA_TOPIC_PRODUCT, message.SerializeToString()
    )
    logger.info(f"Product updated message sent for product ID: {product_id}")


async def publish_product_deleted(product_id: str):
    message = product_pb2.Product(
        product_id=product_id, option=product_pb2.SelectOption.DELETE
    )
    await kafka.send_kafka_message(
        settings.KAFKA_TOPIC_PRODUCT, message.SerializeToString()
    )
    logger.info(f"Product deleted message sent for product ID: {product_id}")
