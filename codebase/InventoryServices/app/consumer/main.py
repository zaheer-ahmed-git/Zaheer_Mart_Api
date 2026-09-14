import logging
from uuid import uuid4

from sqlmodel import Session

from app import crud, db, inventory_pb2, kafka, settings
from app.models import (
    InventoryItem,
    InventoryItemCreate,
    InventoryItemUpdate,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def handle_inventory_check_by_product(product_id: str):
    with Session(db.engine) as session:
        return await crud.get_inventory_item_by_product(session, product_id)


async def handle_inventory_check(inventory_id: str):
    with Session(db.engine) as session:
        return await crud.get_inventory_item(session, inventory_id)


async def handle_inventory_stock_level_update(product_id: str, quantity_change: int):
    with Session(db.engine) as session:
        return await crud.update_stock_level(session, product_id, quantity_change)


async def handle_inventory_update(inventory: InventoryItemUpdate, inventory_id: str):
    with Session(db.engine) as session:
        return await crud.update_inventory_item(session, inventory_id, inventory)


async def handle_inventory_create(inventory: InventoryItemCreate):
    with Session(db.engine) as session:
        new_item = await crud.create_inventory_item(session, inventory)
        logger.info(f"New inventory item created: {new_item}")
        return new_item


async def handle_inventory_reduce(product_id: str, quantity_change: int):
    with Session(db.engine) as session:
        return await crud.reduce_inventory_item(session, product_id, quantity_change)


async def handle_delete_inventory(inventory_id: str):
    with Session(db.engine) as session:
        return await crud.delete_inventory_item(session, inventory_id)


async def process_message(inventory: inventory_pb2.Inventory):
    """Commands sent by the HTTP API of this service."""
    try:
        option = inventory.option
        if option == inventory_pb2.SelectOption.CHECK:
            await handle_inventory_check(inventory.inventory_id)
        elif option == inventory_pb2.SelectOption.CREATE:
            await handle_inventory_create(
                InventoryItemCreate(
                    inventory_id=inventory.inventory_id or uuid4().hex,
                    product_id=inventory.product_id,
                    stock_level=inventory.stock_level,
                    reserved_stock=inventory.reserved_stock,
                    sold_stock=inventory.sold_stock,
                )
            )
        elif option == inventory_pb2.SelectOption.CHECK_BY_PRODUCT:
            await handle_inventory_check_by_product(inventory.product_id)
        elif option == inventory_pb2.SelectOption.UPDATE:
            # Only the fields that were really sent are changed. Note that a
            # protobuf int is 0 when unset, so 0 means "not changed" here.
            changes = {}
            if inventory.stock_level:
                changes["stock_level"] = inventory.stock_level
            if inventory.reserved_stock:
                changes["reserved_stock"] = inventory.reserved_stock
            if inventory.sold_stock:
                changes["sold_stock"] = inventory.sold_stock
            await handle_inventory_update(
                InventoryItemUpdate(**changes), inventory.inventory_id
            )
        elif option == inventory_pb2.SelectOption.ADD:
            await handle_inventory_stock_level_update(
                inventory.product_id, inventory.stock_level
            )
        elif option == inventory_pb2.SelectOption.REDUCE:
            await handle_inventory_reduce(inventory.product_id, inventory.stock_level)
        elif option == inventory_pb2.SelectOption.DELETE:
            await handle_delete_inventory(inventory.inventory_id)
        else:
            logger.warning(f"Unknown option: {option}")
    except Exception:
        logger.exception("Error processing inventory message")


async def _publish_stock_level(item: InventoryItem):
    proto = inventory_pb2.Inventory(
        product_id=item.product_id, stock_level=item.stock_level
    )
    await kafka.send_producer_message(
        settings.KAFKA_TOPIC_STOCK_LEVEL_CHECK, proto.SerializeToString()
    )


async def process_message_inventory_check(new_msg: inventory_pb2.Order):
    """Request / reply with the order service. The answer always carries the
    correlation id of the request."""
    is_product_available = False
    is_stock_available = False
    stock_changed_item = None
    try:
        with Session(db.engine) as session:
            # row lock: two orders for the same product cannot oversell the stock
            inventory = await crud.get_inventory_item_by_product(
                session, new_msg.product_id, for_update=True
            )
            quantity = new_msg.quantity
            option = new_msg.option

            if inventory is None:
                logger.warning(f"Inventory not found for product {new_msg.product_id}")
            elif option == inventory_pb2.SelectOption.PAYMENT_DONE:
                inventory.reserved_stock = max(inventory.reserved_stock - quantity, 0)
                inventory.sold_stock += quantity
                is_product_available = is_stock_available = True
            elif option == inventory_pb2.SelectOption.CREATE:
                if inventory.stock_level > 0:
                    is_product_available = True
                    if quantity > 0 and inventory.stock_level >= quantity:
                        is_stock_available = True
                        inventory.stock_level -= quantity
                        inventory.reserved_stock += quantity
            elif option == inventory_pb2.SelectOption.UPDATE:
                # `quantity` is the DIFFERENCE with the quantity already reserved
                # for this order (positive: reserve more, negative: release).
                is_product_available = True
                if quantity > 0:
                    if inventory.stock_level >= quantity:
                        is_stock_available = True
                        inventory.stock_level -= quantity
                        inventory.reserved_stock += quantity
                else:
                    is_stock_available = True
                    inventory.stock_level += -quantity
                    inventory.reserved_stock = max(
                        inventory.reserved_stock + quantity, 0
                    )
            elif option == inventory_pb2.SelectOption.DELETE:
                inventory.stock_level += quantity
                inventory.reserved_stock = max(inventory.reserved_stock - quantity, 0)
                is_product_available = is_stock_available = True
            else:
                logger.warning(f"Unknown option received: {option}")

            if inventory is not None and (is_stock_available):
                session.add(inventory)
                session.commit()
                session.refresh(inventory)
                stock_changed_item = inventory
            else:
                session.rollback()
    except Exception:
        logger.exception("Error processing inventory check")

    answer = inventory_pb2.Order(
        is_stock_available=is_stock_available,
        is_product_available=is_product_available,
        correlation_id=new_msg.correlation_id,
        product_id=new_msg.product_id,
    )
    await kafka.send_producer_message(
        settings.KAFKA_TOPIC_INVENTORY_CHECK_RESPONSE, answer.SerializeToString()
    )
    if stock_changed_item is not None:
        try:
            await _publish_stock_level(stock_changed_item)
        except Exception:
            logger.exception("Could not publish the new stock level")


async def process_message_product(product: inventory_pb2.Product):
    """Events from the product service."""
    try:
        with Session(db.engine) as session:
            if product.option == inventory_pb2.SelectOption.CREATE:
                if await crud.get_inventory_item_by_product(session, product.product_id):
                    logger.info("Inventory already exists for this product")
                    return
                await crud.create_inventory_item(
                    session,
                    InventoryItemCreate(
                        inventory_id=uuid4().hex,
                        product_id=product.product_id,
                        stock_level=0,
                        reserved_stock=0,
                        sold_stock=0,
                    ),
                )
                logger.info(f"Created inventory for product {product.product_id}")
            elif product.option == inventory_pb2.SelectOption.DELETE:
                item = await crud.get_inventory_item_by_product(
                    session, product.product_id
                )
                if item:
                    session.delete(item)
                    session.commit()
                    logger.info(f"Deleted inventory of product {product.product_id}")
                else:
                    logger.warning(f"No inventory for product {product.product_id}")
            else:
                logger.warning(f"Unknown option: {product.option}")
    except Exception:
        logger.exception("Error processing product event")


async def _run(consumer, handler):
    try:
        async for message in consumer:
            await handler(message)
    except Exception:
        logger.exception("Error in consumer")


async def start_consuming():
    await _run(
        kafka.consume_messages(
            settings.KAFKA_TOPIC_INVENTORY,
            settings.KAFKA_CONSUMER_GROUP_ID_FOR_INVENTORY,
        ),
        process_message,
    )


async def start_consuming_product():
    await _run(
        kafka.consume_messages_product(
            settings.KAFKA_TOPIC_GET_PRODUCT,
            settings.KAFKA_CONSUMER_GROUP_ID_FOR_PRODUCT,
        ),
        process_message_product,
    )


async def start_consuming_inventory_check():
    await _run(
        kafka.consume_messages_inventory_check(
            settings.KAFKA_TOPIC_INVENTORY_CHECK_REQUEST,
            settings.KAFKA_CONSUMER_GROUP_ID_FOR_INVENTORY_CHECK,
        ),
        process_message_inventory_check,
    )
