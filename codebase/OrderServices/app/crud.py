import logging
from typing import List, Optional

from sqlmodel import Session, select

from app.models import Orders

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def create_order(
    session: Session,
    order_id: str,
    user_id: str,
    product_id: str,
    quantity: int,
    shipping_address: str,
    customer_notes: str,
    order_status: str,
    payment_status: str,
) -> Orders:
    new_order = Orders(
        order_id=order_id,
        product_id=product_id,
        user_id=user_id,
        quantity=quantity,
        shipping_address=shipping_address,
        customer_notes=customer_notes,
        order_status=order_status,
        payment_status=payment_status,
    )
    session.add(new_order)
    session.commit()
    session.refresh(new_order)
    return new_order


async def get_order(session: Session, order_id: str) -> Optional[Orders]:
    return session.exec(select(Orders).where(Orders.order_id == order_id)).first()


async def get_orders_by_user(session: Session, user_id: str) -> List[Orders]:
    return list(session.exec(select(Orders).where(Orders.user_id == user_id)).all())


async def update_order(session: Session, order_id: str, changes: dict) -> Optional[Orders]:
    """Apply only the keys present in `changes` (never overwrite with empty values)."""
    db_order = await get_order(session, order_id)
    if db_order is None:
        logger.error(f"Order with ID {order_id} not found.")
        return None
    db_order.sqlmodel_update(changes)
    session.add(db_order)
    session.commit()
    session.refresh(db_order)
    return db_order


async def delete_order(session: Session, order_id: str) -> Optional[Orders]:
    db_order = await get_order(session, order_id)
    if db_order:
        session.delete(db_order)
        session.commit()
    return db_order
