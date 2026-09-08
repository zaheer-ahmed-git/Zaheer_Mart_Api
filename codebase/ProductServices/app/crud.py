import logging
from typing import List, Optional

from sqlmodel import Session, select

from app.models import Product, ProductBase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def create_product_crud(session: Session, product2: ProductBase) -> Product:
    new_product = Product(
        product_id=product2.product_id,
        name=product2.name,
        description=product2.description,
        price=product2.price,
        is_available=product2.is_available,
        category=product2.category,
    )
    session.add(new_product)
    session.commit()
    session.refresh(new_product)
    logger.info("Product added to the database")
    return new_product


async def get_product(session: Session, product_id: str) -> Optional[Product]:
    return session.exec(select(Product).where(Product.product_id == product_id)).first()


async def get_all_products(session: Session) -> List[Product]:
    return list(session.exec(select(Product)).all())


async def update_product(
    session: Session, product_id: str, changes: dict
) -> Optional[Product]:
    """Apply only the keys of `changes`: a partial update never erases other fields."""
    db_product = await get_product(session, product_id)
    if db_product is None:
        logger.error(f"Product with ID {product_id} not found.")
        return None
    db_product.sqlmodel_update(changes)
    session.add(db_product)
    session.commit()
    session.refresh(db_product)
    return db_product


async def delete_product(session: Session, product_id: str) -> Optional[Product]:
    db_product = await get_product(session, product_id)
    if db_product is None:
        logger.error(f"Product with ID {product_id} not found.")
        return None
    session.delete(db_product)
    session.commit()
    return db_product
