import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException
from sqlmodel import Session

from app import crud, kafka, models
from app.consumer.main import start_consuming, start_consuming_stock
from app.producer import producer
from .db import create_db_and_tables, get_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    await kafka.create_topic()
    tasks = [
        asyncio.create_task(start_consuming()),
        asyncio.create_task(start_consuming_stock()),
    ]
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


app = FastAPI(lifespan=lifespan)

SessionDep = Annotated[Session, Depends(get_session)]


@app.get("/")
async def main():
    return "welcome to Product services "


@app.get("/products/{product_id}/details", response_model=models.Product)
async def get_product_details(product_id: str, session: SessionDep):
    db_product = await crud.get_product(session, product_id)
    if db_product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return db_product


@app.get("/products/", response_model=list[models.ProductBase])
async def read_products(session: SessionDep):
    return await crud.get_all_products(session)


@app.post("/products/", response_model=models.ProductBase, status_code=202)
async def create_product(product: models.ProductBase, session: SessionDep):
    if await crud.get_product(session, product.product_id):
        raise HTTPException(status_code=409, detail="Product id already exists")
    await producer.publish_product_created(product)
    return product


@app.patch("/products/{product_id}", status_code=202)
async def update_product(
    product_id: str, product: models.ProductUpdate, session: SessionDep
):
    if await crud.get_product(session, product_id) is None:
        raise HTTPException(status_code=404, detail="Product not found")
    await producer.publish_product_updated(product, product_id)
    return {"detail": f"Update request for product ID {product_id} has been sent."}


@app.delete("/products/{product_id}", status_code=202)
async def delete_product(product_id: str, session: SessionDep):
    if await crud.get_product(session, product_id) is None:
        raise HTTPException(status_code=404, detail="Product not found")
    await producer.publish_product_deleted(product_id)
    return {"detail": f"Delete request for product ID {product_id} has been sent."}
