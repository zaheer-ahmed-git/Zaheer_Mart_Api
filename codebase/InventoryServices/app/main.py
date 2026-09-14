import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException
from sqlmodel import Session

from app import crud, kafka, models
from app.consumer import main
from app.producer import producer
from .db import create_db_and_tables, get_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    await kafka.create_topic()
    tasks = [
        asyncio.create_task(main.start_consuming_product()),
        asyncio.create_task(main.start_consuming()),
        asyncio.create_task(main.start_consuming_inventory_check()),
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
async def read_root():
    return {"Hello": "Welcome to Inventory Service ."}


@app.get("/inventory/{inventory_id}", response_model=models.InventoryItem)
async def see_inventory_item(inventory_id: str, session: SessionDep):
    item = await crud.get_inventory_item(session, inventory_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Inventory item not found")
    return item


@app.get("/inventory/", response_model=list[models.InventoryItem])
async def get_all_inventory_items(session: SessionDep):
    return await crud.get_all_inventory_items(session)


@app.post("/inventory/create/", status_code=202)
async def create_inventory_item(item: models.InventoryItemCreate):
    await producer.publish_inventory_create(item.inventory_id, item)
    return {
        "message": "Inventory creation request sent.",
        "inventory_id": item.inventory_id,
    }


@app.put("/inventory/{inventory_id}/add", status_code=202)
async def update_inventory_item(
    inventory_id: str, item: models.InventoryItemUpdate, session: SessionDep
):
    if await crud.get_inventory_item(session, inventory_id) is None:
        raise HTTPException(status_code=404, detail="Inventory item not found")
    await producer.publish_inventory_update(inventory_id, item)
    return {"message": "Inventory update request sent.", "inventory_id": inventory_id}


@app.delete("/inventory/{inventory_id}", status_code=202)
async def delete_inventory_item(inventory_id: str):
    await producer.publish_product_deleted(inventory_id)
    return {"message": f"Deletion of inventory {inventory_id} requested"}


@app.put("/inventory/{product_id}/update_stock", status_code=202)
async def update_stock_level(product_id: str, quantity_change: int):
    await producer.publish_inventory_update_stock_level(product_id, quantity_change)
    return {"message": "Stock update request sent"}


@app.put("/inventory/{product_id}/reduce", status_code=202)
async def reduce_stock_level(product_id: str, quantity_change: int):
    await producer.publish_inventory_reduce(product_id, quantity_change)
    return {"message": "Stock reduction request sent"}
