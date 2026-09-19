import asyncio
from contextlib import asynccontextmanager
from typing import Annotated, List

from fastapi import Depends, FastAPI, HTTPException
from sqlmodel import Session

from app import auth, crud, db, kafka, models, user_client
from app.db import get_session
from app.consumer import main
from app.producer import producer
from app.router import user

SessionDep = Annotated[Session, Depends(get_session)]
CurrentUsername = Annotated[str, Depends(auth.verify_access_token)]


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.create_table()
    await kafka.create_topic()
    tasks = [
        asyncio.create_task(main.start_consuming()),
        asyncio.create_task(main.start_consuming_payment()),
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


# Authentication is applied route by route: /user/login must stay public.
app: FastAPI = FastAPI(lifespan=lifespan)

app.include_router(router=user.user_router)


@app.get("/")
async def read_root():
    return {"message": "Order Management Service"}


async def _owned_order(session: Session, order_id: str, user_id: str) -> models.Orders:
    order = await crud.get_order(session, order_id)
    # 404 (not 403) so that other users cannot discover order ids
    if order is None or order.user_id != user_id:
        raise HTTPException(status_code=404, detail="Order not found")
    return order


@app.get("/orders", response_model=List[models.Orders])
async def get_my_orders(session: SessionDep, username: CurrentUsername):
    user_proto = await user_client.get_user(username)
    return await crud.get_orders_by_user(session, str(user_proto.user_id))


@app.get("/order/{order_id}", response_model=models.Orders)
async def get_order_detail(
    order_id: str, session: SessionDep, username: CurrentUsername
):
    user_proto = await user_client.get_user(username)
    return await _owned_order(session, order_id, str(user_proto.user_id))


@app.post("/order", status_code=202)
async def create_order(order: models.OrdersCreate, username: CurrentUsername):
    """Order creation is asynchronous: poll GET /order/{order_id} for the result
    (order_status is 'In_progress' when accepted, 'Rejected' when out of stock)."""
    user_proto = await user_client.get_user(username)
    order_id = models.new_order_id()
    await producer.publish_order_created(order_id, order, user_proto)
    return {"order_id": order_id, "detail": "Order request received"}


@app.put("/order/{order_id}", status_code=202)
async def update_order(
    order_id: str,
    order: models.OrdersUpdate,
    session: SessionDep,
    username: CurrentUsername,
):
    user_proto = await user_client.get_user(username)
    await _owned_order(session, order_id, str(user_proto.user_id))
    await producer.publish_order_update(order_id, order, str(user_proto.user_id))
    return {"order_id": order_id, "detail": "Update request received"}


@app.delete("/order/{order_id}", status_code=202)
async def delete_order(order_id: str, session: SessionDep, username: CurrentUsername):
    user_proto = await user_client.get_user(username)
    order = await _owned_order(session, order_id, str(user_proto.user_id))
    if order.payment_status == models.PAYMENT_DONE:
        raise HTTPException(status_code=409, detail="A paid order cannot be deleted")
    await producer.publish_order_deleted(order_id, str(user_proto.user_id))
    return {"order_id": order_id, "detail": "Delete request received"}
