import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Annotated

import stripe
from fastapi import Depends, FastAPI, HTTPException
from sqlmodel import Session

from app import auth, crud, db, kafka, models, payment_pb2, settings, user_client
from app.consumer import main
from app.models import CheckoutRequest, PaymentRequest
from app.producer.main import publish_payment_result
from app.router import user
from app.stripe.payment import create_payment

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.create_table()
    await kafka.create_topic()
    task = asyncio.create_task(main.start_order_consumer())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


app: FastAPI = FastAPI(lifespan=lifespan)
app.include_router(router=user.user_router)

SessionDep = Annotated[Session, Depends(db.get_session)]
CurrentUsername = Annotated[str, Depends(auth.verify_access_token)]


@app.get("/")
async def root():
    return {"message": "Payment Management Service Of Mart Api"}


@app.put("/payment", response_model=dict)
async def payment_request(
    payment_request_detail: PaymentRequest,
    session: SessionDep,
    username: CurrentUsername,
):
    """Pay an order with a Stripe PaymentMethod id (never a raw card number)."""
    user_proto = await user_client.get_user(username)
    payment = await crud.get_order(session, payment_request_detail.order_id)
    # same answer for "does not exist" and "belongs to someone else"
    if payment is None or payment.user_id != str(user_proto.user_id):
        raise HTTPException(
            status_code=404, detail="No accepted order found with this id"
        )
    if payment.payment_status == models.PAYMENT_DONE:
        raise HTTPException(status_code=409, detail="Payment already done")

    status = await create_payment(payment_request_detail)
    if status == payment_pb2.PaymentStatus.PAID:
        payment.payment_status = models.PAYMENT_DONE
    else:
        payment.payment_status = models.PAYMENT_FAILED
    session.add(payment)
    session.commit()

    if status == payment_pb2.PaymentStatus.PAID:
        await publish_payment_result(
            order_id=payment.order_id,
            user_id=payment.user_id,
            username=user_proto.username,
            email=user_proto.email,
            status=payment_pb2.PaymentStatus.PAID,
        )
        return {"payment_status": "Paid", "message": "Payment Successful"}
    return {"payment_status": "Failed", "message": "Payment Unsuccessful"}


@app.post("/create-checkout-session/")
async def create_checkout_session(
    checkout_request: CheckoutRequest, _: CurrentUsername
):
    try:
        session = await asyncio.to_thread(
            stripe.checkout.Session.create,
            payment_method_types=["card"],
            line_items=[
                {
                    "price_data": {
                        "currency": checkout_request.currency,
                        "product_data": {"name": checkout_request.product_name},
                        "unit_amount": checkout_request.amount,
                    },
                    "quantity": 1,
                }
            ],
            mode="payment",
            success_url=settings.STRIPE_SUCCESS_URL,
            cancel_url=settings.STRIPE_CANCEL_URL,
        )
        return {"sessionId": session.id}
    except stripe.StripeError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/success")
async def success_page():
    return {"message": "Payment Successful"}


@app.get("/cancel")
async def cancel_page():
    return {"message": "Payment Cancelled"}
