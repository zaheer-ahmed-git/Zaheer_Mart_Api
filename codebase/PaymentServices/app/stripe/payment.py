import asyncio
import logging

import stripe

from app import models, payment_pb2, settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

stripe.api_key = str(settings.STRIPE_API_KEY)


async def create_payment(payment_request: models.PaymentRequest) -> int:
    """Charge a Stripe PaymentMethod (token). Returns a payment_pb2.PaymentStatus."""
    try:
        # The stripe SDK is blocking: run it in a thread so the event loop
        # (and the Kafka consumers) keep working.
        payment_intent = await asyncio.to_thread(
            stripe.PaymentIntent.create,
            amount=payment_request.amount,
            currency="usd",
            payment_method=payment_request.payment_method_id,
            confirm=True,
            automatic_payment_methods={"enabled": True, "allow_redirects": "never"},
            # the same order can never be charged twice
            idempotency_key=f"order-{payment_request.order_id}",
        )
        if payment_intent["status"] == "succeeded":
            return payment_pb2.PaymentStatus.PAID
        return payment_pb2.PaymentStatus.FAILED
    except stripe.StripeError as e:
        logger.error(f"Stripe error: {e}")
        return payment_pb2.PaymentStatus.FAILED
