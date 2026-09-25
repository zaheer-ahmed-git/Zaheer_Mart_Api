from typing import Optional
from uuid import uuid4

from pydantic import BaseModel, field_validator
from sqlmodel import Field, SQLModel

PAYMENT_PENDING = "Pending"
PAYMENT_DONE = "Payment Done"
PAYMENT_FAILED = "Payment Failed"


class CheckoutRequest(BaseModel):
    amount: int  # Amount in cents
    currency: str = "usd"  # Currency code, e.g., 'usd'
    product_name: str = "Mart order"

    @field_validator("amount")
    @classmethod
    def amount_positive(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("amount must be greater than 0")
        return value


class PAYMENTmodel(SQLModel):
    order_id: str = Field(index=True, unique=True)
    user_id: str = Field(index=True)


class Payment(PAYMENTmodel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    payment_status: str = Field(default=PAYMENT_PENDING, index=True)
    payment_id: str = Field(default_factory=lambda: uuid4().hex, index=True, unique=True)


class PaymentRequest(BaseModel):
    """The card is tokenised by Stripe in the browser/app: the card number, the
    expiry date and the CVC NEVER reach this API."""

    order_id: str
    payment_method_id: str  # e.g. "pm_..." created by Stripe.js / Elements
    amount: int  # Amount in cents

    @field_validator("amount")
    @classmethod
    def amount_positive(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("amount must be greater than 0")
        return value
