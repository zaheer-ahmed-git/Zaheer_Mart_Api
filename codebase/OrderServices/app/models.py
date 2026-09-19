from typing import Optional
from uuid import uuid4

from pydantic import field_validator
from sqlmodel import Field, SQLModel

# Single source of truth for the statuses stored in the database.
ORDER_IN_PROGRESS = "In_progress"
ORDER_COMPLETED = "Completed"
ORDER_REJECTED = "Rejected"
PAYMENT_PENDING = "Pending"
PAYMENT_DONE = "Payment Done"
PAYMENT_NOT_REQUIRED = "Not required"


def new_order_id() -> str:
    return uuid4().hex


class OrdersBase(SQLModel):
    order_id: str = Field(default_factory=new_order_id, unique=True, index=True)
    product_id: str = Field(index=True)
    user_id: str = Field(index=True)
    quantity: int
    shipping_address: str
    customer_notes: str = ""
    order_status: str = Field(default=ORDER_IN_PROGRESS, index=True)
    payment_status: str = Field(default=PAYMENT_PENDING, index=True)


class Orders(OrdersBase, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)


class OrdersCreate(SQLModel):
    """Body of POST /order. The user and the order id are never sent by the client."""

    product_id: str
    quantity: int
    shipping_address: str
    customer_notes: str = ""

    @field_validator("quantity")
    @classmethod
    def quantity_must_be_positive(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("quantity must be greater than 0")
        return value


class OrdersUpdate(SQLModel):
    """Fields a client may change. Statuses are managed by the services only."""

    quantity: Optional[int] = None
    shipping_address: Optional[str] = None
    customer_notes: Optional[str] = None

    @field_validator("quantity")
    @classmethod
    def quantity_must_be_positive(cls, value: Optional[int]) -> Optional[int]:
        if value is not None and value <= 0:
            raise ValueError("quantity must be greater than 0")
        return value
