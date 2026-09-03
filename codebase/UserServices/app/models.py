# models.py
from typing import Optional

from pydantic import BaseModel
from sqlalchemy import BigInteger
from sqlmodel import Field, SQLModel


class UserCreate(SQLModel):
    username: str = Field(unique=True, index=True)
    email: str = Field(unique=True, index=True)
    password: str
    shipping_address: str
    # BigInteger: phone numbers do not fit in a 32-bit INTEGER
    phone: int = Field(sa_type=BigInteger)


class USERLOGIN(SQLModel):
    username: str
    password: str


class Usertoken(SQLModel):
    username: str
    email: str


class UserClass(UserCreate, table=True):
    user_id: Optional[int] = Field(default=None, primary_key=True)


class UserPublic(SQLModel):
    """What the API may return: never the password hash."""

    user_id: int
    username: str
    email: str
    shipping_address: str
    phone: int


class Token(BaseModel):
    access_token: str
