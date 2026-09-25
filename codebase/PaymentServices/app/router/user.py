import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordRequestForm

from app import auth, user_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

user_router = APIRouter(
    prefix="/user", tags=["user"], responses={404: {"description": "Not found"}}
)


@user_router.post("/login")
async def login_user(
    login: Annotated[OAuth2PasswordRequestForm, Depends(OAuth2PasswordRequestForm)]
):
    user_proto = await user_client.login(login.username, login.password)
    return {
        "access_token": user_proto.access_token,
        "token_type": "bearer",
        "refresh_token": user_proto.refresh_token,
    }


@user_router.get("/me")
async def get_current_user(
    username: Annotated[str, Depends(auth.verify_access_token)]
):
    user_proto = await user_client.get_user(username)
    # Never return the password (hash) of the user.
    return {
        "user_id": user_proto.user_id,
        "username": user_proto.username,
        "email": user_proto.email,
    }


@user_router.post("/refresh_token")
async def refresh_token(old_refresh_token: str):
    email = auth.verify_refresh_token(old_refresh_token)
    if not email:
        raise HTTPException(
            status_code=401,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    user_proto = await user_client.refresh(email)
    return {
        "access_token": user_proto.access_token,
        "token_type": "bearer",
        "refresh_token": user_proto.refresh_token,
    }
