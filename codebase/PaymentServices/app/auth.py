import logging
from typing import Annotated, Optional

from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt

from app import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Login is available on this service as /user/login.
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/user/login")

ACCESS = "access"
REFRESH = "refresh"


def _decode_sub(token: str, expected_type: str) -> Optional[str]:
    try:
        payload = jwt.decode(
            token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM]
        )
    except JWTError:
        return None
    if payload.get("type") != expected_type:
        return None
    return payload.get("sub")


def verify_access_token(token: Annotated[str, Depends(oauth2_scheme)]) -> str:
    """FastAPI dependency: returns the username, or raises 401."""
    username = _decode_sub(token, ACCESS)
    if username is None:
        raise HTTPException(
            status_code=401,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return username


def verify_refresh_token(token: str) -> Optional[str]:
    """Returns the email stored in a refresh token, or None."""
    return _decode_sub(token, REFRESH)
