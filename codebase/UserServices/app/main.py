# main.py
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Annotated, List

from datetime import timedelta

from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from sqlmodel import Session

from app import auth, crud, db, kafka, models, settings
from app.consumer import main
from app.producer import producer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.create_db_and_tables()
    await kafka.create_topic()
    user_task = asyncio.create_task(main.start_consuming())
    logger.info("Start consuming")
    try:
        yield
    finally:
        user_task.cancel()
        try:
            await user_task
        except asyncio.CancelledError:
            pass


app = FastAPI(lifespan=lifespan)

SessionDep = Annotated[Session, Depends(db.get_session)]
CurrentUsername = Annotated[str, Depends(auth.verify_access_token)]


@app.get("/")
async def read_root():
    return {"Hello": "Welcome To User Services **"}


@app.post("/user/register", response_model=models.UserPublic)
async def register_user(register: models.UserCreate, session: SessionDep):
    if await main.check_user_in_db(register.username, register.email):
        raise HTTPException(status_code=400, detail="User already exists")
    await producer.publish_user_register(register)
    # The user is created asynchronously by the Kafka consumer: wait for it.
    for _ in range(20):
        session.expire_all()
        userdata = await crud.get_user_by_username(session, register.username)
        if userdata is not None:
            return userdata
        await asyncio.sleep(0.5)
    raise HTTPException(status_code=504, detail="User creation is still pending")


@app.post("/user/login")
async def login(
    form: Annotated[OAuth2PasswordRequestForm, Depends()], session: SessionDep
):
    user = await crud.get_user_by_username(session, form.username)
    if user is None or not auth.verify_password(form.password, user.password):
        raise HTTPException(status_code=401, detail="Invalid username or password")
    access_token = auth.generate_token(
        {"sub": user.username},
        timedelta(minutes=settings.JWT_EXPIRY_TIME),
        auth.ACCESS,
    )
    refresh_token = auth.generate_token(
        {"sub": user.email}, timedelta(days=15), auth.REFRESH
    )
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "refresh_token": refresh_token,
    }


@app.get("/user/me", response_model=models.UserPublic)
async def read_me(username: CurrentUsername, session: SessionDep):
    user = await crud.get_user_by_username(session, username)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return user


@app.get("/user/{useremail}", response_model=models.UserPublic)
async def read_user(useremail: str, _: CurrentUsername, session: SessionDep):
    user = await crud.get_user_by_email(session, useremail)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return user


@app.get("/users/all", response_model=List[models.UserPublic])
async def detail_of_all_users(_: CurrentUsername, session: SessionDep):
    return await crud.get_all_users(session)


@app.delete("/user/{user_id}")
async def delete_user(user_id: int, username: CurrentUsername, session: SessionDep):
    current = await crud.get_user_by_username(session, username)
    if current is None or current.user_id != user_id:
        raise HTTPException(
            status_code=403, detail="You can only delete your own account"
        )
    await producer.publish_user_delete(user_id)
    return {"message": f"Deletion of user {user_id} requested"}
