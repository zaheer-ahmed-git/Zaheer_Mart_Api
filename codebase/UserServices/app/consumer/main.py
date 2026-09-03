# consumer/main.py
import logging
from datetime import timedelta
from typing import Optional, Union

from sqlmodel import Session

from app import auth, crud, db, kafka, models, settings, user_pb2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REFRESH_TOKEN_DAYS = 15


async def check_user_in_db(username: Optional[str], email: Optional[str] = None):
    """Return the user matching the username, or else the email."""
    with Session(db.engine) as session:
        user = None
        if username:
            user = await crud.get_user_by_username(session, username)
        if user is None and email:
            user = await crud.get_user_by_email(session, email)
        if user:
            logger.info("User found in the database")
        return user


async def reply(request: user_pb2.User, response: user_pb2.User) -> None:
    """Send the response to the topic of the asking service, with its correlation id."""
    response.correlation_id = request.correlation_id
    payload = response.SerializeToString()
    if request.service == user_pb2.SelectService.PAYMENT:
        topic = settings.KAFKA_TOPIC_RESPONSE_FROM_USER_TO_PAYMENT
    elif request.service == user_pb2.SelectService.ORDER:
        topic = settings.KAFKA_TOPIC_RESPONSE_FROM_USER_TO_ORDER
    else:
        logger.warning("Unknown requesting service, response dropped")
        return
    await kafka.send_message(topic, payload)


def _error(message: str, status: int) -> user_pb2.User:
    return user_pb2.User(error_message=message, http_status_code=status)


async def handle_register_user(new_msg: user_pb2.User):
    if await check_user_in_db(new_msg.username, new_msg.email):
        logger.info("User with these credentials already exists")
        return
    user = models.UserClass(
        username=new_msg.username,
        email=new_msg.email,
        password=auth.hash_password(new_msg.password),
        phone=new_msg.phone,
        shipping_address=new_msg.shipping_address,
    )
    with Session(db.engine) as session:
        session.add(user)
        session.commit()
        session.refresh(user)
        event = user_pb2.User(
            id=user.user_id or 0,
            user_id=str(user.user_id),
            username=user.username,
            email=user.email,
            option=user_pb2.SelectOption.REGISTER,
        )
    logger.info("User added to database")
    # Event for the notification service: never contains the password.
    await kafka.send_message(
        settings.KAFKA_TOPIC_USER_REGISTERED, event.SerializeToString()
    )


async def handle_login(new_msg: user_pb2.User):
    user = await check_user_in_db(new_msg.username)
    if user is None or not auth.verify_password(new_msg.password, user.password):
        await reply(new_msg, _error("Invalid username or password", 401))
        return
    access_token = auth.generate_token(
        data={"sub": user.username},
        expires_delta=timedelta(minutes=settings.JWT_EXPIRY_TIME),
        token_type=auth.ACCESS,
    )
    refresh_token = auth.generate_token(
        data={"sub": user.email},
        expires_delta=timedelta(days=REFRESH_TOKEN_DAYS),
        token_type=auth.REFRESH,
    )
    await reply(
        new_msg,
        user_pb2.User(
            access_token=access_token,
            refresh_token=refresh_token,
            option=user_pb2.SelectOption.LOGIN,
        ),
    )


async def handle_verify_user(new_msg: user_pb2.User):
    user = await check_user_in_db(new_msg.username, new_msg.email)
    if not user:
        await reply(new_msg, _error("User does not exist", 404))
        return
    # The password hash is never sent to other services.
    await reply(
        new_msg,
        user_pb2.User(
            id=user.user_id or 0,
            user_id=str(user.user_id),
            username=user.username,
            email=user.email,
            shipping_address=user.shipping_address,
            phone=user.phone,
            option=user_pb2.SelectOption.CURRENT_USER,
        ),
    )


async def handle_refresh_token(new_msg: user_pb2.User):
    user = await check_user_in_db(None, new_msg.email)
    if not user:
        await reply(new_msg, _error("User does not exist", 404))
        return
    access_token = auth.generate_token(
        data={"sub": user.username},
        expires_delta=timedelta(minutes=settings.JWT_EXPIRY_TIME),
        token_type=auth.ACCESS,
    )
    refresh_token = auth.generate_token(
        data={"sub": user.email},
        expires_delta=timedelta(days=REFRESH_TOKEN_DAYS),
        token_type=auth.REFRESH,
    )
    await reply(
        new_msg,
        user_pb2.User(
            access_token=access_token,
            refresh_token=refresh_token,
            option=user_pb2.SelectOption.REFRESH_TOKEN,
        ),
    )


async def handle_delete_user(userid: Union[str, int]):
    with Session(db.engine) as session:
        user = await crud.get_user_id(session, int(userid))
        if user:
            session.delete(user)
            session.commit()
            logger.info(f"User with id {userid} deleted")


async def process_message(new_msg: user_pb2.User):
    try:
        option = new_msg.option
        if option == user_pb2.SelectOption.REGISTER:
            await handle_register_user(new_msg)
        elif option == user_pb2.SelectOption.LOGIN:
            await handle_login(new_msg)
        elif option == user_pb2.SelectOption.CURRENT_USER:
            await handle_verify_user(new_msg)
        elif option == user_pb2.SelectOption.REFRESH_TOKEN:
            await handle_refresh_token(new_msg)
        elif option == user_pb2.SelectOption.DELETE:
            await handle_delete_user(new_msg.user_id)
        else:
            logger.warning(f"Unknown option received: {option}")
    except Exception:
        logger.exception("Error processing message")


async def start_consuming():
    try:
        async for message in kafka.consume_messages_user(
            settings.KAFKA_TOPIC_USER, settings.KAFKA_CONSUMER_GROUP_ID_FOR_USER
        ):
            await process_message(message)
    except Exception:
        logger.exception("Error in consumer")
