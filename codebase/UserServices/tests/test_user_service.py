from datetime import timedelta
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine

from app import auth, crud, user_pb2
from app.consumer import main as consumer
from app.db import get_session
from app.main import app
from app.models import UserClass


@pytest.fixture(name="session")
def session_fixture():
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


@pytest.fixture(name="client")
def client_fixture(session: Session):
    app.dependency_overrides[get_session] = lambda: session
    # no `with`: the lifespan (Kafka) is not started in unit tests
    yield TestClient(app)
    app.dependency_overrides.clear()


def _add_user(session: Session, username="testuser", email="test@example.com"):
    user = UserClass(
        username=username,
        email=email,
        password=auth.hash_password("secret"),
        shipping_address="123 Test St",
        phone=5555555555,  # larger than a 32-bit integer on purpose
    )
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _auth_header(username="testuser"):
    return {"Authorization": f"Bearer {auth.generate_token({'sub': username}, timedelta(minutes=5))}"}


def test_read_root(client):
    assert client.get("/").json() == {"Hello": "Welcome To User Services **"}


def test_endpoints_require_a_token(client):
    assert client.get("/users/all").status_code == 401
    assert client.get("/user/a@b.c").status_code == 401
    assert client.delete("/user/1").status_code == 401


def test_refresh_token_is_not_an_access_token(client, session):
    _add_user(session)
    refresh = auth.generate_token(
        {"sub": "test@example.com"}, timedelta(days=1), auth.REFRESH
    )
    response = client.get("/users/all", headers={"Authorization": f"Bearer {refresh}"})
    assert response.status_code == 401


def test_password_hash_is_never_returned(client, session):
    user = _add_user(session)
    response = client.get(f"/user/{user.email}", headers=_auth_header())
    assert response.status_code == 200
    assert "password" not in response.json()
    assert "password" not in client.get("/user/me", headers=_auth_header()).json()
    assert all("password" not in u for u in client.get("/users/all", headers=_auth_header()).json())


def test_login_returns_both_tokens(client, session):
    _add_user(session)
    ok = client.post("/user/login", data={"username": "testuser", "password": "secret"})
    assert ok.status_code == 200
    assert {"access_token", "refresh_token"} <= ok.json().keys()
    bad = client.post("/user/login", data={"username": "testuser", "password": "nope"})
    assert bad.status_code == 401


def test_register_waits_for_the_consumer(client, session):
    async def fake_publish(register):
        _add_user(session, register.username, register.email)

    with patch("app.main.main.check_user_in_db", AsyncMock(return_value=None)), patch(
        "app.main.producer.publish_user_register", side_effect=fake_publish
    ):
        response = client.post(
            "/user/register",
            json={
                "username": "newuser",
                "email": "new@example.com",
                "password": "password",
                "shipping_address": "789 New St",
                "phone": 5555555555,
            },
        )
    assert response.status_code == 200
    assert response.json()["username"] == "newuser"
    assert "password" not in response.json()


def test_cannot_delete_someone_else(client, session):
    _add_user(session)
    other = _add_user(session, "other", "other@example.com")
    with patch("app.main.producer.publish_user_delete", new_callable=AsyncMock) as pub:
        response = client.delete(f"/user/{other.user_id}", headers=_auth_header())
    assert response.status_code == 403
    pub.assert_not_called()


@pytest.mark.asyncio
async def test_crud(session):
    _add_user(session)
    assert (await crud.get_user_by_username(session, "testuser")) is not None
    assert (await crud.get_user_by_email(session, "test@example.com")) is not None
    assert len(await crud.get_all_users(session)) == 1


def test_tokens_have_a_type():
    token = auth.generate_token({"sub": "u"}, timedelta(minutes=1), auth.ACCESS)
    assert auth._decode_sub(token, auth.ACCESS) == "u"
    assert auth._decode_sub(token, auth.REFRESH) is None


@pytest.mark.asyncio
async def test_login_reply_carries_the_correlation_id():
    user = UserClass(
        username="u", email="e@x.c", password=auth.hash_password("pw"),
        shipping_address="a", phone=1,
    )
    request = user_pb2.User(
        username="u", password="pw", correlation_id="abc",
        service=user_pb2.SelectService.ORDER, option=user_pb2.SelectOption.LOGIN,
    )
    with patch.object(consumer, "check_user_in_db", AsyncMock(return_value=user)), \
         patch("app.kafka.send_message", new_callable=AsyncMock) as send:
        await consumer.handle_login(request)
    topic, payload = send.call_args.args
    reply = user_pb2.User()
    reply.ParseFromString(payload)
    assert reply.correlation_id == "abc"
    assert reply.access_token and reply.refresh_token


def test_enum_values_are_shared_between_services():
    # LOGIN must have the same number in every service (it is sent as an integer).
    assert user_pb2.SelectOption.LOGIN == 4
    assert user_pb2.SelectOption.DELETE == 10
