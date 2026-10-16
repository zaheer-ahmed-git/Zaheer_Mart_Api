from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient
from jose import jwt
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine

from app import crud, db, models, order_pb2, settings
from app.consumer import main as consumer
from app.main import app


def _token(sub="alice", token_type="access"):
    return jwt.encode(
        {
            "sub": sub,
            "type": token_type,
            "exp": datetime.now(timezone.utc) + timedelta(minutes=5),
        },
        settings.SECRET_KEY,
        algorithm=settings.ALGORITHM,
    )


HEADERS = {"Authorization": f"Bearer {_token()}"}
USER = order_pb2.User(user_id="u1", username="alice", email="alice@example.com")


@pytest.fixture(name="engine")
def engine_fixture(monkeypatch):
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    monkeypatch.setattr(db, "engine", engine)  # used by the Kafka consumers
    return engine


@pytest.fixture(name="session")
def session_fixture(engine):
    with Session(engine) as session:
        yield session


@pytest.fixture(name="client")
def client_fixture(session):
    app.dependency_overrides[db.get_session] = lambda: session
    # no `with`: the lifespan (Kafka) is not started in unit tests
    yield TestClient(app)
    app.dependency_overrides.clear()


def _order(session, order_id="o1", user_id="u1", **kw):
    data = dict(
        order_id=order_id,
        product_id="p1",
        user_id=user_id,
        quantity=2,
        shipping_address="addr",
        customer_notes="",
        order_status=models.ORDER_IN_PROGRESS,
        payment_status=models.PAYMENT_PENDING,
    )
    data.update(kw)
    order = models.Orders(**data)
    session.add(order)
    session.commit()
    return order


@pytest.fixture(autouse=True)
def current_user():
    with patch("app.user_client.get_user", AsyncMock(return_value=USER)):
        yield


# ---------- authentication ----------
def test_root_is_public(client):
    assert client.get("/").json() == {"message": "Order Management Service"}


def test_invalid_token_is_rejected(client):
    bad = {"Authorization": "Bearer garbage"}
    assert client.get("/orders", headers=bad).status_code == 401
    assert client.get("/orders").status_code == 401


def test_refresh_token_cannot_be_used_as_access_token(client):
    h = {"Authorization": f"Bearer {_token(token_type='refresh')}"}
    assert client.get("/orders", headers=h).status_code == 401


# ---------- API ----------
def test_user_only_sees_own_orders(client, session):
    _order(session, "mine", "u1")
    _order(session, "theirs", "u2")
    ids = [o["order_id"] for o in client.get("/orders", headers=HEADERS).json()]
    assert ids == ["mine"]
    assert client.get("/order/theirs", headers=HEADERS).status_code == 404
    assert client.get("/order/missing", headers=HEADERS).status_code == 404


def test_create_order_generates_the_id_server_side(client):
    with patch("app.main.producer.publish_order_created", new_callable=AsyncMock) as pub:
        r = client.post(
            "/order",
            json={"product_id": "p1", "quantity": 2, "shipping_address": "addr"},
            headers=HEADERS,
        )
    assert r.status_code == 202
    order_id = r.json()["order_id"]
    assert len(order_id) == 32
    assert pub.call_args.args[0] == order_id


def test_create_order_rejects_bad_quantity(client):
    r = client.post(
        "/order",
        json={"product_id": "p1", "quantity": 0, "shipping_address": "addr"},
        headers=HEADERS,
    )
    assert r.status_code == 422


def test_update_and_delete_require_ownership(client, session):
    _order(session, "theirs", "u2")
    with patch("app.main.producer.publish_order_update", new_callable=AsyncMock) as upd, patch(
        "app.main.producer.publish_order_deleted", new_callable=AsyncMock
    ) as dele:
        r1 = client.put("/order/theirs", json={"quantity": 3}, headers=HEADERS)
        r2 = client.delete("/order/theirs", headers=HEADERS)
    assert r1.status_code == 404 and r2.status_code == 404
    upd.assert_not_called()
    dele.assert_not_called()


def test_paid_order_cannot_be_deleted(client, session):
    _order(session, "paid", payment_status=models.PAYMENT_DONE)
    assert client.delete("/order/paid", headers=HEADERS).status_code == 409


# ---------- consumer ----------
def _msg(**kw):
    return order_pb2.Order(
        order_id="o1", user_id="u1", product_id="p1", quantity=2,
        shipping_address="addr", **kw
    )


@pytest.mark.asyncio
async def test_create_accepted_when_stock_is_available(session):
    ok = SimpleNamespace(is_product_available=True, is_stock_available=True)
    with patch.object(consumer, "inventory_check", AsyncMock(return_value=ok)), patch.object(
        consumer.producer, "publish_order_event", AsyncMock()
    ) as event:
        await consumer.handle_create_order(_msg())
    order = await crud.get_order(session, "o1")
    assert order.order_status == models.ORDER_IN_PROGRESS
    event.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_rejected_when_out_of_stock(session):
    ko = SimpleNamespace(is_product_available=True, is_stock_available=False)
    with patch.object(consumer, "inventory_check", AsyncMock(return_value=ko)), patch.object(
        consumer.producer, "publish_order_event", AsyncMock()
    ) as event:
        await consumer.handle_create_order(_msg())
    assert (await crud.get_order(session, "o1")).order_status == models.ORDER_REJECTED
    event.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_is_idempotent(session):
    _order(session, "o1")
    check = AsyncMock()
    with patch.object(consumer, "inventory_check", check):
        await consumer.handle_create_order(_msg())
    check.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_only_changes_the_sent_fields(session):
    _order(session, "o1", customer_notes="keep me")
    msg = order_pb2.Order(order_id="o1", user_id="u1", shipping_address="new addr")
    await consumer.handle_update_order(msg)
    order = await crud.get_order(session, "o1")
    session.refresh(order)
    assert order.shipping_address == "new addr"
    assert order.customer_notes == "keep me"
    assert order.quantity == 2


@pytest.mark.asyncio
async def test_update_quantity_sends_the_difference_to_inventory(session):
    _order(session, "o1", quantity=2)
    check = AsyncMock(return_value=SimpleNamespace(is_stock_available=True))
    with patch.object(consumer, "inventory_check", check):
        await consumer.handle_update_order(
            order_pb2.Order(order_id="o1", user_id="u1", quantity=5)
        )
    assert check.call_args.args[1] == 3
    order = await crud.get_order(session, "o1")
    session.refresh(order)
    assert order.quantity == 5


@pytest.mark.asyncio
async def test_payment_done_updates_the_order_once(session):
    _order(session, "o1")
    msg = order_pb2.Payment(
        order_id="o1", user_id="u1", payment_status=order_pb2.PaymentStatus.PAID
    )
    check = AsyncMock()
    with patch.object(consumer, "inventory_check", check):
        await consumer.process_message_payment(msg)
        await consumer.process_message_payment(msg)
    assert check.await_count == 1
    order = await crud.get_order(session, "o1")
    session.refresh(order)
    assert order.payment_status == models.PAYMENT_DONE
    assert order.order_status == models.ORDER_COMPLETED


@pytest.mark.asyncio
async def test_delete_is_refused_for_other_user_or_paid_order(session):
    _order(session, "o1", user_id="u2")
    _order(session, "o2", payment_status=models.PAYMENT_DONE)
    with patch.object(consumer, "inventory_check", AsyncMock()) as check:
        await consumer.handle_delete_order(order_pb2.Order(order_id="o1", user_id="u1"))
        await consumer.handle_delete_order(order_pb2.Order(order_id="o2", user_id="u1"))
    check.assert_not_awaited()
    assert await crud.get_order(session, "o1") is not None
    assert await crud.get_order(session, "o2") is not None


@pytest.mark.asyncio
async def test_delete_releases_stock(session):
    _order(session, "o1")
    check = AsyncMock()
    with patch.object(consumer, "inventory_check", check), patch.object(
        consumer.producer, "publish_order_event", AsyncMock()
    ):
        await consumer.handle_delete_order(order_pb2.Order(order_id="o1", user_id="u1"))
    assert check.call_args.args[2] == order_pb2.SelectOption.DELETE
    assert await crud.get_order(session, "o1") is None


def test_enum_numbers_match_the_other_services():
    assert order_pb2.SelectOption.LOGIN == 4
    assert order_pb2.SelectOption.CREATE == 7
    assert order_pb2.SelectOption.UPDATE == 8
    assert order_pb2.SelectOption.PAYMENT_DONE == 11
