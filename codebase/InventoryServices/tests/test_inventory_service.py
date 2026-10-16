from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine

from app import crud, db, inventory_pb2, models
from app.consumer import main as consumer
from app.main import app


@pytest.fixture(name="engine")
def engine_fixture(monkeypatch):
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    monkeypatch.setattr(db, "engine", engine)
    return engine


@pytest.fixture(name="session")
def session_fixture(engine):
    with Session(engine) as session:
        yield session


@pytest.fixture(name="client")
def client_fixture(session):
    app.dependency_overrides[db.get_session] = lambda: session
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture(name="sent")
def sent_fixture():
    """Capture what the service publishes on Kafka."""
    sent = []

    async def fake_send(topic, payload):
        sent.append((topic, payload))

    with patch("app.kafka.send_producer_message", fake_send):
        yield sent


def _item(session, product_id="p1", stock=10, reserved=0, sold=0):
    item = models.InventoryItem(
        inventory_id=f"inv-{product_id}", product_id=product_id,
        stock_level=stock, reserved_stock=reserved, sold_stock=sold,
    )
    session.add(item)
    session.commit()
    return item


def _answer(sent):
    answers = [p for t, p in sent if t == "inv-res"]
    msg = inventory_pb2.Order()
    msg.ParseFromString(answers[-1])
    return msg


def _request(option, quantity, cid="c1"):
    return inventory_pb2.Order(product_id="p1", quantity=quantity, option=option, correlation_id=cid)


def test_routes_do_not_crash(client, session):
    _item(session)
    assert client.get("/inventory/inv-p1").json()["stock_level"] == 10
    assert client.get("/inventory/nope").status_code == 404
    assert len(client.get("/inventory/").json()) == 1
    with patch("app.main.producer.publish_product_deleted", new_callable=AsyncMock):
        r = client.delete("/inventory/inv-p1")
    assert r.status_code == 202 and "message" in r.json()
    with patch("app.main.producer.publish_inventory_reduce", new_callable=AsyncMock):
        assert client.put("/inventory/p1/reduce?quantity_change=1").status_code == 202


@pytest.mark.asyncio
async def test_create_reserves_stock_and_answers_with_correlation_id(session, sent):
    _item(session, stock=10)
    await consumer.process_message_inventory_check(_request(inventory_pb2.SelectOption.CREATE, 4, "abc"))
    answer = _answer(sent)
    assert answer.is_stock_available and answer.is_product_available
    assert answer.correlation_id == "abc"
    item = await crud.get_inventory_item_by_product(session, "p1")
    session.refresh(item)
    assert (item.stock_level, item.reserved_stock) == (6, 4)


@pytest.mark.asyncio
async def test_create_refused_when_not_enough_stock(session, sent):
    _item(session, stock=3)
    await consumer.process_message_inventory_check(_request(inventory_pb2.SelectOption.CREATE, 5))
    assert not _answer(sent).is_stock_available
    item = await crud.get_inventory_item_by_product(session, "p1")
    session.refresh(item)
    assert (item.stock_level, item.reserved_stock) == (3, 0)


@pytest.mark.asyncio
async def test_unknown_product_gets_a_negative_answer(session, sent):
    await consumer.process_message_inventory_check(_request(inventory_pb2.SelectOption.CREATE, 1))
    answer = _answer(sent)
    assert not answer.is_product_available and answer.correlation_id == "c1"


@pytest.mark.asyncio
async def test_update_uses_the_difference(session, sent):
    _item(session, stock=6, reserved=4)
    # order grows by 2
    await consumer.process_message_inventory_check(_request(inventory_pb2.SelectOption.UPDATE, 2))
    item = await crud.get_inventory_item_by_product(session, "p1")
    session.refresh(item)
    assert (item.stock_level, item.reserved_stock) == (4, 6)
    # order shrinks by 3
    await consumer.process_message_inventory_check(_request(inventory_pb2.SelectOption.UPDATE, -3))
    session.refresh(item)
    assert (item.stock_level, item.reserved_stock) == (7, 3)


@pytest.mark.asyncio
async def test_update_refused_when_difference_exceeds_stock(session, sent):
    _item(session, stock=1, reserved=4)
    await consumer.process_message_inventory_check(_request(inventory_pb2.SelectOption.UPDATE, 5))
    assert not _answer(sent).is_stock_available


@pytest.mark.asyncio
async def test_delete_releases_and_payment_done_sells(session, sent):
    _item(session, stock=6, reserved=4)
    await consumer.process_message_inventory_check(_request(inventory_pb2.SelectOption.DELETE, 4))
    item = await crud.get_inventory_item_by_product(session, "p1")
    session.refresh(item)
    assert (item.stock_level, item.reserved_stock) == (10, 0)
    await consumer.process_message_inventory_check(_request(inventory_pb2.SelectOption.CREATE, 2))
    await consumer.process_message_inventory_check(_request(inventory_pb2.SelectOption.PAYMENT_DONE, 2))
    session.refresh(item)
    assert (item.reserved_stock, item.sold_stock) == (0, 2)


@pytest.mark.asyncio
async def test_product_events_create_and_delete_inventory(session):
    created = inventory_pb2.Product(product_id="px", option=inventory_pb2.SelectOption.CREATE)
    await consumer.process_message_product(created)
    await consumer.process_message_product(created)  # idempotent
    item = await crud.get_inventory_item_by_product(session, "px")
    assert item is not None and item.inventory_id and item.stock_level == 0
    await consumer.process_message_product(
        inventory_pb2.Product(product_id="px", option=inventory_pb2.SelectOption.DELETE)
    )
    session.expire_all()
    assert await crud.get_inventory_item_by_product(session, "px") is None


@pytest.mark.asyncio
async def test_partial_update_keeps_other_fields(session):
    _item(session, stock=10, reserved=2, sold=1)
    await consumer.process_message(
        inventory_pb2.Inventory(
            inventory_id="inv-p1", reserved_stock=5, option=inventory_pb2.SelectOption.UPDATE
        )
    )
    item = await crud.get_inventory_item(session, "inv-p1")
    session.refresh(item)
    assert (item.stock_level, item.reserved_stock, item.sold_stock) == (10, 5, 1)


def test_enum_numbers_match_the_other_services():
    assert inventory_pb2.SelectOption.CREATE == 7
    assert inventory_pb2.SelectOption.UPDATE == 8
    assert inventory_pb2.SelectOption.DELETE == 10
    assert inventory_pb2.SelectOption.PAYMENT_DONE == 11
