from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine

from app import crud, db, models, product_pb2
from app.consumer import main as consumer
from app.main import app
from app.producer import producer


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


def _product(session, product_id="p1", **kw):
    data = dict(product_id=product_id, name="Shirt", description="Blue", price=10.0,
                is_available=True, category="clothes")
    data.update(kw)
    p = models.Product(**data)
    session.add(p)
    session.commit()
    return p


def test_root(client):
    assert client.get("/").status_code == 200


def test_get_and_list(client, session):
    _product(session)
    assert client.get("/products/p1/details").json()["name"] == "Shirt"
    assert client.get("/products/missing/details").status_code == 404
    assert len(client.get("/products/").json()) == 1


def test_create_refuses_duplicate_id(client, session):
    _product(session)
    body = {"product_id": "p1", "name": "x", "description": "y", "price": 1.0}
    assert client.post("/products/", json=body).status_code == 409
    with patch("app.main.producer.publish_product_created", new_callable=AsyncMock) as pub:
        body["product_id"] = "p2"
        assert client.post("/products/", json=body).status_code == 202
    pub.assert_awaited_once()


def test_update_and_delete_return_404_for_unknown_product(client):
    assert client.patch("/products/zzz", json={"name": "n"}).status_code == 404
    assert client.delete("/products/zzz").status_code == 404


def test_delete_is_accepted(client, session):
    _product(session)
    with patch("app.main.producer.publish_product_deleted", new_callable=AsyncMock) as pub:
        assert client.delete("/products/p1").status_code == 202
    pub.assert_awaited_once_with("p1")


@pytest.mark.asyncio
async def test_partial_update_message_keeps_unsent_fields():
    sent = []

    async def fake_send(topic, payload):
        sent.append(payload)

    with patch("app.kafka.send_kafka_message", fake_send):
        await producer.publish_product_updated(models.ProductUpdate(price=12.5), "p1")
    msg = product_pb2.Product()
    msg.ParseFromString(sent[0])
    assert msg.HasField("price") and not msg.HasField("is_available")
    assert consumer._changes_from_message(msg) == {"price": 12.5}


@pytest.mark.asyncio
async def test_consumer_update_does_not_erase_other_fields(session):
    _product(session)
    msg = product_pb2.Product(product_id="p1", option=product_pb2.SelectOption.UPDATE, price=99.0)
    await consumer.process_message(msg)
    p = await crud.get_product(session, "p1")
    session.refresh(p)
    assert (p.price, p.name, p.description, p.category, p.is_available) == (
        99.0, "Shirt", "Blue", "clothes", True,
    )


@pytest.mark.asyncio
async def test_consumer_can_set_available_to_false(session):
    _product(session)
    msg = product_pb2.Product(product_id="p1", option=product_pb2.SelectOption.UPDATE, is_available=False)
    await consumer.process_message(msg)
    p = await crud.get_product(session, "p1")
    session.refresh(p)
    assert p.is_available is False


@pytest.mark.asyncio
async def test_consumer_create_is_idempotent_and_delete_works(session):
    msg = product_pb2.Product(
        product_id="p5", name="n", description="d", price=1.0, option=product_pb2.SelectOption.CREATE
    )
    await consumer.process_message(msg)
    await consumer.process_message(msg)
    assert session.query(models.Product).count() == 1
    await consumer.process_message(
        product_pb2.Product(product_id="p5", option=product_pb2.SelectOption.DELETE)
    )
    session.expire_all()
    assert session.query(models.Product).count() == 0


@pytest.mark.asyncio
async def test_stock_message_updates_availability_both_ways(session):
    _product(session)
    await consumer.process_message_stock(product_pb2.Inventory(product_id="p1", stock_level=0))
    session.expire_all()
    assert (await crud.get_product(session, "p1")).is_available is False
    await consumer.process_message_stock(product_pb2.Inventory(product_id="p1", stock_level=4))
    session.expire_all()
    assert (await crud.get_product(session, "p1")).is_available is True
