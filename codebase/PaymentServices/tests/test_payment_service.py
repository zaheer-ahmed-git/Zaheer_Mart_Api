from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient
from jose import jwt
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine

from app import db, models, payment_pb2, settings
from app.consumer import main as consumer
from app.main import app


def _token(token_type="access"):
    return jwt.encode(
        {
            "sub": "alice",
            "type": token_type,
            "exp": datetime.now(timezone.utc) + timedelta(minutes=5),
        },
        settings.SECRET_KEY,
        algorithm=settings.ALGORITHM,
    )


HEADERS = {"Authorization": f"Bearer {_token()}"}
USER = payment_pb2.User(user_id="u1", username="alice", email="alice@example.com")
BODY = {"order_id": "o1", "payment_method_id": "pm_card_visa", "amount": 1500}


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


@pytest.fixture(autouse=True)
def current_user():
    with patch("app.user_client.get_user", AsyncMock(return_value=USER)):
        yield


def _payment(session, order_id="o1", user_id="u1", status=models.PAYMENT_PENDING):
    p = models.Payment(order_id=order_id, user_id=user_id, payment_status=status)
    session.add(p)
    session.commit()
    return p


def test_root(client):
    assert client.get("/").status_code == 200


def test_payment_requires_a_valid_token(client):
    assert client.put("/payment", json=BODY).status_code == 401
    bad = {"Authorization": "Bearer nope"}
    assert client.put("/payment", json=BODY, headers=bad).status_code == 401


def test_card_number_is_not_accepted_anymore(client):
    # the body has no card_number field: it is just ignored and never forwarded
    assert "card_number" not in models.PaymentRequest.model_fields
    assert "cvc" not in models.PaymentRequest.model_fields


def test_unknown_or_foreign_order_is_404(client, session):
    assert client.put("/payment", json=BODY, headers=HEADERS).status_code == 404
    _payment(session, "o1", user_id="someone-else")
    assert client.put("/payment", json=BODY, headers=HEADERS).status_code == 404


def test_successful_payment(client, session):
    _payment(session)
    with patch("app.main.create_payment", AsyncMock(return_value=payment_pb2.PaymentStatus.PAID)), patch(
        "app.main.publish_payment_result", AsyncMock()
    ) as publish:
        r = client.put("/payment", json=BODY, headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["payment_status"] == "Paid"
    publish.assert_awaited_once()
    assert publish.call_args.kwargs["email"] == "alice@example.com"
    session.expire_all()
    assert session.query(models.Payment).first().payment_status == models.PAYMENT_DONE


def test_failed_payment_publishes_nothing(client, session):
    _payment(session)
    with patch("app.main.create_payment", AsyncMock(return_value=payment_pb2.PaymentStatus.FAILED)), patch(
        "app.main.publish_payment_result", AsyncMock()
    ) as publish:
        r = client.put("/payment", json=BODY, headers=HEADERS)
    assert r.json()["payment_status"] == "Failed"
    publish.assert_not_awaited()


def test_double_payment_is_refused(client, session):
    _payment(session, status=models.PAYMENT_DONE)
    with patch("app.main.create_payment", AsyncMock()) as stripe_call:
        r = client.put("/payment", json=BODY, headers=HEADERS)
    assert r.status_code == 409
    stripe_call.assert_not_awaited()


def test_amount_must_be_positive(client):
    r = client.put("/payment", json={**BODY, "amount": 0}, headers=HEADERS)
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_order_events_create_and_delete_payment_rows(session):
    created = payment_pb2.Order(
        order_id="o9", user_id="u1", option=payment_pb2.SelectOption.CREATE
    )
    await consumer.process_order_message(created)
    await consumer.process_order_message(created)  # idempotent
    assert session.query(models.Payment).count() == 1
    await consumer.process_order_message(
        payment_pb2.Order(order_id="o9", user_id="u1", option=payment_pb2.SelectOption.DELETE)
    )
    session.expire_all()
    assert session.query(models.Payment).count() == 0


def test_payment_result_message_has_no_card_fields():
    fields = {f.name for f in payment_pb2.Payment.DESCRIPTOR.fields}
    assert not fields & {"card_number", "cvc", "exp_month", "exp_year", "password"}
