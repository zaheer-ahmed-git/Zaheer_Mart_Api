from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine

from app import db, notification_pb2
from app.consumer import main as consumer
from app.main import app
from app.models import Notification


@pytest.fixture(name="session")
def session_fixture(monkeypatch):
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    monkeypatch.setattr(db, "engine", engine)
    with Session(engine) as session:
        yield session


@pytest.fixture(name="email")
def email_fixture():
    with patch("app.handle_email.send_email", new_callable=AsyncMock) as send:
        yield send


def test_root():
    assert TestClient(app).get("/").json() == {"message": "Notification Management Service"}


@pytest.mark.asyncio
async def test_registration_email_and_user_id(session, email):
    msg = notification_pb2.User(
        id=7, user_id="7", username="bob", email="bob@example.com",
        option=notification_pb2.SelectOption.REGISTER,
    )
    await consumer.process_message_user(msg)
    email.assert_awaited_once()
    assert email.call_args.kwargs["user_email"] == "bob@example.com"
    note = session.query(Notification).one()
    assert note.user_id == 7 and note.notification_type == "USER_REGISTRATION"


@pytest.mark.asyncio
async def test_order_email_only_for_created_events(session, email):
    base = dict(order_id="o1", user_id="12", username="bob", email="bob@example.com")
    await consumer.process_message_order(
        notification_pb2.Order(option=notification_pb2.SelectOption.DELETE, **base)
    )
    email.assert_not_awaited()
    await consumer.process_message_order(
        notification_pb2.Order(option=notification_pb2.SelectOption.CREATE, **base)
    )
    email.assert_awaited_once()
    assert session.query(Notification).one().user_id == 12


@pytest.mark.asyncio
async def test_payment_email_only_when_paid(session, email):
    base = dict(order_id="o1", user_id="3", username="bob", email="bob@example.com")
    await consumer.process_message_payment(
        notification_pb2.Payment(payment_status=notification_pb2.PaymentStatus.FAILED, **base)
    )
    email.assert_not_awaited()
    await consumer.process_message_payment(
        notification_pb2.Payment(payment_status=notification_pb2.PaymentStatus.PAID, **base)
    )
    email.assert_awaited_once()
    body = email.call_args.kwargs["body"]
    assert "dispatched" not in body  # we do not claim a shipment that never happened


@pytest.mark.asyncio
async def test_send_email_runs_smtp_in_a_thread():
    from app import handle_email

    with patch("app.handle_email.smtplib.SMTP_SSL") as smtp:
        await handle_email.send_email("body", "subject", "to@example.com")
    smtp.return_value.__enter__.return_value.sendmail.assert_called_once()
