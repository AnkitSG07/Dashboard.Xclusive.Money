import os
import sys
import json
import pytest
import redis

os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from models import db, User
from services import webhook_receiver
from services.webhook_server import app

@pytest.fixture
def client(monkeypatch):
    events = []
    
    class DummyRedis:
        def xadd(self, stream, data):
            events.append((stream, data))
            
    monkeypatch.setattr(webhook_receiver, "redis_client", DummyRedis())
    monkeypatch.setattr(webhook_receiver, "check_duplicate_and_risk", lambda e: True)

    app.config["TESTING"] = True
    with app.app_context():
        db.create_all()
        db.session.add(User(id=1, email="u@example.com", webhook_token="tok1"))
        db.session.commit()
    with app.test_client() as c:
        yield c, events
    with app.app_context():
        db.drop_all()
        db.session.remove()


def test_webhook_enqueues_event(client):
    client_app, events = client
    payload = {
        "exchange": "NSE",
        "orderType": "market",
        "orderValidity": "day",
        "productType": "intraday",
        "masterAccounts": ["50"],
        "transactionType": "BUY",
        "orderQty": 1,
        "tradingSymbols": ["NSE:SBIN"],
    }
    resp = client_app.post("/webhook/tok1", json=payload)
    assert resp.status_code == 202
    assert events
    stream, data = events[0]
    assert stream == "webhook_events"
    assert data["symbol"] == "NSE:SBIN"
    assert data["action"] == "BUY"
    assert data["qty"] == 1
    assert data["orderType"] == "market"
    assert data["productType"] == "intraday"
    # Lists are serialized to JSON strings for Redis compatibility
    assert json.loads(data["masterAccounts"]) == ["50"]
    assert json.loads(data["tradingSymbols"]) == ["NSE:SBIN"]

def test_enqueue_webhook_redis_failure(monkeypatch):
    class FailingRedis:
        def xadd(self, stream, data):
            raise redis.exceptions.ConnectionError()

    monkeypatch.setattr(webhook_receiver, "redis_client", FailingRedis())
    monkeypatch.setattr(webhook_receiver, "check_duplicate_and_risk", lambda e: True)

    payload = {"symbol": "NSE:SBIN", "action": "BUY", "qty": 1}
    with pytest.raises(redis.exceptions.RedisError):
        webhook_receiver.enqueue_webhook(1, None, payload)


def test_enqueue_webhook_sanitizes_none(monkeypatch):
    events = []

    class DummyRedis:
        def xadd(self, stream, data):
            events.append((stream, data))

    monkeypatch.setattr(webhook_receiver, "redis_client", DummyRedis())
    monkeypatch.setattr(webhook_receiver, "check_duplicate_and_risk", lambda e: True)

    payload = {"symbol": "NSE:SBIN", "action": "BUY", "qty": 1}
    webhook_receiver.enqueue_webhook(1, None, payload)

    assert events, "Event was not published"
    _, data = events[0]
    assert data["strategy_id"] == ""
    assert data["exchange"] == ""
