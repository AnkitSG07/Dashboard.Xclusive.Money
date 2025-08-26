import os
import sys
import json
import pytest
import redis

# Ensure required environment variables before importing app
os.environ.setdefault("SECRET_KEY", "test")
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("ADMIN_EMAIL", "a@a.com")
os.environ.setdefault("ADMIN_PASSWORD", "pass")
os.environ.setdefault("RUN_SCHEDULER", "0")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from services import webhook_receiver
import app as app_module


@pytest.fixture
def client(monkeypatch):
    events = []

    class DummyRedis:
        def xadd(self, stream, data):
            events.append((stream, data))

    monkeypatch.setattr(webhook_receiver, "redis_client", DummyRedis())
    monkeypatch.setattr(webhook_receiver, "check_duplicate_and_risk", lambda e: True)

    from importlib import reload

    reload(app_module)
    local_app = app_module.app
    local_db = app_module.db
    User = app_module.User

    local_app.config["TESTING"] = True
    with local_app.app_context():
        local_db.create_all()
        local_db.session.add(User(id=1, email="u@example.com", webhook_token="tok1"))
        local_db.session.commit()
    with local_app.test_client() as c:
        yield c, events
    with local_app.app_context():
        local_db.drop_all()
        local_db.session.remove()


def test_webhook_endpoint_handles_tokens(client):
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

    resp = client_app.post("/webhook/unknown", json=payload)
    assert resp.status_code == 404
    assert resp.get_json()["error"] == "Unknown webhook token"


def test_webhook_endpoint_survives_redis_down(client, monkeypatch):
    client_app, _ = client
    # Ensure local queue is empty before test
    monkeypatch.setattr(webhook_receiver, "_LOCAL_STREAMS", {})

    class FailingRedis:
        def xadd(self, stream, data):
            raise redis.exceptions.ConnectionError()

    monkeypatch.setattr(webhook_receiver, "redis_client", FailingRedis())

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
    queue = webhook_receiver._LOCAL_STREAMS["webhook_events"]
    assert queue and queue[0]["symbol"] == "NSE:SBIN"
