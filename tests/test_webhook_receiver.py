import os
import sys
import json
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

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
    with app.test_client() as c:
        yield c, events


def test_webhook_enqueues_event(client):
    client_app, events = client
    payload = {"symbol": "NSE:SBIN", "action": "BUY", "quantity": 1}
    resp = client_app.post("/webhook/1", json=payload)
    assert resp.status_code == 202
    assert events
    stream, data = events[0]
    assert stream == "webhook_events"
    assert data["symbol"] == "NSE:SBIN"
    assert data["action"] == "BUY"
    assert data["qty"] == 1
