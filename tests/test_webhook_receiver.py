import os
import sys
import json
import pytest

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
    payload = {"symbol": "NSE:SBIN", "action": "BUY", "quantity": 1}
    resp = client_app.post("/webhook/tok1", json=payload)
    assert resp.status_code == 202
    assert events
    stream, data = events[0]
    assert stream == "webhook_events"
    assert data["symbol"] == "NSE:SBIN"
    assert data["action"] == "BUY"
    assert data["qty"] == 1
