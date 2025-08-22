import os
import sys
import tempfile
import json
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("SECRET_KEY", "test")
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("ADMIN_EMAIL", "a@a.com")
os.environ.setdefault("ADMIN_PASSWORD", "pass")
os.environ.setdefault("RUN_SCHEDULER", "0")

import app as app_module
from services import webhook_receiver


@pytest.fixture
def client(monkeypatch):
    db_fd, db_path = tempfile.mkstemp()
    os.environ["DATABASE_URL"] = "sqlite:///" + db_path
    from importlib import reload
    reload(app_module)
    app = app_module.app
    db = app_module.db
    User = app_module.User

    # stub redis client
    events = []
    class DummyRedis:
        def xadd(self, stream, data):
            events.append((stream, data))
    monkeypatch.setattr(webhook_receiver, "redis_client", DummyRedis())

    app.config["TESTING"] = True
    app.config["WTF_CSRF_ENABLED"] = False
    with app.app_context():
        db.create_all()
        u = User(email="test@example.com")
        u.webhook_token = "tok"
        db.session.add(u)
        db.session.commit()
        yield app.test_client(), events
        db.session.remove()
        db.drop_all()
    os.close(db_fd)
    os.unlink(db_path)


def test_webhook_enqueues_event(client):
    client_app, events = client
    payload = {"symbol": "NSE:SBIN", "action": "BUY", "quantity": 1}
    resp = client_app.post("/webhook/tok", json=payload)
    assert resp.status_code == 202
    assert events
    stream, data = events[0]
    assert stream == "webhook_events"
    assert data["symbol"] == "NSE:SBIN"
    assert data["action"] == "BUY"
    assert data["qty"] == 1
