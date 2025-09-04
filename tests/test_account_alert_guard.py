import os
import tempfile
from importlib import reload

os.environ.setdefault("SECRET_KEY", "test")
os.environ.setdefault("ADMIN_EMAIL", "a@a.com")
os.environ.setdefault("ADMIN_PASSWORD", "pass")
os.environ.setdefault("RUN_SCHEDULER", "0")
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")

import pytest

import app as app_module


@pytest.fixture
def client():
    db_fd, db_path = tempfile.mkstemp()
    os.environ["DATABASE_URL"] = "sqlite:///" + db_path
    reload(app_module)
    local_app = app_module.app
    local_db = app_module.db
    User = app_module.User
    local_app.config["TESTING"] = True
    local_app.config["WTF_CSRF_ENABLED"] = False
    with local_app.app_context():
        local_db.create_all()
        if not local_db.session.query(User).filter_by(email="test@example.com").first():
            user = User(email="test@example.com")
            user.set_password("secret")
            local_db.session.add(user)
            local_db.session.commit()
    with local_app.test_client() as client:
        with client.session_transaction() as sess:
            sess["user"] = "test@example.com"
        yield client
    os.close(db_fd)
    os.unlink(db_path)


def test_add_account_updates_alert_guard(client, monkeypatch):
    app = app_module.app
    db = app_module.db
    User = app_module.User

    class DummyBroker:
        def __init__(self, *a, **k):
            self.access_token = "tok"
        def check_token_valid(self):
            return True

    monkeypatch.setattr(app_module, "get_broker_class", lambda name: DummyBroker)

    captured = {}
    monkeypatch.setattr(app_module, "get_user_settings", lambda uid: {"max_qty": 5})
    def fake_update(user_id, settings):
        captured["user_id"] = user_id
        captured["settings"] = settings
    monkeypatch.setattr(app_module, "update_user_settings", fake_update)

    data = {
        "broker": "finvasia",
        "client_id": "FIN123",
        "username": "u",
        "password": "p",
        "totp_secret": "t",
        "vendor_code": "v",
        "api_key": "a",
        "imei": "i",
    }
    resp = client.post("/api/add-account", json=data)
    assert resp.status_code == 200
    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
    assert captured["user_id"] == user.id
    assert captured["settings"]["brokers"] == [
        {
            "name": "finvasia",
            "client_id": "FIN123",
            "access_token": "tok",
            "api_key": "a",
        }
    ]
    assert captured["settings"]["max_qty"] == 5


def test_update_account_updates_alert_guard(client, monkeypatch):
    app = app_module.app
    db = app_module.db
    User = app_module.User

    class DummyBroker:
        def __init__(self, *a, **k):
            self.access_token = "tok"
        def check_token_valid(self):
            return True

    monkeypatch.setattr(app_module, "get_broker_class", lambda name: DummyBroker)
    monkeypatch.setattr(app_module, "get_user_settings", lambda uid: {})
    monkeypatch.setattr(app_module, "update_user_settings", lambda *a, **k: None)

    data = {
        "broker": "finvasia",
        "client_id": "FIN124",
        "username": "u",
        "password": "p",
        "totp_secret": "t",
        "vendor_code": "v",
        "api_key": "a",
        "imei": "i",
    }
    assert client.post("/api/add-account", json=data).status_code == 200

    captured = {}
    monkeypatch.setattr(
        app_module,
        "get_user_settings",
        lambda uid: {
            "brokers": [
                {"name": "finvasia", "client_id": "FIN124", "access_token": "old"}
            ],
            "max_qty": 1,
        },
    )
    def fake_update(user_id, settings):
        captured["user_id"] = user_id
        captured["settings"] = settings
    monkeypatch.setattr(app_module, "update_user_settings", fake_update)

    resp = client.post("/api/update-account", json={"client_id": "FIN124"})
    assert resp.status_code == 200
    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
    assert captured["user_id"] == user.id
    assert captured["settings"]["brokers"] == [
        {
            "name": "finvasia",
            "client_id": "FIN124",
            "access_token": "tok",
            "api_key": "a",
        }
    ]
    assert captured["settings"]["max_qty"] == 1

def test_update_account_case_insensitive_replace(client, monkeypatch):
    app = app_module.app

    class DummyBroker:
        def __init__(self, *a, **k):
            self.access_token = "tok"

        def check_token_valid(self):
            return True

    monkeypatch.setattr(app_module, "get_broker_class", lambda name: DummyBroker)
    monkeypatch.setattr(app_module, "get_user_settings", lambda uid: {})
    monkeypatch.setattr(app_module, "update_user_settings", lambda *a, **k: None)

    data = {
        "broker": "finvasia",
        "client_id": "FIN124",
        "username": "u",
        "password": "p",
        "totp_secret": "t",
        "vendor_code": "v",
        "api_key": "a",
        "imei": "i",
    }
    assert client.post("/api/add-account", json=data).status_code == 200

    captured = {}
    monkeypatch.setattr(
        app_module,
        "get_user_settings",
        lambda uid: {
            "brokers": [
                {"name": "FINVASIA", "client_id": "fin124", "access_token": "old"}
            ]
        },
    )

    def fake_update(user_id, settings):
        captured["settings"] = settings

    monkeypatch.setattr(app_module, "update_user_settings", fake_update)

    resp = client.post("/api/update-account", json={"client_id": "FIN124"})
    assert resp.status_code == 200
    assert captured["settings"]["brokers"] == [
        {
            "name": "finvasia",
            "client_id": "FIN124",
            "access_token": "tok",
            "api_key": "a",
        }
    ]
