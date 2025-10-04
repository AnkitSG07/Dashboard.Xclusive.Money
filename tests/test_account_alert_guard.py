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


def test_fyers_redirect_updates_alert_guard(client, monkeypatch):
    app = app_module.app
    User = app_module.User

    monkeypatch.setattr(app_module, "get_user_settings", lambda uid: {})

    captured = {}

    def fake_update(user_id, settings):
        captured["user_id"] = user_id
        captured["settings"] = settings

    monkeypatch.setattr(app_module, "update_user_settings", fake_update)

    def fake_exchange(cls, client_id, secret_key, auth_code):
        return {"s": "ok", "access_token": "new-token", "refresh_token": "ref"}

    monkeypatch.setattr(
        app_module.FyersBroker,
        "exchange_code_for_token",
        classmethod(fake_exchange),
    )

    client_id = "FY123"

    with app.app_context():
        app_module.set_pending_fyers(
            {
                client_id: {
                    "secret_key": "sec",
                    "username": "fyuser",
                    "redirect_uri": "https://example.com",
                    "state": "s123",
                    "owner": "test@example.com",
                }
            }
        )

    resp = client.get(f"/fyers_redirects/{client_id}?auth_code=code&state=s123")
    assert resp.status_code == 302

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()

    assert captured["user_id"] == user.id
    assert captured["settings"]["brokers"] == [
        {"name": "fyers", "client_id": client_id, "access_token": "new-token"}
    ]
