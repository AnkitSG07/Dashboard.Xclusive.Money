import os
import sys
import tempfile
from importlib import reload

import pytest

os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("ADMIN_EMAIL", "a@a.com")
os.environ.setdefault("ADMIN_PASSWORD", "pass")
os.environ.setdefault("RUN_SCHEDULER", "0")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

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
        with local_app.test_client() as test_client:
            yield test_client
        local_db.session.remove()
        local_db.drop_all()
    os.close(db_fd)
    os.unlink(db_path)


def login(client):
    return client.post(
        "/login",
        data={"email": "test@example.com", "password": "secret"},
        follow_redirects=True,
    )


def _create_account(app, client_id):
    db = app_module.db
    User = app_module.User
    Account = app_module.Account
    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        account = Account(
            user_id=user.id,
            client_id=client_id,
            broker="dhan",
            credentials={"access_token": "token"},
        )
        db.session.add(account)
        db.session.commit()


def test_rejection_with_fund_hint(client, monkeypatch):
    login(client)
    app = app_module.app
    _create_account(app, "HINT-FUNDS")

    sample_order = {
        "orderId": "101",
        "status": "REJECTED",
        "ReasonDescription": "Fund limit insufficient",
    }

    class DummyBroker:
        def get_order_list(self):
            return [sample_order]

    monkeypatch.setattr(app_module, "broker_api", lambda acc: DummyBroker())

    response = client.get("/api/order-book/HINT-FUNDS")
    assert response.status_code == 200
    orders = response.get_json()
    assert len(orders) == 1
    order = orders[0]
    assert order["remarks"] == "Fund limit insufficient"
    assert order["resolution_hint"] == (
        "Add funds or reduce the order size so it fits within the available margin limit."
    )


def test_rejection_with_strike_hint(client, monkeypatch):
    login(client)
    app = app_module.app
    _create_account(app, "HINT-STRIKE")

    sample_order = {
        "orderId": "202",
        "status": "REJECTED",
        "ReasonDescription": "Strike price is not within range for the selected expiry",
    }

    class DummyBroker:
        def get_order_list(self):
            return [sample_order]

    monkeypatch.setattr(app_module, "broker_api", lambda acc: DummyBroker())

    response = client.get("/api/order-book/HINT-STRIKE")
    assert response.status_code == 200
    order = response.get_json()[0]
    assert order["remarks"].startswith("Strike price is not within range")
    assert order["resolution_hint"] == (
        "Choose an option strike that is allowed for the selected expiry to stay within the exchange range."
    )


def test_successful_order_has_no_hint(client, monkeypatch):
    login(client)
    app = app_module.app
    _create_account(app, "HINT-SUCCESS")

    sample_order = {
        "orderId": "303",
        "status": "COMPLETE",
        "remarks": "Trade successful",
    }

    class DummyBroker:
        def get_order_list(self):
            return [sample_order]

    monkeypatch.setattr(app_module, "broker_api", lambda acc: DummyBroker())

    response = client.get("/api/order-book/HINT-SUCCESS")
    assert response.status_code == 200
    order = response.get_json()[0]
    assert order["remarks"] == "Trade successful"
    assert "resolution_hint" not in order
