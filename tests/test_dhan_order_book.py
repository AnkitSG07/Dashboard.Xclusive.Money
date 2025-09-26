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
        with local_app.test_client() as client:
            yield client
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


def test_dhan_order_parsing_handles_quantity_and_price(client, monkeypatch):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        acc = Account(
            user_id=user.id,
            client_id="DH1",
            broker="dhan",
            credentials={"access_token": "token"},
        )
        db.session.add(acc)
        db.session.commit()

    sample_order = {
        "orderId": "1",
        "status": "COMPLETE",
        "orderQty": 1,
        "quantity": 999,  # price-related field that should not be used as qty
        "price": 123.45,
        "tradedPrice": 123.45,
        "tradingSymbol": "IDEA",
        "transactionType": "BUY",
    }

    class DummyBroker:
        def get_order_list(self):
            return [sample_order]

    monkeypatch.setattr(app_module, "broker_api", lambda acc: DummyBroker())

    resp = client.get("/api/order-book/DH1")
    assert resp.status_code == 200
    data = resp.get_json()
    assert isinstance(data, list) and len(data) == 1
    order = data[0]
    assert order["placed_qty"] == 1
    assert order["avg_price"] == sample_order["price"]




def test_dhan_rejected_order_surfaces_nested_reason(client, monkeypatch):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    nested_reason = {
        "omsErrorMessage": "RMS: Margin not available",
        "oms_error_description": "Insufficient funds",
    }

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        acc = Account(
            user_id=user.id,
            client_id="DH2",
            broker="dhan",
            credentials={"access_token": "token"},
        )
        db.session.add(acc)
        db.session.commit()

    sample_order = {
        "orderId": "2",
        "status": "REJECTED",
        "orderResponse": {
            "details": nested_reason,
        },
    }

    class DummyBroker:
        def get_order_list(self):
            return [sample_order]

    monkeypatch.setattr(app_module, "broker_api", lambda acc: DummyBroker())

    resp = client.get("/api/order-book/DH2")
    assert resp.status_code == 200
    data = resp.get_json()
    assert isinstance(data, list) and len(data) == 1
    order = data[0]
    assert order["status"] == "REJECTED"
    assert order["remarks"] == nested_reason["omsErrorMessage"]


@pytest.fixture
def dhan_rejection_payload():
    return {
        "orderId": "3",
        "status": "REJECTED",
        "transactionType": "SELL",
        "tradingSymbol": "SBIN",
        "orderQty": 1,
        "reasonCode": 0,
        "ReasonDescription": "Fund limit insufficient",
    }


def test_dhan_rejected_order_prefers_description_over_numeric_code(
    client, monkeypatch, dhan_rejection_payload
):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        acc = Account(
            user_id=user.id,
            client_id="DH3",
            broker="dhan",
            credentials={"access_token": "token"},
        )
        db.session.add(acc)
        db.session.commit()

    class DummyBroker:
        def get_order_list(self):
            return [dhan_rejection_payload]

    monkeypatch.setattr(app_module, "broker_api", lambda acc: DummyBroker())

    resp = client.get("/api/order-book/DH3")
    assert resp.status_code == 200
    data = resp.get_json()
    assert isinstance(data, list) and len(data) == 1
    order = data[0]
    assert order["status"] == "REJECTED"
    assert order["remarks"] == "Fund limit insufficient"
