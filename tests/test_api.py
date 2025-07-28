import os
import sys
import pytest
import tempfile
import io
import json
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("SECRET_KEY", "test")
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("ADMIN_EMAIL", "a@a.com")
os.environ.setdefault("ADMIN_PASSWORD", "pass")
os.environ.setdefault("RUN_SCHEDULER", "0")

import app as app_module

@pytest.fixture
def client():
    db_fd, db_path = tempfile.mkstemp()
    os.environ["DATABASE_URL"] = "sqlite:///" + db_path
    from importlib import reload
    reload(app_module)
    local_app = app_module.app
    local_db = app_module.db
    User = app_module.User
    local_app.config["TESTING"] = True
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
    return client.post("/login", data={"email": "test@example.com", "password": "secret"})


def test_account_endpoint_requires_auth(client):
    resp = client.get("/api/account")
    assert resp.status_code == 401
    login(client)
    resp = client.get("/api/account")
    assert resp.status_code in (200, 400)


def test_orders_endpoint_requires_auth(client):
    resp = client.get("/api/orders")
    assert resp.status_code == 401
    login(client)
    resp = client.get("/api/orders")
    assert resp.status_code in (200, 400, 500)


def test_portfolio_endpoint_requires_auth(client):
    resp = client.get("/api/portfolio")
    assert resp.status_code == 401
    login(client)
    resp = client.get("/api/portfolio")
    assert resp.status_code in (200, 400, 500)


def test_holdings_endpoint_requires_auth(client):
    resp = client.get("/api/holdings")
    assert resp.status_code == 401
    login(client)
    resp = client.get("/api/holdings")
    assert resp.status_code in (200, 400, 500)

def test_portfolio_parses_net_positions(client, monkeypatch):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    class DummyBroker:
        def __init__(self, *a, **k):
            pass
        def get_positions(self):
            return {"netPositions": [{"symbol": "ABC"}]}

    monkeypatch.setattr(app_module, "broker_api", lambda acc: DummyBroker())

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        acc = Account(user_id=user.id, broker="fyers", client_id="F1", credentials={"access_token": "x"})
        db.session.add(acc)
        db.session.commit()

    resp = client.get("/api/portfolio/F1")
    assert resp.status_code == 200
    assert resp.get_json() == [{"symbol": "ABC"}]


def test_active_children_scoped_to_user(client):
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account
    with app.app_context():
        user1 = User.query.filter_by(email="test@example.com").first()
        user2 = User(email="other@example.com")
        user2.set_password("x")
        db.session.add(user2)
        db.session.commit()
        master = Account(user_id=user1.id, role="master", client_id="M1")
        c1 = Account(user_id=user1.id, role="child", client_id="C1", linked_master_id="M1", copy_status="On")
        c2 = Account(user_id=user2.id, role="child", client_id="C2", linked_master_id="M1", copy_status="On")
        db.session.add_all([master, c1, c2])
        db.session.commit()

        from helpers import active_children_for_master

        children = active_children_for_master(master)
        ids = {c.client_id for c in children}
        assert ids == {"C1"}

def test_active_children_case_insensitive_status(client):
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account
    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        master = Account(user_id=user.id, role="master", client_id="M2")
        c1 = Account(user_id=user.id, role="child", client_id="C3", linked_master_id="M2", copy_status="ON")
        db.session.add_all([master, c1])
        db.session.commit()

        from helpers import active_children_for_master

        children = active_children_for_master(master)
        ids = {c.client_id for c in children}
        assert ids == {"C3"}


def test_order_mappings_endpoint_requires_auth(client):
    resp = client.get("/api/order-mappings")
    assert resp.status_code == 401
    login(client)
    resp = client.get("/api/order-mappings")
    assert resp.status_code == 200


def test_child_orders_endpoint_requires_auth(client):
    resp = client.get("/api/child-orders")
    assert resp.status_code == 401
    login(client)
    resp = client.get("/api/child-orders")
    assert resp.status_code == 200

def test_save_account_persists_username(client):
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account
    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        data = {
            "broker": "fyers",
            "client_id": "F1",
            "username": "demo",
            "credentials": {"access_token": "x"}
        }
        app_module.save_account_to_user(user.email, data)
        acc = Account.query.filter_by(client_id="F1", user_id=user.id).first()
        assert acc.username == "demo"

def test_poll_and_copy_trades_cross_broker(client, monkeypatch):
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    import brokers

    class DummyMasterBroker(brokers.base.BrokerBase):
        def place_order(self, *a, **k):
            pass
        def get_order_list(self):
            return [{
                "orderId": "1",
                "status": "COMPLETE",
                "filledQuantity": 1,
                "price": 100,
                "tradingSymbol": "TESTSYM",
                "transactionType": "BUY"
            }]
        def get_positions(self):
            return []
        def cancel_order(self, order_id):
            pass

    placed = []
    class DummyChildBroker(brokers.base.BrokerBase):
        def place_order(self, **kwargs):
            placed.append(kwargs)
            return {"status": "success", "order_id": "child1"}
        def get_order_list(self):
            return []
        def get_positions(self):
            return []
        def cancel_order(self, order_id):
            pass

    def fake_get_broker_class(name):
        if name == "master_broker":
            return DummyMasterBroker
        return DummyChildBroker

    monkeypatch.setattr(brokers.factory, "get_broker_class", fake_get_broker_class)
    monkeypatch.setattr(app_module, "get_broker_class", fake_get_broker_class)
    assert app_module.poll_and_copy_trades.__globals__["get_broker_class"] is fake_get_broker_class
    monkeypatch.setattr(app_module, "save_log", lambda *a, **k: None)
    monkeypatch.setattr(app_module, "save_order_mapping", lambda *a, **k: None)
    monkeypatch.setattr(app_module, "record_trade", lambda *a, **k: None)

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        master = Account(user_id=user.id, role="master", broker="master_broker", client_id="M", credentials={"access_token": "x"})
        child = Account(user_id=user.id, role="child", broker="child_broker", client_id="C", linked_master_id="M", copy_status="On", credentials={"access_token": "y"}, last_copied_trade_id="0")
        db.session.add_all([master, child])
        db.session.commit()

        monkeypatch.setitem(brokers.symbol_map.SYMBOL_MAP, "TESTSYM", {
            "master_broker": {"trading_symbol": "TESTSYM"},
            "child_broker": {"tradingsymbol": "TESTSYM"}
        })

        app_module.poll_and_copy_trades()

        assert placed
        db.session.refresh(child)
        assert child.last_copied_trade_id == "1"

def test_poll_and_copy_trades_token_lookup(client, monkeypatch):
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    import brokers

    placed = []

    class DummyZerodha(brokers.base.BrokerBase):
        def __init__(self, *a, **k):
            pass

        def place_order(self, **kwargs):
            placed.append(kwargs)
            return {"status": "success", "order_id": "child1"}

        def get_order_list(self):
            return [{
                "orderId": "2",
                "status": "COMPLETE",
                "filledQuantity": 1,
                "price": 50,
                "transactionType": "BUY",
                "instrument_token": "926241"
            }]

        def get_positions(self):
            return []

        def cancel_order(self, order_id):
            pass

    def fake_get_broker_class(name):
        return DummyZerodha

    monkeypatch.setattr(brokers.factory, "get_broker_class", fake_get_broker_class)
    monkeypatch.setattr(app_module, "get_broker_class", fake_get_broker_class)
    monkeypatch.setattr(app_module, "save_log", lambda *a, **k: None)
    monkeypatch.setattr(app_module, "save_order_mapping", lambda *a, **k: None)
    monkeypatch.setattr(app_module, "record_trade", lambda *a, **k: None)

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        master = Account(user_id=user.id, role="master", broker="zerodha", client_id="ZM", credentials={"access_token": "x"})
        child = Account(user_id=user.id, role="child", broker="zerodha", client_id="ZC", linked_master_id="ZM", copy_status="On", credentials={"access_token": "y"}, last_copied_trade_id="0")
        db.session.add_all([master, child])
        db.session.commit()

        monkeypatch.setitem(brokers.symbol_map.SYMBOL_MAP, "IDEA", {
            "zerodha": {"trading_symbol": "IDEA", "token": "926241"}
        })

        app_module.poll_and_copy_trades()

        assert placed
        db.session.refresh(child)
        assert child.last_copied_trade_id == "2"


def test_poll_and_copy_trades_dhan_invalid_syntax_skipped(client, monkeypatch):
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    import brokers

    class DummyChildBroker(brokers.base.BrokerBase):
        def place_order(self, **kwargs):
            pass
        def get_order_list(self):
            return []
        def get_positions(self):
            return []
        def cancel_order(self, order_id):
            pass

    def failing_get_broker_class(name):
        if name == "dhan":
            raise ImportError(
                "Failed to load broker 'dhan': invalid syntax (dhan.py, line 96)"
            )
        return DummyChildBroker

    monkeypatch.setattr(brokers.factory, "get_broker_class", failing_get_broker_class)
    monkeypatch.setattr(app_module, "get_broker_class", failing_get_broker_class)
    monkeypatch.setattr(app_module, "save_log", lambda *a, **k: None)
    monkeypatch.setattr(app_module, "save_order_mapping", lambda *a, **k: None)
    monkeypatch.setattr(app_module, "record_trade", lambda *a, **k: None)

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        master = Account(
            user_id=user.id,
            role="master",
            broker="dhan",
            client_id="DM",
            credentials={"access_token": "x"},
            status="Error",
            copy_status="Off",
        )
        child = Account(
            user_id=user.id,
            role="child",
            broker="child_broker",
            client_id="DC",
            linked_master_id="DM",
            copy_status="On",
            credentials={"access_token": "y"},
            last_copied_trade_id="0",
        )
        db.session.add_all([master, child])
        db.session.commit()

        # Seed a couple of old error logs that should be cleared
        for _ in range(2):
            log = app_module.SystemLog(
                timestamp=datetime.utcnow(),
                level="ERROR",
                message="Failed to initialize master API (dhan): invalid syntax",
                user_id=str(user.id),
                details=json.dumps({"client_id": "DM"}),
            )
            db.session.add(log)
        db.session.commit()

        app_module.poll_and_copy_trades()

        db.session.refresh(master)
        assert master.status == "Connected"
        assert master.copy_status == "On"
        logs = app_module.SystemLog.query.all()
        assert not any(
            "invalid syntax" in (log.message or "").lower() for log in logs
        )


def test_opening_balance_cache(monkeypatch):
    app = app_module.app

    class DummyBroker:
        def get_opening_balance(self):
            calls.append(1)
            return 42

    calls = []
    monkeypatch.setattr(app_module, "broker_api", lambda acc: DummyBroker())
    app_module.OPENING_BALANCE_CACHE.clear()

    acc = {"client_id": "A1", "broker": "dummy", "credentials": {}}

    bal1 = app_module.get_opening_balance_for_account(acc)
    bal2 = app_module.get_opening_balance_for_account(acc)

    assert bal1 == 42
    assert bal2 == 42
    assert len(calls) == 1

def test_profile_image_stored_in_db(client):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User

    img_data = b"imgdata"
    resp = client.post(
        "/users",
        data={
            "action": "save_profile",
            "first_name": "A",
            "last_name": "B",
            "profile_image": (io.BytesIO(img_data), "profile.png"),
        },
        content_type="multipart/form-data",
    )
    assert resp.status_code == 200

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        assert user.profile_image.startswith("data:")

def test_add_account_stores_all_credentials(client, monkeypatch):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    class DummyBroker:
        def __init__(self, *a, **k):
            self.access_token = "dummy_token"
        def check_token_valid(self):
            return True

    monkeypatch.setattr(app_module, "get_broker_class", lambda name: DummyBroker)

    data = {
        "broker": "finvasia",
        "client_id": "FIN123",
        "username": "finuser",
        "password": "p",
        "totp_secret": "t",
        "vendor_code": "v",
        "api_key": "a",
        "imei": "i"
    }

    resp = client.post("/api/add-account", json=data)
    assert resp.status_code == 200

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        acc = Account.query.filter_by(user_id=user.id, client_id="FIN123").first()
        assert acc is not None
        expected = {
            "password": "p",
            "totp_secret": "t",
            "vendor_code": "v",
            "api_key": "a",
            "imei": "i",
            "access_token": "dummy_token"
        }
        assert acc.credentials == expected

def test_add_zerodha_sets_token_time(client, monkeypatch):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    class DummyBroker:
        def __init__(self, *a, **k):
            self.access_token = "tok"
            self.token_time = 1111
        def check_token_valid(self):
            return True

    monkeypatch.setattr(app_module, "get_broker_class", lambda name: DummyBroker)

    data = {
        "broker": "zerodha",
        "client_id": "Z1",
        "username": "user",
        "access_token": "tok",
        "api_key": "k",
        "api_secret": "s",
    }

    resp = client.post("/api/add-account", json=data)
    assert resp.status_code == 200

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        acc = Account.query.filter_by(user_id=user.id, client_id="Z1").first()
        assert isinstance(acc.credentials.get("token_time"), (int, float))

def test_reconnect_uses_stored_credentials(client, monkeypatch):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    captured = {}
    class DummyBroker:
        def __init__(self, *a, **k):
            captured["args"] = a
            captured["kwargs"] = k
            self.access_token = "newtoken"
        def check_token_valid(self):
            return True

    monkeypatch.setattr(app_module, "get_broker_class", lambda name: DummyBroker)

    data = {
        "broker": "finvasia",
        "client_id": "FIN124",
        "username": "finuser",
        "password": "p",
        "totp_secret": "t",
        "vendor_code": "v",
        "api_key": "a",
        "imei": "i"
    }

    resp = client.post("/api/add-account", json=data)
    assert resp.status_code == 200

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        acc = Account.query.filter_by(user_id=user.id, client_id="FIN124").first()
        acc.status = "Failed"
        db.session.commit()

    resp = client.post("/api/reconnect-account", json={"client_id": "FIN124"})
    assert resp.status_code == 200

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        acc = Account.query.filter_by(user_id=user.id, client_id="FIN124").first()
        assert acc.status == "Connected"
        assert acc.credentials["access_token"] == "newtoken"
        assert captured["kwargs"]["password"] == "p"
        assert captured["kwargs"]["totp_secret"] == "t"
        assert captured["kwargs"]["vendor_code"] == "v"
        assert captured["kwargs"]["api_key"] == "a"
        assert captured["kwargs"]["imei"] == "i"
        count = Account.query.filter_by(user_id=user.id, client_id="FIN124").count()
        assert count == 1


def test_reconnect_totp_error_propagated(client, monkeypatch):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    from brokers import zerodha
    import brokers

    class FakeKite:
        def __init__(self, *a, **k):
            pass
        def set_access_token(self, *a, **k):
            pass
        def generate_session(self, *a, **k):
            return {"access_token": "tok"}

    class FakeTOTP:
        def __init__(self, *a, **k):
            pass
        def now(self):
            return "000000"

    class FakeResp:
        def __init__(self, url, data):
            self.url = url
            self._data = data
            self.text = json.dumps(data)
        def json(self):
            return self._data

    class FakeSession:
        def mount(self, *a, **k):
            pass
        def post(self, url, *a, **k):
            if url.endswith("/login"):
                return FakeResp(url, {"status": "success", "data": {"request_id": "1"}})
            return FakeResp(url, {"status": "error", "message": "Invalid TOTP"})

    monkeypatch.setattr(zerodha, "KiteConnect", FakeKite)
    monkeypatch.setattr(zerodha.pyotp, "TOTP", FakeTOTP)
    monkeypatch.setattr(zerodha.requests, "Session", lambda: FakeSession())
    monkeypatch.setattr(app_module, "get_broker_class", lambda name: zerodha.ZerodhaBroker)
    monkeypatch.setattr(brokers.factory, "get_broker_class", lambda name: zerodha.ZerodhaBroker)

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        acc = Account(
            user_id=user.id,
            broker="zerodha",
            client_id="ZT1",
            credentials={
                "api_key": "k",
                "api_secret": "s",
                "password": "p",
                "totp_secret": "t",
            },
        )
        db.session.add(acc)
        db.session.commit()

    resp = client.post("/api/reconnect-account", json={"client_id": "ZT1"})
    assert resp.status_code == 500
    assert "Invalid TOTP" in resp.get_json()["error"]


def test_check_auto_logins_reconnects_all(client, monkeypatch):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    class DummyBroker:
        def __init__(self, *a, **k):
            self.access_token = "tok"
        def check_token_valid(self):
            return True

    monkeypatch.setattr(app_module, "get_broker_class", lambda name: DummyBroker)

    data1 = {
        "broker": "finvasia", "client_id": "A1", "username": "u",
        "password": "p", "totp_secret": "t", "vendor_code": "v",
        "api_key": "a", "imei": "i"
    }
    data2 = {
        "broker": "finvasia", "client_id": "A2", "username": "u",
        "password": "p", "totp_secret": "t", "vendor_code": "v",
        "api_key": "a", "imei": "i"
    }

    assert client.post("/api/add-account", json=data1).status_code == 200
    assert client.post("/api/add-account", json=data2).status_code == 200

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        Account.query.filter_by(user_id=user.id, client_id="A1").first().status = "Failed"
        Account.query.filter_by(user_id=user.id, client_id="A2").first().status = "Failed"
        db.session.commit()

    resp = client.post("/api/check-auto-logins")
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data["results"]) == 2
    statuses = {r["client_id"]: r["status"] for r in data["results"]}
    assert statuses["A1"] == "Connected"
    assert statuses["A2"] == "Connected"

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        a1 = Account.query.filter_by(user_id=user.id, client_id="A1").first()
        a2 = Account.query.filter_by(user_id=user.id, client_id="A2").first()
        assert a1.status == "Connected"
        assert a2.status == "Connected"

def test_exit_all_positions_uses_position_product(client, monkeypatch):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    placed = {}

    import brokers

    class DummyBroker(brokers.base.BrokerBase):
        def __init__(self, *a, **k):
            pass
        def get_positions(self):
            return {"positions": [{"tradingSymbol": "SBIN", "netQty": 2, "productType": "CNC"}]}
        def place_order(self, **kwargs):
            placed.update(kwargs)
            return {"status": "success"}
        def get_order_list(self):
            return []
        def cancel_order(self, order_id):
            pass

    monkeypatch.setattr(app_module, "broker_api", lambda acc: DummyBroker("c", "t"))
    monkeypatch.setattr(app_module, "save_log", lambda *a, **k: None)

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        acc = Account(user_id=user.id, role="child", broker="dhan", client_id="C1", credentials={"access_token": "x"})
        db.session.add(acc)
        db.session.commit()

        results = app_module.exit_all_positions_for_account(acc)

    assert placed.get("product_type") == "CNC"
    assert results[0]["status"] == "SUCCESS"


@pytest.mark.parametrize("broker", ["dhan", "aliceblue", "zerodha", "fyers", "finvasia"])
def test_exit_all_positions_handles_netqty_and_quantity(client, monkeypatch, broker):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    placed = {}

    import brokers

    class DummyBroker(brokers.base.BrokerBase):
        def __init__(self, *a, **k):
            pass
        def get_positions(self):
            return {
                "positions": [
                    {"tradingSymbol": "SBIN", "netqty": 1},
                    {"tradingSymbol": "TATAMOTORS", "quantity": 2},
                ]
            }
        def place_order(self, **kwargs):
            placed.setdefault("orders", []).append(kwargs)
            return {"status": "success"}
        def get_order_list(self):
            return []
        def cancel_order(self, order_id):
            pass

    monkeypatch.setattr(app_module, "broker_api", lambda acc: DummyBroker("c", "t"))
    monkeypatch.setattr(app_module, "save_log", lambda *a, **k: None)

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        acc = Account(user_id=user.id, role="child", broker=broker, client_id="CX", credentials={"access_token": "x"})
        db.session.add(acc)
        db.session.commit()

        results = app_module.exit_all_positions_for_account(acc)

    assert placed.get("orders")
    statuses = [r["status"] for r in results]
    assert statuses and all(s == "SUCCESS" for s in statuses)


def test_exit_all_positions_handles_nested_positions(client, monkeypatch):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    placed = {}

    import brokers

    class DummyBroker(brokers.base.BrokerBase):
        def __init__(self, *a, **k):
            pass
        def get_positions(self):
            return {"data": {"payload": {"netPositions": [{"tradingSymbol": "SBIN", "netQty": 1}]}}}
        def place_order(self, **kwargs):
            placed.update(kwargs)
            return {"status": "success"}
        def get_order_list(self):
            return []
        def cancel_order(self, order_id):
            pass

    monkeypatch.setattr(app_module, "broker_api", lambda acc: DummyBroker("c", "t"))
    monkeypatch.setattr(app_module, "save_log", lambda *a, **k: None)

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        acc = Account(user_id=user.id, role="child", broker="dhan", client_id="CX", credentials={"access_token": "x"})
        db.session.add(acc)
        db.session.commit()

        results = app_module.exit_all_positions_for_account(acc)

    assert placed.get("tradingsymbol") == "SBIN"
    assert results and all(r["status"] == "SUCCESS" for r in results)


def test_exit_all_positions_handles_wrapped_position_list(client, monkeypatch):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    placed = {}

    import brokers

    class DummyBroker(brokers.base.BrokerBase):
        def __init__(self, *a, **k):
            pass
        def get_positions(self):
            return {"data": {"payload": [{"netPositions": [{"tradingSymbol": "SBIN", "netQty": 1}]}]}}
        def place_order(self, **kwargs):
            placed.update(kwargs)
            return {"status": "success"}
        def get_order_list(self):
            return []
        def cancel_order(self, order_id):
            pass

    monkeypatch.setattr(app_module, "broker_api", lambda acc: DummyBroker("c", "t"))
    monkeypatch.setattr(app_module, "save_log", lambda *a, **k: None)

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        acc = Account(user_id=user.id, role="child", broker="dhan", client_id="CX", credentials={"access_token": "x"})
        db.session.add(acc)
        db.session.commit()

        results = app_module.exit_all_positions_for_account(acc)

    assert placed.get("tradingsymbol") == "SBIN"
    assert results and all(r["status"] == "SUCCESS" for r in results)


@pytest.mark.parametrize("broker", ["dhan", "aliceblue", "zerodha", "fyers", "finvasia"])
def test_exit_child_positions_endpoint(client, monkeypatch, broker):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    placed = {}

    import brokers

    class DummyBroker(brokers.base.BrokerBase):
        def __init__(self, *a, **k):
            pass
        def get_positions(self):
            return {"positions": [{"tradingSymbol": "SBIN", "netQty": 2, "productType": "CNC"}]}
        def place_order(self, **kwargs):
            placed.update(kwargs)
            return {"status": "success"}
        def get_order_list(self):
            return []
        def cancel_order(self, order_id):
            pass

    monkeypatch.setattr(app_module, "broker_api", lambda acc: DummyBroker("c", "t"))
    monkeypatch.setattr(app_module, "save_log", lambda *a, **k: None)

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        acc = Account(user_id=user.id, role="child", broker=broker, client_id="C1", credentials={"access_token": "x"})
        db.session.add(acc)
        db.session.commit()

    resp = client.post("/api/exit-child-positions", json={"child_id": "C1"})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["exited"]
    if broker == "dhan":
        assert placed.get("product_type") == "CNC"
    else:
        assert placed.get("product") == "CNC"


@pytest.mark.parametrize("broker", ["dhan", "aliceblue", "zerodha", "fyers", "finvasia"])
def test_exit_all_children_endpoint(client, monkeypatch, broker):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    placed_orders = []

    import brokers

    class DummyBroker(brokers.base.BrokerBase):
        def __init__(self, *a, **k):
            pass
        def get_positions(self):
            return {"positions": [{"tradingSymbol": "SBIN", "netQty": 2, "productType": "CNC"}]}
        def place_order(self, **kwargs):
            placed_orders.append(kwargs)
            return {"status": "success"}
        def get_order_list(self):
            return []
        def cancel_order(self, order_id):
            pass

    monkeypatch.setattr(app_module, "broker_api", lambda acc: DummyBroker("c", "t"))
    monkeypatch.setattr(app_module, "save_log", lambda *a, **k: None)

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        master = Account(user_id=user.id, role="master", broker=broker, client_id="M1", credentials={"access_token": "x"})
        child = Account(user_id=user.id, role="child", broker=broker, client_id="C1", linked_master_id="M1", credentials={"access_token": "x"})
        db.session.add_all([master, child])
        db.session.commit()

    resp = client.post("/api/exit-all-children", json={"master_id": "M1"})
    assert resp.status_code == 200
    data = resp.get_json()
    assert "C1" in data["exited_children"]
    assert len(placed_orders) == 1
    if broker == "dhan":
        assert placed_orders[0].get("product_type") == "CNC"
    else:
        assert placed_orders[0].get("product") == "CNC"


def test_account_to_dict_rolls_back_on_log_error(client, monkeypatch):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        acc = Account(user_id=user.id, role="child", broker="dhan", client_id="X1")
        db.session.add(acc)
        db.session.commit()

        class FailingQuery:
            def filter_by(self, **kwargs):
                raise Exception("boom")

        monkeypatch.setattr(app_module.SystemLog, "query", FailingQuery())

        result = app_module._account_to_dict(acc)
        assert result["id"] == acc.id
        # Session should still be usable after failure
        assert Account.query.count() >= 1

def test_broker_api_does_not_pass_duplicate_client_id(monkeypatch):
    app = app_module.app

    captured = {}

    class DummyBroker:
        def __init__(self, client_id, access_token=None, **kwargs):
            captured["client_id"] = client_id
            captured["kwargs"] = kwargs

    monkeypatch.setattr(app_module, "get_broker_class", lambda name: DummyBroker)

    acc = {
        "broker": "zerodha",
        "client_id": "Z1",
        "credentials": {
            "client_id": "Z1",
            "access_token": "tok",
            "api_key": "k",
            "api_secret": "s",
        },
    }

    broker = app_module.broker_api(acc)
    assert isinstance(broker, DummyBroker)
    assert captured["client_id"] == "Z1"
    assert "client_id" not in captured["kwargs"]


def test_get_opening_balance_with_client_id_in_credentials(monkeypatch):
    class DummyBroker:
        def __init__(self, *a, **k):
            pass

        def get_opening_balance(self):
            return 99.0

    monkeypatch.setattr(app_module, "get_broker_class", lambda name: DummyBroker)
    app_module.OPENING_BALANCE_CACHE.clear()

    acc = {
        "broker": "fyers",
        "client_id": "F1",
        "credentials": {"client_id": "F1", "access_token": "tok"},
    }

    bal = app_module.get_opening_balance_for_account(acc)
    assert bal == 99.0


def test_create_strategy_endpoint(client):
    login(client)
    # Missing required field
    resp = client.post("/api/strategies", json={"name": "S1"})
    assert resp.status_code == 400

    app = app_module.app
    db = app_module.db
    Account = app_module.Account
    User = app_module.User
    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        acc = Account(user_id=user.id, broker="finvasia", client_id="FIN1")
        db.session.add(acc)
        db.session.commit()
        acc_id = acc.id

    data = {
        "name": "S1",
        "asset_class": "Stocks",
        "style": "Systematic",
        "allow_auto_submit": True,
        "allow_live_trading": True,
        "allow_any_ticker": True,
        "allowed_tickers": "AAPL,MSFT",
        "notification_emails": "a@example.com",
        "notify_failures_only": False,
        "account_id": acc_id,
        "signal_source": "TradingView",
        "schedule": "0 9 * * *",
        "risk_max_positions": 3,
        "risk_max_allocation": 10.5,
        "webhook_secret": "secret",
        "track_performance": True,
        "log_retention_days": 7,
    }
    resp = client.post("/api/strategies", json=data)
    assert resp.status_code == 200
    out = resp.get_json()
    assert out.get("id")

    app = app_module.app
    db = app_module.db
    Strategy = app_module.Strategy
    User = app_module.User
    with app.app_context():
        s = Strategy.query.get(out["id"])
        assert s is not None
        assert s.name == "S1"
        user = User.query.filter_by(email="test@example.com").first()
        assert s.user_id == user.id
        assert s.account_id == acc_id
        assert s.signal_source == "TradingView"
        assert s.schedule == "0 9 * * *"
        assert s.risk_max_positions == 3
        assert s.risk_max_allocation == 10.5
        assert s.webhook_secret == "secret"
        assert s.track_performance is True
        assert s.log_retention_days == 7


def test_create_strategy_invalid_account(client):
    login(client)
    resp = client.post(
        "/api/strategies",
        json={"name": "Bad", "asset_class": "Stocks", "style": "Systematic", "account_id": 999},
    )
    assert resp.status_code == 400

def test_create_strategy_auto_secret(client):
    login(client)
    resp = client.post(
        "/api/strategies",
        json={"name": "AutoSec", "asset_class": "Stocks", "style": "Systematic"},
    )
    assert resp.status_code == 200
    sid = resp.get_json()["id"]
    app = app_module.app
    db = app_module.db
    Strategy = app_module.Strategy
    with app.app_context():
        s = db.session.get(Strategy, sid)
        assert s.webhook_secret

def test_strategy_list_and_delete(client):
    login(client)
    resp = client.get("/api/strategies")
    assert resp.status_code == 200
    assert resp.get_json() == []

    data = {
        "name": "ListTest",
        "asset_class": "Stocks",
        "style": "Systematic"
    }
    create = client.post("/api/strategies", json=data)
    sid = create.get_json()["id"]

    app = app_module.app
    db = app_module.db
    Strategy = app_module.Strategy
    User = app_module.User
    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        s = Strategy.query.get(sid)
        assert s.user_id == user.id


    resp = client.get("/api/strategies")
    assert resp.status_code == 200
    strategies = resp.get_json()
    assert any(s["id"] == sid for s in strategies)

    resp = client.delete(f"/api/strategies/{sid}")
    assert resp.status_code == 200

    resp = client.get("/api/strategies")
    ids = [s["id"] for s in resp.get_json()]
    assert sid not in ids

def test_strategy_user_scoping(client):
    login(client)
    data = {
        "name": "U1",
        "asset_class": "Stocks",
        "style": "Systematic"
    }
    resp = client.post("/api/strategies", json=data)
    sid1 = resp.get_json()["id"]

    # create second user
    app = app_module.app
    db = app_module.db
    User = app_module.User
    with app.app_context():
        u2 = User(email="other@example.com")
        u2.set_password("x")
        db.session.add(u2)
        db.session.commit()
    client.get("/logout")
    client.post("/login", data={"email": "other@example.com", "password": "x"})

    # should not see user1 strategy
    resp = client.get("/api/strategies")
    assert resp.get_json() == []

    data2 = {"name": "U2", "asset_class": "Stocks", "style": "Systematic"}
    sid2 = client.post("/api/strategies", json=data2).get_json()["id"]

    # cannot access other user"s strategy
    assert client.get(f"/api/strategies/{sid1}").status_code == 404
    assert client.put(f"/api/strategies/{sid1}", json={"name": "x"}).status_code == 404
    assert client.delete(f"/api/strategies/{sid1}").status_code == 404

    # logout and login as user1 again
    client.get("/logout")
    login(client)
    resp = client.get("/api/strategies")
    ids = [s["id"] for s in resp.get_json()]
    assert sid1 in ids and sid2 not in ids

def test_strategy_activation_and_monitoring(client):
    login(client)
    data = {"name": "Act", "asset_class": "Stocks", "style": "Systematic"}
    sid = client.post("/api/strategies", json=data).get_json()["id"]

    resp = client.get(f"/api/strategies/{sid}")
    assert resp.get_json()["is_active"] is False

    assert client.post(f"/api/strategies/{sid}/activate").status_code == 200
    assert client.get(f"/api/strategies/{sid}").get_json()["is_active"] is True

    assert client.post(f"/api/strategies/{sid}/ping").status_code == 200
    assert client.get(f"/api/strategies/{sid}").get_json()["last_run_at"] is not None

    assert client.post(f"/api/strategies/{sid}/deactivate").status_code == 200
    assert client.get(f"/api/strategies/{sid}").get_json()["is_active"] is False


def test_strategy_activation_scoped(client):
    login(client)
    sid = client.post(
        "/api/strategies",
        json={"name": "Priv", "asset_class": "Stocks", "style": "Systematic"},
    ).get_json()["id"]

    app = app_module.app
    db = app_module.db
    User = app_module.User
    with app.app_context():
        u2 = User(email="scoped@example.com")
        u2.set_password("x")
        db.session.add(u2)
        db.session.commit()
    client.get("/logout")
    client.post("/login", data={"email": "scoped@example.com", "password": "x"})

    assert client.post(f"/api/strategies/{sid}/activate").status_code == 404

def test_strategy_logs_endpoint(client):
    login(client)
    sid = client.post(
        "/api/strategies",
        json={"name": "LogTest", "asset_class": "Stocks", "style": "Systematic"},
    ).get_json()["id"]

    resp = client.post(
        f"/api/strategies/{sid}/logs",
        json={"level": "INFO", "message": "Run completed", "performance": {"pnl": 5}},
    )
    assert resp.status_code == 200
    lid = resp.get_json()["id"]

    resp = client.get(f"/api/strategies/{sid}/logs")
    assert resp.status_code == 200
    logs = resp.get_json()
    assert any(l["id"] == lid for l in logs)

def test_webhook_secret_enforced(client, monkeypatch):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account
    Strategy = app_module.Strategy
    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        user.webhook_token = "tok1"
        acc = Account(user_id=user.id, broker="dhan", client_id="C1", credentials={"access_token": "x"})
        db.session.add(acc)
        strategy = Strategy(
            user_id=user.id,
            account_id=acc.id,
            name="S1",
            asset_class="Stocks",
            style="Systematic",
            webhook_secret="sec123",
            is_active=True,
        )
        db.session.add(strategy)
        db.session.commit()
    import brokers
    class DummyBroker(brokers.base.BrokerBase):
        BUY="BUY"; SELL="SELL"; MARKET="MARKET"; INTRA="INTRADAY"; NSE="NSE"
        def place_order(self, *a, **k):
            return {"status": "success", "order_id": "1"}
        def get_order_list(self):
            return []
        def get_positions(self):
            return []
        def cancel_order(self, order_id):
            pass
    monkeypatch.setattr(app_module, "get_broker_class", lambda name: DummyBroker)
    monkeypatch.setattr(app_module, "get_symbol_for_broker", lambda s, b: {"security_id": "1"})
    monkeypatch.setattr(app_module, "record_trade", lambda *a, **k: None)
    resp = client.post(f"/webhook/tok1", json={"symbol": "A", "action": "BUY", "quantity": 1})
    assert resp.status_code == 403
    resp = client.post(
        f"/webhook/tok1",
        json={"symbol": "A", "action": "BUY", "quantity": 1},
        headers={"X-Webhook-Secret": "sec123"},
    )
    assert resp.status_code == 200


def test_webhook_accepts_ticker_field(client, monkeypatch):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account
    Strategy = app_module.Strategy
    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        user.webhook_token = "tok1a"
        acc = Account(user_id=user.id, broker="dhan", client_id="C1a", credentials={"access_token": "x"})
        db.session.add(acc)
        strategy = Strategy(
            user_id=user.id,
            account_id=acc.id,
            name="S1a",
            asset_class="Stocks",
            style="Systematic",
            webhook_secret="sec123a",
            is_active=True,
        )
        db.session.add(strategy)
        db.session.commit()
    import brokers
    class DummyBroker(brokers.base.BrokerBase):
        BUY="BUY"; SELL="SELL"; MARKET="MARKET"; INTRA="INTRADAY"; NSE="NSE"
        def place_order(self, *a, **k):
            return {"status": "success", "order_id": "1"}
        def get_order_list(self):
            return []
        def get_positions(self):
            return []
        def cancel_order(self, order_id):
            pass
    monkeypatch.setattr(app_module, "get_broker_class", lambda name: DummyBroker)
    monkeypatch.setattr(app_module, "get_symbol_for_broker", lambda s, b: {"security_id": "1"})
    monkeypatch.setattr(app_module, "record_trade", lambda *a, **k: None)
    resp = client.post(f"/webhook/tok1a", json={"ticker": "A", "action": "BUY", "quantity": 1})
    assert resp.status_code == 403
    resp = client.post(
        f"/webhook/tok1a",
        json={"ticker": "A", "action": "BUY", "quantity": 1},
        headers={"X-Webhook-Secret": "sec123a"},
    )
    assert resp.status_code == 200


def test_webhook_tradingview_payload(client, monkeypatch):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account
    Strategy = app_module.Strategy
    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        user.webhook_token = "toktv"
        acc = Account(user_id=user.id, broker="dhan", client_id="CTV", credentials={"access_token": "x"})
        db.session.add(acc)
        strategy = Strategy(
            user_id=user.id,
            account_id=acc.id,
            name="STV",
            asset_class="Stocks",
            style="Systematic",
            webhook_secret="secrettv",
            is_active=True,
        )
        db.session.add(strategy)
        db.session.commit()
    import brokers
    class DummyBroker(brokers.base.BrokerBase):
        BUY = "BUY"; SELL = "SELL"; MARKET = "MARKET"; INTRA = "INTRADAY"; NSE = "NSE"
        def place_order(self, *a, **k):
            return {"status": "success", "order_id": "1"}
        def get_order_list(self):
            return []
        def get_positions(self):
            return []
        def cancel_order(self, order_id):
            pass
    monkeypatch.setattr(app_module, "get_broker_class", lambda name: DummyBroker)
    monkeypatch.setattr(app_module, "get_symbol_for_broker", lambda s, b: {"security_id": "1"})
    monkeypatch.setattr(app_module, "record_trade", lambda *a, **k: None)
    payload = {
        "strategyName": "new",
        "exchange": "NSE",
        "transactionType": "le",
        "orderType": "market",
        "orderValidity": "day",
        "productType": "intraday",
        "masterAccounts": ["36"],
        "orderQty": 5,
        "tradingSymbols": ["IDEA"],
    }
    resp = client.post(f"/webhook/toktv", json=payload)
    assert resp.status_code == 403
    resp = client.post(
        f"/webhook/toktv",
        json=payload,
        headers={"X-Webhook-Secret": "secrettv"},
    )
    assert resp.status_code == 200

def test_risk_limit_max_positions(client, monkeypatch):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account
    Strategy = app_module.Strategy
    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        user.webhook_token = "tok2"
        acc = Account(user_id=user.id, broker="dhan", client_id="C2", credentials={"access_token": "x"})
        db.session.add(acc)
        strategy = Strategy(
            user_id=user.id,
            account_id=acc.id,
            name="S2",
            asset_class="Stocks",
            style="Systematic",
            webhook_secret="sec2",
            is_active=True,
            risk_max_positions=1,
        )
        db.session.add(strategy)
        db.session.commit()
    import brokers
    class DummyBroker(brokers.base.BrokerBase):
        BUY="BUY"; SELL="SELL"; MARKET="MARKET"; INTRA="INTRADAY"; NSE="NSE"
        def __init__(self, *a, **k):
            pass
        def place_order(self, *a, **k):
            return {"status": "success", "order_id": "1"}
        def get_order_list(self):
            return []
        def get_positions(self):
            return [{"tradingSymbol": "X", "netQty": 1, "ltp": 10}]
        def cancel_order(self, order_id):
            pass
    monkeypatch.setattr(app_module, "get_broker_class", lambda name: DummyBroker)
    monkeypatch.setattr(app_module, "get_symbol_for_broker", lambda s, b: {"security_id": "1"})
    monkeypatch.setattr(app_module, "record_trade", lambda *a, **k: None)
    resp = client.post(
        "/webhook/tok2",
        json={"symbol": "A", "action": "BUY", "quantity": 1},
        headers={"X-Webhook-Secret": "sec2"},
    )
    assert resp.status_code == 403


def test_schedule_enforced(client, monkeypatch):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account
    Strategy = app_module.Strategy
    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        user.webhook_token = "tok3"
        acc = Account(user_id=user.id, broker="dhan", client_id="C3", credentials={"access_token": "x"})
        db.session.add(acc)
        strategy = Strategy(
            user_id=user.id,
            account_id=acc.id,
            name="S3",
            asset_class="Stocks",
            style="Systematic",
            webhook_secret="sec3",
            is_active=True,
            schedule="00:00-00:01",
        )
        db.session.add(strategy)
        db.session.commit()
    import brokers
    class DummyBroker(brokers.base.BrokerBase):
        BUY="BUY"; SELL="SELL"; MARKET="MARKET"; INTRA="INTRADAY"; NSE="NSE"
        def place_order(self, *a, **k):
            return {"status": "success", "order_id": "1"}
        def get_order_list(self):
            return []
        def get_positions(self):
            return []
        def cancel_order(self, order_id):
            pass
    monkeypatch.setattr(app_module, "get_broker_class", lambda name: DummyBroker)
    monkeypatch.setattr(app_module, "get_symbol_for_broker", lambda s, b: {"security_id": "1"})
    monkeypatch.setattr(app_module, "record_trade", lambda *a, **k: None)
    resp = client.post(
        "/webhook/tok3",
        json={"symbol": "A", "action": "BUY", "quantity": 1},
        headers={"X-Webhook-Secret": "sec3"},
    )
    assert resp.status_code == 403


def test_strategy_performance_page(client):
    login(client)
    sid = client.post(
        "/api/strategies",
        json={"name": "Perf", "asset_class": "Stocks", "style": "Systematic", "track_performance": True},
    ).get_json()["id"]
    client.post(
        f"/api/strategies/{sid}/logs",
        json={"level": "INFO", "message": "Run", "performance": {"pnl": 1}},
    )
    resp = client.get(f"/strategy-performance/{sid}")
    assert resp.status_code == 200
    assert "Run" in resp.get_data(as_text=True)



def test_regenerate_webhook_secret(client):
    login(client)
    sid = client.post(
        "/api/strategies",
        json={"name": "Sec", "asset_class": "Stocks", "style": "Systematic", "webhook_secret": "old"},
    ).get_json()["id"]

    resp = client.post(f"/api/strategies/{sid}/regenerate-secret")
    assert resp.status_code == 200
    new_secret = resp.get_json()["webhook_secret"]
    assert new_secret and new_secret != "old"


def test_test_webhook_endpoint(client, monkeypatch):
    login(client)
    sid = client.post(
        "/api/strategies",
        json={"name": "TestW", "asset_class": "Stocks", "style": "Systematic"},
    ).get_json()["id"]

    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account
    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        user.webhook_token = "toktest"
        acc = Account(user_id=user.id, broker="dhan", client_id="C4", credentials={"access_token": "x"})
        db.session.add(acc)
        db.session.commit()
        strategy = db.session.get(app_module.Strategy, sid)
        strategy.account_id = acc.id
        strategy.webhook_secret = "secret"
        strategy.is_active = True
        db.session.commit()

    import brokers

    class DummyBroker(brokers.base.BrokerBase):
        BUY = "BUY"; SELL = "SELL"; MARKET = "MARKET"; INTRA = "INTRADAY"; NSE = "NSE"

        def place_order(self, *a, **k):
            return {"status": "success", "order_id": "1"}

        def get_order_list(self):
            return []

        def get_positions(self):
            return []

        def cancel_order(self, order_id):
            pass

    monkeypatch.setattr(app_module, "get_broker_class", lambda name: DummyBroker)
    monkeypatch.setattr(app_module, "get_symbol_for_broker", lambda s, b: {"security_id": "1"})
    monkeypatch.setattr(app_module, "record_trade", lambda *a, **k: None)

    resp = client.post(f"/api/strategies/{sid}/test-webhook")
    assert resp.status_code == 200
    out = resp.get_json()
    assert out["status"] == 200



def test_strategy_subscription_and_clone(client):
    login(client)
    sid = client.post(
        "/api/strategies",
        json={"name": "Orig", "asset_class": "Stocks", "style": "Systematic", "is_public": True},
    ).get_json()["id"]

    client.get("/logout")
    client.post("/signup", data={"email": "sub@example.com", "password": "x"})
    client.post("/login", data={"email": "sub@example.com", "password": "x"})

    resp = client.post(f"/api/strategies/{sid}/subscribe")
    assert resp.status_code == 200
    assert resp.get_json()["approved"]

    resp = client.post(f"/api/strategies/{sid}/clone")
    assert resp.status_code == 404  # not owner

    client.get("/logout")
    login(client)
    resp = client.post(f"/api/strategies/{sid}/clone")
    assert resp.status_code == 200
    new_id = resp.get_json()["id"]
    assert new_id != sid
    resp = client.get(f"/api/strategies/{sid}/subscribers")
    assert resp.status_code == 200
    subs = resp.get_json()
    assert any(s["subscriber_id"] for s in subs)


def test_webhook_token_generated_on_page_load(client):
    email = "tokgen@example.com"
    client.post("/signup", data={"email": email, "password": "x"})
    app = app_module.app
    db = app_module.db
    User = app_module.User
    with app.app_context():
        user = User.query.filter_by(email=email).first()
        assert not user.webhook_token
    resp = client.get("/demat-strategies")
    assert resp.status_code == 200
    with app.app_context():
        user = User.query.filter_by(email=email).first()
        assert user.webhook_token
