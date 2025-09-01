import pytest

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("SECRET_KEY", "test")
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("ADMIN_EMAIL", "a@a.com")
os.environ.setdefault("ADMIN_PASSWORD", "pass")
os.environ.setdefault("RUN_SCHEDULER", "0")

import tempfile
from importlib import reload

import app as app_module
import brokers

@pytest.fixture
def client():
    db_fd, db_path = tempfile.mkstemp()
    os.environ["DATABASE_URL"] = "sqlite:///" + db_path
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


def make_dummy_broker(broker_name, master_id, child_id, orders, placed):
    required_fields = {
        "zerodha": ["api_key", "api_secret", "access_token"],
        "aliceblue": ["api_key"],
        "dhan": ["access_token"],
        "finvasia": ["password", "totp_secret", "vendor_code", "api_key", "imei"],
        "fyers": ["access_token"],
        "flattrade": ["access_token"],
        "groww": ["access_token"],
    }

    class DummyBroker(brokers.base.BrokerBase):
        BROKER = broker_name

        def __init__(self, client_id, access_token=None, *args, **kwargs):
            for field in required_fields.get(broker_name, []):
                if field == "access_token":
                    token = access_token or kwargs.get(field)
                    assert token, f"{field} is required"
                    if access_token is None and field in kwargs:
                        access_token = kwargs[field]
                else:
                    assert field in kwargs, f"{field} is required"
            if access_token is None and "access_token" in kwargs:
                access_token = kwargs.pop("access_token")
            super().__init__(client_id, access_token or "", **kwargs)

        def place_order(self, **kwargs):
            if self.client_id == child_id:
                placed.append(kwargs)
                return {"status": "success", "order_id": "child1"}
            return {"status": "success"}

        def get_order_list(self):
            if self.client_id == master_id:
                return orders
            return []

        def get_positions(self):
            return []

        def cancel_order(self, order_id):
            pass

    return DummyBroker


BROKER_CASES = [
    (
        "zerodha",
        [
            {
                "orderId": "1",
                "status": "COMPLETE",
                "filledQuantity": 10,
                "price": 100,
                "tradingSymbol": "IDEA",
                "transactionType": "BUY",
            },
            {
                "orderId": "2",
                "status": "COMPLETE",
                "filledQuantity": 5,
                "price": 90,
                "tradingSymbol": "IDEA",
                "transactionType": "SELL",
            },
        ],
        {"api_key": "k", "api_secret": "s", "access_token": "x"},
        {"api_key": "k", "api_secret": "s", "access_token": "y"},
    ),
    (
        "aliceblue",
        [
            {
                "Nstordno": "1",
                "Status": "COMPLETE",
                "Filledqty": 10,
                "price": 100,
                "Tsym": "IDEA-EQ",
                "Trantype": "B",
            },
            {
                "Nstordno": "2",
                "Status": "COMPLETE",
                "Filledqty": 5,
                "price": 90,
                "Tsym": "IDEA-EQ",
                "Trantype": "S",
            },
        ],
        {"api_key": "k", "device_number": "d"},
        {"api_key": "k", "device_number": "d"},
    ),
    (
        "dhan",
        [
            {
                "orderId": "1",
                "status": "COMPLETE",
                "filledQuantity": 10,
                "price": 100,
                "tradingSymbol": "IDEA",
                "transactionType": "BUY",
            },
            {
                "orderId": "2",
                "status": "COMPLETE",
                "filledQuantity": 5,
                "price": 90,
                "tradingSymbol": "IDEA",
                "transactionType": "SELL",
            },
        ],
        {"access_token": "x"},
        {"access_token": "y"},
    ),
    (
        "finvasia",
        [
            {
                "norenordno": "1",
                "status": "COMPLETE",
                "fillshares": 10,
                "price": 100,
                "tsym": "IDEA-EQ",
                "buyOrSell": "B",
            },
            {
                "norenordno": "2",
                "status": "COMPLETE",
                "fillshares": 5,
                "price": 90,
                "tsym": "IDEA-EQ",
                "buyOrSell": "S",
            },
        ],
        {
            "password": "p",
            "totp_secret": "t",
            "vendor_code": "v",
            "api_key": "a",
            "imei": "i",
        },
        {
            "password": "p",
            "totp_secret": "t",
            "vendor_code": "v",
            "api_key": "a",
            "imei": "i",
        },
    ),
    (
        "fyers",
        [
            {
                "id": "1",
                "status": "FILLED",
                "filledQuantity": 10,
                "price": 100,
                "symbol": "NSE:IDEA-EQ",
                "side": 1,
            },
            {
                "id": "2",
                "status": "FILLED",
                "filledQuantity": 5,
                "price": 90,
                "symbol": "NSE:IDEA-EQ",
                "side": -1,
            },
        ],
        {"access_token": "x"},
        {"access_token": "y"},
    ),
    (
        "flattrade",
        [
            {
                "orderId": "1",
                "status": "COMPLETE",
                "filledQuantity": 10,
                "price": 100,
                "tradingSymbol": "IDEA",
                "transactionType": "BUY",
            },
            {
                "orderId": "2",
                "status": "COMPLETE",
                "filledQuantity": 5,
                "price": 90,
                "tradingSymbol": "IDEA",
                "transactionType": "SELL",
            },
        ],
        {"access_token": "x"},
        {"access_token": "y"},
    ),
    (
        "groww",
        [
            {
                "orderId": "1",
                "status": "COMPLETE",
                "filledQuantity": 10,
                "price": 100,
                "tradingSymbol": "IDEA",
                "transaction_type": "BUY",
            },
            {
                "orderId": "2",
                "status": "COMPLETE",
                "filledQuantity": 5,
                "price": 90,
                "tradingSymbol": "IDEA",
                "transaction_type": "SELL",
            },
        ],
        {"access_token": "x"},
        {"access_token": "y"},
    ),
]

@pytest.mark.parametrize(
    "broker, creds, missing",
    [
        ("aliceblue", {"device_number": "d"}, "api_key"),
        ("zerodha", {"api_secret": "s", "access_token": "x"}, "api_key"),
        ("zerodha", {"api_key": "k", "access_token": "x"}, "api_secret"),
        ("zerodha", {"api_key": "k", "api_secret": "s"}, "access_token"),
    ],
)
def test_dummy_broker_requires_mandatory_credentials(broker, creds, missing):
    Dummy = make_dummy_broker(broker, "M", "C", [], [])
    with pytest.raises(AssertionError, match=missing):
        Dummy("id", **creds)


@pytest.mark.skip(reason="poll_and_copy_trades migrated to queue-based service")
@pytest.mark.parametrize("broker, orders, master_creds, child_creds", BROKER_CASES)
def test_buy_sell_copied_for_each_broker(client, monkeypatch, broker, orders, master_creds, child_creds):
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    master_id = f"M_{broker}"
    child_id = f"C_{broker}"
    placed = []

    DummyBroker = make_dummy_broker(broker, master_id, child_id, orders, placed)

    def fake_get_broker_class(name):
        return DummyBroker

    monkeypatch.setattr(brokers.factory, "get_broker_class", fake_get_broker_class)
    monkeypatch.setattr(app_module, "get_broker_class", fake_get_broker_class)
    assert (
        app_module.poll_and_copy_trades.__globals__["get_broker_class"]
        is fake_get_broker_class
    )

    monkeypatch.setattr(app_module, "save_log", lambda *a, **k: None)
    monkeypatch.setattr(app_module, "save_order_mapping", lambda *a, **k: None)
    monkeypatch.setattr(app_module, "record_trade", lambda *a, **k: None)

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        master = Account(
            user_id=user.id,
            role="master",
            broker=broker,
            client_id=master_id,
            credentials=master_creds,
        )
        child = Account(
            user_id=user.id,
            role="child",
            broker=broker,
            client_id=child_id,
            linked_master_id=master_id,
            copy_status="On",
            credentials=child_creds,
            last_copied_trade_id="0",
        )
        db.session.add_all([master, child])
        db.session.commit()

        if broker == "groww":
            symbol_map = brokers.symbol_map.get_symbol_map()
            mapping = symbol_map.get("IDEA", {}).copy()
            mapping["groww"] = {"trading_symbol": "IDEA", "exchange": "NSE"}
            monkeypatch.setitem(symbol_map, "IDEA", mapping)

        app_module.poll_and_copy_trades()

        assert len(placed) == 2
        result = {p["transaction_type"]: p["quantity"] for p in placed}
        assert result["BUY"] == 10
        assert result["SELL"] == 5

@pytest.mark.skip(reason="poll_and_copy_trades migrated to queue-based service")
def test_dhan_copy_without_symbol_map(client, monkeypatch):
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    master_id = "M_DHAN_NOMAP"
    child_id = "C_DHAN_NOMAP"
    placed = []

    orders = [
        {
            "orderId": "1",
            "status": "COMPLETE",
            "filledQuantity": 10,
            "price": 100,
            "tradingSymbol": "IDEA",
            "transactionType": "BUY",
            "securityId": "12345",
            "exchangeSegment": "NSE_EQ",
        }
    ]

    DummyBroker = make_dummy_broker("dhan", master_id, child_id, orders, placed)

    def fake_get_broker_class(name):
        return DummyBroker

    monkeypatch.setattr(brokers.factory, "get_broker_class", fake_get_broker_class)
    monkeypatch.setattr(app_module, "get_broker_class", fake_get_broker_class)
    monkeypatch.setattr(app_module, "save_log", lambda *a, **k: None)
    monkeypatch.setattr(app_module, "save_order_mapping", lambda *a, **k: None)
    monkeypatch.setattr(app_module, "record_trade", lambda *a, **k: None)

    monkeypatch.setattr(brokers.symbol_map, "get_symbol_for_broker", lambda *a, **k: {})
    monkeypatch.setattr(app_module, "get_symbol_for_broker", lambda *a, **k: {})

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        master = Account(
            user_id=user.id,
            role="master",
            broker="dhan",
            client_id=master_id,
            credentials={"access_token": "x"},
        )
        child = Account(
            user_id=user.id,
            role="child",
            broker="dhan",
            client_id=child_id,
            linked_master_id=master_id,
            copy_status="On",
            credentials={"access_token": "y"},
            last_copied_trade_id="0",
        )
        db.session.add_all([master, child])
        db.session.commit()

        app_module.poll_and_copy_trades()

        assert len(placed) == 1
        params = placed[0]
        assert params["security_id"] == "12345"
        assert params["exchange_segment"] == "NSE_EQ"

@pytest.mark.skip(reason="poll_and_copy_trades migrated to queue-based service")
def test_aliceblue_market_order_without_price_copies(client, monkeypatch):
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    master_id = "M_AB_ZERO"
    child_id = "C_AB_ZERO"
    placed = []

    orders = [
        {
            "order_id": "1",
            "status": "FILLED",
            "filled_qty": 1,
            "avg_price": 0,
            "placed_qty": 1,
            "symbol": "IDEA-EQ",
            "side": "B",
        }
    ]

    DummyBroker = make_dummy_broker("aliceblue", master_id, child_id, orders, placed)

    def fake_get_broker_class(name):
        return DummyBroker

    monkeypatch.setattr(brokers.factory, "get_broker_class", fake_get_broker_class)
    monkeypatch.setattr(app_module, "get_broker_class", fake_get_broker_class)
    monkeypatch.setattr(app_module, "save_log", lambda *a, **k: None)
    monkeypatch.setattr(app_module, "save_order_mapping", lambda *a, **k: None)
    monkeypatch.setattr(app_module, "record_trade", lambda *a, **k: None)

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        master = Account(
            user_id=user.id,
            role="master",
            broker="aliceblue",
            client_id=master_id,
            credentials={"api_key": "k"},
        )
        child = Account(
            user_id=user.id,
            role="child",
            broker="aliceblue",
            client_id=child_id,
            linked_master_id=master_id,
            copy_status="On",
            credentials={"api_key": "k"},
            last_copied_trade_id="0",
        )
        db.session.add_all([master, child])
        db.session.commit()

        app_module.poll_and_copy_trades()

        assert len(placed) == 1
        assert placed[0]["price"] == 0
        assert placed[0]["quantity"] == 1
