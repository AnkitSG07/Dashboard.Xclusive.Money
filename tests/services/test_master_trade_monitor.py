from types import SimpleNamespace
import importlib
import os
import sys
import tempfile

import pytest

from services import master_trade_monitor, trade_copier


class RedisStub:
    def __init__(self):
        self.stream = []
        self.acks = []

    def xgroup_create(self, *args, **kwargs):
        pass

    def xadd(self, stream, event):
        self.stream.append((f"{len(self.stream)+1}".encode(), event))

    def xreadgroup(self, group, consumer, streams, count, block):
        if not self.stream:
            return []
        events = self.stream[:count]
        self.stream = self.stream[count:]
        return [(list(streams.keys())[0], events)]

    def xack(self, stream, group, msg_id):
        self.acks.append(msg_id)


class SessionStub:
    def __init__(self, master, children):
        self.master = master
        if not hasattr(self.master, "copy_status"):
            self.master.copy_status = "On"
        self.children = children
        self.kwargs = None

    def rollback(self):
        pass

    def expire_all(self):
        pass

    def query(self, model):
        return self

    def filter_by(self, **kwargs):
        self.kwargs = kwargs
        return self

    def filter(self, *args):
        self._filtered = True
        return self

    def all(self):
        if getattr(self, "_filtered", False):
            return self.children
        if self.kwargs.get("role") == "master":
            return [self.master]
        return []

    def first(self):
        if (
            self.kwargs.get("client_id") == self.master.client_id
            and self.kwargs.get("role") == "master"
        ):
            return self.master
        return None


class BrokerStub:
    placed = []

    def __init__(self, client_id, token=None, access_token=None, **kwargs):
        self.orders = kwargs.get("orders", [])
        self.client_id = client_id

    def list_orders(self):
        return self.orders

    def place_order(self, symbol, action, qty, exchange=None, order_type=None, **kwargs):
        BrokerStub.placed.append(
            {
                "client_id": self.client_id,
                "symbol": symbol,
                "action": action,
                "qty": qty,
            }
        )


def fake_get_broker_client(name):
    return BrokerStub


def test_manual_orders_are_published_and_copied(monkeypatch):
    BrokerStub.placed = []
    order = {"id": "1", "symbol": "AAPL", "action": "BUY", "qty": 1}
    master = SimpleNamespace(
        client_id="m",
        broker="mock",
        credentials={"access_token": "", "orders": [order]},
        role="master",
        user_id=1,
    )
    child = SimpleNamespace(
        broker="mock",
        client_id="c1",
        credentials={},
        copy_qty=None,
        role="child",
    )
    session = SessionStub(master, [child])
    redis = RedisStub()

    monkeypatch.setattr(master_trade_monitor, "get_broker_client", fake_get_broker_client)
    monkeypatch.setattr(trade_copier, "get_broker_client", fake_get_broker_client)
    monkeypatch.setattr(
        trade_copier,
        "active_children_for_master",
        lambda m, *args, **kwargs: [child],
    )
    
    # Publish manual orders
    master_trade_monitor.monitor_master_trades(
        session, redis_client=redis, max_iterations=1, poll_interval=0
    )

    # Ensure trade copier processes the published order
    processed = trade_copier.poll_and_copy_trades(
        session,
        processor=trade_copier.copy_order,
        redis_client=redis,
        max_messages=1,
    )

    assert processed == 1
    assert len(BrokerStub.placed) == 1
    placed_order = BrokerStub.placed[0]
    assert placed_order["client_id"] == "c1"
    assert placed_order["action"] == "BUY"
    assert placed_order["qty"] == 1
    assert placed_order["symbol"] in {"AAPL", "AAPL-EQ"}
    assert redis.acks == [b"1"]


def test_monitor_emits_for_master_created_via_save_account(monkeypatch):
    BrokerStub.placed = []
    fd, db_path = tempfile.mkstemp()
    original_db_url = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = "sqlite:///" + db_path
    os.environ.setdefault("SECRET_KEY", "test")
    os.environ.setdefault("ADMIN_EMAIL", "admin@example.com")
    os.environ.setdefault("ADMIN_PASSWORD", "password")
    os.environ.setdefault("RUN_SCHEDULER", "0")

    import app as app_module

    app_module = importlib.reload(app_module)
    app = app_module.app
    db = app_module.db

    try:
        with app.app_context():
            db.create_all()
            order = {"id": "42", "symbol": "NIFTY", "action": "BUY", "qty": 5}
            app_module.save_account_to_user(
                "owner@example.com",
                {
                    "client_id": "MREAL",
                    "broker": "mock",
                    "role": "master",
                    "credentials": {"access_token": "", "orders": [order]},
                },
            )

            master = app_module.Account.query.filter_by(client_id="MREAL").one()
            assert str(master.copy_status).lower() == "on"

            redis = RedisStub()
            monkeypatch.setattr(master_trade_monitor, "get_broker_client", fake_get_broker_client)

            master_trade_monitor.monitor_master_trades(
                db.session, redis_client=redis, max_iterations=1, poll_interval=0
            )

            assert redis.stream, "expected a manual trade event to be published"
            event = redis.stream[0][1]
            assert event["master_id"] == "MREAL"
            assert event["symbol"] in {"NIFTY", "NIFTY-EQ"}
    finally:
        with app.app_context():
            db.session.remove()
            db.drop_all()

        os.close(fd)
        os.unlink(db_path)

        if original_db_url is None:
            os.environ.pop("DATABASE_URL", None)
            sys.modules.pop("app", None)
        else:
            os.environ["DATABASE_URL"] = original_db_url
            importlib.reload(app_module)


def test_manual_orders_with_metadata_are_copied(monkeypatch):
    BrokerStub.placed = []
    order = {
        "order_id": "abc123",
        "symbol": "AAPL",
        "action": "BUY",
        "qty": 1,
        "status": "COMPLETE",
        "order_time": "2024-01-01T00:00:00Z",
    }
    master = SimpleNamespace(
        client_id="m",
        broker="mock",
        credentials={"access_token": "", "orders": [order]},
        role="master",
        user_id=1,
    )
    child = SimpleNamespace(
        broker="mock",
        client_id="c1",
        credentials={},
        copy_qty=None,
        role="child",
    )
    session = SessionStub(master, [child])
    redis = RedisStub()

    monkeypatch.setattr(master_trade_monitor, "get_broker_client", fake_get_broker_client)
    monkeypatch.setattr(trade_copier, "get_broker_client", fake_get_broker_client)
    monkeypatch.setattr(
        trade_copier,
        "active_children_for_master",
        lambda m, *args, **kwargs: [child],
    )

    master_trade_monitor.monitor_master_trades(
        session, redis_client=redis, max_iterations=1, poll_interval=0
    )

    # Ensure the published event includes extra metadata that should be ignored by the copier
    redis.stream[0][1].update(
        {
            "order_time": "2024-01-01T00:00:00Z",
            "status_message": "Completed",
            "message": "metadata",
        }
    )

    processed = trade_copier.poll_and_copy_trades(
        session,
        processor=trade_copier.copy_order,
        redis_client=redis,
        max_messages=1,
    )

    assert processed == 1
    assert len(BrokerStub.placed) == 1
    placed_order = BrokerStub.placed[0]
    assert placed_order["client_id"] == "c1"
    assert placed_order["action"] == "BUY"
    assert placed_order["qty"] == 1
    assert placed_order["symbol"] in {"AAPL", "AAPL-EQ"}
    assert redis.acks == [b"1"]


def test_rejected_orders_not_copied(monkeypatch):
    BrokerStub.placed = []
    order = {
        "id": "1",
        "symbol": "AAPL",
        "action": "BUY",
        "qty": 1,
        "status": "REJECTED",
    }
    master = SimpleNamespace(
        client_id="m",
        broker="mock",
        credentials={"access_token": "", "orders": [order]},
        role="master",
        user_id=1,
    )
    child = SimpleNamespace(
        broker="mock",
        client_id="c1",
        credentials={},
        copy_qty=None,
        role="child",
    )
    session = SessionStub(master, [child])
    redis = RedisStub()

    monkeypatch.setattr(master_trade_monitor, "get_broker_client", fake_get_broker_client)
    monkeypatch.setattr(trade_copier, "get_broker_client", fake_get_broker_client)
    monkeypatch.setattr(
        trade_copier,
        "active_children_for_master",
        lambda m, *args, **kwargs: [child],
    )

    master_trade_monitor.monitor_master_trades(
        session, redis_client=redis, max_iterations=1, poll_interval=0
    )

    processed = trade_copier.poll_and_copy_trades(
        session,
        processor=trade_copier.copy_order,
        redis_client=redis,
        max_messages=1,
    )

    assert processed == 0
    assert BrokerStub.placed == []
    assert redis.stream == []


def test_duplicate_orders_not_republished(monkeypatch):
    order = {"id": "1", "symbol": "AAPL", "action": "BUY", "qty": 1}
    master = SimpleNamespace(
        client_id="m",
        broker="mock",
        credentials={"access_token": "", "orders": [order]},
        role="master",
    )
    session = SessionStub(master, [])
    redis = RedisStub()

    monkeypatch.setattr(master_trade_monitor, "get_broker_client", fake_get_broker_client)

    # Run two polling iterations; the same order should only be published once
    master_trade_monitor.monitor_master_trades(
        session, redis_client=redis, max_iterations=2, poll_interval=0
    )

    assert len(redis.stream) == 1

def test_credentials_with_embedded_client_id(monkeypatch):
    """Ensure a client_id stored inside credentials doesn't break instantiation."""
    BrokerStub.placed = []
    order = {"id": "1", "symbol": "AAPL", "action": "BUY", "qty": 1}
    master = SimpleNamespace(
        client_id="m",
        broker="mock",
        credentials={"client_id": "ignored", "access_token": "", "orders": [order]},
        role="master",
        user_id=1,
    )
    child = SimpleNamespace(
        broker="mock",
        client_id="c1",
        credentials={},
        copy_qty=None,
        role="child",
    )
    session = SessionStub(master, [child])
    redis = RedisStub()

    monkeypatch.setattr(master_trade_monitor, "get_broker_client", fake_get_broker_client)
    monkeypatch.setattr(trade_copier, "get_broker_client", fake_get_broker_client)
    monkeypatch.setattr(
        trade_copier,
        "active_children_for_master",
        lambda m, *args, **kwargs: [child],
    )

    master_trade_monitor.monitor_master_trades(
        session, redis_client=redis, max_iterations=1, poll_interval=0
    )

    processed = trade_copier.poll_and_copy_trades(
        session,
        processor=trade_copier.copy_order,
        redis_client=redis,
        max_messages=1,
    )

    assert processed == 1
    assert len(BrokerStub.placed) == 1
    placed_order = BrokerStub.placed[0]
    assert placed_order["client_id"] == "c1"
    assert placed_order["action"] == "BUY"
    assert placed_order["qty"] == 1
    assert placed_order["symbol"] in {"AAPL", "AAPL-EQ"}



def test_dhan_orders_are_published_and_copied(monkeypatch):
    BrokerStub.placed = []
    order = {
        "orderId": "1",
        "tradingSymbol": "AAPL",
        "transactionType": "BUY",
        "orderQty": 1,
        "exchangeSegment": "NSE",
        "orderType": "LIMIT",
    }
    master = SimpleNamespace(
        client_id="m",
        broker="mock",
        credentials={"access_token": "", "orders": [order]},
        role="master",
        user_id=1,
    )
    child = SimpleNamespace(
        broker="mock",
        client_id="c1",
        credentials={},
        copy_qty=None,
        role="child",
    )
    session = SessionStub(master, [child])
    redis = RedisStub()

    monkeypatch.setattr(master_trade_monitor, "get_broker_client", fake_get_broker_client)
    monkeypatch.setattr(trade_copier, "get_broker_client", fake_get_broker_client)
    monkeypatch.setattr(
        trade_copier,
        "active_children_for_master",
        lambda m, *args, **kwargs: [child],
    )

    master_trade_monitor.monitor_master_trades(
        session, redis_client=redis, max_iterations=1, poll_interval=0
    )

    assert len(redis.stream) == 1
    msg = redis.stream[0][1]
    assert msg["master_id"] == "m"
    assert msg["symbol"] in {"AAPL", "AAPL-EQ"}
    assert msg["action"] == "BUY"
    assert msg["qty"] == "1"
    assert msg.get("exchange") == "NSE"
    assert msg.get("order_type") == "LIMIT"
    assert msg.get("order_id") == "1"
    assert msg.get("source") == "manual_trade_monitor"

    processed = trade_copier.poll_and_copy_trades(
        session,
        processor=trade_copier.copy_order,
        redis_client=redis,
        max_messages=1,
    )

    assert processed == 1
    assert len(BrokerStub.placed) == 1
    placed_order = BrokerStub.placed[0]
    assert placed_order["client_id"] == "c1"
    assert placed_order["action"] == "BUY"
    assert placed_order["qty"] == 1
    assert placed_order["symbol"] in {"AAPL", "AAPL-EQ"}
    assert redis.acks == [b"1"]


def test_dhan_duplicate_orders_not_republished(monkeypatch):
    order = {
        "orderId": "1",
        "tradingSymbol": "AAPL",
        "transactionType": "BUY",
        "orderQty": 1,
    }
    master = SimpleNamespace(
        client_id="m",
        broker="mock",
        credentials={"access_token": "", "orders": [order]},
        role="master",
    )
    session = SessionStub(master, [])
    redis = RedisStub()

    monkeypatch.setattr(master_trade_monitor, "get_broker_client", fake_get_broker_client)

    master_trade_monitor.monitor_master_trades(
        session, redis_client=redis, max_iterations=2, poll_interval=0
    )

    assert len(redis.stream) == 1


def test_none_values_filtered(monkeypatch):
    order = {
        "id": "1",
        "symbol": "AAPL",
        "action": "BUY",
        "qty": 1,
        "exchange": None,
    }
    master = SimpleNamespace(
        client_id="m",
        broker="mock",
        credentials={"access_token": "", "orders": [order]},
        role="master",
    )
    session = SessionStub(master, [])
    redis = RedisStub()

    monkeypatch.setattr(master_trade_monitor, "get_broker_client", fake_get_broker_client)

    master_trade_monitor.monitor_master_trades(
        session, redis_client=redis, max_iterations=1, poll_interval=0
    )

    # Ensure ``None`` values are normalized before publishing
    msg = redis.stream[0][1]
    assert msg.get("exchange") == "NSE"


def test_poll_interval_from_env(monkeypatch):
    """If no interval is passed, the environment variable is used."""
    order = {"id": "1", "symbol": "AAPL", "action": "BUY", "qty": 1}
    master = SimpleNamespace(
        client_id="m",
        broker="mock",
        credentials={"access_token": "", "orders": [order]},
        role="master",
    )
    session = SessionStub(master, [])
    redis = RedisStub()

    monkeypatch.setattr(master_trade_monitor, "get_broker_client", fake_get_broker_client)

    calls = []

    async def fake_sleep(interval):
        calls.append(interval)

    monkeypatch.setenv("ORDER_MONITOR_INTERVAL", "0.01")
    monkeypatch.setattr(master_trade_monitor.asyncio, "sleep", fake_sleep)

    master_trade_monitor.monitor_master_trades(
        session, redis_client=redis, max_iterations=2
    )

    assert calls == [0.01]


def test_zerodha_manual_orders_published_once(monkeypatch):
    order = {
        "order_id": "1",
        "tradingsymbol": "AAPL",
        "transaction_type": "BUY",
        "quantity": 1,
        "status": "COMPLETE",
    }

    class DummyKite:
        def __init__(self, api_key):
            self.api_key = api_key

        def set_access_token(self, token):
            pass

        def orders(self, timeout=None):
            return [dict(order)]

    import brokers.zerodha as zerodha

    monkeypatch.setattr(zerodha, "KiteConnect", lambda api_key: DummyKite(api_key))
    monkeypatch.setattr(zerodha.ZerodhaBroker, "ensure_token", lambda self: None)
    monkeypatch.setattr(
        zerodha.ZerodhaBroker,
        "_kite_call",
        lambda self, func, *args, **kwargs: func(*args, **kwargs),
    )

    master = SimpleNamespace(
        client_id="m",
        broker="zerodha",
        credentials={"access_token": "t", "api_key": "k"},
        role="master",
    )
    session = SessionStub(master, [])
    redis = RedisStub()

    monkeypatch.setattr(
        master_trade_monitor, "get_broker_client", lambda name: zerodha.ZerodhaBroker
    )

    master_trade_monitor.monitor_master_trades(
        session, redis_client=redis, max_iterations=2, poll_interval=0
    )

    assert len(redis.stream) == 1
    event = redis.stream[0][1]
    assert event["master_id"] == "m"
    assert event["symbol"] in {"AAPL", "AAPL-EQ"}
    assert event["action"] == "BUY"
    assert event["qty"] == "1"
    assert event.get("order_id") == "1"
    assert event.get("source") == "manual_trade_monitor"

@pytest.mark.parametrize(
    "status", ["FULL_EXECUTED", "FULLY_EXECUTED", "CONFIRMED", "SUCCESS", "2"]
)
def test_orders_with_new_completed_statuses_published(monkeypatch, status):
    order = {
        "id": "1",
        "symbol": "AAPL",
        "action": "BUY",
        "qty": 1,
        "status": status,
    }
    master = SimpleNamespace(
        client_id="m",
        broker="mock",
        credentials={"access_token": "", "orders": [order]},
        role="master",
    )
    session = SessionStub(master, [])
    redis = RedisStub()

    monkeypatch.setattr(
        master_trade_monitor, "get_broker_client", fake_get_broker_client
    )

    master_trade_monitor.monitor_master_trades(
        session, redis_client=redis, max_iterations=1, poll_interval=0
    )

    assert len(redis.stream) == 1
    msg = redis.stream[0][1]
    assert msg["master_id"] == "m"
    assert msg["symbol"] in {"AAPL", "AAPL-EQ"}
    assert msg["action"] == "BUY"
    assert msg["qty"] == "1"
    assert msg.get("order_id") == "1"
    assert msg.get("source") == "manual_trade_monitor"
