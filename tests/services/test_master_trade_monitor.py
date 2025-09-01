from types import SimpleNamespace

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
        self.children = children
        self.kwargs = None

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

    def place_order(self, symbol, action, qty, exchange=None, order_type=None):
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
        multiplier=1,
        role="child",
    )
    session = SessionStub(master, [child])
    redis = RedisStub()

    monkeypatch.setattr(master_trade_monitor, "get_broker_client", fake_get_broker_client)
    monkeypatch.setattr(trade_copier, "get_broker_client", fake_get_broker_client)
    monkeypatch.setattr(trade_copier, "active_children_for_master", lambda m: [child])
    
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
    assert BrokerStub.placed == [
        {"client_id": "c1", "symbol": "AAPL", "action": "BUY", "qty": 1}
    ]
    assert redis.acks == [b"1"]


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

    # Ensure ``None`` values are not included in the published event
    msg = redis.stream[0][1]
    assert "exchange" not in msg


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
