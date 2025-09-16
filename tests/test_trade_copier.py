from collections import OrderedDict    
import pytest

from services import trade_copier


class StubRedis:
    """Simple in-memory Redis Stream stub using consumer groups."""

    def __init__(self, events):
        self.stream = "trade_events"
        self.messages = [
            (f"{idx + 1}-0", data) for idx, data in enumerate(events)
        ]
        self.delivered_index = 0
        self.pending: dict[str, OrderedDict[str, dict]] = {}

    def xgroup_create(self, *_, **__):
        pass

    def _deliver_new(self, consumer, count):
        remaining = len(self.messages) - self.delivered_index
        if remaining <= 0:
            return []
        limit = remaining if count is None else min(remaining, count)
        batch = self.messages[self.delivered_index : self.delivered_index + limit]
        self.delivered_index += limit
        if batch:
            pending = self.pending.setdefault(consumer, OrderedDict())
            for msg_id, data in batch:
                pending[msg_id] = data
        return batch

    def _read_pending(self, consumer, count):
        pending = self.pending.get(consumer)
        if not pending:
            return []
        items = list(pending.items())
        if count is not None:
            items = items[:count]
        return items

    def xreadgroup(self, group, consumer, streams, count=None, block=None):
        stream, start = next(iter(streams.items()))
        if start == ">":
            batch = self._deliver_new(consumer, count)
            return [(stream, batch)] if batch else []
        if start == "0":
            batch = self._read_pending(consumer, count)
            return [(stream, batch)] if batch else []
        raise NotImplementedError(f"Unsupported start id {start!r}")

    def xautoclaim(
        self,
        stream,
        group,
        consumer,
        min_idle_time,
        start_id="0-0",
        count=None,
        justid=False,
    ):
        claimed = []
        remaining = count
        for owner, pending in list(self.pending.items()):
            if owner == consumer:
                continue
            for msg_id, data in list(pending.items()):
                if remaining is not None and remaining <= 0:
                    break
                if justid:
                    claimed.append(msg_id)
                else:
                    claimed.append((msg_id, data))
                dest = self.pending.setdefault(consumer, OrderedDict())
                dest[msg_id] = data
                del pending[msg_id]
                if remaining is not None:
                    remaining -= 1
            if not pending:
                del self.pending[owner]
            if remaining is not None and remaining <= 0:
                break
        return "0-0", claimed

    def xack(self, stream, group, *ids):
        for msg_id in ids:
            for owner, pending in list(self.pending.items()):
                if msg_id in pending:
                    del pending[msg_id]
                    if not pending:
                        del self.pending[owner]
                    break



class DummySession:
    class _Query:
        def __init__(self, data):
            self.data = data

        def filter_by(self, **kwargs):
            self.kwargs = kwargs
            return self
        def filter(self, *args):
             return self
            
        def first(self):
            # Return master account when querying for master
            if self.kwargs.get("role") == "master":
                return type(
                    "Master",
                    (),
                    {"client_id": "m1", "user_id": 1, "broker": "master-broker"},
                )()
            return None

        def all(self):
           # Return two child accounts when querying for children
            return [object(), object()]

    def query(self, model):
        return self._Query(model)

    def rollback(self):
        pass

    def expire_all(self):
        pass

def test_poll_and_copy_trades_processes_event(monkeypatch):
    processed = []

    def processor(master, child, order):
        processed.append(order["symbol"])

    event = {
        b"master_id": b"m1",
        b"symbol": b"AAPL",
        b"action": b"BUY",
        b"qty": b"1",
    }
    stub_redis = StubRedis([event])
    session = DummySession()
    monkeypatch.setattr(
        trade_copier,
        "active_children_for_master",
        lambda *_, **__: [object(), object()],
    )
    count = trade_copier.poll_and_copy_trades(
        session, processor=processor, redis_client=stub_redis, max_messages=1
    )

    assert count == 1
    # two children processed
    assert processed == ["AAPL", "AAPL"]


def test_poll_skips_events_missing_master_id(monkeypatch):
    """Events without a master identifier should be acknowledged and skipped."""

    stub_redis = StubRedis([
        {b"symbol": b"AAPL", b"action": b"BUY", b"qty": b"1"}
    ])

    session = DummySession()

    count = trade_copier.poll_and_copy_trades(
        session,
        processor=lambda *args, **kwargs: None,
        redis_client=stub_redis,
        max_messages=1,
    )

    assert count == 1
    assert stub_redis.pending == {}


def test_child_orders_submitted(monkeypatch):
    """Verify that ``copy_order`` submits orders for each child account."""

    orders = []

    # Stub broker that records ``place_order`` invocations
    class StubBroker:
        def __init__(self, client_id, access_token, **_):
            self.client_id = client_id

        def place_order(self, **params):
            orders.append((self.client_id, params))
            return {"status": "ok"}

    def fake_get_broker_client(name):
        assert name == "stub"
        return StubBroker

    class Child:
        def __init__(self, cid):
            self.client_id = cid
            self.broker = "stub"
            self.credentials = {"access_token": "t"}
            self.copy_qty = None

    class Master:
        client_id = "m1"
        user_id = 1
        broker = "master-broker"
        
    class Session:
        def query(self, model):
            return self

        def filter_by(self, **kwargs):
            self.kwargs = kwargs
            return self

        def filter(self, *args):
            return self

        def first(self):
            if self.kwargs.get("role") == "master":
                return Master()
            return None

        def all(self):
            # Two child accounts to replicate the trade to
            return [Child("c1"), Child("c2")]

        def rollback(self):
            pass

        def expire_all(self):
            pass

    event = {
        b"master_id": b"m1",
        b"symbol": b"AAPL",
        b"action": b"BUY",
        b"qty": b"1",
        b"product_type": b"CNC",
    }
    stub_redis = StubRedis([event])

    monkeypatch.setattr(trade_copier, "get_broker_client", fake_get_broker_client)
    monkeypatch.setattr(
        trade_copier,
        "active_children_for_master",
        lambda *_, **__: [Child("c1"), Child("c2")],
    )
    
    count = trade_copier.poll_and_copy_trades(
        Session(), processor=trade_copier.copy_order, redis_client=stub_redis, max_messages=1
    )

    assert count == 1
    assert orders == [
        (
            "c1",
            {"symbol": "AAPL", "action": "BUY", "qty": 1, "product_type": "CNC"},
        ),
        (
            "c2",
            {"symbol": "AAPL", "action": "BUY", "qty": 1, "product_type": "CNC"},
        ),
    ]

def test_child_orders_mirror_master_params(monkeypatch):
    """Optional order fields should be forwarded to child accounts."""

    orders = []

    class StubBroker:
        def __init__(self, client_id, access_token, **_):
            self.client_id = client_id

        def place_order(self, **params):
            orders.append((self.client_id, params))
            return {"status": "ok"}

    def fake_get_broker_client(name):
        assert name == "stub"
        return StubBroker

    class Child:
        def __init__(self, cid):
            self.client_id = cid
            self.broker = "stub"
            self.credentials = {"access_token": "t"}
            self.copy_qty = None

    class Session:
        def query(self, model):
            return self

        def filter_by(self, **kwargs):
            self.kwargs = kwargs
            return self

        def filter(self, *args):
            return self

        def first(self):
            if self.kwargs.get("role") == "master":
                return type(
                    "Master",
                    (),
                    {"client_id": "m1", "user_id": 1, "broker": "master-broker"},
                )()
            return None

        def all(self):
            return [Child("c1"), Child("c2")]

        def rollback(self):
            pass

        def expire_all(self):
            pass

    event = {
        b"master_id": b"m1",
        b"symbol": b"AAPL",
        b"action": b"BUY",
        b"qty": b"1",
        b"exchange": b"NSE",
        b"order_type": b"LIMIT",
        b"product_type": b"CNC",
        b"validity": b"DAY",
        b"price": b"100",
    }
    stub_redis = StubRedis([event])

    monkeypatch.setattr(trade_copier, "get_broker_client", fake_get_broker_client)
    monkeypatch.setattr(
        trade_copier,
        "active_children_for_master",
        lambda *_, **__: [Child("c1"), Child("c2")],
    )

    count = trade_copier.poll_and_copy_trades(
        Session(),
        processor=trade_copier.copy_order,
        redis_client=stub_redis,
        max_messages=1,
    )

    assert count == 1
    expected_params = {
        "symbol": "AAPL",
        "action": "BUY",
        "qty": 1,
        "exchange": "NSE",
        "order_type": "LIMIT",
        "product_type": "CNC",
        "validity": "DAY",
        "price": 100,
    }
    assert orders == [
        ("c1", expected_params),
        ("c2", expected_params),
    ]

def test_manual_trade_events_replicate(monkeypatch):
    """Manual trade events should replicate and ignore metadata fields."""

    orders = []

    # Broker that only supports a subset of parameters to ensure filtering works
    class StubBroker:
        def __init__(self, client_id, access_token, **_):
            self.client_id = client_id

        def place_order(self, symbol, action, qty, product_type="CNC", **_):
            orders.append((self.client_id, symbol, action, qty, product_type))
            return {"status": "ok"}

    def fake_get_broker_client(name):
        assert name == "stub"
        return StubBroker

    class Child:
        def __init__(self, cid):
            self.client_id = cid
            self.broker = "stub"
            self.credentials = {"access_token": "t"}
            self.copy_qty = None

    class Session:
        def query(self, model):
            return self

        def filter_by(self, **kwargs):
            self.kwargs = kwargs
            return self

        def filter(self, *args):
            return self

        def first(self):
            if self.kwargs.get("role") == "master":
                return type(
                    "Master",
                    (),
                    {"client_id": "m1", "user_id": 1, "broker": "master-broker"},
                )()
            return None

        def all(self):
            return [Child("c1"), Child("c2")]

        def rollback(self):
            pass

        def expire_all(self):
            pass

    event = {
        b"master_id": b"m1",
        b"symbol": b"AAPL",
        b"action": b"BUY",
        b"qty": b"1",
        b"product_type": b"CNC",
        b"source": b"manual_trade_monitor",
        b"order_id": b"123",
        b"order_time": b"2024-01-01T00:00:00Z",
        b"exchange": b"NSE",
    }
    stub_redis = StubRedis([event])

    monkeypatch.setattr(trade_copier, "get_broker_client", fake_get_broker_client)
    monkeypatch.setattr(
        trade_copier,
        "active_children_for_master",
        lambda *_, **__: [Child("c1"), Child("c2")],
    )

    count = trade_copier.poll_and_copy_trades(
        Session(),
        processor=trade_copier.copy_order,
        redis_client=stub_redis,
        max_messages=1,
    )

    assert count == 1
    assert orders == [
        ("c1", "AAPL", "BUY", 1, "CNC"),
        ("c2", "AAPL", "BUY", 1, "CNC"),
    ]

def test_pending_messages_processed_after_restart(monkeypatch):
    """Messages left pending should be processed when the worker restarts."""

    event = {
        b"master_id": b"m1",
        b"symbol": b"AAPL",
        b"action": b"BUY",
        b"qty": b"1",
    }
    stub_redis = StubRedis([event])
    session = DummySession()

    class Child:
        def __init__(self, cid):
            self.client_id = cid
            self.broker = "stub"

    def fake_children(master, _session, logger=None):
        return [Child("c1"), Child("c2")]

    monkeypatch.setattr(trade_copier, "active_children_for_master", fake_children)

    original_replicate = trade_copier._replicate_to_children

    async def failing_replicate(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(trade_copier, "_replicate_to_children", failing_replicate)

    trade_copier.poll_and_copy_trades(
        session,
        processor=lambda *_: None,
        redis_client=stub_redis,
        max_messages=1,
    )

    assert "worker-1" in stub_redis.pending
    assert stub_redis.pending["worker-1"]

    monkeypatch.setattr(trade_copier, "_replicate_to_children", original_replicate)

    processed = []

    def processor(master, child, order):
        processed.append((child["client_id"], order["symbol"]))

    count = trade_copier.poll_and_copy_trades(
        session,
        processor=processor,
        redis_client=stub_redis,
        max_messages=1,
        max_workers=1,
    )

    assert count == 1
    assert processed == [("c1", "AAPL"), ("c2", "AAPL")]
    assert not stub_redis.pending
