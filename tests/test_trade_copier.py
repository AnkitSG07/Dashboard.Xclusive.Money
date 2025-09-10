import pytest

from services import trade_copier


class StubRedis:
    """Simple in-memory Redis Stream stub using consumer groups."""

    def __init__(self, events):
        self.events = events

    def xgroup_create(self, *_, **__):
        pass

    def xreadgroup(self, *_, **__):
        if not self.events:
            return []
        data = self.events.pop(0)
        return [("trade_events", [("1-0", data)])]

    def xack(self, *_, **__):
        pass



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
                return type("Master", (), {"client_id": "m1", "user_id": 1})()
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
        lambda m: [object(), object()],
    )
    count = trade_copier.poll_and_copy_trades(
        session, processor=processor, redis_client=stub_redis, max_messages=1
    )

    assert count == 1
    # two children processed
    assert processed == ["AAPL", "AAPL"]


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
        lambda m: [Child("c1"), Child("c2")],
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
                return type("Master", (), {"client_id": "m1", "user_id": 1})()
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
        lambda m: [Child("c1"), Child("c2")],
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
