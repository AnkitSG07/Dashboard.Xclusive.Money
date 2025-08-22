import pytest

from services import trade_copier


class StubRedis:
    """Simple in-memory Redis Stream stub."""

    def __init__(self, events):
        self.events = events

    def xread(self, *_, **__):
        if not self.events:
            return []
        data = self.events.pop(0)
        # mimic redis-py return structure
        return [("trade_events", [("1-0", data)])]


class DummySession:
    class _Query:
        def __init__(self, data):
            self.data = data

        def filter_by(self, **kwargs):
            self.kwargs = kwargs    
            return self
            
        def first(self):
            # Return master account when querying for master
            if self.kwargs.get("role") == "master":
                return type("Master", (), {"client_id": "m1"})()
            return None

        def all(self):
           # Return two child accounts when querying for children
            return [object(), object()]

    def query(self, model):
        return self._Query(model)

def test_poll_and_copy_trades_processes_event(monkeypatch):
    processed = []

    def processor(master, child, order):
        processed.append(order["symbol"])

    event = {b"master_id": b"m1", b"symbol": b"AAPL", b"action": b"BUY", b"qty": b"1"}
    stub_redis = StubRedis([event])
    session = DummySession()

    count = trade_copier.poll_and_copy_trades(
        session, processor=processor, redis_client=stub_redis, max_messages=1
    )

    assert count == 1
    # two children processed
    assert processed == ["AAPL", "AAPL"]
