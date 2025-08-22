from services import order_consumer
from marshmallow import ValidationError

class StubRedis:
    def __init__(self, events):
        self.events = events
        self.added = []

    def xgroup_create(self, *_, **__):
        pass

    def xreadgroup(self, *_, **__):
        if not self.events:
            return []
        data = self.events.pop(0)
        return [("webhook_events", [("1", data)])]

    def xack(self, *_, **__):
        pass

    def xadd(self, stream, data):
        self.added.append((stream, data))



class MockBroker:
    orders = []

    def __init__(self, client_id, access_token, **_):
        self.client_id = client_id
        self.access_token = access_token

    def place_order(self, **order):
        MockBroker.orders.append(order)


def reset_metrics():
    order_consumer.orders_success._value.set(0)
    order_consumer.orders_failed._value.set(0)
    MockBroker.orders = []


def test_consumer_places_order(monkeypatch):
    event = {"user_id": 1, "symbol": "AAPL", "action": "BUY", "qty": 1, "alert_id": "1"}
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)
    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: MockBroker)
    monkeypatch.setattr(order_consumer, "check_duplicate_and_risk", lambda e: True)
    monkeypatch.setattr(order_consumer, "USER_SETTINGS", {1: {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]}})
    reset_metrics()

    processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)
    assert processed == 1
    assert MockBroker.orders == [{"symbol": "AAPL", "action": "BUY", "qty": 1, "exchange": None, "order_type": None}]
    assert order_consumer.orders_success._value.get() == 1
    assert order_consumer.orders_failed._value.get() == 0
    assert stub.added == [
        (
            "trade_events",
            {
                "master_id": "c",
                "symbol": "AAPL",
                "action": "BUY",
                "qty": 1,
                "exchange": None,
                "order_type": None,
            },
        )
    ]

def test_consumer_handles_risk_failure(monkeypatch):
    event = {"user_id": 1, "symbol": "AAPL", "action": "BUY", "qty": 10, "alert_id": "1"}
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)
    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: MockBroker)

    def guard(event):
        raise ValidationError("risk")

    monkeypatch.setattr(order_consumer, "check_duplicate_and_risk", guard)
    monkeypatch.setattr(order_consumer, "USER_SETTINGS", {1: {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]}})
    reset_metrics()

    processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)
    assert processed == 1
    assert MockBroker.orders == []
    assert order_consumer.orders_success._value.get() == 0
    assert order_consumer.orders_failed._value.get() == 1
    assert stub.added == []
