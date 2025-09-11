from services import order_consumer
from marshmallow import ValidationError
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
import logging
import time
from datetime import date
import pytest

def guard(event):
    """Test helper that always allows an event."""
    return True

class StubRedis:
    def __init__(self, events):
        self.events = events
        self.added = []

    def xgroup_create(self, *_, **__):
        pass

    def xreadgroup(self, group, consumer, streams, count, block):
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
    status = "COMPLETE"
    
    def __init__(self, client_id, access_token, **_):
        self.client_id = client_id
        self.access_token = access_token

    def place_order(self, **order):
        MockBroker.orders.append(order)
        return {"status": "success", "order_id": "1"}

    def get_order(self, order_id):
        return {"status": self.status}

def reset_metrics():
    order_consumer.orders_success._value.set(0)
    order_consumer.orders_failed._value.set(0)
    MockBroker.orders = []
    MockBroker.status = "COMPLETE"

class DummySession:
    def __init__(self, accounts=None, strategy=None):
        self._accounts = accounts or []
        self._strategy = strategy

    class _Query:
        def __init__(self, session, model):
            self.session = session
            self.model = model

        def get(self, _id):
            return self.session._strategy

        def filter(self, *_args):
            return self

        def all(self):
            return self.session._accounts

    def query(self, model):
        return DummySession._Query(self, model)

    def close(self):
        pass


@pytest.mark.parametrize(
    "symbol",
    [
        "NIFTYNXT5025NOV35500CE",
        "NIFTYNXT5030SEP33300CE",
        "MIDCPNIFTY30SEP33300PE",
    ],
)

def test_normalize_derivative_symbol_preserves_dhan_day_format(symbol):
    assert order_consumer.normalize_derivative_symbol(symbol) == symbol


def test_normalize_derivative_symbol_preserves_dhan_format():
    symbol = "NIFTY09SEP24950PE"
    assert order_consumer.normalize_derivative_symbol(symbol) == symbol


def test_consumer_places_order(monkeypatch):
    event = {"user_id": 1, "symbol": "AAPL", "action": "BUY", "qty": 1, "alert_id": "1"}
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)
    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: MockBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", lambda e: True)

    def settings(_: int):
        return {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]}

    monkeypatch.setattr(order_consumer, "get_user_settings", settings)
    reset_metrics()

    processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)
    assert processed == 1
    assert MockBroker.orders == [
        {"symbol": "AAPL", "action": "BUY", "qty": 1}
    ]
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
            },
        )
    ]


def test_consumer_places_derivative_order(monkeypatch):
    event = {
        "user_id": 1,
        "symbol": "NIFTY24AUGFUT",
        "action": "BUY",
        "qty": 2,
        "alert_id": "1",
        "exchange": "NSE",
        "instrument_type": "FUT",
        "expiry": "2024-08-29",
    }
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)
    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: MockBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", lambda e: True)

    def settings(_: int):
        return {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]}

    monkeypatch.setattr(order_consumer, "get_user_settings", settings)
    # Provide a minimal symbol map with lot size information so that the
    # consumer can resolve the contract size automatically.
    monkeypatch.setattr(
        order_consumer.symbol_map,
        "SYMBOL_MAP",
        {"NIFTY24AUGFUT": {"NFO": {"mock": {"lot_size": 25}}}},
    )
    reset_metrics()

    processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)
    assert processed == 1
    assert MockBroker.orders == [
        {
            "symbol": "NIFTY24AUGFUT",
            "action": "BUY",
            "qty": 50,
            "exchange": "NFO",
            "instrument_type": "FUT",
            "expiry": "2024-08-29",
            "lot_size": 25,
        }
    ]
    assert stub.added == [
        (
            "trade_events",
            {
                "master_id": "c",
                "symbol": "NIFTY24AUGFUT",
                "action": "BUY",
                "qty": 50,
                "exchange": "NFO",
                "instrument_type": "FUT",
                "expiry": "2024-08-29",
                "lot_size": 25,
            },
        )
    ]


def test_consumer_errors_when_lot_size_not_found(monkeypatch, caplog):
    event = {
        "user_id": 1,
        "symbol": "ABC24AUGFUT",
        "action": "BUY",
        "qty": 2,
        "alert_id": "1",
        "exchange": "NSE",
        "instrument_type": "FUT",
        "expiry": "2024-08-29",
    }
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)
    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: MockBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", lambda e: True)

    def settings(_: int):
        return {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]}

    monkeypatch.setattr(order_consumer, "get_user_settings", settings)
    monkeypatch.setattr(
        order_consumer.symbol_map,
        "get_symbol_for_broker",
        lambda symbol, broker, exchange=None: {},
    )
    monkeypatch.setattr(
        order_consumer.symbol_map, "refresh_symbol_map", lambda force=False: None
    )
    reset_metrics()

    with caplog.at_level(logging.ERROR):
        processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)
    assert processed == 1
    assert MockBroker.orders == []
    assert stub.added == []
    assert order_consumer.orders_failed._value.get() == 1
    assert "lot size" in caplog.text


def test_consumer_errors_when_lot_size_is_one(monkeypatch, caplog):
    event = {
        "user_id": 1,
        "symbol": "NIFTY24AUGFUT",
        "action": "BUY",
        "qty": 2,
        "alert_id": "1",
        "exchange": "NSE",
        "instrument_type": "FUT",
        "expiry": "2024-08-29",
    }
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)
    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: MockBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", lambda e: True)

    def settings(_: int):
        return {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]}

    monkeypatch.setattr(order_consumer, "get_user_settings", settings)
    monkeypatch.setattr(
        order_consumer.symbol_map, "get_symbol_for_broker", lambda *a, **k: {"lot_size": 1}
    )
    monkeypatch.setattr(
        order_consumer.symbol_map, "refresh_symbol_map", lambda force=False: None
    )
    reset_metrics()

    with caplog.at_level(logging.ERROR):
        processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)

    assert processed == 1
    assert MockBroker.orders == []
    assert "lot size" in caplog.text

def test_consumer_niftynxt50_fallback_lot_size(monkeypatch):
    """NIFTYNXT50 symbols should not use the generic NIFTY fallback."""
    event = {
        "user_id": 1,
        "symbol": "NIFTYNXT5025SEP38000CE",
        "action": "BUY",
        "qty": 2,
        "alert_id": "1",
        "exchange": "NSE",
    }
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)
    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: MockBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", lambda e: True)

    def settings(_: int):
        return {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]}

    monkeypatch.setattr(order_consumer, "get_user_settings", settings)
    monkeypatch.setattr(order_consumer.symbol_map, "get_symbol_for_broker", lambda *a, **k: {})
    monkeypatch.setattr(order_consumer.symbol_map, "debug_symbol_lookup", lambda *a, **k: {})
    monkeypatch.setattr(
        order_consumer.symbol_map, "refresh_symbol_map", lambda force=False: None
    )
    reset_metrics()

    processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)
    assert processed == 1
    # Lot size should default to 40, not the generic NIFTY 50 value
    assert len(MockBroker.orders) == 1
    order = MockBroker.orders[0]
    assert order["qty"] == 80
    assert order.get("lot_size") == 40
    assert order["symbol"].startswith("NIFTYNXT50")


def test_consumer_refreshes_symbol_map_on_missing_lot_size(monkeypatch):
    event = {
        "user_id": 1,
        "symbol": "NIFTY24AUGFUT",
        "action": "BUY",
        "qty": 2,
        "alert_id": "1",
        "exchange": "NSE",
        "instrument_type": "FUT",
        "expiry": "2024-08-29",
    }
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)
    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: MockBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", lambda e: True)

    def settings(_: int):
        return {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]}

    monkeypatch.setattr(order_consumer, "get_user_settings", settings)

    calls = {"get": 0}

    def fake_get_symbol(symbol, broker, exchange=None):
        calls["get"] += 1
        if calls["get"] == 1:
            return {}
        return {"lot_size": 25}

    monkeypatch.setattr(
        order_consumer.symbol_map, "get_symbol_for_broker", fake_get_symbol
    )

    refreshed = {}

    def fake_refresh(force=False):
        refreshed["force"] = force

    monkeypatch.setattr(
        order_consumer.symbol_map, "refresh_symbol_map", fake_refresh
    )
    reset_metrics()

    processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)
    assert processed == 1
    assert refreshed == {"force": True}
    assert calls["get"] == 2
    assert MockBroker.orders == [
        {
            "symbol": "NIFTY24AUGFUT",
            "action": "BUY",
            "qty": 50,
            "exchange": "NFO",
            "instrument_type": "FUT",
            "expiry": "2024-08-29",
            "lot_size": 25,
        }
    ]
    assert stub.added == [
        (
            "trade_events",
            {
                "master_id": "c",
                "symbol": "NIFTY24AUGFUT",
                "action": "BUY",
                "qty": 50,
                "exchange": "NFO",
                "instrument_type": "FUT",
                "expiry": "2024-08-29",
                "lot_size": 25,
            },
        )
    ]

def test_consumer_derivative_symbol_map_uses_normalized_exchange(monkeypatch):
    events = [
        {
            "user_id": 1,
            "symbol": "NIFTY24AUGFUT",
            "action": "BUY",
            "qty": 1,
            "alert_id": "1",
            "exchange": "NSE",
            "instrument_type": "FUT",
            "expiry": "2024-08-29",
        },
        {
            "user_id": 1,
            "symbol": "SENSEX24AUGFUT",
            "action": "BUY",
            "qty": 1,
            "alert_id": "2",
            "exchange": "BSE",
            "instrument_type": "FUT",
            "expiry": "2024-08-29",
        },
    ]
    stub = StubRedis(events)
    monkeypatch.setattr(order_consumer, "redis_client", stub)
    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: MockBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", lambda e: True)

    def settings(_: int):
        return {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]}

    monkeypatch.setattr(order_consumer, "get_user_settings", settings)

    calls = []

    def fake_get_symbol(symbol, broker, exchange=None):
        calls.append(exchange)
        return {"lot_size": 25}

    monkeypatch.setattr(
        order_consumer.symbol_map, "get_symbol_for_broker", fake_get_symbol
    )
    reset_metrics()

    processed = order_consumer.consume_webhook_events(max_messages=2, redis_client=stub)
    assert processed == 2
    assert calls == ["NFO", "BFO"]
    assert [o["exchange"] for o in MockBroker.orders] == ["NFO", "BFO"]
def test_consumer_publishes_traded_status(monkeypatch):
    """Orders with a Dhan ``TRADED`` status should publish events."""
    event = {"user_id": 1, "symbol": "AAPL", "action": "BUY", "qty": 1, "alert_id": "1"}
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)
    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: MockBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", lambda e: True)

    def settings(_: int):
        return {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]}

    monkeypatch.setattr(order_consumer, "get_user_settings", settings)
    reset_metrics()
    MockBroker.status = "TRADED"

    processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)
    assert processed == 1
    assert MockBroker.orders == [
        {"symbol": "AAPL", "action": "BUY", "qty": 1}
    ]
    assert stub.added == [
        (
            "trade_events",
            {"master_id": "c", "symbol": "AAPL", "action": "BUY", "qty": 1},
        )
    ]


def test_consumer_publishes_pending_status(monkeypatch):
    """Orders with an incomplete status are still published."""
    event = {"user_id": 1, "symbol": "AAPL", "action": "BUY", "qty": 1, "alert_id": "1"}
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)
    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: MockBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", lambda e: True)

    def settings(_: int):
        return {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]}

    monkeypatch.setattr(order_consumer, "get_user_settings", settings)
    reset_metrics()
    MockBroker.status = "PENDING"

    processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)
    assert processed == 1
    assert MockBroker.orders == [
        {"symbol": "AAPL", "action": "BUY", "qty": 1}
    ]
    assert stub.added == [
        (
            "trade_events",
            {"master_id": "c", "symbol": "AAPL", "action": "BUY", "qty": 1},
        )
    ]


def test_consumer_handles_api_key_and_client_id(monkeypatch):
    event = {"user_id": 1, "symbol": "AAPL", "action": "BUY", "qty": 1, "alert_id": "1"}
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)

    class ApiKeyBroker:
        orders = []

        def __init__(self, client_id, api_key, access_token):
            self.client_id = client_id
            self.api_key = api_key
            self.access_token = access_token

        def place_order(self, **order):
            ApiKeyBroker.orders.append(order)
            return {"status": "success", "order_id": "1"}

        def get_order(self, order_id):
            return {"status": "COMPLETE"}

    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: ApiKeyBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", lambda e: True)

    def settings(_: int):
        return {
            "brokers": [
                {
                    "name": "mock",
                    "client_id": "c",
                    "access_token": "t",
                    "api_key": "k",
                }
            ]
        }

    monkeypatch.setattr(order_consumer, "get_user_settings", settings)

    order_consumer.orders_success._value.set(0)
    order_consumer.orders_failed._value.set(0)
    ApiKeyBroker.orders = []

    processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)
    assert processed == 1
    assert ApiKeyBroker.orders == [{"symbol": "AAPL", "action": "BUY", "qty": 1}]
    assert stub.added == [
        (
            "trade_events",
            {"master_id": "c", "symbol": "AAPL", "action": "BUY", "qty": 1},
        )
    ]


def test_consumer_normalizes_camelcase_credentials(monkeypatch):
    """Credentials with camelCase keys should be accepted."""
    event = {"user_id": 1, "symbol": "AAPL", "action": "BUY", "qty": 1, "alert_id": "1"}
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)

    captured = {}

    class CamelBroker:
        def __init__(self, client_id, api_key, access_token):
            captured["client_id"] = client_id
            captured["api_key"] = api_key
            captured["access_token"] = access_token

        def place_order(self, **order):
            return {"status": "success", "order_id": "1"}

        def get_order(self, order_id):
            return {"status": "COMPLETE"}

    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: CamelBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", lambda e: True)

    def settings(_: int):
        return {
            "brokers": [
                {
                    "name": "mock",
                    "clientId": "c",
                    "accessToken": "t",
                    "apiKey": "k",
                }
            ]
        }

    monkeypatch.setattr(order_consumer, "get_user_settings", settings)

    processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)
    assert processed == 1
    assert captured == {"client_id": "c", "api_key": "k", "access_token": "t"}
    assert stub.added == [
        (
            "trade_events",
            {"master_id": "c", "symbol": "AAPL", "action": "BUY", "qty": 1},
        )
    ]

def test_consumer_errors_on_missing_api_key(monkeypatch, caplog):
    event = {"user_id": 1, "symbol": "AAPL", "action": "BUY", "qty": 1, "alert_id": "1"}
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)

    class ApiKeyBroker:
        orders = []

        def __init__(self, client_id, api_key, access_token):
            self.client_id = client_id
            self.api_key = api_key
            self.access_token = access_token

        def place_order(self, **order):
            ApiKeyBroker.orders.append(order)
            return {"status": "success", "order_id": "1"}

        def get_order(self, order_id):
            return {"status": "COMPLETE"}

    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: ApiKeyBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", lambda e: True)

    def settings(_: int):
        return {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]}

    monkeypatch.setattr(order_consumer, "get_user_settings", settings)
    reset_metrics()
    ApiKeyBroker.orders = []

    with caplog.at_level(logging.ERROR):
        processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)

    assert processed == 1
    assert ApiKeyBroker.orders == []
    assert order_consumer.orders_success._value.get() == 0
    # The broker failure is counted once for the specific broker and once more
    # for the overall event which produced no trade events.
    assert order_consumer.orders_failed._value.get() == 2
    assert stub.added == []
    assert "missing required broker credential(s): api_key" in caplog.text

def test_consumer_passes_optional_order_fields(monkeypatch):
    event = {
        "user_id": 1,
        "symbol": "AAPL",
        "action": "BUY",
        "qty": 1,
        "alert_id": "1",
        "productType": "INTRADAY",
        "orderValidity": "DAY",
        "masterAccounts": [1],
    }
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)
    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: MockBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", guard)

    def settings(_: int):
        return {
            "brokers": [
                {"name": "mock", "client_id": "c", "access_token": "t"}
            ]
        }

    monkeypatch.setattr(order_consumer, "get_user_settings", settings)
    account = SimpleNamespace(id=1, broker="mock", client_id="c")
    monkeypatch.setattr(
        order_consumer, "get_session", lambda: DummySession(accounts=[account])
    )
    reset_metrics()

    processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)
    assert processed == 1
    assert MockBroker.orders == [
        {
            "symbol": "AAPL",
            "action": "BUY",
            "qty": 1,
            "product_type": "INTRADAY",
            "validity": "DAY",
            "master_accounts": [1],
        }
    ]
    assert stub.added == [
        (
            "trade_events",
            {
                "master_id": "c",
                "symbol": "AAPL",
                "action": "BUY",
                "qty": 1,
                "product_type": "INTRADAY",
                "validity": "DAY",
            },
        )
    ]


def test_consumer_restricts_to_selected_master_accounts(monkeypatch):
    event = {
        "user_id": 1,
        "symbol": "AAPL",
        "action": "BUY",
        "qty": 1,
        "alert_id": "1",
        "masterAccounts": [1],
    }
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)
    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: MockBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", guard)

    def settings(_: int):
        return {
            "brokers": [
                {"name": "mock", "client_id": "c", "access_token": "t"},
                {"name": "mock", "client_id": "d", "access_token": "t2"},
            ]
        }

    monkeypatch.setattr(order_consumer, "get_user_settings", settings)
    account = SimpleNamespace(id=1, broker="mock", client_id="c")
    monkeypatch.setattr(
        order_consumer, "get_session", lambda: DummySession(accounts=[account])
    )
    reset_metrics()

    processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)
    assert processed == 1
    assert MockBroker.orders == [
        {"symbol": "AAPL", "action": "BUY", "qty": 1, "master_accounts": [1]}
    ]
    assert stub.added == [
        (
            "trade_events",
            {"master_id": "c", "symbol": "AAPL", "action": "BUY", "qty": 1},
        )
    ]


def test_consumer_rejects_non_numeric_master_accounts(monkeypatch, caplog):
    event = {
        "user_id": 1,
        "symbol": "AAPL",
        "action": "BUY",
        "qty": 1,
        "alert_id": "1",
        "masterAccounts": ["bad"],
    }
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)
    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: MockBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", guard)

    def settings(_: int):
        return {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]}

    monkeypatch.setattr(order_consumer, "get_user_settings", settings)
    reset_metrics()

    with caplog.at_level(logging.ERROR):
        processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)

    assert processed == 1
    assert MockBroker.orders == []
    assert order_consumer.orders_success._value.get() == 0
    assert order_consumer.orders_failed._value.get() == 1
    assert stub.added == []
    assert any(
        "non-numeric master account id" in r.message and "bad" in r.message
        for r in caplog.records
    )


def test_consumer_handles_json_encoded_master_accounts(monkeypatch):
    event = {
        "user_id": 1,
        "symbol": "AAPL",
        "action": "BUY",
        "qty": 1,
        "alert_id": "1",
        "masterAccounts": "[1]",
    }
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)
    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: MockBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", guard)

    def settings(_: int):
        return {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]}

    monkeypatch.setattr(order_consumer, "get_user_settings", settings)
    account = SimpleNamespace(id=1, broker="mock", client_id="c")
    monkeypatch.setattr(
        order_consumer, "get_session", lambda: DummySession(accounts=[account])
    )
    reset_metrics()

    processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)
    assert processed == 1
    assert MockBroker.orders == [
        {"symbol": "AAPL", "action": "BUY", "qty": 1, "master_accounts": [1]}
    ]
    assert stub.added == [
        (
            "trade_events",
            {"master_id": "c", "symbol": "AAPL", "action": "BUY", "qty": 1},
        )
    ]


def test_consumer_errors_when_no_brokers_configured(monkeypatch, caplog):
    event = {"user_id": 1, "symbol": "AAPL", "action": "BUY", "qty": 1, "alert_id": "1"}
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)
    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: MockBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", guard)
    monkeypatch.setattr(order_consumer, "get_user_settings", lambda _: {"brokers": []})
    reset_metrics()

    with caplog.at_level(logging.ERROR):
        processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)

    assert processed == 1
    assert MockBroker.orders == []
    assert order_consumer.orders_success._value.get() == 0
    assert order_consumer.orders_failed._value.get() == 1
    assert stub.added == []
    assert any("no brokers configured for user" in r.message for r in caplog.records)

def test_consumer_uses_strategy_master_accounts(monkeypatch):
    event = {
        "user_id": 1,
        "symbol": "AAPL",
        "action": "BUY",
        "qty": 1,
        "alert_id": "1",
        "strategy_id": 99,
    }
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)
    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: MockBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", guard)

    def settings(_: int):
        return {
            "brokers": [
                {"name": "mock", "client_id": "c", "access_token": "t"},
                {"name": "mock", "client_id": "d", "access_token": "t2"},
            ]
        }

    monkeypatch.setattr(order_consumer, "get_user_settings", settings)
    strategy = SimpleNamespace(master_accounts="1")
    account = SimpleNamespace(id=1, broker="mock", client_id="c")
    monkeypatch.setattr(
        order_consumer,
        "get_session",
        lambda: DummySession(accounts=[account], strategy=strategy),
    )
    reset_metrics()

    processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)
    assert processed == 1
    assert MockBroker.orders == [{"symbol": "AAPL", "action": "BUY", "qty": 1}]
    assert stub.added == [
        (
            "trade_events",
            {"master_id": "c", "symbol": "AAPL", "action": "BUY", "qty": 1},
        )
    ]

def test_consumer_missing_order_type_uses_broker_default(monkeypatch):
    event = {"user_id": 1, "symbol": "AAPL", "action": "BUY", "qty": 1, "alert_id": "1"}
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)

    class DefaultingBroker(MockBroker):
        def place_order(self, **order):
            order.setdefault("order_type", "MARKET")
            return super().place_order(**order)

    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: DefaultingBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", guard)

    def settings(_: int):
        return {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]}

    monkeypatch.setattr(order_consumer, "get_user_settings", settings)
    reset_metrics()

    processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)
    assert processed == 1
    assert MockBroker.orders == [
        {"symbol": "AAPL", "action": "BUY", "qty": 1, "order_type": "MARKET"}
    ]
    assert stub.added == [
        (
            "trade_events",
            {"master_id": "c", "symbol": "AAPL", "action": "BUY", "qty": 1},
        )
    ]

def test_consumer_handles_risk_failure(monkeypatch):
    event = {"user_id": 1, "symbol": "AAPL", "action": "BUY", "qty": 10, "alert_id": "1"}
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)
    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: MockBroker)

    def guard(event):
        raise ValidationError("risk")

    monkeypatch.setattr(order_consumer, "check_risk_limits", guard)

    monkeypatch.setattr(order_consumer, "get_user_settings", lambda _: {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]})
    reset_metrics()

    processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)
    assert processed == 1
    assert MockBroker.orders == []
    assert order_consumer.orders_success._value.get() == 0
    assert order_consumer.orders_failed._value.get() == 1
    assert stub.added == []

def test_consumer_handles_broker_failure(monkeypatch, caplog):
    event = {"user_id": 1, "symbol": "AAPL", "action": "BUY", "qty": 1, "alert_id": "1"}
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)

    class FailingBroker(MockBroker):
        def place_order(self, **order):
            MockBroker.orders.append(order)
            return {"status": "failure"}

    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: FailingBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", lambda e: True)
    monkeypatch.setattr(order_consumer, "get_user_settings", lambda _: {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]})
    reset_metrics()

    with caplog.at_level(logging.ERROR):
        processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)

    assert processed == 1
    assert stub.added == []
    assert any("failed to place master order" in rec.message for rec in caplog.records)

def test_consumer_skips_rejected_order(monkeypatch):
    event = {"user_id": 1, "symbol": "AAPL", "action": "BUY", "qty": 1, "alert_id": "1"}
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)

    class RejectingBroker(MockBroker):
        def get_order(self, order_id):
            return {"status": "REJECTED"}

    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: RejectingBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", lambda e: True)
    monkeypatch.setattr(
        order_consumer,
        "get_user_settings",
        lambda _: {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]},
    )
    reset_metrics()

    processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)
    assert processed == 1
    assert stub.added == []
    assert order_consumer.orders_success._value.get() == 0
    assert order_consumer.orders_failed._value.get() == 1

def test_consumer_skips_duplicate_check(monkeypatch):
    event = {"user_id": 1, "symbol": "AAPL", "action": "BUY", "qty": 1, "alert_id": "1"}
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)
    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: MockBroker)

    def explode(event):
        raise AssertionError("duplicate check called")

    monkeypatch.setattr(order_consumer, "check_duplicate_and_risk", explode, raising=False)
    monkeypatch.setattr(order_consumer, "check_risk_limits", lambda e: True)
    monkeypatch.setattr(order_consumer, "get_user_settings", lambda _: {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]})
    reset_metrics()

    processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)
    assert processed == 1
    assert MockBroker.orders == [{"symbol": "AAPL", "action": "BUY", "qty": 1}]
    assert order_consumer.orders_success._value.get() == 1
    assert order_consumer.orders_failed._value.get() == 0


def test_consumer_places_orders_for_multiple_brokers(monkeypatch):
    event = {"user_id": 1, "symbol": "AAPL", "action": "BUY", "qty": 1, "alert_id": "1"}
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)
    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: MockBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", lambda e: True)

    def settings(_: int):
        return {
            "brokers": [
                {"name": "mock", "client_id": "c1", "access_token": "t1"},
                {"name": "mock", "client_id": "c2", "access_token": "t2"},
            ]
        }

    monkeypatch.setattr(order_consumer, "get_user_settings", settings)
    reset_metrics()

    processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)
    assert processed == 1
    assert len(MockBroker.orders) == 2
    assert order_consumer.orders_success._value.get() == 1
    assert order_consumer.orders_failed._value.get() == 0
    expected = [
        (
            "trade_events",
            {
                "master_id": "c1",
                "symbol": "AAPL",
                "action": "BUY",
                "qty": 1,
            },
        ),
        (
            "trade_events",
            {
                "master_id": "c2",
                "symbol": "AAPL",
                "action": "BUY",
                "qty": 1,
            },
        ),
    ]
    assert sorted(stub.added, key=lambda x: x[1]["master_id"]) == expected


def test_consumer_processes_events_concurrently(monkeypatch):
    import threading, time

    events = [
        {"user_id": 1, "symbol": "A", "action": "BUY", "qty": 1, "alert_id": "1"},
        {"user_id": 1, "symbol": "B", "action": "BUY", "qty": 1, "alert_id": "2"},
    ]

    class BatchRedis:
        def __init__(self):
            self.added = []

        def xgroup_create(self, *_, **__):
            pass

        def xreadgroup(self, group, consumer, streams, count, block):
            if events:
                batch = []
                for idx, e in enumerate(list(events), 1):
                    batch.append((str(idx).encode(), e))
                events.clear()
                return [("webhook_events", batch)]
            return []

        def xack(self, *_, **__):
            pass

        def xadd(self, stream, data):
            self.added.append((stream, data))

    barrier = threading.Barrier(2)

    class ConcurrentBroker(MockBroker):
        def place_order(self, **order):
            barrier.wait()
            time.sleep(0.1)
            return super().place_order(**order)

    stub = BatchRedis()
    monkeypatch.setattr(order_consumer, "redis_client", stub)
    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: ConcurrentBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", lambda e: True)
    monkeypatch.setattr(
        order_consumer,
        "get_user_settings",
        lambda _: {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]},
    )
    reset_metrics()

    start = time.perf_counter()
    processed = order_consumer.consume_webhook_events(
        max_messages=2, redis_client=stub, batch_size=2
    )
    duration = time.perf_counter() - start

    assert processed == 2
    assert duration < 0.25
    assert len(stub.added) == 2


def test_consumer_respects_max_workers(monkeypatch):
    event = {"user_id": 1, "symbol": "AAPL", "action": "BUY", "qty": 1, "alert_id": "1"}
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)
    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: MockBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", lambda e: True)

    def settings(_: int):
        return {
            "brokers": [
                {"name": "mock", "client_id": "c1", "access_token": "t1"},
                {"name": "mock", "client_id": "c2", "access_token": "t2"},
            ]
        }

    monkeypatch.setattr(order_consumer, "get_user_settings", settings)

    class RecordingExecutor(ThreadPoolExecutor):
        calls: list[int] = []

        def __init__(self, max_workers, *a, **k):
            RecordingExecutor.calls.append(max_workers)
            super().__init__(max_workers, *a, **k)

    monkeypatch.setattr(order_consumer, "ThreadPoolExecutor", RecordingExecutor)
    reset_metrics()
    processed = order_consumer.consume_webhook_events(
        max_messages=1, redis_client=stub, max_workers=1
    )
    assert processed == 1
    assert RecordingExecutor.calls == [1]


def test_consumer_reuses_thread_pool(monkeypatch):
    import threading

    events = [
        {"user_id": 1, "symbol": "A", "action": "BUY", "qty": 1, "alert_id": "1"},
        {"user_id": 1, "symbol": "B", "action": "BUY", "qty": 1, "alert_id": "2"},
    ]

    class MultiRedis:
        def __init__(self):
            self.added = []

        def xgroup_create(self, *_, **__):
            pass

        def xreadgroup(self, group, consumer, streams, count, block):
            if events:
                e = events.pop(0)
                return [("webhook_events", [(b"1", e)])]
            return []

        def xack(self, *_, **__):
            pass

        def xadd(self, stream, data):
            self.added.append((stream, data))

    thread_names: list[str] = []

    class RecordingBroker(MockBroker):
        def place_order(self, **order):
            thread_names.append(threading.current_thread().name)
            return super().place_order(**order)

    class RecordingExecutor(ThreadPoolExecutor):
        instances = 0

        def __init__(self, *a, **k):
            RecordingExecutor.instances += 1
            super().__init__(*a, **k)

    stub = MultiRedis()
    monkeypatch.setattr(order_consumer, "redis_client", stub)
    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: RecordingBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", lambda e: True)
    monkeypatch.setattr(order_consumer, "get_user_settings", lambda _: {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]})
    monkeypatch.setattr(order_consumer, "ThreadPoolExecutor", RecordingExecutor)

    reset_metrics()
    processed = order_consumer.consume_webhook_events(
        max_messages=2, redis_client=stub, batch_size=1, max_workers=1
    )

    assert processed == 2
    assert len(set(thread_names)) == 1
    assert RecordingExecutor.instances == 1


def test_consumer_times_out_slow_broker(monkeypatch):
    import time

    event = {"user_id": 1, "symbol": "AAPL", "action": "BUY", "qty": 1, "alert_id": "1"}
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)

    class SlowBroker(MockBroker):
        def place_order(self, **order):
            if self.client_id == "slow":
                time.sleep(0.2)
            return super().place_order(**order)

    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: SlowBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", lambda e: True)

    def settings(_: int):
        return {
            "brokers": [
                {"name": "mock", "client_id": "slow", "access_token": "t1"},
                {"name": "mock", "client_id": "fast", "access_token": "t2"},
            ]
        }

    monkeypatch.setattr(order_consumer, "get_user_settings", settings)
    reset_metrics()
    processed = order_consumer.consume_webhook_events(
        max_messages=1, redis_client=stub, order_timeout=0.05, max_workers=2
    )

    assert processed == 1
    assert any(te[1]["master_id"] == "fast" for te in stub.added)
    assert all(te[1]["master_id"] != "slow" for te in stub.added)
    assert order_consumer.orders_failed._value.get() >= 1


def test_consumer_timeout_does_not_scale(monkeypatch):
    import time

    event = {"user_id": 1, "symbol": "AAPL", "action": "BUY", "qty": 1, "alert_id": "1"}
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)

    class SlowBroker(MockBroker):
        def place_order(self, **order):
            time.sleep(0.2)
            return super().place_order(**order)

    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: SlowBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", lambda e: True)

    def settings(_: int):
        return {
            "brokers": [
                {"name": "mock", "client_id": "c1", "access_token": "t1"},
                {"name": "mock", "client_id": "c2", "access_token": "t2"},
            ]
        }

    monkeypatch.setattr(order_consumer, "get_user_settings", settings)
    reset_metrics()

    start = time.perf_counter()
    processed = order_consumer.consume_webhook_events(
        max_messages=1, redis_client=stub, order_timeout=0.1, max_workers=2
    )
    duration = time.perf_counter() - start

    assert processed == 1
    assert duration < 0.2
    assert stub.added == []
    assert order_consumer.orders_failed._value.get() >= 3


def test_env_overrides_default_timeout(monkeypatch):
    event = {"user_id": 1, "symbol": "AAPL", "action": "BUY", "qty": 1, "alert_id": "1"}

    def settings(_: int):
        return {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]}

    timeouts: list[float] = []

    def fake_wait(futures, timeout):
        timeouts.append(timeout)
        return (futures, set())

    monkeypatch.setattr(order_consumer, "wait", fake_wait)
    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: MockBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", lambda e: True)
    monkeypatch.setattr(order_consumer, "get_user_settings", settings)

    # first call with one timeout value
    stub = StubRedis([event])
    monkeypatch.setenv("ORDER_CONSUMER_TIMEOUT", "0.1")
    order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)

    # second call with a different timeout
    stub = StubRedis([event])
    monkeypatch.setenv("ORDER_CONSUMER_TIMEOUT", "0.2")
    order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)

    assert timeouts == [0.1, 0.2]

def test_consumer_accepts_camel_case_order_id(monkeypatch, caplog):
    event = {"user_id": 1, "symbol": "AAPL", "action": "BUY", "qty": 1, "alert_id": "1"}
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)

    class CamelBroker(MockBroker):
        def place_order(self, **order):
            MockBroker.orders.append(order)
            return {"status": "success", "orderId": "1"}

    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: CamelBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", lambda e: True)
    monkeypatch.setattr(
        order_consumer,
        "get_user_settings",
        lambda _: {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]},
    )
    reset_metrics()

    with caplog.at_level(logging.ERROR):
        processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)

    assert processed == 1
    assert MockBroker.orders == [{"symbol": "AAPL", "action": "BUY", "qty": 1}]
    assert not caplog.records
    assert order_consumer.orders_success._value.get() == 1
    assert order_consumer.orders_failed._value.get() == 0


def test_consumer_logs_error_when_order_id_missing(monkeypatch, caplog):
    event = {"user_id": 1, "symbol": "AAPL", "action": "BUY", "qty": 1, "alert_id": "1"}
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)

    class NoIdBroker(MockBroker):
        def place_order(self, **order):
            MockBroker.orders.append(order)
            return {"status": "success"}

    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: NoIdBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", lambda e: True)
    monkeypatch.setattr(
        order_consumer,
        "get_user_settings",
        lambda _: {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]},
    )
    reset_metrics()

    with caplog.at_level(logging.ERROR):
        processed = order_consumer.consume_webhook_events(max_messages=1, redis_client=stub)

    assert processed == 1
    assert MockBroker.orders == [{"symbol": "AAPL", "action": "BUY", "qty": 1}]
    assert any("failed to place master order" in r.message for r in caplog.records)
    assert order_consumer.orders_success._value.get() == 0
    assert order_consumer.orders_failed._value.get() >= 1



def test_slow_broker_logs_late_success(monkeypatch, caplog):
    event = {"user_id": 1, "symbol": "AAPL", "action": "BUY", "qty": 1, "alert_id": "1"}
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)

    class SlowBroker(MockBroker):
        def place_order(self, **order):
            time.sleep(0.05)
            return super().place_order(**order)

    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: SlowBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", guard)
    monkeypatch.setattr(
        order_consumer,
        "get_user_settings",
        lambda _: {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]},
    )
    reset_metrics()

    with caplog.at_level(logging.WARNING):
        processed = order_consumer.consume_webhook_events(
            max_messages=1, redis_client=stub, order_timeout=0.01
        )
    assert processed == 1
    assert any("broker order timed out" in r.message for r in caplog.records)
    time.sleep(0.06)
    assert any("completed after timeout" in r.message for r in caplog.records)


def test_slow_broker_logs_late_failure(monkeypatch, caplog):
    event = {"user_id": 1, "symbol": "AAPL", "action": "BUY", "qty": 1, "alert_id": "1"}
    stub = StubRedis([event])
    monkeypatch.setattr(order_consumer, "redis_client", stub)

    class FailingSlowBroker(MockBroker):
        def place_order(self, **order):
            time.sleep(0.05)
            raise RuntimeError("boom")

    monkeypatch.setattr(order_consumer, "get_broker_client", lambda name: FailingSlowBroker)
    monkeypatch.setattr(order_consumer, "check_risk_limits", guard)
    monkeypatch.setattr(
        order_consumer,
        "get_user_settings",
        lambda _: {"brokers": [{"name": "mock", "client_id": "c", "access_token": "t"}]},
    )
    reset_metrics()

    with caplog.at_level(logging.WARNING):
        processed = order_consumer.consume_webhook_events(
            max_messages=1, redis_client=stub, order_timeout=0.01
        )
    assert processed == 1
    assert any("broker order timed out" in r.message for r in caplog.records)
    time.sleep(0.06)
    assert any("failed after timeout" in r.message for r in caplog.records)
