import datetime

import pytest
from marshmallow import ValidationError

from services import webhook_receiver as wr


class StubRedis:
    def __init__(self):
        self.messages = []

    def xadd(self, stream, data):
        self.messages.append((stream, data))


def test_optional_fields_and_aliases(monkeypatch):
    stub = StubRedis()
    monkeypatch.setattr(wr, "redis_client", stub)
    monkeypatch.setattr(wr, "check_duplicate_and_risk", lambda e: True)

    payload = {
        "ticker": "AAPL",
        "side": "buy",
        "orderQty": 5,
        "exchange": "nse",
        "orderType": "market",
        "orderValidity": "day",
        "productType": "intraday",
        "masterAccounts": ["50"],
        "transactionType": "buy",
        "tradingSymbols": ["AAPL"],
        "alert_id": "abc",
    }
    event = wr.enqueue_webhook(1, None, payload)
    assert event["symbol"] == "AAPL-EQ"
    assert event["action"] == "BUY"
    assert event["qty"] == 5
    assert event["exchange"] == "NSE"
    assert event["order_type"] == "MARKET"
    assert event["orderType"] == "market"
    assert event["orderValidity"] == "DAY"
    assert event["productType"] == "INTRADAY"
    assert event["alert_id"] == "abc"
    assert stub.messages


def test_missing_optional_fields(monkeypatch):
    stub = StubRedis()
    monkeypatch.setattr(wr, "redis_client", stub)
    monkeypatch.setattr(wr, "check_duplicate_and_risk", lambda e: True)

    payload = {"symbol": "aapl", "action": "SELL", "qty": 1}
    event = wr.enqueue_webhook(1, 2, payload)
    assert event["symbol"] == "AAPL-EQ"
    assert event["exchange"] == "NSE"
    assert event["order_type"] is None
    assert isinstance(event["alert_id"], str)
    assert event["orderType"] is None
    assert event["productType"] is None

def test_uppercases_product_type_and_order_validity(monkeypatch):
    stub = StubRedis()
    monkeypatch.setattr(wr, "redis_client", stub)
    monkeypatch.setattr(wr, "check_duplicate_and_risk", lambda e: True)

    payload = {
        "symbol": "AAPL",
        "action": "buy",
        "qty": 1,
        "productType": "intraday",
        "orderValidity": "day",
    }
    event = wr.enqueue_webhook(1, None, payload)
    assert event["productType"] == "INTRADAY"
    assert event["orderValidity"] == "DAY"


def test_symbol_preserves_existing_suffix(monkeypatch):
    stub = StubRedis()
    monkeypatch.setattr(wr, "redis_client", stub)
    monkeypatch.setattr(wr, "check_duplicate_and_risk", lambda e: True)

    payload = {"symbol": "IDEA-EQ", "action": "buy", "qty": 1, "exchange": "BSE"}
    event = wr.enqueue_webhook(1, None, payload)
    assert event["symbol"] == "IDEA-EQ"
    assert event["exchange"] == "BSE"


def test_bse_equity_lot_size(monkeypatch):
    stub = StubRedis()
    monkeypatch.setattr(wr, "redis_client", stub)
    monkeypatch.setattr(wr, "check_duplicate_and_risk", lambda e: True)

    captured = {}

    def fake_get_symbol_for_broker(symbol, broker, exchange=None):
        captured["call"] = (symbol, broker, exchange)
        return {"lot_size": "600"}

    monkeypatch.setattr(wr.symbol_map, "SYMBOL_MAP", {"SBVCL-EQ": {}})
    monkeypatch.setattr(wr.symbol_map, "get_symbol_for_broker", fake_get_symbol_for_broker)

    payload = {"symbol": "SBVCL", "action": "buy", "qty": 1, "exchange": "BSE"}
    event = wr.enqueue_webhook(1, None, payload)

    assert captured["call"] == ("SBVCL-EQ", "dhan", "BSE")
    assert event["symbol"] == "SBVCL-EQ"
    assert event["lot_size"] == 600

def test_parses_human_readable_futures_symbol(monkeypatch):
    stub = StubRedis()
    monkeypatch.setattr(wr, "redis_client", stub)
    monkeypatch.setattr(wr, "check_duplicate_and_risk", lambda e: True)

    class FixedDate(datetime.date):
        @classmethod
        def today(cls):
            return cls(2024, 9, 1)

    monkeypatch.setattr(wr, "date", FixedDate)

    payload = {"symbol": "NIFTY SEP FUT", "action": "buy", "qty": 1}
    event = wr.enqueue_webhook(1, None, payload)
    assert event["symbol"] == "NIFTY24SEPFUT"
    assert event["exchange"] == "NFO"


def test_rejects_invalid_human_futures_symbol(monkeypatch):
    stub = StubRedis()
    monkeypatch.setattr(wr, "redis_client", stub)
    monkeypatch.setattr(wr, "check_duplicate_and_risk", lambda e: True)

    payload = {"symbol": "NIFTY BAD FUT", "action": "buy", "qty": 1}
    with pytest.raises(ValidationError):
        wr.enqueue_webhook(1, None, payload)



@pytest.mark.parametrize(
    "symbol,expected",
    [
        ("NIFTY SEP 24500 CALL", "NIFTY24SEP24500CE"),
        ("NIFTY SEP 24500 PUT", "NIFTY24SEP24500PE"),
    ],
)
def test_parses_human_readable_option_symbol(monkeypatch, symbol, expected):
    stub = StubRedis()
    monkeypatch.setattr(wr, "redis_client", stub)
    monkeypatch.setattr(wr, "check_duplicate_and_risk", lambda e: True)

    class FixedDate(datetime.date):
        @classmethod
        def today(cls):
            return cls(2024, 9, 1)

    monkeypatch.setattr(wr, "date", FixedDate)

    payload = {"symbol": symbol, "action": "buy", "qty": 1}
    event = wr.enqueue_webhook(1, None, payload)
    assert event["symbol"] == expected
    assert event["exchange"] == "NFO"


@pytest.mark.parametrize(
    "symbol,expected",
    [
        ("NIFTYNXT50 SEP FUT", "NIFTYNXT5024SEPFUT"),
        ("MIDCPNIFTY SEP 50000 CALL", "MIDCPNIFTY24SEP50000CE"),
    ],
)
def test_parses_symbols_with_numeric_roots(monkeypatch, symbol, expected):
    stub = StubRedis()
    monkeypatch.setattr(wr, "redis_client", stub)
    monkeypatch.setattr(wr, "check_duplicate_and_risk", lambda e: True)

    class FixedDate(datetime.date):
        @classmethod
        def today(cls):
            return cls(2024, 8, 1)

    monkeypatch.setattr(wr, "date", FixedDate)

    payload = {"symbol": symbol, "action": "buy", "qty": 1}
    event = wr.enqueue_webhook(1, None, payload)
    assert event["symbol"] == expected
    assert event["exchange"] == "NFO"
