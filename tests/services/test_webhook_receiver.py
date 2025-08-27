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
    assert event["order_type"] == "market"
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
    assert event["alert_id"] is None
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
