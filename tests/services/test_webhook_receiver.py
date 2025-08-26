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
        "exchange": "NSE",
        "orderType": "market",
        "orderValidity": "day",
        "productType": "intraday",
        "masterAccounts": ["50"],
        "transactionType": "buy",
        "tradingSymbols": ["AAPL"],
        "alert_id": "abc",
    }
    event = wr.enqueue_webhook(1, None, payload)
    assert event["action"] == "BUY"
    assert event["qty"] == 5
    assert event["exchange"] == "NSE"
    assert event["order_type"] == "market"
    assert event["orderType"] == "market"
    assert event["alert_id"] == "abc"
    assert stub.messages


def test_missing_optional_fields(monkeypatch):
    stub = StubRedis()
    monkeypatch.setattr(wr, "redis_client", stub)
    monkeypatch.setattr(wr, "check_duplicate_and_risk", lambda e: True)

    payload = {"symbol": "AAPL", "action": "SELL", "qty": 1}
    event = wr.enqueue_webhook(1, 2, payload)
    assert event["exchange"] is None
    assert event["order_type"] is None
    assert event["alert_id"] is None
    assert event["orderType"] is None
    assert event["productType"] is None
