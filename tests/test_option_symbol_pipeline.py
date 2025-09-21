from brokers.dhan import DhanBroker
from services import webhook_receiver

class Resp:
    def __init__(self, data, payload=None):
        self._data = data
        self.payload = payload
    def json(self):
        return self._data

def test_option_symbol_pipeline(monkeypatch):
    """Option symbols should retain base exchange and map to F&O segment."""
    class DummyRedis:
        def xadd(self, stream, data):
            pass

    monkeypatch.setattr(webhook_receiver, "redis_client", DummyRedis())
    monkeypatch.setattr(webhook_receiver, "check_duplicate_and_risk", lambda e: True)

    event = webhook_receiver.enqueue_webhook(
        1,
        None,
        {"symbol": "INDUSTOWER 28 OCT 205 PUT", "action": "BUY", "qty": 1},
    )

    captured = {}

    def fake_request(self, method, url, **kwargs):
        captured["payload"] = kwargs.get("json")
        return Resp({"orderId": "1"})

    monkeypatch.setattr(DhanBroker, "_request", fake_request, raising=False)

    def fake_mapper(symbol, broker, exchange=None):
        assert exchange == "NFO"
        return {"security_id": "XYZ123", "exchange_segment": "NSE_FNO"}

    monkeypatch.setattr("brokers.dhan.get_symbol_for_broker_lazy", fake_mapper)

    br = DhanBroker("C1", "token")
    result = br.place_order(
        symbol=event["symbol"],
        action=event["action"],
        qty=event["qty"],
        exchange=event["exchange"],
    )

    assert result["status"] == "success"
    assert captured["payload"]["exchangeSegment"] == "NSE_FNO"
