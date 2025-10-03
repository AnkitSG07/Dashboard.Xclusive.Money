import brokers.fyers as fyers_module
from brokers.fyers import FyersBroker


class DummyAPI:
    def __init__(self):
        self.captured = None

    def place_order(self, data):
        self.captured = data
        return {"s": "ok", "id": "1"}


def test_place_order_accepts_generic_params():
    br = FyersBroker("C1", "token")
    br.api = DummyAPI()
    result = br.place_order(symbol="SBIN", action="buy", qty=1, product_type="mis")

    assert result["status"] == "success"
    payload = br.api.captured
    assert payload["symbol"] == "NSE:SBIN-EQ"
    assert payload["qty"] == 1
    assert payload["side"] == 1
    assert payload["productType"] == "INTRADAY"



def test_initializes_sdk_with_combined_token(monkeypatch):
    captured = {}

    class DummyFyersModel:
        def __init__(self, token, client_id):
            captured["token"] = token
            captured["client_id"] = client_id

    dummy_module = type("DummyModule", (), {"FyersModel": DummyFyersModel})
    monkeypatch.setattr(fyers_module, "fyersModel", dummy_module)

    FyersBroker("APP123", "access-token")

    assert captured["token"] == "APP123:access-token"
    assert captured["client_id"] == "APP123"
