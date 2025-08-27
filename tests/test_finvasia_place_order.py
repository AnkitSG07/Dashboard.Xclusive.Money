from brokers.finvasia import FinvasiaBroker


class DummyAPI:
    def __init__(self):
        self.captured = None

    def place_order(self, **params):
        self.captured = params
        return {"stat": "Ok", "norenordno": "1"}


def test_place_order_accepts_generic_params(monkeypatch):
    # Avoid network calls or real login
    monkeypatch.setattr(FinvasiaBroker, "_is_logged_in", lambda self: True)

    br = FinvasiaBroker("C1", imei="123")
    br.api = DummyAPI()

    result = br.place_order(symbol="IDEA-EQ", action="BUY", qty=5, exchange="NSE")

    assert result["status"] == "success"
    params = br.api.captured
    assert params["tradingsymbol"] == "IDEA-EQ"
    assert params["buy_or_sell"] == "B"
    assert params["quantity"] == 5
