from brokers.zerodha import ZerodhaBroker


class DummyKite:
    VARIETY_REGULAR = "regular"

    def __init__(self, api_key):
        self.api_key = api_key
        self.captured = None

    def set_access_token(self, token):
        pass

    def place_order(self, variety, **params):
        self.captured = params
        return "1"


def test_place_order_accepts_generic_params(monkeypatch):
    # Patch KiteConnect and symbol map lookup
    monkeypatch.setattr("brokers.zerodha.KiteConnect", DummyKite)
    monkeypatch.setattr("brokers.zerodha.get_symbol_for_broker", lambda symbol, broker: {"trading_symbol": symbol, "exchange": "NSE"})

    br = ZerodhaBroker("C1", access_token="token", api_key="key")
    result = br.place_order(symbol="TCS", action="buy", qty=1)

    assert result["status"] == "success"
    params = br.kite.captured
    assert params["tradingsymbol"] == "TCS"
    assert params["transaction_type"] == "BUY"
    assert params["quantity"] == 1
    assert params["product"] == "MIS"
