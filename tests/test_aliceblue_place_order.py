from brokers.aliceblue import AliceBlueBroker
import json


class Resp:
    def __init__(self, data, payload=None):
        self._data = data
        self.payload = payload

    def json(self):
        return self._data


def test_place_order_accepts_generic_params(monkeypatch):
    captured = {}

    def fake_auth(self):
        self.session_id = "SID"
        self.headers = {}

    monkeypatch.setattr(AliceBlueBroker, "authenticate", fake_auth, raising=False)

    def fake_request(self, method, url, **kwargs):
        captured["payload"] = json.loads(kwargs.get("data"))
        return Resp([{"stat": "Ok", "nestOrderNumber": "1"}])

    monkeypatch.setattr(AliceBlueBroker, "_request", fake_request, raising=False)

    def fake_mapper(symbol, broker):
        return {"symbol_id": "123", "trading_symbol": "AAPL", "exch": "NSE"}

    monkeypatch.setattr("brokers.aliceblue.get_symbol_for_broker", fake_mapper)

    br = AliceBlueBroker("C1", "token")
    result = br.place_order(symbol="AAPL", action="BUY", qty=5)

    assert result["status"] == "success"
    payload = captured["payload"][0]
    assert payload["symbol_id"] == "123"
    assert payload["trading_symbol"] == "AAPL"
    assert payload["qty"] == 5
    assert payload["transtype"] == "BUY"

def test_place_order_handles_nordno(monkeypatch):
    def fake_auth(self):
        self.session_id = "SID"
        self.headers = {}

    monkeypatch.setattr(AliceBlueBroker, "authenticate", fake_auth, raising=False)

    def fake_request(self, method, url, **kwargs):
        return Resp([{"stat": "Ok", "NOrdNo": "10"}])

    monkeypatch.setattr(AliceBlueBroker, "_request", fake_request, raising=False)

    def fake_mapper(symbol, broker):
        return {"symbol_id": "123", "trading_symbol": "AAPL", "exch": "NSE"}

    monkeypatch.setattr("brokers.aliceblue.get_symbol_for_broker", fake_mapper)

    br = AliceBlueBroker("C1", "token")
    result = br.place_order(symbol="AAPL", action="BUY", qty=1)

    assert result["status"] == "success"
    assert result["order_id"] == "10"


def test_place_order_handles_nordno(monkeypatch):
    def fake_auth(self):
        self.session_id = "SID"
        self.headers = {}

    monkeypatch.setattr(AliceBlueBroker, "authenticate", fake_auth, raising=False)

    def fake_request(self, method, url, **kwargs):
        return Resp([{"stat": "Ok", "NOrdNo": "10"}])

    monkeypatch.setattr(AliceBlueBroker, "_request", fake_request, raising=False)

    def fake_mapper(symbol, broker):
        return {"symbol_id": "123", "trading_symbol": "AAPL", "exch": "NSE"}

    monkeypatch.setattr("brokers.aliceblue.get_symbol_for_broker", fake_mapper)

    br = AliceBlueBroker("C1", "token")
    result = br.place_order(symbol="AAPL", action="BUY", qty=1)

    assert result["status"] == "success"
    assert result["order_id"] == "10"


def test_place_order_handles_status_whitespace(monkeypatch):
    def fake_auth(self):
        self.session_id = "SID"
        self.headers = {}

    monkeypatch.setattr(AliceBlueBroker, "authenticate", fake_auth, raising=False)

    def fake_request(self, method, url, **kwargs):
        # API returns status with trailing space
        return Resp([{"stat": "Ok ", "NOrdNo": "42"}])

    monkeypatch.setattr(AliceBlueBroker, "_request", fake_request, raising=False)

    def fake_mapper(symbol, broker):
        return {"symbol_id": "123", "trading_symbol": "AAPL", "exch": "NSE"}

    monkeypatch.setattr("brokers.aliceblue.get_symbol_for_broker", fake_mapper)

    br = AliceBlueBroker("C1", "token")
    result = br.place_order(symbol="AAPL", action="BUY", qty=1)

    assert result["status"] == "success"
    assert result["order_id"] == "42"


def test_get_order_list_infers_complete_status(monkeypatch):
    def fake_auth(self):
        self.session_id = "SID"
        self.headers = {}

    monkeypatch.setattr(AliceBlueBroker, "authenticate", fake_auth, raising=False)

    def fake_trade_book(self):
        return {"status": "success", "trades": [{"NOrdNo": "11", "Fillshares": "1"}]}

    monkeypatch.setattr(AliceBlueBroker, "get_trade_book", fake_trade_book, raising=False)

    br = AliceBlueBroker("C1", "token")

    resp = br.get_order_list()
    assert resp["status"] == "success"
    order = resp["data"][0]
    assert order["order_id"] == "11"
    assert order["status"] == "COMPLETE"

    assert br.list_orders()[0]["status"] == "COMPLETE"
    assert br.get_order("11")["status"] == "COMPLETE"
