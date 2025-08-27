from brokers.flattrade import FlattradeBroker
import time
import requests


class DummyResponse:
    def __init__(self):
        self._data = {"status": "Success", "result": "ok", "orderid": "1"}

    def raise_for_status(self):
        pass

    def json(self):
        return self._data


def test_place_order_accepts_generic_params(monkeypatch):
    captured = {}

    def fake_post(url, json, headers):
        captured["payload"] = json
        return DummyResponse()

    monkeypatch.setattr(requests, "post", fake_post)

    br = FlattradeBroker("C1", access_token="token")
    br.session_expiry = time.time() + 1000

    result = br.place_order(symbol="SBIN", action="sell", qty=1, product_type="mis")

    assert result["status"] == "success"
    payload = captured["payload"]
    assert payload["tradingsymbol"] == "SBIN"
    assert payload["transactiontype"] == "SELL"
    assert payload["quantity"] == 1
    assert payload["producttype"] == "MIS"
