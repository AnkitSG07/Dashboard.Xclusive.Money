from brokers.dhan import DhanBroker

class Resp:
    def __init__(self, data, payload=None):
        self._data = data
        self.payload = payload
    def json(self):
        return self._data

def test_place_order_accepts_generic_params(monkeypatch):
    captured = {}
    def fake_request(self, method, url, **kwargs):
        captured['url'] = url
        captured['payload'] = kwargs.get('json')
        return Resp({"orderId": "1"})
    monkeypatch.setattr(DhanBroker, '_request', fake_request, raising=False)

    def fake_mapper(symbol, broker):
        return {"security_id": "XYZ123", "exchange_segment": "NSE_EQ"}
    monkeypatch.setattr('brokers.dhan.get_symbol_for_broker', fake_mapper)

    br = DhanBroker('C1', 'token')
    result = br.place_order(symbol='AAPL', action='BUY', qty=5)
    assert result['status'] == 'success'
    assert captured['payload']['securityId'] == 'XYZ123'
    assert captured['payload']['transactionType'] == 'BUY'
    assert captured['payload']['quantity'] == 5
