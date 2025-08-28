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

    def fake_mapper(symbol, broker, exchange=None):
        return {"security_id": "XYZ123", "exchange_segment": "NSE_EQ"}
    monkeypatch.setattr('brokers.dhan.get_symbol_for_broker', fake_mapper)

    br = DhanBroker('C1', 'token')
    result = br.place_order(symbol='AAPL', action='BUY', qty=5)
    assert result['status'] == 'success'
    assert captured['payload']['securityId'] == 'XYZ123'
    assert captured['payload']['transactionType'] == 'BUY'
    assert captured['payload']['quantity'] == 5

def test_symbol_map_injection_does_not_override_exchange(monkeypatch):
    """Ensure provided symbol maps do not clobber the per-exchange lookup."""
    captured = {}

    def fake_request(self, method, url, **kwargs):
        captured['payload'] = kwargs.get('json')
        return Resp({"orderId": "1"})

    monkeypatch.setattr(DhanBroker, '_request', fake_request, raising=False)

    def fake_mapper(symbol, broker, exchange=None):
        assert exchange == 'BSE'
        return {"security_id": "500325", "exchange_segment": "BSE_EQ"}

    monkeypatch.setattr('brokers.dhan.get_symbol_for_broker', fake_mapper)

    # Inject a symbol map that only knows the NSE security id
    br = DhanBroker('C1', 'token', symbol_map={'RELIANCE': '2885'})
    result = br.place_order(symbol='RELIANCE', action='BUY', qty=1, exchange='BSE')

    assert result['status'] == 'success'
    assert captured['payload']['securityId'] == '500325'
    assert captured['payload']['exchangeSegment'] == 'BSE_EQ'
