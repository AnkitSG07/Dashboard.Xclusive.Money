from brokers.dhan import DhanBroker

class Resp:
    def __init__(self, data):
        self._data = data
    def json(self):
        return self._data

def test_get_positions_enriches_ltp_and_pnl(monkeypatch):
    def fake_request(self, method, url, **kwargs):
        if url.endswith('/positions'):
            return Resp({'data': [
                {
                    'securityId': '101',
                    'exchangeSegment': 'NSE_EQ',
                    'buyQty': '1',
                    'sellQty': '0',
                    'buyAvg': '100'
                }
            ]})
        elif url.endswith('/marketfeed/ltp'):
            assert kwargs.get('json') == {'NSE_EQ': [101]}
            return Resp({'data': {'NSE_EQ': {'101': {'ltp': 110}}}})
        raise AssertionError('unexpected url')

    monkeypatch.setattr(DhanBroker, '_request', fake_request, raising=False)

    br = DhanBroker('C1', 'token')
    result = br.get_positions()
    assert result['status'] == 'success'
    positions = result['data']
    assert len(positions) == 1
    p = positions[0]
    assert p['ltp'] == 110
    assert p['last_price'] == 110
    assert p['unrealizedProfit'] == 10
    assert p['profitAndLoss'] == 10

def test_get_positions_uses_last_traded_price(monkeypatch):
    def fake_request(self, method, url, **kwargs):
        if url.endswith('/positions'):
            return Resp({'data': [
                {
                    'securityId': '101',
                    'exchangeSegment': 'NSE_EQ',
                    'buyQty': '2',
                    'sellQty': '0',
                    'buyAvg': '50',
                    'lastTradedPrice': '55'
                }
            ]})
        elif url.endswith('/marketfeed/ltp'):
            return Resp({'data': {}})
        raise AssertionError('unexpected url')

    monkeypatch.setattr(DhanBroker, '_request', fake_request, raising=False)

    br = DhanBroker('C1', 'token')
    result = br.get_positions()
    assert result['status'] == 'success'
    positions = result['data']
    assert len(positions) == 1
    p = positions[0]
    assert p['ltp'] == 55.0
    assert p['last_price'] == 55.0
    assert p['unrealizedProfit'] == 10.0
    assert p['profitAndLoss'] == 10.0
