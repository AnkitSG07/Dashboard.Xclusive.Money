import pytest
from brokers.dhan import DhanBroker

class Resp:
    def __init__(self, data):
        self._data = data
    def json(self):
        return self._data


def test_get_holdings_enriches_ltp_and_pnl(monkeypatch):
    def fake_request(self, method, url, **kwargs):
        if url.endswith('/holdings'):
            return Resp([
                {
                    'securityId': '101',
                    'exchangeSegment': 'NSE_EQ',
                    'availableQty': '2',
                    'avgCostPrice': '100'
                }
            ])
        elif url.endswith('/marketfeed/ltp'):
            expected = {'NSE_EQ': [101]}
            assert kwargs.get('json') == expected
            return Resp({'data': {'NSE_EQ': {'101': {'ltp': 110}}}})
        raise AssertionError('unexpected url')

    monkeypatch.setattr(DhanBroker, '_request', fake_request, raising=False)

    br = DhanBroker('C1', 'token')
    result = br.get_holdings()
    assert result['status'] == 'success'
    holdings = result['data']
    assert len(holdings) == 1
    h = holdings[0]
    assert h['ltp'] == 110
    assert h['last_price'] == 110
    assert h['pnl'] == 20
    assert h['unrealizedProfit'] == 20
