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


def test_exit_all_positions_for_account_normalizes_exchange(monkeypatch):
    placed = {}

    def fake_request(self, method, url, **kwargs):
        if url.endswith('/positions'):
            return Resp({
                'data': [
                    {
                        'securityId': '101',
                        'exchange': 'NSE_FNO',
                        'buyQty': '1',
                        'sellQty': '0',
                        'buyAvg': '100',
                        'tradingSymbol': 'SBIN',
                        'productType': 'INTRADAY'
                    }
                ]
            })
        elif url.endswith('/marketfeed/ltp'):
            return Resp({'data': {'NSE_FNO': {'101': {'ltp': 110}}}})
        raise AssertionError('unexpected url')

    monkeypatch.setattr(DhanBroker, '_request', fake_request, raising=False)

    def fake_place_order(self, **kwargs):
        placed.update(kwargs)
        return {'status': 'success'}

    monkeypatch.setattr(DhanBroker, 'place_order', fake_place_order, raising=False)

    import os
    os.environ.setdefault("SECRET_KEY", "test")
    os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
    os.environ.setdefault("ADMIN_EMAIL", "a@a.com")
    os.environ.setdefault("ADMIN_PASSWORD", "pass")
    os.environ.setdefault("RUN_SCHEDULER", "0")
    import app as app_module
    from types import SimpleNamespace

    br = DhanBroker('C1', 'token')
    monkeypatch.setattr(app_module, 'broker_api', lambda acc: br)
    monkeypatch.setattr(
        app_module,
        '_account_to_dict',
        lambda acc: {'broker': 'dhan', 'client_id': 'C1', 'credentials': {'access_token': 'token'}},
    )
    monkeypatch.setattr(app_module, 'save_log', lambda *a, **k: None)

    account = SimpleNamespace(client_id='C1', broker='dhan', user_id=1)
    results = app_module.exit_all_positions_for_account(account)

    assert results[0]['status'] == 'SUCCESS'
    assert placed.get('exchange_segment') == 'NSE_FNO'
