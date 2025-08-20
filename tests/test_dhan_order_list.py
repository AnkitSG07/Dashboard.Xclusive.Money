from brokers.dhan import DhanBroker

class Resp:
    def __init__(self, data):
        self._data = data
    def json(self):
        return self._data

def test_get_order_list_stops_on_duplicate_page(monkeypatch):
    calls = []
    def fake_request(self, method, url, **kwargs):
        calls.append(url)
        # return same page regardless of offset
        return Resp([
            {"orderId": "1"},
            {"orderId": "2"},
        ])
    monkeypatch.setattr(DhanBroker, '_request', fake_request, raising=False)

    br = DhanBroker('C1', 'token')
    result = br.get_order_list(batch_size=2, max_batches=5)

    assert result['status'] == 'success'
    data = result['data']
    assert len(data) == 2
    assert {o['orderId'] for o in data} == {'1', '2'}
    # ensure we attempted a second fetch to detect duplicates
    assert len(calls) == 2
