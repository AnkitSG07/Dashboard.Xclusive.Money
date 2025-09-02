from brokers.dhan import DhanBroker

class Resp:
    def __init__(self, data, status_code=200):
        self._data = data
        self.status_code = status_code
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

def test_get_order_list_invalid_auth_does_not_retry(monkeypatch):
    calls = []
    def fake_request(self, method, url, **kwargs):
        calls.append(url)
        return Resp(
            {
                "errorType": "Invalid_Authentication",
                "errorCode": "DH-901",
                "errorMessage": "Client ID or user generated access token is invalid or expired.",
            },
            status_code=401,
        )

    monkeypatch.setattr(DhanBroker, '_request', fake_request, raising=False)

    br = DhanBroker('C1', 'token')
    result = br.get_order_list()

    assert result['status'] == 'failure'
    assert 'invalid' in result['error'].lower()
    # ensure we did not retry with non-paginated request
    assert len(calls) == 1



def test_list_orders_normalizes_status(monkeypatch):
    sample = [
        {"orderId": "1", "orderStatus": "COMPLETE"},
        {"order_id": "2", "orderStatus": "REJECTED"},
    ]

    def fake_get_order_list(self, **kwargs):
        return {"status": "success", "data": sample}

    monkeypatch.setattr(DhanBroker, "get_order_list", fake_get_order_list)

    br = DhanBroker("C1", "token")
    orders = br.list_orders()

    assert isinstance(orders, list) and len(orders) == 2
    assert orders[0]["status"] == "COMPLETE"
    assert orders[0]["orderId"] == "1"
    assert orders[1]["orderId"] == "2"
    assert orders[1]["status"] == "REJECTED"
