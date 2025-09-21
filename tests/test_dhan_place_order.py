from brokers.dhan import DhanBroker
import pytest

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
    monkeypatch.setattr('brokers.dhan.get_symbol_for_broker_lazy', fake_mapper)

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

    monkeypatch.setattr('brokers.dhan.get_symbol_for_broker_lazy', fake_mapper)

    # Inject a symbol map that only knows the NSE security id
    br = DhanBroker('C1', 'token', symbol_map={'RELIANCE': '2885'})
    result = br.place_order(symbol='RELIANCE', action='BUY', qty=1, exchange='BSE')

    assert result['status'] == 'success'
    assert captured['payload']['securityId'] == '500325'
    assert captured['payload']['exchangeSegment'] == 'BSE_EQ'

def test_derivative_exchange_adjustment(monkeypatch):
    """Derivative orders should map to F&O segments even when exchange is NSE."""
    captured = {}

    def fake_request(self, method, url, **kwargs):
        captured['payload'] = kwargs.get('json')
        return Resp({"orderId": "1"})

    monkeypatch.setattr(DhanBroker, '_request', fake_request, raising=False)

    def fake_mapper(symbol, broker, exchange=None):
        # Ensure the broker lookup is performed against the F&O exchange
        assert exchange == 'NFO'
        return {"security_id": "12345", "exchange_segment": "NSE_FNO"}

    monkeypatch.setattr('brokers.dhan.get_symbol_for_broker_lazy', fake_mapper)

    br = DhanBroker('C1', 'token')
    result = br.place_order(symbol='BANKNIFTY50DECFUT', action='BUY', qty=25, exchange='NSE')

    assert result['status'] == 'success'
    assert captured['payload']['exchangeSegment'] == 'NSE_FNO'
    assert captured['payload']['securityId'] == '12345'

def test_rejects_expired_contract(monkeypatch):
    """Expired FUT/OPT symbols should not be submitted for trading."""
    def fake_request(*args, **kwargs):  # Should not be called
        raise AssertionError("request should not be made for expired contracts")

    monkeypatch.setattr(DhanBroker, '_request', fake_request, raising=False)

    def fake_mapper(symbol, broker, exchange=None):  # Should not be called
        raise AssertionError("symbol lookup should not be performed")

    monkeypatch.setattr('brokers.dhan.get_symbol_for_broker_lazy', fake_mapper)

    br = DhanBroker('C1', 'token')
    # Use a clearly expired symbol (Aug 2020)
    result = br.place_order(symbol='BANKNIFTY20AUGFUT', action='BUY', qty=25)
    assert result['status'] == 'failure'
    assert 'expired' in result['error'].lower()
    
def test_missing_security_id_triggers_refresh(monkeypatch):
    calls = {"refresh": 0, "mapper": 0}

    def fake_refresh():
        calls["refresh"] += 1

    monkeypatch.setattr("brokers.dhan.refresh_symbol_map", fake_refresh)

    def fake_mapper(symbol, broker, exchange=None):
        calls["mapper"] += 1
        return {}

    monkeypatch.setattr('brokers.dhan.get_symbol_for_broker_lazy', fake_mapper)

    br = DhanBroker("C1", "token")
    with pytest.raises(ValueError) as exc:
        br.place_order(symbol="MISSING", action="BUY", qty=1)

    assert "expired" in str(exc.value)
    assert calls["refresh"] == 1
    assert calls["mapper"] == 2


def test_explicit_exchange_mismatch_raises_clear_error(monkeypatch):
    calls = {"refresh": 0, "mapper": 0}

    def fake_refresh():
        calls["refresh"] += 1

    monkeypatch.setattr("brokers.dhan.refresh_symbol_map", fake_refresh)

    def fake_mapper(symbol, broker, exchange=None):
        calls["mapper"] += 1
        assert exchange == "NSE"
        return {}

    monkeypatch.setattr('brokers.dhan.get_symbol_for_broker_lazy', fake_mapper)

    def fake_request(*args, **kwargs):  # Should not be called
        raise AssertionError("request should not be made when exchange is invalid")

    monkeypatch.setattr(DhanBroker, "_request", fake_request, raising=False)

    br = DhanBroker("C1", "token", symbol_map={"BBB": "500100"})

    with pytest.raises(ValueError) as exc:
        br.place_order(symbol="BBB", action="BUY", qty=1, exchange="NSE")

    message = str(exc.value)
    assert "not available" in message
    assert "NSE" in message
    assert calls["refresh"] == 1
    assert calls["mapper"] == 2
