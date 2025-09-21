from brokers.dhan import DhanBroker
import pytest
import tests.test_symbol_map

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
    
def test_missing_security_id_triggers_slice_refresh(monkeypatch, tmp_path):
    import brokers.symbol_map as sm

    sm, cleanup = tests.test_symbol_map._prepare_sparse_symbol_map(monkeypatch, tmp_path)

    class DummyResponse:
        def json(self):
            return {"orderId": "1"}

    monkeypatch.setattr(
        DhanBroker,
        "_request",
        lambda self, method, url, json=None, headers=None, timeout=None: DummyResponse(),
        raising=False,
    )

    lazy_calls: list[tuple[str, str | None]] = []

    def fake_lazy(symbol, broker, exchange=None):
        lazy_calls.append((symbol, exchange))
        if len(lazy_calls) == 1:
            return {}
        return sm.get_symbol_for_broker_lazy(symbol, broker, exchange)

    refresh_calls: list[tuple[str, str | None]] = []

    def fake_refresh(symbol, exchange=None):
        refresh_calls.append((symbol, exchange))
        sm.SYMBOL_MAP.pop(sm.extract_root_symbol(symbol), None)
        return sm.ensure_symbol_slice(symbol, exchange)

    monkeypatch.setattr("brokers.dhan.get_symbol_for_broker_lazy", fake_lazy)
    monkeypatch.setattr("brokers.dhan.refresh_symbol_slice", fake_refresh)

    try:
        broker = DhanBroker("CID", "TOKEN")
        result = broker.place_order(symbol="RELIANCE", action="BUY", qty=1)
        assert result["status"] == "success"
        assert len(refresh_calls) == 1
        assert lazy_calls[-1] == ("RELIANCE", None)
        assert len(lazy_calls) == 2
        assert set(sm.SYMBOL_MAP.keys()) == {"RELIANCE"}
    finally:
        cleanup()


def test_explicit_exchange_mismatch_raises_clear_error(monkeypatch):
    calls = {"refresh": 0, "mapper": 0}

    def fake_refresh(symbol, exchange=None):
        calls["refresh"] += 1

    monkeypatch.setattr("brokers.dhan.refresh_symbol_slice", fake_refresh)

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
