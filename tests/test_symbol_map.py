from brokers.symbol_map import get_symbol_for_broker, refresh_symbol_map


def _make_response(text: str):
    class Resp:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self):
            return None

    return Resp(text)


def test_symbol_map_includes_bse_x_series():
    mapping = get_symbol_for_broker("BSE:CASPIAN", "dhan")
    assert mapping.get("security_id")
    assert mapping.get("exchange_segment") == "BSE_EQ"

def test_refresh_symbol_map(monkeypatch):
    # Save original map so we can restore it after the test to avoid
    # side effects for other tests.
    import brokers.symbol_map as sm

    original_map = sm.SYMBOL_MAP.copy()

    # First load a mapping containing only AAA.
    zerodha_csv1 = (
        "instrument_token,exchange,tradingsymbol,segment,instrument_type\n"
        "1,NSE,AAA,NSE,EQ\n"
    )
    dhan_csv1 = (
        "SEM_EXM_EXCH_ID,SEM_TRADING_SYMBOL,SEM_SEGMENT,SEM_SERIES,SEM_SMST_SECURITY_ID\n"
        "NSE,AAA,E,EQ,10\n"
    )

    def fake_get1(url, timeout=30):
        return _make_response(zerodha_csv1 if "kite" in url else dhan_csv1)

    monkeypatch.setattr(sm.requests, "get", fake_get1)
    refresh_symbol_map()
    assert "AAA" in sm.SYMBOL_MAP

    # Now switch the datasets to contain only BBB and refresh again.
    zerodha_csv2 = (
        "instrument_token,exchange,tradingsymbol,segment,instrument_type\n"
        "2,NSE,BBB,NSE,EQ\n"
    )
    dhan_csv2 = (
        "SEM_EXM_EXCH_ID,SEM_TRADING_SYMBOL,SEM_SEGMENT,SEM_SERIES,SEM_SMST_SECURITY_ID\n"
        "NSE,BBB,E,EQ,20\n"
    )

    def fake_get2(url, timeout=30):
        return _make_response(zerodha_csv2 if "kite" in url else dhan_csv2)

    monkeypatch.setattr(sm.requests, "get", fake_get2)
    refresh_symbol_map()
    assert "BBB" in sm.SYMBOL_MAP
    assert "AAA" not in sm.SYMBOL_MAP

    # Restore original map for subsequent tests.
    sm.SYMBOL_MAP.clear()
    sm.SYMBOL_MAP.update(original_map)
