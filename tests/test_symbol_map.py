from brokers.symbol_map import get_symbol_for_broker, refresh_symbol_map


def _make_response(text: str):
    class Resp:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self):
            return None

    return Resp(text)


def test_symbol_map_includes_bse_x_series():
    mapping = get_symbol_for_broker("CASPIAN", "dhan", "BSE")
    assert mapping.get("security_id")
    assert mapping.get("exchange_segment") == "BSE_EQ"

def test_build_symbol_map_preserves_multiple_exchanges(monkeypatch):
    import brokers.symbol_map as sm

    zerodha_csv = (
        "instrument_token,exchange,tradingsymbol,segment,instrument_type\n"
        "1,NSE,AAA,NSE,EQ\n"
        "2,BSE,AAA,BSE,EQ\n"
    )
    dhan_csv = (
        "SEM_EXM_EXCH_ID,SEM_TRADING_SYMBOL,SEM_SEGMENT,SEM_SERIES,SEM_SMST_SECURITY_ID\n"
        "NSE,AAA,E,EQ,10\n"
        "BSE,AAA,E,EQ,20\n"
    )

    def fake_get(url, timeout=30):
        return _make_response(zerodha_csv if "kite" in url else dhan_csv)

    monkeypatch.setattr(sm.requests, "get", fake_get)
    sm._load_zerodha.cache_clear()
    sm._load_dhan.cache_clear()
    mapping = sm.build_symbol_map()
    assert "NSE" in mapping["AAA"]
    assert "BSE" in mapping["AAA"]
    assert mapping["AAA"]["NSE"]["zerodha"]["token"] == "1"
    assert mapping["AAA"]["BSE"]["zerodha"]["token"] == "2"


def test_build_symbol_map_includes_derivatives(monkeypatch):
    import brokers.symbol_map as sm

    zerodha_csv = (
        "instrument_token,exchange,tradingsymbol,segment,instrument_type\n"
        "1,NFO,BANKNIFTY24AUGFUT,NFO,FUTIDX\n"
    )
    dhan_csv = (
        "SEM_EXM_EXCH_ID,SEM_TRADING_SYMBOL,SEM_SEGMENT,SEM_SERIES,SEM_SMST_SECURITY_ID\n"
        "NSE,BANKNIFTY24AUGFUT,D,,100\n"
    )

    def fake_get(url, timeout=30):
        return _make_response(zerodha_csv if "kite" in url else dhan_csv)

    monkeypatch.setattr(sm.requests, "get", fake_get)
    sm._load_zerodha.cache_clear()
    sm._load_dhan.cache_clear()
    mapping = sm.build_symbol_map()
    assert mapping["BANKNIFTY24AUGFUT"]["NFO"]["zerodha"]["token"] == "1"
    assert mapping["BANKNIFTY24AUGFUT"]["NFO"]["dhan"]["security_id"] == "100"
    assert (
        mapping["BANKNIFTY24AUGFUT"]["NFO"]["dhan"]["exchange_segment"]
        == "NSE_FNO"
    )

def test_canonical_dhan_option_symbol(monkeypatch):
    import brokers.symbol_map as sm

    zerodha_csv = (
        "instrument_token,exchange,tradingsymbol,segment,instrument_type\n"
        "1,NFO,NIFTYNXT5025NOV35500CE,NFO,OPTIDX\n"
    )
    dhan_csv = (
        "SEM_EXM_EXCH_ID,SEM_TRADING_SYMBOL,SEM_SEGMENT,SEM_SERIES,SEM_SMST_SECURITY_ID\n"
        "NSE,NIFTYNXT50 25NOV2023 35500 CALL,D,,500\n"
    )

    def fake_get(url, timeout=30):
        return _make_response(zerodha_csv if "kite" in url else dhan_csv)

    monkeypatch.setattr(sm.requests, "get", fake_get)
    sm._load_zerodha.cache_clear()
    sm._load_dhan.cache_clear()
    mapping = sm.build_symbol_map()
    assert (
        mapping["NIFTYNXT5025NOV35500CE"]["NFO"]["dhan"]["security_id"]
        == "500"
    )
    assert (
        sm._canonical_dhan_symbol("NIFTYNXT50 25NOV2023 35500 CALL")
        == "NIFTYNXT5025NOV35500CE"
    )


def test_canonical_dhan_option_symbol_without_year_and_two_digit_year():
    import brokers.symbol_map as sm

    expected = "NIFTYNXT5025NOV35500CE"
    assert sm._canonical_dhan_symbol("NIFTYNXT50 25NOV 35500 CALL") == expected
    assert sm._canonical_dhan_symbol("NIFTYNXT50 25NOV23 35500 CALL") == expected


def test_canonical_dhan_option_symbol_missing_day(monkeypatch):
    import brokers.symbol_map as sm

    zerodha_csv = (
        "instrument_token,exchange,tradingsymbol,segment,instrument_type\n"
        "1,NFO,NIFTYNXT5025SEP38000CE,NFO,OPTIDX\n"
    )
    dhan_csv = (
        "SEM_EXM_EXCH_ID,SEM_TRADING_SYMBOL,SEM_SEGMENT,SEM_SERIES,SEM_SMST_SECURITY_ID,SEM_EXPIRY_DATE\n"
        "NSE,NIFTYNXT50-Sep2025-38000-CE,D,,500,2025-09-25 14:30:00\n"
    )

    def fake_get(url, timeout=30):
        return _make_response(zerodha_csv if "kite" in url else dhan_csv)

    monkeypatch.setattr(sm.requests, "get", fake_get)
    sm._load_zerodha.cache_clear()
    sm._load_dhan.cache_clear()
    mapping = sm.build_symbol_map()
    assert (
        mapping["NIFTYNXT5025SEP38000CE"]["NFO"]["dhan"]["security_id"]
        == "500"
    )
    assert (
        sm._canonical_dhan_symbol(
            "NIFTYNXT50-Sep2025-38000-CE", "2025-09-25 14:30:00"
        )
        == "NIFTYNXT5025SEP38000CE"
    )



def test_get_symbol_for_broker_derivative(monkeypatch):
    import brokers.symbol_map as sm

    original_map = sm.get_symbol_map().copy()

    zerodha_csv = (
        "instrument_token,exchange,tradingsymbol,segment,instrument_type\n"
        "1,NFO,NIFTY24SEPFUT,NFO,FUTIDX\n"
    )
    dhan_csv = (
        "SEM_EXM_EXCH_ID,SEM_TRADING_SYMBOL,SEM_SEGMENT,SEM_SERIES,SEM_SMST_SECURITY_ID\n"
        "NSE,NIFTY24SEPFUT,D,,200\n"
    )

    def fake_get(url, timeout=30):
        return _make_response(zerodha_csv if "kite" in url else dhan_csv)

    monkeypatch.setattr(sm.requests, "get", fake_get)
    refresh_symbol_map(force=True)
    mapping = get_symbol_for_broker("NIFTY24SEPFUT", "dhan", "NFO")
    assert mapping["security_id"] == "200"
    assert mapping["exchange_segment"] == "NSE_FNO"

    sm.SYMBOL_MAP.clear()
    sm.SYMBOL_MAP.update(original_map)
    
def test_refresh_symbol_map(monkeypatch):
    # Save original map so we can restore it after the test to avoid
    # side effects for other tests.
    import brokers.symbol_map as sm

    original_map = sm.get_symbol_map().copy()

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
    refresh_symbol_map(force=True)
    assert "AAA" in sm.get_symbol_map()

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
    refresh_symbol_map(force=True)
    assert "BBB" in sm.get_symbol_map()
    assert "AAA" not in sm.get_symbol_map()

    # Restore original map for subsequent tests.
    sm.SYMBOL_MAP.clear()
    sm.SYMBOL_MAP.update(original_map)
