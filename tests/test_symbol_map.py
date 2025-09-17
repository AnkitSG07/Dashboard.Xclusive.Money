from brokers.symbol_map import get_symbol_for_broker, refresh_symbol_map


def _make_response(text: str):
    class Resp:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self):
            return None

    return Resp(text)


def test_symbol_map_includes_bse_x_series():
    mapping = get_symbol_for_broker(
        "CASPIANCORPORATESERVICES", "dhan", "BSE"
    )
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


def test_derivative_variants_map_to_distinct_ids(monkeypatch):
    import brokers.symbol_map as sm

    zerodha_csv = (
        "instrument_token,exchange,tradingsymbol,segment,instrument_type\n"
        "1,NFO,NIFTY24SEP24000CE,NFO,OPTIDX\n"
        "2,NFO,NIFTY24SEP24500CE,NFO,OPTIDX\n"
    )
    dhan_csv = (
        "SEM_EXM_EXCH_ID,SEM_TRADING_SYMBOL,SEM_SEGMENT,SEM_SERIES,SEM_SMST_SECURITY_ID\n"
        "NSE,NIFTY24SEP24000CE,D,,1000\n"
        "NSE,NIFTY24SEP24500CE,D,,2000\n"
    )

    def fake_fetch(url, cache_name):
        return zerodha_csv if "kite" in url else dhan_csv

    monkeypatch.setattr(sm, "_fetch_csv", fake_fetch)
    sm._load_zerodha.cache_clear()
    sm._load_dhan.cache_clear()
    mapping = sm.build_symbol_map()

    original_map = sm.SYMBOL_MAP
    sm.SYMBOL_MAP = mapping
    try:
        first = get_symbol_for_broker("NIFTY24SEP24000CE", "dhan")
        second = get_symbol_for_broker("NIFTY24SEP24500CE", "dhan")
        assert first["security_id"] == "1000"
        assert second["security_id"] == "2000"
    finally:
        sm.SYMBOL_MAP = original_map

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


def test_canonical_dhan_symbol_handles_embedded_year():
    import brokers.symbol_map as sm

    base = "NIFTYNXT5025NOV35500CE"
    with_year = "NIFTYNXT5025NOV2535500CE"
    assert sm._canonical_dhan_symbol(base) == base
    assert sm._canonical_dhan_symbol(with_year) == base


def test_canonical_dhan_symbol_basic_spaces_and_types():
    import brokers.symbol_map as sm

    assert (
        sm._canonical_dhan_symbol("FINNIFTY 30 SEP 33300 CALL")
        == "FINNIFTY30SEP33300CE"
    )


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


def test_canonical_dhan_option_symbol_missing_day_non_iso():
    import brokers.symbol_map as sm

    expected = "NIFTYNXT5025SEP38000CE"
    assert (
        sm._canonical_dhan_symbol(
            "NIFTYNXT50-Sep2025-38000-CE", "25-09-2025"
        )
        == expected
    )


def test_dhan_derivative_includes_lot_size(monkeypatch):
    import brokers.symbol_map as sm

    zerodha_csv = (
        "instrument_token,exchange,tradingsymbol,segment,instrument_type,lot_size\n"
        "1,NFO,NIFTYNXT5025NOV35500CE,NFO,OPTIDX,\n"
    )
    dhan_csv = (
        "SEM_EXM_EXCH_ID,SEM_TRADING_SYMBOL,SEM_SEGMENT,SEM_SERIES,SEM_SMST_SECURITY_ID,SEM_LOT_UNITS\n"
        "NSE,NIFTYNXT50 25NOV2023 35500 CALL,D,,500,40\n"
    )

    def fake_get(url, timeout=30):
        return _make_response(zerodha_csv if "kite" in url else dhan_csv)

    monkeypatch.setattr(sm.requests, "get", fake_get)
    sm._load_zerodha.cache_clear()
    sm._load_dhan.cache_clear()
    mapping = sm.build_symbol_map()
    assert (
        mapping["NIFTYNXT5025NOV35500CE"]["NFO"]["dhan"].get("lot_size") == 40
    )


def test_dhan_missing_lot_size_preserves_existing(monkeypatch):
    import brokers.symbol_map as sm

    zerodha_csv = (
        "instrument_token,exchange,tradingsymbol,segment,instrument_type\n"
        "1,NSE,AAA,NSE,EQ\n"
    )
    dhan_csv = (
        "SEM_EXM_EXCH_ID,SEM_TRADING_SYMBOL,SEM_SEGMENT,SEM_SERIES,SEM_SMST_SECURITY_ID,SEM_LOT_UNITS\n"
        "NSE,AAA,E,BE,10,25\n"
        "NSE,AAA,E,EQ,10,\n"
    )

    def fake_get(url, timeout=30):
        return _make_response(zerodha_csv if "kite" in url else dhan_csv)

    monkeypatch.setattr(sm.requests, "get", fake_get)
    sm._load_zerodha.cache_clear()
    sm._load_dhan.cache_clear()
    mapping = sm.build_symbol_map()
    assert mapping["AAA"]["NSE"]["dhan"]["lot_size"] == 25
    assert mapping["AAA"]["NSE"]["zerodha"]["lot_size"] == 25


def test_load_dhan_retains_existing_lot_size(monkeypatch):
    import brokers.symbol_map as sm

    dhan_csv = (
        "SEM_EXM_EXCH_ID,SEM_TRADING_SYMBOL,SEM_SEGMENT,SEM_SERIES,SEM_SMST_SECURITY_ID,SEM_LOT_UNITS\n"
        "NSE,AAA,E,BE,10,25\n"
        "NSE,AAA,E,EQ,10,\n"
    )

    def fake_get(url, timeout=30):
        return _make_response(dhan_csv)

    monkeypatch.setattr(sm.requests, "get", fake_get)
    sm._load_dhan.cache_clear()
    data = sm._load_dhan(force=True)
    assert data[("AAA", "NSE")]["lot_size"] == 25


def test_load_dhan_parses_float_lot_size(monkeypatch):
    import brokers.symbol_map as sm

    dhan_csv = (
        "SEM_EXM_EXCH_ID,SEM_TRADING_SYMBOL,SEM_SEGMENT,SEM_SERIES,SEM_SMST_SECURITY_ID,SEM_LOT_UNITS\n"
        "NSE,AAA,E,EQ,10,25.0\n"
        "NSE,BBB,E,EQ,20,40\n"
    )

    def fake_get(url, timeout=30):
        return _make_response(dhan_csv)

    monkeypatch.setattr(sm.requests, "get", fake_get)
    sm._load_dhan.cache_clear()
    data = sm._load_dhan(force=True)

    assert data[("AAA", "NSE")]["lot_size"] == 25
    assert data[("BBB", "NSE")]["lot_size"] == 40

def test_load_dhan_prefers_custom_symbol(monkeypatch, caplog):
    import brokers.symbol_map as sm
    import logging

    dhan_csv = (
        "SEM_EXM_EXCH_ID,SEM_TRADING_SYMBOL,SEM_CUSTOM_SYMBOL,SEM_SEGMENT,"
        "SEM_SERIES,SEM_SMST_SECURITY_ID\n"
        "NSE,BADSYMBOL,GOODSYMBOL,E,EQ,10\n"
    )

    def fake_get(url, timeout=30):
        return _make_response(dhan_csv)

    monkeypatch.setattr(sm.requests, "get", fake_get)
    sm._load_dhan.cache_clear()
    with caplog.at_level(logging.WARNING):
        data = sm._load_dhan(force=True)

    assert ("GOODSYMBOL", "NSE") in data
    assert ("BADSYMBOL", "NSE") not in data
    assert "1 trading/custom symbol mismatches skipped" in caplog.text


def test_load_dhan_retains_derivative_lot_size_when_missing(monkeypatch):
    import brokers.symbol_map as sm

    dhan_csv = (
        "SEM_EXM_EXCH_ID,SEM_TRADING_SYMBOL,SEM_SEGMENT,SEM_SERIES,SEM_SMST_SECURITY_ID,SEM_LOT_UNITS\n"
        "NSE,NIFTY24AUGFUT,D,,100,50\n"
        "NSE,NIFTY24AUGFUT,D,,100,\n"
    )

    def fake_get(url, timeout=30):
        return _make_response(dhan_csv)

    monkeypatch.setattr(sm.requests, "get", fake_get)
    sm._load_dhan.cache_clear()
    data = sm._load_dhan(force=True)
    assert data[("NIFTY24AUGFUT", "NFO")]["lot_size"] == 50


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


def test_get_symbol_for_broker_handles_raw_dhan_symbol():
    import brokers.symbol_map as sm

    original_map = sm.SYMBOL_MAP
    sm.SYMBOL_MAP = {
        "FINNIFTY 30 SEP 33300 CE": {
            "NFO": {
                "dhan": {
                    "security_id": "42",
                    "exchange_segment": "NSE_FNO",
                    "lot_size": "1",
                }
            }
        }
    }
    try:
        mapping = get_symbol_for_broker("FINNIFTY30SEP33300CE", "dhan")
        assert mapping["security_id"] == "42"
    finally:
        sm.SYMBOL_MAP = original_map
        
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
