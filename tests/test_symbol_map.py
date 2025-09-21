from brokers.symbol_map import (
    get_symbol_for_broker,
    refresh_symbol_map,
)


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

def test_direct_lookup_populates_only_requested_root(monkeypatch):
    import brokers.symbol_map as sm

    entry = {
        "zerodha": {
            "trading_symbol": "AAA",
            "exchange": "NSE",
            "token": "1",
            "lot_size": "1",
        }
    }
    slice_entry = {
        "NSE": entry,
        sm.SYMBOLS_KEY: {
            "NSE": {
                "AAA": entry,
            }
        },
    }

    calls: list[tuple[str, str | None]] = []

    def fake_load(symbol: str, exchange: str | None = None):
        calls.append((symbol, exchange))
        if sm.extract_root_symbol(symbol) == "AAA":
            return slice_entry
        return {}

    monkeypatch.setattr(sm, "load_symbol_slice", fake_load)

    original_map = sm.SYMBOL_MAP
    sm.SYMBOL_MAP = {}
    try:
        result = sm._direct_symbol_lookup("AAA", "zerodha", "NSE")
        assert result["token"] == "1"
        assert set(sm.SYMBOL_MAP.keys()) == {"AAA"}
        assert calls and calls[-1] == ("AAA", "NSE")
    finally:
        sm.SYMBOL_MAP = original_map

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

    def fake_fetch(url, cache_name):
        return zerodha_csv if "kite" in url else dhan_csv

    monkeypatch.setattr(sm, "_fetch_csv", fake_fetch)
    sm._load_zerodha.cache_clear()
    sm._load_dhan.cache_clear()
    mapping = sm.build_symbol_map()
    assert "NSE" in mapping["AAA"]
    assert "BSE" in mapping["AAA"]
    assert mapping["AAA"]["NSE"]["zerodha"]["token"] == "1"
    assert mapping["AAA"]["BSE"]["zerodha"]["token"] == "2"


def test_explicit_exchange_missing_returns_empty(monkeypatch):
    import brokers.symbol_map as sm

    zerodha_csv = (
        "instrument_token,exchange,tradingsymbol,segment,instrument_type\n"
        "1,BSE,BBB,BSE,EQ\n"
    )
    dhan_csv = (
        "SEM_EXM_EXCH_ID,SEM_TRADING_SYMBOL,SEM_SEGMENT,SEM_SERIES,SEM_SMST_SECURITY_ID\n"
        "BSE,BBB,E,EQ,500100\n"
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
        assert sm.get_symbol_for_broker("BBB", "dhan", "NSE") == {}
        bse_data = sm.get_symbol_for_broker("BBB", "dhan", "BSE")
        assert bse_data["security_id"] == "500100"
    finally:
        sm.SYMBOL_MAP = original_map


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



def test_derivative_fallback_populates_only_requested_root(monkeypatch):
    import brokers.symbol_map as sm

    dhan_entry = {
        "security_id": "123",
        "exchange_segment": "NSE_FNO",
        "lot_size": "50",
        "trading_symbol": "NIFTY24SEP24000CE",
    }
    exchange_entry = {"dhan": dhan_entry}
    slice_entry = {
        "NFO": exchange_entry,
        sm.SYMBOLS_KEY: {
            "NFO": {
                "NIFTY24SEP24000CE": exchange_entry,
            }
        },
    }

    load_calls: list[tuple[str, str | None]] = []

    def fake_load(symbol: str, exchange: str | None = None):
        load_calls.append((symbol, exchange))
        if sm.extract_root_symbol(symbol) == "NIFTY":
            return slice_entry
        return {}

    original_direct = sm._direct_symbol_lookup

    def fake_direct(symbol: str, broker: str, exchange: str | None = None):
        original_direct(symbol, broker, exchange)
        return {}

    monkeypatch.setattr(sm, "load_symbol_slice", fake_load)
    monkeypatch.setattr(sm, "_direct_symbol_lookup", fake_direct)

    original_map = sm.SYMBOL_MAP
    sm.SYMBOL_MAP = {}
    try:
        result = get_symbol_for_broker("NIFTY24SEP24000CE", "dhan")
        assert result["security_id"] == "123"
        assert result["lot_size"] == "50"
        assert set(sm.SYMBOL_MAP.keys()) == {"NIFTY"}
        assert load_calls and load_calls[0][0] == "NIFTY24SEP24000CE"
    finally:
        sm.SYMBOL_MAP = original_map


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

def test_dhan_equity_suffix_maps_to_nse_security(monkeypatch):
    import brokers.symbol_map as sm

    zerodha_csv = (
        "instrument_token,exchange,tradingsymbol,segment,instrument_type\n"
        "1,NSE,RELIANCE,NSE,EQ\n"
        "2,BSE,RELIANCE,BSE,EQ\n"
    )

    dhan_csv = (
        "SEM_EXM_EXCH_ID,SEM_TRADING_SYMBOL,SEM_SEGMENT,SEM_SERIES,SEM_SMST_SECURITY_ID,SEM_LOT_UNITS\n"
        "NSE,RELIANCE-EQ,E,EQ,100,1\n"
        "BSE,RELIANCE,E,EQ,200,1\n"
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
        # Both the EQ suffix and the bare equity symbol should resolve for Dhan NSE
        result = get_symbol_for_broker("RELIANCE-EQ", "dhan", "NSE")
        assert result["security_id"] == "100"
        assert result["exchange_segment"] == "NSE_EQ"
        assert result["security_id"] != "200"
        bare = get_symbol_for_broker("RELIANCE", "dhan", "NSE")
        assert bare["security_id"] == "100"
        assert bare["exchange_segment"] == "NSE_EQ"
    finally:
        sm.SYMBOL_MAP = original_map

    nse_symbols = mapping["RELIANCE"][sm.SYMBOLS_KEY]["NSE"]
    assert "RELIANCE" in nse_symbols
    assert "RELIANCE-EQ" in nse_symbols
    assert mapping["RELIANCE"]["NSE"]["dhan"]["security_id"] == "100"


def test_bse_equity_generates_plain_and_eq_aliases(monkeypatch):
    import brokers.symbol_map as sm

    zerodha_csv = (
        "instrument_token,exchange,tradingsymbol,segment,instrument_type,lot_size\n"
        "1,BSE,SBVCL,BSE,EQ,1\n"
    )

    dhan_csv = (
        "SEM_EXM_EXCH_ID,SEM_TRADING_SYMBOL,SEM_SEGMENT,SEM_SERIES,SEM_SMST_SECURITY_ID,SEM_LOT_UNITS\n"
        "BSE,SBVCL-EQ,E,EQ,504000,15\n"
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
        eq_alias = get_symbol_for_broker("SBVCL-EQ", "dhan", "BSE")
        assert eq_alias["security_id"] == "504000"
        assert eq_alias["lot_size"] == "15"

        plain_alias = get_symbol_for_broker("SBVCL", "dhan", "BSE")
        assert plain_alias["security_id"] == "504000"
        assert plain_alias["lot_size"] == "15"
    finally:
        sm.SYMBOL_MAP = original_map

    bse_symbols = mapping["SBVCL"][sm.SYMBOLS_KEY]["BSE"]
    assert "SBVCL" in bse_symbols
    assert "SBVCL-EQ" in bse_symbols


def test_dhan_alias_merges_with_existing_equity(monkeypatch):
    import brokers.symbol_map as sm

    zerodha_csv = (
        "instrument_token,exchange,tradingsymbol,segment,instrument_type,lot_size\n"
        "1,BSE,SBVCL,BSE,EQ,1\n"
    )

    dhan_csv = (
        "SEM_EXM_EXCH_ID,SEM_TRADING_SYMBOL,SEM_SEGMENT,SEM_SERIES,SEM_SMST_SECURITY_ID,SEM_LOT_UNITS\n"
        "BSE,SBVCL-EQ,E,EQ,504000,15\n"
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
        zerodha_equity = get_symbol_for_broker("SBVCL", "zerodha", "BSE")
        dhan_equity = get_symbol_for_broker("SBVCL-EQ", "dhan", "BSE")
    finally:
        sm.SYMBOL_MAP = original_map

    assert zerodha_equity["lot_size"] == "15"
    assert dhan_equity["security_id"] == "504000"
    assert dhan_equity["lot_size"] == zerodha_equity["lot_size"]


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

def test_lazy_symbol_slice_avoids_full_map(monkeypatch, tmp_path):
    import brokers.symbol_map as sm

    zerodha_csv = tmp_path / "zerodha_instruments.csv"
    zerodha_csv.write_text(
        "instrument_token,exchange,tradingsymbol,segment,instrument_type,lot_size\n"
        "101,NSE,RELIANCE,NSE,EQ,1\n"
    )

    dhan_csv = tmp_path / "dhan_scrip_master.csv"
    dhan_csv.write_text(
        "SEM_SMST_SECURITY_ID,SEM_EXM_EXCH_ID,SEM_SEGMENT,SEM_TRADING_SYMBOL,SEM_SERIES,SEM_LOT_UNITS\n"
        "5001,NSE,E,RELIANCE-EQ,EQ,1\n"
    )

    original_map = sm.SYMBOL_MAP
    sm.SYMBOL_MAP = {}

    class _FailLoader:
        def __call__(self, *args, **kwargs):
            raise AssertionError("should not load entire dataset")

        def cache_clear(self):
            pass

    monkeypatch.setattr(sm, "_load_zerodha", _FailLoader())
    monkeypatch.setattr(sm, "_load_dhan", _FailLoader())
    monkeypatch.setattr(
        sm,
        "build_symbol_map",
        lambda: (_ for _ in ()).throw(AssertionError("should not build full map")),
    )

    def fake_ensure(url: str, cache_name: str):
        if "zerodha" in cache_name:
            return zerodha_csv
        if "dhan" in cache_name:
            return dhan_csv
        raise AssertionError(f"unexpected cache request {cache_name}")

    monkeypatch.setattr(sm, "_ensure_cached_csv", fake_ensure)

    try:
        entry = sm.ensure_symbol_slice("RELIANCE", "NSE")
        assert entry
        mapping = sm.get_symbol_for_broker_lazy("RELIANCE", "dhan", "NSE")
        assert mapping["trading_symbol"] == "RELIANCE-EQ"
        assert mapping["lot_size"] == "1"
        assert "RELIANCE" in sm.SYMBOL_MAP
    finally:
        sm.SYMBOL_MAP = original_map
        sm._load_zerodha.cache_clear()
        sm._load_dhan.cache_clear()


def test_lazy_derivative_slice(monkeypatch, tmp_path):
    import brokers.symbol_map as sm

    zerodha_csv = tmp_path / "zerodha_instruments.csv"
    zerodha_csv.write_text(
        "instrument_token,exchange,tradingsymbol,segment,instrument_type,lot_size\n"
        "201,NFO,NIFTY24SEPFUT,NFO,FUT,50\n"
    )

    dhan_csv = tmp_path / "dhan_scrip_master.csv"
    dhan_csv.write_text(
        "SEM_SMST_SECURITY_ID,SEM_EXM_EXCH_ID,SEM_SEGMENT,SEM_TRADING_SYMBOL,SEM_LOT_UNITS,SEM_EXPIRY_FLAG\n"
        "9001,NSE,D,NIFTY24SEPFUT,50,W\n"
    )

    original_map = sm.SYMBOL_MAP
    sm.SYMBOL_MAP = {}

    class _FailLoader:
        def __call__(self, *args, **kwargs):
            raise AssertionError("should not load entire dataset")

        def cache_clear(self):
            pass

    monkeypatch.setattr(sm, "_load_zerodha", _FailLoader())
    monkeypatch.setattr(sm, "_load_dhan", _FailLoader())
    monkeypatch.setattr(
        sm,
        "build_symbol_map",
        lambda: (_ for _ in ()).throw(AssertionError("should not build full map")),
    )

    def fake_ensure(url: str, cache_name: str):
        if "zerodha" in cache_name:
            return zerodha_csv
        if "dhan" in cache_name:
            return dhan_csv
        raise AssertionError(f"unexpected cache request {cache_name}")

    monkeypatch.setattr(sm, "_ensure_cached_csv", fake_ensure)

    try:
        entry = sm.ensure_symbol_slice("NIFTY24SEPFUT", "NFO")
        assert entry
        mapping = sm.get_symbol_for_broker_lazy("NIFTY24SEPFUT", "dhan", "NFO")
        assert mapping["lot_size"] == "50"
        assert mapping["security_id"] == "9001"
    finally:
        sm.SYMBOL_MAP = original_map
        sm._load_zerodha.cache_clear()
        sm._load_dhan.cache_clear()

def _prepare_sparse_symbol_map(monkeypatch, tmp_path):
    import brokers.symbol_map as sm

    zerodha_csv = tmp_path / "zerodha_instruments.csv"
    zerodha_csv.write_text(
        "instrument_token,exchange,tradingsymbol,segment,instrument_type,lot_size\n"
        "101,NSE,RELIANCE,NSE,EQ,1\n"
    )

    dhan_csv = tmp_path / "dhan_scrip_master.csv"
    dhan_csv.write_text(
        "SEM_SMST_SECURITY_ID,SEM_EXM_EXCH_ID,SEM_SEGMENT,SEM_TRADING_SYMBOL,SEM_SERIES,SEM_LOT_UNITS\n"
        "5001,NSE,E,RELIANCE-EQ,EQ,1\n"
    )

    original_map = sm.SYMBOL_MAP
    sm.SYMBOL_MAP = {}

    monkeypatch.setattr(
        sm,
        "build_symbol_map",
        lambda: (_ for _ in ()).throw(AssertionError("should not build full map")),
    )

    def fake_ensure(url: str, cache_name: str):
        if "zerodha" in cache_name:
            return zerodha_csv
        if "dhan" in cache_name:
            return dhan_csv
        raise AssertionError(f"unexpected cache request {cache_name}")

    monkeypatch.setattr(sm, "_ensure_cached_csv", fake_ensure)

    def cleanup():
        sm.SYMBOL_MAP = original_map
        sm._load_zerodha.cache_clear()
        sm._load_dhan.cache_clear()

    return sm, cleanup


def test_refresh_symbol_slice_only_updates_target_root(monkeypatch, tmp_path):
    sm, cleanup = _prepare_sparse_symbol_map(monkeypatch, tmp_path)

    try:
        initial = sm.ensure_symbol_slice("RELIANCE", "NSE")
        assert initial["NSE"]["dhan"]["security_id"] == "5001"

        sm.SYMBOL_MAP["OTHER"] = {"placeholder": {}}

        dhan_csv = tmp_path / "dhan_scrip_master.csv"
        dhan_csv.write_text(
            "SEM_SMST_SECURITY_ID,SEM_EXM_EXCH_ID,SEM_SEGMENT,SEM_TRADING_SYMBOL,SEM_SERIES,SEM_LOT_UNITS\n"
            "6001,NSE,E,RELIANCE-EQ,EQ,5\n"
        )

        updated = sm.refresh_symbol_slice("RELIANCE", "NSE")

        assert set(sm.SYMBOL_MAP.keys()) == {"OTHER", "RELIANCE"}
        assert updated["NSE"]["dhan"]["security_id"] == "6001"
        assert updated["NSE"]["dhan"]["lot_size"] == "5"
    finally:
        cleanup()


def test_dhan_broker_lazy_lookup_keeps_symbol_map_sparse(monkeypatch, tmp_path):
    sm, cleanup = _prepare_sparse_symbol_map(monkeypatch, tmp_path)

    from brokers.dhan import DhanBroker

    class DummyResponse:
        def json(self):
            return {"orderId": "1"}

    monkeypatch.setattr(
        DhanBroker,
        "_request",
        lambda self, method, url, json=None, headers=None, timeout=None: DummyResponse(),
        raising=False,
    )

    try:
        broker = DhanBroker("CID", "TOKEN")
        result = broker.place_order(symbol="RELIANCE", action="BUY", qty=1)
        assert result["status"] == "success"
        assert set(sm.SYMBOL_MAP.keys()) == {"RELIANCE"}
    finally:
        cleanup()


def test_aliceblue_lazy_lookup_keeps_symbol_map_sparse(monkeypatch, tmp_path):
    sm, cleanup = _prepare_sparse_symbol_map(monkeypatch, tmp_path)

    from brokers.aliceblue import AliceBlueBroker

    def fake_auth(self):
        self.session_id = "sid"
        self.headers = {"Authorization": "Bearer token"}

    class DummyResponse:
        def json(self):
            return {"stat": "Ok", "nestOrderNumber": "1"}

    monkeypatch.setattr(AliceBlueBroker, "authenticate", fake_auth, raising=False)
    monkeypatch.setattr(
        AliceBlueBroker,
        "_request",
        lambda self, method, url, headers=None, data=None: DummyResponse(),
        raising=False,
    )

    try:
        broker = AliceBlueBroker("CID", "APIKEY", device_number="device123")
        result = broker.place_order(symbol="RELIANCE", action="BUY", qty=1)
        assert result["status"] == "success"
        assert set(sm.SYMBOL_MAP.keys()) == {"RELIANCE"}
    finally:
        cleanup()


def test_order_consumer_lazy_lookup_keeps_symbol_map_sparse(monkeypatch, tmp_path):
    sm, cleanup = _prepare_sparse_symbol_map(monkeypatch, tmp_path)

    from services import order_consumer

    try:
        original_lazy = sm.get_symbol_for_broker_lazy
        lazy_calls: list[tuple[str, str | None]] = []

        def fake_lazy(symbol, broker, exchange=None):
            lazy_calls.append((symbol, exchange))
            if len(lazy_calls) == 1:
                return {}
            return original_lazy(symbol, broker, exchange)

        refresh_calls: list[tuple[str, str | None]] = []

        def fake_refresh(symbol, exchange=None):
            refresh_calls.append((symbol, exchange))
            sm.SYMBOL_MAP.pop(sm.extract_root_symbol(symbol), None)
            return sm.ensure_symbol_slice(symbol, exchange)

        monkeypatch.setattr(
            order_consumer.symbol_map, "get_symbol_for_broker_lazy", fake_lazy
        )
        monkeypatch.setattr(
            order_consumer.symbol_map, "refresh_symbol_slice", fake_refresh
        )

        lot_size = order_consumer._lookup_lot_size_from_symbol_map(
            "RELIANCE",
            "dhan",
            "NSE",
            event={"symbol": "RELIANCE"},
            broker_cfg={"name": "dhan"},
        )

        assert lot_size == "1"
        assert len(refresh_calls) == 1
        assert refresh_calls[0] == ("RELIANCE", "NSE")
        assert len(lazy_calls) == 2
        assert set(sm.SYMBOL_MAP.keys()) == {"RELIANCE"}
    finally:
        cleanup()
