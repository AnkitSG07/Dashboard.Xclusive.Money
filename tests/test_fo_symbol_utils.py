import importlib
import sys
import types

def test_currency_future_lazy_symbol_map_import(monkeypatch):
    """Currency futures should lazy load the symbol map without ImportError."""

    fo_utils = None
    original_symbol_map_module = sys.modules.pop("brokers.symbol_map", None)
    original_fo_utils_module = sys.modules.pop("services.fo_symbol_utils", None)

    brokers_pkg = importlib.import_module("brokers")
    if hasattr(brokers_pkg, "symbol_map"):
        monkeypatch.delattr(brokers_pkg, "symbol_map")

    try:
        fo_utils = importlib.import_module("services.fo_symbol_utils")
        fo_utils._lookup_currency_future_expiry_day.cache_clear()

        assert "brokers.symbol_map" not in sys.modules

        fake_symbol_map = types.ModuleType("brokers.symbol_map")
        load_calls = []

        def ensure_symbol_slice(symbol: str, exchange: str | None = None) -> None:
            load_calls.append((symbol, exchange))

        fake_symbol_map.ensure_symbol_slice = ensure_symbol_slice  # type: ignore[attr-defined]
        fake_symbol_map.SYMBOLS_KEY = "_symbols"  # type: ignore[attr-defined]
        fake_symbol_map.SYMBOL_MAP = {  # type: ignore[attr-defined]
            "USDINR": {
                fake_symbol_map.SYMBOLS_KEY: {
                    "NFO": {
                        "USDINR-27SEP2024-FUT": {
                            "dhan": {"expiry_day": "27"},
                        }
                    }
                }
            }
        }

        sys.modules["brokers.symbol_map"] = fake_symbol_map
        monkeypatch.setattr(brokers_pkg, "symbol_map", fake_symbol_map, raising=False)
        monkeypatch.setattr(fo_utils, "_symbol_map_module", None)
        monkeypatch.setattr(fo_utils, "get_expiry_year", lambda month, day=None: "24")

        normalized = fo_utils.normalize_symbol_to_dhan_format("USDINR SEP FUT")

        assert normalized == "USDINR-27Sep2024-FUT"
        assert load_calls == [("USDINR", None)]
        assert fo_utils._symbol_map_module is fake_symbol_map
    finally:
        if fo_utils is not None:
            fo_utils._lookup_currency_future_expiry_day.cache_clear()
        sys.modules.pop("brokers.symbol_map", None)
        if original_symbol_map_module is not None:
            sys.modules["brokers.symbol_map"] = original_symbol_map_module
        sys.modules.pop("services.fo_symbol_utils", None)
        if original_fo_utils_module is not None:
            sys.modules["services.fo_symbol_utils"] = original_fo_utils_module
