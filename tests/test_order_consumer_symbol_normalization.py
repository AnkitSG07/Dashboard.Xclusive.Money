import pytest

import services.order_consumer as order_consumer
import services.webhook_receiver as webhook_receiver
import services.fo_symbol_utils as fo_symbol_utils
from services.fo_symbol_utils import is_fo_symbol
from brokers import symbol_map


@pytest.mark.parametrize(
    "symbol,expected",
    [
        ("NIFTY-Sep2025-25500-CE", "NIFTY-Sep2025-25500-CE"),
        ("FINNIFTY25SEP33300CE", "FINNIFTY-Sep2025-33300-CE"),
        ("FINNIFTY25SEPFUT", "FINNIFTY-Sep2025-FUT"),
        ("NIFTY-SEP2025-25500-CE", "NIFTY-Sep2025-25500-CE"),
    ],
)
def test_normalize_symbol_month_casing(symbol, expected):
    """Ensure month casing remains Title-case for Dhan formatted symbols."""
    assert order_consumer.normalize_symbol_to_dhan_format(symbol) == expected


@pytest.mark.parametrize(
    "symbol",
    ["RELIANCE", "RELIANCECE", "SBVCL", "SBVCLPE"],
)
def test_equity_like_symbols_unchanged(symbol):
    """Equity tickers, even those ending with CE/PE, stay untouched."""
    assert order_consumer.normalize_symbol_to_dhan_format(symbol) == symbol


@pytest.mark.parametrize(
    "symbol,expected",
    [
        ("RELIANCE", "RELIANCE-EQ"),
        ("RELIANCECE", "RELIANCECE-EQ"),
        ("SBVCL", "SBVCL-EQ"),
        ("SBVCLPE", "SBVCLPE-EQ"),
    ],
)
def test_webhook_equity_suffix_handling(symbol, expected):
    normalized, metadata = webhook_receiver.normalize_fo_symbol(symbol)
    assert normalized == expected
    assert metadata["instrument_type"] == "EQ"


@pytest.mark.parametrize(
    "symbol,expected",
    [
        ("RELIANCECE", False),
        ("SBVCLPE", False),
        ("RELIANCE-CE", True),
        ("SBIN 2024PE", True),
        ("NIFTY-Sep2025-FUT", True),
    ],
)
def test_is_fo_symbol_detection(symbol, expected):
    assert is_fo_symbol(symbol) is expected


def test_weekly_option_normalization_preserves_metadata(monkeypatch):
    """Weekly expiries drop the day from the symbol but keep it in metadata."""

    weekly_key = "NIFTY-Sep2024-25500-CE"

    monkeypatch.setattr(order_consumer, "get_expiry_year", lambda month, day=None: "24")
    monkeypatch.setattr(webhook_receiver, "get_expiry_year", lambda month, day=None: "24")
    monkeypatch.setattr(webhook_receiver, "get_lot_size_from_symbol_map", lambda *a, **k: None)

    normalized_consumer = order_consumer.normalize_symbol_to_dhan_format(
        "NIFTY 23 SEP 25500 CALL"
    )
    normalized_webhook, metadata = webhook_receiver.normalize_fo_symbol(
        "NIFTY 23 SEP 25500 CALL"
    )

    assert normalized_consumer == weekly_key
    assert normalized_webhook == weekly_key
    assert metadata["expiry_day"] == 23


def test_weekly_symbol_lookup_prefers_weekly_contract(monkeypatch):
    """Symbol map lookup should continue to prefer weekly contracts."""

    weekly_key = "NIFTY-Sep2024-25500-CE"
    calls = []

    monkeypatch.setattr(order_consumer, "get_expiry_year", lambda month, day=None: "24")

    def fake_get_symbol(symbol, broker, exchange=None):
        calls.append(symbol)
        if symbol == weekly_key:
            return {"security_id": "WEEKLY123"}
        return {}

    monkeypatch.setattr(order_consumer.symbol_map, "get_symbol_for_broker", fake_get_symbol)
    monkeypatch.setattr(order_consumer.symbol_map, "get_symbol_for_broker_lazy", fake_get_symbol)

    normalized = order_consumer.normalize_symbol_to_dhan_format("NIFTY 23 SEP 25500 CALL")
    mapping = order_consumer.symbol_map.get_symbol_for_broker(normalized, "dhan", "NFO")

    assert normalized == weekly_key
    assert mapping["security_id"] == "WEEKLY123"
    assert calls == [weekly_key]


def test_symbol_map_retains_weekly_alias(monkeypatch):
    """Weekly expiries from Dhan should retain their own security id alias."""

    weekly_trading_symbol = "NIFTYNXT50-25Nov2025-35500-CE"
    canonical_symbol = "NIFTYNXT50-Nov2025-35500-CE"

    monkeypatch.setattr(order_consumer, "get_expiry_year", lambda month, day=None: "25")

    monthly_entry = {
        "security_id": "MONTHLY123",
        "lot_size": "25",
        "trading_symbol": canonical_symbol,
        "expiry_flag": "M",
    }
    weekly_entry = {
        "security_id": "WEEKLY456",
        "lot_size": "25",
        "trading_symbol": weekly_trading_symbol,
        "expiry_flag": "W",
        "expiry_day": 25,
        "expiry_date": "2025-11-25",
        "aliases": [canonical_symbol],
    }

    dhan_stub = {
        (canonical_symbol, "NFO"): monthly_entry,
        (weekly_trading_symbol, "NFO"): weekly_entry,
    }

    monkeypatch.setattr(symbol_map, "_load_zerodha", lambda: {})
    monkeypatch.setattr(symbol_map, "_load_dhan", lambda: dhan_stub)
    monkeypatch.setattr(symbol_map, "SYMBOL_MAP", {})
    monkeypatch.setattr(symbol_map, "_LAST_REFRESH", 0)

    mapping = symbol_map.build_symbol_map()
    symbol_map.SYMBOL_MAP = mapping

    normalized = order_consumer.normalize_symbol_to_dhan_format(
        "NIFTYNXT50 25 NOV 35500 CALL"
    )

    assert normalized == canonical_symbol

    canonical_data = symbol_map.get_symbol_for_broker(canonical_symbol, "dhan", "NFO")

    assert canonical_data["security_id"] == "WEEKLY456"
    assert canonical_symbol in canonical_data.get("aliases", [])


def test_currency_weekly_symbols_retain_day(monkeypatch):
    """Currency derivatives should keep the day component in the symbol."""

    weekly_key = "USDINR-23Sep2024-83-CE"

    monkeypatch.setattr(order_consumer, "get_expiry_year", lambda month, day=None: "24")
    monkeypatch.setattr(webhook_receiver, "get_expiry_year", lambda month, day=None: "24")
    monkeypatch.setattr(webhook_receiver, "get_lot_size_from_symbol_map", lambda *a, **k: None)

    normalized_consumer = order_consumer.normalize_symbol_to_dhan_format(
        "USDINR 23 SEP 83 CALL"
    )
    normalized_webhook, metadata = webhook_receiver.normalize_fo_symbol(
        "USDINR 23 SEP 83 CALL"
    )

    assert normalized_consumer == weekly_key
    assert normalized_webhook == weekly_key
    assert metadata["expiry_day"] == 23


def test_currency_future_without_day_resolves_from_symbol_map(monkeypatch):
    """Dayless currency futures should consult the symbol map for expiry day."""

    fo_symbol_utils._lookup_currency_future_expiry_day.cache_clear()

    fake_entry = {
        "dhan": {
            "security_id": "USDINR20240926",
            "expiry_day": 26,
        }
    }
    fake_map = {
        "USDINR": {
            symbol_map.SYMBOLS_KEY: {
                "NFO": {
                    "USDINR-26Sep2024-FUT": fake_entry,
                    "USDINR-Sep2024-FUT": fake_entry,
                }
            }
        }
    }
    calls = []

    def fake_ensure(symbol, exchange=None):
        calls.append((symbol, exchange))
        return fake_map.get("USDINR", {})

    def fake_get_symbol(symbol, broker, exchange=None):
        if symbol == "USDINR-26Sep2024-FUT" and broker == "dhan":
            return fake_entry["dhan"]
        return {}

    monkeypatch.setattr(symbol_map, "SYMBOL_MAP", fake_map)
    monkeypatch.setattr(fo_symbol_utils.symbol_map, "SYMBOL_MAP", fake_map)
    monkeypatch.setattr(order_consumer.symbol_map, "SYMBOL_MAP", fake_map)
    monkeypatch.setattr(symbol_map, "ensure_symbol_slice", fake_ensure)
    monkeypatch.setattr(fo_symbol_utils.symbol_map, "ensure_symbol_slice", fake_ensure)
    monkeypatch.setattr(order_consumer.symbol_map, "ensure_symbol_slice", fake_ensure)
    monkeypatch.setattr(order_consumer.symbol_map, "get_symbol_for_broker_lazy", fake_get_symbol)
    monkeypatch.setattr(order_consumer.symbol_map, "get_symbol_for_broker", fake_get_symbol)
    monkeypatch.setattr(order_consumer, "get_expiry_year", lambda month, day=None: "24")

    normalized = order_consumer.normalize_symbol_to_dhan_format("USDINRSEPFUT")
    assert normalized == "USDINR-26Sep2024-FUT"

    mapping = order_consumer.symbol_map.get_symbol_for_broker_lazy(
        normalized, "dhan", "NFO"
    )
    assert mapping["expiry_day"] == 26

    second = order_consumer.normalize_symbol_to_dhan_format("USDINRSEPFUT")
    assert second == normalized
    assert len(calls) == 1


def test_decimal_strike_normalization(monkeypatch):
    monkeypatch.setattr(order_consumer, "get_expiry_year", lambda month, day=None: "24")
    monkeypatch.setattr(webhook_receiver, "get_expiry_year", lambda month, day=None: "24")
    monkeypatch.setattr(webhook_receiver, "get_lot_size_from_symbol_map", lambda *a, **k: None)

    normalized_day_first = order_consumer.normalize_symbol_to_dhan_format(
        "USDINR 23 SEP 83.5 CALL"
    )
    assert normalized_day_first == "USDINR-23Sep2024-83.50-CE"

    normalized_webhook, webhook_metadata = webhook_receiver.normalize_fo_symbol(
        "USDINR 23 SEP 83.5 CALL"
    )
    assert normalized_webhook == "USDINR-23Sep2024-83.50-CE"
    assert webhook_metadata["strike"] == 83.5

    normalized_with_year = order_consumer.normalize_symbol_to_dhan_format("USDINR25SEP83.5CE")
    assert normalized_with_year == "USDINR-Sep2025-83.50-CE"

    normalized_webhook_with_year, metadata_with_year = webhook_receiver.normalize_fo_symbol(
        "USDINR25SEP83.5CE"
    )
    assert normalized_webhook_with_year == "USDINR-Sep2025-83.50-CE"
    assert metadata_with_year["strike"] == 83.5

    normalized_without_year = order_consumer.normalize_symbol_to_dhan_format(
        "USDINRSEP83.5CE"
    )
    assert normalized_without_year == "USDINR-Sep2024-83.50-CE"

    normalized_webhook_no_year, metadata_no_year = webhook_receiver.normalize_fo_symbol(
        "USDINRSEP83.5CE"
    )
    assert normalized_webhook_no_year == "USDINR-Sep2024-83.50-CE"
    assert metadata_no_year["strike"] == 83.5
