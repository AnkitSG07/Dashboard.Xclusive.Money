import pytest

import services.order_consumer as order_consumer
import services.webhook_receiver as webhook_receiver
from services.fo_symbol_utils import is_fo_symbol


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


def test_weekly_option_normalization_encodes_day(monkeypatch):
    """Weekly expiries should retain their day in the canonical symbol."""

    weekly_key = "NIFTY-23Sep2024-25500-CE"

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
    """Symbol map lookup should use the weekly key instead of falling back to monthly."""

    weekly_key = "NIFTY-23Sep2024-25500-CE"
    monthly_key = "NIFTY-Sep2024-25500-CE"
    calls = []

    monkeypatch.setattr(order_consumer, "get_expiry_year", lambda month, day=None: "24")

    def fake_get_symbol(symbol, broker, exchange=None):
        calls.append(symbol)
        if symbol == weekly_key:
            return {"security_id": "WEEKLY123"}
        if symbol == monthly_key:
            return {"security_id": "MONTHLY999"}
        return {}

    monkeypatch.setattr(order_consumer.symbol_map, "get_symbol_for_broker", fake_get_symbol)

    normalized = order_consumer.normalize_symbol_to_dhan_format("NIFTY 23 SEP 25500 CALL")
    mapping = order_consumer.symbol_map.get_symbol_for_broker(normalized, "dhan", "NFO")

    assert normalized == weekly_key
    assert mapping["security_id"] == "WEEKLY123"
    assert monthly_key not in calls
