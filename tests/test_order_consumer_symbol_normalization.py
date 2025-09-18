import pytest

from services.order_consumer import normalize_symbol_to_dhan_format
from services.webhook_receiver import normalize_fo_symbol
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
    assert normalize_symbol_to_dhan_format(symbol) == expected


@pytest.mark.parametrize(
    "symbol",
    ["RELIANCE", "RELIANCECE", "SBVCL", "SBVCLPE"],
)
def test_equity_like_symbols_unchanged(symbol):
    """Equity tickers, even those ending with CE/PE, stay untouched."""
    assert normalize_symbol_to_dhan_format(symbol) == symbol


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
    normalized, metadata = normalize_fo_symbol(symbol)
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
