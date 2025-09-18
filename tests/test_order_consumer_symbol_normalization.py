import pytest

from services.order_consumer import normalize_symbol_to_dhan_format


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
