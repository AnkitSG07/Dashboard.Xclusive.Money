from services.fo_symbol_utils import parse_fo_symbol


def test_parse_fo_symbol_handles_fyers_weekly_symbols():
    result = parse_fo_symbol("NIFTY25O0719400CE", "fyers")

    assert result == {
        "underlying": "NIFTY",
        "year": "2025",
        "month": "O07",
        "strike": "19400",
        "option_type": "CE",
        "instrument": "OPT",
    }
