from brokers.symbol_map import get_symbol_for_broker


def test_symbol_map_includes_bse_x_series():
    mapping = get_symbol_for_broker("BSE:CASPIAN", "dhan")
    assert mapping.get("security_id")
    assert mapping.get("exchange_segment") == "BSE_EQ"
