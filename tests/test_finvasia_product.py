import brokers.finvasia as fin


def test_normalize_product_intraday_variants():
    broker = fin.FinvasiaBroker.__new__(fin.FinvasiaBroker)
    assert broker._normalize_product("MIS") == "M"
    assert broker._normalize_product("intra") == "M"
    assert broker._normalize_product("INTRADAY") == "M"
    assert broker._normalize_product("Intra Day") == "M"
