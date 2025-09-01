from brokers.symbol_map import get_symbol_map


def get_symbols():
    """Return list of symbols with token mappings for common brokers."""
    symbols = []
    symbol_map = get_symbol_map()
    for sym, data in symbol_map.items():
        entry = {"symbol": sym}

        dhan_id = data.get("dhan", {}).get("security_id")
        if dhan_id:
            entry["dhan"] = dhan_id

        alice_id = data.get("aliceblue", {}).get("symbol_id")
        if alice_id:
            entry["aliceblue"] = alice_id

        angel_id = (
            data.get("angelone", {}).get("token")
            or data.get("angel", {}).get("token")
        )
        if angel_id:
            entry["angelone"] = angel_id

        iifl_id = data.get("iifl", {}).get("token")
        if iifl_id:
            entry["iifl"] = iifl_id

        kotak_id = data.get("kotakneo", {}).get("token")
        if kotak_id:
            entry["kotak"] = kotak_id

        upstox_id = data.get("upstox", {}).get("token")
        if upstox_id:
            entry["upstox"] = upstox_id

        if len(entry) > 1:
            symbols.append(entry)
    return symbols
