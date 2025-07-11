from brokers.symbol_map import SYMBOL_MAP

def get_symbols():
    """Return list of symbol and security_id pairs from SYMBOL_MAP."""
    symbols = []
    for sym, data in SYMBOL_MAP.items():
        sec_id = data.get('dhan', {}).get('security_id')
        if sec_id:
            symbols.append({'symbol': sym, 'security_id': sec_id})
    return symbols
