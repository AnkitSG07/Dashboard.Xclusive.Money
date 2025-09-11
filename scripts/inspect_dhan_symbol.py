#!/usr/bin/env python3
"""Inspect Dhan symbol mapping for a raw trading symbol.

This script accepts a raw ``SEM_TRADING_SYMBOL`` from Dhan, normalises it via
``_canonical_dhan_symbol`` and prints the canonical key together with any
matching entry in the global symbol map (including ``lot_size`` and
``security_id``).
"""

from __future__ import annotations

import argparse
import json

from brokers.symbol_map import _canonical_dhan_symbol, get_symbol_map


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect symbol map for a Dhan trading symbol"
    )
    parser.add_argument("symbol", help="Raw SEM_TRADING_SYMBOL")
    args = parser.parse_args()

    key = _canonical_dhan_symbol(args.symbol)
    print(f"Canonical key: {key}")

    symbol_map = get_symbol_map()
    entry = symbol_map.get(key)
    if not entry:
        print("No entry found in symbol map.")
        return

    dhan_entries = {}
    for exchange, brokers in entry.items():
        info = brokers.get("dhan")
        if info:
            dhan_entries[exchange] = {
                "security_id": info.get("security_id"),
                "lot_size": info.get("lot_size"),
            }

    if not dhan_entries:
        print("No Dhan entry found in symbol map.")
        return

    print(json.dumps(dhan_entries, indent=2))


if __name__ == "__main__":
    main()
