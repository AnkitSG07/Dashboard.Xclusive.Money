"""Dynamic symbol map for Indian equities.

This module builds a mapping of every equity symbol available on the
Indian stock exchanges (NSE and BSE) across a number of supported
brokers.  The data is generated from publicly available instrument dumps
so that the map automatically stays up to date without having to maintain
it manually.

Only the common exchange token is used for most brokers as they rely on
the standard exchange instrumentation.  Dhan uses its own security id,
which is retrieved from their master scrip file and joined with the
exchange token information.
"""

from __future__ import annotations

import csv
from functools import lru_cache
from io import StringIO
from typing import Dict, Tuple

import requests


# URLs that expose complete instrument dumps for the respective brokers.
ZERODHA_URL = "https://api.kite.trade/instruments"
DHAN_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"


Key = Tuple[str, str]


@lru_cache(maxsize=1)
def _load_zerodha() -> Dict[Key, str]:
    """Return mapping of (symbol, exchange) to instrument token.

    Zerodha publishes an instrument dump containing tokens for all
    instruments across exchanges.  Only equity instruments from NSE and
    BSE are considered here.
    """

    resp = requests.get(ZERODHA_URL, timeout=30)
    resp.raise_for_status()

    reader = csv.DictReader(StringIO(resp.text))
    data: Dict[Key, str] = {}
    for row in reader:
        if row["segment"] in {"NSE", "BSE"} and row["instrument_type"] == "EQ":
            key = (row["tradingsymbol"], row["exchange"].upper())
            data[key] = row["instrument_token"]
    return data


@lru_cache(maxsize=1)
def _load_dhan() -> Dict[Key, str]:
    """Return mapping of (symbol, exchange) to Dhan security id."""

    resp = requests.get(DHAN_URL, timeout=30)
    resp.raise_for_status()

    reader = csv.DictReader(StringIO(resp.text))
    data: Dict[Key, str] = {}
    for row in reader:
        if row["SEM_EXM_EXCH_ID"] in {"NSE", "BSE"} and row["SEM_SEGMENT"] == "E":
            key = (row["SEM_TRADING_SYMBOL"], row["SEM_EXM_EXCH_ID"].upper())
            # prefer the EQ series when available but fall back to any
            # other equity series so that newly listed or less common
            # scrips (e.g. BSE "X" group) are still included
            if key not in data or row["SEM_SERIES"] == "EQ":
                data[key] = row["SEM_SMST_SECURITY_ID"]
    return data


def build_symbol_map() -> Dict[str, Dict[str, Dict[str, str]]]:
    """Construct the complete symbol map.

    For each equity symbol we create entries for a variety of brokers.  The
    Zerodha instrument token is used for brokers that rely on the exchange
    token.  Dhan's security id is joined whenever available.
    """

    zerodha = _load_zerodha()
    dhan = _load_dhan()

    mapping: Dict[str, Dict[str, Dict[str, str]]] = {}

    for (symbol, exchange), token in zerodha.items():
        entry = {
            "zerodha": {
                "trading_symbol": symbol,
                "exchange": exchange,
                "token": token,
            },
            "aliceblue": {
                "trading_symbol": f"{symbol}-EQ",
                "symbol_id": token,
                "exch": exchange,
            },
            "fyers": {
                "symbol": f"{exchange}:{symbol}-EQ",
                "token": token,
            },
            "finvasia": {
                "symbol": f"{symbol}-EQ",
                "token": token,
                "exchange": exchange,
            },
            "flattrade": {
                "symbol": f"{symbol}-EQ",
                "token": token,
                "exchange": exchange,
            },
            "acagarwal": {
                "symbol": f"{symbol}-EQ",
                "token": token,
                "exchange": exchange,
            },
            "motilaloswal": {
                "symbol": symbol,
                "token": token,
                "exchange": exchange,
            },
            "kotakneo": {
                "symbol": symbol,
                "token": token,
                "exchange": exchange,
            },
            "tradejini": {
                "symbol": f"{symbol}-EQ",
                "token": token,
                "exchange": exchange,
            },
            "zebu": {
                "symbol": f"{symbol}-EQ",
                "token": token,
                "exchange": exchange,
            },
            "enrichmoney": {
                "symbol": f"{symbol}-EQ",
                "token": token,
                "exchange": exchange,
            },
        }

        dhan_id = dhan.get((symbol, exchange))
        if dhan_id:
            entry["dhan"] = {
                "security_id": dhan_id,
                "exchange_segment": f"{exchange}_EQ",
            }

        mapping[symbol] = entry

    return mapping


# Public symbol map used throughout the application.
SYMBOL_MAP = build_symbol_map()


__all__ = ["SYMBOL_MAP", "build_symbol_map"]


def get_symbol_for_broker(symbol: str, broker: str) -> Dict[str, str]:
    """Return the mapping for *symbol* for the given *broker*.

    The lookup normalises broker and symbol names so callers can pass
    tokens in a variety of formats (e.g. ``NSE:IDEA-EQ``).
    """
    symbol = symbol.upper()
    broker = broker.lower()
    
    base = symbol.split(":")[-1]
    if base.endswith("-EQ"):
        base = base[:-3]

    mapping = SYMBOL_MAP.get(symbol) or SYMBOL_MAP.get(base)
    if not mapping:
        base2 = base.split("-")[0]
        mapping = SYMBOL_MAP.get(base2, {})

    return mapping.get(broker, {}) if mapping else {}


def get_symbol_by_token(token: str, broker: str) -> str | None:
    """Return the base symbol for a broker given its token/instrument id."""
    
    broker = broker.lower()
    token = str(token)

    for sym, mapping in SYMBOL_MAP.items():
        broker_map = mapping.get(broker)
        if broker_map and str(broker_map.get("token")) == token:
            return sym
    return None


__all__.extend(["get_symbol_for_broker", "get_symbol_by_token"])
