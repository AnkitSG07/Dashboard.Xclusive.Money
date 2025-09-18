from __future__ import annotations

"""Asynchronous worker that consumes webhook events and places orders."""

import asyncio
import json
import logging
import os
import re
import time
import inspect
from concurrent.futures import ThreadPoolExecutor, wait
from functools import partial
from typing import Any, Dict, Iterable, List
from datetime import datetime, date

from prometheus_client import Counter

from brokers.factory import get_broker_client
from brokers import symbol_map
import redis

from .alert_guard import check_risk_limits, get_user_settings
from .webhook_receiver import redis_client, get_redis_client
from .utils import _decode_event
from .db import get_session
from models import Strategy, Account
from .master_trade_monitor import COMPLETED_STATUSES

log = logging.getLogger(__name__)

orders_success = Counter(
    "order_consumer_success_total",
    "Number of webhook events processed successfully",
)
orders_failed = Counter(
    "order_consumer_failure_total",
    "Number of webhook events that failed processing",
)

DEFAULT_MAX_WORKERS = int(os.getenv("ORDER_CONSUMER_MAX_WORKERS", "10"))
REJECTED_STATUSES = {"REJECTED", "CANCELLED", "CANCELED", "FAILED"}


def get_expiry_year(month: str, day: int = None) -> str:
    """Determine the correct expiry year for a given month and day."""
    current_date = date.today()
    current_year = current_date.year
    current_month = current_date.month
    current_day = current_date.day
    
    month_map = {
        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
        'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
    }
    
    month_num = month_map[month]
    
    if day:
        if month_num < current_month:
            year = current_year + 1
        elif month_num == current_month and day < current_day:
            year = current_year + 1
        else:
            year = current_year
    else:
        if month_num < current_month:
            year = current_year + 1
        else:
            year = current_year
    
    return str(year % 100).zfill(2)


def parse_fo_symbol(symbol: str, broker: str) -> dict:
    """Parse F&O symbol into components based on broker format."""
    if not symbol:
        return None
    
    symbol = symbol.upper().strip()
    broker = broker.lower()
    
    if broker == 'dhan':
        # NIFTY-Dec2024-24000-CE or NIFTY-Dec2024-FUT format
        opt_match = re.match(r'^(.+?)-(\w{3})(\d{4})-(\d+)-(CE|PE)$', symbol)
        if opt_match:
            return {
                'underlying': opt_match.group(1),
                'month': opt_match.group(2),
                'year': opt_match.group(3),
                'strike': opt_match.group(4),
                'option_type': opt_match.group(5),
                'instrument': 'OPT'
            }
        
        fut_match = re.match(r'^(.+?)-(\w{3})(\d{4})-FUT$', symbol)
        if fut_match:
            return {
                'underlying': fut_match.group(1),
                'month': fut_match.group(2),
                'year': fut_match.group(3),
                'instrument': 'FUT'
            }
    
    elif broker in ['zerodha', 'aliceblue', 'fyers', 'finvasia']:
        # NIFTY24DEC24000CE format
        opt_match = re.match(r'^(.+?)(\d{2})(\w{3})(\d+)(CE|PE)$', symbol)
        if opt_match:
            return {
                'underlying': opt_match.group(1),
                'year': '20' + opt_match.group(2),
                'month': opt_match.group(3),
                'strike': opt_match.group(4),
                'option_type': opt_match.group(5),
                'instrument': 'OPT'
            }
        
        # NIFTY24DECFUT format
        fut_match = re.match(r'^(.+?)(\d{2})(\w{3})FUT$', symbol)
        if fut_match:
            return {
                'underlying': fut_match.group(1),
                'year': '20' + fut_match.group(2),
                'month': fut_match.group(3),
                'instrument': 'FUT'
            }
    
    return None


def format_fo_symbol(components: dict, to_broker: str) -> str:
    """Format symbol components for target broker."""
    if not components:
        return None
    
    to_broker = to_broker.lower()
    
    if to_broker == 'dhan':
        if components['instrument'] == 'OPT':
            return f"{components['underlying']}-{components['month']}{components['year']}-{components['strike']}-{components['option_type']}"
        elif components['instrument'] == 'FUT':
            return f"{components['underlying']}-{components['month']}{components['year']}-FUT"
    
    elif to_broker in ['zerodha', 'aliceblue', 'fyers', 'finvasia']:
        year_short = components['year'][-2:]  # Get last 2 digits
        if components['instrument'] == 'OPT':
            return f"{components['underlying']}{year_short}{components['month'].upper()}{components['strike']}{components['option_type']}"
        elif components['instrument'] == 'FUT':
            return f"{components['underlying']}{year_short}{components['month'].upper()}FUT"
    
    return None


def convert_symbol_between_brokers(symbol: str, from_broker: str, to_broker: str, instrument_type: str = None) -> str:
    """Convert F&O symbol from one broker format to another."""
    if not symbol or from_broker.lower() == to_broker.lower():
        return symbol
    
    # First, parse the symbol to extract components
    components = parse_fo_symbol(symbol, from_broker)
    
    if not components:
        log.debug(f"Could not parse F&O symbol: {symbol} for broker: {from_broker}")
        return symbol  # Return original if can't parse
    
    # Convert to target broker format
    converted = format_fo_symbol(components, to_broker)
    
    if converted:
        log.info(f"Converted F&O symbol from {symbol} ({from_broker}) to {converted} ({to_broker})")
        return converted
    
    log.warning(f"Could not convert symbol {symbol} from {from_broker} to {to_broker}")
    return symbol


def normalize_symbol_to_dhan_format(symbol: str) -> str:
    """Convert various symbol formats to Dhan's expected format.
    
    Examples:
        NIFTYNXT50SEPFUT -> NIFTYNXT50-Sep2025-FUT
        FINNIFTY25SEP33300CE -> FINNIFTY-Sep2025-33300-CE
        RELIANCE -> RELIANCE (no change for equity)
    """
    if not symbol:
        return symbol
    
    original_symbol = symbol.strip()
    sym = original_symbol.upper()
    log.debug(f"Normalizing symbol: {sym}")
    
    # CRITICAL: Don't modify plain equity symbols
    if not any(x in sym for x in ['FUT', 'CE', 'PE', 'CALL', 'PUT', '-']) and \
       not re.search(r'(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)', sym):
        # It's a simple equity symbol, return as-is
        log.debug(f"Identified as equity symbol, no normalization needed: {sym}")
        return original_symbol

    # Handle already correctly formatted symbols (with hyphens)
    if '-' in sym and re.search(r'(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)20\d{2}', sym):
        pattern = re.compile(
            r'^(?P<root>.+?)-(?P<month>[A-Za-z]{3})(?P<year>\d{4})(?P<suffix>-(?:\d+-(?:CE|PE)|FUT))$',
            re.IGNORECASE,
        )
        match = pattern.match(original_symbol)
        if match:
            month = match.group('month').upper().title()
            normalized = f"{match.group('root')}-{month}{match.group('year')}{match.group('suffix')}"
            log.debug(
                "Symbol already in correct format, preserving casing: %s -> %s",
                original_symbol,
                normalized,
            )
            return normalized

        log.debug(
            "Symbol already in correct format, returning original: %s",
            original_symbol,
        )
        return original_symbol
        
    # CRITICAL FIX: Handle day-month format "UNDERLYING DD MON STRIKE TYPE"
    # Pattern 1: Format with day first: "NIFTY 23 SEP 25500 CALL"
    day_first_pattern = re.match(
        r'^([A-Z]+(?:\d+)?)\s+(\d{1,2})\s+(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+(\d+)\s+(CALL|PUT|CE|PE)$',
        sym
    )
    if day_first_pattern:
        root = day_first_pattern.group(1)
        day = int(day_first_pattern.group(2))
        month = day_first_pattern.group(3)
        strike = day_first_pattern.group(4)
        opt_type = day_first_pattern.group(5)
        
        # The day is the expiry day, not year
        year = get_expiry_year(month, day)
        full_year = f"20{year}"
        opt_code = "CE" if opt_type in ("CALL", "CE") else "PE"
        
        normalized = f"{root}-{month.title()}{full_year}-{strike}-{opt_code}"
        log.info(f"Normalized from day-first format '{sym}' to '{normalized}'")
        return normalized
    
    # Pattern 2: Futures with day: "NIFTY 23 SEP FUT"
    fut_with_day = re.match(
        r'^([A-Z]+(?:\d+)?)\s+(\d{1,2})\s+(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+FUT$',
        sym
    )
    if fut_with_day:
        root = fut_with_day.group(1)
        day = int(fut_with_day.group(2))
        month = fut_with_day.group(3)
        
        year = get_expiry_year(month, day)
        full_year = f"20{year}"
        normalized = f"{root}-{month.title()}{full_year}-FUT"
        log.info(f"Normalized futures with day from '{sym}' to '{normalized}'")
        return normalized
    
    # Pattern 3: Compact futures format with explicit year: FINNIFTY25SEPFUT
    fut_with_year = re.match(
        r'^(.+?)(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)FUT$',
        sym
    )
    if fut_with_year:
        root, year, month = fut_with_year.groups()
        year_num = int(year)
        if 24 <= year_num <= 30:
            full_year = f"20{year}"
            normalized = f"{root}-{month.title()}{full_year}-FUT"
            log.info(f"Normalized futures with year from '{sym}' to '{normalized}'")
            return normalized
    
    # Pattern 4: Compact futures format without explicit year: NIFTYNXT50SEPFUT
    fut_no_year = re.match(
        r'^(.+?)(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)FUT$',
        sym
    )
    if fut_no_year:
        root, month = fut_no_year.groups()
        year = get_expiry_year(month)
        full_year = f"20{year}"
        normalized = f"{root}-{month.title()}{full_year}-FUT"
        log.info(f"Normalized futures without year from '{sym}' to '{normalized}'")
        return normalized
    
    # Pattern 5: Options with explicit year: FINNIFTY25SEP33300CE
    opt_with_year = re.match(
        r'^(.+?)(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d+)(CE|PE)$',
        sym
    )
    if opt_with_year:
        root, year, month, strike, opt_type = opt_with_year.groups()
        year_num = int(year)
        if 24 <= year_num <= 30:
            full_year = f"20{year}"
            normalized = f"{root}-{month.title()}{full_year}-{strike}-{opt_type}"
            log.info(f"Normalized options with year from '{sym}' to '{normalized}'")
            return normalized
    
    # Pattern 6: Options without explicit year: NIFTYNXT50SEP33300CE
    opt_no_year = re.match(
        r'^(.+?)(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d+)(CE|PE)$',
        sym
    )
    if opt_no_year:
        root, month, strike, opt_type = opt_no_year.groups()
        year = get_expiry_year(month)
        full_year = f"20{year}"
        normalized = f"{root}-{month.title()}{full_year}-{strike}-{opt_type}"
        log.info(f"Normalized options without year from '{sym}' to '{normalized}'")
        return normalized
    
    # Pattern 7: Handle equity symbols - IMPROVED LOGIC
    # Check if it's NOT a derivative and looks like an equity symbol
    if (not re.search(r'(FUT|CE|PE|CALL|PUT)$', sym) and 
        not re.search(r'(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)', sym) and
        not sym.endswith('-EQ')):
        # It's likely an equity symbol - add -EQ suffix
        normalized = f"{sym}-EQ"
        log.info(f"Normalized equity symbol from '{sym}' to '{normalized}'")
        return normalized
    
    # If already has -EQ suffix, keep it
    if sym.endswith('-EQ'):
        log.debug(f"Symbol already has -EQ suffix: {sym}")
        return sym
    
    log.debug(f"No normalization pattern matched for: {sym}")
    return sym


def get_default_lot_size(symbol: str) -> int:
    """Get default lot size for common F&O instruments."""
    symbol_upper = symbol.upper()
    
    lot_size_map = {
        'NIFTY': 50,
        'BANKNIFTY': 25, 
        'FINNIFTY': 40,
        'NIFTYNXT': 50,
        'MIDCPNIFTY': 75,
        'SENSEX': 10,
        'BANKEX': 15,
    }
    
    for underlying, size in lot_size_map.items():
        if underlying in symbol_upper:
            return size
    
    # Default for stock F&O
    return 1


def consume_webhook_events(
    *,
    stream: str = "webhook_events",
    group: str = "order_consumer",
    consumer: str = "worker-1",
    redis_client=redis_client,
    max_messages: int | None = None,
    block: int = 0,
    batch_size: int = 10,
    max_workers: int | None = None,
    order_timeout: float | None = None,
) -> int:
    """Consume events from *stream* using a consumer group and place orders."""
    
    max_workers = max_workers or DEFAULT_MAX_WORKERS
    if order_timeout is None:
        order_timeout = float(
            os.getenv(
                "ORDER_CONSUMER_TIMEOUT",
                os.getenv("BROKER_TIMEOUT", "20"),
            )
        )

    if redis_client is None:
        redis_client = get_redis_client()

    try:
        redis_client.xgroup_create(stream, group, id="0", mkstream=True)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise

    executor = ThreadPoolExecutor(max_workers=max_workers)

    def process_message(msg_id: str, data: Dict[Any, Any]) -> None:
        event = _decode_event(data)
        try:
            check_risk_limits(event)
            settings = get_user_settings(event["user_id"])
            brokers = settings.get("brokers", [])

            # Enhanced symbol normalization for derivatives
            symbol = event.get("symbol", "")
            instrument_type = event.get("instrument_type", "")
            
            # Check if it's a derivative
            is_derivative = (
                instrument_type.upper() in {"FUT", "FUTSTK", "FUTIDX", "OPT", "OPTSTK", "OPTIDX", "CE", "PE"} or
                bool(re.search(r'(FUT|CE|PE)$', symbol.upper()))
            )
            
            if is_derivative:
                normalized_symbol = normalize_symbol_to_dhan_format(symbol)
                if normalized_symbol != symbol:
                    log.info(f"Normalized derivative symbol from '{symbol}' to '{normalized_symbol}'")
                    event["symbol"] = normalized_symbol
                    
                    # Update instrument type based on normalized symbol
                    if "-FUT" in normalized_symbol:
                        event["instrument_type"] = "FUTIDX" if "NIFTY" in normalized_symbol else "FUTSTK"
                    elif re.search(r'-\d+-(CE|PE)$', normalized_symbol):
                        event["instrument_type"] = "OPTIDX" if "NIFTY" in normalized_symbol else "OPTSTK"
                        # Extract and set strike price and option type
                        match = re.search(r'-(\d+)-(CE|PE)$', normalized_symbol)
                        if match:
                            event["strike"] = int(match.group(1))
                            event["option_type"] = match.group(2)

            # Handle allowed accounts
            allowed_accounts = event.get("masterAccounts") or []
            if isinstance(allowed_accounts, str):
                try:
                    allowed_accounts = json.loads(allowed_accounts)
                except json.JSONDecodeError:
                    log.error("invalid masterAccounts JSON: %s", allowed_accounts, extra={"event": event})
                    orders_failed.inc()
                    return
                if not isinstance(allowed_accounts, list):
                    log.error("masterAccounts JSON was not a list: %s", allowed_accounts, extra={"event": event})
                    orders_failed.inc()
                    return
                event["masterAccounts"] = allowed_accounts
                
            if not allowed_accounts:
                strategy_id = event.get("strategy_id")
                if strategy_id is not None:
                    session = get_session()
                    try:
                        strategy = session.query(Strategy).get(strategy_id)
                        if strategy and strategy.master_accounts:
                            allowed_accounts = [
                                a.strip()
                                for a in str(strategy.master_accounts).split(",")
                                if a.strip()
                            ]
                    finally:
                        session.close()
                        
            if allowed_accounts:
                invalid_ids = [acc_id for acc_id in allowed_accounts if not str(acc_id).isdigit()]
                if invalid_ids:
                    log.error("non-numeric master account id(s): %s", invalid_ids, extra={"event": event})
                    orders_failed.inc()
                    return

                ids: List[int] = [int(acc_id) for acc_id in allowed_accounts]
                session = get_session()
                try:
                    rows = session.query(Account).filter(Account.id.in_(ids)).all()
                finally:
                    session.close()
                allowed_pairs = {(r.broker, r.client_id) for r in rows}
                brokers = [
                    b for b in brokers
                    if (b.get("name"), b.get("client_id")) in allowed_pairs
                ]

                if not brokers:
                    log.error("no brokers permitted for user", extra={"event": event})
                    orders_failed.inc()
                    return
            elif not brokers:
                log.error("no brokers configured for user", extra={"event": event})
                orders_failed.inc()
                return

            def _normalize_keys(data: Dict[str, Any]) -> Dict[str, Any]:
                """Return a dict with camelCase keys converted to snake_case."""
                normalized: Dict[str, Any] = {}
                for key, value in data.items():
                    new_key = re.sub(r"([A-Z])", lambda m: "_" + m.group(1).lower(), key)
                    normalized[new_key] = value
                return normalized

            def submit(broker_cfg: Dict[str, Any]) -> Dict[str, Any]:
                client_cls = get_broker_client(broker_cfg["name"])
                credentials = _normalize_keys(dict(broker_cfg))
                access_token = credentials.pop("access_token", "")
                client_id = credentials.pop("client_id", None)
                credentials.pop("name", None)
                
                # Validate required credentials
                try:
                    sig = inspect.signature(client_cls)
                except (TypeError, ValueError):
                    sig = None
                if sig is not None:
                    required = [
                        p.name
                        for p in sig.parameters.values()
                        if p.kind in (
                            inspect.Parameter.POSITIONAL_OR_KEYWORD,
                            inspect.Parameter.KEYWORD_ONLY,
                        )
                        and p.default is inspect._empty
                        and p.name not in ("self", "client_id", "access_token")
                    ]
                    missing = [p for p in required if p not in credentials]
                    if missing:
                        raise ValueError(
                            "missing required broker credential(s): " + ", ".join(missing)
                        )

                client = client_cls(
                    client_id=client_id,
                    access_token=access_token,
                    **credentials,
                )
                
                # Get original symbol and determine if it's F&O
                original_symbol = event.get("symbol", "")
                instrument_type = event.get("instrument_type", "")
                
                is_fo = bool(re.search(r'(FUT|CE|PE)$', str(original_symbol).upper())) or \
                        instrument_type.upper() in {"FUT", "FUTSTK", "FUTIDX", "OPT", "OPTSTK", "OPTIDX", "CE", "PE"}
                
                # Convert symbol if it's F&O and brokers are different
                converted_symbol = original_symbol
                if is_fo and broker_cfg["name"].lower() != "dhan":  # Assuming event comes from Dhan format
                    converted_symbol = convert_symbol_between_brokers(
                        original_symbol, 
                        "dhan",  # Source broker format
                        broker_cfg["name"], 
                        instrument_type
                    )
                    if converted_symbol and converted_symbol != original_symbol:
                        log.info(f"Converted F&O symbol from {original_symbol} to {converted_symbol} for {broker_cfg['name']}")

                order_params = {
                    "symbol": converted_symbol,
                    "action": event["action"],
                    "qty": event["qty"],
                }
                
                # Set exchange
                exchange = event.get("exchange")
                if exchange is not None:
                    if exchange in {"NSE", "BSE"} and is_fo:
                        exchange = {"NSE": "NFO", "BSE": "BFO"}[exchange]
                    order_params["exchange"] = exchange
                
                # Copy F&O specific parameters
                fo_params = ["instrument_type", "expiry", "strike", "option_type", "lot_size", "security_id"]
                for param in fo_params:
                    if event.get(param) is not None:
                        order_params[param] = event[param]
                
                # Handle additional parameters
                if event.get("order_type") is not None:
                    order_params["order_type"] = event["order_type"]
                    
                optional_map = {
                    "productType": "product_type",
                    "orderValidity": "validity",
                    "masterAccounts": "master_accounts",
                    "securityId": "security_id",
                }
                for src, dest in optional_map.items():
                    if event.get(src) is not None:
                        order_params[dest] = event[src]
                
                # Enhanced lot size handling for F&O
                lot_size = None
                if is_fo:
                    lot_size = event.get("lot_size")
                    
                    # Try to get from symbol map using converted symbol
                    if lot_size is None:
                        try:
                            mapping = symbol_map.get_symbol_for_broker(
                                converted_symbol, broker_cfg["name"], exchange
                            )
                            lot_size = mapping.get("lot_size") or mapping.get("lotSize")
                            
                            if lot_size:
                                log.info(f"Found lot size {lot_size} for {converted_symbol} from symbol map")
                        except Exception as e:
                            log.warning(
                                "Could not retrieve lot size from symbol map for %s: %s",
                                converted_symbol, str(e), extra={"event": event, "broker": broker_cfg}
                            )
                    
                    # Enhanced fallback lot sizes
                    if not lot_size:
                        lot_size = get_default_lot_size(converted_symbol)
                        log.info(f"Using default lot size {lot_size} for {converted_symbol}")
                    
                    if lot_size:
                        try:
                            original_qty = int(float(event["qty"]))
                            lot_size_int = int(float(lot_size))
                            calculated_qty = original_qty * lot_size_int
                            order_params["qty"] = calculated_qty
                            order_params["lot_size"] = lot_size_int
                            log.info(
                                f"F&O quantity adjustment: {original_qty} lots × {lot_size_int} = {calculated_qty}"
                            )
                        except (ValueError, TypeError):
                            log.error("Invalid lot size or quantity for F&O order")
                            return None
                
                try:
                    result = client.place_order(**order_params)
                    order_id = None
                    if isinstance(result, dict):
                        order_id = (
                            result.get("order_id")
                            or result.get("id")
                            or result.get("data", {}).get("order_id")
                            or result.get("orderId")
                            or result.get("data", {}).get("orderId")
                        )
                    if not isinstance(result, dict) or result.get("status") != "success" or not order_id:
                        raise RuntimeError(f"broker order failed: {result}")
                    
                    # Check order status
                    status = None
                    try:
                        if hasattr(client, "get_order"):
                            info = client.get_order(order_id)
                            if isinstance(info, dict):
                                status = info.get("status") or info.get("data", {}).get("status")
                        elif hasattr(client, "list_orders"):
                            orders = client.list_orders()
                            for order in orders:
                                oid = (
                                    order.get("id")
                                    or order.get("order_id")
                                    or order.get("orderId")
                                    or order.get("data", {}).get("order_id")
                                )
                                if str(oid) == str(order_id):
                                    status = order.get("status") or order.get("data", {}).get("status")
                                    break
                    except Exception:
                        log.warning("failed to fetch order status", extra={"order_id": order_id}, exc_info=True)
                    
                    status_upper = str(status).upper() if status is not None else None
                    if status_upper in REJECTED_STATUSES:
                        log.info("skipping trade event due to rejected status", extra={"order_id": order_id, "status": status})
                        return None
                    if status_upper not in COMPLETED_STATUSES:
                        log.info("publishing trade event with incomplete status", extra={"order_id": order_id, "status": status})
                        
                    trade_event = {
                        "master_id": client_id,
                        **{k: v for k, v in order_params.items() if k != "master_accounts"},
                    }
                    return trade_event
                    
                except Exception as exc:
                    message = getattr(exc, "message", None) or (
                        exc.args[0] if getattr(exc, "args", None) else str(exc)
                    )
                    raise RuntimeError(message) from exc

            def _late_completion(cfg, fut):
                try:
                    result = fut.result()
                    log.warning("broker %s completed after timeout: %s", cfg["name"], result)
                except BaseException as exc:
                    log.warning("broker %s failed after timeout: %s", cfg["name"], exc, exc_info=exc)

            trade_events: List[Dict[str, Any]] = []
            if brokers:
                futures = {executor.submit(submit, cfg): cfg for cfg in brokers}
                done, pending = wait(futures, timeout=order_timeout)
                timed_out = []
                for future in done:
                    cfg = futures[future]
                    try:
                        trade_event = future.result()
                        if trade_event:
                            trade_events.append(trade_event)
                    except Exception:
                        orders_failed.inc()
                        log.exception("failed to place master order", extra={"event": event, "broker": cfg})
                for future in pending:
                    cfg = futures[future]
                    orders_failed.inc()
                    log.warning("broker order timed out", extra={"event": event, "broker": cfg})
                    if not future.cancel():
                        timed_out.append((future, cfg))
                for fut, cfg in timed_out:
                    fut.add_done_callback(partial(_late_completion, cfg))
            else:
                orders_failed.inc()
                log.warning("no brokers configured for user", extra={"event": event})
                
            for trade_event in trade_events:
                redis_client.xadd("trade_events", trade_event)

            if trade_events:
                orders_success.inc()
                log.info("processed webhook event", extra={"event": event})
            else:
                orders_failed.inc()
        except Exception:
            orders_failed.inc()
            log.exception("failed to process webhook event", extra={"event": event})
        finally:
            redis_client.xack(stream, group, msg_id)

    async def _consume() -> int:
        processed = 0
        while max_messages is None or processed < max_messages:
            count = batch_size
            if max_messages is not None:
                count = min(count, max_messages - processed)
            messages: Iterable = redis_client.xreadgroup(
                group, consumer, {stream: ">"}, count=count, block=block
            )
            if not messages:
                break

            tasks = []
            for _stream, events in messages:
                for msg_id, data in events:
                    tasks.append(asyncio.to_thread(process_message, msg_id, data))
            if tasks:
                await asyncio.gather(*tasks)
                processed += len(tasks)

        return processed

    try:
        return asyncio.run(_consume())
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def main() -> None:
    """Run the consumer indefinitely."""
    while True:
        try:
            consume_webhook_events(block=5000)
        except redis.exceptions.RedisError:
            log.exception("redis unavailable, retrying", exc_info=True)
            time.sleep(5)


__all__ = [
    "consume_webhook_events",
    "orders_success",
    "orders_failed",
    "normalize_symbol_to_dhan_format",
    "convert_symbol_between_brokers",
    "parse_fo_symbol",
    "format_fo_symbol",
    "get_default_lot_size",
]


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    try:
        main()
    except KeyboardInterrupt:
        pass
