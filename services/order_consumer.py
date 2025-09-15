    # services/order_consumer.py - CORRECTED VERSION
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
    """Convert various symbol formats to Dhan's expected format."""
    if not symbol:
        return symbol
    
    sym = symbol.upper().strip()
    log.debug(f"Normalizing symbol: {sym}")
    
    # Handle already correctly formatted symbols (with hyphens)
    if '-' in sym and re.search(r'(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)20\d{2}', sym):
        log.debug(f"Symbol already in correct format: {sym}")
        return sym
    
    # Pattern 1: Compact futures format with explicit year: FINNIFTY25SEPFUT
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
    
    # Pattern 2: Compact futures format without explicit year: NIFTYNXT50SEPFUT
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
    
    # Pattern 3: Options with explicit year: FINNIFTY25SEP33300CE
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
    
    # Pattern 4: Options without explicit year: NIFTYNXT50SEP33300CE
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
    
    # Pattern 5: Handle equity symbols
    if not re.search(r'(FUT|CE|PE)$', sym) and not sym.endswith('-EQ'):
        if not re.search(r'\d', sym) or re.match(r'^[A-Z]+\d+$', sym):
            normalized = f"{sym}-EQ" if not sym.endswith('-EQ') else sym
            log.info(f"Normalized equity symbol: {normalized}")
            return normalized
    
    log.debug(f"No normalization pattern matched for: {sym}")
    return sym


def get_lot_size_from_symbol_map(symbol: str, broker: str, exchange: str = None) -> int:
    """Get lot size from symbol map for the given symbol and broker."""
    try:
        # Ensure symbol map is loaded
        if not symbol_map.SYMBOL_MAP:
            symbol_map.SYMBOL_MAP = symbol_map.build_symbol_map()
        
        # Get the symbol mapping
        mapping = symbol_map.get_symbol_for_broker(symbol, broker, exchange)
        
        if mapping and "lot_size" in mapping:
            lot_size = mapping["lot_size"]
            if lot_size:
                try:
                    return int(float(lot_size))
                except (ValueError, TypeError) as exc:
                    log.error(
                        f"Invalid lot size '{lot_size}' for symbol {symbol}: {exc}"
                    )
        
        log.warning(f"Could not find lot size for symbol {symbol} from symbol map")
        return None
        
    except Exception as e:
        log.error(f"Error fetching lot size from symbol map for {symbol}: {e}")
        return None


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

            # REMOVED: Symbol normalization that was overriding user input
            # The webhook receiver already handles symbol normalization while preserving user choices

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


                # Detect broker names and convert symbol if needed
                child_broker = str(broker_cfg.get("name", "")).lower()
                master_broker = str(
                    event.get("master_broker")
                    or event.get("broker")
                    or child_broker
                ).lower()
                converted_symbol = convert_symbol_between_brokers(
                    event.get("symbol"), master_broker, child_broker
                )

                
                # CRITICAL FIX: Build order parameters preserving ALL user choices
                order_params = {
                    "symbol": event.get("symbol"),
                    "action": event.get("action"),
                    "qty": event.get("qty"),
                }
                order_params["symbol"] = converted_symbol
                
                # PRESERVE USER EXCHANGE CHOICE
                exchange = event.get("exchange")
                if exchange:
                    order_params["exchange"] = exchange
                    log.info(f"Using user-specified exchange: {exchange}")
                
                # PRESERVE USER PRODUCT TYPE CHOICE  
                product_type = event.get("productType") or event.get("product_type")
                if product_type:
                    order_params["product_type"] = product_type
                    log.info(f"Using user-specified product type: {product_type}")
                
                # PRESERVE USER ORDER TYPE CHOICE
                order_type = event.get("order_type") or event.get("orderType")
                if order_type:
                    order_params["order_type"] = order_type
                    log.info(f"Using user-specified order type: {order_type}")
                else:
                    order_params["order_type"] = "MARKET"  # Default only if not specified
                
                # PRESERVE USER VALIDITY CHOICE
                validity = event.get("orderValidity") or event.get("validity")
                if validity:
                    order_params["validity"] = validity
                    log.info(f"Using user-specified validity: {validity}")
                
                # Handle price for limit orders
                price = event.get("price")
                if price is not None:
                    order_params["price"] = price
                
                # Handle trigger price for stop loss orders
                trigger_price = event.get("trigger_price") or event.get("triggerPrice")
                if trigger_price is not None:
                    order_params["trigger_price"] = trigger_price
                
                # Copy F&O specific parameters
                fo_params = ["instrument_type", "expiry", "strike", "option_type", "lot_size", "security_id"]
                for param in fo_params:
                    if event.get(param) is not None:
                        order_params[param] = event[param]
                
                # Check if it's F&O and handle lot size multiplication
                is_fo = bool(re.search(r'(FUT|CE|PE)$', str(event.get("symbol", "")).upper())) or \
                        event.get("instrument_type", "").upper() in {"FUT", "FUTSTK", "FUTIDX", "OPT", "OPTSTK", "OPTIDX", "CE", "PE"}
                
                if is_fo:
                    lot_size = event.get("lot_size")
                    
                    # Try to get lot size if not provided
                    if not lot_size:
                        lot_size = get_lot_size_from_symbol_map(
                            event.get("symbol"), broker_cfg["name"], event.get("exchange")
                        )
                        if lot_size:
                            log.info(f"Found lot size {lot_size} from symbol map")
                    
                    # Fallback to default lot sizes
                    if not lot_size:
                        lot_size = get_default_lot_size(event.get("symbol"))
                        log.warning(f"Using default lot size {lot_size}")
                    
                    if lot_size:
                        try:
                            original_qty = int(event["qty"])
                            calculated_qty = original_qty * int(lot_size)
                            order_params["qty"] = calculated_qty
                            order_params["lot_size"] = lot_size
                            log.info(f"F&O quantity adjustment: {original_qty} lots × {lot_size} = {calculated_qty}")
                        except (ValueError, TypeError):
                            log.error("Invalid lot size or quantity for F&O order")
                            return {
                                "status": "failure", 
                                "error": f"Invalid lot size ({lot_size}) or quantity ({event['qty']}) for F&O order"
                            }
                    else:
                        log.error(f"Could not determine lot size for F&O symbol {event.get('symbol')}")
                        return {
                            "status": "failure", 
                            "error": f"Could not determine lot size for F&O symbol {event.get('symbol')}"
                        }
                
                log.info(f"Placing order with preserved parameters: {order_params}")
                
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
    "get_lot_size_from_symbol_map",
]


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    try:
        main()
    except KeyboardInterrupt:
        pass
