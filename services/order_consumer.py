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
    """Determine the correct expiry year for a given month and day.
    
    Args:
        month: Three-letter month code (JAN, FEB, etc.)
        day: Optional day of month for more accurate year determination
        
    Returns:
        Two-digit year string
    """
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


def normalize_symbol_to_dhan_format(symbol: str) -> str:
    """Convert various symbol formats to Dhan's expected format.
    
    Examples:
    - NIFTYNXT50SEPFUT -> NIFTYNXT50-Sep2025-FUT
    - FINNIFTY25SEP33300CE -> FINNIFTY-Sep2025-33300-CE
    - BANKNIFTY25SEP55000PE -> BANKNIFTY-Sep2025-55000-PE
    """
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
        # Only treat as year if it's in reasonable range (24-30)
        year_num = int(year)
        if 24 <= year_num <= 30:
            full_year = f"20{year}"
            normalized = f"{root}-{month.title()}{full_year}-FUT"
            log.info(f"Normalized futures with year from '{sym}' to '{normalized}' (root: {root}, year: {year})")
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
        log.info(f"Normalized futures without year from '{sym}' to '{normalized}' (root: {root}, calculated year: {year})")
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
            log.info(f"Normalized options with year from '{sym}' to '{normalized}' (root: {root}, year: {year})")
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
        log.info(f"Normalized options without year from '{sym}' to '{normalized}' (root: {root}, calculated year: {year})")
        return normalized
    
    # Pattern 5: Spaced format "NIFTYNXT50 SEP FUT" or "FINNIFTY 25 SEP 33300 CE"
    spaced_fut = re.match(
        r'^([A-Z0-9]+)\s+(?:(\d{2})\s+)?(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+FUT$',
        sym
    )
    if spaced_fut:
        root, year, month = spaced_fut.groups()
        if not year:
            year = get_expiry_year(month)
        elif int(year) < 24 or int(year) > 30:
            # If year is not in reasonable range, treat as part of root
            root = f"{root}{year}"
            year = get_expiry_year(month)
        full_year = f"20{year}"
        normalized = f"{root}-{month.title()}{full_year}-FUT"
        log.info(f"Normalized spaced futures from '{sym}' to '{normalized}'")
        return normalized
    
    spaced_opt = re.match(
        r'^([A-Z0-9]+)\s+(?:(\d{2})\s+)?(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+(\d+)\s+(CE|PE)$',
        sym
    )
    if spaced_opt:
        root, year, month, strike, opt_type = spaced_opt.groups()
        if not year:
            year = get_expiry_year(month)
        elif int(year) < 24 or int(year) > 30:
            root = f"{root}{year}"
            year = get_expiry_year(month)
        full_year = f"20{year}"
        normalized = f"{root}-{month.title()}{full_year}-{strike}-{opt_type}"
        log.info(f"Normalized spaced options from '{sym}' to '{normalized}'")
        return normalized
    
    # Pattern 6: Handle equity symbols without -EQ suffix
    if not re.search(r'(FUT|CE|PE)$', sym) and not sym.endswith('-EQ'):
        if not re.search(r'\d', sym) or re.match(r'^[A-Z]+\d+$', sym):  # Pure alphabetic or name with numbers
            normalized = f"{sym}-EQ" if not sym.endswith('-EQ') else sym
            log.info(f"Normalized equity symbol: {normalized}")
            return normalized
    
    # If no pattern matches, return original
    log.debug(f"No normalization pattern matched for: {sym}")
    return sym


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
    """Consume events from *stream* using a consumer group and place orders.

    Args:
        stream: Redis Stream name to consume from.
        group: Consumer group name used for coordinated consumption.
        consumer: Consumer name within the group.
        redis_client: Redis client instance.  Tests may supply a stub.
        max_messages: Optional limit for the number of messages processed.  If
            ``None`` the consumer runs until the stream is exhausted.
        block: Milliseconds to block waiting for new events.  ``0`` means do not
            block.
        batch_size: Number of messages to fetch per ``xreadgroup`` call.
        max_workers: Maximum number of broker submissions dispatched
            concurrently for a single webhook event.  Defaults to the value of
            the ``ORDER_CONSUMER_MAX_WORKERS`` environment variable.
        order_timeout: Maximum time in seconds to wait for a broker API
            response. ``None`` disables the timeout.

    Returns the number of messages processed.
    """
    
    max_workers = max_workers or DEFAULT_MAX_WORKERS
    if order_timeout is None:
        # Wait slightly longer than broker HTTP calls so the worker doesn't
        # cancel orders prematurely.  Defaults to the broker timeout (25s) but
        # can be overridden via ``ORDER_CONSUMER_TIMEOUT``.
        order_timeout = float(
            os.getenv(
                "ORDER_CONSUMER_TIMEOUT",
                os.getenv("BROKER_TIMEOUT", "20"),
            )
        )

    # Lazily create a Redis client if one was not supplied.
    if redis_client is None:
        redis_client = get_redis_client()

    # Ensure the consumer group exists. Ignore error if it already exists.
    try:
        redis_client.xgroup_create(stream, group, id="0", mkstream=True)
    except Exception as exc:  # pragma: no cover - stub may not raise
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

            allowed_accounts = event.get("masterAccounts") or []
            if isinstance(allowed_accounts, str):
                try:
                    allowed_accounts = json.loads(allowed_accounts)
                except json.JSONDecodeError:
                    log.error(
                        "invalid masterAccounts JSON: %s",
                        allowed_accounts,
                        extra={"event": event},
                    )
                    orders_failed.inc()
                    return
                if not isinstance(allowed_accounts, list):
                    log.error(
                        "masterAccounts JSON was not a list: %s",
                        allowed_accounts,
                        extra={"event": event},
                    )
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
                invalid_ids = [
                    acc_id for acc_id in allowed_accounts if not str(acc_id).isdigit()
                ]
                if invalid_ids:
                    log.error(
                        "non-numeric master account id(s): %s",
                        invalid_ids,
                        extra={"event": event},
                    )
                    orders_failed.inc()
                    return

                ids: List[int] = [int(acc_id) for acc_id in allowed_accounts]
                session = get_session()
                try:
                    rows = (
                        session.query(Account)
                        .filter(Account.id.in_(ids))
                        .all()
                    )
                finally:
                    session.close()
                allowed_pairs = {(r.broker, r.client_id) for r in rows}
                brokers = [
                    b
                    for b in brokers
                    if (b.get("name"), b.get("client_id")) in allowed_pairs
                ]

                if not brokers:
                    log.error(
                        "no brokers permitted for user", extra={"event": event}
                    )
                    orders_failed.inc()
                    return
            elif not brokers:
                log.error(
                    "no brokers configured for user", extra={"event": event}
                )
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
                # Validate that all required credentials are present before
                # instantiating the broker client.  This avoids cryptic
                # ``TypeError`` exceptions when mandatory parameters like
                # ``api_key`` are missing from the configuration.
                try:
                    sig = inspect.signature(client_cls)
                except (TypeError, ValueError):
                    sig = None
                if sig is not None:
                    required = [
                        p.name
                        for p in sig.parameters.values()
                        if p.kind
                        in (
                            inspect.Parameter.POSITIONAL_OR_KEYWORD,
                            inspect.Parameter.KEYWORD_ONLY,
                        )
                        and p.default is inspect._empty
                        and p.name not in ("self", "client_id", "access_token")
                    ]
                    missing = [p for p in required if p not in credentials]
                    if missing:
                        raise ValueError(
                            "missing required broker credential(s): "
                            + ", ".join(missing)
                        )

                client = client_cls(
                    client_id=client_id,
                    access_token=access_token,
                    **credentials,
                )
                order_params = {
                    "symbol": event["symbol"],
                    "action": event["action"],
                    "qty": event["qty"],
                }
                instrument_type = event.get("instrument_type")
                symbol = str(event.get("symbol", ""))
                inst_upper = instrument_type.upper() if instrument_type else ""
                sym_upper = symbol.upper()
                is_derivative = bool(re.search(r"(FUT|CE|PE)$", inst_upper)) or bool(
                    re.search(r"(FUT|CE|PE)$", sym_upper)
                )
                exchange = event.get("exchange")
                if event.get("security_id") is not None:
                    order_params["security_id"] = event["security_id"]
                if exchange is not None:
                    if exchange in {"NSE", "BSE"} and is_derivative:
                        exchange = {"NSE": "NFO", "BSE": "BFO"}[exchange]
                    order_params["exchange"] = exchange
                if event.get("order_type") is not None:
                    order_params["order_type"] = event["order_type"]
                for field in [
                    "instrument_type",
                    "expiry",
                    "strike",
                    "option_type",
                    "lot_size",
                ]:
                    if event.get(field) is not None:
                        order_params[field] = event[field]
                optional_map = {
                    "productType": "product_type",
                    "orderValidity": "validity",
                    "masterAccounts": "master_accounts",
                    "securityId": "security_id",
                }
                for src, dest in optional_map.items():
                    if event.get(src) is not None:
                        order_params[dest] = event[src]
                
                # Handle lot size for derivatives with improved symbol mapping
                lot_size = None
                if is_derivative:
                    # First, try to get lot_size from the original event payload
                    lot_size = event.get("lot_size")
                    
                    # If not available, try to get it from the symbol map using normalized symbol
                    if lot_size is None:
                        try:
                            # Use the normalized symbol for better lookup success
                            normalized_symbol = event.get("symbol", "")
                            log.info(f"Looking up lot size for normalized symbol: {normalized_symbol}")
                            
                            mapping = symbol_map.get_symbol_for_broker(
                                normalized_symbol, broker_cfg["name"], exchange
                            )
                            lot_size = mapping.get("lot_size") or mapping.get("lotSize")
                            
                            if lot_size:
                                log.info(f"Found lot size {lot_size} for {normalized_symbol} from symbol map")
                            else:
                                # Log debug info for troubleshooting
                                log.debug(f"Symbol map lookup returned: {mapping}")
                                
                                # Try to debug what symbols are available
                                debug_info = symbol_map.debug_symbol_lookup(
                                    normalized_symbol, 
                                    broker_cfg["name"], 
                                    exchange
                                )
                                log.info(f"Symbol debug info: {json.dumps(debug_info, indent=2)}")
                                
                        except Exception as e:
                            log.warning(
                                "Could not retrieve lot size from symbol map for %s: %s",
                                event.get("symbol"),
                                str(e),
                                extra={"event": event, "broker": broker_cfg}
                            )

                    # Enhanced fallback lot sizes with more symbols
                    if not lot_size and is_derivative:
                        symbol_upper = symbol.upper()
                        if "FINNIFTY" in symbol_upper:
                            lot_size = 40
                            log.info(f"Using default FINNIFTY lot size: {lot_size}")
                        elif "NIFTYNXT" in symbol_upper or "NIFTY NEXT" in symbol_upper:
                            lot_size = 50  # NIFTY Next 50 lot size
                            log.info(f"Using default NIFTY Next 50 lot size: {lot_size}")
                        elif "NIFTY" in symbol_upper and "BANK" not in symbol_upper:
                            lot_size = 50
                            log.info(f"Using default NIFTY lot size: {lot_size}")
                        elif "BANKNIFTY" in symbol_upper:
                            lot_size = 25
                            log.info(f"Using default BANKNIFTY lot size: {lot_size}")
                        elif "SENSEX" in symbol_upper:
                            lot_size = 10
                            log.info(f"Using default SENSEX lot size: {lot_size}")
                        elif "BANKEX" in symbol_upper:
                            lot_size = 15
                            log.info(f"Using default BANKEX lot size: {lot_size}")
                        else:
                            # For individual stock derivatives, default to 1 lot = qty
                            lot_size = 1
                            log.warning(f"Using fallback lot size 1 for unknown derivative: {symbol}")
                    
                    if lot_size:
                        try:
                            # Multiply quantity by lot size for derivatives
                            original_qty = int(event["qty"])
                            calculated_qty = original_qty * int(lot_size)
                            order_params["qty"] = calculated_qty
                            if "lot_size" not in order_params:
                                order_params["lot_size"] = lot_size
                            log.info(f"Adjusted quantity for {symbol}: {original_qty} lots * {lot_size} = {calculated_qty}")
                        except (ValueError, TypeError):
                            log.error(
                                "Invalid lot size or quantity. Could not calculate final order quantity. "
                                "lot_size: %s, event_qty: %s",
                                lot_size,
                                event.get("qty"),
                                extra={"event": event, "broker": broker_cfg}
                            )
                            return None
                    else:
                        log.error(
                            "Unable to determine lot size for %s (%s) on broker %s. "
                            "Cannot proceed with order.",
                            event.get("symbol"),
                            exchange,
                            broker_cfg["name"],
                            extra={"event": event, "broker": broker_cfg},
                        )
                        return None
                
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
                    log.warning(
                        "failed to fetch order status",
                        extra={"order_id": order_id},
                        exc_info=True,
                    )
                status_upper = str(status).upper() if status is not None else None
                if status_upper in REJECTED_STATUSES:
                    log.info(
                        "skipping trade event due to rejected status",
                        extra={"order_id": order_id, "status": status},
                    )
                    return None
                if status_upper not in COMPLETED_STATUSES:
                    log.info(
                        "publishing trade event with incomplete status",
                        extra={"order_id": order_id, "status": status},
                    )
                    
                trade_event = {
                    "master_id": client_id,
                    **{k: v for k, v in order_params.items() if k != "master_accounts"},
                }
                return trade_event


            def _late_completion(cfg, fut):
                try:
                    result = fut.result()
                    log.warning(
                        "broker %s completed after timeout: %s",
                        cfg["name"],
                        result,
                    )
                except BaseException as exc:
                    log.warning(
                        "broker %s failed after timeout: %s",
                        cfg["name"],
                        exc,
                        exc_info=exc,
                    )

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
                        log.exception(
                            "failed to place master order", extra={"event": event, "broker": cfg}
                        )
                for future in pending:
                    cfg = futures[future]
                    orders_failed.inc()
                    log.warning(
                        "broker order timed out", extra={"event": event, "broker": cfg}
                    )
                    if not future.cancel():
                        timed_out.append((future, cfg))
                for fut, cfg in timed_out:
                    fut.add_done_callback(partial(_late_completion, cfg))
            else:
                # Provide explicit feedback when no brokers are configured for a user.
                orders_failed.inc()
                log.warning("no brokers configured for user", extra={"event": event})
                
            for trade_event in trade_events:
                # Publish master order to trade copier stream so child
                # accounts can replicate the trade asynchronously.
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


__all__ = [
    "consume_webhook_events",
    "orders_success",
    "orders_failed",
    "normalize_symbol_to_dhan_format",
]

def main() -> None:
    """Run the consumer indefinitely.

    ``consume_webhook_events`` exits once the Redis stream is exhausted.
    Wrapping it in an endless loop ensures the worker process stays alive
    and continues to block for new webhook events.
    """

    while True:
        # Block for up to 5 seconds waiting for new events so the loop
        # doesn't spin when the stream is idle.
        try:
            consume_webhook_events(block=5000)
        except redis.exceptions.RedisError:
            log.exception("redis unavailable, retrying", exc_info=True)
            time.sleep(5)


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    try:
        main()
    except KeyboardInterrupt:  # pragma: no cover - interactive use
        pass
