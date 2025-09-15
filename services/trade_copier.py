# services/trade_copier.py - FIXED VERSION
"""Trade copier service that consumes master order events from Redis.

This module listens to the ``trade_events`` Redis stream and replicates
orders from master accounts to their linked child accounts.
"""

from __future__ import annotations

import argparse
import asyncio
import time
import json
from concurrent.futures import ThreadPoolExecutor
import logging
import os
from functools import partial
from typing import Any, Callable, Dict, Iterable, Optional

from prometheus_client import Histogram
from sqlalchemy.orm import Session

from models import Account
from brokers.factory import get_broker_client
from brokers import symbol_map
import requests
from .webhook_receiver import redis_client, get_redis_client
from .db import get_session
from .utils import _decode_event
from helpers import active_children_for_master
import redis

LATENCY = Histogram(
    "trade_copier_latency_seconds", "Seconds spent processing a master order event"
)

log = logging.getLogger(__name__)

DEFAULT_MAX_WORKERS = 10
DEFAULT_CHILD_TIMEOUT = 20.0

# Track recently processed events to avoid duplicates
RECENT_EVENTS_KEY = "recent_trade_events"
RECENT_EVENTS_TTL = 300  # 5 minutes

def _load_symbol_map_or_exit() -> None:
    """Pre-load the broker symbol map, aborting on failure."""
    try:
        symbol_map.SYMBOL_MAP = symbol_map.build_symbol_map()
        log.info("Successfully loaded symbol mappings for trade copying")
    except requests.RequestException as exc:  # pragma: no cover - network errors
        log.error("failed to load instrument data: %s", exc)
        raise SystemExit(1)

def copy_order(master: Account, child: Account, order: Dict[str, Any]) -> Any:
    """Instantiate the appropriate broker client for *child* and copy *order*."""

    # Look up the concrete broker implementation or service client.
    client_cls = get_broker_client(child.broker)

    # Credentials are stored on the ``Account`` model as a JSON blob.
    credentials = dict(child.credentials or {})
    access_token = credentials.pop("access_token", "")
    credentials.pop("client_id", None)

    broker = client_cls(
        client_id=child.client_id, access_token=access_token, **credentials
    )

    # Apply fixed quantity override for the child account if provided.
    # For value-based copying, calculate quantity based on available balance
    if getattr(child, 'copy_value_limit', None) is not None:
        value_limit = float(child.copy_value_limit)
        copied_value = float(getattr(child, 'copied_value', 0) or 0)
        remaining_value = value_limit - copied_value
        
        if remaining_value <= 0:
            raise RuntimeError(f"Child {child.client_id} has exhausted copy value limit (₹{value_limit})")
        
        # Calculate quantity based on price and remaining value
        price = float(order.get("price", 0) or 0)
        if price > 0:
            calculated_qty = int(remaining_value / price)
            if calculated_qty <= 0:
                raise RuntimeError(f"Insufficient remaining value for trade: ₹{remaining_value:.2f} (need ₹{price})")
            qty = calculated_qty
            log.info(f"Value-based copy for {child.client_id}: qty={qty} (price=₹{price}, remaining=₹{remaining_value:.2f})")
        else:
            # If no price available, use original quantity but log warning
            qty = int(order.get("qty", 0))
            log.warning(f"No price available for value-based copying, using original qty={qty}")
    else:
        # Use fixed quantity override or original quantity
        qty = (
            int(child.copy_qty)
            if getattr(child, "copy_qty", None) is not None
            else int(order.get("qty", 0))
        )

    # Convert symbol between brokers before building params
    original_symbol = order.get("symbol")
    converted_symbol = symbol_map.convert_symbol_between_brokers(
        original_symbol, master.broker, child.broker
    )
    if converted_symbol != original_symbol:
        log.info(
            "Converted symbol for child %s: %s -> %s",
            child.client_id,
            original_symbol,
            converted_symbol,
        )
    else:
        log.warning(
            "Symbol conversion returned original symbol for child %s (%s -> %s): %s",
            child.client_id,
            master.broker,
            child.broker,
            original_symbol,
        )

    # CRITICAL FIX: Build parameters preserving all original order details
    params = {
        "symbol": converted_symbol,
        "action": order.get("action"),
        "qty": qty,
    }
    
    # CRITICAL: Preserve ALL parameters from original order
    preserve_params = [
        "exchange", "order_type", "product_type", "productType", 
        "validity", "orderValidity", "instrument_type", "expiry", 
        "strike", "option_type", "lot_size", "security_id", "price", 
        "trigger_price", "triggerPrice"
    ]
    
    for param in preserve_params:
        value = order.get(param)
        if value is not None:
            # Convert string values back to appropriate types for broker APIs
            if param == "price" and value:
                try:
                    params[param] = float(value)
                except (ValueError, TypeError):
                    params[param] = value
            elif param == "strike" and value:
                try:
                    params[param] = int(float(value))
                except (ValueError, TypeError):
                    params[param] = value
            elif param in ["qty", "lot_size"] and value:
                try:
                    # Don't override qty we already calculated above
                    if param != "qty":
                        params[param] = int(value)
                except (ValueError, TypeError):
                    params[param] = value
            else:
                params[param] = value
    
    # Log the parameters being used for copying
    log.info(f"Copying order to child {child.client_id} with parameters: {params}")
    
    try:
        result = broker.place_order(**params)
        
        # Update copied value for value-based copying
        if getattr(child, 'copy_value_limit', None) is not None and isinstance(result, dict) and result.get("status") == "success":
            try:
                price = float(order.get("price", 0) or 0)
                if price > 0:
                    trade_value = price * qty
                    new_copied_value = float(getattr(child, 'copied_value', 0) or 0) + trade_value
                    # TODO: Update child's copied_value in database
                    log.info(f"Updated copied value for child {child.client_id}: +₹{trade_value:.2f} (total: ₹{new_copied_value:.2f})")
            except Exception as e:
                log.warning(f"Failed to update copied value for child {child.client_id}: {e}")
        
        return result
        
    except Exception as exc:
        # Re-raise with the original broker message
        message = getattr(exc, "message", None) or (
            exc.args[0] if getattr(exc, "args", None) else str(exc)
        )
        raise RuntimeError(message) from exc

async def _replicate_to_children(
    db_session: Session,
    master: Account,
    order: Dict[str, Any],
    processor: Callable[[Account, Account, Dict[str, Any]], Any],
    *,
    executor: ThreadPoolExecutor | None = None,
    max_workers: int | None = None,
    timeout: float | None = None,
) -> None:
    """Execute ``processor`` concurrently for all active children."""

    try:
        children = active_children_for_master(master, db_session, logger=log)
    except TypeError:
        try:
            children = active_children_for_master(master, db_session)
        except TypeError:
            children = active_children_for_master(master)

    if not children:
        log.debug("No active children found for master %s", master.client_id)
        return

    log.info(
        "Replicating trade to %d active children for master %s",
        len(children),
        master.client_id,
    )
    log.info(f"Original order parameters: {order}")

    loop = asyncio.get_running_loop()
    own_executor = False
    if executor is None:
        max_workers = max_workers or len(children) or 1
        executor = ThreadPoolExecutor(max_workers=max_workers)
        own_executor = True

    def _late_completion(child, fut):
        client_id = getattr(child, "client_id", None)
        broker_name = getattr(child, "broker", None)
        try:
            result = fut.result()
            log.warning(
                "child %s (%s) copy completed after timeout: %s",
                client_id,
                broker_name,
                result,
                extra={"child": client_id, "broker": broker_name},
            )
        except asyncio.CancelledError:
            log.warning(
                "child %s (%s) copy cancelled after timeout",
                client_id,
                broker_name,
                extra={"child": client_id, "broker": broker_name},
            )
            return
        except Exception as exc:
            msg = str(exc)
            log.warning(
                "child %s (%s) copy failed after timeout: %s",
                client_id,
                broker_name,
                msg,
                exc_info=exc,
                extra={"child": client_id, "broker": broker_name, "error": msg},
            )
    
    try:
        async_tasks = []
        timed_out: list[tuple[Account, asyncio.Future]] = []
        for child in children:
            orig_fut = loop.run_in_executor(
                executor, processor, master, child, order
            )
            shielded = asyncio.shield(orig_fut)
            wrapped = asyncio.wait_for(shielded, timeout) if timeout is not None else shielded
            async_tasks.append((child, wrapped, orig_fut))

        results = await asyncio.gather(
            *(f for _c, f, _of in async_tasks), return_exceptions=True
        )

        success_count = 0
        error_count = 0
        
        for (child, _wrapped, orig_fut), result in zip(async_tasks, results):
            client_id = getattr(child, "client_id", None)
            broker_name = getattr(child, "broker", None)
            
            if isinstance(result, (asyncio.TimeoutError, TimeoutError)):
                if isinstance(result, asyncio.TimeoutError) and not orig_fut.done():
                    timed_out.append((child, orig_fut))
                log.warning(
                    "child %s copy timed out; order may still have executed",
                    client_id,
                    extra={"child": client_id, "error": "TimeoutError"},
                )
                error_count += 1
            elif isinstance(result, Exception):
                msg = str(result)
                log.error(
                    "child %s (%s) copy failed: %s",
                    client_id,
                    broker_name,
                    msg,
                    exc_info=result,
                    extra={
                        "child": client_id,
                        "broker": broker_name,
                        "error": msg,
                    },
                )
                error_count += 1
            else:
                # Successful copy
                success_count += 1
                if isinstance(result, dict) and result.get("status") == "success":
                    log.info(
                        "Successfully copied trade to child %s (%s) - Order ID: %s",
                        client_id,
                        broker_name,
                        result.get("order_id", "unknown")
                    )
                else:
                    log.warning(
                        "Trade copy to child %s (%s) may have failed: %s",
                        client_id,
                        broker_name,
                        result
                    )

        for child, fut in timed_out:
            fut.add_done_callback(partial(_late_completion, child))
            
            log.info(
                "Trade copy completed for master %s: %d success, %d errors out of %d children",
                master.client_id, success_count, error_count, len(children)
            )
    finally:
        if own_executor:
            executor.shutdown(wait=False)


def poll_and_copy_trades(
    db_session: Session,
    processor: Optional[Callable[[Account, Account, Dict[str, Any]], Any]] = None,
    *,
    stream: str = "trade_events",
    group: str = "trade_copier",
    consumer: str = "worker-1",
    redis_client=redis_client,
    max_messages: int | None = None,
    block: int = 0,
    batch_size: int = 10,
    max_workers: int | None = None,
    child_timeout: float | None = None,
) -> int:
    """Consume trade events from *stream* using a consumer group."""

    processor = processor or (lambda m, c, o: None)
    max_workers = max_workers or int(
        os.getenv("TRADE_COPIER_MAX_WORKERS", str(DEFAULT_MAX_WORKERS))
    )
    if child_timeout is None:
        env_timeout = os.getenv("TRADE_COPIER_TIMEOUT", str(DEFAULT_CHILD_TIMEOUT))
        env_timeout = env_timeout.strip().lower()
        if env_timeout in {"none", ""}:
            child_timeout = None
        else:
            try:
                parsed = float(env_timeout)
            except ValueError:
                parsed = DEFAULT_CHILD_TIMEOUT
            child_timeout = None if parsed <= 0 else parsed
    elif child_timeout <= 0:
        child_timeout = None

    executor = ThreadPoolExecutor(max_workers=max_workers)
    
    # Helper to check for duplicate events
    def is_duplicate_event(event: Dict[str, Any]) -> bool:
        try:
            # Create a unique key for this event
            # Create a unique key for this event relying on order_id
            order_id = event.get("order_id")
            if order_id is None:
                # Fallback to legacy fields if order_id is missing
                order_id = f"{event.get('symbol')}:{event.get('action')}:{event.get('qty')}"
            event_key = f"{event.get('master_id')}:{order_id}"
            
            # Check if we've seen this event recently
            if redis_client.sismember(RECENT_EVENTS_KEY, event_key):
                return True
            
            # Mark this event as seen
            redis_client.sadd(RECENT_EVENTS_KEY, event_key)
            redis_client.expire(RECENT_EVENTS_KEY, RECENT_EVENTS_TTL)
            return False
        except redis.exceptions.RedisError:
            # If Redis check fails, assume not duplicate to avoid missing trades
            log.warning("Redis duplicate check failed, proceeding with trade")
            return False
    
    try:
        # Ensure the consumer group exists. Ignore errors if already created.
        try:
            redis_client.xgroup_create(stream, group, id="0", mkstream=True)
        except Exception as exc:  # pragma: no cover - stub may not raise
            if "BUSYGROUP" not in str(exc):
                raise

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
                count = 0
                for _stream, events in messages:
                    for msg_id, data in events:
                        event = _decode_event(data)
                        
                        # Skip duplicate events
                        if is_duplicate_event(event):
                            log.debug("Skipping duplicate trade event: %s", event.get('order_id', msg_id))
                            redis_client.xack(stream, group, msg_id)
                            continue

                        async def handle(msg_id=msg_id, event=event):
                            start = time.time()
                            try:
                                # CRITICAL FIX: Force refresh database session to get latest credentials
                                db_session.rollback()
                                db_session.expire_all()
                                
                                # Re-query master account to get fresh credentials
                                master = (
                                    db_session.query(Account)
                                    .filter_by(client_id=str(event["master_id"]), role="master")
                                    .first()
                                )
                                if master:
                                    # Check if master account has copy trading enabled
                                    master_copy_status = getattr(master, 'copy_status', 'Off')
                                    if master_copy_status != 'On':
                                        log.debug(
                                            "Skipping trade copy - master %s has copy status: %s", 
                                            master.client_id, master_copy_status
                                        )
                                        return
                                    
                                    # CRITICAL FIX: Force another session refresh before processing children
                                    # This ensures we get the absolute latest credentials for child accounts
                                    db_session.rollback()
                                    db_session.expire_all()
                                    
                                    log.info(
                                        "Processing trade event from master %s: %s %s %s (source: %s)",
                                        event.get('master_id'),
                                        event.get('action'),
                                        event.get('qty'), 
                                        event.get('symbol'),
                                        event.get('source', 'webhook')
                                    )
                                    
                                    # Pass the fresh session to ensure latest credentials are used
                                    await _replicate_to_children(
                                        db_session,
                                        master,
                                        event,
                                        processor,
                                        executor=executor,
                                        timeout=child_timeout,
                                    )
                                else:
                                    log.warning("Master account not found for trade event: %s", event.get('master_id'))
                            except Exception as exc:  # pragma: no cover - exercised in tests
                                log.exception(
                                    "error processing trade event %s", msg_id, exc_info=exc
                                )
                                db_session.rollback()
                                log.info(
                                    "database session rolled back after error processing trade event %s",
                                    msg_id,
                                )
                                raise
                            else:
                                redis_client.xack(stream, group, msg_id)
                            finally:
                                LATENCY.observe(time.time() - start)

                        tasks.append((msg_id, asyncio.create_task(handle())))
                        count += 1

                if tasks:
                    results = await asyncio.gather(
                        *(t for _, t in tasks), return_exceptions=True
                    )
                    for (msg_id, _), result in zip(tasks, results):
                        if isinstance(result, Exception):
                            log.exception(
                                "error processing trade event %s", msg_id, exc_info=result
                            )
                processed += count
                if max_messages is not None and processed >= max_messages:
                    break

            return processed

        return asyncio.run(_consume())
    finally:
        executor.shutdown(wait=False)

def main() -> None:
    """Entry point for the trade copier service."""
    
    block = int(os.getenv("TRADE_COPIER_BLOCK_MS", "5000"))

    # Ensure symbol mappings are available before processing any trades.
    _load_symbol_map_or_exit()
    log.info("trade copier worker starting")

    while True:
        session = get_session()
        client = get_redis_client()
        try:
            processed = poll_and_copy_trades(
                session,
                processor=copy_order,
                redis_client=client,
                block=block,
            )
            if processed > 0:
                log.info("processed %d trade event(s)", processed)
        except redis.exceptions.RedisError:
            log.exception("redis error while copying trades")
            time.sleep(5)  # Brief pause before retrying
        except Exception as e:
            log.exception("unexpected error in trade copier main loop")
            time.sleep(10)  # Longer pause for unexpected errors
        finally:
            session.close()


__all__ = ["poll_and_copy_trades", "copy_order", "LATENCY", "main"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the trade copier worker")
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="enable debug logging",
    )
    args = parser.parse_args()
    level = "DEBUG" if args.verbose else os.getenv("LOG_LEVEL", "INFO")
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    try:
        main()
    except KeyboardInterrupt:
        log.info("Trade copier service stopped by user")
        pass
