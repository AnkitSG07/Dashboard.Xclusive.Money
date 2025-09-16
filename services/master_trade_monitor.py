# services/master_trade_monitor.py - FIXED VERSION
from __future__ import annotations

"""Monitor master broker accounts for manually placed trades - OPTIMIZED FOR SPEED.

This service polls master accounts every 2 seconds and immediately publishes
trade events for instant copying to child accounts.
"""

import asyncio
import redis
import logging
import os
import time
from collections import defaultdict
from typing import Any, Dict, Iterable, Set

from brokers.factory import get_broker_client
from models import Account
from sqlalchemy.orm import Session

from .webhook_receiver import get_redis_client
from .db import get_session

log = logging.getLogger(__name__)

# Order statuses that indicate a completed or filled order
COMPLETED_STATUSES = {
    "COMPLETE", "COMPLETED", "FILLED", "EXECUTED", "TRADED",
    "FULL_EXECUTED", "FULLY_EXECUTED", "2", "CONFIRMED",
    "SUCCESS", "OK", "FULLY_FILLED", "PARTIAL_FILLED",
    "COMPLETE", "AMO REQ RECEIVED"  # Added AMO status
}

# Keep track of processed orders to avoid duplicates
PROCESSED_ORDERS_KEY = "processed_manual_orders:{master_id}"
PROCESSED_ORDERS_TTL = 86400 * 7  # 7 days

# Cache broker clients to avoid recreation
BROKER_CLIENT_CACHE = {}
BROKER_CLIENT_CACHE_TTL = 300  # 5 minutes

def get_cached_broker_client(master: Account):
    """Get or create cached broker client for faster access."""
    cache_key = f"{master.broker}:{master.client_id}"
    cached = BROKER_CLIENT_CACHE.get(cache_key)
    
    if cached and cached['expires'] > time.time():
        return cached['client']
    
    credentials = dict(master.credentials or {})
    access_token = credentials.pop("access_token", "")
    credentials.pop("client_id", None)
    
    try:
        client_cls = get_broker_client(master.broker)
        client = client_cls(
            master.client_id, 
            access_token=access_token, 
            **credentials
        )
        
        BROKER_CLIENT_CACHE[cache_key] = {
            'client': client,
            'expires': time.time() + BROKER_CLIENT_CACHE_TTL
        }
        return client
    except Exception as e:
        log.error(f"Failed to create broker client for {master.client_id}: {e}")
        return None

def extract_order_details(order: Dict, broker: str) -> Dict[str, Any]:
    """Enhanced order detail extraction with better fallbacks."""
    
    broker_lower = broker.lower()
    
    # Extract order ID with comprehensive fallbacks
    order_id = (
        order.get("id") or
        order.get("order_id") or
        order.get("orderId") or
        order.get("orderID") or
        order.get("norenordno") or  # Finvasia
        order.get("NOrdNo") or  # AliceBlue
        order.get("nestOrderNumber") or  # AliceBlue
        order.get("orderNumber") or  # Fyers
        order.get("orderid") or  # Flattrade
        order.get("order_no") or
        order.get("orderNo") or
        order.get("exchordid") or  # Exchange order ID
        order.get("ExchOrdID")
    )
    
    # Extract symbol with broker-specific logic
    symbol = None
    if broker_lower == "dhan":
        symbol = (
            order.get("tradingSymbol") or 
            order.get("tradingsymbol") or
            order.get("trading_symbol") or
            order.get("symbol")
        )
        # Dhan may provide security_id without symbol
        if not symbol and order.get("securityId"):
            # You might need to map security_id to symbol
            log.warning(f"Dhan order missing symbol, has securityId: {order.get('securityId')}")
            
    elif broker_lower == "zerodha":
        symbol = (
            order.get("tradingsymbol") or
            order.get("tradingSymbol") or
            order.get("symbol")
        )
        
    elif broker_lower == "fyers":
        symbol = order.get("symbol") or order.get("tradingSymbol")
        # Fyers format: NSE:SBIN-EQ, extract just the symbol
        if symbol and ":" in symbol:
            parts = symbol.split(":")
            if len(parts) == 2:
                symbol = parts[1]  # Take part after colon
                
    elif broker_lower in ["aliceblue", "finvasia"]:
        symbol = (
            order.get("symbol") or
            order.get("tsym") or
            order.get("trading_symbol") or
            order.get("tradingSymbol")
        )
        
    else:
        # Generic extraction
        symbol = (
            order.get("symbol") or
            order.get("tradingSymbol") or
            order.get("tradingsymbol") or
            order.get("trading_symbol") or
            order.get("tsym")
        )
    
    # Extract action/transaction type
    action = (
        order.get("action") or
        order.get("transactionType") or
        order.get("transaction_type") or
        order.get("trantype") or
        order.get("side") or
        order.get("buysell") or
        order.get("buyOrSell") or
        order.get("buy_or_sell")
    )
    
    # Normalize action
    if action:
        action_upper = str(action).upper()
        if action_upper in ["B", "BUY", "1"]:
            action = "BUY"
        elif action_upper in ["S", "SELL", "-1", "2"]:
            action = "SELL"
    
    # Extract quantity with fallbacks
    qty = (
        order.get("qty") or
        order.get("quantity") or
        order.get("orderQty") or
        order.get("filled_qty") or
        order.get("filledQty") or
        order.get("fillshares") or
        order.get("Fillshares") or
        order.get("executed_qty") or
        order.get("executedQty") or
        order.get("tradedQty") or
        order.get("traded_qty")
    )
    
    # Try to convert qty to int
    if qty is not None:
        try:
            qty = int(float(qty))
        except (ValueError, TypeError):
            qty = None
    
    # Extract exchange
    exchange = (
        order.get("exchange") or
        order.get("exchangeSegment") or
        order.get("exch") or
        order.get("Exchange") or
        order.get("segment")
    )
    
    # Normalize exchange
    if exchange:
        exchange = str(exchange).upper()
        # Map segment codes to standard exchange names
        exchange_map = {
            "NSE_EQ": "NSE",
            "BSE_EQ": "BSE", 
            "NSE_FNO": "NFO",
            "BSE_FNO": "BFO",
            "NSE_FO": "NFO",
            "BSE_FO": "BFO"
        }
        exchange = exchange_map.get(exchange, exchange)
    
    # Extract order type
    order_type = (
        order.get("order_type") or
        order.get("orderType") or
        order.get("prctyp") or
        order.get("priceType") or
        order.get("type")
    )
    
    # Extract product type
    product_type = (
        order.get("product") or
        order.get("productType") or
        order.get("product_type") or
        order.get("pCode") or
        order.get("prd")
    )
    
    # Extract price
    price = (
        order.get("price") or
        order.get("avg_price") or
        order.get("average_price") or
        order.get("avgPrice") or
        order.get("limitPrice") or
        order.get("limit_price")
    )
    
    return {
        "order_id": order_id,
        "symbol": symbol,
        "action": action,
        "qty": qty,
        "exchange": exchange,
        "order_type": order_type,
        "product_type": product_type,
        "price": price,
        "status": order.get("status") or order.get("orderStatus"),
        "order_time": order.get("order_time") or order.get("orderTime") or order.get("order_timestamp")
    }

def monitor_master_trades(
    db_session: Session,
    *,
    redis_client=None,
    poll_interval: float | None = None,
    max_iterations: int | None = None,
) -> None:
    """Poll master accounts for new orders with MINIMAL DELAY.

    Parameters:
        db_session: SQLAlchemy session
        redis_client: Redis client for publishing events
        poll_interval: Seconds between polls (default 2 seconds for speed)
        max_iterations: Max polling cycles (None = infinite)
    """

    if poll_interval is None:
        # REDUCED FROM 10 TO 2 SECONDS FOR FASTER DETECTION
        poll_interval = float(os.getenv("ORDER_MONITOR_INTERVAL", "2"))

    if redis_client is None:
        redis_client = get_redis_client()

    async def _monitor() -> None:
        iterations = 0
        seen_order_ids: Dict[str, Set[str]] = defaultdict(set)
        invalid_creds: Dict[str, str] = {}
        
        def load_processed_orders(master_id: str) -> Set[str]:
            try:
                key = PROCESSED_ORDERS_KEY.format(master_id=master_id)
                processed = redis_client.smembers(key)
                return {p.decode() if isinstance(p, bytes) else str(p) for p in processed}
            except Exception:
                return set()
        
        def mark_order_processed(master_id: str, order_id: str):
            try:
                key = PROCESSED_ORDERS_KEY.format(master_id=master_id)
                redis_client.sadd(key, order_id)
                redis_client.expire(key, PROCESSED_ORDERS_TTL)
            except Exception:
                pass

        while max_iterations is None or iterations < max_iterations:
            start_time = time.time()
            
            # Refresh database session for latest data
            db_session.rollback()
            db_session.expire_all()
            
            masters: Iterable[Account] = (
                db_session.query(Account)
                .filter_by(role="master", copy_status="On")  # Only active masters
                .all()
            )
            
            # Process all masters in parallel for speed
            tasks = []
            
            for master in masters:
                # Skip if invalid credentials
                access_token = (master.credentials or {}).get("access_token", "")
                cached_token = invalid_creds.get(master.client_id)
                if cached_token and cached_token == access_token:
                    continue

                # Get or create broker client (cached for speed)
                client = get_cached_broker_client(master)
                if not client:
                    continue

                # Create async task for this master
                async def process_master(master=master, client=client):
                    try:
                        # Fetch orders with timeout
                        orders = await asyncio.wait_for(
                            asyncio.to_thread(client.list_orders),
                            timeout=3.0  # 3 second timeout per broker
                        )
                        
                        # Load previously processed orders
                        if master.client_id not in seen_order_ids:
                            seen_order_ids[master.client_id] = load_processed_orders(master.client_id)
                        
                        seen = seen_order_ids[master.client_id]
                        new_orders = []
                        
                        for order in orders:
                            # Extract order details with enhanced logic
                            details = extract_order_details(order, master.broker)
                            
                            order_id = details["order_id"]
                            status = str(details.get("status") or "").upper()
                            
                            # Skip if not completed or already processed
                            if status and status not in COMPLETED_STATUSES:
                                continue
                            if order_id and str(order_id) in seen:
                                continue
                            
                            # Validate essential fields
                            if not details["symbol"] or not details["action"] or not details["qty"]:
                                log.warning(
                                    f"Skipping order with missing data from {master.client_id}: "
                                    f"symbol={details['symbol']}, action={details['action']}, "
                                    f"qty={details['qty']}, order_id={order_id}"
                                )
                                continue
                            
                            # Build event for publishing
                            event = {
                                "master_id": master.client_id,
                                "symbol": details["symbol"],
                                "action": details["action"],
                                "qty": details["qty"],
                                "source": "manual_trade_monitor"
                            }
                            
                            # Add optional fields
                            for field in ["exchange", "order_type", "product_type", "price", "order_time"]:
                                if details.get(field):
                                    event[field] = details[field]
                            
                            # Add order_id for tracking
                            if order_id:
                                event["order_id"] = str(order_id)
                            
                            new_orders.append((order_id, event))
                        
                        # Publish new orders immediately
                        for order_id, event in new_orders:
                            try:
                                # Filter None values
                                event = {k: str(v) for k, v in event.items() if v is not None}
                                
                                # PUBLISH IMMEDIATELY FOR INSTANT COPYING
                                redis_client.xadd("trade_events", event)
                                
                                log.info(
                                    f"Published trade from {master.client_id} ({master.broker}): "
                                    f"{event.get('action')} {event.get('qty')} {event.get('symbol')}"
                                )
                                
                                # Mark as processed
                                if order_id:
                                    seen.add(str(order_id))
                                    mark_order_processed(master.client_id, str(order_id))
                                    
                            except Exception as e:
                                log.error(f"Failed to publish trade event: {e}")
                        
                        if new_orders:
                            log.info(f"Found {len(new_orders)} new orders from master {master.client_id}")
                        
                        # Clear invalid creds on success
                        invalid_creds.pop(master.client_id, None)
                        
                    except asyncio.TimeoutError:
                        log.warning(f"Timeout fetching orders from {master.client_id}")
                    except Exception as e:
                        msg = str(e).lower()
                        if any(x in msg for x in ["invalid", "token", "expired", "unauthorized"]):
                            invalid_creds[master.client_id] = access_token
                            log.error(f"Invalid credentials for {master.client_id}: {e}")
                        else:
                            log.error(f"Error fetching orders from {master.client_id}: {e}")
                
                tasks.append(process_master())
            
            # Process all masters concurrently
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            
            # Calculate time taken and sleep if needed
            elapsed = time.time() - start_time
            if elapsed < poll_interval:
                await asyncio.sleep(poll_interval - elapsed)
            
            iterations += 1
            if max_iterations is not None and iterations >= max_iterations:
                break

    asyncio.run(_monitor())


def main() -> None:
    """Run the optimized master trade monitor service."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    log.info("Starting FAST master trade monitor service (2 second polling)...")
    
    session = get_session()
    try:
        monitor_master_trades(session)
    finally:
        session.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Master trade monitor service stopped by user")
        pass


__all__ = ["monitor_master_trades", "main"]
