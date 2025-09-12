from flask import session
from sqlalchemy import or_
from models import User, OrderMapping, Account, db
from datetime import datetime
import json
import logging
from services.logging import publish_log_event
from typing import List, Optional


def current_user():
    """Return the logged in ``User`` instance or ``None``."""
    user_key = session.get("user")
    if user_key is None:
        return None

    # ``session['user']`` historically stored the user email but some
    # workflows may put the user id (as int or str).  Handle both cases
    # gracefully to avoid type mismatch errors when querying.
    if isinstance(user_key, int):
        return db.session.get(User, user_key)

    # When ``user_key`` is a digit string it might actually be the user id.
    # Try id lookup first and fall back to email if no match.
    if isinstance(user_key, str) and user_key.isdigit():
        by_id = db.session.get(User, int(user_key))
        if by_id:
            return by_id

    return User.query.filter_by(email=user_key).first()


def user_account_ids(user):
    return [acc.client_id for acc in user.accounts]


def get_primary_account(user):
    """Return the first account for a user, if any."""
    if user.accounts:
        return user.accounts[0]
    return None


def order_mappings_for_user(user):
    ids = user_account_ids(user)
    return OrderMapping.query.filter(
        or_(OrderMapping.master_client_id.in_(ids),
            OrderMapping.child_client_id.in_(ids))
    )


def active_children_for_master(master, session=db.session):
    """Return active child accounts belonging to the same user as ``master``.

    Parameters
    ----------
    master: Account
        Master account for which active children should be fetched.
    session: sqlalchemy.orm.Session
        Database session to use for querying accounts. Defaults to ``db.session``.
    """
    # Get all child accounts linked to this master
    children = (
        session.query(Account)
        .filter(
            Account.role == "child",
            db.func.lower(Account.linked_master_id) == str(master.client_id).lower(),
            db.func.lower(Account.client_id) != str(master.client_id).lower(),
        )
        .all()
    )
    
    # Filter children based on copy_status and account health
    active_children = []
    for child in children:
        # Check copy status - only include children with copy_status 'On'
        copy_status = getattr(child, 'copy_status', 'Off')
        if str(copy_status).lower() != 'on':
            continue
            
        # Check for system errors that would prevent copying
        system_errors = getattr(child, 'system_errors', [])
        if system_errors:
            # Skip children with system errors
            continue
            
        # Check if child has valid credentials
        credentials = getattr(child, 'credentials', {})
        if not credentials or not credentials.get('access_token'):
            # Skip children without valid credentials
            continue
            
        active_children.append(child)
    
    return active_children


def get_child_accounts_for_master(master_id: str, db_session=db.session) -> List[Account]:
    """Get all child accounts for a specific master ID.
    
    Args:
        master_id: Client ID of the master account
        db_session: SQLAlchemy session for database queries
        
    Returns:
        List of child Account objects linked to the master
    """
    return db_session.query(Account).filter_by(
        linked_master_id=master_id,
        role="child"
    ).all()


def is_account_active_for_copying(account: Account) -> bool:
    """Check if an account is active and ready for trade copying.
    
    Args:
        account: Account object to check
        
    Returns:
        True if account is active for copying, False otherwise
    """
    # Check copy status
    copy_status = getattr(account, 'copy_status', 'Off')
    if str(copy_status).lower() != 'on':
        return False
        
    # Check for system errors
    system_errors = getattr(account, 'system_errors', [])
    if system_errors:
        return False
        
    # Check credentials
    credentials = getattr(account, 'credentials', {})
    if not credentials or not credentials.get('access_token'):
        return False
        
    return True


def update_account_copy_status(account_id: str, status: str, db_session=db.session) -> bool:
    """Update the copy status for an account.
    
    Args:
        account_id: Client ID of the account
        status: New copy status ('On' or 'Off')
        db_session: SQLAlchemy session for database operations
        
    Returns:
        True if update was successful, False otherwise
    """
    try:
        account = db_session.query(Account).filter_by(client_id=account_id).first()
        if not account:
            return False
            
        account.copy_status = status
        db_session.commit()
        return True
    except Exception:
        db_session.rollback()
        return False


def get_master_account_stats(master_id: str, db_session=db.session) -> dict:
    """Get statistics for a master account and its children.
    
    Args:
        master_id: Client ID of the master account
        db_session: SQLAlchemy session for database queries
        
    Returns:
        Dictionary with master account statistics
    """
    master = db_session.query(Account).filter_by(
        client_id=master_id, 
        role="master"
    ).first()
    
    if not master:
        return {}
        
    children = get_child_accounts_for_master(master_id, db_session)
    active_children = [child for child in children if is_account_active_for_copying(child)]
    
    return {
        "master_id": master_id,
        "master_copy_status": getattr(master, 'copy_status', 'Off'),
        "total_children": len(children),
        "active_children": len(active_children),
        "inactive_children": len(children) - len(active_children),
        "children_details": [
            {
                "client_id": child.client_id,
                "broker": child.broker,
                "copy_status": getattr(child, 'copy_status', 'Off'),
                "copy_qty": getattr(child, 'copy_qty', None),
                "copy_value_limit": getattr(child, 'copy_value_limit', None),
                "copied_value": getattr(child, 'copied_value', 0),
                "has_errors": bool(getattr(child, 'system_errors', []))
            }
            for child in children
        ]
    }


def log_connection_error(
    account: Account,
    message: str,
    *,
    disable_children: bool = False,
    module: str = "broker",
) -> None:
    """Persist a connection/order error and mark accounts inactive.

    Parameters
    ----------
    account : Account
        The account experiencing the error.
    message : str
        Error message to store.
    disable_children : bool, optional
        Whether to also disable all child accounts when the failing account is a
        master, by default ``False``.
    module : str, optional
        The source of the error for logging purposes. Defaults to ``"broker"``
        so broker failures (e.g., order placement issues) are excluded from
        system error badges. Pass ``"copy_trading"`` or ``"system"`` for
        internal failures that should surface in the UI.
    """
    logger = logging.getLogger(__name__)
    try:
        message_lower = (message or "").lower()
        if (
            (account.broker or "").lower() == "dhan"
            and (
                "invalid syntax" in message_lower
                or "failed to load broker" in message_lower
            )
        ):
            # Treat invalid Dhan broker imports as non-fatal
            logger.warning(message)
            account.status = "Connected"
            if account.copy_status == "Off":
                account.copy_status = "On"
            db.session.commit()
            publish_log_event(
                {
                    "level": "INFO",
                    "message": "Cleared previous invalid syntax logs",
                    "user_id": str(account.user_id),
                    "module": "system",
                    "details": {"client_id": account.client_id},
                }
            )
            return

        account.status = "Error"
        account.copy_status = "Off"

        if disable_children and account.role == "master":
            children = Account.query.filter_by(
                user_id=account.user_id,
                role="child",
                linked_master_id=account.client_id,
            ).all()
            for child in children:
                child.copy_status = "Off"
                child.status = "Error"

        publish_log_event(
            {
                "timestamp": datetime.utcnow().isoformat(),
                "level": "ERROR",
                "message": message,
                "user_id": str(account.user_id),
                "module": module,
                "details": {
                    "client_id": account.client_id,
                    "broker": account.broker,
                },
            }
        )
        db.session.commit()
    except Exception as e:  # pragma: no cover - logging failures shouldn't crash
        db.session.rollback()
        logger.error(f"Failed to log connection error: {e}")


def clear_init_error_logs(account: Account, *, is_master: bool) -> None:
    """Previous implementation removed initialization error logs.

    With the external log service, logs are append-only so this now
    simply records an informational event without modifying the store.
    """
    publish_log_event(
        {
            "level": "INFO",
            "message": f"clear_init_error_logs called for {account.client_id}",
            "user_id": str(account.user_id),
            "module": "system",
            "details": {"is_master": is_master},
        }
    )


def clear_connection_error_logs(account: Account) -> None:
    """Record a log indicating error logs would be cleared."""
    publish_log_event(
        {
            "level": "INFO",
            "message": f"clear_connection_error_logs called for {account.client_id}",
            "user_id": str(account.user_id),
            "module": "system",
        }
    )


def extract_product_type(position: dict) -> str | None:
    """Attempt to extract a product type from a raw position dict."""
    lower = {k.lower(): v for k, v in position.items()}
    for key in (
        "producttype",
        "product_type",
        "product",
        "pcode",
        "prd",
        "prdt",
        "prctyp",
    ):
        if key in lower and lower[key] is not None:
            prod = str(lower[key]).strip()
            if not prod:
                continue
            if key in {"pcode", "prd", "prdt"} and len(prod) == 1:
                mapping = {"c": "CNC", "m": "MIS", "h": "NRML"}
                prod = mapping.get(prod.lower(), prod)
            if key == "prctyp" and len(prod) == 1:
                mapping = {"i": "MIS", "d": "CNC"}
                prod = mapping.get(prod.lower(), prod)
            return prod.upper()
    return None


def extract_order_type(order: dict) -> str | None:
    """Attempt to extract an order type from a raw order dict."""
    lower = {k.lower(): v for k, v in order.items()}
    for key in (
        "order_type",
        "ordertype",
        "type",
        "pricetype",
        "order_type_desc",
        "order_type_str",
    ):
        if key in lower and lower[key] is not None:
            otype = str(lower[key]).strip()
            if otype:
                return otype.upper()
    return None


def canonical_product_type(product: str | None) -> str | None:
    """Return a normalized product string like ``MIS`` or ``CNC``."""
    if not product:
        return None
    prod = str(product).strip().upper()
    if prod in {"MIS", "INTRADAY", "INTRA", "I"}:
        return "MIS"
    if prod in {"CNC", "C", "LONG TERM", "LONGTERM", "LT"}:
        return "CNC"
    if prod in {"DELIVERY", "DELIVER"}:
        return "DELIVERY"
    if prod in {"NRML", "NORMAL", "MARGIN", "H"}:
        return "NRML"
    if prod in {"MTF"}:
        return "MTF"
    return prod


def map_product_for_broker(product: str | None, broker: str) -> str | None:
    """Return product string appropriate for the given broker."""
    base = canonical_product_type(product)
    if base is None:
        return None
    b = (broker or "").lower()
    if b == "dhan":
        if base == "MIS":
            return "INTRADAY"
        if base in {"CNC", "DELIVERY"}:
            return "CNC"
    return base


def normalize_position(position: dict, broker: str) -> dict | None:
    """Return a standardized position dict for the front-end.

    All keys are normalized to the style used by the copy trading page:
    ``tradingSymbol``, ``buyQty``, ``sellQty``, ``netQty``, ``buyAvg``,
    ``sellAvg``, ``ltp`` and ``profitAndLoss``.  If a position cannot be
    interpreted or represents a closed position (net quantity zero) the
    function returns ``None`` so callers can easily filter it out.
    """

    b = (broker or "").lower()
    lower = {k.lower(): v for k, v in position.items()}

    def f(keys, default=0.0):
        if isinstance(keys, str):
            keys = [keys]
        for key in keys:
            val = lower.get(key.lower())
            if val is not None:
                try:
                    return float(val)
                except (TypeError, ValueError):
                    break
        return default

    def i(keys, default=0):
        if isinstance(keys, str):
            keys = [keys]
        for key in keys:
            val = lower.get(key.lower())
            if val is not None:
                try:
                    return int(float(val))
                except (TypeError, ValueError):
                    break
        return default

    if b == "finvasia" and any(k in lower for k in ("buyqty", "sellqty", "netqty", "netQty")):
        new = {
            "tradingSymbol": lower.get("tsym"),
            "buyQty": i("buyqty"),
            "sellQty": i("sellqty"),
            "netQty": i("netqty"),
            "buyAvg": f("avgprc"),
            "sellAvg": f("avgprc"),
            "ltp": f(["ltp", "lp"]),
            "profitAndLoss": f("urmtom"),
        }
        return new if new["netQty"] != 0 else None

    if b == "aliceblue" and any(k in lower for k in ("buyqty", "sellqty", "netqty")):
        new = {
            "tradingSymbol": lower.get("symbol"),
            "buyQty": i("buyqty"),
            "sellQty": i("sellqty"),
            "netQty": i("netqty"),
            "buyAvg": f("buyavgprc"),
            "sellAvg": f("sellavgprc"),
            "ltp": f("ltp"),
            "profitAndLoss": f(["unrealisedprofit", "urpl"]),
        }
        return new if new["netQty"] != 0 else None

    if b == "zerodha" and any(k in lower for k in ("quantity", "buy_quantity", "sell_quantity")):
        net_qty = i("quantity")
        avg_price = f("average_price")
        new = {
            "tradingSymbol": lower.get("tradingsymbol"),
            "buyQty": i("buy_quantity"),
            "sellQty": i("sell_quantity"),
            "netQty": net_qty,
            "buyAvg": avg_price if net_qty > 0 else 0.0,
            "sellAvg": avg_price if net_qty < 0 else 0.0,
            "ltp": f("last_price"),
            "profitAndLoss": f("pnl"),
        }
        return new if new["netQty"] != 0 else None

    if b == "fyers" and any(k in lower for k in ("netqty", "buyqty", "sellqty")):
        new = {
            "tradingSymbol": lower.get("symbol"),
            "buyQty": i("buyqty"),
            "sellQty": i("sellqty"),
            "netQty": i("netqty"),
            "buyAvg": f("buyavg"),
            "sellAvg": f("sellavg"),
            "ltp": f("ltp"),
            "profitAndLoss": f("pl"),
        }
        return new if new["netQty"] != 0 else None

    if b == "dhan" and any(k in lower for k in ("buyqty", "sellqty", "netqty")):
        buy_qty = i("buyqty")
        sell_qty = i("sellqty")
        net_qty = i("netqty", buy_qty - sell_qty)
        new = {
            "tradingSymbol": lower.get("tradingsymbol") or lower.get("tradingSymbol"),
            "buyQty": buy_qty,
            "sellQty": sell_qty,
            "netQty": net_qty,
            "buyAvg": f("buyavg"),
            "sellAvg": f("sellavg"),
            "ltp": f(["lasttradedprice", "ltp"]),
            "profitAndLoss": f("unrealizedprofit"),
        }
        return new if new["netQty"] != 0 else None
    # Default path: attempt generic keys
    new = {
        "tradingSymbol": lower.get("tradingsymbol") or lower.get("symbol"),
        "buyQty": i(["buyqty", "buy_quantity"]),
        "sellQty": i(["sellqty", "sell_quantity"]),
        "netQty": i(["netqty", "quantity", "net_quantity"]),
        "buyAvg": f(["buyavg", "buyavgprc", "average_price"]),
        "sellAvg": f(["sellavg", "sellavgprc"]),
        "ltp": f(["ltp", "last_price"]),
        "profitAndLoss": f(["pnl", "pl", "unrealizedprofit"]),
    }
    has_qty = any(k in lower for k in [
        "netqty", "quantity", "net_quantity", "buyqty", "buy_quantity",
        "sellqty", "sell_quantity"
    ])
    if not has_qty:
        return position
    return new if new["netQty"] != 0 else None
