from flask import session
from sqlalchemy import or_, cast
from models import User, OrderMapping, Account, SystemLog, db
from datetime import datetime
import json
import logging


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


def active_children_for_master(master):
    """Return active child accounts belonging to the same user as ``master``."""
    return Account.query.filter(
        Account.user_id == master.user_id,
        Account.role == 'child',
        Account.linked_master_id == master.client_id,
        db.func.lower(Account.copy_status) == 'on'
    ).all()

def log_connection_error(account: Account, message: str, *, disable_children: bool = False) -> None:
    """Persist a connection/order error and mark accounts inactive."""
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
            # Remove related error logs
            try:
                logs = (
                    SystemLog.query.filter(
                        SystemLog.user_id == account.user_id,
                        SystemLog.level == "ERROR",
                        or_(
                            SystemLog.message.ilike("%invalid syntax%"),
                            SystemLog.message.ilike("%failed to load broker%"),
                            SystemLog.message.ilike("%Failed to initialize master API%"),
                        ),
                    ).all()
                )
                for log in logs:
                    details = log.details
                    if isinstance(details, str):
                        try:
                            details = json.loads(details)
                        except Exception:
                            details = {}
                    if (
                        isinstance(details, dict)
                        and str(details.get("client_id")) == account.client_id
                    ):
                        db.session.delete(log)
                db.session.commit()
            except Exception:
                db.session.rollback()
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

        log_entry = SystemLog(
            timestamp=datetime.utcnow().isoformat(),
            level="ERROR",
            message=message,
            user_id=str(account.user_id),
            module="copy_trading",
            details=json.dumps({
                "client_id": account.client_id,
                "broker": account.broker,
            })
        )
        db.session.add(log_entry)
        db.session.commit()
    except Exception as e:  # pragma: no cover - logging failures shouldn't crash
        db.session.rollback()
        logger.error(f"Failed to log connection error: {e}")


def clear_init_error_logs(account: Account, *, is_master: bool) -> None:
    """Remove previous initialization error logs for an account."""
    logger = logging.getLogger(__name__)
    prefix = (
        "Failed to initialize master API"
        if is_master
        else "Failed to initialize child API"
    )
    try:
        logs = (
            SystemLog.query.filter(
                cast(SystemLog.user_id, db.String) == str(account.user_id),
                SystemLog.level == "ERROR",
                SystemLog.message.ilike(f"{prefix}%"),
            ).all()
        )
        for log in logs:
            details = log.details
            if isinstance(details, str):
                try:
                    details = json.loads(details)
                except Exception:
                    details = {}
            if isinstance(details, dict) and str(details.get("client_id")) == account.client_id:
                db.session.delete(log)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.error(
            f"Failed to clear init error logs for {account.client_id}: {e}"
        )


def clear_connection_error_logs(account: Account) -> None:
    """Remove previous connection/order error logs for an account."""
    logger = logging.getLogger(__name__)
    try:
        pattern_plain = f'%"client_id": "{account.client_id}"%'
        pattern_escaped = f'%\\"client_id\\": \\"{account.client_id}\\"%'
        client_match = SystemLog.details["client_id"].as_string() == str(account.client_id)
        (
            SystemLog.query.filter(
                cast(SystemLog.user_id, db.String) == str(account.user_id),
                SystemLog.level == "ERROR",
                db.or_(
                    client_match,
                    cast(SystemLog.details, db.Text).ilike(pattern_plain),
                    cast(SystemLog.details, db.Text).ilike(pattern_escaped),
                ),
            ).delete(synchronize_session=False)
        )
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.error(
            f"Failed to clear connection error logs for {account.client_id}: {e}"
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
            return "DELIVERY"
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
            "profitAndLoss": f("urmtm"),
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
