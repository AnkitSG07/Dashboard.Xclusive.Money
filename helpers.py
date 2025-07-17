
from flask import session
from sqlalchemy import or_
from models import User, OrderMapping, Account, db


def current_user():
    """Return the logged in ``User`` instance or ``None``."""
    email = session.get("user")
    if not email:
        return None
    return User.query.filter_by(email=email).first()


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
