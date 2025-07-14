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

def normalize_position(position: dict, broker: str) -> dict:
    """Return a standardized position dict for the front-end."""
    b = (broker or "").lower()

    def f(key, default=0.0):
        try:
            return float(position.get(key, default))
        except (TypeError, ValueError):
            return default

    def i(key, default=0):
        try:
            return int(float(position.get(key, default)))
        except (TypeError, ValueError):
            return default

    if b == "finvasia" and any(k in position for k in ("buyqty", "sellqty", "netQty")):
        new = position.copy()
        new.setdefault("tradingSymbol", position.get("tsym"))
        new.setdefault("buyQty", i("buyqty"))
        new.setdefault("sellQty", i("sellqty"))
        new.setdefault("netQty", i("netQty"))
        new.setdefault("buyAvg", f("avgprc"))
        new.setdefault("sellAvg", f("avgprc"))
        new.setdefault("ltp", f("ltp"))
        new.setdefault("profitAndLoss", f("urmtm"))
        return new

    if b == "aliceblue" and any(k in position for k in ("buyqty", "sellqty", "netqty")):
        new = position.copy()
        new.setdefault("tradingSymbol", position.get("symbol"))
        new.setdefault("buyQty", i("buyqty"))
        new.setdefault("sellQty", i("sellqty"))
        new.setdefault("netQty", i("netqty"))
        new.setdefault("buyAvg", f("buyavgprc"))
        new.setdefault("sellAvg", f("sellavgprc"))
        new.setdefault("ltp", f("ltp"))
        new.setdefault("profitAndLoss", f("unrealisedprofit", position.get("urpl", 0)))
        return new

    if b == "zerodha" and any(k in position for k in ("quantity", "buy_quantity", "sell_quantity")):
        new = position.copy()
        net_qty = i("quantity")
        avg_price = f("average_price")
        new.setdefault("tradingSymbol", position.get("tradingsymbol"))
        new.setdefault("buyQty", i("buy_quantity"))
        new.setdefault("sellQty", i("sell_quantity"))
        new.setdefault("netQty", net_qty)
        new.setdefault("buyAvg", avg_price if net_qty > 0 else 0.0)
        new.setdefault("sellAvg", avg_price if net_qty < 0 else 0.0)
        new.setdefault("ltp", f("last_price"))
        new.setdefault("profitAndLoss", f("pnl"))
        return new

    if b == "fyers" and any(k in position for k in ("netQty", "buyQty", "sellQty")):
        new = position.copy()
        new.setdefault("tradingSymbol", position.get("symbol"))
        new.setdefault("buyQty", i("buyQty"))
        new.setdefault("sellQty", i("sellQty"))
        new.setdefault("netQty", i("netQty"))
        new.setdefault("buyAvg", f("buyAvg"))
        new.setdefault("sellAvg", f("sellAvg"))
        new.setdefault("ltp", f("ltp"))
        new.setdefault("profitAndLoss", f("pl"))
        return new

    if b == "dhan" and any(k in position for k in ("netQty", "buyQty", "sellQty")):
        new = position.copy()
        new.setdefault("tradingSymbol", position.get("tradingSymbol"))
        new.setdefault("buyQty", i("buyQty"))
        new.setdefault("sellQty", i("sellQty"))
        new.setdefault("netQty", i("netQty"))
        new.setdefault("buyAvg", f("buyAvg"))
        new.setdefault("sellAvg", f("sellAvg"))
        new.setdefault("ltp", f("lastTradedPrice", position.get("ltp", 0)))
        new.setdefault("profitAndLoss", f("unrealizedProfit"))
        return new
    # Default: return as-is
    return position
