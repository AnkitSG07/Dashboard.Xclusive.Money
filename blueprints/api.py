from flask import Blueprint, jsonify, session, request
from helpers import (
    current_user,
    get_primary_account,
    order_mappings_for_user,
    normalize_position,
)
from models import Account, db, Strategy
from dhanhq import dhanhq
from functools import wraps
from datetime import datetime

api_bp = Blueprint("api", __name__, url_prefix="/api")


def login_required_api(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("user"):
            return jsonify({"error": "Unauthorized"}), 401
        return fn(*args, **kwargs)
        
    return wrapper


@api_bp.route("/portfolio", defaults={"client_id": None})
@api_bp.route("/portfolio/<client_id>")
@login_required_api
def portfolio(client_id=None):
    """Return positions for the requested account."""
    user = current_user()
    if client_id:
        account = Account.query.filter_by(user_id=user.id, client_id=client_id).first()
    else:
        account = get_primary_account(user)
    if not account or not account.credentials:
        return jsonify({"error": "Account not configured"}), 400

    from app import broker_api, _account_to_dict
    api = broker_api(_account_to_dict(account))
    try:
        resp = api.get_positions()
        if isinstance(resp, dict):
            data = (
                resp.get("data")
                or resp.get("positions")
                or resp.get("net")
                or resp.get("netPositions")
                or resp.get("net_positions")
                or []
            )
        else:
            data = resp or []

        if isinstance(data, dict):
            data = (
                data.get("data")
                or data.get("positions")
                or data.get("net")
                or data.get("netPositions")
                or data.get("net_positions")
                or []
            )

        if not isinstance(data, list):
            data = []

        standardized = [normalize_position(p, account.broker) for p in data]
        standardized = [p for p in standardized if p]
        return jsonify(standardized)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api_bp.route("/orders", defaults={"client_id": None})
@api_bp.route("/orders/<client_id>")
@login_required_api
def orders(client_id=None):
    """Return order list for the requested account."""
    user = current_user()
    if client_id:
        account = Account.query.filter_by(user_id=user.id, client_id=client_id).first()
    else:
        account = get_primary_account(user)
    if not account or not account.credentials:
        return jsonify({"error": "Account not configured"}), 400

    from app import broker_api, _account_to_dict
    api = broker_api(_account_to_dict(account))
    try:
        resp = api.get_order_list()
        orders = []
        if isinstance(resp, dict):
            orders = resp.get("data") or resp.get("orders") or []
        elif isinstance(resp, list):
            orders = resp
        return jsonify(orders)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api_bp.route("/holdings", defaults={"client_id": None})
@api_bp.route("/holdings/<client_id>")
@login_required_api
def holdings(client_id=None):
    """Return holdings for the requested account if supported."""
    user = current_user()
    if client_id:
        account = Account.query.filter_by(user_id=user.id, client_id=client_id).first()
    else:
        account = get_primary_account(user)
    if not account or not account.credentials:
        return jsonify({"error": "Account not configured"}), 400

    from app import broker_api, _account_to_dict
    api = broker_api(_account_to_dict(account))

    if not hasattr(api, "get_holdings"):
        return jsonify({"error": "Holdings not supported for this broker"}), 400

    try:
        resp = api.get_holdings()
        if isinstance(resp, dict):
            data = resp.get("data") or resp.get("holdings") or []
        else:
            data = resp or []
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api_bp.route("/account")
@login_required_api
def account_stats():
    user = current_user()
    account = get_primary_account(user)
    if not account or not account.credentials:
        return jsonify({"error": "Account not configured"}), 400

    dhan = dhanhq(account.client_id, account.credentials.get("access_token"))
    try:
        stats_resp = dhan.get_fund_limits()
        if not isinstance(stats_resp, dict) or "data" not in stats_resp:
            return jsonify({"error": "Unexpected response format", "details": stats_resp}), 500
        stats = stats_resp["data"]
        mapped_stats = {
            "total_funds": stats.get("availabelBalance", 0),
            "available_margin": stats.get("withdrawableBalance", 0),
            "used_margin": stats.get("utilizedAmount", 0)
        }
        return jsonify(mapped_stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api_bp.route("/order-mappings")
@login_required_api
def order_mappings():
    user = current_user()
    mappings = [
        {
            "master_order_id": m.master_order_id,
            "child_order_id": m.child_order_id,
            "master_client_id": m.master_client_id,
            "master_broker": m.master_broker,
            "child_client_id": m.child_client_id,
            "child_broker": m.child_broker,
            "symbol": m.symbol,
            "status": m.status,
            "timestamp": m.timestamp,
            "child_timestamp": m.child_timestamp,
            "remarks": m.remarks,
            "multiplier": m.multiplier,
        }
        for m in order_mappings_for_user(user).all()
    ]
    return jsonify(mappings)


@api_bp.route("/child-orders")
@login_required_api
def child_orders():
    user = current_user()
    master_order_id = request.args.get("master_order_id")
    mappings = order_mappings_for_user(user)
    if master_order_id:
        mappings = mappings.filter_by(master_order_id=master_order_id)
    data = [
        {
            "master_order_id": m.master_order_id,
            "child_order_id": m.child_order_id,
            "master_client_id": m.master_client_id,
            "master_broker": m.master_broker,
            "child_client_id": m.child_client_id,
            "child_broker": m.child_broker,
            "symbol": m.symbol,
            "status": m.status,
            "timestamp": m.timestamp,
            "child_timestamp": m.child_timestamp,
            "remarks": m.remarks,
            "multiplier": m.multiplier,
        }
        for m in mappings.all()
    ]
    return jsonify(data)


@api_bp.route("/strategies", methods=["GET"])
@login_required_api
def list_strategies():
    """Return all strategies for the logged-in user."""
    user = current_user()
    strategies = (
        Strategy.query.filter_by(user_id=user.id)
        .order_by(Strategy.id.desc())
        .all()
    )
    data = [
        {
            "id": s.id,
            "name": s.name,
            "description": s.description,
            "asset_class": s.asset_class,
            "style": s.style,
            "allow_auto_submit": s.allow_auto_submit,
            "allow_live_trading": s.allow_live_trading,
            "allow_any_ticker": s.allow_any_ticker,
            "allowed_tickers": s.allowed_tickers,
            "notification_emails": s.notification_emails,
            "notify_failures_only": s.notify_failures_only,
        }
        for s in strategies
    ]
    return jsonify(data)


@api_bp.route("/strategies", methods=["POST"])
@login_required_api
def create_strategy():
    """Create a new trading strategy."""
    data = request.get_json() or {}

    required = ["name", "asset_class", "style"]
    for field in required:
        if not data.get(field):
            return jsonify({"error": f"{field} is required"}), 400
            
    user = current_user()
    strategy = Strategy(
        user_id=user.id,
        name=data.get("name"),
        description=data.get("description"),
        asset_class=data.get("asset_class"),
        style=data.get("style"),
        allow_auto_submit=bool(data.get("allow_auto_submit", True)),
        allow_live_trading=bool(data.get("allow_live_trading", True)),
        allow_any_ticker=bool(data.get("allow_any_ticker", True)),
        allowed_tickers=data.get("allowed_tickers"),
        notification_emails=data.get("notification_emails"),
        notify_failures_only=bool(data.get("notify_failures_only", False)),
    )

    db.session.add(strategy)
    db.session.commit()

    return jsonify({"message": "Strategy created", "id": strategy.id})

@api_bp.route("/strategies/<int:strategy_id>", methods=["GET", "PUT", "DELETE"])
@login_required_api
def strategy_detail(strategy_id):
    """Retrieve, update or delete a strategy."""
    user = current_user()
    strategy = Strategy.query.filter_by(id=strategy_id, user_id=user.id).first_or_404()

    if request.method == "GET":
        return jsonify({
            "id": strategy.id,
            "name": strategy.name,
            "description": strategy.description,
            "asset_class": strategy.asset_class,
            "style": strategy.style,
            "allow_auto_submit": strategy.allow_auto_submit,
            "allow_live_trading": strategy.allow_live_trading,
            "allow_any_ticker": strategy.allow_any_ticker,
            "allowed_tickers": strategy.allowed_tickers,
            "notification_emails": strategy.notification_emails,
            "notify_failures_only": strategy.notify_failures_only,
        })

    if request.method == "PUT":
        data = request.get_json() or {}
        for field in [
            "name",
            "description",
            "asset_class",
            "style",
            "allow_auto_submit",
            "allow_live_trading",
            "allow_any_ticker",
            "allowed_tickers",
            "notification_emails",
            "notify_failures_only",
            "is_active",
        ]:
            if field in data:
                setattr(strategy, field, data[field])
        db.session.commit()
        return jsonify({"message": "Strategy updated"})

    if request.method == "DELETE":
        db.session.delete(strategy)
        db.session.commit()
        return jsonify({"message": "Strategy deleted"})


@api_bp.route("/strategies/<int:strategy_id>/activate", methods=["POST"])
@login_required_api
def activate_strategy(strategy_id):
    """Activate a strategy"""
    user = current_user()
    strategy = Strategy.query.filter_by(id=strategy_id, user_id=user.id).first_or_404()
    strategy.is_active = True
    db.session.commit()
    return jsonify({"message": "Strategy activated"})

@api_bp.route("/strategies/<int:strategy_id>/deactivate", methods=["POST"])
@login_required_api
def deactivate_strategy(strategy_id):
    """Deactivate a strategy"""
    user = current_user()
    strategy = Strategy.query.filter_by(id=strategy_id, user_id=user.id).first_or_404()
    strategy.is_active = False
    db.session.commit()
    return jsonify({"message": "Strategy deactivated"})


@api_bp.route("/strategies/<int:strategy_id>/ping", methods=["POST"])
@login_required_api
def ping_strategy(strategy_id):
    """Update last run timestamp"""
    user = current_user()
    strategy = Strategy.query.filter_by(id=strategy_id, user_id=user.id).first_or_404()
    strategy.last_run_at = datetime.utcnow()
    db.session.commit()
    return jsonify({"message": "Strategy pinged"})
