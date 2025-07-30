from flask import Blueprint, jsonify, session, request
from helpers import (
    current_user,
    get_primary_account,
    order_mappings_for_user,
    normalize_position,
)
from models import Account, db, Strategy, StrategyLog, StrategySubscription, Trade
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
            return (
                jsonify({"error": "Unexpected response format", "details": stats_resp}),
                500,
            )
        stats = stats_resp["data"]
        mapped_stats = {
            "total_funds": stats.get("availabelBalance", 0),
            "available_margin": stats.get("withdrawableBalance", 0),
            "used_margin": stats.get("utilizedAmount", 0),
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
        Strategy.query.filter_by(user_id=user.id).order_by(Strategy.id.desc()).all()
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
            "account_id": s.account_id,
            "is_active": s.is_active,
            "last_run_at": s.last_run_at,
            "signal_source": s.signal_source,
            "risk_max_positions": s.risk_max_positions,
            "risk_max_allocation": s.risk_max_allocation,
            "schedule": s.schedule,
            "webhook_secret": s.webhook_secret,
            "track_performance": s.track_performance,
            "log_retention_days": s.log_retention_days,
            "is_public": s.is_public,
            "icon": s.icon,
            "brokers": s.brokers,
            "master_accounts": s.master_accounts,
        }
        for s in strategies
    ]
    return jsonify(data)


@api_bp.route("/strategies", methods=["POST"])
@login_required_api
def create_strategy():
    """Create a new trading strategy."""
    data = request.get_json(silent=True) or {}

    required = ["name", "asset_class", "style"]
    for field in required:
        if not data.get(field):
            return jsonify({"error": f"{field} is required"}), 400
            
    user = current_user()
    account_id = data.get("account_id")
    if account_id:
        acc = Account.query.filter_by(id=account_id, user_id=user.id).first()
        if not acc:
            return jsonify({"error": "Invalid account_id"}), 400    
    def _to_int(val):
        try:
            return int(val)
        except (TypeError, ValueError):
            return None

    def _to_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None
    brokers = data.get("brokers")
    if isinstance(brokers, list):
        brokers = ",".join(brokers)
    master_accounts = data.get("master_accounts")
    if isinstance(master_accounts, list):
        master_accounts = ",".join(str(a) for a in master_accounts)
    if not data.get("webhook_secret"):
        import secrets
        data["webhook_secret"] = secrets.token_hex(16)

    strategy = Strategy(
        user_id=user.id,
        account_id=_to_int(account_id) if account_id else None,
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
        is_active=bool(data.get("is_active", False)),
        signal_source=data.get("signal_source"),
        risk_max_positions=_to_int(data.get("risk_max_positions")),
        risk_max_allocation=_to_float(data.get("risk_max_allocation")),
        schedule=data.get("schedule"),
        webhook_secret=data.get("webhook_secret"),
        track_performance=bool(data.get("track_performance", False)),
        log_retention_days=_to_int(data.get("log_retention_days")),
        is_public=bool(data.get("is_public", False)),
        icon=data.get("icon"),
        brokers=brokers,
        master_accounts=master_accounts,
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
        return jsonify(
            {
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
                "account_id": strategy.account_id,
                "is_active": strategy.is_active,
                "last_run_at": strategy.last_run_at,
                "signal_source": strategy.signal_source,
                "risk_max_positions": strategy.risk_max_positions,
                "risk_max_allocation": strategy.risk_max_allocation,
                "schedule": strategy.schedule,
                "webhook_secret": strategy.webhook_secret,
                "track_performance": strategy.track_performance,
                "log_retention_days": strategy.log_retention_days,
                "is_public": strategy.is_public,
                "icon": strategy.icon,
                "brokers": strategy.brokers,
                "master_accounts": strategy.master_accounts,
            }
        )

    if request.method == "PUT":
        data = request.get_json() or {}
        def _to_int(val):
            try:
                return int(val)
            except (TypeError, ValueError):
                return None

        def _to_float(val):
            try:
                return float(val)
            except (TypeError, ValueError):
                return None

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
            "account_id",
            "signal_source",
            "risk_max_positions",
            "risk_max_allocation",
            "schedule",
            "webhook_secret",
            "track_performance",
            "log_retention_days",
            "is_public",
            "icon",
            "brokers",
            "master_accounts",
        ]:
            if field in data:
                if field == "account_id" and data[field] is not None:
                    acc = Account.query.filter_by(
                        id=data[field], user_id=user.id
                    ).first()
                    if not acc:
                        return jsonify({"error": "Invalid account_id"}), 400
                    setattr(strategy, field, _to_int(data[field]))
                elif field in ["risk_max_positions", "log_retention_days"]:
                    setattr(strategy, field, _to_int(data[field]))
                elif field == "risk_max_allocation":
                    setattr(strategy, field, _to_float(data[field]))
                else:
                    if field in ["brokers", "master_accounts"] and isinstance(
                        data[field], list
                    ):
                        setattr(strategy, field, ",".join(str(v) for v in data[field]))
                    else:
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

@api_bp.route("/strategies/<int:strategy_id>/regenerate-secret", methods=["POST"])
@login_required_api
def regenerate_webhook_secret(strategy_id):
    """Generate a new webhook secret for the strategy."""
    user = current_user()
    strategy = Strategy.query.filter_by(id=strategy_id, user_id=user.id).first_or_404()
    import secrets
    strategy.webhook_secret = secrets.token_hex(16)
    db.session.commit()
    return jsonify({"webhook_secret": strategy.webhook_secret})


@api_bp.route("/strategies/<int:strategy_id>/test-webhook", methods=["POST"])
@login_required_api
def test_webhook(strategy_id):
    """Send a test payload to the webhook endpoint."""
    user = current_user()
    strategy = Strategy.query.filter_by(id=strategy_id, user_id=user.id).first_or_404()
    token = user.webhook_token
    if not token:
        import secrets
        token = secrets.token_hex(16)
        user.webhook_token = token
        db.session.commit()
    from brokers.symbol_map import SYMBOL_MAP
    symbol = next(iter(SYMBOL_MAP.keys()))
    payload = {"symbol": symbol, "action": "BUY", "quantity": 1}
    if not strategy.webhook_secret:
        import secrets
        strategy.webhook_secret = secrets.token_hex(16)
        db.session.commit()
    payload["secret"] = strategy.webhook_secret
    from flask import current_app
    with current_app.test_client() as c:
        resp = c.post(f"/webhook/{token}", json=payload)
        try:
            data = resp.get_json()
        except Exception:
            data = resp.data.decode()
    return jsonify({"status": resp.status_code, "response": data})


@api_bp.route("/strategies/<int:strategy_id>/subscribe", methods=["POST"])
@login_required_api
def subscribe_strategy(strategy_id):
    """Subscribe the logged in user to a strategy."""
    user = current_user()
    strategy = Strategy.query.get_or_404(strategy_id)
    if strategy.user_id == user.id:
        return jsonify({"error": "Cannot subscribe to your own strategy"}), 400
    data = request.get_json(silent=True) or {}
    
    sub = StrategySubscription.query.filter_by(
        strategy_id=strategy_id, subscriber_id=user.id
    ).first()
    if not sub:
        sub = StrategySubscription(strategy_id=strategy_id, subscriber_id=user.id)
        if strategy.is_public:
            sub.approved = True
        db.session.add(sub)
        db.session.commit()

    account_id = data.get("account_id")
    if account_id:
        acc = Account.query.filter_by(id=account_id, user_id=user.id).first()
        if not acc:
            return jsonify({"error": "Invalid account_id"}), 400
        sub.account_id = account_id

    sub.auto_submit = bool(data.get("auto_submit", True))
    sub.order_type = data.get("order_type") or sub.order_type
    sub.qty_mode = data.get("qty_mode") or sub.qty_mode
    sub.fixed_qty = (
        data.get("fixed_qty") if data.get("fixed_qty") is not None else sub.fixed_qty
    )

    db.session.commit()

    return jsonify(
        {"message": "Subscription saved", "id": sub.id, "approved": sub.approved}
    )


@api_bp.route("/strategies/<int:strategy_id>/subscribers", methods=["GET"])
@login_required_api
def list_subscribers(strategy_id):
    user = current_user()
    strategy = Strategy.query.filter_by(id=strategy_id, user_id=user.id).first_or_404()
    subs = [
        {
            "id": s.id,
            "subscriber_id": s.subscriber_id,
            "approved": s.approved,
            "created_at": s.created_at,
        }
        for s in StrategySubscription.query.filter_by(strategy_id=strategy_id).all()
    ]
    return jsonify(subs)


@api_bp.route("/strategies/<int:strategy_id>/clone", methods=["POST"])
@login_required_api
def clone_strategy(strategy_id):
    """Clone an existing strategy."""
    user = current_user()
    src = Strategy.query.filter_by(id=strategy_id, user_id=user.id).first_or_404()
    clone = Strategy(
        user_id=user.id,
        account_id=src.account_id,
        name=f"{src.name} (Copy)",
        description=src.description,
        asset_class=src.asset_class,
        style=src.style,
        allow_auto_submit=src.allow_auto_submit,
        allow_live_trading=src.allow_live_trading,
        allow_any_ticker=src.allow_any_ticker,
        allowed_tickers=src.allowed_tickers,
        notification_emails=src.notification_emails,
        notify_failures_only=src.notify_failures_only,
        signal_source=src.signal_source,
        risk_max_positions=src.risk_max_positions,
        risk_max_allocation=src.risk_max_allocation,
        schedule=src.schedule,
        webhook_secret=src.webhook_secret,
        track_performance=src.track_performance,
        log_retention_days=src.log_retention_days,
        is_public=src.is_public,
        icon=src.icon,
        brokers=src.brokers,
        master_accounts=src.master_accounts,
    )
    db.session.add(clone)
    db.session.commit()
    return jsonify({"message": "Strategy cloned", "id": clone.id})

@api_bp.route("/strategies/<int:strategy_id>/logs", methods=["GET", "POST"])
@login_required_api
def strategy_logs(strategy_id):
    """Retrieve or add log entries for a strategy"""
    user = current_user()
    strategy = Strategy.query.filter_by(id=strategy_id, user_id=user.id).first_or_404()

    if request.method == "POST":
        data = request.get_json() or {}
        log = StrategyLog(
            strategy_id=strategy.id,
            level=data.get("level", "INFO"),
            message=data.get("message"),
            performance=data.get("performance"),
        )
        db.session.add(log)
        db.session.commit()
        return jsonify({"message": "Log added", "id": log.id})

    logs = [
        {
            "id": l.id,
            "timestamp": l.timestamp,
            "level": l.level,
            "message": l.message,
            "performance": l.performance,
        }
        for l in StrategyLog.query.filter_by(strategy_id=strategy.id)
        .order_by(StrategyLog.timestamp.desc())
        .all()
    ]
    return jsonify(logs)


@api_bp.route("/strategies/<int:strategy_id>/orders", methods=["GET"])
@login_required_api
def strategy_orders(strategy_id):
    """Return trade logs grouped by broker for a strategy."""
    user = current_user()
    strategy = Strategy.query.filter_by(id=strategy_id, user_id=user.id).first_or_404()

    if not strategy.master_accounts:
        return jsonify({})

    try:
        ids = [int(s) for s in str(strategy.master_accounts).split(",") if s.strip()]
    except ValueError:
        ids = []
    if not ids:
        return jsonify({})

    accounts = Account.query.filter(
        Account.user_id == user.id, Account.id.in_(ids)
    ).all()
    result = {}
    for acc in accounts:
        trades = (
            Trade.query.filter_by(user_id=user.id, client_id=acc.client_id)
            .order_by(Trade.timestamp.desc())
            .all()
        )
        if not trades:
            continue
        broker = acc.broker or "Unknown"
        result[broker] = [
            {
                "id": t.id,
                "timestamp": t.timestamp.isoformat() if t.timestamp else None,
                "symbol": t.symbol,
                "action": t.action,
                "qty": t.qty,
                "price": t.price,
                "status": t.status,
                "order_id": t.order_id,
                "client_id": t.client_id,
            }
            for t in trades
        ]

    return jsonify(result)


@api_bp.route("/subscriptions", methods=["GET"])
@login_required_api
def list_subscriptions():
    """Return subscriptions for the logged in user."""
    user = current_user()
    subs = StrategySubscription.query.filter_by(subscriber_id=user.id).all()
    out = []
    for s in subs:
        out.append(
            {
                "id": s.id,
                "strategy_id": s.strategy_id,
                "strategy_name": s.strategy.name if s.strategy else None,
                "account_id": s.account_id,
                "account_client_id": s.account.client_id if s.account else None,
                "order_type": s.order_type,
                "qty_mode": s.qty_mode,
                "fixed_qty": s.fixed_qty,
                "auto_submit": s.auto_submit,
                "approved": s.approved,
            }
        )
    return jsonify(out)


@api_bp.route("/subscriptions/<int:sub_id>", methods=["PUT", "DELETE"])
@login_required_api
def update_subscription(sub_id):
    """Update or delete a subscription."""
    user = current_user()
    sub = StrategySubscription.query.filter_by(
        id=sub_id, subscriber_id=user.id
    ).first_or_404()

    if request.method == "DELETE":
        db.session.delete(sub)
        db.session.commit()
        return jsonify({"message": "Subscription deleted"})

    data = request.get_json() or {}
    if "account_id" in data:
        acc = Account.query.filter_by(id=data["account_id"], user_id=user.id).first()
        if not acc:
            return jsonify({"error": "Invalid account_id"}), 400
        sub.account_id = data["account_id"]
    if "auto_submit" in data:
        sub.auto_submit = bool(data.get("auto_submit"))
    if "order_type" in data:
        sub.order_type = data.get("order_type") or sub.order_type
    if "qty_mode" in data:
        sub.qty_mode = data.get("qty_mode") or sub.qty_mode
    if "fixed_qty" in data:
        sub.fixed_qty = data.get("fixed_qty")

    db.session.commit()
    return jsonify({"message": "Subscription updated"})
