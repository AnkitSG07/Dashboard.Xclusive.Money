import logging
import os
import secrets
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from copy import deepcopy
from threading import Lock

from flask import Blueprint, jsonify, session, request
from kombu.exceptions import OperationalError
from redis.exceptions import RedisError
from helpers import (
    current_user,
    get_primary_account,
    order_mappings_for_user,
    normalize_position,
)
from models import Account, db, Strategy, StrategyLog, StrategySubscription, Trade
from functools import wraps
from datetime import datetime, timedelta

from cache import cache_delete, cache_get, cache_set
from celery.exceptions import CeleryError

api_bp = Blueprint("api", __name__, url_prefix="/api")

logger = logging.getLogger(__name__)

_HOLDINGS_CACHE: dict[str, dict] = {}
_HOLDINGS_LOCK = Lock()

_SNAPSHOT_INTERVAL = timedelta(
    seconds=int(os.environ.get("DASHBOARD_SNAPSHOT_INTERVAL", "15"))
)
_HOLDINGS_INTERVAL = timedelta(
    seconds=int(os.environ.get("DASHBOARD_HOLDINGS_INTERVAL", "30"))
)
_BROKER_TIMEOUT_SECONDS = float(os.environ.get("BROKER_TIMEOUT_SECONDS", "4"))
_EXECUTOR = ThreadPoolExecutor(max_workers=4)

_TRUE_STRINGS = {"true", "1", "yes", "y", "on"}
_FALSE_STRINGS = {"false", "0", "no", "n", "off"}


def _parse_bool(value):
    """Best-effort parsing of truthy and falsy values from common inputs."""

    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if not text:
            return None
        if text in _TRUE_STRINGS:
            return True
        if text in _FALSE_STRINGS:
            return False
    return None


def _bool_or_default(value, default: bool) -> bool:
    parsed = _parse_bool(value)
    if parsed is None:
        return default
    return parsed


_BOOL_FIELDS = {
    "allow_auto_submit",
    "allow_live_trading",
    "allow_any_ticker",
    "notify_failures_only",
    "is_active",
    "track_performance",
    "is_public",
}

def snapshot_cache_key_for(user_id: int, client_id: str | None) -> str:
    client_key = client_id or "primary"
    return f"snapshot:{user_id}:{client_key}"


def login_required_api(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("user"):
            return jsonify({"error": "Unauthorized"}), 401
        return fn(*args, **kwargs)


    return wrapper


def _snapshot_cache_key(account: Account) -> str:
    return snapshot_cache_key_for(account.user_id, account.client_id)


def _holdings_cache_key(account: Account) -> str:
    client_id = account.client_id or "primary"
    return f"holdings:{account.user_id}:{client_id}"


def _default_order_summary() -> dict:
    return {"total_trades": 0, "total_quantity": 0, "last_status": "-"}


def _safe_float(val, default: float = 0.0) -> float:
    try:
        if val is None:
            return default
        return float(val)
    except (TypeError, ValueError):
        return default


def _call_with_timeout(func, timeout: float):
    future = _EXECUTOR.submit(func)
    try:
        return future.result(timeout=timeout)
    except FuturesTimeoutError as exc:  # pragma: no cover - concurrency edge
        future.cancel()
        raise TimeoutError(str(exc))


def _prepare_snapshot_for_response(entry: dict) -> dict:
    now = datetime.utcnow()
    data = deepcopy(entry.get("data", {}))
    data.setdefault("errors", {})
    data.setdefault("stale", False)
    cached_at = entry.get("timestamp", now)
    if isinstance(cached_at, datetime):
        data["cached_at"] = cached_at.isoformat() + "Z"
        data["age"] = max((now - cached_at).total_seconds(), 0.0)
    else:  # pragma: no cover - defensive
        data["cached_at"] = None
        data["age"] = None
    return data


def _refreshing_cache_key(key: str) -> str:
    return f"{key}:refreshing"


def _load_snapshot_entry(key: str) -> dict | None:
    entry = cache_get(key)
    if not isinstance(entry, dict):
        return None

    timestamp = entry.get("timestamp")
    if isinstance(timestamp, str):
        cleaned = timestamp.rstrip("Z")
        try:
            entry["timestamp"] = datetime.fromisoformat(cleaned)
        except ValueError:
            entry["timestamp"] = datetime.min
    return entry


def _save_snapshot_entry(key: str, snapshot: dict) -> dict:
    timestamp = datetime.utcnow()
    ttl_seconds = max(int(_SNAPSHOT_INTERVAL.total_seconds() * 6), 1)
    cache_set(
        key,
        {"timestamp": timestamp.isoformat() + "Z", "data": snapshot},
        ttl=ttl_seconds,
    )
    cache_delete(_refreshing_cache_key(key))
    return {"timestamp": timestamp, "data": snapshot}


def _refresh_snapshot_now(
    account: Account, *, key: str | None = None, entry: dict | None = None
) -> dict:
    key = key or _snapshot_cache_key(account)
    refresh_key = _refreshing_cache_key(key)
    existing_data: dict = {}
    if entry and isinstance(entry.get("data"), dict):
        existing_data = deepcopy(entry["data"])

    try:
        snapshot = _collect_snapshot(account, existing_data)
    except Exception:
        cache_delete(refresh_key)
        raise

    return _save_snapshot_entry(key, snapshot)


def _mark_refresh_scheduled(key: str) -> bool:
    refresh_key = _refreshing_cache_key(key)
    if cache_get(refresh_key):
        return False
    min_ttl = int((_SNAPSHOT_INTERVAL * 2).total_seconds())
    ttl_seconds = max(min_ttl, 60)
    cache_set(
        refresh_key,
        {"timestamp": datetime.utcnow().isoformat() + "Z"},
        ttl=ttl_seconds,
    )
    return True


def _enqueue_snapshot_refresh(account: Account, key: str) -> bool:
    if not _mark_refresh_scheduled(key):
        refresh_key = _refreshing_cache_key(key)
        marker = cache_get(refresh_key)
        if isinstance(marker, dict):
            timestamp = marker.get("timestamp")
            if isinstance(timestamp, str):
                cleaned = timestamp.rstrip("Z")
                try:
                    timestamp = datetime.fromisoformat(cleaned)
                except ValueError:
                    timestamp = None
            if not isinstance(timestamp, datetime):
                cache_delete(refresh_key)
                return False
            age = datetime.utcnow() - timestamp
            if age >= _SNAPSHOT_INTERVAL * 2:
                cache_delete(refresh_key)
                return False
            return True
        cache_delete(refresh_key)
        return False

    try:
        from task import celery

        # Summary requests must fail fast when the broker is offline so cached data
        # continues to render instantly.
        celery.send_task(
            "services.tasks.refresh_dashboard_snapshot",
            args=[account.user_id, account.client_id],
            ignore_result=True,
            retry=False,
            throw=False,
        )
    except (OperationalError, CeleryError, RedisError) as exc:
        logger.debug(
            "Unable to enqueue snapshot refresh due to broker issue: %s", exc,
        )
        cache_delete(_refreshing_cache_key(key))
        return False
    except Exception:
        cache_delete(_refreshing_cache_key(key))
        raise

    return True


def update_dashboard_snapshot_cache(account: Account) -> dict:
    entry = _refresh_snapshot_now(account)
    return _prepare_snapshot_for_response(entry)


def get_cached_dashboard_snapshot(
    account: Account, *, prefer_cache: bool = False
) -> dict:
    key = _snapshot_cache_key(account)
    entry = _load_snapshot_entry(key)
    now = datetime.utcnow()
    entry_age = None
    if entry and isinstance(entry.get("timestamp"), datetime):
        entry_age = now - entry["timestamp"]

    if prefer_cache and entry:
        response = _prepare_snapshot_for_response(entry)
        if entry_age is None or entry_age >= _SNAPSHOT_INTERVAL:
            response["stale"] = response.get("stale") or True
            scheduled = _enqueue_snapshot_refresh(account, key)
            if scheduled is False:
                errors = response.setdefault("errors", {})
                message = "Snapshot refresh deferred: background queue unavailable"
                if errors.get("snapshot"):
                    errors["snapshot"] = f"{errors['snapshot']}; {message}"
                else:
                    errors["snapshot"] = message
                logger.warning(
                    "Skipping synchronous snapshot refresh for user %s client %s; background queue unavailable",
                    account.user_id,
                    account.client_id or "primary",
                )
        return response

    if entry and entry_age is not None and entry_age < _SNAPSHOT_INTERVAL:
        return _prepare_snapshot_for_response(entry)

    try:
        refreshed = _refresh_snapshot_now(account, key=key, entry=entry)
        return _prepare_snapshot_for_response(refreshed)
    except Exception as exc:
        if entry:
            cached = _prepare_snapshot_for_response(entry)
            cached.setdefault("errors", {})["snapshot"] = str(exc)
            cached["stale"] = True
            return cached
        raise


def _normalize_portfolio_data(resp, account: Account) -> list:
    data = []
    if isinstance(resp, dict):
        if resp.get("status") == "success" and isinstance(resp.get("data"), list):
            data = resp.get("data")
        else:
            data = (
                resp.get("data")
                or resp.get("positions")
                or resp.get("net")
                or resp.get("netPositions")
                or resp.get("net_positions")
                or []
            )
    elif isinstance(resp, list):
        data = resp
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
    return [p for p in standardized if p]


def _order_summary_from_items(items: list) -> dict:
    summary = _default_order_summary()
    total_qty = 0
    last_status = summary["last_status"]
    for order in items:
        qty = order.get("quantity") or order.get("qty") or order.get("filledQty")
        total_qty += _safe_float(qty, 0)
        status = (
            order.get("order_status")
            or order.get("status")
            or order.get("statusMessage")
            or last_status
        )
        last_status = status
    summary["total_trades"] = len(items)
    summary["total_quantity"] = int(total_qty)
    summary["last_status"] = last_status or "-"
    return summary


def _normalize_orders(resp) -> dict:
    items = []
    summary = _default_order_summary()

    if isinstance(resp, dict):
        candidate = resp.get("data") or resp.get("orders") or []
        items = candidate if isinstance(candidate, list) else []
        raw_summary = resp.get("summary")
        if isinstance(raw_summary, dict):
            summary.update({k: raw_summary.get(k, summary[k]) for k in summary})
    elif isinstance(resp, list):
        items = resp
    else:
        items = []

    if not isinstance(items, list):
        items = []

    computed = _order_summary_from_items(items)
    summary = {
        "total_trades": summary.get("total_trades") or computed["total_trades"],
        "total_quantity": summary.get("total_quantity") or computed["total_quantity"],
        "last_status": summary.get("last_status") or computed["last_status"],
    }

    return {"items": items, "summary": summary}


def _normalize_account_stats(resp) -> dict:
    data = resp
    if isinstance(resp, dict):
        data = resp.get("data") or resp

    def first_float(keys):
        if not isinstance(data, dict):
            return None
        for key in keys:
            if key in data:
                val = _safe_float(data.get(key), default=None)
                if val is not None:
                    return val
        return None

    stats: dict[str, float] = {}
    if isinstance(data, dict):
        stats = {
            "total_funds": first_float(
                [
                    "availabelBalance",
                    "availableBalance",
                    "availableAmount",
                    "netCashAvailable",
                    "netCash",
                ]
            ),
            "available_margin": first_float(
                ["withdrawableBalance", "availableBalance", "availableAmount"]
            ),
            "used_margin": first_float(
                ["utilizedAmount", "usedMargin", "blockedMargin"]
            ),
        }

    def _search_numeric(value, keywords: tuple[str, ...]):
        if isinstance(value, dict):
            for key, candidate in value.items():
                if isinstance(key, str):
                    lowered = key.lower()
                    if any(word in lowered for word in keywords):
                        result = _safe_float(candidate, default=None)
                        if result is not None:
                            return result
                result = _search_numeric(candidate, keywords)
                if result is not None:
                    return result
        elif isinstance(value, list):
            for item in value:
                result = _search_numeric(item, keywords)
                if result is not None:
                    return result
        return None

    fallback: dict[str, float] = {}
    search_source = data if isinstance(data, (dict, list)) else resp
    try:
        from app import extract_balance
    except Exception:  # pragma: no cover - defensive
        extract_balance = None

    if extract_balance:
        balance = extract_balance(search_source)
        if balance is None and search_source is not resp:
            balance = extract_balance(resp)
        if balance is not None:
            fallback["total_funds"] = balance

    available = _search_numeric(
        search_source,
        (
            "availablemargin",
            "marginavailable",
            "availmargin",
            "withdrawablebalance",
            "availablebalance",
        ),
    )
    if available is not None:
        fallback["available_margin"] = available

    used = _search_numeric(
        search_source,
        ("usedmargin", "utilizedmargin", "utilizedamount", "blockedmargin", "marginused"),
    )
    if used is not None:
        fallback["used_margin"] = used

    if "total_funds" in fallback and "available_margin" not in fallback:
        fallback["available_margin"] = fallback["total_funds"]

    keys = ("total_funds", "available_margin", "used_margin")
    result: dict[str, float] = {}
    has_value = False
    for key in keys:
        value = None
        if stats:
            value = stats.get(key)
        if value is None and fallback:
            value = fallback.get(key)
        if value is not None:
            result[key] = value
            has_value = True
        else:
            result[key] = 0.0

    if has_value:
        return result

    return {}


def _collect_snapshot(account: Account, existing: dict | None = None) -> dict:
    existing = existing or {}
    from app import broker_api, _account_to_dict

    api = broker_api(_account_to_dict(account))
    if hasattr(api, "timeout"):
        try:
            api.timeout = min(getattr(api, "timeout", _BROKER_TIMEOUT_SECONDS), _BROKER_TIMEOUT_SECONDS)
        except Exception:  # pragma: no cover - defensive
            api.timeout = _BROKER_TIMEOUT_SECONDS

    existing_portfolio = deepcopy(existing.get("portfolio", []))
    existing_account = deepcopy(existing.get("account", {}))

    snapshot = {
        "portfolio": existing_portfolio,
        "orders": existing.get(
            "orders", {"items": [], "summary": _default_order_summary()}
        ),
        "account": existing_account,
        "errors": {},
        "stale": False,
    }

    positions_failed = False

    if hasattr(api, "get_positions"):
        try:
            resp = _call_with_timeout(api.get_positions, _BROKER_TIMEOUT_SECONDS)
            snapshot["portfolio"] = _normalize_portfolio_data(resp, account)
        except Exception as exc:
            snapshot["errors"]["portfolio"] = str(exc)
            snapshot["portfolio"] = existing_portfolio
            positions_failed = True
    else:
        snapshot["errors"]["portfolio"] = "Broker does not expose positions"
        positions_failed = True

    if hasattr(api, "get_order_list"):
        try:
            resp = _call_with_timeout(api.get_order_list, _BROKER_TIMEOUT_SECONDS)
            snapshot["orders"] = _normalize_orders(resp)
        except Exception as exc:
            snapshot["errors"]["orders"] = str(exc)
    else:
        snapshot["errors"]["orders"] = "Broker does not expose order list"

    account_failed = False
    profile_method = None
    for candidate in ("get_profile", "get_fund_limits", "get_account_details"):
        if hasattr(api, candidate):
            profile_method = getattr(api, candidate)
            break

    if profile_method is not None:
        try:
            resp = _call_with_timeout(profile_method, _BROKER_TIMEOUT_SECONDS)
            stats = _normalize_account_stats(resp)
            if stats:
                snapshot["account"] = stats
            else:
                snapshot["errors"].setdefault(
                    "account", "Account response missing expected fields"
                )
                snapshot["account"] = existing_account
                account_failed = True
        except Exception as exc:
            snapshot["errors"]["account"] = str(exc)
            snapshot["account"] = existing_account
            account_failed = True
    elif not snapshot["account"]:
        snapshot["errors"]["account"] = "Broker does not expose account stats"

    snapshot["stale"] = positions_failed or account_failed
    return snapshot


def _get_dashboard_snapshot(account: Account) -> dict:
    return get_cached_dashboard_snapshot(account, prefer_cache=False)


def _resolve_account(user, client_id: str | None) -> Account | None:
    if client_id:
        return Account.query.filter_by(user_id=user.id, client_id=client_id).first()
    return get_primary_account(user)


def _get_holdings_payload(account: Account) -> tuple[list, bool]:
    key = _holdings_cache_key(account)
    now = datetime.utcnow()

    with _HOLDINGS_LOCK:
        entry = _HOLDINGS_CACHE.get(key)
        if entry and now - entry["timestamp"] < _HOLDINGS_INTERVAL:
            return deepcopy(entry["data"]), entry.get("stale", False)

    from app import broker_api, _account_to_dict

    api = broker_api(_account_to_dict(account))
    if hasattr(api, "timeout"):
        try:
            api.timeout = min(getattr(api, "timeout", _BROKER_TIMEOUT_SECONDS), _BROKER_TIMEOUT_SECONDS)
        except Exception:  # pragma: no cover - defensive
            api.timeout = _BROKER_TIMEOUT_SECONDS

    if not hasattr(api, "get_holdings"):
        raise AttributeError("Broker does not expose holdings")

    cached_data = deepcopy(entry["data"]) if entry else []
    try:
        resp = _call_with_timeout(api.get_holdings, _BROKER_TIMEOUT_SECONDS)
        if isinstance(resp, dict):
            data = resp.get("data") or resp.get("holdings") or []
        else:
            data = resp or []
        if not isinstance(data, list):
            data = []
        entry = {
            "timestamp": datetime.utcnow(),
            "data": data,
            "stale": False,
        }
        with _HOLDINGS_LOCK:
            _HOLDINGS_CACHE[key] = entry
        return deepcopy(data), False
    except Exception as exc:
        if cached_data:
            return cached_data, True
        raise exc
@api_bp.route("/dashboard/snapshot", defaults={"client_id": None})
@api_bp.route("/dashboard/snapshot/<client_id>")
@login_required_api
def dashboard_snapshot(client_id=None):
    user = current_user()
    account = _resolve_account(user, client_id)
    if not account or not account.credentials:
        return jsonify({"error": "Account not configured"}), 400

    prefer_cache = request.args.get("cache", "").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    snapshot = get_cached_dashboard_snapshot(account, prefer_cache=prefer_cache)
    return jsonify(snapshot)


@api_bp.route("/portfolio", defaults={"client_id": None})
@api_bp.route("/portfolio/<client_id>")
@login_required_api
def portfolio(client_id=None):
    """Return positions for the requested account."""
    user = current_user()
    account = _resolve_account(user, client_id)
    if not account or not account.credentials:
        return jsonify({"error": "Account not configured"}), 400

    snapshot = _get_dashboard_snapshot(account)
    return jsonify(snapshot.get("portfolio", []))


@api_bp.route("/orders", defaults={"client_id": None})
@api_bp.route("/orders/<client_id>")
@login_required_api
def orders(client_id=None):
    """Return order list for the requested account."""
    user = current_user()
    account = _resolve_account(user, client_id)
    if not account or not account.credentials:
        return jsonify({"error": "Account not configured"}), 400

    snapshot = _get_dashboard_snapshot(account)
    orders = snapshot.get("orders", {})
    if isinstance(orders, dict):
        payload = {
            "orders": orders.get("items", []),
            "summary": orders.get("summary", _default_order_summary()),
            "stale": bool(snapshot.get("errors", {}).get("orders")),
            "cached_at": snapshot.get("cached_at"),
            "age": snapshot.get("age"),
        }
        error = snapshot.get("errors", {}).get("orders")
        if error:
            payload["error"] = error
        return jsonify(payload)
    return jsonify(orders)


@api_bp.route("/holdings", defaults={"client_id": None})
@api_bp.route("/holdings/<client_id>")
@login_required_api
def holdings(client_id=None):
    """Return holdings for the requested account if supported."""
    user = current_user()
    account = _resolve_account(user, client_id)
    if not account or not account.credentials:
        return jsonify({"error": "Account not configured"}), 400

    try:
        data, stale = _get_holdings_payload(account)
    except AttributeError:
        return jsonify({"error": "Holdings not supported for this broker"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({"data": data, "stale": stale})


@api_bp.route("/account")
@login_required_api
def account_stats():
    user = current_user()
    account = get_primary_account(user)
    if not account or not account.credentials:
        return jsonify({"error": "Account not configured"}), 400

    snapshot = _get_dashboard_snapshot(account)
    stats = snapshot.get("account", {})
    payload = dict(stats)
    payload.update(
        {
            "stale": bool(snapshot.get("errors", {}).get("account")),
            "cached_at": snapshot.get("cached_at"),
            "age": snapshot.get("age"),
        }
    )
    error = snapshot.get("errors", {}).get("account")
    if error:
        payload["error"] = error
    if not stats and error:
        return jsonify(payload), 502
    return jsonify(payload)


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
        data["webhook_secret"] = secrets.token_hex(16)

    strategy = Strategy(
        user_id=user.id,
        account_id=_to_int(account_id) if account_id else None,
        name=data.get("name"),
        description=data.get("description"),
        asset_class=data.get("asset_class"),
        style=data.get("style"),
        allow_auto_submit=_bool_or_default(data.get("allow_auto_submit"), True),
        allow_live_trading=_bool_or_default(data.get("allow_live_trading"), True),
        allow_any_ticker=_bool_or_default(data.get("allow_any_ticker"), True),
        allowed_tickers=data.get("allowed_tickers"),
        notification_emails=data.get("notification_emails"),
        notify_failures_only=_bool_or_default(data.get("notify_failures_only"), False),
        is_active=_bool_or_default(data.get("is_active"), False),
        signal_source=data.get("signal_source"),
        risk_max_positions=_to_int(data.get("risk_max_positions")),
        risk_max_allocation=_to_float(data.get("risk_max_allocation")),
        schedule=data.get("schedule"),
        webhook_secret=data.get("webhook_secret"),
        track_performance=_bool_or_default(data.get("track_performance"), False),
        log_retention_days=_to_int(data.get("log_retention_days")),
        is_public=_bool_or_default(data.get("is_public"), False),
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
                elif field in _BOOL_FIELDS:
                    value = data[field]
                    if value is None:
                        setattr(strategy, field, None)
                    else:
                        parsed = _parse_bool(value)
                        if parsed is None:
                            return jsonify({"error": f"Invalid value for {field}"}), 400
                        setattr(strategy, field, parsed)
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
    from brokers.symbol_map import get_symbol_map
    symbol = next(iter(get_symbol_map().keys()))
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
        webhook_secret=secrets.token_hex(16),
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

    ids = []
    if strategy.master_accounts:
        try:
            ids = [int(s) for s in str(strategy.master_accounts).split(",") if s.strip()]
        except ValueError:
            ids = []
    if not ids and strategy.account_id:
        ids = [strategy.account_id]
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
