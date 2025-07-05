from brokers.factory import get_broker_class
import uuid
from brokers.zerodha import ZerodhaBroker, KiteConnect
from brokers.fyers import FyersBroker
try:
    from brokers.symbol_map import get_symbol_for_broker
except Exception:  # pragma: no cover - fallback if import fails
    def get_symbol_for_broker(symbol: str, broker: str) -> dict:
        """Fallback stub returning empty mapping."""
        return {}
from flask import Flask, request, jsonify, render_template, session, redirect, url_for, flash
from dhanhq import dhanhq
import os
import json
import pandas as pd
from flask_cors import CORS
# from flask_limiter import Limiter
# from flask_limiter.util import get_remote_address
from flask_talisman import Talisman
import io
from datetime import datetime, timedelta, date
import requests
from bs4 import BeautifulSoup
from apscheduler.schedulers.background import BackgroundScheduler
import tempfile
import shutil
from functools import wraps
from werkzeug.utils import secure_filename
import random
import string
from urllib.parse import quote
from models import (
    db,
    User,
    Account,
    Trade,
    WebhookLog,
    SystemLog,
    Setting,
    Group,
    OrderMapping,
    TradeLog,
)
from werkzeug.security import generate_password_hash, check_password_hash
import re
import logging
from logging.handlers import RotatingFileHandler
import time
import signal
import sys
import threading
import atexit
from collections import defaultdict
from threading import Lock

# Define emoji regex pattern
EMOJI_RE = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)

# Configuration Constants
POLLING_INTERVAL = int(os.environ.get("POLLING_INTERVAL", "10"))
API_TIMEOUT = int(os.environ.get("API_TIMEOUT", "3"))
MAX_MASTERS_PER_CYCLE = int(os.environ.get("MAX_MASTERS_PER_CYCLE", "100"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "development")

# Admin credentials
ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "admin@example.com")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "admin")

# Flask App Creation
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "change-me-in-production")

# Database Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(BASE_DIR, "data"))
os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, "quantbot.db")
db_url = os.environ.get("DATABASE_URL", f"sqlite:///{DB_PATH}")
app.config["SQLALCHEMY_DATABASE_URI"] = db_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_pre_ping": True,
    "pool_size": int(os.environ.get("DB_POOL_SIZE", "10")),
    "max_overflow": int(os.environ.get("DB_MAX_OVERFLOW", "20")),
    "pool_recycle": int(os.environ.get("DB_POOL_RECYCLE", "3600"))
}

db.init_app(app)
start_time = datetime.utcnow()

# Authentication decorators
def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("user"):
            return redirect(url_for("login"))
        return view(*args, **kwargs)
    return wrapped

def admin_login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("admin"):
            return redirect(url_for("admin_login"))
        return view(*args, **kwargs)
    return wrapped

def require_user(f):
    """Decorator to get current user and handle authentication"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user_email = session.get("user")
        if not user_email:
            return jsonify({"error": "Authentication required"}), 401
            
        user = User.query.filter_by(email=user_email).first()
        if not user:
            session.clear()
            return jsonify({"error": "User not found"}), 404
            
        request.current_user = user
        return f(*args, **kwargs)
    return decorated_function

class RequestIDFormatter(logging.Formatter):
    """Custom formatter that includes request ID"""
    def format(self, record):
        try:
            from flask import has_request_context, request
            if has_request_context() and hasattr(request, 'request_id'):
                record.request_id = request.request_id
            else:
                record.request_id = 'N/A'
            
            if has_request_context() and hasattr(request, 'current_user'):
                record.user_email = getattr(request.current_user, 'email', 'anonymous')
            else:
                record.user_email = 'anonymous'
        except Exception:
            record.request_id = 'N/A'
            record.user_email = 'anonymous'
            
        return super().format(record)
        
def setup_logging_with_request_id():
    """Configure comprehensive logging with request ID tracking"""
    log_level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
    
    # Create logs directory
    log_dir = os.path.join(DATA_DIR, "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    # Main application logger
    app_logger = logging.getLogger(__name__)
    app_logger.setLevel(log_level)
    
    # Custom formatter with request ID
    formatter = RequestIDFormatter(
        '%(asctime)s [%(levelname)s] [ReqID:%(request_id)s] [User:%(user_email)s] %(name)s: %(message)s'
    )
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    app_logger.addHandler(console_handler)
    
    # File handler with rotation
    if ENVIRONMENT == "production":
        file_handler = RotatingFileHandler(
            os.path.join(log_dir, "app.log"),
            maxBytes=10*1024*1024,  # 10MB
            backupCount=10
        )
        file_handler.setFormatter(formatter)
        app_logger.addHandler(file_handler)
        
        # Error-only log file
        error_handler = RotatingFileHandler(
            os.path.join(log_dir, "errors.log"),
            maxBytes=5*1024*1024,  # 5MB
            backupCount=5
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        app_logger.addHandler(error_handler)
    
    return app_logger

# Initialize logger
logger = setup_logging_with_request_id()

# Graceful Shutdown Handler
class GracefulShutdown:
    """Handle graceful shutdown of the application"""
    
    def __init__(self):
        self.shutdown_requested = False
        self.active_requests = 0
        self.shutdown_timeout = 30
        
    def request_shutdown(self):
        self.shutdown_requested = True
        
    def is_shutdown_requested(self):
        return self.shutdown_requested
        
    def increment_active_requests(self):
        self.active_requests += 1
        
    def decrement_active_requests(self):
        self.active_requests = max(0, self.active_requests - 1)
        
    def wait_for_requests_to_finish(self):
        if self.active_requests > 0:
            logger.info(f"Waiting for {self.active_requests} active requests to complete...")
            
            timeout_counter = 0
            while self.active_requests > 0 and timeout_counter < self.shutdown_timeout:
                time.sleep(1)
                timeout_counter += 1
                
            if self.active_requests > 0:
                logger.warning(f"Timeout: {self.active_requests} requests still active after {self.shutdown_timeout}s")
            else:
                logger.info("All requests completed successfully")

# Create global shutdown handler
shutdown_handler = GracefulShutdown()

# Security Configuration
CORS(app, origins=os.environ.get("ALLOWED_ORIGINS", "*").split(","))
if ENVIRONMENT == "production":
    Talisman(app, force_https=True)

# Rate Limiting
# limiter = Limiter(
 #   app,
  #  key_func=get_remote_address,
   # default_limits=["1000 per day", "100 per hour"],
    #storage_uri=os.environ.get("REDIS_URL", "memory://")
# )

# Performance Monitoring
performance_metrics = defaultdict(list)
metrics_lock = Lock()

def monitor_performance(threshold_seconds=2.0):
    """Decorator to monitor endpoint performance"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            start_time = time.time()
            endpoint = request.endpoint or f.__name__
            method = request.method
            
            try:
                result = f(*args, **kwargs)
                duration = time.time() - start_time
                status_code = getattr(result, 'status_code', 200) if hasattr(result, 'status_code') else 200
                
                # Log slow requests
                if duration > threshold_seconds:
                    logger.warning(
                        f"SLOW REQUEST: {method} {endpoint} took {duration:.2f}s "
                        f"(threshold: {threshold_seconds}s) - Status: {status_code}"
                    )
                
                # Store metrics
                with metrics_lock:
                    performance_metrics[endpoint].append({
                        'duration': duration,
                        'timestamp': datetime.utcnow().isoformat(),
                        'method': method,
                        'status_code': status_code,
                        'user': getattr(request, 'current_user', {}).get('email', 'anonymous') if hasattr(request, 'current_user') else 'anonymous'
                    })
                    
                    # Keep only last 100 entries per endpoint
                    if len(performance_metrics[endpoint]) > 100:
                        performance_metrics[endpoint] = performance_metrics[endpoint][-100:]
                
                return result
                
            except Exception as e:
                duration = time.time() - start_time
                logger.error(
                    f"REQUEST FAILED: {method} {endpoint} after {duration:.2f}s - Error: {str(e)}"
                )
                raise
                
        return decorated_function
    return decorator

BROKER_STATUS_URLS = {
    "dhan": "https://api.dhan.co",
    "zerodha": "https://api.kite.trade",
    "aliceblue": "https://ant.aliceblueonline.com",
    "finvasia": "https://api.shoonya.com",
    "fyers": "https://api.fyers.in",
    "groww": "https://groww.in",
}

# Validation patterns
CLIENT_ID_PATTERN = re.compile(r'^[A-Za-z0-9_-]{3,50}$')
BROKER_PATTERN = re.compile(r'^[a-z]{3,20}$')

def validate_client_id(client_id: str) -> bool:
    """Validate client ID format"""
    return bool(CLIENT_ID_PATTERN.match(client_id)) if client_id else False

def validate_broker(broker: str) -> bool:
    """Validate broker name format"""
    return bool(BROKER_PATTERN.match(broker)) if broker else False

def safe_json_response(data, status_code=200):
    """Return safe JSON response without exposing internal details in production"""
    if ENVIRONMENT == "production" and status_code >= 500:
        if isinstance(data, dict) and "details" in data:
            data = {"error": data.get("error", "Internal server error")}
    return jsonify(data), status_code

def map_order_type(order_type: str, broker: str) -> str:
    """Convert generic order types to broker specific codes."""
    if not order_type:
        return ""
    broker = broker.lower() if broker else ""
    if broker in ("aliceblue", "finvasia") and order_type.upper() == "MARKET":
        return "MKT"
    return str(order_type)

def _resolve_data_path(path: str) -> str:
    """Return an absolute path inside ``DATA_DIR`` for relative paths."""
    if os.path.isabs(path) or os.path.dirname(path):
        return path
    return os.path.join(DATA_DIR, path)

def _account_to_dict(acc: Account) -> dict:
    return {
        "id": acc.id,
        "owner": acc.user.email if acc.user else None,
        "broker": acc.broker,
        "client_id": acc.client_id,
        "username": acc.username,
        "token_expiry": acc.token_expiry,
        "status": acc.status,
        "role": acc.role,
        "linked_master_id": acc.linked_master_id,
        "copy_status": acc.copy_status,
        "multiplier": acc.multiplier,
        "credentials": acc.credentials,
        "last_copied_trade_id": acc.last_copied_trade_id,
        "auto_login": acc.auto_login,
        "last_login": acc.last_login_time,
        "device_number": acc.device_number,
    }

def get_user_by_token(token: str):
    """Get user by webhook token."""
    return User.query.filter_by(webhook_token=token).first()

def get_primary_account(user):
    """Get the primary account for a user."""
    if user.accounts:
        return user.accounts[0]
    return None

def get_accounts_for_user(user_email: str):
    """Get all accounts for a user."""
    user = User.query.filter_by(email=user_email).first()
    if not user:
        return []
    return user.accounts

def get_master_accounts():
    """Get all master accounts with their children."""
    masters = Account.query.filter_by(role='master').limit(MAX_MASTERS_PER_CYCLE).all()
    result = []
    for master in masters:
        children = Account.query.filter_by(
            role='child', 
            linked_master_id=master.client_id
        ).all()
        master_dict = _account_to_dict(master)
        master_dict['children'] = [_account_to_dict(child) for child in children]
        result.append(master_dict)
    return result

def get_account_by_client_id(client_id: str):
    """Get account by client ID."""
    return Account.query.filter_by(client_id=client_id).first()

def _group_to_dict(g: Group) -> dict:
    return {
        "name": g.name,
        "owner": g.user.email if g.user else None,
        "members": [a.client_id for a in g.accounts],
    }

def strip_emojis_from_obj(obj):
    """Recursively remove emoji characters from strings in lists/dicts."""
    if isinstance(obj, dict):
        return {k: strip_emojis_from_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [strip_emojis_from_obj(v) for v in obj]
    if isinstance(obj, str):
        return EMOJI_RE.sub('', obj)
    return obj

def parse_order_list(resp):
    """Return a normalized list of order dictionaries from diverse API responses."""
    if resp is None:
        return []

    # Convert dictionaries that wrap the actual list
    if isinstance(resp, dict):
        data_field = resp.get("data", resp)
        if isinstance(data_field, dict):
            resp = (
                data_field.get("orderBook")
                or data_field.get("orders")
                or data_field.get("OrderBookDetail")
                or data_field.get("tradeBook")
                or data_field.get("TradeBook")
                or data_field.get("tradebook")
                or data_field.get("Tradebook")
                or data_field.get("trade_book")
                or data_field.get("trades")
                or []
            )
        else:
            resp = data_field

    # Ensure we have a list to work with
    if not isinstance(resp, list):
        resp = [resp]

    orders = []
    for item in resp:
        if isinstance(item, dict):
            orders.append(item)
        elif isinstance(item, list):
            orders.extend(parse_order_list(item))
    return orders

TIME_FORMATS = [
    "%d-%b-%Y %H:%M:%S",
    "%d-%m-%Y %H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%d-%b-%Y %H:%M",
    "%d-%m-%Y %H:%M",
]

def parse_timestamp(value):
    """Parse diverse timestamp formats to a ``datetime`` object."""
    if not value:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value))
        except Exception:
            return None
    for fmt in TIME_FORMATS:
        try:
            return datetime.strptime(str(value), fmt)
        except Exception:
            continue
    return None

def get_order_sort_key(order):
    """Return a sort key for an order based on timestamp or order ID."""
    ts = parse_timestamp(
        order.get("orderTimestamp")
        or order.get("order_time")
        or order.get("create_time")
        or order.get("orderDateTime")
    )
    if ts:
        return ts
    try:
        return int(
            order.get("orderId")
            or order.get("order_id")
            or order.get("id")
            or order.get("NOrdNo")
            or order.get("nestOrderNumber")
            or order.get("orderNumber")
            or order.get("Nstordno")
            or order.get("norenordno")
            or 0
        )
    except Exception:
        return 0

def extract_balance(data):
    """Recursively search for a numeric balance value in API data."""
    if isinstance(data, dict):
        for k, v in data.items():
            if k.lower() in [
                "balance",
                "cash",
                "netbalance",
                "openingbalance",
                "availablebalance",
                "available_cash",
                "availablecash",
                "availabelbalance",
                "withdrawablebalance",
                "equityamount",
                "netcash",
            ]:
                try:
                    return float(v)
                except (TypeError, ValueError):
                    pass
            val = extract_balance(v)
            if val is not None:
                return val
    elif isinstance(data, list):
        for item in data:
            val = extract_balance(item)
            if val is not None:
                return val
    return None

def broker_api(obj):
    """Instantiate a broker adapter using stored account credentials."""
    broker = obj.get("broker", "Unknown").lower()
    client_id = obj.get("client_id")

    # Combine credentials dict with any legacy top-level fields
    credentials = dict(obj.get("credentials", {}))
    for key in [
        "api_key",
        "api_secret",
        "request_token",
        "password",
        "totp_secret",
        "access_token",
        "vendor_code",
        "imei",
    ]:
        if key in obj and key not in credentials:
            credentials[key] = obj[key]

    access_token = credentials.get("access_token")
    BrokerClass = get_broker_class(broker)
    rest = {k: v for k, v in credentials.items() if k != "access_token"}

    if broker == "aliceblue":
        api_key = rest.pop("api_key", None)
        return BrokerClass(client_id, api_key, **rest)

    elif broker == "finvasia":
        password = rest.pop("password", None)
        totp_secret = rest.pop("totp_secret", None)
        vendor_code = rest.pop("vendor_code", None)
        api_key = rest.pop("api_key", None)
        imei = rest.pop("imei", "abc1234") or "abc1234"
        return BrokerClass(
            client_id=client_id,
            password=password,
            totp_secret=totp_secret,
            vendor_code=vendor_code,
            api_key=api_key,
            imei=imei,
            **rest
        )

    elif broker == "groww":
        return BrokerClass(client_id, access_token, **rest)
    return BrokerClass(client_id, access_token, **rest)

def get_opening_balance_for_account(acc):
    """Instantiate broker and try to fetch opening balance."""
    try:
        api = broker_api(acc)
        if hasattr(api, "get_opening_balance"):
            bal = api.get_opening_balance()
            if bal is not None:
                return bal
        if hasattr(api, "get_profile"):
            resp = api.get_profile()
            data = resp.get("data", resp) if isinstance(resp, dict) else resp
            return extract_balance(data)
    except Exception as e:
        logger.error(f"Failed to fetch balance for {acc.get('client_id')}: {e}")
    return None

# Utility loaders for admin dashboard
def load_users():
    return User.query.all()

def load_accounts():
    return Account.query.all()

def load_trades():
    return Trade.query.all()

def load_logs():
    return WebhookLog.query.all(), SystemLog.query.all()

def load_settings():
    result = {'trading_enabled': True}
    for s in Setting.query.all():
        if s.key == 'trading_enabled':
            result['trading_enabled'] = s.value.lower() == 'true'
        else:
            result[s.key] = s.value
    return result

def save_settings(settings):
    for key, value in settings.items():
        s = Setting.query.filter_by(key=key).first()
        if not s:
            s = Setting(key=key, value=str(value))
            db.session.add(s)
        else:
            s.value = str(value)
    db.session.commit()

def save_account_to_user(owner: str, account: dict):
    """Persist account credentials to the database."""
    user = User.query.filter_by(email=owner).first()
    if not user:
        user = User(email=owner)
        db.session.add(user)
        db.session.commit()

    acc = Account(
        user_id=user.id,
        broker=account.get("broker"),
        client_id=account.get("client_id"),
        username=account.get("username"),
        token_expiry=account.get("token_expiry"),
        status=account.get("status", "Connected"),
        role=account.get("role"),
        linked_master_id=account.get("linked_master_id"),
        copy_status=account.get("copy_status", "Off"),
        multiplier=account.get("multiplier", 1.0),
        credentials=account.get("credentials"),
        last_copied_trade_id=account.get("last_copied_trade_id"),
        auto_login=account.get("auto_login", True),
        last_login_time=account.get("last_login"),
        device_number=account.get("device_number")
    )
    db.session.add(acc)
    db.session.commit()

def get_pending_zerodha() -> dict:
    setting = Setting.query.filter_by(key="pending_zerodha").first()
    if setting:
        try:
            return json.loads(setting.value)
        except Exception:
            return {}
    return {}

def set_pending_zerodha(data: dict):
    setting = Setting.query.filter_by(key="pending_zerodha").first()
    if not setting:
        setting = Setting(key="pending_zerodha", value=json.dumps(data))
        db.session.add(setting)
    else:
        setting.value = json.dumps(data)
    db.session.commit()

def get_pending_fyers() -> dict:
    setting = Setting.query.filter_by(key="pending_fyers").first()
    if setting:
        try:
            return json.loads(setting.value)
        except Exception:
            return {}
    return {}

def set_pending_fyers(data: dict):
    setting = Setting.query.filter_by(key="pending_fyers").first()
    if not setting:
        setting = Setting(key="pending_fyers", value=json.dumps(data))
        db.session.add(setting)
    else:
        setting.value = json.dumps(data)
    db.session.commit()

def get_user_credentials(identifier: str):
    """Return basic broker credentials for a user id or client id."""
    user = User.query.filter_by(email=identifier).first()
    account = None
    if user and user.accounts:
        account = user.accounts[0]
    if account is None:
        account = Account.query.filter_by(client_id=identifier).first()
    if not account:
        return None
    creds = account.credentials or {}
    return {
        "broker": account.broker,
        "client_id": account.client_id,
        "access_token": creds.get("access_token"),
        "credentials": creds,
    }

def format_uptime():
    delta = datetime.utcnow() - start_time
    hours, remainder = divmod(int(delta.total_seconds()), 3600)
    minutes, _ = divmod(remainder, 60)
    return f"{hours}h {minutes}m"

def check_api(url: str) -> bool:
    try:
        resp = requests.get(url, timeout=API_TIMEOUT)
        return resp.ok
    except Exception:
        return False

def find_account_by_client_id(client_id):
    """Return ``(account, parent_master)`` for the provided ``client_id`` using the database."""
    acc = Account.query.filter_by(client_id=str(client_id)).first()
    if not acc:
        return None, None

    if acc.role == "child":
        master = Account.query.filter_by(client_id=acc.linked_master_id).first()
        return acc, master
    return acc, None

def clean_response_message(response):
    if isinstance(response, dict):
        remarks = response.get("remarks")
        if isinstance(remarks, dict):
            return (
                remarks.get("errorMessage")
                or remarks.get("error_message")
                or str(remarks)
            )

        error_fields = [
            remarks,
            response.get("error"),
            response.get("errorMessage"),
            response.get("error_message"),
            response.get("emsg"),
            response.get("message"),
            response.get("reason"),
        ]
        for field in error_fields:
            if field:
                text = str(field)
                if text.lower() in {"none", "null", ""}:
                    continue
                return text

        if any(field is not None for field in error_fields):
            return "Unknown error"

        return json.dumps(response)

    return str(response)

# Initialize SQLite DB
def init_db():
    """Create all database tables."""
    with app.app_context():
        db.create_all()
        logger.info("Database tables initialized")

# Only run init_db, not migrations on startup
init_db()

# Save logs
def save_log(user_id, symbol, action, quantity, status, response):
    """Persist a trade log entry using SQLAlchemy."""
    log = TradeLog(
        timestamp=datetime.now().isoformat(),
        user_id=str(user_id),
        symbol=symbol,
        action=action,
        quantity=int(quantity),
        status=status,
        response=str(response)
    )
    db.session.add(log)
    db.session.commit()

def save_order_mapping(master_order_id, child_order_id, master_id, master_broker, child_id, child_broker, symbol):
    """Persist order mapping to the database instead of JSON."""
    mapping = OrderMapping(
        master_order_id=master_order_id,
        child_order_id=child_order_id,
        master_client_id=master_id,
        master_broker=master_broker,
        child_client_id=child_id,
        child_broker=child_broker,
        symbol=symbol,
        status="ACTIVE",
        timestamp=datetime.utcnow().isoformat()
    )
    db.session.add(mapping)
    db.session.commit()

def record_trade(user_email, symbol, action, qty, price, status):
    """Persist a trade record to the database."""
    user = User.query.filter_by(email=user_email).first()
    trade = Trade(
        user_id=user.id if user else None,
        symbol=symbol,
        action=action,
        qty=int(qty),
        price=float(price or 0),
        status=status,
        timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    )
    db.session.add(trade)
    db.session.commit()

def poll_and_copy_trades():
    """Run trade copying logic with full database storage - no JSON files."""
    with app.app_context():
        try:
            logger.info("🔄 Starting poll_and_copy_trades() cycle...")

            masters = Account.query.filter_by(role='master').limit(MAX_MASTERS_PER_CYCLE).all()
            if not masters:
                logger.debug("No master accounts configured")
                return

            logger.info(f"Found {len(masters)} master accounts to process")
            
            for master in masters:
                master_id = master.client_id
                if not master_id:
                    logger.error("Master account missing client_id, skipping...")
                    continue

                master_broker = (master.broker or "Unknown").lower()
                credentials = master.credentials or {}
                
                if not credentials:
                    logger.error(f"No credentials found for master {master_id}, skipping...")
                    continue

                # Initialize master broker API
                try:
                    BrokerClass = get_broker_class(master_broker)
                    if master_broker == "aliceblue":
                        api_key = credentials.get("api_key")
                        if not api_key:
                            logger.error(f"Missing API key for AliceBlue master {master_id}")
                            continue
                        rest = {k: v for k, v in credentials.items()
                                if k not in ("access_token", "api_key", "device_number")}
                        master_api = BrokerClass(
                            master.client_id,
                            api_key,
                            device_number=credentials.get("device_number") or master.device_number,
                            **rest
                        )
                    elif master_broker == "finvasia":
                        required = ["password", "totp_secret", "vendor_code", "api_key"]
                        if not all(credentials.get(r) for r in required):
                            logger.error(f"Missing credentials for finvasia master {master_id}")
                            continue
                        imei = credentials.get("imei") or "abc1234"
                        master_api = BrokerClass(
                            client_id=master.client_id,
                            password=credentials["password"],
                            totp_secret=credentials["totp_secret"],
                            vendor_code=credentials["vendor_code"],
                            api_key=credentials["api_key"],
                            imei=imei
                        )
                    else:
                        access_token = credentials.get("access_token")
                        if not access_token:
                            logger.error(f"Missing access token for {master_broker} master {master_id}")
                            continue
                        rest = {k: v for k, v in credentials.items() if k != "access_token"}
                        master_api = BrokerClass(
                            client_id=master.client_id,
                            access_token=access_token,
                            **rest
                        )
                except Exception as e:
                    logger.error(f"Failed to initialize master API ({master_broker}) for {master_id}: {str(e)}")
                    continue

                # Fetch orders from master account
                try:
                    if master_broker == "aliceblue" and hasattr(master_api, "get_trade_book"):
                        orders_resp = master_api.get_trade_book()
                    else:
                        orders_resp = master_api.get_order_list()
                    order_list = parse_order_list(orders_resp)
                    
                    if (
                        master_broker == "aliceblue"
                        and not order_list
                        and hasattr(master_api, "get_order_list")
                    ):
                        orders_resp = master_api.get_order_list()
                        order_list = parse_order_list(orders_resp)
                except Exception as e:
                    logger.error(f"Failed to fetch orders for master {master_id}: {str(e)}")
                    continue

                order_list = strip_emojis_from_obj(order_list or [])
                if not isinstance(order_list, list):
                    logger.error(f"Invalid order list type for master {master_id}: {type(order_list)}")
                    continue
                if not order_list:
                    logger.debug(f"No orders found for master {master_id}")
                    continue

                try:
                    order_list = sorted(order_list, key=get_order_sort_key, reverse=True)
                except Exception as e:
                    logger.error(f"Failed to sort orders for master {master_id}: {str(e)}")
                    continue

                children = Account.query.filter_by(
                    role='child',
                    linked_master_id=master_id,
                    copy_status='On'
                ).all()

                if not children:
                    logger.debug(f"No active children found for master {master_id}")
                    continue

                logger.info(f"Processing {len(children)} active children for master {master_id}")

                for child in children:
                    child_id = child.client_id
                    last_copied_trade_id = child.last_copied_trade_id
                    new_last_trade_id = None

                    if not last_copied_trade_id:
                        if order_list:
                            first = order_list[0]
                            init_id = (
                                first.get("orderId")
                                or first.get("order_id")
                                or first.get("id")
                                or first.get("NOrdNo")
                                or first.get("nestOrderNumber")
                                or first.get("orderNumber")
                                or first.get("Nstordno")
                                or first.get("norenordno")
                            )
                            if init_id:
                                child.last_copied_trade_id = str(init_id)
                                db.session.commit()
                                logger.info(f"Initialized marker for child {child_id} to {init_id}")
                        continue

                    logger.debug(f"Processing child {child_id}, last copied: {last_copied_trade_id}")

                    for order in order_list:
                        order_id = (
                            order.get("orderId")
                            or order.get("order_id")
                            or order.get("id")
                            or order.get("NOrdNo")
                            or order.get("nestOrderNumber")
                            or order.get("orderNumber")
                            or order.get("Nstordno")
                            or order.get("norenordno")
                        )
                        
                        if not order_id:
                            continue
                            
                        if str(order_id) == last_copied_trade_id:
                            logger.debug(f"[{master_id}->{child_id}] Reached last copied trade {order_id}")
                            break

                        if new_last_trade_id is None:
                            new_last_trade_id = str(order_id)

                        try:
                            filled_qty = int(
                                order.get("filledQuantity")
                                or order.get("filled_qty")
                                or order.get("filledQty")
                                or order.get("tradedQty")
                                or order.get("executed_qty")
                                or order.get("filled_quantity")
                                or order.get("executed_quantity")
                                or order.get("quantity")
                                or order.get("Fillshares")
                                or order.get("Filledqty")
                                or order.get("fillshares")
                                or 0
                            )

                            order_status_raw = (
                                order.get("orderStatus")
                                or order.get("status")
                                or order.get("Status")
                                or order.get("order_status")
                                or order.get("report_type")
                                or ("COMPLETE" if filled_qty > 0 else "")
                            )
                            
                            order_status = str(order_status_raw).upper() if order_status_raw else ""
                            status_mapping = {
                                "TRADED": "COMPLETE",
                                "FILLED": "COMPLETE",
                                "COMPLETE": "COMPLETE",
                                "COMPLETED": "COMPLETE",
                                "EXECUTED": "COMPLETE",
                                "FULL_EXECUTED": "COMPLETE",
                                "FULLY_EXECUTED": "COMPLETE",
                                "2": "COMPLETE",
                                "CONFIRMED": "COMPLETE",
                                "SUCCESS": "COMPLETE",
                                "REJECTED": "REJECTED",
                                "CANCELLED": "CANCELLED",
                                "FAILED": "FAILED"
                            }
                            status = status_mapping.get(order_status, "UNKNOWN")

                            if filled_qty <= 0:
                                if master_broker == "aliceblue" and status == "COMPLETE":
                                    filled_qty = int(
                                        order.get("placed_qty")
                                        or order.get("quantity")
                                        or 0
                                    )
                                    if filled_qty <= 0:
                                        continue
                                else:
                                    continue

                            if status != "COMPLETE":
                                continue

                        except Exception as e:
                            logger.debug(f"Failed to extract order data: {e}")
                            continue

                        try:
                            price = float(
                                order.get("price")
                                or order.get("orderPrice")
                                or order.get("avg_price")
                                or order.get("avgPrice")
                                or order.get("avgprc")
                                or order.get("tradePrice")
                                or order.get("tradedPrice")
                                or order.get("executedPrice")
                                or order.get("lastTradedPrice")
                                or 0
                            )
                            if price < 0:
                                continue

                            transaction_type_raw = (
                                order.get("transactionType")
                                or order.get("transaction_type")
                                or order.get("side")
                                or order.get("orderSide")
                                or order.get("buyOrSell")
                                or order.get("Trantype")
                                or "BUY"
                            )
                            
                            if isinstance(transaction_type_raw, (int, float)):
                                transaction_type_raw = (
                                    "BUY" if int(transaction_type_raw) in (1, 0) else "SELL"
                                )
                                
                            transaction_type_map = {
                                "B": "BUY",
                                "S": "SELL",
                                "BUY": "BUY",
                                "SELL": "SELL",
                                "1": "BUY",
                                "2": "SELL",
                                "0": "BUY",
                                "-1": "SELL",
                            }
                            transaction_type = transaction_type_map.get(
                                str(transaction_type_raw).upper(), "BUY"
                            )

                        except Exception as e:
                            logger.debug(f"Failed to extract price/transaction data: {e}")
                            continue

                        symbol = (
                            order.get("tradingSymbol")
                            or order.get("symbol")
                            or order.get("stock")
                            or order.get("scripCode")
                            or order.get("instrumentToken")
                            or order.get("Tsym")
                            or order.get("tsym")
                        )
                        if not symbol:
                            continue

                        try:
                            multiplier = float(child.multiplier or 1)
                            if multiplier <= 0:
                                continue
                            copied_qty = max(1, int(float(filled_qty) * multiplier))
                        except Exception as e:
                            logger.debug(f"Failed to calculate quantity: {e}")
                            continue

                        try:
                            child_broker = (child.broker or "Unknown").lower()
                            child_credentials = child.credentials or {}
                            ChildBrokerClass = get_broker_class(child_broker)
                            
                            if child_broker == "aliceblue":
                                api_key = child_credentials.get("api_key")
                                if not api_key:
                                    logger.warning(f"Missing API key for AliceBlue child {child_id}")
                                    continue
                                device_number = child_credentials.get("device_number") or child.device_number
                                rest_child = {
                                    k: v for k, v in child_credentials.items()
                                    if k not in ("access_token", "api_key", "device_number")
                                }
                                child_api = ChildBrokerClass(
                                    child.client_id, 
                                    api_key, 
                                    device_number=device_number, 
                                    **rest_child
                                )
                            elif child_broker == "finvasia":
                                required = ['password', 'totp_secret', 'vendor_code', 'api_key']
                                if not all(child_credentials.get(r) for r in required):
                                    logger.warning(f"Missing credentials for Finvasia child {child_id}")
                                    continue
                                imei = child_credentials.get('imei') or 'abc1234'
                                child_api = ChildBrokerClass(
                                    client_id=child.client_id,
                                    password=child_credentials['password'],
                                    totp_secret=child_credentials['totp_secret'],
                                    vendor_code=child_credentials['vendor_code'],
                                    api_key=child_credentials['api_key'],
                                    imei=imei
                                )
                            else:
                                access_token = child_credentials.get("access_token")
                                if not access_token:
                                    logger.warning(f"Missing access token for {child_broker} child {child_id}")
                                    continue
                                rest_child = {k: v for k, v in child_credentials.items() if k != "access_token"}
                                child_api = ChildBrokerClass(
                                    client_id=child.client_id,
                                    access_token=access_token,
                                    **rest_child
                                )
                        except Exception as e:
                            logger.error(f"Failed to initialize child API for {child_id}: {e}")
                            continue

                        try:
                            mapping_child = get_symbol_for_broker(symbol, child_broker)
                            if not mapping_child:
                                logger.warning(f"Symbol mapping not found for {symbol} on {child_broker}")
                                continue
                        except Exception as e:
                            logger.warning(f"Symbol mapping error for {symbol}: {e}")
                            continue

                        try:
                            if child_broker == "dhan":
                                security_id = (
                                    mapping_child.get("security_id")
                                    or mapping_child.get("securityId")
                                )
                                if not security_id:
                                    continue
                                order_params = {
                                    "tradingsymbol": symbol,
                                    "security_id": security_id,
                                    "exchange_segment": (
                                        mapping_child.get("exchange_segment")
                                        or getattr(child_api, "NSE", "NSE_EQ")
                                    ),
                                    "transaction_type": transaction_type,
                                    "quantity": copied_qty,
                                    "order_type": "MARKET",
                                    "product_type": getattr(child_api, "INTRA", "INTRADAY"),
                                    "price": price or 0,
                                }
                            elif child_broker == "aliceblue":
                                symbol_id = mapping_child.get("symbol_id")
                                if not symbol_id:
                                    continue
                                order_params = {
                                    "tradingsymbol": mapping_child.get("tradingsymbol", symbol),
                                    "symbol_id": symbol_id,
                                    "exchange": "NSE",
                                    "transaction_type": transaction_type,
                                    "quantity": copied_qty,
                                    "order_type": "MKT",
                                    "product": "MIS",
                                    "price": price or 0
                                }
                            elif child_broker == "finvasia":
                                token = mapping_child.get("token")
                                if not token:
                                    continue
                                order_params = {
                                    "tradingsymbol": mapping_child.get("symbol", symbol),
                                    "exchange": mapping_child.get("exchange", "NSE"),
                                    "transaction_type": transaction_type,
                                    "quantity": copied_qty,
                                    "order_type": "MKT",
                                    "product": "MIS",
                                    "price": price or 0,
                                    "token": token
                                }
                            else:
                                order_params = {
                                    "tradingsymbol": mapping_child.get("tradingsymbol", symbol),
                                    "exchange": "NSE",
                                    "transaction_type": transaction_type,
                                    "quantity": copied_qty,
                                    "order_type": "MARKET",
                                    "product": "MIS",
                                    "price": price or 0
                                }

                            logger.info(f"Placing {transaction_type} order for {copied_qty} {symbol} on {child_broker} for child {child_id}")
                            response = child_api.place_order(**order_params)

                        except Exception as e:
                            logger.error(f"Failed to place order for child {child_id}: {e}")
                            continue

                        try:
                            user_email = child.user.email if child.user else "unknown"
                            
                            if isinstance(response, dict):
                                if response.get("status") == "failure":
                                    error_msg = (
                                        response.get("error")
                                        or response.get("remarks")
                                        or response.get("message")
                                        or "Unknown error"
                                    )
                                    logger.warning(f"Order failed for child {child_id}: {error_msg}")
                                    
                                    save_log(
                                        child_id,
                                        symbol,
                                        transaction_type,
                                        copied_qty,
                                        "FAILED",
                                        error_msg
                                    )
                                    record_trade(
                                        user_email,
                                        symbol,
                                        transaction_type,
                                        copied_qty,
                                        price,
                                        'FAILED'
                                    )
                                else:
                                    order_id_child = (
                                        response.get("order_id")
                                        or response.get("orderId")
                                        or response.get("id")
                                        or response.get("nestOrderNumber")
                                        or response.get("orderNumber")
                                        or response.get("norenordno")
                                    )
                                    
                                    logger.info(f"Order successful for child {child_id}: {order_id_child}")
                                    
                                    save_log(
                                        child_id,
                                        symbol,
                                        transaction_type,
                                        copied_qty,
                                        "SUCCESS",
                                        str(response)
                                    )
                                    
                                    if order_id_child:
                                        save_order_mapping(
                                            master_order_id=str(order_id),
                                            child_order_id=str(order_id_child),
                                            master_id=master_id,
                                            master_broker=master_broker,
                                            child_id=child_id,
                                            child_broker=child_broker,
                                            symbol=symbol
                                        )
                                    
                                    record_trade(
                                        user_email,
                                        symbol,
                                        transaction_type,
                                        copied_qty,
                                        price,
                                        'SUCCESS'
                                    )
                            else:
                                logger.warning(f"Unexpected response format for child {child_id}: {type(response)}")
                                continue
                                
                        except Exception as e:
                            logger.error(f"Failed to handle response for child {child_id}: {e}")
                            continue

                    if new_last_trade_id:
                        child.last_copied_trade_id = new_last_trade_id
                        try:
                            db.session.commit()
                            logger.debug(f"Updated marker for child {child_id} to {new_last_trade_id}")
                        except Exception as e:
                            logger.error(f"Failed to update marker for child {child_id}: {e}")
                            db.session.rollback()

            logger.info("🏁 Poll and copy trades cycle completed")
        except Exception as e:
            logger.error(f"Error in poll_and_copy_trades: {str(e)}")

# Singleton scheduler to prevent multiple instances
_scheduler = None

def start_scheduler():
    """Initialize and start the background scheduler for trade copying."""
    global _scheduler
    
    # Only start in production or if explicitly enabled
    if ENVIRONMENT != "production" and os.environ.get("ENABLE_SCHEDULER") != "true":
        logger.info("Scheduler disabled in development mode")
        return None
        
    if _scheduler is not None:
        logger.info("Scheduler already running")
        return _scheduler
        
    try:
        _scheduler = BackgroundScheduler(
            timezone=datetime.now().astimezone().tzinfo
        )
        
        _scheduler.add_job(
            func=poll_and_copy_trades,
            trigger="interval",
            seconds=POLLING_INTERVAL,
            id="copy_trades",
            name="Trade Copy Job",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30
        )
        
        _scheduler.start()
        logger.info(f"✅ Background copy trader scheduler started (interval: {POLLING_INTERVAL}s)")
        
        return _scheduler
        
    except Exception as e:
        logger.error(f"Failed to start scheduler: {str(e)}")
        return None

# Request tracking middleware
@app.before_request
def before_request_tracking():
    request.request_id = str(uuid.uuid4())[:8]
    request.start_time = time.time()
    
    logger.info(
        f"REQUEST START: {request.method} {request.path} "
        f"from {request.remote_addr} "
        f"User-Agent: {request.headers.get('User-Agent', 'Unknown')[:50]}"
    )

@app.after_request
def after_request_tracking(response):
    if hasattr(request, 'start_time') and hasattr(request, 'request_id'):
        duration = time.time() - request.start_time
        
        logger.info(
            f"REQUEST END: {request.method} {request.path} "
            f"Status: {response.status_code} "
            f"Duration: {duration:.3f}s "
            f"Size: {response.content_length or 0} bytes"
        )
        
        response.headers['X-Request-ID'] = request.request_id
        response.headers['X-Response-Time'] = f"{duration * 1000:.1f}ms"
    
    return response

# Request tracking for graceful shutdown
@app.before_request
def before_request_shutdown_tracking():
    if shutdown_handler.is_shutdown_requested():
        return jsonify({
            "error": "Server is shutting down",
            "message": "Please try again later"
        }), 503
    
    shutdown_handler.increment_active_requests()

@app.after_request
def after_request_shutdown_tracking(response):
    shutdown_handler.decrement_active_requests()
    return response

# Health Check Endpoint
@app.route('/health')
def health_check():
    """Health check endpoint for monitoring"""
    try:
        db.session.execute('SELECT 1')
        scheduler_status = "running" if _scheduler and _scheduler.running else "stopped"
        
        return jsonify({
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "uptime": format_uptime(),
            "database": "connected",
            "scheduler": scheduler_status,
            "environment": ENVIRONMENT
        }), 200
    except Exception as e:
        return safe_json_response({
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }, 503)

@app.route("/connect-zerodha", methods=["POST"])
# @limiter.limit("5 per minute")
def connect_zerodha():
    """Connect a Zerodha account using provided credentials."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        required_fields = ["client_id", "api_key", "api_secret", "request_token"]
        missing_fields = [field for field in required_fields if not data.get(field)]
        
        if missing_fields:
            return jsonify({
                "error": f"Missing required fields: {', '.join(missing_fields)}"
            }), 400
            
        if not validate_client_id(data["client_id"]):
            return jsonify({"error": "Invalid client_id format"}), 400
            
        try:
            broker = ZerodhaBroker(
                client_id=data["client_id"],
                api_key=data["api_key"],
                api_secret=data["api_secret"],
                request_token=data["request_token"]
            )
        except Exception as e:
            logger.error(f"Failed to initialize Zerodha broker: {str(e)}")
            return safe_json_response({
                "error": "Failed to initialize broker connection",
                "details": str(e)
            }, 500)
            
        if not broker.access_token:
            return jsonify({
                "error": "Failed to generate access token"
            }), 500
            
        logger.info(f"Successfully connected Zerodha account for {data['client_id']}")
        
        return jsonify({
            "status": "success",
            "access_token": broker.access_token
        })
        
    except Exception as e:
        logger.error(f"Error in connect_zerodha: {str(e)}")
        return safe_json_response({
            "error": "Internal server error",
            "details": str(e)
        }, 500)

@app.route('/api/order-book/<client_id>', methods=['GET'])
# @limiter.limit("30 per minute")
@require_user
def get_order_book(client_id):
    """Get order book for a master account."""
    logger.info(f"Fetching order book for client {client_id}")
    
    try:
        if not validate_client_id(client_id):
            return jsonify({"error": "Invalid client_id format"}), 400
            
        master_account = Account.query.filter_by(
            user_id=request.current_user.id,
            client_id=client_id, 
            role='master'
        ).first()
        
        if not master_account:
            logger.warning(f"Master account not found for client {client_id}")
            return jsonify({
                "error": "Master account not found"
            }), 404

        master = _account_to_dict(master_account)

        try:
            api = broker_api(master)
        except Exception as e:
            logger.error(f"Failed to initialize broker API: {str(e)}")
            return safe_json_response({
                "error": "Failed to initialize broker connection",
                "details": str(e)
            }, 500)

        try:
            broker_name = master.get('broker', '').lower()
            
            if broker_name == "aliceblue" and hasattr(api, "get_trade_book"):
                orders_resp = api.get_trade_book()
            else:
                orders_resp = api.get_order_list()
                
            if isinstance(orders_resp, dict) and orders_resp.get("status") == "failure":
                error_msg = orders_resp.get("error", "Failed to fetch orders")
                logger.error(f"API error: {error_msg}")
                return jsonify({"error": error_msg}), 500
                
            orders = parse_order_list(orders_resp)
            
        except Exception as e:
            logger.error(f"Failed to fetch orders: {str(e)}")
            return safe_json_response({
                "error": "Failed to fetch orders",
                "details": str(e)
            }, 500)
            
        orders = strip_emojis_from_obj(orders)
        
        if not isinstance(orders, list):
            orders = []
        
        formatted = []
        for order in orders:
            if not isinstance(order, dict):
                continue
                
            try:
                order_id = (
                    order.get("orderId") or order.get("order_id") or order.get("id") or
                    order.get("orderNumber") or order.get("NOrdNo") or order.get("Nstordno") or
                    order.get("nestOrderNumber") or order.get("ExchOrdID") or order.get("norenordno") or "N/A"
                )

                side = order.get("transactionType") or order.get("side") or order.get("Trantype") or "N/A"
                
                status_raw = (
                    order.get("orderStatus") or order.get("report_type") or order.get("status") or
                    order.get("Status") or ("FILLED" if order.get("tradedQty") else "PENDING")
                )
                status = str(status_raw).upper() if status_raw else "UNKNOWN"
                
                symbol = (
                    order.get("tradingSymbol") or order.get("symbol") or order.get("Tsym") or 
                    order.get("tsym") or order.get("Trsym") or "—"
                )

                product_type = (
                    order.get("productType") or order.get("product") or order.get("Pcode") or order.get("prd") or "—"
                )

                try:
                    placed_qty = int(order.get("orderQuantity") or order.get("qty") or order.get("Qty") or order.get("quantity") or 0)
                except (TypeError, ValueError):
                    placed_qty = 0

                try:
                    filled_qty = int(
                        order.get("filledQuantity") or order.get("filled_qty") or order.get("filledQty") or
                        order.get("Filledqty") or order.get("Fillshares") or order.get("fillshares") or
                        order.get("tradedQty") or order.get("executedQty") or 
                        (placed_qty if status in ["FILLED", "COMPLETE", "TRADED"] else 0)
                    )
                except (TypeError, ValueError):
                    filled_qty = 0

                try:
                    avg_price = float(
                        order.get("averagePrice") or order.get("avg_price") or order.get("Avgprc") or
                        order.get("avgprc") or order.get("Prc") or order.get("tradePrice") or
                        order.get("tradedPrice") or order.get("executedPrice") or 0
                    )
                except (TypeError, ValueError):
                    avg_price = 0.0

                order_time_raw = (
                    order.get("orderTimestamp") or order.get("order_time") or order.get("create_time") or
                    order.get("orderDateTime") or order.get("ExchConfrmtime") or order.get("norentm") or
                    order.get("exchtime") or ""
                )
                order_time = str(order_time_raw).replace("T", " ").split(".")[0] if order_time_raw else "—"

                remarks = (
                    order.get("remarks") or order.get("Remark") or order.get("orderTag") or
                    order.get("usercomment") or order.get("Usercomments") or order.get("remarks1") or
                    order.get("rejreason") or "—"
                )

                formatted_order = {
                    "order_id": str(order_id),
                    "side": str(side).upper(),
                    "status": status,
                    "symbol": str(symbol),
                    "product_type": str(product_type),
                    "placed_qty": placed_qty,
                    "filled_qty": filled_qty,
                    "avg_price": round(avg_price, 2),
                    "order_time": order_time,
                    "remarks": str(remarks)[:100]
                }
                
                formatted.append(formatted_order)
                
            except Exception as e:
                logger.error(f"Error formatting order {order.get('orderId', 'unknown')}: {str(e)}")
                continue

        try:
            formatted.sort(key=lambda x: x.get('order_time', ''), reverse=True)
        except Exception as e:
            logger.warning(f"Failed to sort orders: {str(e)}")

        logger.info(f"Successfully formatted {len(formatted)} orders for client {client_id}")
        
        return jsonify(formatted), 200

    except Exception as e:
        logger.error(f"Unexpected error in get_order_book: {str(e)}")
        return safe_json_response({
            "error": "Internal server error",
            "details": str(e)
        }, 500)

@app.route("/zerodha_redirects/<client_id>")
def zerodha_redirect_handler(client_id):
    """Handle OAuth redirect from Zerodha login flow."""
    logger.info(f"Processing Zerodha redirect for client {client_id}")
    
    try:
        if not validate_client_id(client_id):
            return render_template(
                "error.html",
                error="Invalid client ID",
                message="The client ID format is invalid."
            ), 400
            
        request_token = request.args.get("request_token")
        if not request_token:
            logger.error("No request token received in redirect")
            return render_template(
                "error.html",
                error="Missing request token",
                message="The authentication process was incomplete."
            ), 400

        try:
            pending = get_pending_zerodha()
        except Exception as e:
            logger.error(f"Failed to load pending auth data: {str(e)}")
            return render_template(
                "error.html",
                error="Authentication Error",
                message="Failed to load authentication data."
            ), 500

        cred = pending.pop(client_id, None)
        if not cred:
            logger.error(f"No pending auth found for client {client_id}")
            return render_template(
                "error.html",
                error="Invalid Request",
                message="No pending authentication found for this client."
            ), 400

        api_key = cred.get("api_key")
        api_secret = cred.get("api_secret")
        if not api_key or not api_secret:
            logger.error("Missing API credentials in pending auth")
            return render_template(
                "error.html",
                error="Invalid Credentials",
                message="Missing required API credentials."
            ), 400

        username = cred.get("username") or client_id
        
        try:
            from kiteconnect import KiteConnect
            kite = KiteConnect(api_key=api_key)
            
            session_data = kite.generate_session(request_token, api_secret)
            if not session_data or "access_token" not in session_data:
                raise ValueError("Failed to generate valid session")
                
            access_token = session_data["access_token"]
            
            account = {
                "broker": "zerodha",
                "client_id": client_id,
                "username": username,
                "credentials": {
                    "access_token": access_token,
                    "api_key": api_key,
                    "api_secret": api_secret,
                },
                "status": "Connected",
                "auto_login": True,
                "last_login": datetime.now().isoformat(),
                "role": None,
                "linked_master_id": None,
                "multiplier": 1,
                "copy_status": "Off",
            }

            try:
                save_account_to_user(cred.get("owner", username), account)
                set_pending_zerodha(pending)
                logger.info(f"Successfully connected Zerodha account for {client_id}")
                flash("Zerodha account connected successfully!", "success")
                
                return redirect(url_for("AddAccount"))
                
            except Exception as e:
                logger.error(f"Failed to save account data: {str(e)}")
                return render_template(
                    "error.html",
                    error="Save Error",
                    message="Failed to save account data."
                ), 500

        except Exception as e:
            logger.error(f"Error in Zerodha session generation: {str(e)}")
            return render_template(
                "error.html",
                error="Authentication Failed",
                message=f"Failed to authenticate with Zerodha: {str(e)}"
            ), 500

    except Exception as e:
        logger.error(f"Unexpected error in Zerodha redirect handler: {str(e)}")
        return render_template(
            "error.html",
            error="Server Error",
            message="An unexpected error occurred."
        ), 500

@app.route("/webhook/<user_id>", methods=["POST"])
# @limiter.limit("60 per minute")
def webhook(user_id):
    """Handle incoming webhook requests for order placement."""
    logger.info(f"Received webhook request for user {user_id}")
    
    try:
        try:
            data = request.get_json(force=True)
        except Exception as e:
            logger.error(f"Failed to parse JSON data: {str(e)}")
            return jsonify({
                "error": "Invalid JSON data",
                "details": str(e)
            }), 400

        if isinstance(data, str) or "message" in data:
            message = data if isinstance(data, str) else data.get("message")
            logger.info(f"Alert received for {user_id}: {message}")
            return jsonify({
                "status": "Alert logged",
                "message": message
            }), 200

        symbol = data.get("symbol")
        action = data.get("action")
        quantity = data.get("quantity")

        if not all([symbol, action, quantity]):
            logger.error(f"Missing required fields: {data}")
            return jsonify({
                "error": "Missing required fields",
                "required": ["symbol", "action", "quantity"],
                "received": {
                    "symbol": bool(symbol),
                    "action": bool(action),
                    "quantity": bool(quantity)
                }
            }), 400

        user = get_user_credentials(user_id)
        if not user:
            logger.error(f"Invalid webhook ID: {user_id}")
            return jsonify({"error": "Invalid webhook ID"}), 403

        broker_name = user.get("broker", "dhan").lower()
        client_id = user.get("client_id")
        access_token = user.get("access_token")

        try:
            BrokerClass = get_broker_class(broker_name)
            broker_api_instance = BrokerClass(client_id, access_token)
        except Exception as e:
            logger.error(f"Failed to initialize broker {broker_name}: {str(e)}")
            return safe_json_response({
                "error": "Failed to initialize broker",
                "details": str(e)
            }, 500)

        try:
            mapping = get_symbol_for_broker(symbol, broker_name)
            
            if broker_name == "dhan":
                security_id = mapping.get("security_id")
                if not security_id:
                    logger.error(f"Symbol not found in mapping: {symbol}")
                    return jsonify({
                        "error": f"Symbol '{symbol}' not found in symbol map"
                    }), 400
                    
                order_params = {
                    "tradingsymbol": symbol,
                    "security_id": security_id,
                    "exchange_segment": broker_api_instance.NSE,
                    "transaction_type": (
                        broker_api_instance.BUY if action.upper() == "BUY" 
                        else broker_api_instance.SELL
                    ),
                    "quantity": int(quantity),
                    "order_type": broker_api_instance.MARKET,
                    "product_type": broker_api_instance.INTRA,
                    "price": 0
                }
                
            elif broker_name == "zerodha":
                tradingsymbol = mapping.get("tradingsymbol", symbol)
                order_params = {
                    "tradingsymbol": tradingsymbol,
                    "exchange": "NSE",
                    "transaction_type": action.upper(),
                    "quantity": int(quantity),
                    "order_type": "MARKET",
                    "product": "MIS",
                    "price": None
                }
                
            else:
                logger.error(f"Unsupported broker: {broker_name}")
                return jsonify({
                    "error": f"Broker '{broker_name}' not supported"
                }), 400
                
        except Exception as e:
            logger.error(f"Error building order parameters: {str(e)}")
            return safe_json_response({
                "error": "Failed to build order parameters",
                "details": str(e)
            }, 500)

        try:
            logger.info(f"Placing {action} order for {quantity} {symbol} via {broker_name}")
            
            response = broker_api_instance.place_order(**order_params)
            
            if isinstance(response, dict) and response.get("status") == "failure":
                status = "FAILED"
                reason = (
                    response.get("remarks") or response.get("error_message") or
                    response.get("errorMessage") or response.get("error") or "Unknown error"
                )
                
                logger.error(f"Order failed: {reason}")
                record_trade(user_id, symbol, action.upper(), quantity, order_params.get('price'), status)
                
                return jsonify({
                    "status": status,
                    "reason": reason
                }), 400

            status = "SUCCESS"
            success_msg = response.get("remarks", "Trade placed successfully")
            
            logger.info(f"Order placed successfully: {success_msg}")
            
            record_trade(user_id, symbol, action.upper(), quantity, order_params.get('price'), status)
            
            try:
                poll_and_copy_trades()
            except Exception as e:
                logger.error(f"Failed to trigger copy trading: {str(e)}")
            
            return jsonify({
                "status": status,
                "result": success_msg,
                "order_id": response.get("order_id")
            }), 200

        except Exception as e:
            logger.error(f"Error placing order: {str(e)}")
            return safe_json_response({
                "error": "Failed to place order",
                "details": str(e)
            }, 500)

    except Exception as e:
        logger.error(f"Unexpected error in webhook: {str(e)}")
        return safe_json_response({
            "error": "Internal server error",
            "details": str(e)
        }, 500)

@app.route('/api/master-squareoff', methods=['POST'])
# @limiter.limit("10 per minute")
@require_user
def master_squareoff():
    """Square off child orders for a master order."""
    logger.info("Processing master square-off request")
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        master_order_id = data.get("master_order_id")
        if not master_order_id:
            return jsonify({"error": "Missing master_order_id"}), 400

        active_mappings = db.session.query(OrderMapping).join(
            Account, Account.client_id == OrderMapping.child_client_id
        ).filter(
            OrderMapping.master_order_id == master_order_id,
            OrderMapping.status == "ACTIVE",
            Account.user_id == request.current_user.id
        ).all()
        
        if not active_mappings:
            return jsonify({
                "message": "No active child orders found for this master order",
                "master_order_id": master_order_id,
                "active_mappings": 0,
                "results": []
            }), 200

        child_mappings = {}
        for mapping in active_mappings:
            child_id = mapping.child_client_id
            if child_id not in child_mappings:
                child_mappings[child_id] = []
            child_mappings[child_id].append(mapping)

        results = []
        successful_squareoffs = 0
        failed_squareoffs = 0

        child_ids = list(child_mappings.keys())
        child_accounts = {a.client_id: a for a in Account.query.filter(
            Account.client_id.in_(child_ids),
            Account.user_id == request.current_user.id
        ).all()}

        for child_id, mappings in child_mappings.items():
            child_account = child_accounts.get(child_id)
            
            if not child_account:
                for mapping in mappings:
                    results.append({
                        "child_client_id": child_id,
                        "symbol": mapping.symbol,
                        "status": "ERROR",
                        "message": "Child account not found"
                    })
                    failed_squareoffs += 1
                continue

            try:
                child_dict = _account_to_dict(child_account)
                broker_api_instance = broker_api(child_dict)
                
                try:
                    positions_response = broker_api_instance.get_positions()
                    
                    if isinstance(positions_response, dict):
                        positions = (
                            positions_response.get("data", []) or 
                            positions_response.get("positions", []) or
                            positions_response.get("net", [])
                        )
                    else:
                        positions = positions_response or []
                    
                except Exception as e:
                    for mapping in mappings:
                        results.append({
                            "child_client_id": child_id,
                            "symbol": mapping.symbol,
                            "status": "ERROR",
                            "message": f"Failed to fetch positions: {str(e)}"
                        })
                        failed_squareoffs += 1
                    continue

                for mapping in mappings:
                    symbol = mapping.symbol
                    
                    matching_position = None
                    for position in positions:
                        pos_symbol = (
                            position.get("tradingSymbol") or position.get("symbol") or
                            position.get("tsym") or position.get("Tsym") or ""
                        ).upper()
                        
                        if pos_symbol == symbol.upper():
                            net_qty = int(
                                position.get("netQty") or position.get("net_quantity") or
                                position.get("netQuantity") or position.get("Netqty") or 0
                            )
                            
                            if net_qty != 0:
                                matching_position = position
                                break

                    if not matching_position:
                        results.append({
                            "child_client_id": child_id,
                            "symbol": symbol,
                            "status": "SKIPPED",
                            "message": f"No open position found for {symbol}"
                        })
                        continue

                    net_qty = int(
                        matching_position.get("netQty") or matching_position.get("net_quantity") or
                        matching_position.get("netQuantity") or matching_position.get("Netqty") or 0
                    )
                    
                    direction = "SELL" if net_qty > 0 else "BUY"
                    abs_qty = abs(net_qty)
                    
                    broker_name = child_account.broker.lower()
                    
                    if broker_name == "dhan":
                        order_params = {
                            "tradingsymbol": symbol,
                            "security_id": matching_position.get("securityId") or matching_position.get("security_id"),
                            "exchange_segment": matching_position.get("exchangeSegment") or "NSE_EQ",
                            "transaction_type": direction,
                            "quantity": abs_qty,
                            "order_type": "MARKET",
                            "product_type": "INTRADAY",
                            "price": 0
                        }
                    elif broker_name == "aliceblue":
                        order_params = {
                            "tradingsymbol": symbol,
                            "symbol_id": matching_position.get("securityId") or matching_position.get("security_id"),
                            "exchange": "NSE",
                            "transaction_type": direction,
                            "quantity": abs_qty,
                            "order_type": "MKT",
                            "product": "MIS",
                            "price": 0
                        }
                    elif broker_name == "finvasia":
                        order_params = {
                            "tradingsymbol": symbol,
                            "exchange": "NSE",
                            "transaction_type": direction,
                            "quantity": abs_qty,
                            "order_type": "MKT",
                            "product": "MIS",
                            "price": 0,
                            "token": matching_position.get("token", "")
                        }
                    else:
                        order_params = {
                            "tradingsymbol": symbol,
                            "exchange": "NSE",
                            "transaction_type": direction,
                            "quantity": abs_qty,
                            "order_type": "MARKET",
                            "product": "MIS",
                            "price": 0
                        }

                    try:
                        square_off_response = broker_api_instance.place_order(**order_params)
                        
                        if isinstance(square_off_response, dict) and square_off_response.get("status") == "failure":
                            error_msg = (
                                square_off_response.get("remarks") or square_off_response.get("error") or
                                square_off_response.get("message") or "Unknown square-off error"
                            )
                            
                            results.append({
                                "child_client_id": child_id,
                                "symbol": symbol,
                                "status": "FAILED",
                                "message": f"Square-off failed: {error_msg}"
                            })
                            failed_squareoffs += 1
                            
                        else:
                            mapping.status = "SQUARED_OFF"
                            mapping.remarks = f"Squared off on {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}"
                            
                            results.append({
                                "child_client_id": child_id,
                                "symbol": symbol,
                                "status": "SUCCESS",
                                "message": "Square-off completed successfully"
                            })
                            successful_squareoffs += 1
                            
                    except Exception as e:
                        results.append({
                            "child_client_id": child_id,
                            "symbol": symbol,
                            "status": "ERROR",
                            "message": f"Order placement failed: {str(e)}"
                        })
                        failed_squareoffs += 1

            except Exception as e:
                for mapping in mappings:
                    results.append({
                        "child_client_id": child_id,
                        "symbol": mapping.symbol,
                        "status": "ERROR",
                        "message": f"Child processing failed: {str(e)}"
                    })
                    failed_squareoffs += 1

        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            return safe_json_response({
                "error": "Failed to update mapping statuses",
                "details": str(e)
            }, 500)

        response_data = {
            "message": f"Master square-off completed for order {master_order_id}",
            "master_order_id": master_order_id,
            "summary": {
                "total_mappings": len(active_mappings),
                "successful_squareoffs": successful_squareoffs,
                "failed_squareoffs": failed_squareoffs
            },
            "results": results
        }

        if failed_squareoffs == 0:
            return jsonify(response_data), 200
        elif successful_squareoffs > 0:
            return jsonify(response_data), 207
        else:
            return jsonify(response_data), 500

    except Exception as e:
        return safe_json_response({
            "error": "Internal server error",
            "details": str(e)
        }, 500)

@app.route('/api/master-orders', methods=['GET'])
# @limiter.limit("60 per minute")
@require_user
def get_master_orders():
    """Get all master orders with their child order details."""
    try:
        master_id_filter = request.args.get("master_id")
        status_filter = request.args.get("status", "").upper()
        
        query = db.session.query(OrderMapping).join(
            Account, Account.client_id == OrderMapping.master_client_id
        ).filter(Account.user_id == request.current_user.id)
        
        if master_id_filter:
            query = query.filter(OrderMapping.master_client_id == master_id_filter)
        if status_filter:
            query = query.filter(OrderMapping.status == status_filter)
            
        mappings = query.all()

        master_summary = {}
        for entry in mappings:
            mid = entry.master_order_id
            if mid not in master_summary:
                master_summary[mid] = {
                    "master_order_id": mid,
                    "symbol": entry.symbol,
                    "master_client_id": entry.master_client_id,
                    "master_broker": entry.master_broker or "Unknown",
                    "status": 'ACTIVE',
                    "children": [],
                    "timestamp": entry.timestamp or ''
                }
            child = {
                'child_client_id': entry.child_client_id,
                'child_broker': entry.child_broker or 'Unknown',
                'status': entry.status,
                'order_id': entry.child_order_id or '',
                'timestamp': entry.child_timestamp or '',
                'remarks': entry.remarks or '—'
            }
            master_summary[mid]['children'].append(child)
                
        orders = list(master_summary.values())
        orders.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        
        return jsonify({'orders': orders}), 200

    except Exception as e:
        return safe_json_response({
            "error": "Internal server error",
            "details": str(e)
        }, 500)

@app.route("/api/zerodha-login-url")
def zerodha_login_url_route():
    api_key = request.args.get("api_key")
    if not api_key:
        return jsonify({"error": "api_key required"}), 400
    try:
        kite = KiteConnect(api_key=api_key)
        return jsonify({"login_url": kite.login_url()})
    except:
        return jsonify({"error": "kiteconnect not available"}), 500

@app.route('/api/init-zerodha-login', methods=['POST'])
# @limiter.limit("10 per minute")
@require_user
def init_zerodha_login():
    data = request.json
    client_id = data.get('client_id')
    api_key = data.get('api_key')
    api_secret = data.get('api_secret')
    username = data.get('username')
    
    if not all([client_id, api_key, api_secret, username]):
        return jsonify({'error': 'Missing fields'}), 400

    if not validate_client_id(client_id):
        return jsonify({'error': 'Invalid client_id format'}), 400

    pending = get_pending_zerodha()

    pending[client_id] = {
        'api_key': api_key,
        'api_secret': api_secret,
        'username': username,
        'owner': request.current_user.email
    }
    set_pending_zerodha(pending)

    redirect_uri = f"https://dhan-trading.onrender.com/zerodha_redirects/{client_id}"
    login_url = f"https://kite.zerodha.com/connect/login?api_key={api_key}&v=3&redirect_uri={quote(redirect_uri, safe='')}"
    return jsonify({'login_url': login_url})

@app.route('/api/init-fyers-login', methods=['POST'])
# @limiter.limit("10 per minute")
@require_user
def init_fyers_login():
    """Start the Fyers OAuth login flow and return the login URL."""
    data = request.json
    client_id = data.get('client_id')
    secret_key = data.get('secret_key')
    username = data.get('username')
    
    if not all([client_id, secret_key, username]):
        return jsonify({'error': 'Missing fields'}), 400

    if not validate_client_id(client_id):
        return jsonify({'error': 'Invalid client_id format'}), 400

    pending = get_pending_fyers()

    state = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    redirect_uri = f"https://dhan-trading.onrender.com/fyers_redirects/{client_id}"

    pending[client_id] = {
        'secret_key': secret_key,
        'redirect_uri': redirect_uri,
        'state': state,
        'username': username,
        'owner': request.current_user.email
    }
    set_pending_fyers(pending)

    login_url = FyersBroker.login_url(client_id, redirect_uri, state)
    return jsonify({'login_url': login_url})

@app.route('/fyers_redirects/<client_id>')
def fyers_redirect_handler(client_id):
    """Handle redirect from Fyers OAuth flow and store tokens."""
    auth_code = request.args.get('auth_code')
    state = request.args.get('state')
    
    if not auth_code:
        return "❌ No auth_code received", 400

    if not validate_client_id(client_id):
        return "❌ Invalid client_id format", 400

    pending = get_pending_fyers()

    cred = pending.pop(client_id, None)
    if not cred or cred.get('state') != state:
        return "❌ No pending auth for this client", 400

    secret_key = cred.get('secret_key')
    username = cred.get('username') or client_id

    token_resp = FyersBroker.exchange_code_for_token(client_id, secret_key, auth_code)
    if token_resp.get('s') != 'ok':
        msg = token_resp.get('message', 'Failed to generate token')
        return f"❌ Error: {msg}", 500

    access_token = token_resp.get('access_token')
    refresh_token = token_resp.get('refresh_token')

    account = {
        'broker': 'fyers',
        'client_id': client_id,
        'username': username,
        'credentials': {
            'access_token': access_token,
            'refresh_token': refresh_token,
            'secret_key': secret_key,
        },
        'status': 'Connected',
        'auto_login': True,
        'last_login': datetime.now().isoformat(),
        'role': None,
        'linked_master_id': None,
        'multiplier': 1,
        'copy_status': 'Off',
    }

    save_account_to_user(cred.get('owner', username), account)
    set_pending_fyers(pending)
    return redirect(url_for('AddAccount'))

@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        email = request.form["email"]
        password = request.form["password"]
        
        if User.query.filter_by(email=email).first():
            flash("Email already exists", "error")
            return render_template("register.html")
        
        webhook_token = ''.join(random.choices(string.ascii_letters + string.digits, k=32))
        
        user = User(
            email=email,
            password_hash=generate_password_hash(password),
            webhook_token=webhook_token
        )
        db.session.add(user)
        db.session.commit()
        
        flash("Registration successful! Please log in.", "success")
        return redirect(url_for("login"))
    
    return render_template("register.html")

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form["email"]
        password = request.form["password"]
        
        user = User.query.filter_by(email=email).first()
        
        if user and check_password_hash(user.password_hash, password):
            session["user"] = email
            flash("Login successful!", "success")
            return redirect(url_for("Dashboard"))
        else:
            flash("Invalid email or password", "error")
    
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    flash("You have been logged out", "info")
    return redirect(url_for("login"))

@app.route("/")
@login_required
def Dashboard():
    return render_template("dashboard.html")

@app.route("/add-account")
@login_required
def AddAccount():
    return render_template("add_account.html")

@app.route("/manage-accounts")
@login_required
def ManageAccounts():
    return render_template("manage_accounts.html")

@app.route("/trading-dashboard")
@login_required
def TradingDashboard():
    return render_template("trading_dashboard.html")

@app.route("/order-history")
@login_required
def OrderHistory():
    return render_template("order_history.html")

@app.route("/settings")
@login_required
def UserSettings():
    return render_template("settings.html")

@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if request.method == "POST":
        email = request.form["email"]
        password = request.form["password"]
        
        if email == ADMIN_EMAIL and password == ADMIN_PASSWORD:
            session["admin"] = True
            flash("Admin login successful!", "success")
            return redirect(url_for("admin_dashboard"))
        else:
            flash("Invalid admin credentials", "error")
    
    return render_template("admin/login.html")

@app.route("/admin/logout")
def admin_logout():
    session.pop("admin", None)
    flash("Admin logged out", "info")
    return redirect(url_for("admin_login"))

@app.route("/admin")
@admin_login_required
def admin_dashboard():
    users = load_users()
    accounts = load_accounts()
    trades = load_trades()
    webhook_logs, system_logs = load_logs()
    settings = load_settings()
    
    return render_template("admin/dashboard.html",
                         users=users,
                         accounts=accounts,
                         trades=trades,
                         webhook_logs=webhook_logs,
                         system_logs=system_logs,
                         settings=settings)

@app.route("/admin/users")
@admin_login_required
def admin_users():
    users = load_users()
    return render_template("admin/users.html", users=users)

@app.route("/admin/accounts")
@admin_login_required
def admin_accounts():
    accounts = load_accounts()
    return render_template("admin/accounts.html", accounts=accounts)

@app.route("/admin/trades")
@admin_login_required
def admin_trades():
    trades = load_trades()
    return render_template("admin/trades.html", trades=trades)

@app.route("/admin/logs")
@admin_login_required
def admin_logs():
    webhook_logs, system_logs = load_logs()
    return render_template("admin/logs.html", 
                         webhook_logs=webhook_logs, 
                         system_logs=system_logs)

@app.route("/admin/settings", methods=["GET", "POST"])
@admin_login_required
def admin_settings():
    if request.method == "POST":
        settings = {
            "trading_enabled": request.form.get("trading_enabled") == "on",
            "polling_interval": request.form.get("polling_interval", POLLING_INTERVAL),
            "max_masters": request.form.get("max_masters", MAX_MASTERS_PER_CYCLE),
            "log_level": request.form.get("log_level", LOG_LEVEL)
        }
        save_settings(settings)
        flash("Settings updated successfully!", "success")
        return redirect(url_for("admin_settings"))
    
    settings = load_settings()
    return render_template("admin/settings.html", settings=settings)

@app.route('/api/accounts', methods=['GET'])
# @limiter.limit("60 per minute")
@require_user
def get_user_accounts():
    """Get all accounts for the authenticated user with their balance information."""
    try:
        accounts = Account.query.filter_by(user_id=request.current_user.id).all()
        
        result = []
        for account in accounts:
            account_dict = _account_to_dict(account)
            
            if account.status == "Connected":
                try:
                    balance = get_opening_balance_for_account(account_dict)
                    account_dict["balance"] = balance
                except Exception as e:
                    logger.warning(f"Failed to fetch balance for {account.client_id}: {str(e)}")
                    account_dict["balance"] = None
            else:
                account_dict["balance"] = None
            
            result.append(account_dict)
        
        return jsonify(result), 200
        
    except Exception as e:
        return safe_json_response({
            "error": "Failed to fetch accounts",
            "details": str(e)
        }, 500)

@app.route('/api/accounts', methods=['POST'])
# @limiter.limit("10 per minute")
@require_user
def create_account():
    """Create a new account for the authenticated user."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        required_fields = ["broker", "client_id", "username"]
        missing_fields = [field for field in required_fields if not data.get(field)]
        
        if missing_fields:
            return jsonify({
                "error": f"Missing required fields: {', '.join(missing_fields)}"
            }), 400
        
        if not validate_client_id(data["client_id"]):
            return jsonify({"error": "Invalid client_id format"}), 400
        
        if not validate_broker(data["broker"]):
            return jsonify({"error": "Invalid broker format"}), 400
        
        existing = Account.query.filter_by(
            user_id=request.current_user.id,
            client_id=data["client_id"]
        ).first()
        
        if existing:
            return jsonify({
                "error": "Account with this client_id already exists",
                "existing_account": _account_to_dict(existing)
            }), 409
        
        account = Account(
            user_id=request.current_user.id,
            broker=data["broker"].lower(),
            client_id=data["client_id"],
            username=data["username"],
            status=data.get("status", "Pending"),
            role=data.get("role"),
            linked_master_id=data.get("linked_master_id"),
            copy_status=data.get("copy_status", "Off"),
            multiplier=float(data.get("multiplier", 1.0)),
            credentials=data.get("credentials", {}),
            auto_login=data.get("auto_login", True),
            device_number=data.get("device_number")
        )
        
        db.session.add(account)
        db.session.commit()
        
        return jsonify({
            "message": "Account created successfully",
            "account": _account_to_dict(account)
        }), 201
        
    except Exception as e:
        db.session.rollback()
        return safe_json_response({
            "error": "Failed to create account",
            "details": str(e)
        }, 500)

@app.route('/api/accounts/<client_id>', methods=['PUT'])
# @limiter.limit("20 per minute")
@require_user
def update_account(client_id):
    """Update an existing account for the authenticated user."""
    try:
        if not validate_client_id(client_id):
            return jsonify({"error": "Invalid client_id format"}), 400
        
        account = Account.query.filter_by(
            user_id=request.current_user.id,
            client_id=client_id
        ).first()
        
        if not account:
            return jsonify({"error": "Account not found"}), 404
        
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        updatable_fields = [
            "username", "status", "role", "linked_master_id", 
            "copy_status", "multiplier", "auto_login", "device_number"
        ]
        
        for field in updatable_fields:
            if field in data:
                if field == "multiplier":
                    setattr(account, field, float(data[field]))
                elif field == "auto_login":
                    setattr(account, field, bool(data[field]))
                else:
                    setattr(account, field, data[field])
        
        if "credentials" in data:
            existing_creds = account.credentials or {}
            new_creds = data["credentials"]
            if isinstance(new_creds, dict):
                existing_creds.update(new_creds)
                account.credentials = existing_creds
        
        db.session.commit()
        
        return jsonify({
            "message": "Account updated successfully",
            "account": _account_to_dict(account)
        }), 200
        
    except Exception as e:
        db.session.rollback()
        return safe_json_response({
            "error": "Failed to update account",
            "details": str(e)
        }, 500)

@app.route('/api/accounts/<client_id>', methods=['DELETE'])
# @limiter.limit("10 per minute")
@require_user
def delete_account(client_id):
    """Delete an account for the authenticated user."""
    try:
        if not validate_client_id(client_id):
            return jsonify({"error": "Invalid client_id format"}), 400
        
        account = Account.query.filter_by(
            user_id=request.current_user.id,
            client_id=client_id
        ).first()
        
        if not account:
            return jsonify({"error": "Account not found"}), 404
        
        if account.role == "master":
            children = Account.query.filter_by(linked_master_id=client_id).all()
            if children:
                return jsonify({
                    "error": "Cannot delete master account with linked children",
                    "linked_children": [child.client_id for child in children]
                }), 400
        
        active_mappings = OrderMapping.query.filter(
            (OrderMapping.master_client_id == client_id) | 
            (OrderMapping.child_client_id == client_id),
            OrderMapping.status == "ACTIVE"
        ).count()
        
        if active_mappings > 0:
            return jsonify({
                "error": "Cannot delete account with active order mappings",
                "active_mappings": active_mappings
            }), 400
        
        db.session.delete(account)
        db.session.commit()
        
        return jsonify({
            "message": "Account deleted successfully",
            "deleted_client_id": client_id
        }), 200
        
    except Exception as e:
        db.session.rollback()
        return safe_json_response({
            "error": "Failed to delete account",
            "details": str(e)
        }, 500)

@app.route('/api/trades', methods=['GET'])
# @limiter.limit("60 per minute")
@require_user
def get_user_trades():
    """Get trading history for the authenticated user."""
    try:
        page = request.args.get('page', 1, type=int)
        per_page = min(request.args.get('per_page', 50, type=int), 100)
        symbol_filter = request.args.get('symbol', '').strip()
        action_filter = request.args.get('action', '').strip().upper()
        status_filter = request.args.get('status', '').strip().upper()
        
        query = Trade.query.filter_by(user_id=request.current_user.id)
        
        if symbol_filter:
            query = query.filter(Trade.symbol.ilike(f'%{symbol_filter}%'))
        if action_filter and action_filter in ['BUY', 'SELL']:
            query = query.filter(Trade.action == action_filter)
        if status_filter:
            query = query.filter(Trade.status.ilike(f'%{status_filter}%'))
        
        query = query.order_by(Trade.timestamp.desc())
        
        pagination = query.paginate(
            page=page, 
            per_page=per_page, 
            error_out=False
        )
        
        trades = []
        for trade in pagination.items:
            trades.append({
                "id": trade.id,
                "symbol": trade.symbol,
                "action": trade.action,
                "qty": trade.qty,
                "price": trade.price,
                "status": trade.status,
                "timestamp": trade.timestamp
            })
        
        response_data = {
            "trades": trades,
            "pagination": {
                "page": page,
                "pages": pagination.pages,
                "per_page": per_page,
                "total": pagination.total,
                "has_next": pagination.has_next,
                "has_prev": pagination.has_prev
            }
        }
        
        return jsonify(response_data), 200
        
    except Exception as e:
        return safe_json_response({
            "error": "Failed to fetch trades",
            "details": str(e)
        }, 500)

@app.route('/api/broker-status', methods=['GET'])
# @limiter.limit("30 per minute")
def get_broker_status():
    """Check status of all broker APIs."""
    try:
        status_results = {}
        
        for broker_name, url in BROKER_STATUS_URLS.items():
            try:
                is_online = check_api(url)
                status_results[broker_name] = {
                    "status": "online" if is_online else "offline",
                    "url": url,
                    "last_checked": datetime.utcnow().isoformat()
                }
            except Exception as e:
                status_results[broker_name] = {
                    "status": "error",
                    "url": url,
                    "error": str(e),
                    "last_checked": datetime.utcnow().isoformat()
                }
        
        overall_status = "healthy" if all(
            result["status"] == "online" for result in status_results.values()
        ) else "degraded"
        
        response_data = {
            "overall_status": overall_status,
            "brokers": status_results,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return jsonify(response_data), 200
        
    except Exception as e:
        return safe_json_response({
            "error": "Failed to check broker status",
            "details": str(e)
        }, 500)

@app.route('/api/system-info', methods=['GET'])
# @limiter.limit("20 per minute")
def get_system_info():
    """Get system information and status."""
    try:
        db_stats = {
            "users": User.query.count(),
            "accounts": Account.query.count(),
            "trades": Trade.query.count(),
            "order_mappings": OrderMapping.query.count(),
            "active_mappings": OrderMapping.query.filter_by(status="ACTIVE").count(),
        }
        
        system_info = {
            "uptime": format_uptime(),
            "environment": ENVIRONMENT,
            "scheduler_status": "running" if _scheduler and _scheduler.running else "stopped",
            "polling_interval": POLLING_INTERVAL,
            "max_masters_per_cycle": MAX_MASTERS_PER_CYCLE,
            "log_level": LOG_LEVEL,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return jsonify({
            "system": system_info,
            "database": db_stats
        }), 200
        
    except Exception as e:
        return safe_json_response({
            "error": "Failed to get system info",
            "details": str(e)
        }, 500)

@app.route('/api/set-master', methods=['POST'])
@login_required
def set_master():
    try:
        client_id = request.json.get('client_id')
        if not client_id:
            return jsonify({"error": "Missing client_id"}), 400

        user_email = session.get("user")
        user = User.query.filter_by(email=user_email).first()
        if not user:
            return jsonify({"error": "User not found"}), 404

        account = Account.query.filter_by(
            user_id=user.id, 
            client_id=client_id
        ).first()
        
        if not account:
            return jsonify({"error": "Account not found"}), 404

        account.role = "master"
        account.linked_master_id = None
        account.copy_status = "Off"
        account.multiplier = 1.0
        db.session.commit()

        return jsonify({"message": "Set as master successfully"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/set-child', methods=['POST'])
@login_required
def set_child():
    try:
        client_id = request.json.get('client_id')
        linked_master_id = request.json.get('linked_master_id')
        
        if not client_id or not linked_master_id:
            return jsonify({"error": "Missing client_id or linked_master_id"}), 400

        user_email = session.get("user")
        user = User.query.filter_by(email=user_email).first()
        if not user:
            return jsonify({"error": "User not found"}), 404

        account = Account.query.filter_by(
            user_id=user.id, 
            client_id=client_id
        ).first()
        
        if not account:
            return jsonify({"error": "Account not found"}), 404

        account.role = "child"
        account.linked_master_id = linked_master_id
        account.copy_status = "Off"
        account.multiplier = 1.0
        db.session.commit()

        return jsonify({"message": "Set as child successfully"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/start-copy', methods=['POST'])
@login_required
def start_copy():
    """Start copying for a child account."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        client_id = data.get("client_id")
        master_id = data.get("master_id")
        
        if not client_id or not master_id:
            return jsonify({"error": "Missing client_id or master_id"}), 400

        user_email = session.get("user")
        if not user_email:
            return jsonify({"error": "User not logged in"}), 401
            
        user = User.query.filter_by(email=user_email).first()
        if not user:
            return jsonify({"error": "User not found"}), 404

        child_account = Account.query.filter_by(
            user_id=user.id, 
            client_id=client_id
        ).first()
        
        if not child_account:
            return jsonify({"error": "Child account not found"}), 404

        master_account = Account.query.filter_by(
            client_id=master_id,
            role='master'
        ).first()
        
        if not master_account:
            return jsonify({"error": "Master account not found"}), 404

        if master_account.user_id != user.id:
            return jsonify({"error": "Master account not accessible"}), 403

        child_account.role = "child"
        child_account.linked_master_id = master_id
        child_account.copy_status = "On"
        
        latest_order_id = "NONE"
        
        try:
            master_dict = _account_to_dict(master_account)
            master_api = broker_api(master_dict)
            
            broker_name = master_account.broker.lower() if master_account.broker else "unknown"
            
            if broker_name == "aliceblue" and hasattr(master_api, "get_trade_book"):
                orders_resp = master_api.get_trade_book()
                order_list = parse_order_list(orders_resp)
                
                if not order_list and hasattr(master_api, "get_order_list"):
                    orders_resp = master_api.get_order_list()
                    order_list = parse_order_list(orders_resp)
            else:
                orders_resp = master_api.get_order_list()
                order_list = parse_order_list(orders_resp)
            
            order_list = strip_emojis_from_obj(order_list or [])
            
            if order_list and isinstance(order_list, list):
                try:
                    order_list = sorted(order_list, key=get_order_sort_key, reverse=True)
                    
                    if order_list:
                        latest_order = order_list[0]
                        
                        latest_order_id = (
                            latest_order.get("orderId") or latest_order.get("order_id") or
                            latest_order.get("id") or latest_order.get("NOrdNo") or
                            latest_order.get("Nstordno") or latest_order.get("nestOrderNumber") or
                            latest_order.get("orderNumber") or latest_order.get("norenordno") or "NONE"
                        )
                        
                        latest_order_id = str(latest_order_id) if latest_order_id else "NONE"
                        
                except Exception as e:
                    latest_order_id = "NONE"
                
        except Exception as e:
            latest_order_id = "NONE"

        child_account.last_copied_trade_id = latest_order_id
        
        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            return jsonify({
                "error": "Failed to save configuration",
                "details": str(e)
            }), 500

        return jsonify({
            'message': f"✅ Started copying for {client_id} under master {master_id}",
            'details': {
                'child_account': client_id,
                'master_account': master_id,
                'copy_status': 'On',
                'initial_marker': latest_order_id,
                'broker': master_account.broker
            }
        }), 200

    except Exception as e:
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500

@app.route('/api/stop-copy', methods=['POST'])
@login_required
def stop_copy():
    """Stop copying for a child account."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        client_id = data.get("client_id")
        
        if not client_id:
            return jsonify({"error": "Missing client_id"}), 400

        user_email = session.get("user")
        if not user_email:
            return jsonify({"error": "User not logged in"}), 401
            
        user = User.query.filter_by(email=user_email).first()
        if not user:
            return jsonify({"error": "User not found"}), 404

        child_account = Account.query.filter_by(
            user_id=user.id, 
            client_id=client_id
        ).first()
        
        if not child_account:
            return jsonify({"error": "Child account not found"}), 404

        if child_account.role != "child":
            return jsonify({"error": "Account is not configured as a child"}), 400

        if child_account.copy_status != "On":
            return jsonify({
                "message": f"Copy trading is already stopped for {client_id}",
                "current_status": child_account.copy_status
            }), 200

        current_master_id = child_account.linked_master_id
        master_account = None
        if current_master_id:
            master_account = Account.query.filter_by(
                client_id=current_master_id
            ).first()

        child_account.copy_status = "Off"
        
        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            return jsonify({
                "error": "Failed to save configuration",
                "details": str(e)
            }), 500

        response_data = {
            'message': f"🛑 Stopped copying for {client_id}",
            'details': {
                'child_account': client_id,
                'master_account': current_master_id,
                'copy_status': 'Off',
                'role': child_account.role,
                'broker': child_account.broker,
                'stopped_at': datetime.now().isoformat()
            }
        }
        
        if master_account:
            response_data['details']['master_broker'] = master_account.broker
            response_data['details']['master_username'] = master_account.username

        return jsonify(response_data), 200

    except Exception as e:
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500

@app.route('/api/start-copy-all', methods=['POST'])
@login_required
def start_copy_all():
    """Start copying for all children of a master."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        master_id = data.get("master_id")
        
        if not master_id:
            return jsonify({"error": "Missing master_id"}), 400

        user_email = session.get("user")
        if not user_email:
            return jsonify({"error": "User not logged in"}), 401
            
        user = User.query.filter_by(email=user_email).first()
        if not user:
            return jsonify({"error": "User not found"}), 404

        master_account = Account.query.filter_by(
            user_id=user.id,
            client_id=master_id,
            role='master'
        ).first()
        
        if not master_account:
            return jsonify({"error": "Master account not found or not accessible"}), 404

        stopped_children = Account.query.filter_by(
            user_id=user.id,
            role='child',
            linked_master_id=master_id,
            copy_status='Off'
        ).all()

        if not stopped_children:
            return jsonify({
                "message": f"No stopped child accounts found for master {master_id}",
                "master_id": master_id,
                "started_count": 0,
                "details": []
            }), 200

        master_latest_order_id = "NONE"
        
        try:
            master_dict = _account_to_dict(master_account)
            master_api = broker_api(master_dict)
            
            broker_name = master_account.broker.lower() if master_account.broker else "unknown"
            
            if broker_name == "aliceblue" and hasattr(master_api, "get_trade_book"):
                orders_resp = master_api.get_trade_book()
                order_list = parse_order_list(orders_resp)
                
                if not order_list and hasattr(master_api, "get_order_list"):
                    orders_resp = master_api.get_order_list()
                    order_list = parse_order_list(orders_resp)
            else:
                orders_resp = master_api.get_order_list()
                order_list = parse_order_list(orders_resp)
            
            order_list = strip_emojis_from_obj(order_list or [])
            
            if order_list and isinstance(order_list, list):
                try:
                    order_list = sorted(order_list, key=get_order_sort_key, reverse=True)
                    
                    if order_list:
                        latest_order = order_list[0]
                        
                        master_latest_order_id = (
                            latest_order.get("orderId") or latest_order.get("order_id") or
                            latest_order.get("id") or latest_order.get("NOrdNo") or
                            latest_order.get("Nstordno") or latest_order.get("nestOrderNumber") or
                            latest_order.get("orderNumber") or latest_order.get("norenordno") or "NONE"
                        )
                        
                        master_latest_order_id = str(master_latest_order_id) if master_latest_order_id else "NONE"
                        
                except Exception as e:
                    master_latest_order_id = "NONE"
                
        except Exception as e:
            master_latest_order_id = "NONE"

        results = []
        started_count = 0
        failed_count = 0

        for child in stopped_children:
            try:
                child.copy_status = "On"
                child.last_copied_trade_id = master_latest_order_id
                
                results.append({
                    "client_id": child.client_id,
                    "broker": child.broker,
                    "username": child.username,
                    "multiplier": child.multiplier,
                    "status": "SUCCESS",
                    "new_marker": master_latest_order_id,
                    "message": "Copy trading started successfully"
                })
                
                started_count += 1

            except Exception as e:
                results.append({
                    "client_id": child.client_id,
                    "broker": child.broker or "Unknown",
                    "username": child.username or "Unknown",
                    "multiplier": child.multiplier or 1.0,
                    "status": "ERROR",
                    "message": f"Failed to start: {str(e)}"
                })
                failed_count += 1

        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            return jsonify({
                "error": "Failed to save bulk configuration changes",
                "details": str(e)
            }, 500)

        response_data = {
            "message": f"Bulk start completed for master {master_id}",
            "master_id": master_id,
            "master_broker": master_account.broker,
            "master_username": master_account.username,
            "master_marker": master_latest_order_id,
            "summary": {
                "eligible_to_start": len(stopped_children),
                "started_successfully": started_count,
                "failed": failed_count
            },
            "details": results
        }

        if failed_count == 0:
            return jsonify(response_data), 200
        elif started_count > 0:
            return jsonify(response_data), 207
        else:
            response_data["error"] = "Failed to start any child accounts"
            return jsonify(response_data), 500

    except Exception as e:
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500

@app.route('/api/stop-copy-all', methods=['POST'])
@login_required
def stop_copy_all():
    """Bulk stop copying for all children of a master."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        master_id = data.get("master_id")
        
        if not master_id:
            return jsonify({"error": "Missing master_id"}), 400

        user_email = session.get("user")
        if not user_email:
            return jsonify({"error": "User not logged in"}), 401
            
        user = User.query.filter_by(email=user_email).first()
        if not user:
            return jsonify({"error": "User not found"}), 404

        master_account = Account.query.filter_by(
            user_id=user.id,
            client_id=master_id,
            role='master'
        ).first()
        
        if not master_account:
            return jsonify({"error": "Master account not found or not accessible"}), 404

        active_children = Account.query.filter_by(
            user_id=user.id,
            role='child',
            linked_master_id=master_id,
            copy_status='On'
        ).all()

        if not active_children:
            return jsonify({
                "message": f"No active child accounts found for master {master_id}",
                "master_id": master_id,
                "stopped_count": 0,
                "details": []
            }), 200

        results = []
        stopped_count = 0
        failed_count = 0

        for child in active_children:
            try:
                child.copy_status = "Off"
                
                results.append({
                    "client_id": child.client_id,
                    "broker": child.broker,
                    "username": child.username,
                    "status": "SUCCESS",
                    "message": "Copy trading stopped successfully"
                })
                
                stopped_count += 1

            except Exception as e:
                results.append({
                    "client_id": child.client_id,
                    "broker": child.broker or "Unknown",
                    "username": child.username or "Unknown",
                    "status": "ERROR",
                    "message": f"Failed to stop: {str(e)}"
                })
                failed_count += 1

        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            return jsonify({
                "error": "Failed to save bulk configuration changes",
                "details": str(e)
            }, 500)

        response_data = {
            "message": f"Bulk stop completed for master {master_id}",
            "master_id": master_id,
            "master_broker": master_account.broker,
            "master_username": master_account.username,
            "summary": {
                "total_processed": len(active_children),
                "stopped_successfully": stopped_count,
                "failed": failed_count
            },
            "details": results
        }

        if failed_count == 0:
            return jsonify(response_data), 200
        elif stopped_count > 0:
            return jsonify(response_data), 207
        else:
            response_data["error"] = "Failed to stop any child accounts"
            return jsonify(response_data), 500

    except Exception as e:
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500

@app.route('/api/square-off', methods=['POST'])
@login_required
def square_off():
    data = request.json
    client_id = data.get("client_id")
    symbol = data.get("symbol")
    is_master = data.get("is_master", False)

    if not client_id or not symbol:
        return jsonify({"error": "Missing client_id or symbol"}), 400

    found, parent = find_account_by_client_id(client_id)

    if not found:
        return jsonify({"error": "Client not found"}), 404
    if parent is None:
        master = found
    else:
        master = parent

    if is_master and parent is None:
        api = broker_api(master)
        try:
            positions_resp = api.get_positions()
            positions = positions_resp.get("data", [])
            match = next((p for p in positions if p.get("tradingSymbol", "").upper() == symbol.upper()), None)
            if not match or int(match.get("netQty", 0)) == 0:
                return jsonify({"message": f"Master → No active position in {symbol} (already squared off)"}), 200

            qty = abs(int(match["netQty"]))
            direction = "SELL" if match["netQty"] > 0 else "BUY"

            resp = api.place_order(
                tradingsymbol=symbol,
                security_id=match["securityId"],
                exchange_segment=match["exchangeSegment"],
                transaction_type=direction,
                quantity=qty,
                order_type="MARKET",
                product_type="INTRADAY",
                price=0
            )
            save_log(master["client_id"], symbol, "SQUARE_OFF", qty, "SUCCESS", str(resp))
            return jsonify({"message": "✅ Master square-off placed", "details": str(resp)}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    else:
        results = []
        for child in master.get("children", []):
            if child.get("copy_status") != "On":
                results.append(f"Child {child['client_id']} → Skipped (copy OFF)")
                continue

            try:
                api = broker_api(child)
                positions_resp = api.get_positions()
                positions = positions_resp.get('data', [])
                match = next((p for p in positions if p.get('tradingSymbol', '').upper() == symbol.upper()), None)

                if not match or int(match.get('netQty', 0)) == 0:
                    results.append(f"Child {child['client_id']} → Skipped (no active position in {symbol})")
                    continue

                security_id = match['securityId']
                exchange_segment = match['exchangeSegment']
                quantity = abs(int(match['netQty']))
                direction = "SELL" if match['netQty'] > 0 else "BUY"

                response = api.place_order(
                    tradingsymbol=symbol,
                    security_id=security_id,
                    exchange_segment=exchange_segment,
                    transaction_type=direction,
                    quantity=quantity,
                    order_type="MARKET",
                    product_type="INTRADAY",
                    price=0
                )

                if isinstance(response, dict) and response.get("status") == "failure":
                    msg = response.get("remarks", "Unknown error")
                    results.append(f"Child {child['client_id']} → FAILED: {msg}")
                    save_log(child['client_id'], symbol, "SQUARE_OFF", quantity, "FAILED", msg)
                else:
                    results.append(f"Child {child['client_id']} → SUCCESS")
                    save_log(child['client_id'], symbol, "SQUARE_OFF", quantity, "SUCCESS", str(response))

            except Exception as e:
                error_msg = str(e)
                results.append(f"Child {child['client_id']} → ERROR: {error_msg}")
                save_log(child['client_id'], symbol, "SQUARE_OFF", 0, "ERROR", error_msg)

        return jsonify({"message": "🔁 Square-off for all children completed", "details": results}), 200

@app.route('/api/order-mappings', methods=['GET'])
@login_required
def get_order_mappings():
    try:
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
            for m in OrderMapping.query.all()
        ]
        return jsonify(mappings), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/child-orders')
@login_required
def child_orders():
    master_order_id = request.args.get('master_order_id')
    mappings = OrderMapping.query
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

@app.route('/api/cancel-order', methods=['POST'])
@login_required
def cancel_order():
    try:
        data = request.json
        master_order_id = data.get("master_order_id")
        mappings = OrderMapping.query.filter_by(
            master_order_id=master_order_id, status="ACTIVE"
        ).all()
        if not mappings:
            return jsonify({"message": "No active child orders found for this master order."}), 200

        results = []
        accounts = {a.client_id: a for a in Account.query.all()}

        for mapping in mappings:
            child_id = mapping.child_client_id
            child_order_id = mapping.child_order_id
            found = accounts.get(child_id)

            if not found:
                results.append(f"{child_id} → ❌ Client not found")
                continue

            try:
                api = broker_api({
                    "broker": found.broker,
                    "client_id": found.client_id,
                    "credentials": found.credentials,
                })
                cancel_resp = api.cancel_order(child_order_id)

                if isinstance(cancel_resp, dict) and cancel_resp.get("status") == "failure":
                    results.append(
                        f"{child_id} → ❌ Cancel failed: {cancel_resp.get('remarks', cancel_resp.get('error', 'Unknown error'))}"
                    )
                else:
                    results.append(f"{child_id} → ✅ Cancelled")
                    mapping.status = "CANCELLED"

            except Exception as e:
                results.append(f"{child_id} → ❌ ERROR: {str(e)}")

        db.session.commit()

        return jsonify({"message": "Cancel process completed", "details": results}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/change-master', methods=['POST'])
@login_required
def change_master():
    """Change master for a child account."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        child_id = data.get("child_id")
        new_master_id = data.get("new_master_id")
        
        if not child_id or not new_master_id:
            return jsonify({"error": "Missing child_id or new_master_id"}), 400

        user_email = session.get("user")
        if not user_email:
            return jsonify({"error": "User not logged in"}), 401
            
        user = User.query.filter_by(email=user_email).first()
        if not user:
            return jsonify({"error": "User not found"}), 404

        child_account = Account.query.filter_by(
            user_id=user.id,
            client_id=child_id
        ).first()
        
        if not child_account:
            return jsonify({"error": "Child account not found"}), 404

        if child_account.role != "child":
            return jsonify({
                "error": "Account is not configured as a child",
                "current_role": child_account.role
            }), 400

        old_master_id = child_account.linked_master_id
        old_master_account = None
        
        if old_master_id:
            old_master_account = Account.query.filter_by(
                client_id=old_master_id
            ).first()

        new_master_account = Account.query.filter_by(
            user_id=user.id,
            client_id=new_master_id,
            role='master'
        ).first()
        
        if not new_master_account:
            return jsonify({
                "error": "New master account not found or not accessible",
                "master_id": new_master_id
            }), 404

        if old_master_id == new_master_id:
            return jsonify({
                "message": f"Child {child_id} is already linked to master {new_master_id}",
                "no_change_needed": True,
                "current_master": {
                    "client_id": new_master_id,
                    "broker": new_master_account.broker,
                    "username": new_master_account.username
                }
            }), 200

        was_copying = child_account.copy_status == "On"
        if was_copying:
            child_account.copy_status = "Off"

        child_account.linked_master_id = new_master_id
        child_account.last_copied_trade_id = "NONE"

        new_latest_order_id = "NONE"
        
        try:
            new_master_dict = _account_to_dict(new_master_account)
            new_master_api = broker_api(new_master_dict)
            
            broker_name = new_master_account.broker.lower() if new_master_account.broker else "unknown"
            
            if broker_name == "aliceblue" and hasattr(new_master_api, "get_trade_book"):
                orders_resp = new_master_api.get_trade_book()
                order_list = parse_order_list(orders_resp)
                
                if not order_list and hasattr(new_master_api, "get_order_list"):
                    orders_resp = new_master_api.get_order_list()
                    order_list = parse_order_list(orders_resp)
            else:
                orders_resp = new_master_api.get_order_list()
                order_list = parse_order_list(orders_resp)
            
            order_list = strip_emojis_from_obj(order_list or [])
            
            if order_list and isinstance(order_list, list):
                try:
                    order_list = sorted(order_list, key=get_order_sort_key, reverse=True)
                    
                    if order_list:
                        latest_order = order_list[0]
                        new_latest_order_id = (
                            latest_order.get("orderId") or latest_order.get("order_id") or
                            latest_order.get("id") or latest_order.get("NOrdNo") or
                            latest_order.get("norenordno") or "NONE"
                        )
                        new_latest_order_id = str(new_latest_order_id) if new_latest_order_id else "NONE"
                        
                except Exception as e:
                    new_latest_order_id = "NONE"
                    
        except Exception as e:
            new_latest_order_id = "NONE"

        child_account.last_copied_trade_id = new_latest_order_id

        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            return jsonify({
                "error": "Failed to save master change",
                "details": str(e)
            }), 500

        response_data = {
            "message": f"Master changed successfully for {child_id}",
            "child_account": {
                "client_id": child_id,
                "broker": child_account.broker,
                "username": child_account.username,
                "copy_status": child_account.copy_status,
                "new_marker": new_latest_order_id
            },
            "old_master": {
                "client_id": old_master_id,
                "broker": old_master_account.broker if old_master_account else "Unknown",
                "username": old_master_account.username if old_master_account else "Unknown"
            },
            "new_master": {
                "client_id": new_master_id,
                "broker": new_master_account.broker,
                "username": new_master_account.username
            },
            "change_details": {
                "was_copying": was_copying,
                "copy_temporarily_disabled": was_copying,
                "new_marker_set": new_latest_order_id,
                "changed_at": "2025-07-05 11:21:25"
            }
        }

        if was_copying:
            response_data["next_action"] = "Please restart copying to begin following the new master"

        return jsonify(response_data), 200

    except Exception as e:
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500

@app.route('/api/remove-child', methods=['POST'])
@login_required
def remove_child():
    """Remove child role from account."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        client_id = data.get("client_id")
        
        if not client_id:
            return jsonify({"error": "Missing client_id"}), 400

        user_email = session.get("user")
        if not user_email:
            return jsonify({"error": "User not logged in"}), 401
            
        user = User.query.filter_by(email=user_email).first()
        if not user:
            return jsonify({"error": "User not found"}), 404

        child_account = Account.query.filter_by(
            user_id=user.id,
            client_id=client_id,
            role='child'
        ).first()
        
        if not child_account:
            return jsonify({"error": "Child account not found or not configured as child"}), 404

        master_account = None
        master_id = child_account.linked_master_id
        
        if master_id:
            master_account = Account.query.filter_by(
                client_id=master_id
            ).first()

        was_copying = child_account.copy_status == "On"

        active_mappings = OrderMapping.query.filter_by(
            child_client_id=client_id,
            status="ACTIVE"
        ).all()

        mapping_count = len(active_mappings)

        child_account.role = None
        child_account.linked_master_id = None
        child_account.copy_status = "Off"
        child_account.multiplier = 1.0
        child_account.last_copied_trade_id = None

        mappings_updated = 0
        for mapping in active_mappings:
            try:
                mapping.status = "CHILD_REMOVED"
                mapping.remarks = f"Child account removed on {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}"
                mappings_updated += 1
            except Exception as e:
                pass

        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            return jsonify({
                "error": "Failed to save child removal",
                "details": str(e)
            }), 500

        response_data = {
            "message": f"Child {client_id} removed from master successfully",
            "removed_account": {
                "client_id": client_id,
                "broker": child_account.broker,
                "username": child_account.username,
                "new_role": None,
                "new_status": "Unassigned"
            },
            "previous_master": {
                "client_id": master_id,
                "broker": master_account.broker if master_account else "Unknown",
                "username": master_account.username if master_account else "Unknown"
            } if master_id else None,
            "removal_details": {
                "was_copying": was_copying,
                "copy_stopped": True,
                "multiplier_reset": True,
                "marker_cleared": True,
                "active_mappings_found": mapping_count,
                "mappings_updated": mappings_updated,
                "removed_at": "2025-07-05 11:21:25"
            }
        }

        if mapping_count > 0:
            response_data["cleanup_summary"] = {
                "message": f"Updated {mappings_updated} order mappings to CHILD_REMOVED status",
                "mappings_affected": mapping_count
            }

        return jsonify(response_data), 200

    except Exception as e:
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500

@app.route('/api/remove-master', methods=['POST'])
@login_required
def remove_master():
    """Remove master role from account."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        client_id = data.get("client_id")
        
        if not client_id:
            return jsonify({"error": "Missing client_id"}), 400

        user_email = session.get("user")
        if not user_email:
            return jsonify({"error": "User not logged in"}), 401
            
        user = User.query.filter_by(email=user_email).first()
        if not user:
            return jsonify({"error": "User not found"}), 404

        master_account = Account.query.filter_by(
            user_id=user.id,
            client_id=client_id,
            role='master'
        ).first()
        
        if not master_account:
            return jsonify({"error": "Master account not found or not configured as master"}), 404

        linked_children = Account.query.filter_by(
            role='child',
            linked_master_id=client_id
        ).all()

        children_count = len(linked_children)
        active_children = [child for child in linked_children if child.copy_status == "On"]
        active_count = len(active_children)

        active_mappings = OrderMapping.query.filter_by(
            master_client_id=client_id,
            status="ACTIVE"
        ).all()

        mapping_count = len(active_mappings)

        children_processed = []
        children_failed = []

        for child in linked_children:
            try:
                child.role = None
                child.linked_master_id = None
                child.copy_status = "Off"
                child.multiplier = 1.0
                child.last_copied_trade_id = None

                children_processed.append({
                    "client_id": child.client_id,
                    "broker": child.broker,
                    "username": child.username,
                    "was_copying": child.copy_status == "On",
                    "status": "ORPHANED"
                })

            except Exception as e:
                children_failed.append({
                    "client_id": child.client_id,
                    "error": str(e)
                })

        master_account.role = None
        master_account.copy_status = "Off"
        master_account.multiplier = 1.0

        mappings_updated = 0
        mapping_details = []

        for mapping in active_mappings:
            try:
                mapping.status = "MASTER_REMOVED"
                mapping.remarks = f"Master account removed on {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}"
                
                mapping_details.append({
                    "master_order_id": mapping.master_order_id,
                    "child_order_id": mapping.child_order_id,
                    "child_client_id": mapping.child_client_id,
                    "symbol": mapping.symbol
                })
                
                mappings_updated += 1
            except Exception as e:
                pass

        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            return jsonify({
                "error": "Failed to save master removal",
                "details": str(e)
            }), 500

        response_data = {
            "message": f"Master {client_id} removed successfully",
            "removed_master": {
                "client_id": client_id,
                "broker": master_account.broker,
                "username": master_account.username,
                "new_role": None,
                "new_status": "Unassigned"
            },
            "children_affected": {
                "total_children": children_count,
                "active_children": active_count,
                "successfully_orphaned": len(children_processed),
                "failed_to_orphan": len(children_failed),
                "orphan_details": children_processed
            },
            "order_mappings": {
                "active_mappings_found": mapping_count,
                "mappings_updated": mappings_updated,
                "mapping_details": mapping_details[:10]
            },
            "removal_details": {
                "master_role_removed": True,
                "copy_status_reset": True,
                "multiplier_reset": True,
                "children_orphaned": len(children_processed),
                "removed_at": "2025-07-05 11:21:25"
            }
        }

        if children_failed:
            response_data["children_affected"]["failures"] = children_failed

        if mapping_count > 10:
            response_data["order_mappings"]["note"] = f"Showing first 10 of {mapping_count} total mappings"

        return jsonify(response_data), 200

    except Exception as e:
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500

@app.route('/api/update-multiplier', methods=['POST'])
@login_required
def update_multiplier():
    """Update multiplier for account."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        client_id = data.get("client_id")
        new_multiplier = data.get("multiplier")
        
        if not client_id or new_multiplier is None:
            return jsonify({"error": "Missing client_id or multiplier"}), 400

        try:
            new_multiplier = float(new_multiplier)
            if new_multiplier < 0.1:
                return jsonify({"error": "Multiplier must be at least 0.1"}), 400
            if new_multiplier > 100.0:
                return jsonify({"error": "Multiplier cannot exceed 100.0"}), 400
        except (ValueError, TypeError):
            return jsonify({"error": "Invalid multiplier format - must be a number"}), 400

        user_email = session.get("user")
        if not user_email:
            return jsonify({"error": "User not logged in"}), 401
            
        user = User.query.filter_by(email=user_email).first()
        if not user:
            return jsonify({"error": "User not found"}), 404

        account = Account.query.filter_by(
            user_id=user.id,
            client_id=client_id
        ).first()
        
        if not account:
            return jsonify({"error": "Account not found"}), 404

        if abs(float(account.multiplier or 1.0) - new_multiplier) < 0.001:
            return jsonify({
                "message": f"Multiplier for {client_id} is already set to {new_multiplier}",
                "no_change_needed": True,
                "current_multiplier": float(account.multiplier or 1.0),
                "account_details": {
                    "client_id": client_id,
                    "broker": account.broker,
                    "username": account.username,
                    "role": account.role
                }
            }), 200

        master_account = None
        if account.role == "child" and account.linked_master_id:
            master_account = Account.query.filter_by(
                client_id=account.linked_master_id
            ).first()

        warnings = []
        
        if account.role == "child":
            if account.copy_status == "On":
                warnings.append("Multiplier changed while copying is active - new multiplier will apply to future trades")
            if new_multiplier > 10.0:
                warnings.append("High multiplier detected - please ensure sufficient margin available")
        elif account.role == "master":
            warnings.append("Multiplier set for master account - this only affects if the master is also used as a child")
        else:
            warnings.append("Multiplier set for unassigned account - will take effect when account is configured as child")

        active_mappings = []
        if account.role == "child":
            active_mappings = OrderMapping.query.filter_by(
                child_client_id=client_id,
                status="ACTIVE"
            ).all()

        active_mapping_count = len(active_mappings)
        
        if active_mapping_count > 0:
            warnings.append(f"{active_mapping_count} active order mappings found - multiplier change affects future orders only")

        previous_multiplier = float(account.multiplier or 1.0)
        account.multiplier = new_multiplier

        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            return jsonify({
                "error": "Failed to save multiplier update",
                "details": str(e)
            }), 500

        impact_estimation = None
        if account.role == "child" and master_account:
            impact_estimation = {
                "example_scenario": {
                    "master_trade_qty": 100,
                    "old_child_qty": int(100 * previous_multiplier),
                    "new_child_qty": int(100 * new_multiplier),
                    "qty_change": int(100 * new_multiplier) - int(100 * previous_multiplier)
                },
                "multiplier_change": {
                    "percentage": f"{((new_multiplier / previous_multiplier) - 1) * 100:+.1f}%",
                    "factor": f"{new_multiplier / previous_multiplier:.2f}x"
                }
            }

        response_data = {
            "message": f"Multiplier updated to {new_multiplier} for {client_id}",
            "account_details": {
                "client_id": client_id,
                "broker": account.broker,
                "username": account.username,
                "role": account.role,
                "copy_status": account.copy_status
            },
            "multiplier_update": {
                "previous_multiplier": previous_multiplier,
                "new_multiplier": new_multiplier,
                "change": new_multiplier - previous_multiplier,
                "updated_at": "2025-07-05 11:21:25"
            },
            "linked_master": {
                "client_id": account.linked_master_id,
                "broker": master_account.broker if master_account else None,
                "username": master_account.username if master_account else None
            } if master_account else None,
            "active_mappings": {
                "count": active_mapping_count,
                "note": "New multiplier applies to future trades only" if active_mapping_count > 0 else None
            },
            "warnings": warnings if warnings else []
        }

        if impact_estimation:
            response_data["impact_estimation"] = impact_estimation

        return jsonify(response_data), 200

    except Exception as e:
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500

@app.route('/api/delete-account', methods=['POST'])
@login_required
def delete_account_api():
    data = request.json
    client_id = data.get("client_id")
    if not client_id:
        return jsonify({"error": "Missing client_id"}), 400
    user = session.get("user")
    db_user = User.query.filter_by(email=user).first()
    if not db_user:
        return jsonify({"error": "Account not found"}), 404
    acc_db = Account.query.filter_by(user_id=db_user.id, client_id=client_id).first()
    if not acc_db:
        return jsonify({"error": "Account not found"}), 404
    db.session.delete(acc_db)
    db.session.commit()

    return jsonify({"message": f"Account {client_id} deleted."})

@app.route("/marketwatch")
@login_required
def market_watch():
    return render_template("marketwatch.html")

@app.route('/api/check-credentials', methods=['POST'])
@login_required
def check_credentials():
    """Validate broker credentials without saving them."""
    data = request.json
    broker = data.get('broker')
    client_id = data.get('client_id')

    if not broker or not client_id:
        return jsonify({'error': 'Missing broker or client_id'}), 400

    credentials = {k: v for k, v in data.items() if k not in ('broker', 'client_id')}
    
    broker_obj = None
    error_message = None

    try:
        BrokerClass = get_broker_class(broker)
        
        if broker == 'aliceblue':
            api_key = credentials.get('api_key')
            if not api_key:
                return jsonify({'error': 'Missing API Key'}), 400
            broker_obj = BrokerClass(client_id, api_key)
            
        elif broker == 'finvasia':
            required = ['password', 'totp_secret', 'vendor_code', 'api_key']
            if not all(credentials.get(r) for r in required):
                return jsonify({'error': 'Missing credentials'}), 400
            imei = credentials.get('imei') or 'abc1234'
            credentials['imei'] = imei
            broker_obj = BrokerClass(
                client_id=client_id,
                password=credentials['password'],
                totp_secret=credentials['totp_secret'],
                vendor_code=credentials['vendor_code'],
                api_key=credentials['api_key'],
                imei=imei
            )

        elif broker == 'groww':
            access_token = credentials.get('access_token')
            if not access_token:
                return jsonify({'error': 'Missing Access Token'}), 400
            broker_obj = BrokerClass(client_id, access_token)
        else:
            access_token = credentials.get('access_token')
            rest = {k: v for k, v in credentials.items() if k != 'access_token'}
            broker_obj = BrokerClass(client_id, access_token, **rest)

        if broker_obj and hasattr(broker_obj, 'check_token_valid'):
            valid = broker_obj.check_token_valid()
            if not valid:
                error_message = broker_obj.last_auth_error() or 'Invalid broker credentials'
                return jsonify({'error': error_message}), 400
        elif not broker_obj:
            return jsonify({'error': 'Broker object could not be initialized.'}), 400
        
        return jsonify({'valid': True})

    except Exception as e:
        if broker_obj and hasattr(broker_obj, 'last_auth_error') and broker_obj.last_auth_error():
            error_message = broker_obj.last_auth_error()
        else:
            error_message = str(e)
        return jsonify({'error': f'Credential validation failed: {error_message}'}), 400

@app.route('/api/groups', methods=['GET'])
@login_required
def get_groups():
    """Return all account groups for the logged-in user."""
    user_email = session.get("user")
    user_obj = User.query.filter_by(email=user_email).first()
    if not user_obj:
        return jsonify([])
    groups = Group.query.filter_by(user_id=user_obj.id).all()
    return jsonify([_group_to_dict(g) for g in groups])

@app.route('/api/create-group', methods=['POST'])
@login_required
def create_group():
    """Create a new account group."""
    data = request.json
    name = data.get("name")
    members = data.get("members", [])
    if not name:
        return jsonify({"error": "Missing group name"}), 400

    user_email = session.get("user")
    user_obj = User.query.filter_by(email=user_email).first()
    if not user_obj:
        return jsonify({"error": "User not found"}), 400
    if Group.query.filter_by(user_id=user_obj.id, name=name).first():
        return jsonify({"error": "Group already exists"}), 400

    group = Group(name=name, user_id=user_obj.id)
    for cid in members:
        acc = Account.query.filter_by(user_id=user_obj.id, client_id=cid).first()
        if acc:
            group.accounts.append(acc)
    db.session.add(group)
    db.session.commit()
    return jsonify({"message": f"Group '{name}' created"})

@app.route('/api/groups/<group_name>/add', methods=['POST'])
@login_required
def add_account_to_group(group_name):
    """Add an account to an existing group."""
    client_id = request.json.get("client_id")
    if not client_id:
        return jsonify({"error": "Missing client_id"}), 400

    user_email = session.get("user")
    user_obj = User.query.filter_by(email=user_email).first()
    if not user_obj:
        return jsonify({"error": "User not found"}), 400
    group = Group.query.filter_by(user_id=user_obj.id, name=group_name).first()
    if not group:
        return jsonify({"error": "Group not found"}), 404
    acc = Account.query.filter_by(user_id=user_obj.id, client_id=client_id).first()
    if not acc:
        return jsonify({"error": "Account not found"}), 404
    if acc in group.accounts:
        return jsonify({"message": "Account already in group"})
    group.accounts.append(acc)
    db.session.commit()
    return jsonify({"message": f"Added {client_id} to {group_name}"})

@app.route('/api/groups/<group_name>/remove', methods=['POST'])
@login_required
def remove_account_from_group(group_name):
    """Remove an account from a group."""
    client_id = request.json.get("client_id")
    if not client_id:
        return jsonify({"error": "Missing client_id"}), 400

    user_email = session.get("user")
    user_obj = User.query.filter_by(email=user_email).first()
    if not user_obj:
        return jsonify({"error": "User not found"}), 400
    group = Group.query.filter_by(user_id=user_obj.id, name=group_name).first()
    if not group:
        return jsonify({"error": "Group not found"}), 404
    acc = Account.query.filter_by(user_id=user_obj.id, client_id=client_id).first()
    if not acc or acc not in group.accounts:
        return jsonify({"error": "Account not in group"}), 400
    group.accounts.remove(acc)
    db.session.commit()
    return jsonify({"message": f"Removed {client_id} from {group_name}"})

@app.route('/api/group-order', methods=['POST'])
@login_required
def place_group_order():
    """Place the same order across all accounts in a group."""
    data = request.json
    group_name = data.get("group_name")
    symbol = data.get("symbol")
    action = data.get("action")
    quantity = data.get("quantity")

    if not all([group_name, symbol, action, quantity]):
        return jsonify({"error": "Missing required fields"}), 400

    user_email = session.get("user")
    user_obj = User.query.filter_by(email=user_email).first()
    if not user_obj:
        return jsonify({"error": "User not found"}), 400
    group = Group.query.filter_by(user_id=user_obj.id, name=group_name).first()
    if not group:
        return jsonify({"error": "Group not found"}), 404

    accounts = [_account_to_dict(acc) for acc in group.accounts.all()]
    results = []
    for acc in accounts:
        try:
            api = broker_api(acc)
            broker_name = acc.get("broker", "dhan").lower()
            order_params = {}
            mapping = get_symbol_for_broker(symbol, broker_name)
            if broker_name == "dhan":
                security_id = mapping.get("security_id")
                order_params = dict(
                    tradingsymbol=symbol,
                    security_id=security_id,
                    exchange_segment=api.NSE,
                    transaction_type=api.BUY if action.upper() == "BUY" else api.SELL,
                    quantity=int(quantity),
                    order_type=map_order_type(api.MARKET, broker_name),
                    product_type=api.INTRA,
                    price=0
                )

            elif broker_name == "aliceblue":
                tradingsymbol = mapping.get("tradingsymbol", symbol)
                symbol_id = (
                    mapping.get("symbol_id")
                    or mapping.get("security_id")
                )
                order_params = dict(
                    tradingsymbol=tradingsymbol,
                    symbol_id=symbol_id,
                    exchange="NSE",
                    transaction_type=action.upper(),
                    quantity=int(quantity),
                    order_type=map_order_type("MARKET", broker_name),
                    product="MIS",
                    price=None,
                )

            elif broker_name == "finvasia":
                finvasia_symbol = mapping.get("symbol", symbol)
                finvasia_token = mapping.get("token")
                finvasia_exchange = mapping.get("exchange", "NSE")

                if not finvasia_token:
                    raise ValueError(f"Finvasia token not found in symbol map for {symbol}")

                order_params = dict(
                    tradingsymbol=finvasia_symbol,
                    exchange=finvasia_exchange,
                    transaction_type=action.upper(),
                    quantity=int(quantity),
                    order_type=map_order_type("MARKET", broker_name),
                    product="MIS",
                    price=0,
                    token=finvasia_token,
                )
            else:
                tradingsymbol = mapping.get("tradingsymbol", symbol)
                order_params = dict(
                    tradingsymbol=tradingsymbol,
                    exchange="NSE",
                    transaction_type=action.upper(),
                    quantity=int(quantity),
                    order_type=map_order_type("MARKET", broker_name),
                    product="MIS",
                    price=None,
                )
            resp = api.place_order(**order_params)
            if isinstance(resp, dict) and resp.get("status") == "failure":
                status = "FAILED"
                results.append({"client_id": acc.get("client_id"), "status": status, "reason": clean_response_message(resp)})
            else:
                status = "SUCCESS"
                results.append({"client_id": acc.get("client_id"), "status": status})

            record_trade(user_email, symbol, action.upper(), quantity, order_params.get('price'), status)
        except Exception as e:
            results.append({"client_id": acc.get("client_id"), "status": "ERROR", "reason": str(e)})

    return jsonify(results)

@app.route("/api/alerts")
@login_required
def get_alerts():
    user_id = request.args.get("user_id")
    logs = (
        TradeLog.query.filter_by(user_id=user_id, status="ALERT")
        .order_by(TradeLog.id.desc())
        .limit(20)
        .all()
    )

    alerts = [
        {"time": log.timestamp, "message": log.response}
        for log in logs
    ]
    return jsonify(alerts)

@app.route("/register", methods=["POST"])
def register_user():
    data = request.json or {}
    token = data.get("user_id") or uuid.uuid4().hex
    client_id = data.get("client_id")
    access_token = data.get("access_token")
    broker = data.get("broker", "dhan")

    if not all([client_id, access_token]):
        return jsonify({"error": "Missing required fields"}), 400

    user = get_user_by_token(token)
    if not user:
        user = User(webhook_token=token)
        db.session.add(user)
        db.session.commit()

    account = Account.query.filter_by(user_id=user.id, client_id=client_id).first()
    creds = {"access_token": access_token}
    if not account:
        account = Account(user_id=user.id, broker=broker, client_id=client_id, credentials=creds)
        db.session.add(account)
    else:
        account.credentials = creds
    db.session.commit()

    return jsonify({"status": "User registered successfully", "webhook": f"/webhook/{token}"})

@app.route("/logs")
@login_required
def get_logs():
    user_id = request.args.get("user_id")
    rows = (
        TradeLog.query.filter_by(user_id=user_id)
        .order_by(TradeLog.id.desc())
        .limit(100)
        .all()
    )
    
    logs = []
    for row in rows:
        logs.append({
            "timestamp": row.timestamp,
            "user_id": row.user_id,
            "symbol": row.symbol,
            "action": row.action,
            "quantity": row.quantity,
            "status": row.status,
            "response": row.response,
        })

    return jsonify(logs)

@app.route("/api/portfolio/<user_id>")
@login_required
def get_portfolio(user_id):
    """Return live positions for any stored account."""
    user_rec = get_user_by_token(user_id)
    if not user_rec:
        user_rec = User.query.filter_by(email=user_id).first()
    if not user_rec:
        return jsonify({"error": "Invalid user ID"}), 403

    account = get_primary_account(user_rec)
    if not account or not account.credentials:
        return jsonify({"error": "Account not configured"}), 400

    dhan = dhanhq(account.client_id, account.credentials.get("access_token"))
    try:
        positions_resp = dhan.get_positions()
        data = (
            positions_resp.get("data")
            or positions_resp.get("positions")
            or (positions_resp if isinstance(positions_resp, list) else [])
        )
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/orders/<user_id>")
@login_required
def get_orders(user_id):
    """Return recent orders for a stored account."""
    user_rec = get_user_by_token(user_id)
    if not user_rec:
        user_rec = User.query.filter_by(email=user_id).first()
    if not user_rec:
        return jsonify({"error": "Invalid user ID"}), 403

    account = get_primary_account(user_rec)
    if not account or not account.credentials:
        return jsonify({"error": "Account not configured"}), 400

    dhan = dhanhq(account.client_id, account.credentials.get("access_token"))

    try:
        resp = dhan.get_order_list()
        
        if not isinstance(resp, dict) or "data" not in resp:
            return jsonify({"error": "Unexpected response format", "details": resp}), 500

        orders = strip_emojis_from_obj(resp["data"])

        total_trades = len(orders)
        last_order = orders[0] if orders else {}
        total_qty = sum(int(o.get("quantity", 0)) for o in orders)

        return jsonify({
            "orders": orders,
            "summary": {
                "total_trades": total_trades,
                "last_status": last_order.get("orderStatus", "N/A"),
                "total_quantity": total_qty
            }
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/account/<user_id>")
@login_required
def get_account_stats(user_id):
    """Return account margin/fund stats."""
    user_rec = get_user_by_token(user_id)
    if not user_rec:
        user_rec = User.query.filter_by(email=user_id).first()
    if not user_rec:
        return jsonify({"error": "Invalid user ID"}), 403

    account = get_primary_account(user_rec)
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

@app.route("/users", methods=["GET", "POST"])
@login_required
def user_profile():
    username = session.get("user")
    user = User.query.filter_by(email=username).first_or_404()
    message = ""

    if request.method == "POST":
        action = request.form.get("action")

        if action == "save_profile":
            first_name = request.form.get("first_name", "")
            last_name = request.form.get("last_name", "")

            user.name = f"{first_name} {last_name}".strip()

            file = request.files.get("profile_image")
            if file and file.filename:
                image_dir = os.path.join("static", "profile_images")
                os.makedirs(image_dir, exist_ok=True)
                filename = secure_filename(username + "_" + file.filename)
                file.save(os.path.join(image_dir, filename))
                user.profile_image = os.path.join("profile_images", filename)
            message = "Profile updated"

            db.session.commit()

    profile_data = {
        "email": username,
        "first_name": (user.name or "").split(" ")[0] if user.name else "",
        "last_name": (user.name or "").split(" ")[1] if user.name and len(user.name.split(" ")) > 1 else "",
        "plan": user.plan,
    }

    return render_template("user.html", user=profile_data, message=message)

@app.route('/Summary')
@login_required
def summary():
    return render_template("Summary.html")

@app.route("/copy-trading")
@login_required
def copytrading():
    return render_template("copy-trading.html")

@app.route("/groups")
@login_required
def groups_page():
    return render_template("groups.html")

@app.route("/kite/callback")
def kite_callback():
    try:
        from kiteconnect import KiteConnect
        request_token = request.args.get("request_token")
        api_key = request.args.get("api_key")
        api_secret = request.args.get("api_secret")
        client_id = request.args.get("client_id")
        username = request.args.get("username")

        if not all([api_key, api_secret, request_token]):
            return "Missing parameters", 400

        kite = KiteConnect(api_key=api_key)
        session_data = kite.generate_session(request_token, api_secret)
        access_token = session_data["access_token"]

        account = {
            "broker": "zerodha",
            "client_id": client_id,
            "username": username,
            "credentials": {
                "access_token": access_token,
                "api_key": api_key,
                "api_secret": api_secret,
            },
            "status": "Connected",
            "auto_login": True,
            "last_login": datetime.now().isoformat(),
            "role": None,
            "linked_master_id": None,
            "multiplier": 1,
            "copy_status": "Off",
        }

        save_account_to_user(username or client_id, account)

        return "✅ Zerodha account connected!"
    except:
        return "❌ Connection failed", 500

@app.route('/dhan-dashboard')
@login_required
def dhan_dashboard():
    return render_template("dhan-dashboard.html")

@app.route('/api/performance', methods=['GET'])
@admin_login_required
def get_performance_metrics():
    """Get performance metrics for monitoring."""
    try:
        with metrics_lock:
            metrics_copy = dict(performance_metrics)
        
        summary = {}
        for endpoint, metrics in metrics_copy.items():
            if metrics:
                durations = [m['duration'] for m in metrics]
                summary[endpoint] = {
                    'total_requests': len(metrics),
                    'avg_duration': sum(durations) / len(durations),
                    'max_duration': max(durations),
                    'min_duration': min(durations),
                    'recent_requests': metrics[-10:]
                }
        
        return jsonify({
            'summary': summary,
            'timestamp': datetime.utcnow().isoformat()
        }), 200
        
    except Exception as e:
        return safe_json_response({
            "error": "Failed to get performance metrics",
            "details": str(e)
        }, 500)

def signal_handler(sig, frame):
    """Handle graceful shutdown signals"""
    signal_names = {
        signal.SIGINT: 'SIGINT',
        signal.SIGTERM: 'SIGTERM'
    }
    signal_name = signal_names.get(sig, f'Signal {sig}')
    
    logger.info(f"🛑 Received {signal_name}, initiating graceful shutdown...")
    
    shutdown_handler.request_shutdown()
    shutdown_handler.wait_for_requests_to_finish()
    
    if _scheduler and _scheduler.running:
        try:
            logger.info("Stopping background scheduler...")
            _scheduler.shutdown(wait=True)
            logger.info("✅ Scheduler stopped successfully")
        except Exception as e:
            logger.error(f"Error stopping scheduler: {e}")
    
    try:
        logger.info("Closing database connections...")
        db.session.close()
        logger.info("✅ Database connections closed")
    except Exception as e:
        logger.error(f"Error closing database: {e}")
    
    logger.info("👋 Graceful shutdown complete")
    sys.exit(0)

def cleanup_on_exit():
    """Cleanup function called on normal exit"""
    logger.info("🧹 Performing cleanup on exit...")
    
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
    
    try:
        db.session.close()
    except Exception:
        pass

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
atexit.register(cleanup_on_exit)

@app.errorhandler(404)
def not_found(error):
    return render_template("error.html", 
                         error="Page Not Found", 
                         message="The page you requested was not found."), 404

@app.errorhandler(500)
def internal_error(error):
    db.session.rollback()
    return render_template("error.html",
                         error="Internal Server Error",
                         message="An internal server error occurred."), 500

@app.errorhandler(429)
def ratelimit_handler(e):
    return jsonify({
        "error": "Rate limit exceeded",
        "message": "Too many requests. Please try again later.",
        "retry_after": getattr(e, 'retry_after', None)
    }), 429

@app.errorhandler(403)
def forbidden(error):
    return jsonify({
        "error": "Forbidden",
        "message": "You don't have permission to access this resource."
    }), 403

@app.errorhandler(401)
def unauthorized(error):
    return jsonify({
        "error": "Unauthorized",
        "message": "Authentication required."
    }), 401

@app.context_processor
def inject_user():
    user_email = session.get("user")
    if user_email:
        user = User.query.filter_by(email=user_email).first()
        return {"current_user": user}
    return {"current_user": None}

@app.context_processor
def inject_globals():
    return {
        "environment": ENVIRONMENT,
        "app_version": "2.0.0",
        "uptime": format_uptime(),
        "current_time": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
    }

def migrate_json_to_database():
    """Migrate existing JSON data to database (run once)."""
    try:
        accounts_file = os.path.join(DATA_DIR, "accounts.json")
        if os.path.exists(accounts_file):
            with open(accounts_file, 'r') as f:
                data = json.load(f)
            
            for acc_data in data.get("accounts", []):
                client_id = acc_data.get("client_id")
                owner_email = acc_data.get("owner")
                
                if not client_id or not owner_email:
                    continue
                
                user = User.query.filter_by(email=owner_email).first()
                if not user:
                    user = User(email=owner_email)
                    db.session.add(user)
                    db.session.flush()
                
                existing = Account.query.filter_by(
                    user_id=user.id, 
                    client_id=client_id
                ).first()
                
                if not existing:
                    account = Account(
                        user_id=user.id,
                        broker=acc_data.get("broker"),
                        client_id=client_id,
                        username=acc_data.get("username"),
                        token_expiry=acc_data.get("token_expiry"),
                        status=acc_data.get("status", "Connected"),
                        role=acc_data.get("role"),
                        linked_master_id=acc_data.get("linked_master_id"),
                        copy_status=acc_data.get("copy_status", "Off"),
                        multiplier=acc_data.get("multiplier", 1.0),
                        credentials=acc_data.get("credentials"),
                        last_copied_trade_id=acc_data.get("last_copied_trade_id"),
                        auto_login=acc_data.get("auto_login", True),
                        last_login_time=acc_data.get("last_login"),
                        device_number=acc_data.get("device_number")
                    )
                    db.session.add(account)
            
            db.session.commit()
            
            backup_path = accounts_file + ".backup"
            if os.path.exists(accounts_file):
                os.rename(accounts_file, backup_path)
            
    except Exception as e:
        db.session.rollback()

def init_admin():
    """Create admin user if not exists"""
    try:
        with app.app_context():
            admin_user = User.query.filter_by(email=ADMIN_EMAIL).first()
            if not admin_user:
                webhook_token = ''.join(random.choices(string.ascii_letters + string.digits, k=32))
                admin_user = User(
                    email=ADMIN_EMAIL,
                    password_hash=generate_password_hash(ADMIN_PASSWORD),
                    webhook_token=webhook_token
                )
                db.session.add(admin_user)
                db.session.commit()
                logger.info(f"✅ Admin user created: {ADMIN_EMAIL}")
            else:
                logger.info(f"✅ Admin user already exists: {ADMIN_EMAIL}")
    except Exception as e:
        logger.error(f"Failed to initialize admin user: {str(e)}")

# Additional Admin Routes
@app.route('/adminusers')
@admin_login_required
def admin_users():
    users = load_users()
    return render_template('admin/users.html', users=users)

@app.route('/adminbrokers')
@admin_login_required
def admin_brokers():
    accounts = load_accounts()
    broker_names = sorted({acc.broker for acc in accounts if acc.broker})
    return render_template('admin/brokers.html', accounts=accounts, broker_names=broker_names)

@app.route('/admintrades')
@admin_login_required
def admin_trades():
    trades = load_trades()
    return render_template('admin/trades.html', trades=trades)

@app.route('/adminsubscriptions')
@admin_login_required
def admin_subscriptions():
    users = load_users()
    subs = [u for u in users]
    return render_template('admin/subscriptions.html', subscriptions=subs)

@app.route('/adminlogs')
@admin_login_required
def admin_logs():
    webhook_logs, system_logs = load_logs()
    return render_template('admin/logs.html', webhook_logs=webhook_logs, system_logs=system_logs)

@app.route('/adminsettings', methods=['GET', 'POST'])
@admin_login_required
def admin_settings():
    settings = load_settings()
    if request.method == 'POST':
        settings['trading_enabled'] = bool(request.form.get('trading_enabled'))
        for key, value in request.form.items():
            if key == 'trading_enabled':
                continue
            settings[key] = value
        save_settings(settings)
    return render_template('admin/settings.html', settings=settings)

@app.route('/adminprofile')
@admin_login_required
def admin_profile():
    return render_template('admin/profile.html', admin={'email': session.get('admin')})

@app.route('/adminusers/<int:user_id>/suspend', methods=['POST'])
@admin_login_required
def admin_suspend_user(user_id):
    user = User.query.get_or_404(user_id)
    user.plan = 'Suspended'
    db.session.commit()
    flash(f'User {user.email} suspended.')
    return redirect(url_for('admin_users'))

@app.route('/adminusers/<int:user_id>/reset', methods=['POST'])
@admin_login_required
def admin_reset_password(user_id):
    user = User.query.get_or_404(user_id)
    new_pass = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    user.password_hash = generate_password_hash(new_pass)
    db.session.commit()
    flash(f'New password for {user.email}: {new_pass}')
    return redirect(url_for('admin_users'))

@app.route('/adminusers/<int:user_id>')
@admin_login_required
def admin_view_user(user_id):
    user = User.query.get_or_404(user_id)
    return render_template('admin/user_detail.html', user=user)

@app.route('/adminbrokers/<int:account_id>/revoke', methods=['POST'])
@admin_login_required
def admin_revoke_account(account_id):
    account = Account.query.get_or_404(account_id)
    account.status = 'Revoked'
    db.session.commit()
    flash('Account revoked')
    return redirect(url_for('admin_brokers'))

@app.route('/admintrades/<int:trade_id>/retry', methods=['POST'])
@admin_login_required
def admin_retry_trade(trade_id):
    trade = Trade.query.get_or_404(trade_id)
    trade.status = 'Pending'
    db.session.commit()
    flash('Trade marked for retry')
    return redirect(url_for('admin_trades'))

@app.route('/adminsubscriptions/<int:user_id>/change', methods=['POST'])
@admin_login_required
def admin_change_subscription(user_id):
    user = User.query.get_or_404(user_id)
    user.plan = 'Pro' if user.plan != 'Pro' else 'Free'
    db.session.commit()
    flash(f'Plan updated to {user.plan} for {user.email}')
    return redirect(url_for('admin_subscriptions'))

@app.route('/admindashboard')
@admin_login_required
def admin_dashboard_complete():
    users = load_users()
    accounts = load_accounts()
    unique_brokers = {acc.broker for acc in accounts if acc.broker}

    today = date.today()
    start_today = today.strftime('%Y-%m-%d')
    end_today = (today + timedelta(days=1)).strftime('%Y-%m-%d')
    trades_today = Trade.query.filter(Trade.timestamp >= start_today, Trade.timestamp < end_today).count()
    active_users = User.query.filter(User.last_login >= start_today).count()
    failed_trades = Trade.query.filter_by(status='Failed').count()
    
    metrics = {
        'total_users': len(users),
        'active_users': active_users,
        'total_accounts': len(accounts),
        'brokers_connected': len(unique_brokers),
        'trades_today': trades_today,
        'failed_trades': failed_trades,
        'uptime': format_uptime()
    }

    labels = []
    trade_counts = []
    signup_counts = []
    for i in range(5):
        day = today - timedelta(days=4 - i)
        start = day.strftime('%Y-%m-%d')
        end = (day + timedelta(days=1)).strftime('%Y-%m-%d')
        labels.append(day.strftime('%a'))
        trade_counts.append(Trade.query.filter(Trade.timestamp >= start,
                                               Trade.timestamp < end).count())
        signup_counts.append(User.query.filter(User.subscription_start >= start,
                                              User.subscription_start < end).count())

    trade_chart = {'labels': labels, 'data': trade_counts}
    signup_chart = {'labels': labels, 'data': signup_counts}

    broker_list = sorted({acc.broker.lower() for acc in accounts if acc.broker})
    api_status = []
    for name in broker_list:
        url = BROKER_STATUS_URLS.get(name)
        online = check_api(url) if url else False
        api_status.append({'name': name.title(), 'online': online})
    
    return render_template('admin/dashboard.html', 
                         metrics=metrics, 
                         api_status=api_status, 
                         trade_chart=trade_chart, 
                         signup_chart=signup_chart)

@app.route('/adminlogin', methods=['GET', 'POST'])
def admin_login_form():
    error = None

    if request.method == 'POST':
        input_email = request.form.get('email')
        input_password = request.form.get('password')

        if input_email == ADMIN_EMAIL and input_password == ADMIN_PASSWORD:
            session['admin'] = ADMIN_EMAIL
            return redirect(url_for('admin_dashboard_complete'))
        else:
            error = 'Invalid credentials'

    return render_template('admin/login.html', error=error)

@app.route('/adminlogout')
def admin_logout_form():
    session.pop('admin', None)
    return redirect(url_for('admin_login_form'))

# Additional API Routes for Complete Functionality
@app.route('/api/positions/<client_id>', methods=['GET'])
# @limiter.limit("30 per minute")
@require_user
def get_account_positions(client_id):
    """Get positions for a specific account."""
    try:
        if not validate_client_id(client_id):
            return jsonify({"error": "Invalid client_id format"}), 400
            
        account = Account.query.filter_by(
            user_id=request.current_user.id,
            client_id=client_id
        ).first()
        
        if not account:
            return jsonify({"error": "Account not found"}), 404

        if account.status != "Connected":
            return jsonify({"error": "Account not connected"}), 400

        try:
            account_dict = _account_to_dict(account)
            api = broker_api(account_dict)
            
            positions_resp = api.get_positions()
            
            if isinstance(positions_resp, dict):
                positions = (
                    positions_resp.get("data", []) or 
                    positions_resp.get("positions", []) or
                    positions_resp.get("net", [])
                )
            else:
                positions = positions_resp or []
                
            formatted_positions = []
            for pos in positions:
                if isinstance(pos, dict):
                    formatted_pos = {
                        "symbol": pos.get("tradingSymbol") or pos.get("symbol") or pos.get("tsym") or "—",
                        "net_qty": int(pos.get("netQty") or pos.get("net_quantity") or pos.get("netQuantity") or 0),
                        "avg_price": float(pos.get("avgPrice") or pos.get("avg_price") or pos.get("averagePrice") or 0),
                        "ltp": float(pos.get("ltp") or pos.get("lastPrice") or pos.get("last_price") or 0),
                        "pnl": float(pos.get("pnl") or pos.get("unrealizedPnl") or pos.get("unrealized_pnl") or 0),
                        "day_change": float(pos.get("dayChange") or pos.get("day_change") or 0),
                        "day_change_percent": float(pos.get("dayChangePercent") or pos.get("day_change_percent") or 0)
                    }
                    formatted_positions.append(formatted_pos)
            
            return jsonify({
                "client_id": client_id,
                "broker": account.broker,
                "positions": formatted_positions,
                "total_positions": len(formatted_positions),
                "timestamp": datetime.utcnow().isoformat()
            }), 200
            
        except Exception as e:
            logger.error(f"Failed to fetch positions for {client_id}: {str(e)}")
            return safe_json_response({
                "error": "Failed to fetch positions",
                "details": str(e)
            }, 500)
            
    except Exception as e:
        return safe_json_response({
            "error": "Internal server error",
            "details": str(e)
        }, 500)

@app.route('/api/funds/<client_id>', methods=['GET'])
# @limiter.limit("30 per minute")
@require_user
def get_account_funds(client_id):
    """Get fund information for a specific account."""
    try:
        if not validate_client_id(client_id):
            return jsonify({"error": "Invalid client_id format"}), 400
            
        account = Account.query.filter_by(
            user_id=request.current_user.id,
            client_id=client_id
        ).first()
        
        if not account:
            return jsonify({"error": "Account not found"}), 404

        if account.status != "Connected":
            return jsonify({"error": "Account not connected"}), 400

        try:
            account_dict = _account_to_dict(account)
            api = broker_api(account_dict)
            
            if hasattr(api, 'get_fund_limits'):
                funds_resp = api.get_fund_limits()
            elif hasattr(api, 'get_funds'):
                funds_resp = api.get_funds()
            else:
                return jsonify({"error": "Fund information not available for this broker"}), 400
            
            if isinstance(funds_resp, dict):
                funds_data = funds_resp.get("data", funds_resp)
            else:
                funds_data = funds_resp
                
            formatted_funds = {
                "available_balance": extract_balance(funds_data) or 0,
                "used_margin": float(funds_data.get("utilizedAmount") or funds_data.get("used_margin") or 0),
                "available_margin": float(funds_data.get("availableBalance") or funds_data.get("available_margin") or 0),
                "opening_balance": float(funds_data.get("openingBalance") or funds_data.get("opening_balance") or 0),
                "total_collateral": float(funds_data.get("collateral") or funds_data.get("total_collateral") or 0)
            }
            
            return jsonify({
                "client_id": client_id,
                "broker": account.broker,
                "funds": formatted_funds,
                "timestamp": datetime.utcnow().isoformat()
            }), 200
            
        except Exception as e:
            logger.error(f"Failed to fetch funds for {client_id}: {str(e)}")
            return safe_json_response({
                "error": "Failed to fetch funds",
                "details": str(e)
            }, 500)
            
    except Exception as e:
        return safe_json_response({
            "error": "Internal server error",
            "details": str(e)
        }, 500)

@app.route('/api/place-order', methods=['POST'])
# @limiter.limit("60 per minute")
@require_user
def place_manual_order():
    """Place a manual order for a specific account."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        required_fields = ["client_id", "symbol", "action", "quantity"]
        missing_fields = [field for field in required_fields if not data.get(field)]
        
        if missing_fields:
            return jsonify({
                "error": f"Missing required fields: {', '.join(missing_fields)}"
            }), 400
            
        client_id = data.get("client_id")
        symbol = data.get("symbol")
        action = data.get("action").upper()
        quantity = int(data.get("quantity"))
        order_type = data.get("order_type", "MARKET").upper()
        price = float(data.get("price", 0))
        product = data.get("product", "MIS").upper()
        
        if not validate_client_id(client_id):
            return jsonify({"error": "Invalid client_id format"}), 400
            
        if action not in ["BUY", "SELL"]:
            return jsonify({"error": "Invalid action. Must be BUY or SELL"}), 400
            
        if quantity <= 0:
            return jsonify({"error": "Quantity must be greater than 0"}), 400
            
        account = Account.query.filter_by(
            user_id=request.current_user.id,
            client_id=client_id
        ).first()
        
        if not account:
            return jsonify({"error": "Account not found"}), 404

        if account.status != "Connected":
            return jsonify({"error": "Account not connected"}), 400

        try:
            account_dict = _account_to_dict(account)
            api = broker_api(account_dict)
            broker_name = account.broker.lower()
            
            mapping = get_symbol_for_broker(symbol, broker_name)
            
            if broker_name == "dhan":
                security_id = mapping.get("security_id")
                if not security_id:
                    return jsonify({"error": f"Symbol '{symbol}' not found in symbol map"}), 400
                    
                order_params = {
                    "tradingsymbol": symbol,
                    "security_id": security_id,
                    "exchange_segment": mapping.get("exchange_segment", "NSE_EQ"),
                    "transaction_type": action,
                    "quantity": quantity,
                    "order_type": order_type,
                    "product_type": product,
                    "price": price if order_type != "MARKET" else 0
                }
                
            elif broker_name == "zerodha":
                tradingsymbol = mapping.get("tradingsymbol", symbol)
                order_params = {
                    "tradingsymbol": tradingsymbol,
                    "exchange": "NSE",
                    "transaction_type": action,
                    "quantity": quantity,
                    "order_type": order_type,
                    "product": product,
                    "price": price if order_type != "MARKET" else None
                }
                
            elif broker_name == "aliceblue":
                symbol_id = mapping.get("symbol_id")
                if not symbol_id:
                    return jsonify({"error": f"Symbol '{symbol}' not found in symbol map"}), 400
                    
                order_params = {
                    "tradingsymbol": mapping.get("tradingsymbol", symbol),
                    "symbol_id": symbol_id,
                    "exchange": "NSE",
                    "transaction_type": action,
                    "quantity": quantity,
                    "order_type": "MKT" if order_type == "MARKET" else "LMT",
                    "product": product,
                    "price": price if order_type != "MARKET" else 0
                }
                
            elif broker_name == "finvasia":
                token = mapping.get("token")
                if not token:
                    return jsonify({"error": f"Symbol '{symbol}' not found in symbol map"}), 400
                    
                order_params = {
                    "tradingsymbol": mapping.get("symbol", symbol),
                    "exchange": mapping.get("exchange", "NSE"),
                    "transaction_type": action,
                    "quantity": quantity,
                    "order_type": "MKT" if order_type == "MARKET" else "LMT",
                    "product": product,
                    "price": price if order_type != "MARKET" else 0,
                    "token": token
                }
                
            else:
                order_params = {
                    "tradingsymbol": mapping.get("tradingsymbol", symbol),
                    "exchange": "NSE",
                    "transaction_type": action,
                    "quantity": quantity,
                    "order_type": order_type,
                    "product": product,
                    "price": price if order_type != "MARKET" else 0
                }
            
            logger.info(f"Placing manual order: {action} {quantity} {symbol} for {client_id}")
            response = api.place_order(**order_params)
            
            if isinstance(response, dict) and response.get("status") == "failure":
                error_msg = (
                    response.get("remarks") or response.get("error_message") or
                    response.get("errorMessage") or response.get("error") or "Unknown error"
                )
                
                record_trade(request.current_user.email, symbol, action, quantity, price, "FAILED")
                
                return jsonify({
                    "status": "FAILED",
                    "message": error_msg,
                    "order_params": order_params
                }), 400
            
            order_id = (
                response.get("order_id") or response.get("orderId") or
                response.get("id") or response.get("nestOrderNumber") or
                response.get("orderNumber") or response.get("norenordno") or "Unknown"
            )
            
            record_trade(request.current_user.email, symbol, action, quantity, price, "SUCCESS")
            
            return jsonify({
                "status": "SUCCESS",
                "message": "Order placed successfully",
                "order_id": order_id,
                "order_params": order_params,
                "broker_response": response
            }), 200
            
        except Exception as e:
            logger.error(f"Failed to place order for {client_id}: {str(e)}")
            return safe_json_response({
                "error": "Failed to place order",
                "details": str(e)
            }, 500)
            
    except Exception as e:
        return safe_json_response({
            "error": "Internal server error",
            "details": str(e)
        }, 500)

@app.route('/api/cancel-order-single', methods=['POST'])
# @limiter.limit("30 per minute")
@require_user
def cancel_single_order():
    """Cancel a single order for a specific account."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        client_id = data.get("client_id")
        order_id = data.get("order_id")
        
        if not client_id or not order_id:
            return jsonify({"error": "Missing client_id or order_id"}), 400
            
        if not validate_client_id(client_id):
            return jsonify({"error": "Invalid client_id format"}), 400
            
        account = Account.query.filter_by(
            user_id=request.current_user.id,
            client_id=client_id
        ).first()
        
        if not account:
            return jsonify({"error": "Account not found"}), 404

        if account.status != "Connected":
            return jsonify({"error": "Account not connected"}), 400

        try:
            account_dict = _account_to_dict(account)
            api = broker_api(account_dict)
            
            logger.info(f"Cancelling order {order_id} for {client_id}")
            response = api.cancel_order(order_id)
            
            if isinstance(response, dict) and response.get("status") == "failure":
                error_msg = (
                    response.get("remarks") or response.get("error_message") or
                    response.get("errorMessage") or response.get("error") or "Unknown error"
                )
                
                return jsonify({
                    "status": "FAILED",
                    "message": f"Failed to cancel order: {error_msg}",
                    "order_id": order_id
                }), 400
            
            return jsonify({
                "status": "SUCCESS",
                "message": "Order cancelled successfully",
                "order_id": order_id,
                "broker_response": response
            }), 200
            
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id} for {client_id}: {str(e)}")
            return safe_json_response({
                "error": "Failed to cancel order",
                "details": str(e)
            }, 500)
            
    except Exception as e:
        return safe_json_response({
            "error": "Internal server error",
            "details": str(e)
        }, 500)

@app.route('/api/square-off-position', methods=['POST'])
# @limiter.limit("30 per minute")
@require_user
def square_off_single_position():
    """Square off a single position for a specific account."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        client_id = data.get("client_id")
        symbol = data.get("symbol")
        
        if not client_id or not symbol:
            return jsonify({"error": "Missing client_id or symbol"}), 400
            
        if not validate_client_id(client_id):
            return jsonify({"error": "Invalid client_id format"}), 400
            
        account = Account.query.filter_by(
            user_id=request.current_user.id,
            client_id=client_id
        ).first()
        
        if not account:
            return jsonify({"error": "Account not found"}), 404

        if account.status != "Connected":
            return jsonify({"error": "Account not connected"}), 400

        try:
            account_dict = _account_to_dict(account)
            api = broker_api(account_dict)
            
            positions_resp = api.get_positions()
            
            if isinstance(positions_resp, dict):
                positions = (
                    positions_resp.get("data", []) or 
                    positions_resp.get("positions", []) or
                    positions_resp.get("net", [])
                )
            else:
                positions = positions_resp or []
            
            matching_position = None
            for pos in positions:
                pos_symbol = (
                    pos.get("tradingSymbol") or pos.get("symbol") or
                    pos.get("tsym") or pos.get("Tsym") or ""
                ).upper()
                
                if pos_symbol == symbol.upper():
                    net_qty = int(
                        pos.get("netQty") or pos.get("net_quantity") or
                        pos.get("netQuantity") or pos.get("Netqty") or 0
                    )
                    
                    if net_qty != 0:
                        matching_position = pos
                        break
            
            if not matching_position:
                return jsonify({
                    "status": "SKIPPED",
                    "message": f"No open position found for {symbol}",
                    "symbol": symbol
                }), 200
            
            net_qty = int(
                matching_position.get("netQty") or matching_position.get("net_quantity") or
                matching_position.get("netQuantity") or matching_position.get("Netqty") or 0
            )
            
            direction = "SELL" if net_qty > 0 else "BUY"
            abs_qty = abs(net_qty)
            
            broker_name = account.broker.lower()
            mapping = get_symbol_for_broker(symbol, broker_name)
            
            if broker_name == "dhan":
                order_params = {
                    "tradingsymbol": symbol,
                    "security_id": matching_position.get("securityId") or matching_position.get("security_id"),
                    "exchange_segment": matching_position.get("exchangeSegment") or "NSE_EQ",
                    "transaction_type": direction,
                    "quantity": abs_qty,
                    "order_type": "MARKET",
                    "product_type": "INTRADAY",
                    "price": 0
                }
            elif broker_name == "aliceblue":
                order_params = {
                    "tradingsymbol": symbol,
                    "symbol_id": matching_position.get("securityId") or matching_position.get("security_id"),
                    "exchange": "NSE",
                    "transaction_type": direction,
                    "quantity": abs_qty,
                    "order_type": "MKT",
                    "product": "MIS",
                    "price": 0
                }
            elif broker_name == "finvasia":
                order_params = {
                    "tradingsymbol": symbol,
                    "exchange": "NSE",
                    "transaction_type": direction,
                    "quantity": abs_qty,
                    "order_type": "MKT",
                    "product": "MIS",
                    "price": 0,
                    "token": matching_position.get("token", "")
                }
            else:
                order_params = {
                    "tradingsymbol": symbol,
                    "exchange": "NSE",
                    "transaction_type": direction,
                    "quantity": abs_qty,
                    "order_type": "MARKET",
                    "product": "MIS",
                    "price": 0
                }
            
            logger.info(f"Squaring off position: {direction} {abs_qty} {symbol} for {client_id}")
            response = api.place_order(**order_params)
            
            if isinstance(response, dict) and response.get("status") == "failure":
                error_msg = (
                    response.get("remarks") or response.get("error_message") or
                    response.get("errorMessage") or response.get("error") or "Unknown error"
                )
                
                return jsonify({
                    "status": "FAILED",
                    "message": f"Square-off failed: {error_msg}",
                    "symbol": symbol,
                    "position_qty": net_qty
                }), 400
            
            order_id = (
                response.get("order_id") or response.get("orderId") or
                response.get("id") or response.get("nestOrderNumber") or
                response.get("orderNumber") or response.get("norenordno") or "Unknown"
            )
            
            record_trade(request.current_user.email, symbol, direction, abs_qty, 0, "SUCCESS")
            
            return jsonify({
                "status": "SUCCESS",
                "message": "Square-off completed successfully",
                "symbol": symbol,
                "position_qty": net_qty,
                "square_off_direction": direction,
                "square_off_qty": abs_qty,
                "order_id": order_id,
                "broker_response": response
            }), 200
            
        except Exception as e:
            logger.error(f"Failed to square off position {symbol} for {client_id}: {str(e)}")
            return safe_json_response({
                "error": "Failed to square off position",
                "details": str(e)
            }, 500)
            
    except Exception as e:
        return safe_json_response({
            "error": "Internal server error",
            "details": str(e)
        }, 500)

# Initialize components
init_admin()
migrate_json_to_database()

# Start scheduler on app startup (only in production or when explicitly enabled)
if ENVIRONMENT == "production" or os.environ.get("ENABLE_SCHEDULER") == "true":
    start_scheduler()

# Run the application
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = ENVIRONMENT == "development"
    
    logger.info(f"🚀 Starting Copy Trading Application")
    logger.info(f"   Environment: {ENVIRONMENT}")
    logger.info(f"   Port: {port}")
    logger.info(f"   Debug: {debug}")
    logger.info(f"   Database: {db_url}")
    logger.info(f"   Scheduler: {'Enabled' if _scheduler else 'Disabled'}")
    logger.info(f"   Log Level: {LOG_LEVEL}")
    logger.info(f"   Admin Email: {ADMIN_EMAIL}")
    logger.info(f"   Current Time: 2025-07-05 11:26:33 UTC")
    logger.info(f"   Current User: AnkitSG07")
    
    app.run(
        host="0.0.0.0",
        port=port,
        debug=debug,
        threaded=True
    )
