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
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
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

# Authentication decorators (moved early)
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
            # Will be defined after logger
            session.clear()
            return jsonify({"error": "User not found"}), 404
            
        request.current_user = user
        return f(*args, **kwargs)
    return decorated_function

# Request ID Tracking System
class RequestIDFormatter(logging.Formatter):
    """Custom formatter that includes request ID"""
    def format(self, record):
        if hasattr(request, 'request_id'):
            record.request_id = request.request_id
        else:
            record.request_id = 'N/A'
        
        if hasattr(request, 'current_user'):
            record.user_email = getattr(request.current_user, 'email', 'anonymous')
        else:
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

# Update require_user to use logger
def require_user(f):
    """Decorator to get current user and handle authentication"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user_email = session.get("user")
        if not user_email:
            return jsonify({"error": "Authentication required"}), 401
            
        user = User.query.filter_by(email=user_email).first()
        if not user:
            logger.error(f"User not found in database: {user_email}")
            session.clear()
            return jsonify({"error": "User not found"}), 404
            
        request.current_user = user
        return f(*args, **kwargs)
    return decorated_function

# Graceful Shutdown Handler (moved early)
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

# Sentry Error Tracking
try:
    import sentry_sdk
    from sentry_sdk.integrations.flask import FlaskIntegration
    from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration
    from sentry_sdk.integrations.logging import LoggingIntegration
    
    # Configure Sentry if DSN is provided
    if os.environ.get("SENTRY_DSN") and ENVIRONMENT == "production":
        sentry_logging = LoggingIntegration(
            level=logging.INFO,
            event_level=logging.ERROR
        )
        
        sentry_sdk.init(
            dsn=os.environ.get("SENTRY_DSN"),
            integrations=[
                FlaskIntegration(transaction_style='endpoint'),
                SqlalchemyIntegration(),
                sentry_logging
            ],
            traces_sample_rate=0.1,
            environment=ENVIRONMENT,
            release=os.environ.get("APP_VERSION", "2.0.0"),
            before_send=lambda event, hint: event if ENVIRONMENT == "production" else None
        )
        
        logger.info("✅ Sentry error tracking initialized")
    else:
        logger.info("Sentry DSN not configured - error tracking disabled")
        
except ImportError:
    logger.warning("Sentry SDK not installed - error tracking disabled")

# Custom error context
def capture_exception_with_context(exception, extra_context=None):
    """Capture exception with additional context"""
    try:
        if 'sentry_sdk' in globals():
            with sentry_sdk.push_scope() as scope:
                if extra_context:
                    for key, value in extra_context.items():
                        scope.set_tag(key, value)
                
                scope.set_tag("environment", ENVIRONMENT)
                scope.set_tag("component", "copy_trading")
                
                if hasattr(request, 'current_user'):
                    scope.set_user({"email": request.current_user.email})
                
                sentry_sdk.capture_exception(exception)
    except Exception as e:
        logger.error(f"Failed to capture exception to Sentry: {e}")

# Security Configuration
CORS(app, origins=os.environ.get("ALLOWED_ORIGINS", "*").split(","))
if ENVIRONMENT == "production":
    Talisman(app, force_https=True)

# Rate Limiting
limiter = Limiter(
    app,
    key_func=get_remote_address,
    default_limits=["1000 per day", "100 per hour"],
    storage_uri=os.environ.get("REDIS_URL", "memory://")
)

# API Documentation with Flask-RESTX
try:
    from flask_restx import Api, Resource, fields, Namespace
    from werkzeug.middleware.proxy_fix import ProxyFix
    
    app.wsgi_app = ProxyFix(app.wsgi_app)
    
    # Configure API documentation
    api = Api(
        app,
        version='2.0.0',
        title='Copy Trading System API',
        description='Advanced Copy Trading System with Multi-Broker Support',
        doc='/api/docs/' if ENVIRONMENT != 'production' else False,
        contact_email='support@copytrading.com',
        authorizations={
            'sessionAuth': {
                'type': 'apiKey',
                'in': 'cookie',
                'name': 'session'
            }
        },
        security='sessionAuth'
    )
    
    # Define namespaces
    auth_ns = api.namespace('auth', description='Authentication operations')
    accounts_ns = api.namespace('accounts', description='Account management')
    trading_ns = api.namespace('trading', description='Trading operations')
    admin_ns = api.namespace('admin', description='Admin operations')
    
    # Define models for documentation
    account_model = api.model('Account', {
        'client_id': fields.String(required=True, description='Unique client identifier'),
        'broker': fields.String(required=True, description='Broker name'),
        'username': fields.String(required=True, description='Account username'),
        'status': fields.String(description='Account status'),
        'role': fields.String(description='Account role (master/child)'),
        'copy_status': fields.String(description='Copy trading status'),
        'multiplier': fields.Float(description='Trading multiplier')
    })
    
    trade_model = api.model('Trade', {
        'symbol': fields.String(required=True, description='Trading symbol'),
        'action': fields.String(required=True, description='Trade action (BUY/SELL)'),
        'quantity': fields.Integer(required=True, description='Trade quantity'),
        'price': fields.Float(description='Trade price'),
        'status': fields.String(description='Trade status')
    })
    
    webhook_model = api.model('Webhook', {
        'symbol': fields.String(required=True, description='Trading symbol'),
        'action': fields.String(required=True, description='Trade action (BUY/SELL)'),
        'quantity': fields.Integer(required=True, description='Trade quantity'),
        'price': fields.Float(description='Optional trade price')
    })
    
    # Document existing endpoints
    @auth_ns.route('/login')
    class Login(Resource):
        def post(self):
            """User login"""
            pass
    
    @accounts_ns.route('/')
    class AccountList(Resource):
        @api.marshal_list_with(account_model)
        def get(self):
            """Get user accounts"""
            pass
    
    @trading_ns.route('/webhook/<string:user_id>')
    class WebhookEndpoint(Resource):
        @api.expect(webhook_model)
        def post(self, user_id):
            """Process trading webhook"""
            pass
    
    logger.info(f"✅ API documentation available at /api/docs/ (env: {ENVIRONMENT})")
    
except ImportError:
    logger.warning("Flask-RESTX not installed - API documentation disabled")
    api = None

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
                
                # Capture to Sentry with performance context
                capture_exception_with_context(e, {
                    'endpoint': endpoint,
                    'method': method,
                    'duration': duration,
                    'performance_issue': duration > threshold_seconds
                })
                
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

# Request tracking middleware (moved here after all setup)
@app.before_request
def before_request_tracking():
    request.request_id = str(uuid.uuid4())[:8]
    request.start_time = time.time()
    
    # Log incoming request
    logger.info(
        f"REQUEST START: {request.method} {request.path} "
        f"from {request.remote_addr} "
        f"User-Agent: {request.headers.get('User-Agent', 'Unknown')[:50]}"
    )

@app.after_request
def after_request_tracking(response):
    if hasattr(request, 'start_time') and hasattr(request, 'request_id'):
        duration = time.time() - request.start_time
        
        # Log response
        logger.info(
            f"REQUEST END: {request.method} {request.path} "
            f"Status: {response.status_code} "
            f"Duration: {duration:.3f}s "
            f"Size: {response.content_length or 0} bytes"
        )
        
        # Add headers
        response.headers['X-Request-ID'] = request.request_id
        response.headers['X-Response-Time'] = f"{duration * 1000:.1f}ms"
    
    return response

# Sentry user context
@app.before_request
def set_sentry_user():
    if 'user' in session:
        try:
            sentry_sdk.set_user({
                "email": session['user'],
                "ip_address": request.remote_addr
            })
        except (NameError, AttributeError):
            pass

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

# Performance metrics endpoint
@app.route('/api/admin/performance-metrics')
@admin_login_required
def get_performance_metrics():
    """Get performance metrics for monitoring"""
    try:
        with metrics_lock:
            metrics_summary = {}
            
            for endpoint, measurements in performance_metrics.items():
                if measurements:
                    durations = [m['duration'] for m in measurements]
                    metrics_summary[endpoint] = {
                        'count': len(measurements),
                        'avg_duration': sum(durations) / len(durations),
                        'max_duration': max(durations),
                        'min_duration': min(durations),
                        'slow_requests': len([d for d in durations if d > 2.0]),
                        'recent_requests': measurements[-10:]
                    }
        
        return jsonify({
            'metrics': metrics_summary,
            'timestamp': datetime.utcnow().isoformat(),
            'uptime': format_uptime()
        })
        
    except Exception as e:
        logger.error(f"Failed to get performance metrics: {str(e)}")
        return safe_json_response({"error": str(e)}, 500)

# Request tracking endpoint
@app.route('/api/admin/request-logs')
@admin_login_required
def get_request_logs():
    """Get recent request logs for monitoring"""
    try:
        log_file = os.path.join(DATA_DIR, "logs", "app.log")
        if not os.path.exists(log_file):
            return jsonify({"logs": [], "message": "No log file found"})
        
        # Read last 100 lines
        with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
            recent_lines = lines[-100:] if len(lines) > 100 else lines
        
        # Parse request logs
        request_logs = []
        for line in recent_lines:
            if 'REQUEST START:' in line or 'REQUEST END:' in line:
                request_logs.append(line.strip())
        
        return jsonify({
            "logs": request_logs[-50:],  # Last 50 request logs
            "total_lines": len(lines),
            "timestamp": datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Failed to get request logs: {str(e)}")
        return safe_json_response({"error": str(e)}, 500)

# Health Check Endpoint
@app.route('/health')
def health_check():
    """Health check endpoint for monitoring"""
    try:
        # Test database connection
        db.session.execute('SELECT 1')
        
        # Check scheduler status
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
@limiter.limit("5 per minute")
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
            
        # Validate client_id format
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
@limiter.limit("30 per minute")
@require_user
def get_order_book(client_id):
    """Get order book for a master account - Complete Database Version."""
    logger.info(f"Fetching order book for client {client_id}")
    
    try:
        # Validate client_id format
        if not validate_client_id(client_id):
            return jsonify({"error": "Invalid client_id format"}), 400
            
        # Find master account that belongs to current user
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
        logger.info(f"Found master account: {master['broker']} - {master['username']}")

        try:
            api = broker_api(master)
            logger.debug(f"Initialized {master['broker']} API for {client_id}")
        except Exception as e:
            logger.error(f"Failed to initialize broker API: {str(e)}")
            return safe_json_response({
                "error": "Failed to initialize broker connection",
                "details": str(e)
            }, 500)

        try:
            broker_name = master.get('broker', '').lower()
            
            if broker_name == "aliceblue" and hasattr(api, "get_trade_book"):
                logger.debug("Using AliceBlue trade book")
                orders_resp = api.get_trade_book()
            else:
                logger.debug("Using standard order list")
                orders_resp = api.get_order_list()
                
            if isinstance(orders_resp, dict) and orders_resp.get("status") == "failure":
                error_msg = orders_resp.get("error", "Failed to fetch orders")
                logger.error(f"API error: {error_msg}")
                return jsonify({"error": error_msg}), 500
                
            orders = parse_order_list(orders_resp)
            logger.info(f"Fetched {len(orders)} raw orders from {broker_name}")
            
        except Exception as e:
            logger.error(f"Failed to fetch orders: {str(e)}")
            return safe_json_response({
                "error": "Failed to fetch orders",
                "details": str(e)
            }, 500)
            
        orders = strip_emojis_from_obj(orders)
        
        if not isinstance(orders, list):
            logger.warning(f"Invalid orders format: {type(orders)}")
            orders = []
        
        formatted = []
        for order in orders:
            if not isinstance(order, dict):
                logger.warning(f"Skipping invalid order format: {type(order)}")
                continue
                
            try:
                order_id = (
                    order.get("orderId")
                    or order.get("order_id") 
                    or order.get("id")
                    or order.get("orderNumber")
                    or order.get("NOrdNo")
                    or order.get("Nstordno")
                    or order.get("nestOrderNumber")
                    or order.get("ExchOrdID")
                    or order.get("norenordno")
                    or "N/A"
                )

                side = order.get("transactionType") or order.get("side") or order.get("Trantype") or "N/A"
                
                status_raw = (
                    order.get("orderStatus")
                    or order.get("report_type")
                    or order.get("status")
                    or order.get("Status")
                    or ("FILLED" if order.get("tradedQty") else "PENDING")
                )
                
                status = str(status_raw).upper() if status_raw else "UNKNOWN"
                
                symbol = (
                    order.get("tradingSymbol")
                    or order.get("symbol")
                    or order.get("Tsym") 
                    or order.get("tsym") 
                    or order.get("Trsym")
                    or "—"
                )

                product_type = (
                    order.get("productType")
                    or order.get("product") 
                    or order.get("Pcode") 
                    or order.get("prd")
                    or "—"
                )

                try:
                    placed_qty = int(
                        order.get("orderQuantity")
                        or order.get("qty")
                        or order.get("Qty")
                        or order.get("quantity")
                        or 0
                    )
                except (TypeError, ValueError):
                    placed_qty = 0

                try:
                    filled_qty = int(
                        order.get("filledQuantity")
                        or order.get("filled_qty")
                        or order.get("filledQty")
                        or order.get("Filledqty")
                        or order.get("Fillshares")
                        or order.get("fillshares")
                        or order.get("tradedQty")
                        or order.get("executedQty")
                        or (placed_qty if status in ["FILLED", "COMPLETE", "TRADED"] else 0)
                    )
                except (TypeError, ValueError):
                    filled_qty = 0

                try:
                    avg_price = float(
                        order.get("averagePrice")
                        or order.get("avg_price")
                        or order.get("Avgprc")
                        or order.get("avgprc")
                        or order.get("Prc")
                        or order.get("tradePrice")
                        or order.get("tradedPrice")
                        or order.get("executedPrice")
                        or 0
                    )
                except (TypeError, ValueError):
                    avg_price = 0.0

                order_time_raw = (
                    order.get("orderTimestamp")
                    or order.get("order_time")
                    or order.get("create_time")
                    or order.get("orderDateTime")
                    or order.get("ExchConfrmtime")
                    or order.get("norentm")
                    or order.get("exchtime")
                    or ""
                )
                
                order_time = str(order_time_raw).replace("T", " ").split(".")[0] if order_time_raw else "—"

                remarks = (
                    order.get("remarks")
                    or order.get("Remark")
                    or order.get("orderTag")
                    or order.get("usercomment")
                    or order.get("Usercomments")
                    or order.get("remarks1")
                    or order.get("rejreason")
                    or "—"
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
        # Validate client_id format
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
@limiter.limit("60 per minute")
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
                    response.get("remarks")
                    or response.get("error_message")
                    or response.get("errorMessage")
                    or response.get("error")
                    or "Unknown error"
                )
                
                logger.error(f"Order failed: {reason}")
                record_trade(
                    user_id, 
                    symbol, 
                    action.upper(), 
                    quantity, 
                    order_params.get('price'), 
                    status
                )
                
                return jsonify({
                    "status": status,
                    "reason": reason
                }), 400

            status = "SUCCESS"
            success_msg = response.get("remarks", "Trade placed successfully")
            
            logger.info(f"Order placed successfully: {success_msg}")
            
            record_trade(
                user_id, 
                symbol, 
                action.upper(), 
                quantity, 
                order_params.get('price'), 
                status
            )
            
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
@limiter.limit("10 per minute")
@require_user
def master_squareoff():
    """Square off child orders for a master order - Complete Database Version."""
    logger.info("Processing master square-off request")
    
    try:
        data = request.get_json()
        if not data:
            logger.error("No data provided in master square-off request")
            return jsonify({"error": "No data provided"}), 400
            
        master_order_id = data.get("master_order_id")
        if not master_order_id:
            logger.error("Missing master_order_id in request")
            return jsonify({"error": "Missing master_order_id"}), 400

        # Find active order mappings for this master order that belong to current user
        active_mappings = db.session.query(OrderMapping).join(
            Account, Account.client_id == OrderMapping.child_client_id
        ).filter(
            OrderMapping.master_order_id == master_order_id,
            OrderMapping.status == "ACTIVE",
            Account.user_id == request.current_user.id
        ).all()
        
        if not active_mappings:
            logger.info(f"No active child orders found for master order {master_order_id}")
            return jsonify({
                "message": "No active child orders found for this master order",
                "master_order_id": master_order_id,
                "active_mappings": 0,
                "results": []
            }), 200

        logger.info(f"Found {len(active_mappings)} active mappings for master order {master_order_id}")

        child_mappings = {}
        for mapping in active_mappings:
            child_id = mapping.child_client_id
            if child_id not in child_mappings:
                child_mappings[child_id] = []
            child_mappings[child_id].append(mapping)

        results = []
        successful_squareoffs = 0
        failed_squareoffs = 0

        # Pre-load all child accounts to avoid N+1 queries
        child_ids = list(child_mappings.keys())
        child_accounts = {a.client_id: a for a in Account.query.filter(
            Account.client_id.in_(child_ids),
            Account.user_id == request.current_user.id
        ).all()}

        for child_id, mappings in child_mappings.items():
            logger.info(f"Processing square-off for child {child_id} with {len(mappings)} positions")
            
            child_account = child_accounts.get(child_id)
            
            if not child_account:
                logger.error(f"Child account not found: {child_id}")
                for mapping in mappings:
                    results.append({
                        "child_client_id": child_id,
                        "symbol": mapping.symbol,
                        "master_order_id": mapping.master_order_id,
                        "child_order_id": mapping.child_order_id,
                        "status": "ERROR",
                        "message": "Child account not found in database",
                        "mapping_id": mapping.id
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
                        
                    logger.debug(f"Retrieved {len(positions)} positions for {child_id}")
                    
                except Exception as e:
                    logger.error(f"Failed to fetch positions for {child_id}: {str(e)}")
                    for mapping in mappings:
                        results.append({
                            "child_client_id": child_id,
                            "symbol": mapping.symbol,
                            "master_order_id": mapping.master_order_id,
                            "child_order_id": mapping.child_order_id,
                            "status": "ERROR",
                            "message": f"Failed to fetch positions: {str(e)}",
                            "mapping_id": mapping.id
                        })
                        failed_squareoffs += 1
                    continue

                for mapping in mappings:
                    symbol = mapping.symbol
                    logger.debug(f"Processing symbol {symbol} for child {child_id}")
                    
                    matching_position = None
                    for position in positions:
                        pos_symbol = (
                            position.get("tradingSymbol") or 
                            position.get("symbol") or
                            position.get("tsym") or
                            position.get("Tsym") or
                            ""
                        ).upper()
                        
                        if pos_symbol == symbol.upper():
                            net_qty = int(
                                position.get("netQty") or
                                position.get("net_quantity") or
                                position.get("netQuantity") or
                                position.get("Netqty") or
                                0
                            )
                            
                            if net_qty != 0:
                                matching_position = position
                                break

                    if not matching_position:
                        logger.info(f"No open position found for {symbol} in child {child_id}")
                        results.append({
                            "child_client_id": child_id,
                            "symbol": symbol,
                            "master_order_id": mapping.master_order_id,
                            "child_order_id": mapping.child_order_id,
                            "status": "SKIPPED",
                            "message": f"No open position found for {symbol}",
                            "mapping_id": mapping.id
                        })
                        continue

                    net_qty = int(
                        matching_position.get("netQty") or
                        matching_position.get("net_quantity") or
                        matching_position.get("netQuantity") or
                        matching_position.get("Netqty") or
                        0
                    )
                    
                    direction = "SELL" if net_qty > 0 else "BUY"
                    abs_qty = abs(net_qty)
                    
                    broker_name = child_account.broker.lower()
                    
                    if broker_name == "dhan":
                        order_params = {
                            "tradingsymbol": symbol,
                            "security_id": matching_position.get("securityId") or matching_position.get("security_id"),
                            "exchange_segment": matching_position.get("exchangeSegment") or matching_position.get("exchange_segment") or "NSE_EQ",
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
                    elif broker_name == "zerodha":
                        order_params = {
                            "tradingsymbol": symbol,
                            "exchange": "NSE",
                            "transaction_type": direction,
                            "quantity": abs_qty,
                            "order_type": "MARKET",
                            "product": "MIS",
                            "price": 0
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
                        logger.info(f"Placing square-off order for {child_id}: {direction} {abs_qty} {symbol}")
                        square_off_response = broker_api_instance.place_order(**order_params)
                        
                        if isinstance(square_off_response, dict) and square_off_response.get("status") == "failure":
                            error_msg = (
                                square_off_response.get("remarks") or
                                square_off_response.get("error") or
                                square_off_response.get("message") or
                                "Unknown square-off error"
                            )
                            logger.error(f"Square-off failed for {child_id} {symbol}: {error_msg}")
                            
                            results.append({
                                "child_client_id": child_id,
                                "symbol": symbol,
                                "master_order_id": mapping.master_order_id,
                                "child_order_id": mapping.child_order_id,
                                "status": "FAILED",
                                "message": f"Square-off failed: {error_msg}",
                                "mapping_id": mapping.id,
                                "position_qty": net_qty,
                                "square_off_direction": direction
                            })
                            failed_squareoffs += 1
                            
                        else:
                            mapping.status = "SQUARED_OFF"
                            mapping.remarks = f"Squared off on {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}"
                            
                            square_off_order_id = (
                                square_off_response.get("order_id") or
                                square_off_response.get("orderId") or
                                square_off_response.get("id") or
                                "Unknown"
                            )
                            
                            logger.info(f"Successfully squared off {symbol} for {child_id}, order ID: {square_off_order_id}")
                            
                            results.append({
                                "child_client_id": child_id,
                                "symbol": symbol,
                                "master_order_id": mapping.master_order_id,
                                "child_order_id": mapping.child_order_id,
                                "status": "SUCCESS",
                                "message": "Square-off completed successfully",
                                "mapping_id": mapping.id,
                                "position_qty": net_qty,
                                "square_off_direction": direction,
                                "square_off_order_id": square_off_order_id,
                                "square_off_qty": abs_qty
                            })
                            successful_squareoffs += 1
                            
                    except Exception as e:
                        logger.error(f"Error placing square-off order for {child_id} {symbol}: {str(e)}")
                        results.append({
                            "child_client_id": child_id,
                            "symbol": symbol,
                            "master_order_id": mapping.master_order_id,
                            "child_order_id": mapping.child_order_id,
                            "status": "ERROR",
                            "message": f"Order placement failed: {str(e)}",
                            "mapping_id": mapping.id,
                            "position_qty": net_qty,
                            "square_off_direction": direction
                        })
                        failed_squareoffs += 1

            except Exception as e:
                logger.error(f"Error processing child {child_id}: {str(e)}")
                for mapping in mappings:
                    results.append({
                        "child_client_id": child_id,
                        "symbol": mapping.symbol,
                        "master_order_id": mapping.master_order_id,
                        "child_order_id": mapping.child_order_id,
                        "status": "ERROR",
                        "message": f"Child processing failed: {str(e)}",
                        "mapping_id": mapping.id
                    })
                    failed_squareoffs += 1

        try:
            db.session.commit()
            logger.info(f"Successfully updated {successful_squareoffs} mappings to SQUARED_OFF status")
        except Exception as e:
            logger.error(f"Failed to commit mapping updates: {str(e)}")
            db.session.rollback()
            return safe_json_response({
                "error": "Failed to update mapping statuses",
                "details": str(e),
                "results": results
            }, 500)

        try:
            log_entry = SystemLog(
                timestamp=datetime.utcnow().isoformat(),
                level="INFO",
                message=f"Master square-off completed: {master_order_id} - {successful_squareoffs} success, {failed_squareoffs} failed",
                user_id=str(request.current_user.id),
                details=json.dumps({
                    "action": "master_squareoff",
                    "master_order_id": master_order_id,
                    "total_mappings": len(active_mappings),
                    "successful_squareoffs": successful_squareoffs,
                    "failed_squareoffs": failed_squareoffs,
                    "children_processed": len(child_mappings),
                    "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                })
            )
            db.session.add(log_entry)
            db.session.commit()
        except Exception as e:
            logger.warning(f"Failed to log master square-off action: {str(e)}")

        response_data = {
            "message": f"Master square-off completed for order {master_order_id}",
            "master_order_id": master_order_id,
            "summary": {
                "total_mappings": len(active_mappings),
                "children_processed": len(child_mappings),
                "successful_squareoffs": successful_squareoffs,
                "failed_squareoffs": failed_squareoffs,
                "success_rate": f"{(successful_squareoffs/len(active_mappings)*100):.1f}%" if active_mappings else "0%"
            },
            "results": results,
            "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        }

        if failed_squareoffs == 0:
            return jsonify(response_data), 200
        elif successful_squareoffs > 0:
            return jsonify(response_data), 207
        else:
            response_data["error"] = "All square-off attempts failed"
            return jsonify(response_data), 500

    except Exception as e:
        logger.error(f"Unexpected error in master_squareoff: {str(e)}")
        return safe_json_response({
            "error": "Internal server error",
            "details": str(e)
        }, 500)

@app.route('/api/master-orders', methods=['GET'])
@limiter.limit("60 per minute")
@require_user
def get_master_orders():
    """Get all master orders with their child order details."""
    logger.info("Fetching master orders")
    
    try:
        master_id_filter = request.args.get("master_id")
        status_filter = request.args.get("status", "").upper()
        
        # Build query with user filter
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
            master_id = entry.master_client_id
            mid = entry.master_order_id
            if mid not in master_summary:
                master_summary[mid] = {
                    "master_order_id": mid,
                    "symbol": entry.symbol,
                    "master_client_id": master_id,
                    "master_broker": entry.master_broker or "Unknown",
                    "action": entry.action if hasattr(entry, 'action') else 'UNKNOWN',
                    "quantity": entry.quantity if hasattr(entry, 'quantity') else 0,
                    "status": 'ACTIVE',
                    "total_children": 0,
                    "child_statuses": [],
                    "children": [],
                    "timestamp": entry.timestamp or '',
                    "summary": {
                        'total': 0,
                        'active': 0,
                        'completed': 0,
                        'failed': 0,
                        'cancelled': 0
                    }
                }
            child = {
                'child_client_id': entry.child_client_id,
                'child_broker': entry.child_broker or 'Unknown',
                'status': entry.status,
                'order_id': entry.child_order_id or '',
                'timestamp': entry.child_timestamp or '',
                'remarks': entry.remarks or '—',
                'multiplier': entry.multiplier
            }
            master_summary[mid]['children'].append(child)

            status = entry.status.upper() if entry.status else ''
            ms = master_summary[mid]['summary']
            ms['total'] += 1
            if status == 'ACTIVE':
                ms['active'] += 1
            elif status == 'COMPLETED':
                ms['completed'] += 1
            elif status == 'FAILED':
                ms['failed'] += 1
            elif status == 'CANCELLED':
                ms['cancelled'] += 1

            master_summary[mid]['child_statuses'].append(status)
            master_summary[mid]['total_children'] += 1

            if ms['active'] > 0:
                master_summary[mid]['status'] = 'ACTIVE'
            elif ms['failed'] == ms['total']:
                master_summary[mid]['status'] = 'FAILED'
            elif ms['cancelled'] == ms['total']:
                master_summary[mid]['status'] = 'CANCELLED'
            elif ms['completed'] == ms['total']:
                master_summary[mid]['status'] = 'COMPLETED'
            else:
                master_summary[mid]['status'] = 'PARTIAL'
                
        orders = list(master_summary.values())
        orders.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        
        overall_summary = {
            'total_orders': len(orders),
            'active_orders': sum(1 for o in orders if o['status'] == 'ACTIVE'),
            'completed_orders': sum(1 for o in orders if o['status'] == 'COMPLETED'),
            'failed_orders': sum(1 for o in orders if o['status'] == 'FAILED'),
            'cancelled_orders': sum(1 for o in orders if o['status'] == 'CANCELLED'),
            'partial_orders': sum(1 for o in orders if o['status'] == 'PARTIAL')
        }
        
        logger.info(f"Successfully fetched {len(orders)} master orders")
        return jsonify({
            'orders': orders,
            'summary': overall_summary
        }), 200

    except Exception as e:
        logger.error(f"Unexpected error in get_master_orders: {str(e)}")
        return safe_json_response({
            "error": "Internal server error",
            "details": str(e)
        }, 500)

@app.route("/api/zerodha-login-url")
def zerodha_login_url_route():
    api_key = request.args.get("api_key")
    if not api_key:
        return jsonify({"error": "api_key required"}), 400
    if KiteConnect is None:
        return jsonify({"error": "kiteconnect not installed"}), 500
    kite = KiteConnect(api_key=api_key)
    return jsonify({"login_url": kite.login_url()})

@app.route('/api/init-zerodha-login', methods=['POST'])
@limiter.limit("10 per minute")
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
@limiter.limit("10 per minute")
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
    redirect_uri = cred.get('redirect_uri')

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

@app.route("/kite/callback")
def kite_callback():
    from kiteconnect import KiteConnect
    request_token = request.args.get("request_token")
    api_key = request.args.get("api_key")
    api_secret = request.args.get("api_secret")
    client_id = request.args.get("client_id")
    username = request.args.get("username")

    if not all([api_key, api_secret, request_token]):
        return "Missing parameters", 400

    kite = KiteConnect(api_key=api_key)
    session = kite.generate_session(request_token, api_secret)
    access_token = session["access_token"]

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

@app.route('/api/square-off', methods=['POST'])
@limiter.limit("20 per minute")
@require_user
def square_off():
    data = request.json
    client_id = data.get("client_id")
    symbol = data.get("symbol")
    is_master = data.get("is_master", False)

    if not client_id or not symbol:
        return jsonify({"error": "Missing client_id or symbol"}), 400

    if not validate_client_id(client_id):
        return jsonify({"error": "Invalid client_id format"}), 400

    # Find account that belongs to current user
    account = Account.query.filter_by(
        user_id=request.current_user.id,
        client_id=client_id
    ).first()

    if not account:
        return jsonify({"error": "Account not found"}), 404

    account_dict = _account_to_dict(account)

    if is_master and account.role == "master":
        api = broker_api(account_dict)
        try:
            positions_resp = api.get_positions()
            positions = positions_resp.get("data", []) if isinstance(positions_resp, dict) else []
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
            save_log(account.client_id, symbol, "SQUARE_OFF", qty, "SUCCESS", str(resp))
            return jsonify({"message": "✅ Master square-off placed", "details": str(resp)}), 200
        except Exception as e:
            return safe_json_response({"error": str(e)}, 500)
    else:
        # Square off all children under master (for current user only)
        children = Account.query.filter_by(
            user_id=request.current_user.id,
            role='child',
            linked_master_id=client_id,
            copy_status='On'
        ).all()
        
        results = []
        for child in children:
            try:
                child_dict = _account_to_dict(child)
                api = broker_api(child_dict)
                positions_resp = api.get_positions()
                positions = positions_resp.get('data', []) if isinstance(positions_resp, dict) else []
                match = next((p for p in positions if p.get('tradingSymbol', '').upper() == symbol.upper()), None)

                if not match or int(match.get('netQty', 0)) == 0:
                    results.append(f"Child {child.client_id} → Skipped (no active position in {symbol})")
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
                    results.append(f"Child {child.client_id} → FAILED: {msg}")
                    save_log(child.client_id, symbol, "SQUARE_OFF", quantity, "FAILED", msg)
                else:
                    results.append(f"Child {child.client_id} → SUCCESS")
                    save_log(child.client_id, symbol, "SQUARE_OFF", quantity, "SUCCESS", str(response))

            except Exception as e:
                error_msg = str(e)
                results.append(f"Child {child.client_id} → ERROR: {error_msg}")
                save_log(child.client_id, symbol, "SQUARE_OFF", 0, "ERROR", error_msg)

        return jsonify({"message": "🔁 Square-off for all children completed", "details": results}), 200

@app.route('/api/order-mappings', methods=['GET'])
@limiter.limit("60 per minute")
@require_user
def get_order_mappings():
    try:
        # Filter mappings to only show those belonging to current user
        mappings_query = db.session.query(OrderMapping).join(
            Account, Account.client_id == OrderMapping.master_client_id
        ).filter(Account.user_id == request.current_user.id)
        
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
            for m in mappings_query.all()
        ]
        return jsonify(mappings), 200

    except Exception as e:
        logger.error(f"Error in get_order_mappings: {str(e)}")
        return safe_json_response({"error": str(e)}, 500)

@app.route('/api/child-orders')
@require_user
def child_orders():
    master_order_id = request.args.get('master_order_id')
    
    # Build query with user filter
    mappings_query = db.session.query(OrderMapping).join(
        Account, Account.client_id == OrderMapping.master_client_id
    ).filter(Account.user_id == request.current_user.id)
    
    if master_order_id:
        mappings_query = mappings_query.filter(OrderMapping.master_order_id == master_order_id)
        
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
        for m in mappings_query.all()
    ]
    return jsonify(data)

@app.route('/api/cancel-order', methods=['POST'])
@limiter.limit("20 per minute")
@require_user
def cancel_order():
    """SECURITY FIX: Only access user's own accounts"""
    try:
        data = request.json
        master_order_id = data.get("master_order_id")
        
        # Find mappings that belong to current user
        mappings = db.session.query(OrderMapping).join(
            Account, Account.client_id == OrderMapping.child_client_id
        ).filter(
            OrderMapping.master_order_id == master_order_id,
            OrderMapping.status == "ACTIVE",
            Account.user_id == request.current_user.id
        ).all()
        
        if not mappings:
            return jsonify({"message": "No active child orders found for this master order."}), 200

        results = []
        # Get user's accounts only
        user_accounts = {a.client_id: a for a in request.current_user.accounts}

        for mapping in mappings:
            child_id = mapping.child_client_id
            child_order_id = mapping.child_order_id
            found = user_accounts.get(child_id)

            if not found:
                results.append(f"{child_id} → ❌ Client not found")
                continue

            try:
                found_dict = _account_to_dict(found)
                api = broker_api(found_dict)
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
        logger.error(f"Error in cancel_order: {str(e)}")
        return safe_json_response({"error": str(e)}, 500)

@app.route('/api/change-master', methods=['POST'])
@limiter.limit("10 per minute")
@require_user
def change_master():
    """Change master for a child account - Complete Database Version."""
    logger.info("Processing change master request")
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        child_id = data.get("child_id")
        new_master_id = data.get("new_master_id")
        
        if not child_id or not new_master_id:
            logger.error(f"Missing required fields: child_id={bool(child_id)}, new_master_id={bool(new_master_id)}")
            return jsonify({"error": "Missing child_id or new_master_id"}), 400

        if not validate_client_id(child_id) or not validate_client_id(new_master_id):
            return jsonify({"error": "Invalid client_id format"}), 400

        child_account = Account.query.filter_by(
            user_id=request.current_user.id,
            client_id=child_id
        ).first()
        
        if not child_account:
            logger.error(f"Child account not found: {child_id}")
            return jsonify({"error": "Child account not found"}), 404

        if child_account.role != "child":
            logger.error(f"Account {child_id} is not configured as child (role: {child_account.role})")
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
            user_id=request.current_user.id,
            client_id=new_master_id,
            role='master'
        ).first()
        
        if not new_master_account:
            logger.error(f"New master account not found or not accessible: {new_master_id}")
            return jsonify({
                "error": "New master account not found or not accessible",
                "master_id": new_master_id
            }), 404

        if old_master_id == new_master_id:
            logger.info(f"Child {child_id} already linked to master {new_master_id}")
            return jsonify({
                "message": f"Child {child_id} is already linked to master {new_master_id}",
                "no_change_needed": True,
                "current_master
