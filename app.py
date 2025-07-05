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
# Request ID Tracking System
from datetime import datetime

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

# Update the logging setup function
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

# Request tracking middleware
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
        
# Sentry Error Tracking
try:
    import sentry_sdk
    from sentry_sdk.integrations.flask import FlaskIntegration
    from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration
    from sentry_sdk.integrations.logging import LoggingIntegration
    
    # Configure Sentry if DSN is provided
    if os.environ.get("SENTRY_DSN") and ENVIRONMENT == "production":
        sentry_logging = LoggingIntegration(
            level=logging.INFO,        # Capture info and above as breadcrumbs
            event_level=logging.ERROR  # Send errors as events
        )
        
        sentry_sdk.init(
            dsn=os.environ.get("SENTRY_DSN"),
            integrations=[
                FlaskIntegration(transaction_style='endpoint'),
                SqlalchemyIntegration(),
                sentry_logging
            ],
            traces_sample_rate=0.1,  # 10% of transactions for performance monitoring
            environment=ENVIRONMENT,
            release=os.environ.get("APP_VERSION", "2.0.0"),
            before_send=lambda event, hint: event if ENVIRONMENT == "production" else None
        )
        
        # Set user context
        @app.before_request
        def set_sentry_user():
            if 'user' in session:
                try:
                    sentry_sdk.set_user({
                        "email": session['user'],
                        "ip_address": request.remote_addr
                    })
                except (NameError, AttributeError):
                    pass  # Sentry not available
        
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

# Define emoji regex pattern
EMOJI_RE = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)

# Configuration Constants
POLLING_INTERVAL = int(os.environ.get("POLLING_INTERVAL", "10"))
API_TIMEOUT = int(os.environ.get("API_TIMEOUT", "3"))
MAX_MASTERS_PER_CYCLE = int(os.environ.get("MAX_MASTERS_PER_CYCLE", "100"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "development")

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "change-me-in-production")

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
        doc='/api/docs/' if ENVIRONMENT != 'production' else False,  # Disable in production
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
import time
from collections import defaultdict
from threading import Lock

# Performance metrics storage
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

    if hasattr(request, 'start_time'):
        duration = time.time() - request.start_time
        
        # Log slow responses
        if duration > 3.0:  # Global threshold
            logger.warning(
                f"SLOW RESPONSE: {request.method} {request.endpoint} - "
                f"{duration:.2f}s - Status: {response.status_code}"
            )
    
    # Add performance headers
    if hasattr(request, 'start_time'):
        response.headers['X-Response-Time'] = f"{(time.time() - request.start_time) * 1000:.1f}ms"
    
    response.headers['X-Request-ID'] = getattr(request, 'request_id', 'unknown')
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
                        'recent_requests': measurements[-10:]  # Last 10 requests
                    }
        
        return jsonify({
            'metrics': metrics_summary,
            'timestamp': datetime.utcnow().isoformat(),
            'uptime': format_uptime()
        })
        
    except Exception as e:
        logger.error(f"Failed to get performance metrics: {str(e)}")
        return safe_json_response({"error": str(e)}, 500)

# Apply performance monitoring to critical endpoints
# Add @monitor_performance() decorator to your existing critical routes

# Logging Configuration
def setup_logging():
    """Configure comprehensive logging for production"""
    log_level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
    
    # Create logs directory
    log_dir = os.path.join(DATA_DIR, "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    # Main application logger
    app_logger = logging.getLogger(__name__)
    app_logger.setLevel(log_level)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    app_logger.addHandler(console_handler)
    
    # File handler with rotation
    if ENVIRONMENT == "production":
        file_handler = RotatingFileHandler(
            os.path.join(log_dir, "app.log"),
            maxBytes=10*1024*1024,  # 10MB
            backupCount=10
        )
        file_formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        app_logger.addHandler(file_handler)
    
    return app_logger

logger = setup_logging_with_request_id()

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
            session.clear()  # Clear invalid session
            return jsonify({"error": "User not found"}), 404
            
        # Add user to request context
        request.current_user = user
        return f(*args, **kwargs)
    return decorated_function

def safe_json_response(data, status_code=200):
    """Return safe JSON response without exposing internal details in production"""
    if ENVIRONMENT == "production" and status_code >= 500:
        if isinstance(data, dict) and "details" in data:
            # Remove detailed error information in production
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

# Authentication decorator
def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("user"):
            return redirect(url_for("login"))
        return view(*args, **kwargs)
    return wrapped

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
                "current_master": {
                    "client_id": new_master_id,
                    "broker": new_master_account.broker,
                    "username": new_master_account.username
                }
            }), 200

        previous_state = {
            "linked_master_id": child_account.linked_master_id,
            "copy_status": child_account.copy_status,
            "last_copied_trade_id": child_account.last_copied_trade_id
        }

        logger.info(f"Changing master for {child_id}: {old_master_id} -> {new_master_id}")
        
        child_account.linked_master_id = new_master_id
        child_account.last_copied_trade_id = "NONE"
        
        was_copying = child_account.copy_status == "On"
        if was_copying:
            child_account.copy_status = "Off"
            logger.info(f"Temporarily disabled copying during master change for {child_id}")

        new_latest_order_id = "NONE"
        
        try:
            new_master_dict = _account_to_dict(new_master_account)
            new_master_api = broker_api(new_master_dict)
            
            broker_name = new_master_account.broker.lower() if new_master_account.broker else "unknown"
            
            if broker_name == "aliceblue" and hasattr(new_master_api, "get_trade_book"):
                logger.debug("Using AliceBlue trade book for new master marker")
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
                            latest_order.get("orderId")
                            or latest_order.get("order_id")
                            or latest_order.get("id")
                            or latest_order.get("NOrdNo")
                            or latest_order.get("norenordno")
                            or "NONE"
                        )
                        new_latest_order_id = str(new_latest_order_id) if new_latest_order_id else "NONE"
                        logger.info(f"Set new master marker to: {new_latest_order_id}")
                        
                except Exception as e:
                    logger.error(f"Failed to sort new master orders: {str(e)}")
                    
        except Exception as e:
            logger.warning(f"Could not fetch latest order from new master {new_master_id}: {str(e)}")

        child_account.last_copied_trade_id = new_latest_order_id

        try:
            db.session.commit()
            logger.info(f"Successfully changed master for {child_id}: {old_master_id} -> {new_master_id}")
        except Exception as e:
            logger.error(f"Failed to commit master change: {str(e)}")
            db.session.rollback()
            return safe_json_response({
                "error": "Failed to save master change",
                "details": str(e)
            }, 500)

        try:
            log_entry = SystemLog(
                timestamp=datetime.utcnow().isoformat(),
                level="INFO",
                message=f"Master changed: {child_id} from {old_master_id} to {new_master_id}",
                user_id=str(request.current_user.id),
                details=json.dumps({
                    "action": "change_master",
                    "child_id": child_id,
                    "old_master_id": old_master_id,
                    "new_master_id": new_master_id,
                    "user": request.current_user.email,
                    "was_copying": was_copying,
                    "new_marker": new_latest_order_id,
                    "timestamp": datetime.utcnow().isoformat(),
                    "previous_state": previous_state
                })
            )
            db.session.add(log_entry)
            db.session.commit()
        except Exception as e:
            logger.warning(f"Failed to log master change: {str(e)}")

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
                "changed_at": datetime.utcnow().isoformat()
            }
        }

        if was_copying:
            response_data["next_action"] = "Please restart copying to begin following the new master"

        return jsonify(response_data), 200

    except Exception as e:
        logger.error(f"Unexpected error in change_master: {str(e)}")
        return safe_json_response({
            "error": "Internal server error",
            "details": str(e)
        }, 500)

@app.route('/api/remove-child', methods=['POST'])
@limiter.limit("10 per minute")
@require_user
def remove_child():
    """Remove child role from account - Complete Database Version."""
    logger.info("Processing remove child request")
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        client_id = data.get("client_id")
        
        if not client_id:
            logger.error("Missing client_id in request")
            return jsonify({"error": "Missing client_id"}), 400

        if not validate_client_id(client_id):
            return jsonify({"error": "Invalid client_id format"}), 400

        child_account = Account.query.filter_by(
            user_id=request.current_user.id,
            client_id=client_id,
            role='child'
        ).first()
        
        if not child_account:
            logger.error(f"Child account not found: {client_id}")
            return jsonify({"error": "Child account not found or not configured as child"}), 404

        master_account = None
        master_id = child_account.linked_master_id
        
        if master_id:
            master_account = Account.query.filter_by(
                client_id=master_id
            ).first()

        previous_state = {
            "role": child_account.role,
            "linked_master_id": child_account.linked_master_id,
            "copy_status": child_account.copy_status,
            "multiplier": child_account.multiplier,
            "last_copied_trade_id": child_account.last_copied_trade_id,
            "broker": child_account.broker,
            "username": child_account.username
        }

        was_copying = child_account.copy_status == "On"
        
        if was_copying:
            logger.info(f"Child {client_id} was actively copying, will stop copying during removal")

        active_mappings = OrderMapping.query.filter_by(
            child_client_id=client_id,
            status="ACTIVE"
        ).all()

        mapping_count = len(active_mappings)
        if mapping_count > 0:
            logger.info(f"Found {mapping_count} active order mappings for child {client_id}")

        logger.info(f"Removing child role from account {client_id}")
        
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
                logger.error(f"Failed to update mapping {mapping.id}: {str(e)}")

        try:
            db.session.commit()
            logger.info(f"Successfully removed child role from {client_id}")
        except Exception as e:
            logger.error(f"Failed to commit child removal: {str(e)}")
            db.session.rollback()
            return safe_json_response({
                "error": "Failed to save child removal",
                "details": str(e)
            }, 500)

        try:
            log_entry = SystemLog(
                timestamp=datetime.utcnow().isoformat(),
                level="INFO",
                message=f"Child removed: {client_id} from master {master_id}",
                user_id=str(request.current_user.id),
                details=json.dumps({
                    "action": "remove_child",
                    "child_id": client_id,
                    "master_id": master_id,
                    "user": request.current_user.email,
                    "was_copying": was_copying,
                    "active_mappings": mapping_count,
                    "mappings_updated": mappings_updated,
                    "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    "previous_state": previous_state
                })
            )
            db.session.add(log_entry)
            db.session.commit()
        except Exception as e:
            logger.warning(f"Failed to log child removal: {str(e)}")

        response_data = {
            "message": f"Child {client_id} removed from master successfully",
            "removed_account": {
                "client_id": client_id,
                "broker": previous_state["broker"],
                "username": previous_state["username"],
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
                "removed_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            },
            "account_status": {
                "role": None,
                "linked_master_id": None,
                "copy_status": "Off",
                "multiplier": 1.0,
                "available_for_reassignment": True
            }
        }

        if mapping_count > 0:
            response_data["cleanup_summary"] = {
                "message": f"Updated {mappings_updated} order mappings to CHILD_REMOVED status",
                "mappings_affected": mapping_count
            }

        return jsonify(response_data), 200

    except Exception as e:
        logger.error(f"Unexpected error in remove_child: {str(e)}")
        return safe_json_response({
            "error": "Internal server error",
            "details": str(e)
        }, 500)

@app.route('/api/remove-master', methods=['POST'])
@limiter.limit("10 per minute")
@require_user
def remove_master():
    """Remove master role from account - Complete Database Version."""
    logger.info("Processing remove master request")
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        client_id = data.get("client_id")
        
        if not client_id:
            logger.error("Missing client_id in request")
            return jsonify({"error": "Missing client_id"}), 400

        if not validate_client_id(client_id):
            return jsonify({"error": "Invalid client_id format"}), 400

        master_account = Account.query.filter_by(
            user_id=request.current_user.id,
            client_id=client_id,
            role='master'
        ).first()
        
        if not master_account:
            logger.error(f"Master account not found: {client_id}")
            return jsonify({"error": "Master account not found or not configured as master"}), 404

        linked_children = Account.query.filter_by(
            user_id=request.current_user.id,  # Only user's children
            role='child',
            linked_master_id=client_id
        ).all()

        children_count = len(linked_children)
        active_children = [child for child in linked_children if child.copy_status == "On"]
        active_count = len(active_children)

        logger.info(f"Master {client_id} has {children_count} linked children ({active_count} actively copying)")

        previous_state = {
            "role": master_account.role,
            "copy_status": master_account.copy_status,
            "multiplier": master_account.multiplier,
            "broker": master_account.broker,
            "username": master_account.username,
            "linked_children": [child.client_id for child in linked_children],
            "active_children": [child.client_id for child in active_children]
        }

        active_mappings = OrderMapping.query.filter_by(
            master_client_id=client_id,
            status="ACTIVE"
        ).all()

        mapping_count = len(active_mappings)
        if mapping_count > 0:
            logger.info(f"Found {mapping_count} active order mappings for master {client_id}")

        children_processed = []
        children_failed = []

        for child in linked_children:
            try:
                child_previous_state = {
                    "client_id": child.client_id,
                    "copy_status": child.copy_status,
                    "multiplier": child.multiplier
                }

                # Orphan the child account
                child.role = None  # Remove child role
                child.linked_master_id = None  # Remove master link
                child.copy_status = "Off"  # Stop copying
                child.multiplier = 1.0  # Reset multiplier
                child.last_copied_trade_id = None  # Clear marker

                children_processed.append({
                    "client_id": child.client_id,
                    "broker": child.broker,
                    "username": child.username,
                    "was_copying": child_previous_state["copy_status"] == "On",
                    "status": "ORPHANED"
                })

                logger.debug(f"Orphaned child {child.client_id}")

            except Exception as e:
                logger.error(f"Failed to orphan child {child.client_id}: {str(e)}")
                children_failed.append({
                    "client_id": child.client_id,
                    "error": str(e)
                })

        logger.info(f"Removing master role from account {client_id}")
        
        master_account.role = None  # Remove role completely
        master_account.copy_status = "Off"  # Ensure status is off
        master_account.multiplier = 1.0  # Reset to default

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
                logger.error(f"Failed to update mapping {mapping.id}: {str(e)}")

        try:
            db.session.commit()
            logger.info(f"Successfully removed master role from {client_id} and orphaned {len(children_processed)} children")
        except Exception as e:
            logger.error(f"Failed to commit master removal: {str(e)}")
            db.session.rollback()
            return safe_json_response({
                "error": "Failed to save master removal",
                "details": str(e)
            }, 500)

        try:
            log_entry = SystemLog(
                timestamp=datetime.utcnow().isoformat(),
                level="INFO",
                message=f"Master removed: {client_id} with {children_count} children orphaned",
                user_id=str(request.current_user.id),
                details=json.dumps({
                    "action": "remove_master",
                    "master_id": client_id,
                    "user": request.current_user.email,
                    "children_count": children_count,
                    "active_children": active_count,
                    "children_processed": len(children_processed),
                    "children_failed": len(children_failed),
                    "active_mappings": mapping_count,
                    "mappings_updated": mappings_updated,
                    "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    "previous_state": previous_state
                })
            )
            db.session.add(log_entry)
            db.session.commit()
        except Exception as e:
            logger.warning(f"Failed to log master removal: {str(e)}")

        response_data = {
            "message": f"Master {client_id} removed successfully",
            "removed_master": {
                "client_id": client_id,
                "broker": previous_state["broker"],
                "username": previous_state["username"],
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
                "mapping_details": mapping_details[:10]  # Limit to first 10 for response size
            },
            "removal_details": {
                "master_role_removed": True,
                "copy_status_reset": True,
                "multiplier_reset": True,
                "children_orphaned": len(children_processed),
                "removed_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            },
            "account_status": {
                "role": None,
                "copy_status": "Off",
                "multiplier": 1.0,
                "available_for_reassignment": True
            }
        }

        if children_failed:
            response_data["children_affected"]["failures"] = children_failed

        if mapping_count > 10:
            response_data["order_mappings"]["note"] = f"Showing first 10 of {mapping_count} total mappings"

        return jsonify(response_data), 200

    except Exception as e:
        logger.error(f"Unexpected error in remove_master: {str(e)}")
        return safe_json_response({
            "error": "Internal server error",
            "details": str(e)
        }, 500)

@app.route('/api/update-multiplier', methods=['POST'])
@limiter.limit("20 per minute")
@require_user
def update_multiplier():
    """Update multiplier for account - Complete Database Version."""
    logger.info("Processing update multiplier request")
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        client_id = data.get("client_id")
        new_multiplier = data.get("multiplier")
        
        if not client_id or new_multiplier is None:
            logger.error(f"Missing required fields: client_id={bool(client_id)}, multiplier={new_multiplier is not None}")
            return jsonify({"error": "Missing client_id or multiplier"}), 400

        if not validate_client_id(client_id):
            return jsonify({"error": "Invalid client_id format"}), 400

        try:
            new_multiplier = float(new_multiplier)
            if new_multiplier < 0.1:
                return jsonify({"error": "Multiplier must be at least 0.1"}), 400
            if new_multiplier > 100.0:  # Reasonable upper limit
                return jsonify({"error": "Multiplier cannot exceed 100.0"}), 400
        except (ValueError, TypeError):
            return jsonify({"error": "Invalid multiplier format - must be a number"}), 400

        account = Account.query.filter_by(
            user_id=request.current_user.id,
            client_id=client_id
        ).first()
        
        if not account:
            logger.error(f"Account not found: {client_id} for user {request.current_user.email}")
            return jsonify({"error": "Account not found"}), 404

        previous_state = {
            "multiplier": account.multiplier,
            "role": account.role,
            "copy_status": account.copy_status,
            "linked_master_id": account.linked_master_id,
            "broker": account.broker,
            "username": account.username
        }

        if abs(float(account.multiplier or 1.0) - new_multiplier) < 0.001:  # Account for floating point precision
            logger.info(f"Multiplier for {client_id} already set to {new_multiplier}")
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

        logger.info(f"Updating multiplier for {client_id}: {previous_state['multiplier']} -> {new_multiplier}")
        
        account.multiplier = new_multiplier

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

        try:
            db.session.commit()
            logger.info(f"Successfully updated multiplier for {client_id} to {new_multiplier}")
        except Exception as e:
            logger.error(f"Failed to commit multiplier update: {str(e)}")
            db.session.rollback()
            return safe_json_response({
                "error": "Failed to save multiplier update",
                "details": str(e)
            }, 500)

        try:
            log_entry = SystemLog(
                timestamp=datetime.utcnow().isoformat(),
                level="INFO",
                message=f"Multiplier updated: {client_id} from {previous_state['multiplier']} to {new_multiplier}",
                user_id=str(request.current_user.id),
                details=json.dumps({
                    "action": "update_multiplier",
                    "client_id": client_id,
                    "user": request.current_user.email,
                    "old_multiplier": previous_state['multiplier'],
                    "new_multiplier": new_multiplier,
                    "account_role": account.role,
                    "copy_status": account.copy_status,
                    "master_id": account.linked_master_id,
                    "active_mappings": active_mapping_count,
                    "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    "warnings_generated": warnings
                })
            )
            db.session.add(log_entry)
            db.session.commit()
        except Exception as e:
            logger.warning(f"Failed to log multiplier update: {str(e)}")

        impact_estimation = None
        if account.role == "child" and master_account:
            impact_estimation = {
                "example_scenario": {
                    "master_trade_qty": 100,
                    "old_child_qty": int(100 * float(previous_state['multiplier'] or 1.0)),
                    "new_child_qty": int(100 * new_multiplier),
                    "qty_change": int(100 * new_multiplier) - int(100 * float(previous_state['multiplier'] or 1.0))
                },
                "multiplier_change": {
                    "percentage": f"{((new_multiplier / float(previous_state['multiplier'] or 1.0)) - 1) * 100:+.1f}%",
                    "factor": f"{new_multiplier / float(previous_state['multiplier'] or 1.0):.2f}x"
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
                "previous_multiplier": float(previous_state['multiplier'] or 1.0),
                "new_multiplier": new_multiplier,
                "change": new_multiplier - float(previous_state['multiplier'] or 1.0),
                "updated_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
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
        logger.error(f"Unexpected error in update_multiplier: {str(e)}")
        return safe_json_response({
            "error": "Internal server error",
            "details": str(e)
        }, 500)

@app.route('/api/delete-account', methods=['POST'])
@limiter.limit("10 per minute")
@require_user
def delete_account():
    data = request.json
    client_id = data.get("client_id")
    if not client_id:
        return jsonify({"error": "Missing client_id"}), 400

    if not validate_client_id(client_id):
        return jsonify({"error": "Invalid client_id format"}), 400

    acc_db = Account.query.filter_by(
        user_id=request.current_user.id, 
        client_id=client_id
    ).first()
    
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
@limiter.limit("10 per minute")
@require_user
def check_credentials():
    """Validate broker credentials without saving them."""
    data = request.json
    broker = data.get('broker')
    client_id = data.get('client_id')

    if not broker or not client_id:
        return jsonify({'error': 'Missing broker or client_id'}), 400

    if not validate_broker(broker) or not validate_client_id(client_id):
        return jsonify({'error': 'Invalid broker or client_id format'}), 400

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
        return safe_json_response({'error': f'Credential validation failed: {error_message}'}, 400)

@app.route('/api/add-account', methods=['POST'])
@limiter.limit("10 per minute")
@require_user
def add_account():
    """Add account - Complete Database Version."""
    logger.info("Processing add account request")
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        broker = data.get('broker')
        client_id = data.get('client_id')
        username = data.get('username')
        
        if not broker or not client_id or not username:
            logger.error(f"Missing required fields: broker={bool(broker)}, client_id={bool(client_id)}, username={bool(username)}")
            return jsonify({"error": "Missing broker, client_id or username"}), 400

        broker = broker.lower().strip()
        
        if not validate_broker(broker) or not validate_client_id(client_id):
            return jsonify({"error": "Invalid broker or client_id format"}), 400
        
        supported_brokers = ['aliceblue', 'finvasia', 'dhan', 'zerodha', 'groww', 'upstox', 'fyers']
        if broker not in supported_brokers:
            return jsonify({
                "error": f"Unsupported broker: {broker}",
                "supported_brokers": supported_brokers
            }), 400

        existing_account = Account.query.filter_by(
            user_id=request.current_user.id,
            client_id=client_id,
            broker=broker
        ).first()
        
        if existing_account:
            logger.warning(f"Account already exists: {client_id} ({broker}) for user {request.current_user.email}")
            return jsonify({
                "error": "Account already exists",
                "existing_account": {
                    "client_id": client_id,
                    "broker": broker,
                    "username": existing_account.username,
                    "status": existing_account.status,
                    "role": existing_account.role
                }
            }), 400

        credentials = {}
        
        for key, value in data.items():
            if key not in ('broker', 'client_id', 'username') and value is not None:
                credentials[key] = value

        validation_error = None
        
        if broker == 'aliceblue':
            api_key = credentials.get('api_key')
            if not api_key:
                validation_error = "Missing API Key for AliceBlue"
            else:
                if 'device_number' not in credentials:
                    existing_alice = Account.query.filter_by(
                        user_id=request.current_user.id,
                        broker='aliceblue'
                    ).first()
                    
                    if existing_alice and existing_alice.device_number:
                        credentials['device_number'] = existing_alice.device_number
                        logger.info(f"Reusing existing device number for AliceBlue: {credentials['device_number']}")
                    else:
                        credentials['device_number'] = str(uuid.uuid4())
                        logger.info(f"Generated new device number for AliceBlue: {credentials['device_number']}")
                        
        elif broker == 'finvasia':
            required_fields = ['password', 'totp_secret', 'vendor_code', 'api_key']
            missing_fields = [field for field in required_fields if not credentials.get(field)]
            
            if missing_fields:
                validation_error = f"Missing required fields for Finvasia: {', '.join(missing_fields)}"
            else:
                if not credentials.get('imei'):
                    credentials['imei'] = 'abc1234'
                    logger.info("Using default IMEI for Finvasia")
                    
        elif broker == 'groww':
            access_token = credentials.get('access_token')
            if not access_token:
                validation_error = "Missing Access Token for Groww"
                
        else:  # dhan, zerodha, upstox, fyers
            access_token = credentials.get('access_token')
            if not access_token:
                validation_error = f"Missing Access Token for {broker.title()}"

        if validation_error:
            logger.error(f"Credential validation failed: {validation_error}")
            return jsonify({"error": validation_error}), 400

        try:
            BrokerClass = get_broker_class(broker)
            if not BrokerClass:
                return jsonify({"error": f"Broker class not found for {broker}"}), 500
            
            if broker == 'aliceblue':
                broker_obj = BrokerClass(
                    client_id, 
                    credentials['api_key'], 
                    device_number=credentials.get('device_number')
                )
                
            elif broker == 'finvasia':
                broker_obj = BrokerClass(
                    client_id=client_id,
                    password=credentials['password'],
                    totp_secret=credentials['totp_secret'],
                    vendor_code=credentials['vendor_code'],
                    api_key=credentials['api_key'],
                    imei=credentials.get('imei', 'abc1234')
                )
                
            elif broker == 'groww':
                broker_obj = BrokerClass(client_id, credentials['access_token'])
                
            else:  # dhan, zerodha, upstox, fyers
                access_token = credentials.get('access_token')
                other_creds = {k: v for k, v in credentials.items() if k != 'access_token'}
                broker_obj = BrokerClass(client_id, access_token, **other_creds)
                
            if hasattr(broker_obj, 'check_token_valid'):
                logger.info(f"Validating credentials for {broker} account {client_id}")
                is_valid = broker_obj.check_token_valid()
                
                if not is_valid:
                    error_message = "Invalid broker credentials"
                    if hasattr(broker_obj, 'last_auth_error') and broker_obj.last_auth_error():
                        error_message = broker_obj.last_auth_error()
                    
                    logger.error(f"Credential validation failed for {broker}: {error_message}")
                    return safe_json_response({"error": f"Credential validation failed: {error_message}"}, 400)
                    
                logger.info(f"Credentials validated successfully for {broker} account {client_id}")
            else:
                logger.info(f"No validation method available for {broker}, skipping credential test")
                
        except Exception as e:
            error_message = str(e)
            if hasattr(broker_obj, 'last_auth_error') and broker_obj.last_auth_error():
                error_message = broker_obj.last_auth_error()
            
            logger.error(f"Failed to initialize/validate {broker} broker: {error_message}")
            return safe_json_response({"error": f"Broker initialization failed: {error_message}"}, 400)

        logger.info(f"Creating new account: {client_id} ({broker}) for user {request.current_user.email}")
        
        new_account = Account(
            user_id=request.current_user.id,
            broker=broker,
            client_id=client_id,
            username=username,
            credentials=credentials,
            status='Connected',
            auto_login=True,
            last_login_time=datetime.utcnow().isoformat(),
            role=None,
            linked_master_id=None,
            multiplier=1.0,
            copy_status='Off',
            device_number=credentials.get('device_number') if broker == 'aliceblue' else None
        )

        try:
            db.session.add(new_account)
            db.session.commit()
            logger.info(f"Successfully added account {client_id} ({broker}) to database")
        except Exception as e:
            logger.error(f"Failed to save account to database: {str(e)}")
            db.session.rollback()
            return safe_json_response({
                "error": "Failed to save account",
                "details": str(e)
            }, 500)

        try:
            log_entry = SystemLog(
                timestamp=datetime.utcnow().isoformat(),
                level="INFO",
                message=f"Account added: {client_id} ({broker}) by {request.current_user.email}",
                user_id=str(request.current_user.id),
                details=json.dumps({
                    "action": "add_account",
                    "client_id": client_id,
                    "broker": broker,
                    "username": username,
                    "user": request.current_user.email,
                    "credentials_provided": list(credentials.keys()),
                    "validation_performed": hasattr(broker_obj, 'check_token_valid'),
                    "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                })
            )
            db.session.add(log_entry)
            db.session.commit()
        except Exception as e:
            logger.warning(f"Failed to log account addition: {str(e)}")

        response_data = {
            "message": f"✅ Account {username} ({broker}) added successfully",
            "account_details": {
                "client_id": client_id,
                "broker": broker,
                "username": username,
                "status": "Connected",
                "role": None,
                "copy_status": "Off",
                "multiplier": 1.0,
                "auto_login": True,
                "added_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            },
            "broker_info": {
                "broker": broker,
                "validation_performed": hasattr(broker_obj, 'check_token_valid'),
                "credentials_stored": len(credentials),
                "device_number": credentials.get('device_number') if broker == 'aliceblue' else None
            },
            "next_steps": [
                "Account is ready for use",
                "Configure as Master or Child to start trading",
                "Check account status in the dashboard"
            ]
        }

        return jsonify(response_data), 200

    except Exception as e:
        logger.error(f"Unexpected error in add_account: {str(e)}")
        return safe_json_response({
            "error": "Internal server error",
            "details": str(e)
        }, 500)

@app.route('/api/accounts')
@limiter.limit("60 per minute")
@require_user
def get_accounts():
    try:
        accounts = [_account_to_dict(acc) for acc in request.current_user.accounts]

        # Add opening balances
        for acc in accounts:
            bal = get_opening_balance_for_account(acc)
            if bal is not None:
                acc["opening_balance"] = bal

        # Group masters with their children (only user's own children)
        masters = []
        for acc in accounts:
            if acc.get("role") == "master":
                children = Account.query.filter_by(
                    user_id=request.current_user.id,  # Only user's children
                    role="child", 
                    linked_master_id=acc.get("client_id")
                ).all()
                acc_copy = dict(acc)
                acc_copy["children"] = [_account_to_dict(child) for child in children]
                masters.append(acc_copy)

        return jsonify({
            "masters": masters,
            "accounts": accounts
        })
    except Exception as e:
        logger.error(f"Error in /api/accounts: {str(e)}")
        return safe_json_response({"error": str(e)}, 500)

@app.route('/api/groups', methods=['GET'])
@limiter.limit("60 per minute")
@require_user
def get_groups():
    """Return all account groups for the logged-in user."""
    groups = Group.query.filter_by(user_id=request.current_user.id).all()
    return jsonify([_group_to_dict(g) for g in groups])

@app.route('/api/create-group', methods=['POST'])
@limiter.limit("10 per minute")
@require_user
def create_group():
    """Create a new account group."""
    data = request.json
    name = data.get("name")
    members = data.get("members", [])
    if not name:
        return jsonify({"error": "Missing group name"}), 400

    if Group.query.filter_by(user_id=request.current_user.id, name=name).first():
        return jsonify({"error": "Group already exists"}), 400

    group = Group(name=name, user_id=request.current_user.id)
    for cid in members:
        acc = Account.query.filter_by(
            user_id=request.current_user.id, 
            client_id=cid
        ).first()
        if acc:
            group.accounts.append(acc)
    db.session.add(group)
    db.session.commit()
    return jsonify({"message": f"Group '{name}' created"})

@app.route('/api/groups/<group_name>/add', methods=['POST'])
@limiter.limit("20 per minute")
@require_user
def add_account_to_group(group_name):
    """Add an account to an existing group."""
    client_id = request.json.get("client_id")
    if not client_id:
        return jsonify({"error": "Missing client_id"}), 400

    if not validate_client_id(client_id):
        return jsonify({"error": "Invalid client_id format"}), 400

    group = Group.query.filter_by(
        user_id=request.current_user.id, 
        name=group_name
    ).first()
    if not group:
        return jsonify({"error": "Group not found"}), 404
        
    acc = Account.query.filter_by(
        user_id=request.current_user.id, 
        client_id=client_id
    ).first()
    if not acc:
        return jsonify({"error": "Account not found"}), 404
    if acc in group.accounts:
        return jsonify({"message": "Account already in group"})
    group.accounts.append(acc)
    db.session.commit()
    return jsonify({"message": f"Added {client_id} to {group_name}"})

@app.route('/api/groups/<group_name>/remove', methods=['POST'])
@limiter.limit("20 per minute")
@require_user
def remove_account_from_group(group_name):
    """Remove an account from a group."""
    client_id = request.json.get("client_id")
    if not client_id:
        return jsonify({"error": "Missing client_id"}), 400

    if not validate_client_id(client_id):
        return jsonify({"error": "Invalid client_id format"}), 400

    group = Group.query.filter_by(
        user_id=request.current_user.id, 
        name=group_name
    ).first()
    if not group:
        return jsonify({"error": "Group not found"}), 404
        
    acc = Account.query.filter_by(
        user_id=request.current_user.id, 
        client_id=client_id
    ).first()
    if not acc or acc not in group.accounts:
        return jsonify({"error": "Account not in group"}), 400
    group.accounts.remove(acc)
    db.session.commit()
    return jsonify({"message": f"Removed {client_id} from {group_name}"})

@app.route('/api/group-order', methods=['POST'])
@limiter.limit("30 per minute")
@require_user
def place_group_order():
    """Place the same order across all accounts in a group."""
    data = request.json
    group_name = data.get("group_name")
    symbol = data.get("symbol")
    action = data.get("action")
    quantity = data.get("quantity")

    if not all([group_name, symbol, action, quantity]):
        return jsonify({"error": "Missing required fields"}), 400

    group = Group.query.filter_by(
        user_id=request.current_user.id, 
        name=group_name
    ).first()
    if not group:
        return jsonify({"error": "Group not found"}), 404

    accounts = [
        _account_to_dict(acc)
        for acc in group.accounts.all()
    ]
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
                results.append({
                    "client_id": acc.get("client_id"), 
                    "status": status, 
                    "reason": clean_response_message(resp)
                })
            else:
                status = "SUCCESS"
                results.append({
                    "client_id": acc.get("client_id"), 
                    "status": status
                })

            record_trade(
                request.current_user.email, 
                symbol, 
                action.upper(), 
                quantity, 
                order_params.get('price'), 
                status
            )
        except Exception as e:
            results.append({
                "client_id": acc.get("client_id"), 
                "status": "ERROR", 
                "reason": str(e)
            })

    return jsonify(results)

@app.route('/api/set-master', methods=['POST'])
@limiter.limit("10 per minute")
@require_user
def set_master():
    try:
        client_id = request.json.get('client_id')
        if not client_id:
            return jsonify({"error": "Missing client_id"}), 400

        if not validate_client_id(client_id):
            return jsonify({"error": "Invalid client_id format"}), 400

        account = Account.query.filter_by(
            user_id=request.current_user.id, 
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
        logger.error(f"Error in set_master: {str(e)}")
        return safe_json_response({"error": str(e)}, 500)

@app.route('/api/set-child', methods=['POST'])
@limiter.limit("10 per minute")
@require_user
def set_child():
    try:
        client_id = request.json.get('client_id')
        linked_master_id = request.json.get('linked_master_id')
        
        if not client_id or not linked_master_id:
            return jsonify({"error": "Missing client_id or linked_master_id"}), 400

        if not validate_client_id(client_id) or not validate_client_id(linked_master_id):
            return jsonify({"error": "Invalid client_id format"}), 400

        account = Account.query.filter_by(
            user_id=request.current_user.id, 
            client_id=client_id
        ).first()
        
        if not account:
            return jsonify({"error": "Account not found"}), 404

        # Verify master account exists and belongs to user
        master_account = Account.query.filter_by(
            user_id=request.current_user.id,
            client_id=linked_master_id,
            role='master'
        ).first()
        
        if not master_account:
            return jsonify({"error": "Master account not found or not accessible"}), 404

        account.role = "child"
        account.linked_master_id = linked_master_id
        account.copy_status = "Off"
        account.multiplier = 1.0
        db.session.commit()

        return jsonify({"message": "Set as child successfully"})
    except Exception as e:
        logger.error(f"Error in set_child: {str(e)}")
        return safe_json_response({"error": str(e)}, 500)

@app.route('/api/start-copy', methods=['POST'])
@limiter.limit("10 per minute")
@require_user
def start_copy():
    """Start copying for a child account - Complete Database Version."""
    logger.info("Processing start copy request")
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        client_id = data.get("client_id")
        master_id = data.get("master_id")
        
        if not client_id or not master_id:
            logger.error(f"Missing required fields: client_id={bool(client_id)}, master_id={bool(master_id)}")
            return jsonify({"error": "Missing client_id or master_id"}), 400

        if not validate_client_id(client_id) or not validate_client_id(master_id):
            return jsonify({"error": "Invalid client_id format"}), 400

        child_account = Account.query.filter_by(
            user_id=request.current_user.id, 
            client_id=client_id
        ).first()
        
        if not child_account:
            logger.error(f"Child account not found: {client_id}")
            return jsonify({"error": "Child account not found"}), 404

        master_account = Account.query.filter_by(
            user_id=request.current_user.id,
            client_id=master_id,
            role='master'
        ).first()
        
        if not master_account:
            logger.error(f"Master account not found: {master_id}")
            return jsonify({"error": "Master account not found"}), 404

        child_account.role = "child"
        child_account.linked_master_id = master_id
        child_account.copy_status = "On"
        
        logger.info(f"Setting up child {client_id} to copy from master {master_id}")

        latest_order_id = "NONE"
        
        try:
            master_dict = _account_to_dict(master_account)
            master_api = broker_api(master_dict)
            
            broker_name = master_account.broker.lower() if master_account.broker else "unknown"
            
            if broker_name == "aliceblue" and hasattr(master_api, "get_trade_book"):
                logger.debug("Using AliceBlue trade book for marker")
                orders_resp = master_api.get_trade_book()
                order_list = parse_order_list(orders_resp)
                
                if not order_list and hasattr(master_api, "get_order_list"):
                    logger.debug("Trade book empty, falling back to order list")
                    orders_resp = master_api.get_order_list()
                    order_list = parse_order_list(orders_resp)
            else:
                logger.debug("Using standard order list for marker")
                orders_resp = master_api.get_order_list()
                order_list = parse_order_list(orders_resp)
            
            order_list = strip_emojis_from_obj(order_list or [])
            
            if order_list and isinstance(order_list, list):
                try:
                    order_list = sorted(order_list, key=get_order_sort_key, reverse=True)
                    
                    if order_list:
                        latest_order = order_list[0]
                        
                        latest_order_id = (
                            latest_order.get("orderId")
                            or latest_order.get("order_id")
                            or latest_order.get("id")
                            or latest_order.get("NOrdNo")
                            or latest_order.get("Nstordno")
                            or latest_order.get("nestOrderNumber")
                            or latest_order.get("orderNumber")
                            or latest_order.get("norenordno")
                            or "NONE"
                        )
                        
                        latest_order_id = str(latest_order_id) if latest_order_id else "NONE"
                        logger.info(f"Set initial marker to latest order: {latest_order_id}")
                        
                except Exception as e:
                    logger.error(f"Failed to sort orders for marker: {str(e)}")
                    latest_order_id = "NONE"
            else:
                logger.info(f"No orders found for master {master_id}, using NONE marker")
                
        except Exception as e:
            logger.error(f"Could not fetch latest order for marker from master {master_id}: {str(e)}")
            latest_order_id = "NONE"

        child_account.last_copied_trade_id = latest_order_id
        
        try:
            db.session.commit()
            logger.info(f"Successfully started copying: {client_id} -> {master_id}, marker: {latest_order_id}")
        except Exception as e:
            logger.error(f"Failed to commit changes: {str(e)}")
            db.session.rollback()
            return safe_json_response({
                "error": "Failed to save configuration",
                "details": str(e)
            }, 500)

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
        logger.error(f"Unexpected error in start_copy: {str(e)}")
        return safe_json_response({
            "error": "Internal server error",
            "details": str(e)
        }, 500)
    
@app.route('/api/stop-copy', methods=['POST'])
@limiter.limit("10 per minute")
@require_user
def stop_copy():
    """Stop copying for a child account - Complete Database Version."""
    logger.info("Processing stop copy request")
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        client_id = data.get("client_id")
        master_id = data.get("master_id")  # Optional for validation
        
        if not client_id:
            logger.error("Missing client_id in request")
            return jsonify({"error": "Missing client_id"}), 400

        if not validate_client_id(client_id):
            return jsonify({"error": "Invalid client_id format"}), 400

        child_account = Account.query.filter_by(
            user_id=request.current_user.id, 
            client_id=client_id
        ).first()
        
        if not child_account:
            logger.error(f"Child account not found: {client_id}")
            return jsonify({"error": "Child account not found"}), 404

        if child_account.role != "child":
            logger.warning(f"Account {client_id} is not configured as child (role: {child_account.role})")
            return jsonify({"error": "Account is not configured as a child"}), 400

        if child_account.copy_status != "On":
            logger.info(f"Copy already stopped for {client_id} (status: {child_account.copy_status})")
            return jsonify({
                "message": f"Copy trading is already stopped for {client_id}",
                "current_status": child_account.copy_status
            }), 200

        current_master_id = child_account.linked_master_id
        if master_id and master_id != current_master_id:
            logger.warning(f"Master ID mismatch: expected {master_id}, current {current_master_id}")
            return jsonify({
                "error": "Master ID mismatch",
                "expected": master_id,
                "current": current_master_id
            }), 400

        master_account = None
        if current_master_id:
            master_account = Account.query.filter_by(
                client_id=current_master_id
            ).first()

        logger.info(f"Stopping copy trading for {client_id} from master {current_master_id}")
        
        previous_state = {
            "role": child_account.role,
            "linked_master_id": child_account.linked_master_id,
            "copy_status": child_account.copy_status,
            "multiplier": child_account.multiplier,
            "last_copied_trade_id": child_account.last_copied_trade_id
        }
        
        child_account.copy_status = "Off"
        
        try:
            db.session.commit()
            logger.info(f"Successfully stopped copying for {client_id}")
        except Exception as e:
            logger.error(f"Failed to commit changes: {str(e)}")
            db.session.rollback()
            return safe_json_response({
                "error": "Failed to save configuration",
                "details": str(e)
            }, 500)

        try:
            log_entry = SystemLog(
                timestamp=datetime.now().isoformat(),
                level="INFO",
                message=f"Copy trading stopped: {client_id} -> {current_master_id}",
                user_id=str(request.current_user.id),
                details=json.dumps({
                    "action": "stop_copy",
                    "child_id": client_id,
                    "master_id": current_master_id,
                    "user": request.current_user.email,
                    "previous_state": previous_state
                })
            )
            db.session.add(log_entry)
            db.session.commit()
        except Exception as e:
            logger.warning(f"Failed to log action: {str(e)}")

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
        logger.error(f"Unexpected error in stop_copy: {str(e)}")
        return safe_json_response({
            "error": "Internal server error",
            "details": str(e)
        }, 500)

@app.route('/api/start-copy-all', methods=['POST'])
@limiter.limit("5 per minute")
@require_user
def start_copy_all():
    """Start copying for all children of a master - Complete Database Version."""
    logger.info("Processing bulk start copy request")
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        master_id = data.get("master_id")
        
        if not master_id:
            logger.error("Missing master_id in request")
            return jsonify({"error": "Missing master_id"}), 400

        if not validate_client_id(master_id):
            return jsonify({"error": "Invalid master_id format"}), 400

        master_account = Account.query.filter_by(
            user_id=request.current_user.id,
            client_id=master_id,
            role='master'
        ).first()
        
        if not master_account:
            logger.error(f"Master account not found or not accessible: {master_id}")
            return jsonify({"error": "Master account not found or not accessible"}), 404

        stopped_children = Account.query.filter_by(
            user_id=request.current_user.id,
            role='child',
            linked_master_id=master_id,
            copy_status='Off'
        ).all()

        if not stopped_children:
            linked_children = Account.query.filter_by(
                user_id=request.current_user.id,
                role='child',
                linked_master_id=master_id
            ).all()
            
            all_children_copying = all(child.copy_status == "On" for child in linked_children)
            
            if all_children_copying and linked_children:
                return jsonify({
                    "message": f"All child accounts are already copying for master {master_id}",
                    "master_id": master_id,
                    "total_children": len(linked_children),
                    "already_copying": len(linked_children),
                    "started_count": 0,
                    "details": []
                }), 200
            else:
                return jsonify({
                    "message": f"No stopped child accounts found for master {master_id}",
                    "master_id": master_id,
                    "total_children": len(linked_children),
                    "started_count": 0,
                    "details": []
                }), 200

        logger.info(f"Found {len(stopped_children)} stopped children to start for master {master_id}")

        master_latest_order_id = "NONE"
        
        try:
            master_dict = _account_to_dict(master_account)
            master_api = broker_api(master_dict)
            
            broker_name = master_account.broker.lower() if master_account.broker else "unknown"
            
            if broker_name == "aliceblue" and hasattr(master_api, "get_trade_book"):
                logger.debug("Using AliceBlue trade book for bulk marker")
                orders_resp = master_api.get_trade_book()
                order_list = parse_order_list(orders_resp)
                
                if not order_list and hasattr(master_api, "get_order_list"):
                    logger.debug("Trade book empty, falling back to order list")
                    orders_resp = master_api.get_order_list()
                    order_list = parse_order_list(orders_resp)
            else:
                logger.debug("Using standard order list for bulk marker")
                orders_resp = master_api.get_order_list()
                order_list = parse_order_list(orders_resp)
            
            order_list = strip_emojis_from_obj(order_list or [])
            
            if order_list and isinstance(order_list, list):
                try:
                    order_list = sorted(order_list, key=get_order_sort_key, reverse=True)
                    
                    if order_list:
                        latest_order = order_list[0]
                        
                        master_latest_order_id = (
                            latest_order.get("orderId")
                            or latest_order.get("order_id")
                            or latest_order.get("id")
                            or latest_order.get("NOrdNo")
                            or latest_order.get("Nstordno")
                            or latest_order.get("nestOrderNumber")
                            or latest_order.get("orderNumber")
                            or latest_order.get("norenordno")
                            or "NONE"
                        )
                        
                        master_latest_order_id = str(master_latest_order_id) if master_latest_order_id else "NONE"
                        logger.info(f"Set bulk marker to latest master order: {master_latest_order_id}")
                        
                except Exception as e:
                    logger.error(f"Failed to sort master orders for bulk marker: {str(e)}")
                    master_latest_order_id = "NONE"
            else:
                logger.info(f"No orders found for master {master_id}, using NONE marker")
                
        except Exception as e:
            logger.error(f"Could not fetch latest order for bulk marker from master {master_id}: {str(e)}")
            master_latest_order_id = "NONE"

        results = []
        started_count = 0
        failed_count = 0

        for child in stopped_children:
            try:
                previous_state = {
                    "client_id": child.client_id,
                    "copy_status": child.copy_status,
                    "multiplier": child.multiplier,
                    "last_copied_trade_id": child.last_copied_trade_id
                }

                child.copy_status = "On"
                child.last_copied_trade_id = master_latest_order_id
                
                results.append({
                    "client_id": child.client_id,
                    "broker": child.broker,
                    "username": child.username,
                    "multiplier": child.multiplier,
                    "status": "SUCCESS",
                    "previous_status": previous_state["copy_status"],
                    "new_marker": master_latest_order_id,
                    "message": "Copy trading started successfully"
                })
                
                started_count += 1
                logger.debug(f"Started copying for child {child.client_id}")

            except Exception as e:
                logger.error(f"Failed to start copying for child {child.client_id}: {str(e)}")
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
            logger.info(f"Successfully started copying for {started_count} children of master {master_id}")
        except Exception as e:
            logger.error(f"Failed to commit bulk changes: {str(e)}")
            db.session.rollback()
            return safe_json_response({
                "error": "Failed to save bulk configuration changes",
                "details": str(e),
                "partial_success": False
            }, 500)

        try:
            log_entry = SystemLog(
                timestamp=datetime.utcnow().isoformat(),
                level="INFO",
                message=f"Bulk start copy: {started_count} children started for master {master_id}",
                user_id=str(request.current_user.id),
                details=json.dumps({
                    "action": "start_copy_all",
                    "master_id": master_id,
                    "user": request.current_user.email,
                    "started_count": started_count,
                    "failed_count": failed_count,
                    "master_marker": master_latest_order_id,
                    "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    "children_affected": [r["client_id"] for r in results if r["status"] == "SUCCESS"]
                })
            )
            db.session.add(log_entry)
            db.session.commit()
        except Exception as e:
            logger.warning(f"Failed to log bulk start action: {str(e)}")

        response_data = {
            "message": f"Bulk start completed for master {master_id}",
            "master_id": master_id,
            "master_broker": master_account.broker,
            "master_username": master_account.username,
            "master_marker": master_latest_order_id,
            "summary": {
                "total_children": len(stopped_children),
                "eligible_to_start": len(stopped_children),
                "started_successfully": started_count,
                "failed": failed_count,
                "success_rate": f"{(started_count/len(stopped_children)*100):.1f}%" if stopped_children else "0%"
            },
            "details": results,
            "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        }

        if failed_count == 0:
            return jsonify(response_data), 200
        elif started_count > 0:
            response_data["warning"] = f"{failed_count} accounts failed to start"
            return jsonify(response_data), 207
        else:
            response_data["error"] = "Failed to start any child accounts"
            return jsonify(response_data), 500

    except Exception as e:
        logger.error(f"Unexpected error in start_copy_all: {str(e)}")
        return safe_json_response({
            "error": "Internal server error",
            "details": str(e)
        }, 500)

@app.route('/api/stop-copy-all', methods=['POST'])
@limiter.limit("5 per minute")
@require_user
def stop_copy_all():
    """Stop copying for all children of a master - Complete Database Version."""
    logger.info("Processing bulk stop copy request")
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        master_id = data.get("master_id")
        
        if not master_id:
            logger.error("Missing master_id in request")
            return jsonify({"error": "Missing master_id"}), 400

        if not validate_client_id(master_id):
            return jsonify({"error": "Invalid master_id format"}), 400

        master_account = Account.query.filter_by(
            user_id=request.current_user.id,
            client_id=master_id,
            role='master'
        ).first()
        
        if not master_account:
            logger.error(f"Master account not found or not accessible: {master_id}")
            return jsonify({"error": "Master account not found or not accessible"}), 404

        active_children = Account.query.filter_by(
            user_id=request.current_user.id,
            role='child',
            linked_master_id=master_id,
            copy_status='On'
        ).all()

        if not active_children:
            linked_children = Account.query.filter_by(
                user_id=request.current_user.id,
                role='child',
                linked_master_id=master_id
            ).all()
            
            return jsonify({
                "message": f"No active child accounts found for master {master_id}",
                "master_id": master_id,
                "total_children": len(linked_children),
                "already_stopped": len([c for c in linked_children if c.copy_status == "Off"]),
                "stopped_count": 0,
                "details": []
            }), 200

        logger.info(f"Found {len(active_children)} active children to stop for master {master_id}")

        results = []
        stopped_count = 0
        failed_count = 0

        for child in active_children:
            try:
                previous_state = {
                    "client_id": child.client_id,
                    "copy_status": child.copy_status,
                    "multiplier": child.multiplier,
                    "last_copied_trade_id": child.last_copied_trade_id
                }

                child.copy_status = "Off"
                
                results.append({
                    "client_id": child.client_id,
                    "broker": child.broker,
                    "username": child.username,
                    "multiplier": child.multiplier,
                    "status": "SUCCESS",
                    "previous_status": previous_state["copy_status"],
                    "message": "Copy trading stopped successfully"
                })
                
                stopped_count += 1
                logger.debug(f"Stopped copying for child {child.client_id}")

            except Exception as e:
                logger.error(f"Failed to stop copying for child {child.client_id}: {str(e)}")
                results.append({
                    "client_id": child.client_id,
                    "broker": child.broker or "Unknown",
                    "username": child.username or "Unknown",
                    "multiplier": child.multiplier or 1.0,
                    "status": "ERROR",
                    "message": f"Failed to stop: {str(e)}"
                })
                failed_count += 1

        try:
            db.session.commit()
            logger.info(f"Successfully stopped copying for {stopped_count} children of master {master_id}")
        except Exception as e:
            logger.error(f"Failed to commit bulk changes: {str(e)}")
            db.session.rollback()
            return safe_json_response({
                "error": "Failed to save bulk configuration changes",
                "details": str(e),
                "partial_success": False
            }, 500)

        try:
            log_entry = SystemLog(
                timestamp=datetime.utcnow().isoformat(),
                level="INFO",
                message=f"Bulk stop copy: {stopped_count} children stopped for master {master_id}",
                user_id=str(request.current_user.id),
                details=json.dumps({
                    "action": "stop_copy_all",
                    "master_id": master_id,
                    "user": request.current_user.email,
                    "stopped_count": stopped_count,
                    "failed_count": failed_count,
                    "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    "children_affected": [r["client_id"] for r in results if r["status"] == "SUCCESS"]
                })
            )
            db.session.add(log_entry)
            db.session.commit()
        except Exception as e:
            logger.warning(f"Failed to log bulk stop action: {str(e)}")

        response_data = {
            "message": f"Bulk stop completed for master {master_id}",
            "master_id": master_id,
            "master_broker": master_account.broker,
            "master_username": master_account.username,
            "summary": {
                "total_children": len(active_children),
                "eligible_to_stop": len(active_children),
                "stopped_successfully": stopped_count,
                "failed": failed_count,
                "success_rate": f"{(stopped_count/len(active_children)*100):.1f}%" if active_children else "0%"
            },
            "details": results,
            "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        }

        if failed_count == 0:
            return jsonify(response_data), 200
        elif stopped_count > 0:
            response_data["warning"] = f"{failed_count} accounts failed to stop"
            return jsonify(response_data), 207
        else:
            response_data["error"] = "Failed to stop any child accounts"
            return jsonify(response_data), 500

    except Exception as e:
        logger.error(f"Unexpected error in stop_copy_all: {str(e)}")
        return safe_json_response({
            "error": "Internal server error",
            "details": str(e)
        }, 500)

@app.route('/api/positions/<client_id>')
@limiter.limit("60 per minute")
@require_user
def get_positions(client_id):
    try:
        if not validate_client_id(client_id):
            return jsonify({"error": "Invalid client_id format"}), 400

        account = Account.query.filter_by(
            user_id=request.current_user.id,
            client_id=client_id
        ).first()
        
        if not account:
            return jsonify({"error": "Account not found"}), 404

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
            
        return jsonify(positions)
        
    except Exception as e:
        logger.error(f"Error in get_positions: {str(e)}")
        return safe_json_response({"error": str(e)}, 500)

@app.route("/api/broker-status")
def broker_status():
    """Check the status of various broker APIs."""
    results = {}
    for broker, url in BROKER_STATUS_URLS.items():
        results[broker] = check_api(url)
    results["uptime"] = format_uptime()
    return jsonify(results)

@app.route("/api/test-webhook/<user_id>", methods=["POST"])
@limiter.limit("10 per minute")
def test_webhook(user_id):
    """Test webhook endpoint for development/debugging."""
    data = request.json or {}
    return jsonify({
        "message": "Test webhook received",
        "user_id": user_id,
        "data": data,
        "timestamp": datetime.now().isoformat()
    })

@app.route("/api/logs")
@limiter.limit("30 per minute")
@require_user
def get_logs():
    """Get trade logs for the current user."""
    try:
        # Get logs for user's accounts only
        user_client_ids = [acc.client_id for acc in request.current_user.accounts]
        
        logs = TradeLog.query.filter(
            TradeLog.user_id.in_(user_client_ids)
        ).order_by(TradeLog.timestamp.desc()).limit(100).all()
        
        return jsonify([{
            "timestamp": log.timestamp,
            "user_id": log.user_id,
            "symbol": log.symbol,
            "action": log.action,
            "quantity": log.quantity,
            "status": log.status,
            "response": log.response
        } for log in logs])
        
    except Exception as e:
        logger.error(f"Error in get_logs: {str(e)}")
        return safe_json_response({"error": str(e)}, 500)

@app.route("/api/trades")
@limiter.limit("30 per minute")
@require_user
def get_trades():
    """Get trade history for the current user."""
    try:
        trades = Trade.query.filter_by(
            user_id=request.current_user.id
        ).order_by(Trade.timestamp.desc()).limit(100).all()
        
        return jsonify([{
            "id": trade.id,
            "symbol": trade.symbol,
            "action": trade.action,
            "qty": trade.qty,
            "price": trade.price,
            "status": trade.status,
            "timestamp": trade.timestamp
        } for trade in trades])
        
    except Exception as e:
        logger.error(f"Error in get_trades: {str(e)}")
        return safe_json_response({"error": str(e)}, 500)

@app.route("/api/system-status")
def system_status():
    """Get comprehensive system status."""
    try:
        # Check database
        db.session.execute('SELECT 1')
        
        # Count various entities
        total_users = User.query.count()
        total_accounts = Account.query.count()
        active_masters = Account.query.filter_by(role='master').count()
        active_children = Account.query.filter_by(role='child', copy_status='On').count()
        
        # Check scheduler
        scheduler_status = "running" if _scheduler and _scheduler.running else "stopped"
        
        # Get broker statuses
        broker_statuses = {}
        for broker, url in BROKER_STATUS_URLS.items():
            broker_statuses[broker] = check_api(url)
        
        return jsonify({
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "uptime": format_uptime(),
            "environment": ENVIRONMENT,
            "statistics": {
                "total_users": total_users,
                "total_accounts": total_accounts,
                "active_masters": active_masters,
                "active_children": active_children
            },
            "services": {
                "database": "connected",
                "scheduler": scheduler_status,
                "brokers": broker_statuses
            },
            "configuration": {
                "polling_interval": POLLING_INTERVAL,
                "max_masters_per_cycle": MAX_MASTERS_PER_CYCLE,
                "log_level": LOG_LEVEL
            }
        }), 200
        
    except Exception as e:
        return safe_json_response({
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }, 503)

# User Authentication Routes
@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        email = request.form["email"]
        password = request.form["password"]
        
        if User.query.filter_by(email=email).first():
            flash("Email already exists", "error")
            return render_template("register.html")
        
        # Generate webhook token
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

# Dashboard Routes
@app.route("/")
@login_required
def Dashboard():
    return render_template("dashboard.html")

@app.route("/add-account")
@login_required
def AddAccount():
    return render_template("add_account.html")

@app.route("/master-child")
@login_required
def MasterChild():
    return render_template("master_child.html")

@app.route("/groups")
@login_required
def Groups():
    return render_template("groups.html")

@app.route("/order-book")
@login_required
def OrderBook():
    return render_template("order_book.html")

@app.route("/trades")
@login_required
def Trades():
    return render_template("trades.html")

@app.route("/logs")
@login_required
def Logs():
    return render_template("logs.html")

@app.route("/settings")
@login_required
def Settings():
    return render_template("settings.html")

@app.route("/help")
@login_required
def Help():
    return render_template("help.html")

# Admin Routes
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

@app.route("/admin/settings", methods=["POST"])
@admin_login_required
def admin_update_settings():
    settings = {
        'trading_enabled': request.form.get('trading_enabled') == 'on',
        'max_accounts_per_user': request.form.get('max_accounts_per_user', '10'),
        'default_multiplier': request.form.get('default_multiplier', '1.0'),
        'polling_interval': request.form.get('polling_interval', str(POLLING_INTERVAL))
    }
    
    save_settings(settings)
    flash("Settings updated successfully!", "success")
    return redirect(url_for("admin_dashboard"))

# Database Backup Strategy
@app.route('/api/admin/backup-db', methods=['POST'])
@admin_login_required
def backup_database():
    """Create database backup"""
    try:
        backup_dir = os.path.join(DATA_DIR, "backups")
        os.makedirs(backup_dir, exist_ok=True)
        
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        backup_file = os.path.join(backup_dir, f"quantbot_backup_{timestamp}.db")
        
        # Create backup
        shutil.copy2(DB_PATH, backup_file)
        
        # Clean old backups (keep last 10)
        backups = sorted(
            [f for f in os.listdir(backup_dir) if f.startswith("quantbot_backup_")],
            reverse=True
        )
        for old_backup in backups[10:]:
            os.remove(os.path.join(backup_dir, old_backup))
        
        file_size = os.path.getsize(backup_file)
        
        logger.info(f"Database backup created: {backup_file} ({file_size} bytes)")
        
        return jsonify({
            "message": "Database backup created successfully",
            "backup_file": os.path.basename(backup_file),
            "timestamp": timestamp,
            "size_bytes": file_size,
            "location": backup_dir
        })
        
    except Exception as e:
        logger.error(f"Database backup failed: {str(e)}")
        return safe_json_response({"error": f"Backup failed: {str(e)}"}, 500)

@app.route('/api/admin/backups', methods=['GET'])
@admin_login_required
def list_backups():
    """List available database backups"""
    try:
        backup_dir = os.path.join(DATA_DIR, "backups")
        if not os.path.exists(backup_dir):
            return jsonify({"backups": []})
        
        backups = []
        for filename in os.listdir(backup_dir):
            if filename.startswith("quantbot_backup_") and filename.endswith(".db"):
                filepath = os.path.join(backup_dir, filename)
                stat = os.stat(filepath)
                backups.append({
                    "filename": filename,
                    "created": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                    "size_bytes": stat.st_size,
                    "size_mb": round(stat.st_size / 1024 / 1024, 2)
                })
        
        backups.sort(key=lambda x: x["created"], reverse=True)
        return jsonify({"backups": backups})
        
    except Exception as e:
        logger.error(f"Failed to list backups: {str(e)}")
        return safe_json_response({"error": str(e)}, 500)

# Auto backup scheduler
def auto_backup_database():
    """Automatically backup database"""
    try:
        if ENVIRONMENT == "production":
            backup_dir = os.path.join(DATA_DIR, "backups")
            os.makedirs(backup_dir, exist_ok=True)
            
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            backup_file = os.path.join(backup_dir, f"auto_backup_{timestamp}.db")
            
            shutil.copy2(DB_PATH, backup_file)
            logger.info(f"Auto backup created: {backup_file}")
            
            # Clean old auto backups (keep last 48 hours worth)
            cutoff = datetime.utcnow() - timedelta(hours=48)
            for filename in os.listdir(backup_dir):
                if filename.startswith("auto_backup_"):
                    filepath = os.path.join(backup_dir, filename)
                    if datetime.fromtimestamp(os.path.getctime(filepath)) < cutoff:
                        os.remove(filepath)
                        
    except Exception as e:
        logger.error(f"Auto backup failed: {str(e)}")

# Error handlers
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

# Context processors for templates
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
        "uptime": format_uptime()
    }

# Graceful Shutdown Handling
import signal
import sys
import threading
import atexit

class GracefulShutdown:
    """Handle graceful shutdown of the application"""
    
    def __init__(self):
        self.shutdown_requested = False
        self.active_requests = 0
        self.shutdown_timeout = 30  # seconds
        
    def request_shutdown(self):
        """Request graceful shutdown"""
        self.shutdown_requested = True
        
    def is_shutdown_requested(self):
        """Check if shutdown was requested"""
        return self.shutdown_requested
        
    def increment_active_requests(self):
        """Increment active request counter"""
        self.active_requests += 1
        
    def decrement_active_requests(self):
        """Decrement active request counter"""
        self.active_requests = max(0, self.active_requests - 1)
        
    def wait_for_requests_to_finish(self):
        """Wait for active requests to complete"""
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

def signal_handler(sig, frame):
    """Handle graceful shutdown signals"""
    signal_names = {
        signal.SIGINT: 'SIGINT',
        signal.SIGTERM: 'SIGTERM'
    }
    signal_name = signal_names.get(sig, f'Signal {sig}')
    
    logger.info(f"🛑 Received {signal_name}, initiating graceful shutdown...")
    
    # Request shutdown
    shutdown_handler.request_shutdown()
    
    # Wait for active requests
    shutdown_handler.wait_for_requests_to_finish()
    
    # Stop background scheduler
    if _scheduler and _scheduler.running:
        try:
            logger.info("Stopping background scheduler...")
            _scheduler.shutdown(wait=True)
            logger.info("✅ Scheduler stopped successfully")
        except Exception as e:
            logger.error(f"Error stopping scheduler: {e}")
    
    # Close database connections
    try:
        logger.info("Closing database connections...")
        db.session.close()
        logger.info("✅ Database connections closed")
    except Exception as e:
        logger.error(f"Error closing database: {e}")
    
    # Final backup if in production
    if ENVIRONMENT == "production":
        try:
            logger.info("Creating shutdown backup...")
            auto_backup_database()
            logger.info("✅ Shutdown backup completed")
        except Exception as e:
            logger.error(f"Error creating shutdown backup: {e}")
    
    logger.info("👋 Graceful shutdown complete")
    sys.exit(0)

def cleanup_on_exit():
    """Cleanup function called on normal exit"""
    logger.info("🧹 Performing cleanup on exit...")
    
    # Stop scheduler if running
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
    
    # Close database
    try:
        db.session.close()
    except Exception:
        pass

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
signal.signal(signal.SIGTERM, signal_handler)  # Termination signal
atexit.register(cleanup_on_exit)               # Normal exit

# Shutdown status endpoint
@app.route('/api/admin/shutdown-status')
@admin_login_required
def shutdown_status():
    """Get shutdown status"""
    return jsonify({
        "shutdown_requested": shutdown_handler.is_shutdown_requested(),
        "active_requests": shutdown_handler.active_requests,
        "uptime": format_uptime(),
        "scheduler_running": _scheduler.running if _scheduler else False
    })

@app.route('/api/admin/shutdown', methods=['POST'])
@admin_login_required
def initiate_shutdown():
    """Initiate graceful shutdown (admin only)"""
    logger.warning(f"Manual shutdown initiated by admin user")
    
    # Start shutdown in a separate thread
    def delayed_shutdown():
        time.sleep(2)  # Give time for response
        os.kill(os.getpid(), signal.SIGTERM)
    
    threading.Thread(target=delayed_shutdown, daemon=True).start()
    
    return jsonify({
        "message": "Graceful shutdown initiated",
        "estimated_time": "30 seconds maximum"
    })

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
    
    app.run(
        host="0.0.0.0",
        port=port,
        debug=debug,
        threaded=True
    )
