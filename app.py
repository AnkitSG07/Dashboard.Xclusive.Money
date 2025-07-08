import os
import uuid
import json
import logging
import random
import re
import string
from datetime import datetime, timedelta, date
from functools import wraps
from urllib.parse import quote
import atexit

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from bs4 import BeautifulSoup
from dhanhq import dhanhq
from flask import (
    Flask,
    Blueprint,
    request,
    jsonify,
    render_template,
    session,
    redirect,
    url_for,
    flash,
)
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from flask_wtf.csrf import CSRFProtect
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

# Import from local modules
from brokers.factory import get_broker_class
from brokers.zerodha import ZerodhaBroker, KiteConnect
from brokers.fyers import FyersBroker

try:
    from brokers.symbol_map import get_symbol_for_broker
except ImportError:
    def get_symbol_for_broker(symbol: str, broker: str) -> dict:
        """Fallback stub returning empty mapping."""
        return {}


# ==============================================================================
# Global Constants and Setup
# ==============================================================================
# Current date/time info
CURRENT_UTC_TIME = "2025-07-08 06:28:03"
CURRENT_USER = "AnkitSG07"

# Broker status URLs for health checks
BROKER_STATUS_URLS = {
    "dhan": "https://api.dhan.co",
    "zerodha": "https://api.kite.trade",
    "aliceblue": "https://ant.aliceblueonline.com",
    "finvasia": "https://api.shoonya.com",
    "fyers": "https://api.fyers.in",
    "groww": "https://groww.in",
}

# ==============================================================================
# Logging Configuration
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==============================================================================
# Database and Extensions Initialization
# ==============================================================================
db = SQLAlchemy()
csrf = CSRFProtect()

# Define emoji regex pattern for sanitization
EMOJI_RE = re.compile("[\U00010000-\U0010ffff]", flags=re.UNICODE)

# Store application start time for uptime calculation
start_time = datetime.utcnow()


# ==============================================================================
# Application Configuration
# ==============================================================================
class Config:
    """Application configuration."""
    SECRET_KEY = os.environ.get("SECRET_KEY", "change-me-in-production-environment")
    
    # PRODUCTION-READINESS: Use PostgreSQL for production
    # SQLite is not suitable for multi-user SaaS applications
    DATABASE_URL = os.environ.get("DATABASE_URL")
    if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
        # Handle Heroku-style PostgreSQL URLs
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    
    SQLALCHEMY_DATABASE_URI = DATABASE_URL or "postgresql://postgres:password@localhost/quantbot"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ENGINE_OPTIONS = {
        "pool_pre_ping": True,
        "pool_size": 10,
        "max_overflow": 20,
        "pool_recycle": 3600
    }
    
    # Admin credentials should always be loaded from environment variables
    ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "admin@example.com")
    ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "admin")
    
    # Data directory for any file storage needs
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.environ.get("DATA_DIR", os.path.join(BASE_DIR, "data"))
    
    # CSRF protection
    WTF_CSRF_ENABLED = True
    WTF_CSRF_TIME_LIMIT = 3600  # 1 hour
    
    # File upload settings
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16 MB max upload
    UPLOAD_FOLDER = os.path.join(BASE_DIR, "static", "uploads")
    ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}


# ==============================================================================
# Database Models
# ==============================================================================
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(256), nullable=False)
    name = db.Column(db.String(120))
    profile_image = db.Column(db.String(120))
    plan = db.Column(db.String(50), default="Free")
    subscription_start = db.Column(db.DateTime, default=datetime.utcnow)
    last_login = db.Column(db.DateTime)
    webhook_token = db.Column(db.String(64), unique=True, default=lambda: uuid.uuid4().hex)
    accounts = db.relationship("Account", backref="user", lazy=True, cascade="all, delete-orphan")
    groups = db.relationship("Group", backref="user", lazy=True, cascade="all, delete-orphan")
    trades = db.relationship("Trade", backref="user", lazy=True, cascade="all, delete-orphan")

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)


class Account(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    broker = db.Column(db.String(50), nullable=False)
    client_id = db.Column(db.String(50), nullable=False, index=True)
    username = db.Column(db.String(120))
    credentials = db.Column(db.JSON, nullable=False)
    status = db.Column(db.String(20), default="Connected")
    role = db.Column(db.String(20))  # 'master' or 'child'
    linked_master_id = db.Column(db.String(50), index=True)
    copy_status = db.Column(db.String(10), default="Off")  # 'On' or 'Off'
    multiplier = db.Column(db.Float, default=1.0)
    last_copied_trade_id = db.Column(db.String(50))
    auto_login = db.Column(db.Boolean, default=True)
    last_login_time = db.Column(db.DateTime)
    token_expiry = db.Column(db.DateTime)
    device_number = db.Column(db.String(64))
    
    # Ensure each user can only have one account with the same broker/client_id combination
    __table_args__ = (db.UniqueConstraint("user_id", "client_id", "broker", name="uq_user_client_broker"),)


class Trade(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    symbol = db.Column(db.String(100), nullable=False)
    action = db.Column(db.String(10), nullable=False)
    qty = db.Column(db.Integer, nullable=False)
    price = db.Column(db.Float)
    status = db.Column(db.String(20), nullable=False, index=True)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, index=True)


class WebhookLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), index=True)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    payload = db.Column(db.Text)
    response_status = db.Column(db.String(20))
    response_body = db.Column(db.Text)


class SystemLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    level = db.Column(db.String(20), nullable=False)  # INFO, WARNING, ERROR
    message = db.Column(db.Text, nullable=False)
    user_id = db.Column(db.String(120))
    details = db.Column(db.JSON)


class Setting(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(50), unique=True, nullable=False, index=True)
    value = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text)


class Group(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    name = db.Column(db.String(100), nullable=False)
    description = db.Column(db.Text)
    accounts = db.relationship(
        "Account",
        secondary="group_accounts",
        lazy="subquery",
        backref=db.backref("groups", lazy=True),
    )
    
    # Each user can only have one group with a given name
    __table_args__ = (db.UniqueConstraint("user_id", "name", name="uq_user_group_name"),)


group_accounts = db.Table(
    "group_accounts",
    db.Column("group_id", db.Integer, db.ForeignKey("group.id"), primary_key=True),
    db.Column("account_id", db.Integer, db.ForeignKey("account.id"), primary_key=True),
)


class OrderMapping(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    master_order_id = db.Column(db.String(50), nullable=False, index=True)
    child_order_id = db.Column(db.String(50), nullable=False)
    master_client_id = db.Column(db.String(50), nullable=False)
    master_broker = db.Column(db.String(50))
    child_client_id = db.Column(db.String(50), nullable=False, index=True)
    child_broker = db.Column(db.String(50))
    symbol = db.Column(db.String(100), nullable=False)
    status = db.Column(db.String(20), default="ACTIVE", index=True)  # ACTIVE, COMPLETED, SQUARED_OFF, CANCELLED
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    child_timestamp = db.Column(db.DateTime)
    remarks = db.Column(db.String(255))
    multiplier = db.Column(db.Float, default=1.0)
    action = db.Column(db.String(10))
    quantity = db.Column(db.Integer)
    price = db.Column(db.Float)


class TradeLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    user_id = db.Column(db.String(50), nullable=False, index=True)
    symbol = db.Column(db.String(100))
    action = db.Column(db.String(20))
    quantity = db.Column(db.Integer)
    status = db.Column(db.String(20), index=True)
    response = db.Column(db.Text)


# ==============================================================================
# Blueprints
# ==============================================================================
main_bp = Blueprint('main', __name__)
api_bp = Blueprint('api', __name__, url_prefix='/api')
admin_bp = Blueprint('admin', __name__, url_prefix='/admin')
webhook_bp = Blueprint('webhook', __name__, url_prefix='/webhook')


# ==============================================================================
# Authentication Decorators
# ==============================================================================
def login_required(view):
    @wraps(view)
    def wrapped_view(*args, **kwargs):
        if "user_id" not in session:
            flash("Please log in to access this page.", "warning")
            return redirect(url_for("main.login"))
        return view(*args, **kwargs)
    return wrapped_view


def admin_required(view):
    @wraps(view)
    def wrapped_view(*args, **kwargs):
        if not session.get("admin"):
            flash("Admin access required.", "danger")
            return redirect(url_for("main.login"))
        if session.get("admin_email") != Config.ADMIN_EMAIL:
            session.clear()
            flash("Admin session is invalid.", "danger")
            return redirect(url_for("main.login"))
        return view(*args, **kwargs)
    return wrapped_view


# ==============================================================================
# Helper & Utility Functions
# ==============================================================================
def _account_to_dict(acc: Account) -> dict:
    """Convert Account model to dictionary."""
    return {
        "id": acc.id,
        "owner": acc.user.email if acc.user else None,
        "broker": acc.broker,
        "client_id": acc.client_id,
        "username": acc.username,
        "token_expiry": acc.token_expiry.isoformat() if acc.token_expiry else None,
        "status": acc.status,
        "role": acc.role,
        "linked_master_id": acc.linked_master_id,
        "copy_status": acc.copy_status,
        "multiplier": acc.multiplier,
        "credentials": acc.credentials,
        "last_copied_trade_id": acc.last_copied_trade_id,
        "auto_login": acc.auto_login,
        "last_login": acc.last_login_time.isoformat() if acc.last_login_time else None,
        "device_number": acc.device_number,
    }


def _group_to_dict(g: Group) -> dict:
    """Convert Group model to dictionary."""
    return {
        "name": g.name,
        "owner": g.user.email if g.user else None,
        "members": [a.client_id for a in g.accounts],
        "description": g.description
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


def map_order_type(order_type: str, broker: str) -> str:
    """Convert generic order types to broker specific codes."""
    if not order_type:
        return ""
    broker = broker.lower() if broker else ""
    if broker in ("aliceblue", "finvasia") and order_type.upper() == "MARKET":
        return "MKT"
    return str(order_type)


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
    """Parse diverse timestamp formats to a datetime object."""
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
    
    if not BrokerClass:
        raise ValueError(f"No broker class found for '{broker}'")
    
    # Keep remaining credentials for passing to constructor
    rest = {k: v for k, v in credentials.items() if k != "access_token"}

    if broker == "aliceblue":
        api_key = rest.pop("api_key", None)
        if not api_key:
            raise ValueError("API key required for AliceBlue")
        return BrokerClass(client_id, api_key, **rest)

    elif broker == "finvasia":
        required = ["password", "totp_secret", "vendor_code", "api_key"]
        missing = [r for r in required if not rest.get(r)]
        if missing:
            raise ValueError(f"Missing required Finvasia credentials: {', '.join(missing)}")
            
        password = rest.pop("password", None)
        totp_secret = rest.pop("totp_secret", None)
        vendor_code = rest.pop("vendor_code", None)
        api_key = rest.pop("api_key", None)
        imei = rest.pop("imei", "abc1234") or "abc1234"
        
        # Pass args as named, not positional, for clarity
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
        if not access_token:
            raise ValueError("Access token required for Groww")
        return BrokerClass(client_id, access_token, **rest)
        
    # Default case for other brokers
    if not access_token:
        raise ValueError(f"Access token required for {broker}")
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


def get_user_by_token(token: str):
    """Get user by webhook token."""
    return User.query.filter_by(webhook_token=token).first()


def get_primary_account(user):
    """Get the primary account for a user."""
    if not user:
        return None
    return Account.query.filter_by(user_id=user.id).first()


def get_accounts_for_user(user_id: int):
    """Get all accounts for a user."""
    return Account.query.filter_by(user_id=user_id).all()


def get_master_accounts(user_id=None):
    """Get all master accounts with their children."""
    query = Account.query.filter_by(role='master')
    if user_id:
        query = query.filter_by(user_id=user_id)
    
    masters = query.all()
    result = []
    
    for master in masters:
        children_query = Account.query.filter_by(role='child', linked_master_id=master.client_id)
        if user_id:
            children_query = children_query.filter_by(user_id=user_id)
            
        children = children_query.all()
        master_dict = _account_to_dict(master)
        master_dict['children'] = [_account_to_dict(child) for child in children]
        result.append(master_dict)
        
    return result


def get_account_by_client_id(client_id: str, user_id=None):
    """Get account by client ID, optionally filtered by user."""
    query = Account.query.filter_by(client_id=client_id)
    if user_id:
        query = query.filter_by(user_id=user_id)
    return query.first()


def format_uptime():
    """Format the application uptime."""
    delta = datetime.utcnow() - start_time
    hours, remainder = divmod(int(delta.total_seconds()), 3600)
    minutes, _ = divmod(remainder, 60)
    return f"{hours}h {minutes}m"


def check_api(url: str) -> bool:
    """Check if a broker API endpoint is reachable."""
    try:
        resp = requests.get(url, timeout=3)
        return resp.ok
    except Exception:
        return False


def find_account_by_client_id(client_id, user_id=None):
    """Return (account, parent_master) for the provided client_id."""
    query = Account.query.filter_by(client_id=str(client_id))
    if user_id:
        query = query.filter_by(user_id=user_id)
        
    acc = query.first()
    if not acc:
        return None, None

    if acc.role == "child" and acc.linked_master_id:
        master_query = Account.query.filter_by(client_id=acc.linked_master_id)
        if user_id:
            master_query = master_query.filter_by(user_id=user_id)
        master = master_query.first()
        return acc, master
        
    return acc, None


def clean_response_message(response):
    """Extract clean error message from broker API response."""
    if isinstance(response, dict):
        remarks = response.get("remarks")
        if isinstance(remarks, dict):
            return (
                remarks.get("errorMessage")
                or remarks.get("error_message")
                or str(remarks)
            )

        # Common error fields returned by different broker APIs
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

        # Fallback to raw JSON dump for debugging
        return json.dumps(response)

    return str(response)


def save_log(user_id, symbol, action, quantity, status, response):
    """Persist a trade log entry."""
    log = TradeLog(
        timestamp=datetime.now(),
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
    """Persist order mapping to the database."""
    mapping = OrderMapping(
        master_order_id=master_order_id,
        child_order_id=child_order_id,
        master_client_id=master_id,
        master_broker=master_broker,
        child_client_id=child_id,
        child_broker=child_broker,
        symbol=symbol,
        status="ACTIVE",
        timestamp=datetime.utcnow()
    )
    db.session.add(mapping)
    db.session.commit()
    return mapping.id


def record_trade(user_id, symbol, action, qty, price, status):
    """Persist a trade record to the database."""
    trade = Trade(
        user_id=user_id,
        symbol=symbol,
        action=action,
        qty=int(qty),
        price=float(price or 0),
        status=status,
        timestamp=datetime.now()
    )
    db.session.add(trade)
    db.session.commit()
    return trade.id


def get_pending_zerodha() -> dict:
    """Get pending Zerodha authentication requests."""
    setting = Setting.query.filter_by(key="pending_zerodha").first()
    if setting:
        try:
            return json.loads(setting.value)
        except Exception:
            return {}
    return {}


def set_pending_zerodha(data: dict):
    """Save pending Zerodha authentication requests."""
    setting = Setting.query.filter_by(key="pending_zerodha").first()
    if not setting:
        setting = Setting(key="pending_zerodha", value=json.dumps(data))
        db.session.add(setting)
    else:
        setting.value = json.dumps(data)
    db.session.commit()


def get_pending_fyers() -> dict:
    """Get pending Fyers authentication requests."""
    setting = Setting.query.filter_by(key="pending_fyers").first()
    if setting:
        try:
            return json.loads(setting.value)
        except Exception:
            return {}
    return {}


def set_pending_fyers(data: dict):
    """Save pending Fyers authentication requests."""
    setting = Setting.query.filter_by(key="pending_fyers").first()
    if not setting:
        setting = Setting(key="pending_fyers", value=json.dumps(data))
        db.session.add(setting)
    else:
        setting.value = json.dumps(data)
    db.session.commit()


def load_settings():
    """Load all application settings from the database."""
    result = {'trading_enabled': True}
    for s in Setting.query.all():
        if s.key == 'trading_enabled':
            result['trading_enabled'] = s.value.lower() == 'true'
        else:
            result[s.key] = s.value
    return result


def save_settings(settings):
    """Save application settings to the database."""
    for key, value in settings.items():
        s = Setting.query.filter_by(key=key).first()
        if not s:
            s = Setting(key=key, value=str(value))
            db.session.add(s)
        else:
            s.value = str(value)
    db.session.commit()


def save_account_to_user(user_id, account: dict):
    """Persist account credentials to the database."""
    user = User.query.get(user_id)
    if not user:
        raise ValueError(f"User not found: {user_id}")

    acc = Account(
        user_id=user.id,
        broker=account.get("broker"),
        client_id=account.get("client_id"),
        username=account.get("username"),
        token_expiry=account.get("token_expiry"),
        status=account.get("status", "active"),
        role=account.get("role"),
        linked_master_id=account.get("linked_master_id"),
        copy_status=account.get("copy_status", "Off"),
        multiplier=account.get("multiplier", 1.0),
        credentials=account.get("credentials"),
        last_copied_trade_id=account.get("last_copied_trade_id"),
        auto_login=account.get("auto_login", True),
        last_login_time=datetime.now(),
        device_number=account.get("device_number")
    )
    db.session.add(acc)
    db.session.commit()
    return acc.id


# ==============================================================================
# Background Tasks
# ==============================================================================
def poll_and_copy_trades(app):
    """Run trade copying logic with full database storage."""
    with app.app_context():
        logger.info("🔄 Starting poll_and_copy_trades() cycle...")
        
        try:
            # Check if trading is enabled globally
            settings = load_settings()
            if settings.get('trading_enabled') != True:
                logger.info("Trading is disabled globally. Skipping copy cycle.")
                return
        
            # Load master accounts from database
            masters = Account.query.filter_by(role='master').all()
            if not masters:
                logger.warning("No master accounts configured")
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
                    master_api = broker_api(_account_to_dict(master))
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
                    
                    # Fallback for AliceBlue if trade book is empty
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
                    logger.info(f"No orders found for master {master_id}")
                    continue

                # Sort orders newest first
                try:
                    order_list = sorted(order_list, key=get_order_sort_key, reverse=True)
                except Exception as e:
                    logger.error(f"Failed to sort orders for master {master_id}: {str(e)}")
                    continue

                # Get active child accounts for this master from database
                children = Account.query.filter_by(
                    role='child',
                    linked_master_id=master_id,
                    copy_status='On'
                ).all()

                if not children:
                    logger.info(f"No active children found for master {master_id}")
                    continue

                logger.info(f"Processing {len(children)} active children for master {master_id}")

                # Process each child account
                for child in children:
                    child_id = child.client_id
                    last_copied_trade_id = child.last_copied_trade_id
                    new_last_trade_id = None

                    # Initialize marker if not set - use most recent order to prevent historical copying
                    if not last_copied_trade_id and order_list:
                        first = order_list[0]
                        init_id = (
                            first.get("orderId")
                            or first.get("order_id")
                            or first.get("id")
                            or first.get("NOrdNo")
                            or first.get("nestOrderNumber")
                            or first.get("orderNumber")
                            or first.get("Nstordno")
                            or first.get("norenordno")  # Finvasia
                        )
                        if init_id:
                            child.last_copied_trade_id = str(init_id)
                            db.session.commit()
                            logger.info(f"Initialized marker for child {child_id} to {init_id}")
                        continue

                    logger.debug(f"Processing child {child_id}, last copied: {last_copied_trade_id}")

                    # Process each order from newest to oldest
                    for order in order_list:
                        order_id = (
                            order.get("orderId")
                            or order.get("order_id")
                            or order.get("id")
                            or order.get("NOrdNo")
                            or order.get("nestOrderNumber")
                            or order.get("orderNumber")
                            or order.get("Nstordno")  # Alice Blue
                            or order.get("norenordno")  # Finvasia
                        )
                        
                        if not order_id:
                            continue
                            
                        # Stop when we reach the last copied trade
                        if str(order_id) == last_copied_trade_id:
                            logger.info(f"[{master_id}->{child_id}] Reached last copied trade {order_id}. Stopping here.")
                            break

                        # Track the newest trade ID for updating marker
                        if new_last_trade_id is None:
                            new_last_trade_id = str(order_id)

                        # Validate and extract order data
                        try:
                            # Extract filled quantity
                            filled_qty = int(
                                order.get("filledQuantity")
                                or order.get("filled_qty")
                                or order.get("filledQty")
                                or order.get("tradedQty")
                                or order.get("executed_qty")
                                or order.get("filled_quantity")
                                or order.get("executed_quantity")
                                or order.get("quantity")
                                or order.get("Fillshares")    # Alice Blue Order Book
                                or order.get("Filledqty")     # Alice Blue Trade Book
                                or order.get("fillshares")    # Finvasia
                                or 0
                            )

                            # Extract and validate order status
                            order_status_raw = (
                                order.get("orderStatus")
                                or order.get("status")
                                or order.get("Status")    # Alice Blue
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

                            # Handle AliceBlue special case for filled quantity
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

                            # Only process completed orders
                            if status != "COMPLETE":
                                continue

                        except Exception as e:
                            logger.debug(f"Failed to extract order data: {e}")
                            continue

                        # Extract price and transaction details
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

                            # Extract transaction type
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

                        # Extract trading symbol
                        symbol = (
                            order.get("tradingSymbol")
                            or order.get("symbol")
                            or order.get("stock")
                            or order.get("scripCode")
                            or order.get("instrumentToken")
                            or order.get("Tsym")  # AliceBlue trade book
                            or order.get("tsym")
                        )
                        if not symbol:
                            continue

                        # Calculate quantity with multiplier
                        try:
                            multiplier = float(child.multiplier or 1)
                            if multiplier <= 0:
                                continue
                            copied_qty = max(1, int(float(filled_qty) * multiplier))
                        except Exception as e:
                            logger.debug(f"Failed to calculate quantity: {e}")
                            continue

                        # Initialize child broker API
                        try:
                            child_broker = (child.broker or "Unknown").lower()
                            
                            child_api = broker_api(_account_to_dict(child))
                            
                        except Exception as e:
                            logger.error(f"Failed to initialize child API for {child_id}: {e}")
                            continue

                        # Get symbol mapping for child broker
                        try:
                            mapping_child = get_symbol_for_broker(symbol, child_broker)
                            if not mapping_child:
                                logger.warning(f"Symbol mapping not found for {symbol} on {child_broker}")
                                continue
                        except Exception as e:
                            logger.warning(f"Symbol mapping error for {symbol}: {e}")
                            continue

                        # Build order parameters based on broker type
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

                            # Place the order
                            logger.info(f"Placing {transaction_type} order for {copied_qty} {symbol} on {child_broker} for child {child_id}")
                            response = child_api.place_order(**order_params)

                        except Exception as e:
                            logger.error(f"Failed to place order for child {child_id}: {e}")
                            continue

                        # Handle order response and record trade
                        try:
                            if isinstance(response, dict):
                                if response.get("status") == "failure":
                                    error_msg = (
                                        response.get("error")
                                        or response.get("remarks")
                                        or response.get("message")
                                        or "Unknown error"
                                    )
                                    logger.warning(f"Order failed for child {child_id}: {error_msg}")
                                    
                                    # Log the failure
                                    save_log(
                                        child_id,
                                        symbol,
                                        transaction_type,
                                        copied_qty,
                                        "FAILED",
                                        error_msg
                                    )
                                    record_trade(
                                        child.user_id,
                                        symbol,
                                        transaction_type,
                                        copied_qty,
                                        price,
                                        'FAILED'
                                    )
                                else:
                                    # Extract child order ID
                                    order_id_child = (
                                        response.get("order_id")
                                        or response.get("orderId")
                                        or response.get("id")
                                        or response.get("nestOrderNumber")
                                        or response.get("orderNumber")
                                        or response.get("norenordno")
                                    )
                                    
                                    logger.info(f"Order successful for child {child_id}: {order_id_child}")
                                    
                                    # Log the success
                                    save_log(
                                        child_id,
                                        symbol,
                                        transaction_type,
                                        copied_qty,
                                        "SUCCESS",
                                        str(response)
                                    )
                                    
                                    # Save order mapping
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
                                    
                                    # Record successful trade
                                    record_trade(
                                        child.user_id,
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

                    # Update the last copied trade marker in database
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
            logger.error(f"Error in poll_and_copy_trades: {e}")
            db.session.rollback()


# ==============================================================================
# Main Application Routes (main_bp)
# ==============================================================================
@main_bp.route("/")
def home():
    return render_template("index.html")


@main_bp.route("/login", methods=["GET", "POST"])
def login():
    """User login page."""
    if session.get("user_id"):
        return redirect(url_for("main.summary"))
    
    if request.method == "POST":
        username = request.form.get("email") or request.form.get("username")
        password = request.form.get("password")
        
        user = User.query.filter_by(email=username).first()
        
        if user and user.check_password(password):
            session["user_id"] = user.id
            session["user_email"] = user.email
            
            user.last_login = datetime.now()
            db.session.commit()
            
            flash("Login successful!", "success")
            return redirect(url_for("main.summary"))
            
        # Check for admin login
        if username == Config.ADMIN_EMAIL and password == Config.ADMIN_PASSWORD:
            session["admin"] = True
            session["admin_email"] = Config.ADMIN_EMAIL
            flash("Admin login successful!", "success")
            return redirect(url_for("admin.dashboard"))
            
        flash("Invalid credentials", "error")
        
    return render_template("log-in.html")


@main_bp.route("/signup", methods=["GET", "POST"])
def signup():
    """User registration page."""
    if session.get("user_id"):
        return redirect(url_for("main.summary"))
        
    if request.method == "POST":
        username = request.form.get("email") or request.form.get("username")
        password = request.form.get("password")
        
        if User.query.filter_by(email=username).first():
            flash("User already exists", "error")
            return render_template("sign-up.html", error="User already exists")
            
        user = User(email=username)
        user.set_password(password)
        user.subscription_start = datetime.now()
        db.session.add(user)
        db.session.commit()
        
        session["user_id"] = user.id
        session["user_email"] = user.email
        
        flash("Account created successfully!", "success")
        return redirect(url_for("main.summary"))
        
    return render_template("sign-up.html")


@main_bp.route("/logout")
def logout():
    """Log out the current user."""
    session.clear()
    flash("You have been logged out", "info")
    return redirect(url_for("main.home"))


@main_bp.route("/Summary")
@login_required
def summary():
    """User dashboard summary page."""
    return render_template("Summary.html")


@main_bp.route("/copy-trading")
@login_required
def copytrading():
    """Copy trading configuration page."""
    return render_template("copy-trading.html")


@main_bp.route("/Add-Account")
@login_required
def add_account():
    """Add new broker account page."""
    return render_template("Add-Account.html")


@main_bp.route("/groups")
@login_required
def groups_page():
    """Account groups management page."""
    return render_template("groups.html")


@main_bp.route("/marketwatch")
@login_required
def market_watch():
    """Market watchlist page."""
    return render_template("marketwatch.html")


@main_bp.route("/users", methods=["GET", "POST"])
@login_required
def user_profile():
    """User profile page."""
    user_id = session.get("user_id")
    user = User.query.get_or_404(user_id)
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
                filename = secure_filename(str(user_id) + "_" + file.filename)
                file.save(os.path.join(image_dir, filename))
                user.profile_image = os.path.join("profile_images", filename)
            
            message = "Profile updated"
            db.session.commit()

    profile_data = {
        "email": user.email,
        "first_name": (user.name or "").split(" ")[0] if user.name else "",
        "last_name": (user.name or "").split(" ")[1] if user.name and len(user.name.split(" ")) > 1 else "",
        "plan": user.plan,
    }

    return render_template("user.html", user=profile_data, message=message)


# ==============================================================================
# API Routes (api_bp)
# ==============================================================================
@api_bp.route("/accounts")
@login_required
def get_accounts():
    """Get all accounts for the logged-in user."""
    user_id = session.get("user_id")
    
    try:
        # Get accounts for this user
        accounts = Account.query.filter_by(user_id=user_id).all()
        account_dicts = [_account_to_dict(acc) for acc in accounts]

        # Add opening balances
        for acc in account_dicts:
            try:
                bal = get_opening_balance_for_account(acc)
                if bal is not None:
                    acc["opening_balance"] = bal
            except Exception as e:
                logger.error(f"Error fetching balance for {acc['client_id']}: {e}")

        # Group masters with their children
        masters = []
        for acc in accounts:
            if acc.role == "master":
                children = Account.query.filter_by(
                    user_id=user_id,
                    role="child", 
                    linked_master_id=acc.client_id
                ).all()
                
                acc_copy = _account_to_dict(acc)
                acc_copy["children"] = [_account_to_dict(child) for child in children]
                masters.append(acc_copy)

        return jsonify({
            "masters": masters,
            "accounts": account_dicts
        })
    except Exception as e:
        logger.error(f"Error in /api/accounts: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/add-account', methods=['POST'])
@login_required
def add_account_api():
    """Add a new broker account for the current user."""
    user_id = session.get("user_id")
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        broker = data.get('broker', '').lower().strip()
        client_id = data.get('client_id')
        username = data.get('username')
        
        if not all([broker, client_id, username]):
            return jsonify({"error": "Missing broker, client_id or username"}), 400

        # Check for duplicate account
        if Account.query.filter_by(user_id=user_id, client_id=client_id, broker=broker).first():
            return jsonify({"error": "Account already exists"}), 409

        # Extract and validate broker-specific credentials
        credentials = {}
        for key, value in data.items():
            if key not in ('broker', 'client_id', 'username') and value is not None:
                credentials[key] = value

        # Create new account
        new_account = Account(
            user_id=user_id,
            broker=broker,
            client_id=client_id,
            username=username,
            credentials=credentials,
            status='Connected',
            auto_login=True,
            last_login_time=datetime.utcnow()
        )

        db.session.add(new_account)
        db.session.commit()
        
        # Log the addition
        log_entry = SystemLog(
            timestamp=datetime.utcnow(),
            level="INFO",
            message=f"Account added: {client_id} ({broker})",
            user_id=str(user_id),
            details=json.dumps({
                "action": "add_account",
                "client_id": client_id,
                "broker": broker,
                "username": username,
                "timestamp": datetime.utcnow().isoformat()
            })
        )
        db.session.add(log_entry)
        db.session.commit()

        return jsonify({"message": f"✅ Account {username} ({broker}) added successfully"}), 201

    except Exception as e:
        logger.error(f"Error adding account: {str(e)}")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@api_bp.route('/check-credentials', methods=['POST'])
@login_required
def check_credentials():
    """Validate broker credentials without saving them."""
    try:
        data = request.json
        broker = data.get('broker')
        client_id = data.get('client_id')

        if not broker or not client_id:
            return jsonify({'error': 'Missing broker or client_id'}), 400

        credentials = {k: v for k, v in data.items() if k not in ('broker', 'client_id')}
        
        # Create test account object
        test_account = {
            "broker": broker,
            "client_id": client_id,
            "credentials": credentials
        }
        
        # Try to initialize broker
        broker_obj = broker_api(test_account)
        
        # Verify credentials
        if hasattr(broker_obj, 'check_token_valid'):
            valid = broker_obj.check_token_valid()
            if not valid:
                error_message = "Invalid credentials"
                if hasattr(broker_obj, 'last_auth_error'):
                    error_message = broker_obj.last_auth_error() or error_message
                return jsonify({'error': error_message}), 400
        
        return jsonify({'valid': True})

    except Exception as e:
        return jsonify({'error': f'Credential validation failed: {str(e)}'}), 400


@api_bp.route('/set-master', methods=['POST'])
@login_required
def set_master():
    """Set an account as a master account."""
    user_id = session.get("user_id")
    
    try:
        data = request.json
        client_id = data.get('client_id')
        
        if not client_id:
            return jsonify({"error": "Missing client_id"}), 400

        account = Account.query.filter_by(user_id=user_id, client_id=client_id).first()
        if not account:
            return jsonify({"error": "Account not found"}), 404

        account.role = "master"
        account.linked_master_id = None
        account.copy_status = "Off"
        account.multiplier = 1.0
        
        db.session.commit()
        
        log_entry = SystemLog(
            timestamp=datetime.utcnow(),
            level="INFO",
            message=f"Account {client_id} set as master",
            user_id=str(user_id)
        )
        db.session.add(log_entry)
        db.session.commit()

        return jsonify({"message": "Set as master successfully"})
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error setting master: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/set-child', methods=['POST'])
@login_required
def set_child():
    """Set an account as a child account."""
    user_id = session.get("user_id")
    
    try:
        data = request.json
        client_id = data.get('client_id')
        linked_master_id = data.get('linked_master_id')
        
        if not client_id or not linked_master_id:
            return jsonify({"error": "Missing client_id or linked_master_id"}), 400

        # Verify both accounts belong to user
        child_account = Account.query.filter_by(user_id=user_id, client_id=client_id).first()
        master_account = Account.query.filter_by(user_id=user_id, client_id=linked_master_id, role="master").first()
        
        if not child_account:
            return jsonify({"error": "Child account not found"}), 404
            
        if not master_account:
            return jsonify({"error": "Master account not found or not configured as master"}), 404
        
        child_account.role = "child"
        child_account.linked_master_id = linked_master_id
        child_account.copy_status = "Off"
        child_account.multiplier = 1.0
        
        db.session.commit()
        
        log_entry = SystemLog(
            timestamp=datetime.utcnow(),
            level="INFO",
            message=f"Account {client_id} set as child to master {linked_master_id}",
            user_id=str(user_id)
        )
        db.session.add(log_entry)
        db.session.commit()

        return jsonify({"message": "Set as child successfully"})
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error setting child: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/start-copy', methods=['POST'])
@login_required
def start_copy():
    """Start copying for a child account."""
    user_id = session.get("user_id")
    
    try:
        data = request.json
        client_id = data.get("client_id")
        master_id = data.get("master_id")
        
        if not client_id or not master_id:
            return jsonify({"error": "Missing client_id or master_id"}), 400

        # Verify both accounts belong to user
        child_account = Account.query.filter_by(user_id=user_id, client_id=client_id).first()
        master_account = Account.query.filter_by(user_id=user_id, client_id=master_id, role="master").first()
        
        if not child_account:
            return jsonify({"error": "Child account not found"}), 404
            
        if not master_account:
            return jsonify({"error": "Master account not found or not configured as master"}), 404

        # Update child account configuration
        child_account.role = "child"
        child_account.linked_master_id = master_id
        child_account.copy_status = "On"
        
        # Get latest order ID from master to set initial marker
        latest_order_id = "NONE"
        
        try:
            # Convert master account to dict for broker_api compatibility
            master_dict = _account_to_dict(master_account)
            master_api = broker_api(master_dict)
            
            # Get orders based on broker type
            broker_name = master_account.broker.lower() if master_account.broker else "unknown"
            
            if broker_name == "aliceblue" and hasattr(master_api, "get_trade_book"):
                logger.debug("Using AliceBlue trade book for marker")
                orders_resp = master_api.get_trade_book()
                order_list = parse_order_list(orders_resp)
                
                # Fallback to order list if trade book is empty
                if not order_list and hasattr(master_api, "get_order_list"):
                    logger.debug("Trade book empty, falling back to order list")
                    orders_resp = master_api.get_order_list()
                    order_list = parse_order_list(orders_resp)
            else:
                logger.debug("Using standard order list for marker")
                orders_resp = master_api.get_order_list()
                order_list = parse_order_list(orders_resp)
            
            # Clean and process orders
            order_list = strip_emojis_from_obj(order_list or [])
            
            if order_list and isinstance(order_list, list):
                # Sort orders to get the latest one
                try:
                    order_list = sorted(order_list, key=get_order_sort_key, reverse=True)
                    
                    if order_list:
                        latest_order = order_list[0]
                        
                        # Extract order ID with multiple fallbacks
                        latest_order_id = (
                            latest_order.get("orderId")
                            or latest_order.get("order_id")
                            or latest_order.get("id")
                            or latest_order.get("NOrdNo")
                            or latest_order.get("Nstordno")
                            or latest_order.get("nestOrderNumber")
                            or latest_order.get("orderNumber")
                            or latest_order.get("norenordno")  # Finvasia
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

        # Set the marker to prevent copying historical orders
        child_account.last_copied_trade_id = latest_order_id
        
        db.session.commit()
        
        log_entry = SystemLog(
            timestamp=datetime.utcnow(),
            level="INFO",
            message=f"Started copying for {client_id} from master {master_id}",
            user_id=str(user_id),
            details=json.dumps({
                "action": "start_copy",
                "child_id": client_id,
                "master_id": master_id,
                "initial_marker": latest_order_id,
                "timestamp": datetime.utcnow().isoformat()
            })
        )
        db.session.add(log_entry)
        db.session.commit()

        return jsonify({
            'message': f"✅ Started copying for {client_id} under master {master_id}",
            'details': {
                'child_account': client_id,
                'master_account': master_id,
                'copy_status': 'On',
                'initial_marker': latest_order_id,
                'broker': master_account.broker
            }
        })
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error starting copy: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/stop-copy', methods=['POST'])
@login_required
def stop_copy():
    """Stop copying for a child account."""
    user_id = session.get("user_id")
    
    try:
        data = request.json
        client_id = data.get("client_id")
        
        if not client_id:
            return jsonify({"error": "Missing client_id"}), 400

        # Verify account belongs to user
        child_account = Account.query.filter_by(user_id=user_id, client_id=client_id, role="child").first()
        
        if not child_account:
            return jsonify({"error": "Child account not found or not configured as child"}), 404

        # Store previous state for logging
        previous_state = {
            "copy_status": child_account.copy_status,
            "linked_master_id": child_account.linked_master_id
        }
        
        # Update child account - stop copying but keep role and relationship
        child_account.copy_status = "Off"
        
        db.session.commit()
        
        log_entry = SystemLog(
            timestamp=datetime.utcnow(),
            level="INFO",
            message=f"Stopped copying for {client_id}",
            user_id=str(user_id),
            details=json.dumps({
                "action": "stop_copy",
                "child_id": client_id,
                "master_id": child_account.linked_master_id,
                "previous_state": previous_state,
                "timestamp": datetime.utcnow().isoformat()
            })
        )
        db.session.add(log_entry)
        db.session.commit()

        return jsonify({
            'message': f"🛑 Stopped copying for {client_id}",
            'details': {
                'child_account': client_id,
                'master_account': child_account.linked_master_id,
                'copy_status': 'Off',
                'role': 'child',
                'stopped_at': datetime.utcnow().isoformat()
            }
        })
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error stopping copy: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/start-copy-all', methods=['POST'])
@login_required
def start_copy_all():
    """Start copying for all children of a master."""
    user_id = session.get("user_id")
    
    try:
        data = request.json
        master_id = data.get("master_id")
        
        if not master_id:
            return jsonify({"error": "Missing master_id"}), 400

        # Verify master account belongs to user
        master_account = Account.query.filter_by(user_id=user_id, client_id=master_id, role="master").first()
        
        if not master_account:
            return jsonify({"error": "Master account not found or not configured as master"}), 404

        # Find all child accounts linked to this master
        linked_children = Account.query.filter_by(
            user_id=user_id,
            role='child',
            linked_master_id=master_id
        ).all()

        # Filter only children that are currently stopped
        stopped_children = [child for child in linked_children if child.copy_status == "Off"]

        if not stopped_children:
            all_children_copying = all(child.copy_status == "On" for child in linked_children)
            
            if all_children_copying and linked_children:
                return jsonify({
                    "message": f"All child accounts are already copying for master {master_id}",
                    "total_children": len(linked_children),
                    "already_copying": len(linked_children),
                    "started_count": 0
                }), 200
            else:
                return jsonify({
                    "message": f"No stopped child accounts found for master {master_id}",
                    "total_children": len(linked_children),
                    "started_count": 0
                }), 200

        # Get latest order from master for initial marker
        master_latest_order_id = "NONE"
        
        try:
            master_dict = _account_to_dict(master_account)
            master_api = broker_api(master_dict)
            
            # Get orders based on broker type
            broker_name = master_account.broker.lower()
            
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
                            latest_order.get("orderId")
                            or latest_order.get("order_id")
                            or latest_order.get("id")
                            or latest_order.get("NOrdNo")
                            or latest_order.get("nestOrderNumber")
                            or latest_order.get("orderNumber")
                            or latest_order.get("norenordno")
                            or "NONE"
                        )
                        
                        master_latest_order_id = str(master_latest_order_id) if master_latest_order_id else "NONE"
                        
                except Exception as e:
                    logger.error(f"Failed to sort master orders for bulk marker: {str(e)}")
            
        except Exception as e:
            logger.error(f"Could not fetch latest order for bulk marker from master {master_id}: {str(e)}")

        # Process each stopped child account
        results = []
        started_count = 0
        failed_count = 0

        for child in stopped_children:
            try:
                # Start copying for this child
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
                logger.error(f"Failed to start copying for child {child.client_id}: {str(e)}")
                results.append({
                    "client_id": child.client_id,
                    "status": "ERROR",
                    "message": f"Failed to start: {str(e)}"
                })
                failed_count += 1

        db.session.commit()

        log_entry = SystemLog(
            timestamp=datetime.utcnow(),
            level="INFO",
            message=f"Bulk start copy: {started_count} children started for master {master_id}",
            user_id=str(user_id),
            details=json.dumps({
                "action": "start_copy_all",
                "master_id": master_id,
                "started_count": started_count,
                "failed_count": failed_count,
                "master_marker": master_latest_order_id,
                "timestamp": datetime.utcnow().isoformat(),
                "children_affected": [r["client_id"] for r in results if r["status"] == "SUCCESS"]
            })
        )
        db.session.add(log_entry)
        db.session.commit()

        response_data = {
            "message": f"Bulk start completed for master {master_id}",
            "master_id": master_id,
            "summary": {
                "total_children": len(linked_children),
                "eligible_to_start": len(stopped_children),
                "started_successfully": started_count,
                "failed": failed_count,
                "already_copying": len(linked_children) - len(stopped_children)
            },
            "details": results
        }

        if failed_count == 0:
            return jsonify(response_data), 200
        elif started_count > 0:
            response_data["warning"] = f"{failed_count} accounts failed to start"
            return jsonify(response_data), 207  # Multi-Status
        else:
            response_data["error"] = "Failed to start any child accounts"
            return jsonify(response_data), 500
            
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error in start_copy_all: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/stop-copy-all', methods=['POST'])
@login_required
def stop_copy_all():
    """Stop copying for all children of a master."""
    user_id = session.get("user_id")
    
    try:
        data = request.json
        master_id = data.get("master_id")
        
        if not master_id:
            return jsonify({"error": "Missing master_id"}), 400

        # Verify master account belongs to user
        master_account = Account.query.filter_by(user_id=user_id, client_id=master_id, role="master").first()
        
        if not master_account:
            return jsonify({"error": "Master account not found or not configured as master"}), 404

        # Find all active child accounts linked to this master
        active_children = Account.query.filter_by(
            user_id=user_id,
            role='child',
            linked_master_id=master_id,
            copy_status='On'
        ).all()

        if not active_children:
            return jsonify({
                "message": f"No active child accounts found for master {master_id}",
                "master_id": master_id,
                "stopped_count": 0
            }), 200

        # Process each active child account
        results = []
        stopped_count = 0
        failed_count = 0

        for child in active_children:
            try:
                # Stop copying for this child
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
                logger.error(f"Failed to stop copying for child {child.client_id}: {str(e)}")
                results.append({
                    "client_id": child.client_id,
                    "status": "ERROR",
                    "message": f"Failed to stop: {str(e)}"
                })
                failed_count += 1

        db.session.commit()

        log_entry = SystemLog(
            timestamp=datetime.utcnow(),
            level="INFO",
            message=f"Bulk stop copy: {stopped_count} children stopped for master {master_id}",
            user_id=str(user_id),
            details=json.dumps({
                "action": "stop_copy_all",
                "master_id": master_id,
                "stopped_count": stopped_count,
                "failed_count": failed_count,
                "timestamp": datetime.utcnow().isoformat(),
                "children_affected": [r["client_id"] for r in results if r["status"] == "SUCCESS"]
            })
        )
        db.session.add(log_entry)
        db.session.commit()

        response_data = {
            "message": f"Bulk stop completed for master {master_id}",
            "master_id": master_id,
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
            response_data["warning"] = f"{failed_count} accounts failed to stop"
            return jsonify(response_data), 207  # Multi-Status
        else:
            response_data["error"] = "Failed to stop any child accounts"
            return jsonify(response_data), 500
            
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error in stop_copy_all: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/update-multiplier', methods=['POST'])
@login_required
def update_multiplier():
    """Update multiplier for an account."""
    user_id = session.get("user_id")
    
    try:
        data = request.json
        client_id = data.get("client_id")
        new_multiplier = data.get("multiplier")
        
        if not client_id or new_multiplier is None:
            return jsonify({"error": "Missing client_id or multiplier"}), 400

        # Validate multiplier value
        try:
            new_multiplier = float(new_multiplier)
            if new_multiplier < 0.1:
                return jsonify({"error": "Multiplier must be at least 0.1"}), 400
            if new_multiplier > 100.0:
                return jsonify({"error": "Multiplier cannot exceed 100.0"}), 400
        except (ValueError, TypeError):
            return jsonify({"error": "Invalid multiplier format - must be a number"}), 400

        # Verify account belongs to user
        account = Account.query.filter_by(user_id=user_id, client_id=client_id).first()
        
        if not account:
            return jsonify({"error": "Account not found"}), 404

        # Store previous state for logging
        previous_multiplier = account.multiplier

        # Check if multiplier is actually changing
        if abs(float(previous_multiplier or 1.0) - new_multiplier) < 0.001:
            return jsonify({
                "message": f"Multiplier for {client_id} is already set to {new_multiplier}",
                "no_change_needed": True,
                "current_multiplier": float(account.multiplier or 1.0)
            }), 200

        # Update multiplier
        account.multiplier = new_multiplier
        db.session.commit()

        log_entry = SystemLog(
            timestamp=datetime.utcnow(),
            level="INFO",
            message=f"Multiplier updated for {client_id}: {previous_multiplier} -> {new_multiplier}",
            user_id=str(user_id),
            details=json.dumps({
                "action": "update_multiplier",
                "client_id": client_id,
                "old_multiplier": previous_multiplier,
                "new_multiplier": new_multiplier,
                "account_role": account.role,
                "copy_status": account.copy_status,
                "timestamp": datetime.utcnow().isoformat()
            })
        )
        db.session.add(log_entry)
        db.session.commit()

        return jsonify({
            "message": f"Multiplier updated to {new_multiplier} for {client_id}",
            "account_details": {
                "client_id": client_id,
                "broker": account.broker,
                "username": account.username,
                "role": account.role,
                "copy_status": account.copy_status
            },
            "multiplier_update": {
                "previous_multiplier": float(previous_multiplier or 1.0),
                "new_multiplier": new_multiplier
            }
        })
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error updating multiplier: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/delete-account', methods=['POST'])
@login_required
def delete_account():
    """Delete an account."""
    user_id = session.get("user_id")
    
    try:
        data = request.json
        client_id = data.get("client_id")
        
        if not client_id:
            return jsonify({"error": "Missing client_id"}), 400

        # Verify account belongs to user
        account = Account.query.filter_by(user_id=user_id, client_id=client_id).first()
        
        if not account:
            return jsonify({"error": "Account not found"}), 404

        # Store details for logging
        account_details = {
            "client_id": account.client_id,
            "broker": account.broker,
            "username": account.username,
            "role": account.role
        }

        # Check for dependent relationships
        if account.role == 'master':
            child_count = Account.query.filter_by(linked_master_id=client_id).count()
            if child_count > 0:
                return jsonify({
                    "error": f"Cannot delete master account with {child_count} linked children. Please unlink or delete the child accounts first."
                }), 400

        # Delete the account
        db.session.delete(account)
        db.session.commit()

        log_entry = SystemLog(
            timestamp=datetime.utcnow(),
            level="INFO",
            message=f"Account deleted: {client_id}",
            user_id=str(user_id),
            details=json.dumps({
                "action": "delete_account",
                "account": account_details,
                "timestamp": datetime.utcnow().isoformat()
            })
        )
        db.session.add(log_entry)
        db.session.commit()

        return jsonify({"message": f"Account {client_id} deleted successfully"})
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error deleting account: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/groups', methods=['GET'])
@login_required
def get_groups():
    """Return all account groups for the logged-in user."""
    user_id = session.get("user_id")
    
    try:
        groups = Group.query.filter_by(user_id=user_id).all()
        return jsonify([_group_to_dict(g) for g in groups])
    except Exception as e:
        logger.error(f"Error fetching groups: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/create-group', methods=['POST'])
@login_required
def create_group():
    """Create a new account group."""
    user_id = session.get("user_id")
    
    try:
        data = request.json
        name = data.get("name")
        members = data.get("members", [])
        description = data.get("description", "")
        
        if not name:
            return jsonify({"error": "Missing group name"}), 400

        # Check if group already exists
        if Group.query.filter_by(user_id=user_id, name=name).first():
            return jsonify({"error": "Group already exists"}), 409

        # Create new group
        group = Group(name=name, user_id=user_id, description=description)
        
        # Add member accounts
        for cid in members:
            acc = Account.query.filter_by(user_id=user_id, client_id=cid).first()
            if acc:
                group.accounts.append(acc)
                
        db.session.add(group)
        db.session.commit()

        return jsonify({"message": f"Group '{name}' created successfully"})
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error creating group: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/groups/<group_name>/add', methods=['POST'])
@login_required
def add_account_to_group(group_name):
    """Add an account to an existing group."""
    user_id = session.get("user_id")
    
    try:
        data = request.json
        client_id = data.get("client_id")
        
        if not client_id:
            return jsonify({"error": "Missing client_id"}), 400

        # Verify group and account belong to user
        group = Group.query.filter_by(user_id=user_id, name=group_name).first()
        if not group:
            return jsonify({"error": "Group not found"}), 404
            
        account = Account.query.filter_by(user_id=user_id, client_id=client_id).first()
        if not account:
            return jsonify({"error": "Account not found"}), 404

        # Check if account already in group
        if account in group.accounts:
            return jsonify({"message": "Account already in group"})

        # Add account to group
        group.accounts.append(account)
        db.session.commit()

        return jsonify({"message": f"Added {client_id} to {group_name}"})
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error adding account to group: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/groups/<group_name>/remove', methods=['POST'])
@login_required
def remove_account_from_group(group_name):
    """Remove an account from a group."""
    user_id = session.get("user_id")
    
    try:
        data = request.json
        client_id = data.get("client_id")
        
        if not client_id:
            return jsonify({"error": "Missing client_id"}), 400

        # Verify group and account belong to user
        group = Group.query.filter_by(user_id=user_id, name=group_name).first()
        if not group:
            return jsonify({"error": "Group not found"}), 404
            
        account = Account.query.filter_by(user_id=user_id, client_id=client_id).first()
        if not account or account not in group.accounts:
            return jsonify({"error": "Account not in group"}), 404

        # Remove account from group
        group.accounts.remove(account)
        db.session.commit()

        return jsonify({"message": f"Removed {client_id} from {group_name}"})
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error removing account from group: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/group-order', methods=['POST'])
@login_required
def place_group_order():
    """Place the same order across all accounts in a group."""
    user_id = session.get("user_id")
    
    try:
        data = request.json
        group_name = data.get("group_name")
        symbol = data.get("symbol")
        action = data.get("action")
        quantity = data.get("quantity")

        if not all([group_name, symbol, action, quantity]):
            return jsonify({"error": "Missing required fields"}), 400

        # Verify group belongs to user
        group = Group.query.filter_by(user_id=user_id, name=group_name).first()
        if not group:
            return jsonify({"error": "Group not found"}), 404

        # Get accounts in the group
        accounts = group.accounts.all()
        if not accounts:
            return jsonify({"error": "No accounts in this group"}), 400

        # Place orders for each account
        results = []
        for acc in accounts:
            try:
                acc_dict = _account_to_dict(acc)
                api = broker_api(acc_dict)
                broker_name = acc.broker.lower()
                
                # Get symbol mapping
                mapping = get_symbol_for_broker(symbol, broker_name)
                
                # Build order parameters based on broker
                if broker_name == "dhan":
                    security_id = mapping.get("security_id")
                    if not security_id:
                        results.append({
                            "client_id": acc.client_id,
                            "status": "FAILED",
                            "reason": f"Symbol mapping not found for {symbol}"
                        })
                        continue
                        
                    order_params = {
                        "tradingsymbol": symbol,
                        "security_id": security_id,
                        "exchange_segment": getattr(api, "NSE", "NSE_EQ"),
                        "transaction_type": action.upper(),
                        "quantity": int(quantity),
                        "order_type": "MARKET",
                        "product_type": getattr(api, "INTRA", "INTRADAY"),
                        "price": 0
                    }
                elif broker_name == "aliceblue":
                    symbol_id = mapping.get("symbol_id")
                    if not symbol_id:
                        results.append({
                            "client_id": acc.client_id,
                            "status": "FAILED",
                            "reason": f"Symbol mapping not found for {symbol}"
                        })
                        continue
                        
                    order_params = {
                        "tradingsymbol": mapping.get("tradingsymbol", symbol),
                        "symbol_id": symbol_id,
                        "exchange": "NSE",
                        "transaction_type": action.upper(),
                        "quantity": int(quantity),
                        "order_type": "MKT",
                        "product": "MIS",
                        "price": 0
                    }
                elif broker_name == "finvasia":
                    token = mapping.get("token")
                    if not token:
                        results.append({
                            "client_id": acc.client_id,
                            "status": "FAILED",
                            "reason": f"Symbol mapping not found for {symbol}"
                        })
                        continue
                        
                    order_params = {
                        "tradingsymbol": mapping.get("symbol", symbol),
                        "exchange": mapping.get("exchange", "NSE"),
                        "transaction_type": action.upper(),
                        "quantity": int(quantity),
                        "order_type": "MKT",
                        "product": "MIS",
                        "price": 0,
                        "token": token
                    }
                else:
                    order_params = {
                        "tradingsymbol": mapping.get("tradingsymbol", symbol),
                        "exchange": "NSE",
                        "transaction_type": action.upper(),
                        "quantity": int(quantity),
                        "order_type": "MARKET",
                        "product": "MIS",
                        "price": 0
                    }

                # Place the order
                response = api.place_order(**order_params)
                
                # Handle response
                if isinstance(response, dict) and response.get("status") == "failure":
                    status = "FAILED"
                    reason = clean_response_message(response)
                    results.append({
                        "client_id": acc.client_id,
                        "status": status,
                        "reason": reason
                    })
                    record_trade(user_id, symbol, action.upper(), quantity, 0, status)
                else:
                    status = "SUCCESS"
                    order_id = (
                        response.get("order_id")
                        or response.get("orderId")
                        or response.get("id")
                        or "Unknown"
                    )
                    results.append({
                        "client_id": acc.client_id,
                        "status": status,
                        "order_id": order_id
                    })
                    record_trade(user_id, symbol, action.upper(), quantity, 0, status)
                    
            except Exception as e:
                logger.error(f"Error placing order for {acc.client_id}: {str(e)}")
                results.append({
                    "client_id": acc.client_id,
                    "status": "ERROR",
                    "reason": str(e)
                })

        # Log the group order
        log_entry = SystemLog(
            timestamp=datetime.utcnow(),
            level="INFO",
            message=f"Group order placed: {action} {quantity} {symbol} for group {group_name}",
            user_id=str(user_id),
            details=json.dumps({
                "action": "group_order",
                "group": group_name,
                "symbol": symbol,
                "order_action": action,
                "quantity": quantity,
                "results": results,
                "timestamp": datetime.utcnow().isoformat()
            })
        )
        db.session.add(log_entry)
        db.session.commit()

        return jsonify({
            "message": f"Group order processed for {len(accounts)} accounts",
            "results": results
        })
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error in group order: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/order-book/<client_id>', methods=['GET'])
@login_required
def get_order_book(client_id):
    """Get order book for an account."""
    user_id = session.get("user_id")
    
    try:
        # Verify account belongs to user
        account = Account.query.filter_by(user_id=user_id, client_id=client_id).first()
        if not account:
            return jsonify({"error": "Account not found or access denied"}), 404

        # Initialize broker API
        api = broker_api(_account_to_dict(account))
        
        # Fetch orders based on broker type
        broker_name = account.broker.lower()
        
        if broker_name == "aliceblue" and hasattr(api, "get_trade_book"):
            orders_resp = api.get_trade_book()
        else:
            orders_resp = api.get_order_list()
            
        # Parse and format the orders
        orders = parse_order_list(orders_resp)
        orders = strip_emojis_from_obj(orders)
        
        if not isinstance(orders, list):
            orders = []
        
        # Format orders for frontend
        formatted = []
        for order in orders:
            if not isinstance(order, dict):
                continue
                
            try:
                # Extract order details with fallbacks
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

                side = (
                    order.get("transactionType")
                    or order.get("side")
                    or order.get("Trantype")
                    or "N/A"
                )
                
                status = str(
                    order.get("orderStatus")
                    or order.get("report_type")
                    or order.get("status")
                    or order.get("Status")
                    or ("FILLED" if order.get("tradedQty") else "PENDING")
                ).upper() or "UNKNOWN"
                
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
                logger.error(f"Error formatting order: {str(e)}")
                continue

        # Sort by order time, newest first
        formatted.sort(key=lambda x: x.get('order_time', ''), reverse=True)

        return jsonify(formatted)
        
    except Exception as e:
        logger.error(f"Error fetching order book: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/square-off', methods=['POST'])
@login_required
def square_off():
    """Square off positions for a master or its children."""
    user_id = session.get("user_id")
    
    try:
        data = request.json
        client_id = data.get("client_id")
        symbol = data.get("symbol")
        is_master_squareoff = data.get("is_master", False)

        if not client_id or not symbol:
            return jsonify({"error": "Missing client_id or symbol"}), 400

        # Find the account and verify it belongs to the user
        clicked_account, parent_master = find_account_by_client_id(client_id, user_id)
        if not clicked_account:
            return jsonify({"error": "Account not found or access denied"}), 404

        # Determine the actual master account for the operation
        master_account = parent_master if parent_master else clicked_account
        if master_account.role != 'master':
             return jsonify({"error": f"Account {master_account.client_id} is not a master account."}), 400

        # This block handles squaring off ONLY the master's personal position
        if is_master_squareoff:
            try:
                master_api = broker_api(_account_to_dict(master_account))
                positions_resp = master_api.get_positions()
                
                # Extract positions list with fallbacks
                if isinstance(positions_resp, dict):
                    positions = (
                        positions_resp.get("data")
                        or positions_resp.get("positions")
                        or positions_resp.get("net")
                        or []
                    )
                else:
                    positions = positions_resp or []
                
                # Find matching position
                match = None
                for p in positions:
                    pos_symbol = (
                        p.get("tradingSymbol")
                        or p.get("symbol")
                        or p.get("tsym")
                        or p.get("Tsym")
                        or ""
                    ).upper()
                    
                    if pos_symbol == symbol.upper():
                        match = p
                        break
                
                if not match or int(match.get("netQty", 0)) == 0:
                    return jsonify({"message": f"No active position in {symbol} (already squared off)"}), 200

                # Extract position details
                qty = abs(int(match.get("netQty") or match.get("netQuantity") or 0))
                direction = "SELL" if int(match.get("netQty") or match.get("netQuantity") or 0) > 0 else "BUY"

                # Build order parameters
                broker_name = master_account.broker.lower()
                if broker_name == "dhan":
                    order_params = {
                        "tradingsymbol": symbol,
                        "security_id": match.get("securityId"),
                        "exchange_segment": match.get("exchangeSegment", "NSE_EQ"),
                        "transaction_type": direction,
                        "quantity": qty,
                        "order_type": "MARKET",
                        "product_type": match.get("productType", "INTRADAY"),
                        "price": 0
                    }
                elif broker_name == "aliceblue":
                    order_params = {
                        "tradingsymbol": symbol,
                        "symbol_id": match.get("token") or match.get("symboltoken"),
                        "exchange": "NSE",
                        "transaction_type": direction,
                        "quantity": qty,
                        "order_type": "MKT",
                        "product": "MIS",
                        "price": 0
                    }
                elif broker_name == "finvasia":
                    order_params = {
                        "tradingsymbol": symbol,
                        "exchange": "NSE",
                        "transaction_type": direction,
                        "quantity": qty,
                        "order_type": "MKT",
                        "product": "MIS",
                        "price": 0,
                        "token": match.get("token")
                    }
                else:
                    order_params = {
                        "tradingsymbol": symbol,
                        "exchange": "NSE",
                        "transaction_type": direction,
                        "quantity": qty,
                        "order_type": "MARKET",
                        "product": "MIS",
                        "price": 0
                    }
                
                # Place the order
                resp = master_api.place_order(**order_params)
                
                # Log the square-off
                save_log(
                    master_account.client_id,
                    symbol,
                    "SQUARE_OFF",
                    qty,
                    "SUCCESS",
                    str(resp)
                )
                
                return jsonify({
                    "message": "✅ Master square-off placed",
                    "details": clean_response_message(resp)
                })
                
            except Exception as e:
                logger.error(f"Error in master square-off: {str(e)}")
                return jsonify({"error": str(e)}), 500
        
        # This block handles squaring off ALL linked children's positions
        else:
            # Get all active children linked to this master
            children = Account.query.filter_by(
                user_id=user_id,
                role='child',
                linked_master_id=master_account.client_id,
                copy_status='On'
            ).all()

            if not children:
                return jsonify({"message": "No active children found to square off."}), 200

            # Process each child
            results = []
            for child in children:
                try:
                    child_api = broker_api(_account_to_dict(child))
                    positions_resp = child_api.get_positions()
                    
                    # Extract positions list with fallbacks
                    if isinstance(positions_resp, dict):
                        positions = (
                            positions_resp.get("data")
                            or positions_resp.get("positions")
                            or positions_resp.get("net")
                            or []
                        )
                    else:
                        positions = positions_resp or []
                    
                    # Find matching position
                    match = None
                    for p in positions:
                        pos_symbol = (
                            p.get("tradingSymbol")
                            or p.get("symbol")
                            or p.get("tsym")
                            or p.get("Tsym")
                            or ""
                        ).upper()
                        
                        if pos_symbol == symbol.upper():
                            match = p
                            break
                    
                    if not match or int(match.get("netQty", 0) or match.get("netQuantity", 0) or 0) == 0:
                        results.append(f"Child {child.client_id} → Skipped (no active position in {symbol})")
                        continue

                    # Extract position details
                    qty = abs(int(match.get("netQty") or match.get("netQuantity") or 0))
                    direction = "SELL" if int(match.get("netQty") or match.get("netQuantity") or 0) > 0 else "BUY"

                    # Build order parameters
                    broker_name = child.broker.lower()
                    if broker_name == "dhan":
                        order_params = {
                            "tradingsymbol": symbol,
                            "security_id": match.get("securityId"),
                            "exchange_segment": match.get("exchangeSegment", "NSE_EQ"),
                            "transaction_type": direction,
                            "quantity": qty,
                            "order_type": "MARKET",
                            "product_type": match.get("productType", "INTRADAY"),
                            "price": 0
                        }
                    elif broker_name == "aliceblue":
                        order_params = {
                            "tradingsymbol": symbol,
                            "symbol_id": match.get("token") or match.get("symboltoken"),
                            "exchange": "NSE",
                            "transaction_type": direction,
                            "quantity": qty,
                            "order_type": "MKT",
                            "product": "MIS",
                            "price": 0
                        }
                    elif broker_name == "finvasia":
                        order_params = {
                            "tradingsymbol": symbol,
                            "exchange": "NSE",
                            "transaction_type": direction,
                            "quantity": qty,
                            "order_type": "MKT",
                            "product": "MIS",
                            "price": 0,
                            "token": match.get("token")
                        }
                    else:
                        order_params = {
                            "tradingsymbol": symbol,
                            "exchange": "NSE",
                            "transaction_type": direction,
                            "quantity": qty,
                            "order_type": "MARKET",
                            "product": "MIS",
                            "price": 0
                        }
                    
                    # Place the order
                    response = child_api.place_order(**order_params)
                    
                    # Handle response
                    if isinstance(response, dict) and response.get("status") == "failure":
                        msg = clean_response_message(response)
                        results.append(f"Child {child.client_id} → FAILED: {msg}")
                        save_log(child.client_id, symbol, "SQUARE_OFF", qty, "FAILED", msg)
                    else:
                        results.append(f"Child {child.client_id} → SUCCESS")
                        save_log(child.client_id, symbol, "SQUARE_OFF", qty, "SUCCESS", str(response))
                        
                except Exception as e:
                    error_msg = str(e)
                    results.append(f"Child {child.client_id} → ERROR: {error_msg}")
                    save_log(child.client_id, symbol, "SQUARE_OFF", 0, "ERROR", error_msg)

            return jsonify({
                "message": "🔁 Square-off for all children completed",
                "details": results
            })
            
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error in square_off: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/master-squareoff', methods=['POST'])
@login_required
def master_squareoff():
    """Square off child orders for a master order."""
    user_id = session.get("user_id")
    
    try:
        data = request.json
        master_order_id = data.get("master_order_id")
        
        if not master_order_id:
            return jsonify({"error": "Missing master_order_id"}), 400

        # Find active order mappings for this master order
        active_mappings = OrderMapping.query.filter_by(
            master_order_id=master_order_id,
            status="ACTIVE"
        ).all()
        
        if not active_mappings:
            return jsonify({
                "message": "No active child orders found for this master order",
                "master_order_id": master_order_id
            }), 200

        # Group mappings by child account for processing
        child_mappings = {}
        for mapping in active_mappings:
            child_id = mapping.child_client_id
            if child_id not in child_mappings:
                child_mappings[child_id] = []
            child_mappings[child_id].append(mapping)

        # Process each child account
        results = []
        successful = 0
        failed = 0

        for child_id, mappings in child_mappings.items():
            # Find child account and verify it belongs to the user
            child_account = Account.query.filter_by(client_id=child_id).first()
            
            if not child_account or (child_account.user_id != user_id):
                for mapping in mappings:
                    results.append({
                        "child_client_id": child_id,
                        "symbol": mapping.symbol,
                        "status": "ERROR",
                        "message": "Child account not found or access denied"
                    })
                    failed += 1
                continue

            try:
                # Initialize broker API
                child_api = broker_api(_account_to_dict(child_account))
                
                # Get current positions
                positions_resp = child_api.get_positions()
                
                # Extract positions list with fallbacks
                if isinstance(positions_resp, dict):
                    positions = (
                        positions_resp.get("data")
                        or positions_resp.get("positions")
                        or positions_resp.get("net")
                        or []
                    )
                else:
                    positions = positions_resp or []

                # Process each symbol mapping for this child
                for mapping in mappings:
                    symbol = mapping.symbol
                    
                    # Find matching position
                    match = None
                    for p in positions:
                        pos_symbol = (
                            p.get("tradingSymbol")
                            or p.get("symbol")
                            or p.get("tsym")
                            or p.get("Tsym")
                            or ""
                        ).upper()
                        
                        if pos_symbol == symbol.upper():
                            # Check if there's a net position
                            net_qty = int(
                                p.get("netQty")
                                or p.get("net_quantity")
                                or p.get("netQuantity")
                                or p.get("Netqty")
                                or 0
                            )
                            
                            if net_qty != 0:
                                match = p
                                break

                    if not match:
                        results.append({
                            "child_client_id": child_id,
                            "symbol": symbol,
                            "status": "SKIPPED",
                            "message": f"No open position found for {symbol}"
                        })
                        continue

                    # Calculate square-off parameters
                    net_qty = int(
                        match.get("netQty")
                        or match.get("net_quantity")
                        or match.get("netQuantity")
                        or match.get("Netqty")
                        or 0
                    )
                    
                    direction = "SELL" if net_qty > 0 else "BUY"
                    abs_qty = abs(net_qty)
                    
                    # Build broker-specific order parameters
                    broker_name = child_account.broker.lower()
                    
                    if broker_name == "dhan":
                        order_params = {
                            "tradingsymbol": symbol,
                            "security_id": match.get("securityId"),
                            "exchange_segment": match.get("exchangeSegment", "NSE_EQ"),
                            "transaction_type": direction,
                            "quantity": abs_qty,
                            "order_type": "MARKET",
                            "product_type": "INTRADAY",
                            "price": 0
                        }
                    elif broker_name == "aliceblue":
                        order_params = {
                            "tradingsymbol": symbol,
                            "symbol_id": match.get("token") or match.get("symboltoken"),
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
                            "token": match.get("token")
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

                    # Place square-off order
                    try:
                        square_off_response = child_api.place_order(**order_params)
                        
                        # Check for API errors
                        if isinstance(square_off_response, dict) and square_off_response.get("status") == "failure":
                            error_msg = clean_response_message(square_off_response)
                            
                            results.append({
                                "child_client_id": child_id,
                                "symbol": symbol,
                                "status": "FAILED",
                                "message": f"Square-off failed: {error_msg}",
                                "position_qty": net_qty,
                                "square_off_direction": direction
                            })
                            failed += 1
                            
                        else:
                            # Success - update mapping status
                            mapping.status = "SQUARED_OFF"
                            mapping.remarks = f"Squared off on {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}"
                            
                            # Extract order ID from response
                            square_off_order_id = (
                                square_off_response.get("order_id")
                                or square_off_response.get("orderId")
                                or square_off_response.get("id")
                                or "Unknown"
                            )
                            
                            results.append({
                                "child_client_id": child_id,
                                "symbol": symbol,
                                "status": "SUCCESS",
                                "message": "Square-off completed successfully",
                                "position_qty": net_qty,
                                "square_off_direction": direction,
                                "square_off_order_id": square_off_order_id,
                                "square_off_qty": abs_qty
                            })
                            successful += 1
                            
                    except Exception as e:
                        logger.error(f"Error placing square-off order: {str(e)}")
                        results.append({
                            "child_client_id": child_id,
                            "symbol": symbol,
                            "status": "ERROR",
                            "message": f"Order placement failed: {str(e)}",
                            "position_qty": net_qty,
                            "square_off_direction": direction
                        })
                        failed += 1

            except Exception as e:
                logger.error(f"Error processing child {child_id}: {str(e)}")
                for mapping in mappings:
                    results.append({
                        "child_client_id": child_id,
                        "symbol": mapping.symbol,
                        "status": "ERROR",
                        "message": f"Child processing failed: {str(e)}"
                    })
                    failed += 1

        # Commit all mapping status updates
        db.session.commit()

        # Log the bulk square-off action
        log_entry = SystemLog(
            timestamp=datetime.utcnow(),
            level="INFO",
            message=f"Master square-off completed: {master_order_id} - {successful} success, {failed} failed",
            user_id=str(user_id),
            details=json.dumps({
                "action": "master_squareoff",
                "master_order_id": master_order_id,
                "total_mappings": len(active_mappings),
                "successful_squareoffs": successful,
                "failed_squareoffs": failed,
                "children_processed": len(child_mappings),
                "timestamp": datetime.utcnow().isoformat()
            })
        )
        db.session.add(log_entry)
        db.session.commit()

        # Prepare response
        response_data = {
            "message": f"Master square-off completed for order {master_order_id}",
            "master_order_id": master_order_id,
            "summary": {
                "total_mappings": len(active_mappings),
                "children_processed": len(child_mappings),
                "successful_squareoffs": successful,
                "failed_squareoffs": failed
            },
            "results": results
        }

        # Return appropriate status code
        if failed == 0:
            return jsonify(response_data), 200  # Complete success
        elif successful > 0:
            return jsonify(response_data), 207  # Partial success
        else:
            response_data["error"] = "All square-off attempts failed"
            return jsonify(response_data), 500  # Complete failure

    except Exception as e:
        db.session.rollback()
        logger.error(f"Unexpected error in master_squareoff: {str(e)}")
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500


@api_bp.route('/cancel-order', methods=['POST'])
@login_required
def cancel_order():
    """Cancel an order and its children."""
    user_id = session.get("user_id")
    
    try:
        data = request.json
        master_order_id = data.get("master_order_id")
        
        if not master_order_id:
            return jsonify({"error": "Missing master_order_id"}), 400

        # Find all active child orders linked to the master order
        mappings = OrderMapping.query.filter_by(
            master_order_id=master_order_id, 
            status="ACTIVE"
        ).all()

        if not mappings:
            return jsonify({"message": "No active child orders found for this master order."}), 200

        results = []
        
        # EFFICIENCY IMPROVEMENT: Get all client_ids first, then fetch accounts in a single query
        child_ids_to_find = {m.child_client_id for m in mappings}
        
        # Fetch only those specific accounts in a single query, filtered by user_id for security
        accounts = {}
        for acc in Account.query.filter(
                Account.client_id.in_(child_ids_to_find),
                Account.user_id == user_id
            ).all():
            accounts[acc.client_id] = acc

        for mapping in mappings:
            child_id = mapping.child_client_id
            child_order_id = mapping.child_order_id
            
            found_account = accounts.get(child_id)

            if not found_account:
                results.append(f"{child_id} → ❌ Account not found or access denied")
                continue

            try:
                # Use the fetched account object to initialize the broker API
                api = broker_api(_account_to_dict(found_account))
                
                cancel_resp = api.cancel_order(child_order_id)

                if isinstance(cancel_resp, dict) and cancel_resp.get("status") == "failure":
                    # Handle API-level failure
                    error_message = clean_response_message(cancel_resp)
                    results.append(f"{child_id} → ❌ Cancel failed: {error_message}")
                else:
                    # On success, update the mapping's status
                    results.append(f"{child_id} → ✅ Cancelled")
                    mapping.status = "CANCELLED"
                    mapping.remarks = f"Cancelled by user on {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}"

            except Exception as e:
                # Handle exceptions during the API call
                results.append(f"{child_id} → ❌ ERROR: {str(e)}")

        # Commit all status changes to the database at once
        db.session.commit()
        
        # Log the action
        log_entry = SystemLog(
            timestamp=datetime.utcnow(),
            level="INFO",
            message=f"Orders cancelled for master order {master_order_id}",
            user_id=str(user_id),
            details=json.dumps({
                "action": "cancel_order",
                "master_order_id": master_order_id,
                "results": results,
                "timestamp": datetime.utcnow().isoformat()
            })
        )
        db.session.add(log_entry)
        db.session.commit()

        return jsonify({
            "message": "Cancel process completed", 
            "results": results
        }), 200

    except Exception as e:
        # Rollback in case of an unexpected error during the process
        db.session.rollback()
        logger.error(f"Unexpected error in cancel_order: {str(e)}")
        return jsonify({
            "error": "An internal server error occurred.", 
            "details": str(e)
        }), 500


@api_bp.route('/master-orders', methods=['GET'])
@login_required
def get_master_orders():
    """Get all master orders with their child order details."""
    user_id = session.get("user_id")
    
    try:
        # Get query parameters
        master_id_filter = request.args.get("master_id")
        status_filter = request.args.get("status", "").upper()
        
        # Build base query for order mappings
        query = OrderMapping.query
        
        # Apply master_id filter if provided
        if master_id_filter:
            # Verify the master account belongs to the user
            master = Account.query.filter_by(
                user_id=user_id,
                client_id=master_id_filter,
                role="master"
            ).first()
            
            if not master:
                return jsonify({"error": "Master account not found or access denied"}), 404
                
            query = query.filter_by(master_client_id=master_id_filter)
            
        # Apply status filter if provided
        if status_filter:
            query = query.filter_by(status=status_filter)
            
        # Execute query
        mappings = query.all()
        
        # Filter mappings to only include those where either:
        # 1. The master account belongs to the user, or
        # 2. The child account belongs to the user
        filtered_mappings = []
        for m in mappings:
            master_account = Account.query.filter_by(
                user_id=user_id,
                client_id=m.master_client_id
            ).first()
            
            child_account = Account.query.filter_by(
                user_id=user_id,
                client_id=m.child_client_id
            ).first()
            
            if master_account or child_account:
                filtered_mappings.append(m)
        
        # Group mappings by master order
        master_summary = {}
        for entry in filtered_mappings:
            mid = entry.master_order_id
            if mid not in master_summary:
                master_summary[mid] = {
                    "master_order_id": mid,
                    "symbol": entry.symbol,
                    "master_client_id": entry.master_client_id,
                    "master_broker": entry.master_broker or "Unknown",
                    "action": entry.action if hasattr(entry, 'action') else 'UNKNOWN',
                    "quantity": entry.quantity if hasattr(entry, 'quantity') else 0,
                    "status": 'ACTIVE',
                    "total_children": 0,
                    "child_statuses": [],
                    "children": [],
                    "timestamp": entry.timestamp.isoformat() if entry.timestamp else '',
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
                'timestamp': entry.child_timestamp.isoformat() if entry.child_timestamp else '',
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

            # Determine overall status based on child statuses
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
                
        # Convert to list and sort by timestamp (newest first)
        orders = list(master_summary.values())
        orders.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        
        # Calculate overall summary
        overall_summary = {
            'total_orders': len(orders),
            'active_orders': sum(1 for o in orders if o['status'] == 'ACTIVE'),
            'completed_orders': sum(1 for o in orders if o['status'] == 'COMPLETED'),
            'failed_orders': sum(1 for o in orders if o['status'] == 'FAILED'),
            'cancelled_orders': sum(1 for o in orders if o['status'] == 'CANCELLED'),
            'partial_orders': sum(1 for o in orders if o['status'] == 'PARTIAL')
        }
        
        return jsonify({
            'orders': orders,
            'summary': overall_summary
        }), 200

    except Exception as e:
        logger.error(f"Error in get_master_orders: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/order-mappings', methods=['GET'])
@login_required
def get_order_mappings():
    """Get all order mappings for the user."""
    user_id = session.get("user_id")
    
    try:
        # Get all accounts for this user
        user_accounts = Account.query.filter_by(user_id=user_id).all()
        user_client_ids = [acc.client_id for acc in user_accounts]
        
        # Get mappings where either master or child belongs to user
        mappings = OrderMapping.query.filter(
            db.or_(
                OrderMapping.master_client_id.in_(user_client_ids),
                OrderMapping.child_client_id.in_(user_client_ids)
            )
        ).all()
        
        result = [
            {
                "master_order_id": m.master_order_id,
                "child_order_id": m.child_order_id,
                "master_client_id": m.master_client_id,
                "master_broker": m.master_broker,
                "child_client_id": m.child_client_id,
                "child_broker": m.child_broker,
                "symbol": m.symbol,
                "status": m.status,
                "timestamp": m.timestamp.isoformat() if m.timestamp else None,
                "child_timestamp": m.child_timestamp.isoformat() if m.child_timestamp else None,
                "remarks": m.remarks,
                "multiplier": m.multiplier,
            }
            for m in mappings
        ]
        
        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Error in get_order_mappings: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/child-orders', methods=['GET'])
@login_required
def child_orders():
    """Get child orders for a master order."""
    user_id = session.get("user_id")
    
    try:
        master_order_id = request.args.get('master_order_id')
        if not master_order_id:
            return jsonify({"error": "Missing master_order_id parameter"}), 400
            
        # Get all accounts for this user
        user_accounts = Account.query.filter_by(user_id=user_id).all()
        user_client_ids = [acc.client_id for acc in user_accounts]
        
        # Build query
        query = OrderMapping.query.filter_by(master_order_id=master_order_id)
        
        # Filter to ensure user has access to either master or child
        query = query.filter(
            db.or_(
                OrderMapping.master_client_id.in_(user_client_ids),
                OrderMapping.child_client_id.in_(user_client_ids)
            )
        )
        
        # Execute query
        mappings = query.all()
        
        result = [
            {
                "master_order_id": m.master_order_id,
                "child_order_id": m.child_order_id,
                "master_client_id": m.master_client_id,
                "master_broker": m.master_broker,
                "child_client_id": m.child_client_id,
                "child_broker": m.child_broker,
                "symbol": m.symbol,
                "status": m.status,
                "timestamp": m.timestamp.isoformat() if m.timestamp else None,
                "child_timestamp": m.child_timestamp.isoformat() if m.child_timestamp else None,
                "remarks": m.remarks,
                "multiplier": m.multiplier
            }
            for m in mappings
        ]
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error in child_orders: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/change-master', methods=['POST'])
@login_required
def change_master():
    """Change master for a child account."""
    user_id = session.get("user_id")
    
    try:
        data = request.json
        child_id = data.get("child_id")
        new_master_id = data.get("new_master_id")
        
        if not child_id or not new_master_id:
            return jsonify({"error": "Missing child_id or new_master_id"}), 400

        # Verify child account belongs to user
        child_account = Account.query.filter_by(
            user_id=user_id,
            client_id=child_id,
            role="child"
        ).first()
        
        if not child_account:
            return jsonify({"error": "Child account not found or not configured as child"}), 404

        # Verify new master account belongs to user
        new_master_account = Account.query.filter_by(
            user_id=user_id,
            client_id=new_master_id,
            role="master"
        ).first()
        
        if not new_master_account:
            return jsonify({"error": "Master account not found or not configured as master"}), 404

        # Get current master details for comparison
        old_master_id = child_account.linked_master_id
        old_master_account = None
        
        if old_master_id:
            old_master_account = Account.query.filter_by(
                client_id=old_master_id,
                role="master"
            ).first()

        # Check if already linked to the same master
        if old_master_id == new_master_id:
            return jsonify({
                "message": f"Child {child_id} is already linked to master {new_master_id}",
                "no_change_needed": True
            }), 200

        # Store previous state for logging
        previous_state = {
            "linked_master_id": child_account.linked_master_id,
            "copy_status": child_account.copy_status,
            "last_copied_trade_id": child_account.last_copied_trade_id
        }

        # Update child account with new master
        child_account.linked_master_id = new_master_id
        
        # Reset marker to prevent copying historical orders from new master
        child_account.last_copied_trade_id = "NONE"
        
        # If copying was active, temporarily turn it off for safety
        was_copying = child_account.copy_status == "On"
        if was_copying:
            child_account.copy_status = "Off"

        # Get latest order from new master for marker
        new_latest_order_id = "NONE"
        
        try:
            # Initialize new master broker API
            new_master_api = broker_api(_account_to_dict(new_master_account))
            
            # Get orders based on broker type
            broker_name = new_master_account.broker.lower()
            
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
                            latest_order.get("orderId")
                            or latest_order.get("order_id")
                            or latest_order.get("id")
                            or latest_order.get("NOrdNo")
                            or latest_order.get("nestOrderNumber")
                            or latest_order.get("norenordno")
                            or "NONE"
                        )
                        
                        new_latest_order_id = str(new_latest_order_id) if new_latest_order_id else "NONE"
                        
                except Exception as e:
                    logger.error(f"Failed to sort orders: {str(e)}")
            
        except Exception as e:
            logger.warning(f"Could not fetch latest order from new master: {str(e)}")

        # Set the new marker
        child_account.last_copied_trade_id = new_latest_order_id

        # Commit changes
        db.session.commit()

        # Log the action
        log_entry = SystemLog(
            timestamp=datetime.utcnow(),
            level="INFO",
            message=f"Master changed for {child_id}: {old_master_id} -> {new_master_id}",
            user_id=str(user_id),
            details=json.dumps({
                "action": "change_master",
                "child_id": child_id,
                "old_master_id": old_master_id,
                "new_master_id": new_master_id,
                "was_copying": was_copying,
                "new_marker": new_latest_order_id,
                "previous_state": previous_state
            })
        )
        db.session.add(log_entry)
        db.session.commit()

        # Prepare response
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
            } if old_master_account else None,
            "new_master": {
                "client_id": new_master_id,
                "broker": new_master_account.broker,
                "username": new_master_account.username
            },
            "change_details": {
                "was_copying": was_copying,
                "copy_temporarily_disabled": was_copying,
                "new_marker_set": new_latest_order_id
            }
        }

        # Add restart instruction if copying was active
        if was_copying:
            response_data["next_action"] = "Please restart copying to begin following the new master"

        return jsonify(response_data), 200

    except Exception as e:
        db.session.rollback()
        logger.error(f"Error in change_master: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/remove-child', methods=['POST'])
@login_required
def remove_child():
    """Remove child role from account."""
    user_id = session.get("user_id")
    
    try:
        data = request.json
        client_id = data.get("client_id")
        
        if not client_id:
            return jsonify({"error": "Missing client_id"}), 400

        # Verify account belongs to user
        child_account = Account.query.filter_by(
            user_id=user_id,
            client_id=client_id,
            role='child'
        ).first()
        
        if not child_account:
            return jsonify({"error": "Child account not found or not configured as child"}), 404

        master_id = child_account.linked_master_id
        master_account = None
        
        if master_id:
            master_account = Account.query.filter_by(client_id=master_id).first()

        # Store previous state for logging
        previous_state = {
            "role": child_account.role,
            "linked_master_id": child_account.linked_master_id,
            "copy_status": child_account.copy_status,
            "multiplier": child_account.multiplier,
            "last_copied_trade_id": child_account.last_copied_trade_id
        }
        
        was_copying = child_account.copy_status == "On"
        
        # Find active order mappings
        active_mappings = OrderMapping.query.filter_by(
            child_client_id=client_id,
            status="ACTIVE"
        ).all()
        
        # Update account to remove child role
        child_account.role = None
        child_account.linked_master_id = None
        child_account.copy_status = "Off"
        child_account.multiplier = 1.0
        child_account.last_copied_trade_id = None

        # Update mappings
        for mapping in active_mappings:
            mapping.status = "CHILD_REMOVED"
            mapping.remarks = f"Child account removed on {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}"

        # Commit changes
        db.session.commit()

        # Log the action
        log_entry = SystemLog(
            timestamp=datetime.utcnow(),
            level="INFO",
            message=f"Child removed: {client_id} from master {master_id}",
            user_id=str(user_id),
            details=json.dumps({
                "action": "remove_child",
                "child_id": client_id,
                "master_id": master_id,
                "was_copying": was_copying,
                "active_mappings": len(active_mappings),
                "previous_state": previous_state
            })
        )
        db.session.add(log_entry)
        db.session.commit()

        # Prepare response
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
                "active_mappings_found": len(active_mappings)
            }
        }

        return jsonify(response_data), 200

    except Exception as e:
        db.session.rollback()
        logger.error(f"Error in remove_child: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/remove-master', methods=['POST'])
@login_required
def remove_master():
    """Remove master role from account."""
    user_id = session.get("user_id")
    
    try:
        data = request.json
        client_id = data.get("client_id")
        
        if not client_id:
            return jsonify({"error": "Missing client_id"}), 400

        # Verify account belongs to user
        master_account = Account.query.filter_by(
            user_id=user_id,
            client_id=client_id,
            role='master'
        ).first()
        
        if not master_account:
            return jsonify({"error": "Master account not found or not configured as master"}), 404

        # Find all child accounts linked to this master
        linked_children = Account.query.filter_by(
            role='child',
            linked_master_id=client_id
        ).all()

        # Store current state for logging
        previous_state = {
            "role": master_account.role,
            "copy_status": master_account.copy_status,
            "linked_children": [child.client_id for child in linked_children],
            "active_children": [child.client_id for child in linked_children if child.copy_status == "On"]
        }

        # Find all active order mappings for this master
        active_mappings = OrderMapping.query.filter_by(
            master_client_id=client_id,
            status="ACTIVE"
        ).all()

        # Process each linked child (orphan them)
        children_processed = []
        children_failed = []

        for child in linked_children:
            try:
                # Save child details for response
                child_previous_state = {
                    "client_id": child.client_id,
                    "copy_status": child.copy_status,
                    "multiplier": child.multiplier
                }

                # Orphan the child account
                child.role = None
                child.linked_master_id = None
                child.copy_status = "Off"
                child.multiplier = 1.0
                child.last_copied_trade_id = None

                children_processed.append({
                    "client_id": child.client_id,
                    "broker": child.broker,
                    "username": child.username,
                    "was_copying": child_previous_state["copy_status"] == "On",
                    "status": "ORPHANED"
                })

            except Exception as e:
                logger.error(f"Failed to orphan child {child.client_id}: {str(e)}")
                children_failed.append({
                    "client_id": child.client_id,
                    "error": str(e)
                })

        # Remove master role from account
        master_account.role = None
        master_account.copy_status = "Off"
        master_account.multiplier = 1.0

        # Update all active order mappings to mark them as orphaned
        for mapping in active_mappings:
            mapping.status = "MASTER_REMOVED"
            mapping.remarks = f"Master account removed on {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}"

        # Commit changes
        db.session.commit()

        # Log the action
        log_entry = SystemLog(
            timestamp=datetime.utcnow(),
            level="INFO",
            message=f"Master removed: {client_id} with {len(linked_children)} children orphaned",
            user_id=str(user_id),
            details=json.dumps({
                "action": "remove_master",
                "master_id": client_id,
                "children_count": len(linked_children),
                "active_children": len([c for c in linked_children if c.copy_status == "On"]),
                "active_mappings": len(active_mappings),
                "previous_state": previous_state
            })
        )
        db.session.add(log_entry)
        db.session.commit()

        # Prepare response
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
                "total_children": len(linked_children),
                "active_children": len([c for c in linked_children if c.copy_status == "On"]),
                "successfully_orphaned": len(children_processed),
                "failed_to_orphan": len(children_failed),
                "orphan_details": children_processed
            },
            "order_mappings": {
                "active_mappings_found": len(active_mappings),
                "mappings_updated": len(active_mappings)
            },
            "removal_details": {
                "master_role_removed": True,
                "copy_status_reset": True,
                "multiplier_reset": True,
                "children_orphaned": len(children_processed)
            }
        }

        return jsonify(response_data), 200

    except Exception as e:
        db.session.rollback()
        logger.error(f"Error in remove_master: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/alerts')
@login_required
def get_alerts():
    """Get passive alert logs for the user."""
    user_id = session.get("user_id")
    
    try:
        # Get user accounts
        accounts = Account.query.filter_by(user_id=user_id).all()
        account_ids = [acc.client_id for acc in accounts]
        
        # Get logs for these accounts
        logs = TradeLog.query.filter(
            TradeLog.user_id.in_(account_ids),
            TradeLog.status == "ALERT"
        ).order_by(TradeLog.id.desc()).limit(20).all()

        alerts = [
            {"time": log.timestamp.isoformat(), "message": log.response}
            for log in logs
        ]
        
        return jsonify(alerts), 200
        
    except Exception as e:
        logger.error(f"Error in get_alerts: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/portfolio/<client_id>')
@login_required
def get_portfolio(client_id):
    """Return live positions for an account."""
    user_id = session.get("user_id")
    
    try:
        # Verify account belongs to user
        account = Account.query.filter_by(user_id=user_id, client_id=client_id).first()
        if not account:
            return jsonify({"error": "Account not found or access denied"}), 404

        # Initialize broker API
        api = broker_api(_account_to_dict(account))
        
        # Get positions
        positions_resp = api.get_positions()
        
        # Extract positions data with fallbacks
        if isinstance(positions_resp, dict):
            data = (
                positions_resp.get("data")
                or positions_resp.get("positions")
                or positions_resp.get("net")
                or []
            )
        else:
            data = positions_resp if isinstance(positions_resp, list) else []
            
        return jsonify(data), 200
        
    except Exception as e:
        logger.error(f"Error in get_portfolio: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/account/<client_id>')
@login_required
def get_account_stats(client_id):
    """Return account margin/fund stats."""
    user_id = session.get("user_id")
    
    try:
        # Verify account belongs to user
        account = Account.query.filter_by(user_id=user_id, client_id=client_id).first()
        if not account:
            return jsonify({"error": "Account not found or access denied"}), 404

        # Initialize broker API
        api = broker_api(_account_to_dict(account))
        
        # Get fund limits
        stats_resp = api.get_fund_limits()
        
        # Extract data with fallbacks
        if isinstance(stats_resp, dict):
            stats = stats_resp.get("data") or stats_resp
        else:
            stats = stats_resp or {}

        # Map to clean keys
        mapped_stats = {
            "total_funds": stats.get("availabelBalance") or stats.get("available_balance") or 0,
            "available_margin": stats.get("withdrawableBalance") or stats.get("withdrawable_balance") or 0,
            "used_margin": stats.get("utilizedAmount") or stats.get("utilized_amount") or 0
        }
        
        return jsonify(mapped_stats), 200
        
    except Exception as e:
        logger.error(f"Error in get_account_stats: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/logs')
@login_required
def get_logs():
    """Get trade logs for the user."""
    user_id = session.get("user_id")
    
    try:
        # Get user accounts
        accounts = Account.query.filter_by(user_id=user_id).all()
        account_ids = [acc.client_id for acc in accounts]
        
        # Get logs for these accounts
        logs = TradeLog.query.filter(
            TradeLog.user_id.in_(account_ids)
        ).order_by(TradeLog.id.desc()).limit(100).all()

        result = [
            {
                "timestamp": log.timestamp.isoformat(),
                "user_id": log.user_id,
                "symbol": log.symbol,
                "action": log.action,
                "quantity": log.quantity,
                "status": log.status,
                "response": log.response
            }
            for log in logs
        ]
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error in get_logs: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/zerodha-login-url')
@login_required
def zerodha_login_url_route():
    """Get Zerodha login URL for OAuth flow."""
    try:
        api_key = request.args.get("api_key")
        if not api_key:
            return jsonify({"error": "api_key required"}), 400
            
        kite = KiteConnect(api_key=api_key)
        return jsonify({"login_url": kite.login_url()})
        
    except Exception as e:
        logger.error(f"Error in zerodha_login_url: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/init-zerodha-login', methods=['POST'])
@login_required
def init_zerodha_login():
    """Initialize Zerodha login flow."""
    user_id = session.get("user_id")
    user_email = session.get("user_email")
    
    try:
        data = request.json
        client_id = data.get('client_id')
        api_key = data.get('api_key')
        api_secret = data.get('api_secret')
        username = data.get('username')
        
        if not all([client_id, api_key, api_secret, username]):
            return jsonify({'error': 'Missing fields'}), 400

        # Store pending authentication data
        pending = get_pending_zerodha()

        pending[client_id] = {
            'api_key': api_key,
            'api_secret': api_secret,
            'username': username,
            'owner': user_id,
            'owner_email': user_email
        }
        set_pending_zerodha(pending)

        # Create redirect URL
        redirect_uri = f"https://dhan-trading.onrender.com/zerodha_redirects/{client_id}"
        login_url = f"https://kite.zerodha.com/connect/login?api_key={api_key}&v=3&redirect_uri={quote(redirect_uri, safe='')}"
        
        return jsonify({'login_url': login_url})
        
    except Exception as e:
        logger.error(f"Error in init_zerodha_login: {str(e)}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/init-fyers-login', methods=['POST'])
@login_required
def init_fyers_login():
    """Initialize Fyers login flow."""
    user_id = session.get("user_id")
    user_email = session.get("user_email")
    
    try:
        data = request.json
        client_id = data.get('client_id')
        secret_key = data.get('secret_key')
        username = data.get('username')
        
        if not all([client_id, secret_key, username]):
            return jsonify({'error': 'Missing fields'}), 400

        # Store pending authentication data
        pending = get_pending_fyers()

        state = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        redirect_uri = f"https://dhan-trading.onrender.com/fyers_redirects/{client_id}"

        pending[client_id] = {
            'secret_key': secret_key,
            'redirect_uri': redirect_uri,
            'state': state,
            'username': username,
            'owner': user_id,
            'owner_email': user_email
        }
        set_pending_fyers(pending)

        # Get login URL
        login_url = FyersBroker.login_url(client_id, redirect_uri, state)
        return jsonify({'login_url': login_url})
        
    except Exception as e:
        logger.error(f"Error in init_fyers_login: {str(e)}")
        return jsonify({"error": str(e)}), 500


# ==============================================================================
# Webhook Routes (webhook_bp)
# ==============================================================================
@webhook_bp.route("/<token>", methods=["POST"])
def webhook(token):
    """Handle incoming webhook requests for order placement."""
    logger.info(f"Received webhook request for token {token}")
    
    try:
        # Get user by webhook token
        user = User.query.filter_by(webhook_token=token).first()
        if not user:
            logger.error(f"Invalid webhook token: {token}")
            return jsonify({"error": "Invalid webhook token"}), 403

        # Parse request data
        try:
            data = request.get_json(force=True)
        except Exception as e:
            logger.error(f"Failed to parse JSON data: {str(e)}")
            return jsonify({
                "error": "Invalid JSON data",
                "details": str(e)
            }), 400

        # Log the webhook request
        webhook_log = WebhookLog(
            user_id=user.id,
            timestamp=datetime.utcnow(),
            payload=json.dumps(data)
        )
        db.session.add(webhook_log)
        db.session.commit()

        # Handle alert messages
        if isinstance(data, str) or "message" in data:
            message = data if isinstance(data, str) else data.get("message")
            logger.info(f"Alert received for {user.email}: {message}")
            
            # Log the alert
            alert_log = TradeLog(
                user_id=str(user.id),
                status="ALERT",
                response=message,
                timestamp=datetime.utcnow()
            )
            db.session.add(alert_log)
            db.session.commit()
            
            webhook_log.response_status = "200"
            webhook_log.response_body = json.dumps({"status": "Alert logged", "message": message})
            db.session.commit()
            
            return jsonify({
                "status": "Alert logged",
                "message": message
            }), 200

        # Check trading is enabled globally
        settings = load_settings()
        if settings.get('trading_enabled') != True:
            error_msg = "Trading is disabled globally"
            logger.warning(f"Webhook rejected: {error_msg}")
            
            webhook_log.response_status = "403"
            webhook_log.response_body = json.dumps({"error": error_msg})
            db.session.commit()
            
            return jsonify({"error": error_msg}), 403

        # Validate required fields
        symbol = data.get("symbol")
        action = data.get("action")
        quantity = data.get("quantity")

        if not all([symbol, action, quantity]):
            error_msg = "Missing required fields"
            logger.error(f"Invalid webhook data: {error_msg}")
            
            webhook_log.response_status = "400"
            webhook_log.response_body = json.dumps({
                "error": error_msg,
                "required": ["symbol", "action", "quantity"]
            })
            db.session.commit()
            
            return jsonify({
                "error": error_msg,
                "required": ["symbol", "action", "quantity"],
                "received": {
                    "symbol": bool(symbol),
                    "action": bool(action),
                    "quantity": bool(quantity)
                }
            }), 400

        # Get primary account for the user
        account = get_primary_account(user)
        if not account:
            error_msg = "No account configured for user"
            logger.error(f"Account not found for webhook {token}")
            
            webhook_log.response_status = "404"
            webhook_log.response_body = json.dumps({"error": error_msg})
            db.session.commit()
            
            return jsonify({"error": error_msg}), 404

        # Initialize broker API
        try:
            api = broker_api(_account_to_dict(account))
        except Exception as e:
            error_msg = f"Failed to initialize broker: {str(e)}"
            logger.error(error_msg)
            
            webhook_log.response_status = "500"
            webhook_log.response_body = json.dumps({"error": error_msg})
            db.session.commit()
            
            return jsonify({"error": error_msg}), 500

        # Get symbol mapping and build order parameters
        try:
            broker_name = account.broker.lower()
            mapping = get_symbol_for_broker(symbol, broker_name)
            
            if not mapping:
                error_msg = f"Symbol '{symbol}' not found in symbol map"
                logger.error(error_msg)
                
                webhook_log.response_status = "400"
                webhook_log.response_body = json.dumps({"error": error_msg})
                db.session.commit()
                
                return jsonify({"error": error_msg}), 400
                
            # Build order parameters based on broker
            if broker_name == "dhan":
                security_id = mapping.get("security_id")
                if not security_id:
                    error_msg = f"Security ID not found for {symbol}"
                    logger.error(error_msg)
                    
                    webhook_log.response_status = "400"
                    webhook_log.response_body = json.dumps({"error": error_msg})
                    db.session.commit()
                    
                    return jsonify({"error": error_msg}), 400
                    
                order_params = {
                    "tradingsymbol": symbol,
                    "security_id": security_id,
                    "exchange_segment": getattr(api, "NSE", "NSE_EQ"),
                    "transaction_type": action.upper(),
                    "quantity": int(quantity),
                    "order_type": "MARKET",
                    "product_type": getattr(api, "INTRA", "INTRADAY"),
                    "price": 0
                }
                
            elif broker_name == "aliceblue":
                symbol_id = mapping.get("symbol_id")
                if not symbol_id:
                    error_msg = f"Symbol ID not found for {symbol}"
                    logger.error(error_msg)
                    
                    webhook_log.response_status = "400"
                    webhook_log.response_body = json.dumps({"error": error_msg})
                    db.session.commit()
                    
                    return jsonify({"error": error_msg}), 400
                    
                order_params = {
                    "tradingsymbol": mapping.get("tradingsymbol", symbol),
                    "symbol_id": symbol_id,
                    "exchange": "NSE",
                    "transaction_type": action.upper(),
                    "quantity": int(quantity),
                    "order_type": "MKT",
                    "product": "MIS",
                    "price": 0
                }
                
            elif broker_name == "finvasia":
                token = mapping.get("token")
                if not token:
                    error_msg = f"Token not found for {symbol}"
                    logger.error(error_msg)
                    
                    webhook_log.response_status = "400"
                    webhook_log.response_body = json.dumps({"error": error_msg})
                    db.session.commit()
                    
                    return jsonify({"error": error_msg}), 400
                    
                order_params = {
                    "tradingsymbol": mapping.get("symbol", symbol),
                    "exchange": mapping.get("exchange", "NSE"),
                    "transaction_type": action.upper(),
                    "quantity": int(quantity),
                    "order_type": "MKT",
                    "product": "MIS",
                    "price": 0,
                    "token": token
                }
                
            else:
                order_params = {
                    "tradingsymbol": mapping.get("tradingsymbol", symbol),
                    "exchange": "NSE",
                    "transaction_type": action.upper(),
                    "quantity": int(quantity),
                    "order_type": "MARKET",
                    "product": "MIS",
                    "price": 0
                }
                
        except Exception as e:
            error_msg = f"Failed to build order parameters: {str(e)}"
            logger.error(error_msg)
            
            webhook_log.response_status = "500"
            webhook_log.response_body = json.dumps({"error": error_msg})
            db.session.commit()
            
            return jsonify({"error": error_msg}), 500

        # Place the order
        try:
            logger.info(f"Placing {action} order for {quantity} {symbol} via {broker_name}")
            response = api.place_order(**order_params)
            
            # Handle error response
            if isinstance(response, dict) and response.get("status") == "failure":
                status = "FAILED"
                reason = clean_response_message(response)
                
                logger.error(f"Order failed: {reason}")
                
                # Record the trade
                record_trade(user.id, symbol, action.upper(), quantity, order_params.get('price', 0), status)
                
                # Log the response
                webhook_log.response_status = "400"
                webhook_log.response_body = json.dumps({"status": status, "reason": reason})
                db.session.commit()
                
                return jsonify({
                    "status": status,
                    "reason": reason
                }), 400

            # Order successful
            status = "SUCCESS"
            success_msg = response.get("remarks", "Trade placed successfully")
            order_id = (
                response.get("order_id")
                or response.get("orderId")
                or response.get("id")
                or "Unknown"
            )
            
            logger.info(f"Order placed successfully: {order_id}")
            
            # Record the trade
            record_trade(user.id, symbol, action.upper(), quantity, order_params.get('price', 0), status)
            
            # Log the response
            webhook_log.response_status = "200"
            webhook_log.response_body = json.dumps({
                "status": status,
                "result": success_msg,
                "order_id": order_id
            })
            db.session.commit()
            
            # Trigger copy trading
            try:
                from threading import Thread
                Thread(target=poll_and_copy_trades, args=[current_app]).start()
            except Exception as e:
                logger.error(f"Failed to trigger copy trading: {str(e)}")
            
            return jsonify({
                "status": status,
                "result": success_msg,
                "order_id": order_id
            }), 200

        except Exception as e:
            error_msg = f"Failed to place order: {str(e)}"
            logger.error(error_msg)
            
            webhook_log.response_status = "500"
            webhook_log.response_body = json.dumps({"error": error_msg})
            db.session.commit()
            
            return jsonify({"error": error_msg}), 500

    except Exception as e:
        logger.error(f"Unexpected error in webhook: {str(e)}")
        return jsonify({"error": "Internal server error", "details": str(e)}), 500


@webhook_bp.route("/zerodha_redirects/<client_id>")
def zerodha_redirect_handler(client_id):
    """Handle OAuth redirect from Zerodha login flow."""
    logger.info(f"Processing Zerodha redirect for client {client_id}")
    
    try:
        # Validate request token
        request_token = request.args.get("request_token")
        if not request_token:
            logger.error("No request token received in redirect")
            return render_template(
                "error.html",
                error="Missing request token",
                message="The authentication process was incomplete."
            ), 400

        # Load pending authentication data
        pending = get_pending_zerodha()
        cred = pending.pop(client_id, None)
        
        if not cred:
            logger.error(f"No pending auth found for client {client_id}")
            return render_template(
                "error.html",
                error="Invalid Request",
                message="No pending authentication found for this client."
            ), 400

        # Extract credentials
        api_key = cred.get("api_key")
        api_secret = cred.get("api_secret")
        owner = cred.get("owner")
        
        if not api_key or not api_secret:
            logger.error("Missing API credentials in pending auth")
            return render_template(
                "error.html",
                error="Invalid Credentials",
                message="Missing required API credentials."
            ), 400

        username = cred.get("username") or client_id
        
        try:
            # Initialize Kite Connect
            kite = KiteConnect(api_key=api_key)
            
            # Generate session
            session_data = kite.generate_session(request_token, api_secret)
            
            if not session_data or "access_token" not in session_data:
                raise ValueError("Failed to generate valid session")
                
            access_token = session_data["access_token"]
            
            # Prepare account data
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
                "last_login_time": datetime.utcnow(),
                "role": None,
                "linked_master_id": None,
                "multiplier": 1.0,
                "copy_status": "Off",
            }

            # Save account data
            save_account_to_user(owner, account)
            set_pending_zerodha(pending)
            
            logger.info(f"Successfully connected Zerodha account for {client_id}")
            flash("Zerodha account connected successfully!", "success")
            
            return redirect(url_for("main.add_account"))
            
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


@webhook_bp.route("/fyers_redirects/<client_id>")
def fyers_redirect_handler(client_id):
    """Handle redirect from Fyers OAuth flow."""
    logger.info(f"Processing Fyers redirect for client {client_id}")
    
    try:
        # Validate auth code
        auth_code = request.args.get('auth_code')
        state = request.args.get('state')
        
        if not auth_code:
            logger.error("No auth_code received in redirect")
            return render_template(
                "error.html",
                error="Missing Auth Code",
                message="The authentication process was incomplete."
            ), 400

        # Load pending authentication data
        pending = get_pending_fyers()
        cred = pending.pop(client_id, None)
        
        if not cred or cred.get('state') != state:
            logger.error(f"No pending auth found for client {client_id} or state mismatch")
            return render_template(
                "error.html",
                error="Invalid Request",
                message="No pending authentication found for this client or state mismatch."
            ), 400

        # Extract credentials
        secret_key = cred.get('secret_key')
        redirect_uri = cred.get('redirect_uri')
        owner = cred.get('owner')
        username = cred.get('username') or client_id

        # Exchange code for token
        token_resp = FyersBroker.exchange_code_for_token(client_id, secret_key, auth_code)
        
        if token_resp.get('s') != 'ok':
            msg = token_resp.get('message', 'Failed to generate token')
            logger.error(f"Failed to exchange code for token: {msg}")
            return render_template(
                "error.html",
                error="Token Generation Failed",
                message=f"Failed to generate token: {msg}"
            ), 500

        access_token = token_resp.get('access_token')
        refresh_token = token_resp.get('refresh_token')

        # Prepare account data
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
            'last_login_time': datetime.utcnow(),
            'role': None,
            'linked_master_id': None,
            'multiplier': 1.0,
            'copy_status': 'Off',
        }

        # Save account data
        save_account_to_user(owner, account)
        set_pending_fyers(pending)
        
        logger.info(f"Successfully connected Fyers account for {client_id}")
        flash("Fyers account connected successfully!", "success")
        
        return redirect(url_for('main.add_account'))
        
    except Exception as e:
        logger.error(f"Unexpected error in Fyers redirect handler: {str(e)}")
        return render_template(
            "error.html",
            error="Server Error",
            message="An unexpected error occurred."
        ), 500


# ==============================================================================
# Admin Routes (admin_bp)
# ==============================================================================
@admin_bp.route('/dashboard')
@admin_required
def dashboard():
    """Admin dashboard page."""
    try:
        users = User.query.all()
        accounts = Account.query.all()
        unique_brokers = {acc.broker for acc in accounts if acc.broker}

        today = date.today()
        start_today = today.strftime('%Y-%m-%d')
        end_today = (today + timedelta(days=1)).strftime('%Y-%m-%d')
        
        trades_today = Trade.query.filter(
            Trade.timestamp >= datetime.strptime(start_today, '%Y-%m-%d'),
            Trade.timestamp < datetime.strptime(end_today, '%Y-%m-%d')
        ).count()
        
        active_users = User.query.filter(
            User.last_login >= datetime.strptime(start_today, '%Y-%m-%d')
        ).count()
        
        failed_trades = Trade.query.filter_by(status='Failed').count()
        
        metrics = {
            'total_users': len(users),
            'active_users': active_users,
            'total_accounts': len(accounts),
            'brokers_connected': len(unique_brokers),
            'trades_today': trades_today,
            'failed_trades': failed_trades,
            'uptime': format_uptime(),
            'current_time': f"{CURRENT_UTC_TIME} (UTC)",
            'current_user': CURRENT_USER
        }

        # Prepare chart data
        labels = []
        trade_counts = []
        signup_counts = []
        
        for i in range(5):
            day = today - timedelta(days=4 - i)
            start = day.strftime('%Y-%m-%d')
            end = (day + timedelta(days=1)).strftime('%Y-%m-%d')
            
            labels.append(day.strftime('%a'))
            
            day_trades = Trade.query.filter(
                Trade.timestamp >= datetime.strptime(start, '%Y-%m-%d'),
                Trade.timestamp < datetime.strptime(end, '%Y-%m-%d')
            ).count()
            trade_counts.append(day_trades)
            
            day_signups = User.query.filter(
                User.subscription_start >= datetime.strptime(start, '%Y-%m-%d'),
                User.subscription_start < datetime.strptime(end, '%Y-%m-%d')
            ).count()
            signup_counts.append(day_signups)

        trade_chart = {'labels': labels, 'data': trade_counts}
        signup_chart = {'labels': labels, 'data': signup_counts}

        # Check broker API status
        broker_list = sorted({acc.broker.lower() for acc in accounts if acc.broker})
        api_status = []
        
        for name in broker_list:
            url = BROKER_STATUS_URLS.get(name)
            online = check_api(url) if url else False
            api_status.append({'name': name.title(), 'online': online})
            
        return render_template(
            'admin/dashboard.html',
            metrics=metrics,
            api_status=api_status,
            trade_chart=trade_chart,
            signup_chart=signup_chart
        )
        
    except Exception as e:
        logger.error(f"Error in admin dashboard: {str(e)}")
        flash(f"Error loading dashboard: {str(e)}", "error")
        return render_template('admin/dashboard.html', error=str(e))


@admin_bp.route('/users')
@admin_required
def admin_users():
    """Admin users management page."""
    try:
        users = User.query.order_by(User.id.desc()).all()
        return render_template('admin/users.html', users=users)
    except Exception as e:
        logger.error(f"Error in admin users: {str(e)}")
        flash(f"Error loading users: {str(e)}", "error")
        return render_template('admin/users.html', error=str(e))


@admin_bp.route('/users/<int:user_id>/suspend', methods=['POST'])
@admin_required
def admin_suspend_user(user_id):
    """Suspend a user."""
    try:
        user = User.query.get_or_404(user_id)
        user.plan = 'Suspended'
        db.session.commit()
        
        flash(f'User {user.email} suspended.', 'success')
        return redirect(url_for('admin.admin_users'))
    except Exception as e:
        logger.error(f"Error suspending user: {str(e)}")
        flash(f"Error suspending user: {str(e)}", "error")
        return redirect(url_for('admin.admin_users'))


@admin_bp.route('/users/<int:user_id>/reset', methods=['POST'])
@admin_required
def admin_reset_password(user_id):
    """Reset a user's password."""
    try:
        user = User.query.get_or_404(user_id)
        new_pass = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        user.set_password(new_pass)
        db.session.commit()
        
        flash(f'New password for {user.email}: {new_pass}', 'success')
        return redirect(url_for('admin.admin_users'))
    except Exception as e:
        logger.error(f"Error resetting password: {str(e)}")
        flash(f"Error resetting password: {str(e)}", "error")
        return redirect(url_for('admin.admin_users'))


@admin_bp.route('/users/<int:user_id>')
@admin_required
def admin_view_user(user_id):
    """View user details."""
    try:
        user = User.query.get_or_404(user_id)
        return render_template('admin/user_detail.html', user=user)
    except Exception as e:
        logger.error(f"Error viewing user: {str(e)}")
        flash(f"Error viewing user: {str(e)}", "error")
        return redirect(url_for('admin.admin_users'))


@admin_bp.route('/accounts')
@admin_required
def admin_accounts():
    """Admin accounts management page."""
    try:
        accounts = Account.query.order_by(Account.id.desc()).all()
        account_data = []
        
        for acc in accounts:
            user = User.query.get(acc.user_id) if acc.user_id else None
            account_data.append({
                'id': acc.id,
                'client_id': acc.client_id,
                'broker': acc.broker,
                'username': acc.username,
                'user_email': user.email if user else 'N/A',
                'status': acc.status,
                'role': acc.role or 'None',
                'copy_status': acc.copy_status,
                'last_login': acc.last_login_time.strftime('%Y-%m-%d %H:%M:%S') if acc.last_login_time else 'Never'
            })
            
        return render_template('admin/accounts.html', accounts=account_data)
    except Exception as e:
        logger.error(f"Error in admin accounts: {str(e)}")
        flash(f"Error loading accounts: {str(e)}", "error")
        return render_template('admin/accounts.html', error=str(e))


@admin_bp.route('/trades')
@admin_required
def admin_trades():
    """Admin trades management page."""
    try:
        trades = Trade.query.order_by(Trade.id.desc()).limit(200).all()
        trade_data = []
        
        for trade in trades:
            user = User.query.get(trade.user_id) if trade.user_id else None
            trade_data.append({
                'id': trade.id,
                'user_email': user.email if user else 'N/A',
                'symbol': trade.symbol,
                'action': trade.action,
                'qty': trade.qty,
                'price': trade.price,
                'status': trade.status,
                'timestamp': trade.timestamp.strftime('%Y-%m-%d %H:%M:%S') if trade.timestamp else 'Unknown'
            })
            
        return render_template('admin/trades.html', trades=trade_data)
    except Exception as e:
        logger.error(f"Error in admin trades: {str(e)}")
        flash(f"Error loading trades: {str(e)}", "error")
        return render_template('admin/trades.html', error=str(e))


@admin_bp.route('/logs')
@admin_required
def admin_logs():
    """Admin system logs page."""
    try:
        logs = SystemLog.query.order_by(SystemLog.id.desc()).limit(200).all()
        return render_template('admin/logs.html', logs=logs)
    except Exception as e:
        logger.error(f"Error in admin logs: {str(e)}")
        flash(f"Error loading logs: {str(e)}", "error")
        return render_template('admin/logs.html', error=str(e))


@admin_bp.route('/settings', methods=['GET', 'POST'])
@admin_required
def admin_settings():
    """Admin system settings page."""
    if request.method == 'POST':
        try:
            trading_enabled = request.form.get('trading_enabled') == 'on'
            poll_interval = request.form.get('poll_interval', '30')
            
            # Validate poll interval
            try:
                poll_interval = int(poll_interval)
                if poll_interval < 5:
                    poll_interval = 5
                elif poll_interval > 120:
                    poll_interval = 120
            except ValueError:
                poll_interval = 30
            
            # Update settings
            settings = {
                'trading_enabled': trading_enabled,
                'poll_interval': poll_interval
            }
            save_settings(settings)
            
            # Update scheduler if poll interval changed
            current_settings = load_settings()
            if int(current_settings.get('poll_interval', '30')) != poll_interval:
                scheduler = current_app.scheduler
                if scheduler:
                    job = scheduler.get_job('copy_trades_job')
                    if job:
                        job.reschedule(trigger='interval', seconds=poll_interval)
                        logger.info(f"Scheduler poll interval updated to {poll_interval} seconds")
            
            flash('Settings updated successfully', 'success')
            return redirect(url_for('admin.admin_settings'))
        except Exception as e:
            logger.error(f"Error updating settings: {str(e)}")
            flash(f"Error updating settings: {str(e)}", "error")
    
    # Get current settings
    settings = load_settings()
    return render_template('admin/settings.html', settings=settings)


@admin_bp.route('/security', methods=['GET', 'POST'])
@admin_required
def admin_security():
    """Admin security settings page."""
    if request.method == 'POST':
        try:
            current_password = request.form.get('current_password')
            new_password = request.form.get('new_password')
            confirm_password = request.form.get('confirm_password')
            
            if not current_password or current_password != Config.ADMIN_PASSWORD:
                flash('Current password is incorrect', 'error')
                return redirect(url_for('admin.admin_security'))
            
            if not new_password:
                flash('New password cannot be empty', 'error')
                return redirect(url_for('admin.admin_security'))
                
            if new_password != confirm_password:
                flash('Passwords do not match', 'error')
                return redirect(url_for('admin.admin_security'))
            
            # Update admin password
            config_setting = Setting.query.filter_by(key='admin_password').first()
            if not config_setting:
                config_setting = Setting(key='admin_password', value=new_password)
                db.session.add(config_setting)
            else:
                config_setting.value = new_password
            
            db.session.commit()
            
            # Update in-memory config
            current_app.config['ADMIN_PASSWORD'] = new_password
            
            flash('Admin password updated successfully', 'success')
            return redirect(url_for('admin.admin_security'))
        except Exception as e:
            logger.error(f"Error updating admin password: {str(e)}")
            flash(f"Error updating admin password: {str(e)}", "error")
    
    return render_template('admin/security.html')


@admin_bp.route('/clear-logs', methods=['POST'])
@admin_required
def admin_clear_logs():
    """Clear system logs."""
    try:
        # Only clear logs older than 7 days
        cutoff_date = datetime.utcnow() - timedelta(days=7)
        SystemLog.query.filter(SystemLog.timestamp < cutoff_date).delete()
        db.session.commit()
        
        flash('Logs older than 7 days cleared successfully', 'success')
        return redirect(url_for('admin.admin_logs'))
    except Exception as e:
        logger.error(f"Error clearing logs: {str(e)}")
        flash(f"Error clearing logs: {str(e)}", "error")
        return redirect(url_for('admin.admin_logs'))


# ==============================================================================
# Application Factory
# ==============================================================================
def create_app(config_class=Config):
    """Create and configure the Flask application."""
    app = Flask(__name__, template_folder='templates', static_folder='static')
    app.config.from_object(config_class)

    # Initialize extensions
    db.init_app(app)
    csrf.init_app(app)
    CORS(app, resources={r"/api/*": {"origins": "*"}})

    # Register blueprints
    app.register_blueprint(main_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(webhook_bp)

    # Store current UTC time and user
    app.config['CURRENT_UTC_TIME'] = "2025-07-08 06:54:02"
    app.config['CURRENT_USER'] = "AnkitSG07"

    with app.app_context():
        # Create all database tables if they don't exist
        db.create_all()
        logger.info("Database tables verified/created.")

        # Initialize admin user if it doesn't exist
        admin_email = Config.ADMIN_EMAIL
        admin_user = User.query.filter_by(email=admin_email).first()
        if not admin_user:
            admin_user = User(
                email=admin_email,
                name="System Administrator",
                plan="Admin"
            )
            admin_user.set_password(Config.ADMIN_PASSWORD)
            db.session.add(admin_user)
            db.session.commit()
            logger.info(f"Admin user {admin_email} created.")

        # Initialize and start the scheduler
        scheduler = BackgroundScheduler(daemon=True)
        
        # Get poll interval from settings or use default
        settings = load_settings()
        poll_interval = int(settings.get('poll_interval', 30))
        
        # Add polling job
        scheduler.add_job(
            func=poll_and_copy_trades,
            args=[app],
            trigger="interval",
            seconds=poll_interval,
            id="copy_trades_job",
            max_instances=1,
            coalesce=True
        )
        
        # Store scheduler in app for later access
        app.scheduler = scheduler
        
        scheduler.start()
        logger.info(f"Background trade copy scheduler started with {poll_interval}s interval.")
        
        # Ensure scheduler shuts down cleanly on exit
        atexit.register(lambda: scheduler.shutdown())

    @app.errorhandler(404)
    def not_found_error(error):
        return render_template('404.html'), 404

    @app.errorhandler(500)
    def internal_error(error):
        db.session.rollback()
        logger.error(f"Internal Server Error: {error}")
        return render_template('500.html'), 500

    @app.context_processor
    def inject_global_variables():
        """Make certain variables available to all templates."""
        return {
            'current_time': app.config['CURRENT_UTC_TIME'],
            'current_user': app.config['CURRENT_USER'],
            'app_version': '2.0.0'
        }

    # Add route to access static files
    @app.route('/static/<path:filename>')
    def static_files(filename):
        return send_from_directory(app.static_folder, filename)

    return app


# ==============================================================================
# Main Execution
# ==============================================================================
if __name__ == "__main__":
    app = create_app()
    
    # For production, use a WSGI server like Gunicorn instead of the development server
    # Command: gunicorn --workers 4 --bind 0.0.0.0:5000 'app:create_app()'
    app.run(debug=False, host="0.0.0.0", port=5001)
