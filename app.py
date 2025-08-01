from brokers.factory import get_broker_class
import uuid
from brokers.zerodha import ZerodhaBroker, KiteConnect
from brokers.fyers import FyersBroker
try:
    from brokers.symbol_map import get_symbol_for_broker, get_symbol_by_token
except Exception:  # pragma: no cover - fallback if import fails
    def get_symbol_for_broker(symbol: str, broker: str) -> dict:
        """Fallback stub returning empty mapping."""
        return {}
    def get_symbol_by_token(token: str, broker: str) -> str:
        return None
from flask import Flask, request, jsonify, render_template, session, redirect, url_for, flash, send_from_directory
from flask_migrate import Migrate
from dhanhq import dhanhq
import os
import json
import pandas as pd
from flask_cors import CORS
from flask_talisman import Talisman
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from apscheduler.schedulers.background import BackgroundScheduler
import io
from datetime import datetime, timedelta, date
from dateutil import parser
import requests
from bs4 import BeautifulSoup
import tempfile
import shutil
from functools import wraps
from werkzeug.utils import secure_filename
import random
import string
from urllib.parse import quote
from time import time
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
    Strategy,
    StrategySubscription,
    StrategyLog,
)
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import or_, text, inspect
import re
from blueprints.auth import auth_bp
from blueprints.api import api_bp
from helpers import (
    current_user,
    user_account_ids,
    order_mappings_for_user,
    active_children_for_master,
    extract_product_type,
    map_product_for_broker,
    log_connection_error,
    clear_init_error_logs,
    clear_connection_error_logs,
    normalize_position,
)
from symbols import get_symbols

# Define emoji regex pattern
EMOJI_RE = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)

app = Flask(__name__)
secret_key = os.environ.get("SECRET_KEY")
if not secret_key:
    raise RuntimeError("SECRET_KEY environment variable is required")
app.secret_key = secret_key
CORS(app)
# Allow required external resources while keeping a restrictive default CSP
csp = {
    'default-src': ["'self'"],
    "img-src": ["'self'", "data:", "https:"],
    'script-src': [
        "'self'",
        "'unsafe-inline'",
        'https://cdn.jsdelivr.net',
        'https://s3.tradingview.com',
    ],
    'style-src': [
        "'self'",
        "'unsafe-inline'",
        'https://cdn.jsdelivr.net',
        'https://fonts.googleapis.com',
    ],
    'font-src': [
        "'self'",
        'https://fonts.gstatic.com',
        'https://cdn.jsdelivr.net',
    ],
    'connect-src': [
        "'self'",
        'https://latest-stock-price.p.rapidapi.com',
    ],
}
Talisman(app, content_security_policy=csp, force_https=os.environ.get("FORCE_HTTPS") == "1")
# Configure rate limiting with a pluggable storage backend. Default to
# in-memory storage but allow overriding via an environment variable.
limiter_storage = os.environ.get("LIMITER_STORAGE_URL", "memory://")
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["200 per day", "50 per hour"],
    storage_uri=limiter_storage,
)
# Persist data in a configurable directory. By default this is ``./data`` so
# that files survive across redeploys on platforms like Render.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(BASE_DIR, "data"))
os.makedirs(DATA_DIR, exist_ok=True)
PROFILE_IMAGE_DIR = os.path.join(DATA_DIR, "profile_images")
os.makedirs(PROFILE_IMAGE_DIR, exist_ok=True)
app.config["PROFILE_IMAGE_DIR"] = PROFILE_IMAGE_DIR

import logging
from logging.handlers import RotatingFileHandler

# Setup basic logging
log_path = os.path.join(DATA_DIR, 'app.log')
log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
file_handler = RotatingFileHandler(log_path, maxBytes=1_000_000, backupCount=10)
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(), file_handler],
)
logger = logging.getLogger(__name__)
_scheduler = None

# Cache for broker opening balances
OPENING_BALANCE_CACHE = {}
# Seconds to keep a cached balance before refreshing
CACHE_TTL = 300

db_url = os.environ.get("DATABASE_URL")
if not db_url:
    raise RuntimeError("DATABASE_URL must be set to a PostgreSQL connection")
app.config["SQLALCHEMY_DATABASE_URI"] = db_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {"pool_pre_ping": True}

db.init_app(app)
migrate = Migrate(app, db)
app.register_blueprint(auth_bp)
app.register_blueprint(api_bp)
start_time = datetime.utcnow()
device_number = None

# Jinja filter to return broker icon URL for different possible image extensions
@app.template_filter('broker_icon_url')
def broker_icon_url(broker: str) -> str:
    """Return the URL for the broker icon if available."""
    if not broker:
        return url_for('static', filename='images/logo.png')
    extensions = ['png', 'jpg', 'jpeg', 'svg', 'webp']
    for ext in extensions:
        path = os.path.join(app.static_folder, 'images', f'{broker}.{ext}')
        if os.path.exists(path):
            return url_for('static', filename=f'images/{broker}.{ext}')
    return url_for('static', filename='images/logo.png')

def get_broker_icon_map() -> dict:
    """Return mapping of broker name to icon URL for templates."""
    img_dir = os.path.join(app.static_folder, 'images')
    icons = {}
    for filename in os.listdir(img_dir):
        name, ext = os.path.splitext(filename)
        if ext.lstrip('.').lower() in ['png', 'jpg', 'jpeg', 'svg', 'webp']:
            icons[name] = url_for('static', filename=f'images/{filename}')
    return icons


@app.context_processor
def inject_broker_icons():
    return {'broker_icons': get_broker_icon_map()}


@app.context_processor
def inject_logged_in_user():
    """Make the currently logged in user available to all templates."""
    user = current_user()
    if user and not user.webhook_token:
        import secrets
        user.webhook_token = secrets.token_hex(16)
        try:
            db.session.commit()
        except Exception:
            db.session.rollback()
    return {'logged_in_user': user}

BROKER_STATUS_URLS = {
    "dhan": "https://api.dhan.co",
    "zerodha": "https://api.kite.trade",
    "aliceblue": "https://ant.aliceblueonline.com",
    "finvasia": "https://api.shoonya.com",
    "fyers": "https://api.fyers.in",
    "groww": "https://groww.in",
}

def ensure_system_log_schema():
    logger.info("Ensuring system_log table schema is up-to-date...")
    with app.app_context():
        insp = inspect(db.engine)
        table_name = 'system_log'

        # 1. Check if table exists, if not, create it
        if table_name not in insp.get_table_names():
            try:
                db.metadata.create_all(bind=db.engine, tables=[SystemLog.__table__])
                logger.info(f'Created {table_name} table as it was missing.')
            except Exception as exc:
                logger.error(f'Failed to create {table_name} table: {exc}')
                return

        # 2. Check and add missing columns
        existing_columns = {col['name'] for col in insp.get_columns(table_name)}

        columns_to_add = {
            'level': 'VARCHAR(50) DEFAULT \'INFO\'',
            'message': 'TEXT',
            'timestamp': 'TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP',
            'details': 'JSONB',
            'user_id': 'VARCHAR(36)',
            'module': 'VARCHAR(50)'
        }

        for col_name, col_definition in columns_to_add.items():
            if col_name not in existing_columns:
                try:
                    with db.engine.begin() as conn:
                        conn.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {col_definition}'))
                    logger.info(f'Added missing column "{col_name}" to "{table_name}" table.')
                except Exception as exc:
                    logger.warning(f'Failed to add column "{col_name}" to "{table_name}": {exc}. Manual migration might be needed.')
            else:
                if col_name == 'details':
                    # Convert to string before calling .upper()
                    current_type_obj = next((col['type'] for col in insp.get_columns(table_name) if col['name'] == col_name), None)
                    current_type = str(current_type_obj).upper() if current_type_obj else ''

                    if 'VARCHAR' in current_type and '255' in current_type:
                        try:
                            with db.engine.begin() as conn:
                                conn.execute(text(f'ALTER TABLE "{table_name}" ALTER COLUMN "{col_name}" TYPE JSONB USING "{col_name}"::jsonb'))
                            logger.info(f'Altered column "{col_name}" to JSONB in "{table_name}" table.')
                        except Exception as exc:
                            logger.warning(f'Failed to alter column "{col_name}" to JSONB in "{table_name}": {exc}. Manual migration for existing data might be required.')
                elif col_name == 'message':
                    # Convert to string before calling .upper()
                    current_type_obj = next((col['type'] for col in insp.get_columns(table_name) if col['name'] == col_name), None)
                    current_type = str(current_type_obj).upper() if current_type_obj else ''

                    if 'VARCHAR' in current_type:
                        try:
                            with db.engine.begin() as conn:
                                conn.execute(text(f'ALTER TABLE "{table_name}" ALTER COLUMN "{col_name}" TYPE TEXT'))
                            logger.info(f'Altered column "{col_name}" to TEXT in "{table_name}" table.')
                        except Exception as exc:
                            logger.warning(f'Failed to alter column "{col_name}" to TEXT in "{table_name}": {exc}.')
                logger.debug(f'Column "{col_name}" already exists in "{table_name}".')

        logger.info(f"Schema check for {table_name} completed.")

def ensure_trade_schema():
    """Ensure the trade table has required columns."""
    logger.info("Ensuring trade table schema is up-to-date...")
    with app.app_context():
        insp = inspect(db.engine)
        table_name = 'trade'

        # 1. Create table if missing
        if table_name not in insp.get_table_names():
            try:
                db.metadata.create_all(bind=db.engine, tables=[Trade.__table__])
                logger.info(f'Created {table_name} table as it was missing.')
            except Exception as exc:
                logger.error(f'Failed to create {table_name} table: {exc}')
                return

        # 2. Add missing columns
        existing_columns = {col['name'] for col in insp.get_columns(table_name)}

        columns_to_add = {
            'broker': 'VARCHAR(50)',
            'order_id': 'VARCHAR(50)',
            'client_id': 'VARCHAR(50)',
        }

        for col_name, col_def in columns_to_add.items():
            if col_name not in existing_columns:
                try:
                    with db.engine.begin() as conn:
                        conn.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {col_def}'))
                    logger.info(f'Added missing column "{col_name}" to "{table_name}" table.')
                except Exception as exc:
                    logger.warning(
                        f'Failed to add column "{col_name}" to "{table_name}": {exc}. Manual migration might be needed.'
                    )
            else:
                logger.debug(f'Column "{col_name}" already exists in "{table_name}".')

        logger.info(f"Schema check for {table_name} completed.")
        
# The original duplicate function has been removed.
# This ensures that schema checks are done consistently.
ensure_system_log_schema()
ensure_trade_schema()

def ensure_setting_schema():
    """Ensure the setting table exists and has a TEXT value column."""
    logger.info("Ensuring setting table schema is up-to-date...")
    with app.app_context():
        insp = inspect(db.engine)
        table_name = 'setting'

        if table_name not in insp.get_table_names():
            try:
                db.metadata.create_all(bind=db.engine, tables=[Setting.__table__])
                logger.info(f'Created {table_name} table as it was missing.')
            except Exception as exc:
                logger.error(f'Failed to create {table_name} table: {exc}')
                return

        existing_columns = {col['name'] for col in insp.get_columns(table_name)}
        if 'value' in existing_columns:
            value_col = next(col for col in insp.get_columns(table_name) if col['name'] == 'value')
            current_type = str(value_col['type']).upper()
            if 'VARCHAR' in current_type and '255' in current_type:
                try:
                    with db.engine.begin() as conn:
                        conn.execute(text(f'ALTER TABLE "{table_name}" ALTER COLUMN "value" TYPE TEXT'))
                    logger.info(f'Altered column "value" to TEXT in "{table_name}" table.')
                except Exception as exc:
                    logger.warning(f'Failed to alter column "value" to TEXT in "{table_name}": {exc}.')
        else:
            try:
                with db.engine.begin() as conn:
                    conn.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN "value" TEXT'))
                logger.info(f'Added missing column "value" to "{table_name}" table.')
            except Exception as exc:
                logger.warning(f'Failed to add column "value" to "{table_name}": {exc}.')

ensure_setting_schema()


def ensure_user_profile_schema():
    """Ensure the user.profile_image column uses TEXT type."""
    logger.info("Ensuring user.profile_image column is up-to-date...")
    with app.app_context():
        insp = inspect(db.engine)
        table_name = 'user'

        if table_name not in insp.get_table_names():
            logger.warning(f'{table_name} table missing; skipping profile_image check.')
            return

        columns = {c['name']: c for c in insp.get_columns(table_name)}
        if 'profile_image' in columns:
            current_type = str(columns['profile_image']['type']).upper()
            if 'VARCHAR' in current_type:
                try:
                    with db.engine.begin() as conn:
                        conn.execute(text('ALTER TABLE "user" ALTER COLUMN "profile_image" TYPE TEXT'))
                    logger.info('Altered column "profile_image" to TEXT in "user" table.')
                except Exception as exc:
                    logger.warning(f'Failed to alter "profile_image" to TEXT: {exc}.')
        else:
            try:
                with db.engine.begin() as conn:
                    conn.execute(text('ALTER TABLE "user" ADD COLUMN "profile_image" TEXT'))
                logger.info('Added column "profile_image" to "user" table.')
            except Exception as exc:
                logger.warning(f'Failed to add "profile_image" column: {exc}.')

ensure_user_profile_schema()

def ensure_trade_log_schema():
    """Ensure the trade_log table has required columns."""
    logger.info("Ensuring trade_log table schema is up-to-date...")
    with app.app_context():
        insp = inspect(db.engine)
        table_name = 'trade_log'

        # 1. Create table if missing
        if table_name not in insp.get_table_names():
            try:
                db.metadata.create_all(bind=db.engine, tables=[TradeLog.__table__])
                logger.info(f'Created {table_name} table as it was missing.')
            except Exception as exc:
                logger.error(f'Failed to create {table_name} table: {exc}')
                return

        # 2. Add missing columns
        existing_columns = {col['name'] for col in insp.get_columns(table_name)}

        columns_to_add = {
            'broker': 'VARCHAR(50)',
            'client_id': 'VARCHAR(50)',
            'order_id': 'VARCHAR(50)',
            'price': 'FLOAT',
            'error_code': 'VARCHAR(50)',
        }

        for col_name, col_def in columns_to_add.items():
            if col_name not in existing_columns:
                try:
                    with db.engine.begin() as conn:
                        conn.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {col_def}'))
                    logger.info(f'Added missing column "{col_name}" to "{table_name}" table.')
                except Exception as exc:
                    logger.warning(
                        f'Failed to add column "{col_name}" to "{table_name}": {exc}. Manual migration might be needed.'
                    )
            else:
                logger.debug(f'Column "{col_name}" already exists in "{table_name}".')

        logger.info(f"Schema check for {table_name} completed.")

ensure_trade_log_schema()

def ensure_strategy_schema():
    """Ensure the strategy table exists with required columns."""
    logger.info("Ensuring strategy table schema is up-to-date...")
    with app.app_context():
        insp = inspect(db.engine)
        table_name = 'strategy'

        if table_name not in insp.get_table_names():
            try:
                db.metadata.create_all(bind=db.engine, tables=[Strategy.__table__])
                logger.info(f'Created {table_name} table as it was missing.')
            except Exception as exc:
                logger.error(f'Failed to create {table_name} table: {exc}')
                return

        existing_columns = {col['name'] for col in insp.get_columns(table_name)}

        columns_to_add = {
            'user_id': 'INTEGER NOT NULL',
            'account_id': 'INTEGER',
            'name': 'VARCHAR(120) NOT NULL',
            'description': 'TEXT',
            'asset_class': 'VARCHAR(50) NOT NULL',
            'style': 'VARCHAR(50) NOT NULL',
            'allow_auto_submit': 'BOOLEAN',
            'allow_live_trading': 'BOOLEAN',
            'allow_any_ticker': 'BOOLEAN',
            'allowed_tickers': 'TEXT',
            'notification_emails': 'TEXT',
            'notify_failures_only': 'BOOLEAN',
            'created_at': 'TIMESTAMP WITHOUT TIME ZONE',
            'is_active': 'BOOLEAN',
            'last_run_at': 'TIMESTAMP WITHOUT TIME ZONE',
            'signal_source': 'VARCHAR(100)',
            'risk_max_positions': 'INTEGER',
            'risk_max_allocation': 'FLOAT',
            'schedule': 'VARCHAR(120)',
            'webhook_secret': 'VARCHAR(120)',
            'track_performance': 'BOOLEAN',
            'log_retention_days': 'INTEGER',
            'is_public': 'BOOLEAN',
            'icon': 'TEXT',
            'brokers': 'TEXT',
            'master_accounts': 'TEXT',
        }

        for col_name, col_def in columns_to_add.items():
            if col_name not in existing_columns:
                try:
                    with db.engine.begin() as conn:
                        conn.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {col_def}'))
                    logger.info(f'Added missing column "{col_name}" to "{table_name}" table.')
                except Exception as exc:
                    logger.warning(
                        f'Failed to add column "{col_name}" to "{table_name}": {exc}.')
            else:
                logger.debug(f'Column "{col_name}" already exists in "{table_name}" table.')

        logger.info(f"Schema check for {table_name} completed.")

ensure_strategy_schema()


def ensure_strategy_subscription_schema():
    """Ensure the strategy_subscription table exists with required columns."""
    logger.info("Ensuring strategy_subscription table schema is up-to-date...")
    with app.app_context():
        insp = inspect(db.engine)
        table_name = 'strategy_subscription'

        if table_name not in insp.get_table_names():
            try:
                db.metadata.create_all(bind=db.engine, tables=[StrategySubscription.__table__])
                logger.info(f'Created {table_name} table as it was missing.')
            except Exception as exc:
                logger.error(f'Failed to create {table_name} table: {exc}')
                return

        existing_columns = {col['name'] for col in insp.get_columns(table_name)}

        columns_to_add = {
            'strategy_id': 'INTEGER',
            'subscriber_id': 'INTEGER',
            'account_id': 'INTEGER',
            'auto_submit': 'BOOLEAN',
            'order_type': "VARCHAR(20)",
            'qty_mode': "VARCHAR(20)",
            'fixed_qty': 'INTEGER',
            'approved': 'BOOLEAN',
            'created_at': 'TIMESTAMP WITHOUT TIME ZONE',
        }

        for col_name, col_def in columns_to_add.items():
            if col_name not in existing_columns:
                try:
                    with db.engine.begin() as conn:
                        conn.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {col_def}'))
                    logger.info(f'Added missing column "{col_name}" to "{table_name}" table.')
                except Exception as exc:
                    logger.warning(
                        f'Failed to add column "{col_name}" to "{table_name}": {exc}. Manual migration might be needed.'
                    )
            else:
                logger.debug(f'Column "{col_name}" already exists in "{table_name}" table.')

        logger.info(f"Schema check for {table_name} completed.")

ensure_strategy_subscription_schema()

def ensure_strategy_log_schema():
    """Ensure the strategy_log table exists with required columns."""
    logger.info("Ensuring strategy_log table schema is up-to-date...")
    with app.app_context():
        insp = inspect(db.engine)
        table_name = 'strategy_log'

        if table_name not in insp.get_table_names():
            try:
                db.metadata.create_all(bind=db.engine, tables=[StrategyLog.__table__])
                logger.info(f'Created {table_name} table as it was missing.')
            except Exception as exc:
                logger.error(f'Failed to create {table_name} table: {exc}')
                return

        existing_columns = {col['name'] for col in insp.get_columns(table_name)}

        columns_to_add = {
            'strategy_id': 'INTEGER',
            'timestamp': 'TIMESTAMP WITHOUT TIME ZONE',
            'level': 'VARCHAR(20)',
            'message': 'TEXT',
            'performance': 'JSONB',
        }

        for col_name, col_def in columns_to_add.items():
            if col_name not in existing_columns:
                try:
                    with db.engine.begin() as conn:
                        conn.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {col_def}'))
                    logger.info(f'Added missing column "{col_name}" to "{table_name}" table.')
                except Exception as exc:
                    logger.warning(
                        f'Failed to add column "{col_name}" to "{table_name}": {exc}. Manual migration might be needed.'
                    )
            else:
                logger.debug(f'Column "{col_name}" already exists in "{table_name}" table.')

        logger.info(f"Schema check for {table_name} completed.")


ensure_strategy_log_schema()


def map_order_type(order_type: str, broker: str) -> str:
    """Convert generic order types to broker specific codes."""
    if not order_type:
        return ""
    broker = broker.lower() if broker else ""
    if broker in ("aliceblue", "finvasia") and order_type.upper() == "MARKET":
        return "MKT"
    return str(order_type)

def build_order_params(broker_name: str, mapping: dict, symbol: str, action: str, qty: int, order_type: str, api) -> dict:
    """Return broker specific order parameters."""
    order_type_mapped = map_order_type(order_type, broker_name)
    if broker_name == "dhan":
        security_id = mapping.get("security_id")
        if not security_id:
            raise ValueError(f"Symbol not found in mapping: {symbol}")
        return {
            "tradingsymbol": symbol,
            "security_id": security_id,
            "exchange_segment": api.NSE,
            "transaction_type": api.BUY if action.upper() == "BUY" else api.SELL,
            "quantity": int(qty),
            "order_type": order_type_mapped,
            "product_type": api.INTRA,
            "price": 0,
        }
    elif broker_name == "zerodha":
        tradingsymbol = mapping.get("tradingsymbol", symbol)
        return {
            "tradingsymbol": tradingsymbol,
            "exchange": "NSE",
            "transaction_type": action.upper(),
            "quantity": int(qty),
            "order_type": order_type_mapped,
            "product": "MIS",
            "price": None,
        }
    elif broker_name == "fyers":
        fy_symbol = mapping.get("symbol") or symbol
        exch = mapping.get("exchange", "NSE")
        return {
            "tradingsymbol": fy_symbol.split(":")[-1],
            "exchange": exch,
            "transaction_type": action.upper(),
            "quantity": int(qty),
            "order_type": order_type_mapped,
            "product": "INTRADAY",
            "price": None,
        }
    elif broker_name == "aliceblue":
        sym_id = mapping.get("symbol_id")
        if not sym_id:
            raise ValueError(f"Symbol not found in mapping: {symbol}")
        tradingsymbol = mapping.get("trading_symbol", symbol)
        exch = mapping.get("exch", "NSE")
        return {
            "tradingsymbol": tradingsymbol,
            "symbol_id": sym_id,
            "exchange": exch,
            "transaction_type": action.upper(),
            "quantity": int(qty),
            "order_type": order_type_mapped,
            "product": "MIS",
            "price": 0,
        }
    elif broker_name == "finvasia":
        tradingsymbol = mapping.get("symbol") or mapping.get("trading_symbol") or symbol
        exch = mapping.get("exchange", "NSE")
        return {
            "tradingsymbol": tradingsymbol,
            "exchange": exch,
            "transaction_type": action.upper(),
            "quantity": int(qty),
            "order_type": order_type_mapped,
            "product": "MIS",
            "price": 0,
        }
    else:
        tradingsymbol = mapping.get("trading_symbol") or mapping.get("symbol") or symbol
        exch = mapping.get("exchange", "NSE")
        return {
            "tradingsymbol": tradingsymbol,
            "exchange": exch,
            "transaction_type": action.upper(),
            "quantity": int(qty),
            "order_type": order_type_mapped,
            "product": "MIS",
            "price": None,
        }

def execute_for_subscriptions(strategy: Strategy, symbol: str, action: str, quantity: int):
    """Execute orders for all approved subscriptions of a strategy."""
    results = []
    subs = StrategySubscription.query.filter_by(strategy_id=strategy.id, approved=True).all()
    allowed = [b.strip().lower() for b in (strategy.brokers or '').split(',') if b.strip()]
    allowed_accounts = [int(a) for a in (strategy.master_accounts or '').split(',') if a.strip()]
    for sub in subs:
        sub_user = sub.subscriber
        account = None
        if sub.account_id:
            account = Account.query.filter_by(id=sub.account_id, user_id=sub_user.id).first()
        if not account:
            account = sub_user.accounts[0] if sub_user.accounts else None
        if not account or not account.credentials:
            results.append({"subscription_id": sub.id, "status": "ERROR", "reason": "Account not configured"})
            continue
        broker_name = (account.broker or "dhan").lower()
        if allowed and broker_name not in allowed:
            results.append({"subscription_id": sub.id, "status": "SKIPPED", "reason": "Broker not allowed"})
            continue
        if allowed and broker_name not in allowed:
            results.append({"subscription_id": sub.id, "status": "SKIPPED", "reason": "Broker not allowed"})
            continue
        try:
            acc_dict = _account_to_dict(account)
            api = broker_api(acc_dict)
            mapping = get_symbol_for_broker(symbol, broker_name)
            qty_use = sub.fixed_qty if sub.qty_mode == "fixed" and sub.fixed_qty else quantity
            order_params = build_order_params(broker_name, mapping, symbol, action, qty_use, sub.order_type or "MARKET", api)
        except Exception as e:
            results.append({"subscription_id": sub.id, "status": "ERROR", "reason": str(e)})
            continue

        if not sub.auto_submit:
            results.append({"subscription_id": sub.id, "status": "PENDING", "reason": "Auto submit disabled"})
            continue

        try:
            response = api.place_order(**order_params)
            if isinstance(response, dict) and response.get("status") == "failure":
                reason = response.get("remarks") or response.get("error_message") or response.get("error") or "Unknown error"
                record_trade(
                    sub_user.id,
                    symbol,
                    action.upper(),
                    qty_use,
                    order_params.get('price'),
                    "FAILED",
                    broker=account.broker,
                    client_id=account.client_id,
                )
                results.append({"subscription_id": sub.id, "status": "FAILED", "reason": reason})
                continue
            record_trade(
                sub_user.id,
                symbol,
                action.upper(),
                qty_use,
                order_params.get('price'),
                "SUCCESS",
                broker=account.broker,
                client_id=account.client_id,
            )
            results.append({"subscription_id": sub.id, "status": "SUCCESS", "order_id": response.get("order_id")})
        except Exception as e:
            results.append({"subscription_id": sub.id, "status": "ERROR", "reason": str(e)})
    return results

def _resolve_data_path(path: str) -> str:
    """Return an absolute path inside ``DATA_DIR`` for relative paths."""
    if os.path.isabs(path) or os.path.dirname(path):
        return path
    return os.path.join(DATA_DIR, path)


def _account_to_dict(acc: Account) -> dict:
    last_error = None
    error_list: list[str] = []
    try:
        logs = (
            SystemLog.query.filter_by(
                user_id=str(acc.user_id),
                level="ERROR",
            )
            .order_by(SystemLog.timestamp.desc())
            .limit(5)
            .all()
        )
        for log in logs:
            details = log.details
            if isinstance(details, str):
                try:
                    details = json.loads(details)
                except Exception:
                    details = {}
            if isinstance(details, dict) and str(details.get("client_id")) == acc.client_id:
                if not last_error:
                    last_error = log.message
                error_list.append(log.message)
    except Exception as e:  # pragma: no cover - logging failures shouldn't break
        # On any failure just ignore - logging should not break API
        last_error = None
        error_list = []
        logger.error(f"Failed to fetch logs for {acc.client_id}: {e}")
        # Ensure the session is usable for subsequent queries
        db.session.rollback()
        
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
        "copy_value_limit": acc.copy_value_limit,
        "copied_value": acc.copied_value,
        "credentials": acc.credentials,
        "last_copied_trade_id": acc.last_copied_trade_id,
        "auto_login": acc.auto_login,
        "last_login": acc.last_login_time,
        "device_number": acc.device_number,
        "last_error": last_error,
        "errors": error_list,        
    }

def get_user_by_token(token: str):
    """Get user by webhook token."""
    return User.query.filter_by(webhook_token=token).first()


def get_accounts_for_user(user_email: str):
    """Get all accounts for a user."""
    user = User.query.filter_by(email=user_email).first()
    if not user:
        return []
    return user.accounts

def get_master_accounts():
    """Get all master accounts with their children for the current user."""
    user = current_user()
    if not user:
        return []
    masters = Account.query.filter_by(role='master', user_id=user.id).all()
    result = []
    for master in masters:
        children = Account.query.filter_by(
            role='child',
            linked_master_id=master.client_id,
            user_id=user.id
        ).all()
        master_dict = _account_to_dict(master)
        master_dict['children'] = [_account_to_dict(child) for child in children]
        result.append(master_dict)
    return result

def get_account_by_client_id(client_id: str):
    """Get account by client ID for the current user."""
    user = current_user()
    if not user:
        return None
    return Account.query.filter_by(client_id=client_id, user_id=user.id).first()


def orphan_children_without_master(user: User) -> None:
    """Reset role for child accounts whose master no longer exists."""
    if not user:
        return
    masters = {
        acc.client_id
        for acc in Account.query.filter_by(user_id=user.id, role="master").all()
    }
    children = Account.query.filter_by(user_id=user.id, role="child").all()
    updated = False
    for child in children:
        if child.linked_master_id not in masters:
            child.role = None
            child.linked_master_id = None
            child.copy_status = "Off"
            child.multiplier = 1.0
            child.copy_value_limit = None
            child.copied_value = 0.0
            child.last_copied_trade_id = None
            updated = True
    if updated:
        db.session.commit()


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
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d %H:%M:%S.%f",
]


def parse_timestamp(value):
    """Parse diverse timestamp formats to a ``datetime`` object."""
    if not value:
        return None
    if isinstance(value, (int, float)):
        try:
            ts = float(value)
            if ts > 1e11:  # assume milliseconds if very large
                ts /= 1000.0
            return datetime.fromtimestamp(ts)
        except Exception:
            return None
    if isinstance(value, str):
        value = value.strip()
        numeric_str = re.fullmatch(r"\d+(?:\.\d+)?", value)
        if numeric_str:
            try:
                ts = float(value)
                if len(value.split(".")[0]) > 10:
                    ts /= 1000.0
                return datetime.fromtimestamp(ts)
            except Exception:
                pass
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except Exception:
            pass
        try:
            return parser.parse(value)
        except Exception:
            pass
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
    # Avoid passing duplicate client_id if stored inside credentials
    rest = {
        k: v
        for k, v in credentials.items()
        if k not in ("access_token", "client_id")
    }

    if broker == "aliceblue":
        api_key = rest.pop("api_key", None)
        return BrokerClass(client_id, api_key, **rest)

    elif broker == "finvasia":
        password = rest.pop("password", None)
        totp_secret = rest.pop("totp_secret", None)
        vendor_code = rest.pop("vendor_code", None)
        api_key = rest.pop("api_key", None)
        imei = rest.pop("imei", None)
        if not imei:
            raise ValueError("IMEI is required for Finvasia accounts")
        # Pass args as named, not positional, for clarity and to match the new class
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
    
    

def get_opening_balance_for_account(acc, cache_only=False):
    """Instantiate broker and try to fetch opening balance with caching.

    Parameters
    ----------
    acc : dict
        Account details containing client_id, broker, etc.
    cache_only : bool, optional
        When True, return the cached value without refreshing it. Defaults
        to ``False``.
    """
    client_id = acc.get("client_id")
    now = time()

    cached = OPENING_BALANCE_CACHE.get(client_id)
    if cached:
        ts, bal = cached
        if cache_only or now - ts < CACHE_TTL:
            return bal
    if cache_only:
        return cached[1] if cached else None

    bal = None
    try:
        api = broker_api(acc)
        if hasattr(api, "get_opening_balance"):
            bal = api.get_opening_balance()
        elif hasattr(api, "get_profile"):
            resp = api.get_profile()
            data = resp.get("data", resp) if isinstance(resp, dict) else resp
            bal = extract_balance(data)
    except Exception as e:
        print(f"Failed to fetch balance for {client_id}: {e}")

    OPENING_BALANCE_CACHE[client_id] = (now, bal)
    return bal

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
    # Return all key/value settings stored in the DB
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
    """Create or update a user's broker account in the database."""
    user = User.query.filter_by(email=owner).first()
    if not user:
        # Create shell user if not present
        user = User(email=owner)
        db.session.add(user)
        db.session.commit()

    client_id = account.get("client_id")
    acc = Account.query.filter_by(user_id=user.id, client_id=client_id).first()

    if acc:
        # Update existing account
        acc.broker = account.get("broker", acc.broker)
        acc.username = account.get("username", acc.username)
        acc.token_expiry = account.get("token_expiry", acc.token_expiry)
        acc.status = account.get("status", acc.status)
        acc.role = account.get("role", acc.role)
        acc.linked_master_id = account.get("linked_master_id", acc.linked_master_id)
        acc.copy_status = account.get("copy_status", acc.copy_status)
        acc.multiplier = account.get("multiplier", acc.multiplier)
        acc.copy_value_limit = account.get("copy_value_limit", acc.copy_value_limit)
        acc.copied_value = account.get("copied_value", acc.copied_value)
        acc.credentials = account.get("credentials", acc.credentials)
        acc.last_copied_trade_id = account.get("last_copied_trade_id", acc.last_copied_trade_id)
        if account.get("auto_login") is not None:
            acc.auto_login = account.get("auto_login")
        last_login = account.get("last_login")
        if last_login:
            try:
                if isinstance(last_login, str):
                    last_login = datetime.fromisoformat(last_login)
            except Exception:
                last_login = None
            acc.last_login_time = last_login or acc.last_login_time
    else:
        acc = Account(
            user_id=user.id,
            broker=account.get("broker"),
            client_id=client_id,
            username=account.get("username"),
            token_expiry=account.get("token_expiry"),
            status=account.get("status", "active"),
            role=account.get("role"),
            linked_master_id=account.get("linked_master_id"),
            copy_status=account.get("copy_status", "Off"),
            multiplier=account.get("multiplier", 1.0),
            copy_value_limit=account.get("copy_value_limit"),
            copied_value=account.get("copied_value", 0.0),
            credentials=account.get("credentials"),
            last_copied_trade_id=account.get("last_copied_trade_id"),
            auto_login=account.get("auto_login", True),
        )
        last_login = account.get("last_login")
        if last_login:
            try:
                if isinstance(last_login, str):
                    last_login = datetime.fromisoformat(last_login)
            except Exception:
                last_login = None
            acc.last_login_time = last_login
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

def get_user_credentials(token: str):
    """Return basic broker credentials for a user webhook token."""
    user = User.query.filter_by(webhook_token=token).first() # Use filter_by(webhook_token=token) directly
    if not user:
        return None
    account = user.accounts[0] if user.accounts else None
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
        resp = requests.get(url, timeout=3)
        return resp.ok
    except Exception:
        return False


def find_account_by_client_id(client_id):
    """Return ``(account, parent_master)`` for the current user."""
    user = current_user()
    acc = Account.query.filter_by(client_id=str(client_id), user_id=user.id).first()
    if not acc:
        return None, None

    if acc.role == "child":
        master = Account.query.filter_by(client_id=acc.linked_master_id, user_id=user.id).first()
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

# === Save logs ===
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



def record_trade(
    user_identifier,
    symbol,
    action,
    qty,
    price,
    status,
    broker=None,
    client_id=None,
):
    """Persist a trade record to the database.

    ``user_identifier`` may be a ``User`` object, user id or email string.
    Optional ``broker`` and ``client_id`` allow associating the trade with a
    specific account.
    """
    if isinstance(user_identifier, User):
        user = user_identifier
    elif isinstance(user_identifier, int):
        user = db.session.get(User, user_identifier)
    elif isinstance(user_identifier, str) and user_identifier.isdigit():
        user = db.session.get(User, int(user_identifier)) or User.query.filter_by(email=user_identifier).first()
    else:
        user = User.query.filter_by(email=user_identifier).first()
    trade = Trade(
        user_id=user.id if user else None,
        symbol=symbol,
        action=action,
        qty=int(qty),
        price=float(price or 0),
        status=status,
        timestamp=datetime.utcnow(),
        broker=broker,
        client_id=client_id,
    )
    db.session.add(trade)
    db.session.commit()

def _find_position_list(obj):
    """Recursively search *obj* for a list of position dicts."""
    def looks_like_position_list(lst):
        if not lst or not all(isinstance(i, dict) for i in lst):
            return False
        keys = {k.lower() for k in lst[0]}
        return any(
            k in keys for k in ("netqty", "net_quantity", "quantity", "tradingsymbol", "symbol")
        )
    visited = set()
    stack = [obj]
    while stack:
        item = stack.pop()
        if id(item) in visited:
            continue
        visited.add(id(item))
        if isinstance(item, dict):
            for key, val in item.items():
                if isinstance(val, list):
                    if "position" in key.lower() and looks_like_position_list(val):
                        return val
                    stack.append(val)
                elif isinstance(val, dict):
                    stack.append(val)
        elif isinstance(item, list):
            if looks_like_position_list(item):
                return item
            stack.extend(item)    
    return []


def exit_all_positions_for_account(account):
    """Square off all open positions for the given account."""
    api = broker_api(_account_to_dict(account))
    try:
        pos_resp = api.get_positions()
        if isinstance(pos_resp, str):
            logger.error(
                "get_positions returned string for %s: %s", account.client_id, pos_resp
            )
            return [{"symbol": None, "status": "ERROR", "message": pos_resp}]
            
        positions = _find_position_list(pos_resp)
        if isinstance(positions, dict):
            positions = [positions]
        elif not isinstance(positions, list):
            positions = []
    except Exception as e:
        logger.error(f"Failed to fetch positions for {account.client_id}: {e}")
        return [{"symbol": None, "status": "ERROR", "message": str(e)}]

    results = []
    for pos in positions:
        normalized = normalize_position(pos, account.broker)
        if normalized:
            pos = {**pos, **normalized}
            net_qty = int(normalized.get("netQty", 0))
        else:
            lower = {k.lower(): v for k, v in pos.items()}
            net_qty = 0
            for key in ("netqty", "net_quantity", "netquantity", "quantity"):
                if key in lower and lower[key] is not None:
                    try:
                        net_qty = int(float(lower[key]))
                        break
                    except (TypeError, ValueError):
                        continue
        if net_qty == 0:
            continue

        symbol = (
            pos.get("tradingSymbol")
            or pos.get("tradingsymbol")
            or pos.get("symbol")
            or pos.get("tsym")
            or pos.get("Tsym")
            or ""
        )
        if not symbol and (
            pos.get("instrument_token")
            or pos.get("instrumentToken")
            or pos.get("token")
        ):
            symbol = get_symbol_by_token(
                pos.get("instrument_token")
                or pos.get("instrumentToken")
                or pos.get("token"),
                account.broker,
            ) or ""

        symbol = str(symbol).upper()
        direction = "SELL" if net_qty > 0 else "BUY"
        qty = abs(net_qty)
        broker_name = (account.broker or "").lower()
        product = extract_product_type(pos) or None
        mapped_product = map_product_for_broker(product, broker_name)

        if broker_name == "dhan":
            order_params = {
                "tradingsymbol": symbol,
                "security_id": pos.get("securityId") or pos.get("security_id"),
                "exchange_segment": pos.get("exchangeSegment")
                or pos.get("exchange_segment")
                or "NSE_EQ",
                "transaction_type": direction,
                "quantity": qty,
                "order_type": "MARKET",
                "product_type": mapped_product or "INTRADAY",
                "price": 0,
            }
        elif broker_name == "aliceblue":
            order_params = {
                "tradingsymbol": symbol,
                "symbol_id": pos.get("securityId") or pos.get("security_id"),
                "exchange": "NSE",
                "transaction_type": direction,
                "quantity": qty,
                "order_type": "MKT",
                "product": mapped_product or "MIS",
                "price": 0,
            }
        elif broker_name == "finvasia":
            order_params = {
                "tradingsymbol": symbol,
                "exchange": "NSE",
                "transaction_type": direction,
                "quantity": qty,
                "order_type": "MKT",
                "product": mapped_product or "MIS",
                "price": 0,
                "token": pos.get("token", ""),
            }
        elif broker_name == "zerodha":
            order_params = {
                "tradingsymbol": symbol,
                "exchange": "NSE",
                "transaction_type": direction,
                "quantity": qty,
                "order_type": "MARKET",
                "product": mapped_product or "MIS",
                "price": 0,
            }
        else:
            order_params = {
                "tradingsymbol": symbol,
                "exchange": "NSE",
                "transaction_type": direction,
                "quantity": qty,
                "order_type": "MARKET",
                "product": mapped_product or "MIS",
                "price": 0,
            }

        try:
            resp = api.place_order(**order_params)
            if isinstance(resp, dict) and resp.get("status") == "failure":
                msg = clean_response_message(resp)
                status = "FAILED"
                results.append({"symbol": symbol, "status": status, "message": msg})
                save_log(account.client_id, symbol, "SQUARE_OFF", qty, status, msg)
            else:
                status = "SUCCESS"
                results.append({"symbol": symbol, "status": status})
                save_log(account.client_id, symbol, "SQUARE_OFF", qty, status, str(resp))
        except Exception as e:
            status = "ERROR"
            results.append({"symbol": symbol, "status": status, "message": str(e)})
            save_log(account.client_id, symbol, "SQUARE_OFF", qty, status, str(e))

    if not results:
        results.append({"symbol": None, "status": "NO_POSITIONS"})
    return results

# Authentication decorator
def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("user"):
            return redirect(url_for("auth.login"))
        return view(*args, **kwargs)
    return wrapped

# Admin authentication
ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD")
if not ADMIN_EMAIL or not ADMIN_PASSWORD:
    raise RuntimeError("ADMIN_EMAIL and ADMIN_PASSWORD must be set")
    
def admin_login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("admin"):
            return redirect(url_for("admin_login"))
        return view(*args, **kwargs)
    return wrapped
    
import os
import json
import logging
from logging.handlers import RotatingFileHandler

# Setup logging at the top of your main file or app entrypoint
log_path = os.path.join(DATA_DIR, 'app.log')
log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
file_handler = RotatingFileHandler(log_path, maxBytes=1_000_000, backupCount=10)
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        file_handler
    ]
)
logger = logging.getLogger(__name__)

def poll_and_copy_trades():
    """Run trade copying logic with full database storage - no JSON files."""
    try:
        with app.app_context():
            logger.info("Starting poll_and_copy_trades cycle")

            # Load master accounts scoped by user to avoid cross-tenant leakage
            users = User.query.all()
            masters_by_user = []
            for user in users:
                user_masters = Account.query.filter_by(user_id=user.id, role="master").all()
                if not user_masters:
                    continue
                logger.debug(f"Processing {len(user_masters)} masters for user {user.email}")
                masters_by_user.extend(user_masters)

            if not masters_by_user:
                logger.warning("No master accounts configured")
                return

            logger.info(f"Found {len(masters_by_user)} master accounts to process")

            for master in masters_by_user:
                master_id = master.client_id
                if not master_id:
                    logger.error("Master account missing client_id, skipping...")
                    continue

                master_broker = (master.broker or "Unknown").lower()
                credentials = master.credentials or {}
                
                if not credentials:
                    logger.error(
                        f"No credentials found for master {master_id}, skipping..."
                    )
                    continue

                # Initialize master broker API
                try:
                    BrokerClass = get_broker_class(master_broker)
                    if master_broker == "aliceblue":
                        api_key = credentials.get("api_key")
                        if not api_key:
                            logger.error(f"Missing API key for AliceBlue master {master_id}")
                            continue
                        rest = {
                            k: v
                            for k, v in credentials.items()
                            if k
                            not in (
                                "access_token",
                                "api_key",
                                "device_number",
                                "client_id",
                            )
                        }
                        master_api = BrokerClass(
                            master.client_id,
                            api_key,
                            device_number=credentials.get("device_number") or master.device_number,
                            **rest
                        )
                        clear_init_error_logs(master, is_master=True)
                    elif master_broker == "finvasia":
                        required = ["password", "totp_secret", "vendor_code", "api_key"]
                        if not all(credentials.get(r) for r in required):
                            logger.error(f"Missing credentials for finvasia master {master_id}")
                            continue
                        imei = credentials.get("imei")
                        if not imei:
                            logger.error("Missing IMEI for finvasia master %s", master_id)
                            continue
                        master_api = BrokerClass(
                            client_id=master.client_id,
                            password=credentials["password"],
                            totp_secret=credentials["totp_secret"],
                            vendor_code=credentials["vendor_code"],
                            api_key=credentials["api_key"],
                            imei=imei
                        )
                        clear_init_error_logs(master, is_master=True)
                    else:
                        access_token = credentials.get("access_token")
                        if not access_token:
                            logger.error(f"Missing access token for {master_broker} master {master_id}")
                            continue
                        rest = {
                            k: v
                            for k, v in credentials.items()
                            if k not in ("access_token", "client_id")
                        }
                        master_api = BrokerClass(
                            client_id=master.client_id,
                            access_token=access_token,
                            **rest
                        )
                        clear_init_error_logs(master, is_master=True)
                except Exception as e:
                    err = f"Failed to initialize master API ({master_broker}) for {master_id}: {str(e)}"
                    err_lower = str(e).lower()    
                    skip_error = (
                        master_broker.lower() == "dhan"
                        and (
                            "invalid syntax" in err_lower
                            or "failed to load broker 'dhan'" in err_lower
                        )
                    )
                    if skip_error:
                        logger.warning(err)
                        master.status = "Connected"
                        if master.copy_status == "Off":
                            master.copy_status = "On"
                        try:
                            db.session.commit()
                        except Exception:
                            db.session.rollback()
                            logger.error("Failed to commit status update")
                        try:
                            logs = (
                                SystemLog.query.filter(
                                    SystemLog.user_id == master.user_id,
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
                                    and str(details.get("client_id"))
                                    == master.client_id
                                ):
                                    db.session.delete(log)
                            db.session.commit()
                        except Exception as e2:
                            db.session.rollback()
                            logger.error(
                                f"Failed to clear Dhan init error logs for {master_id}: {e2}"
                            )
                    else:
                        logger.error(err)
                        log_connection_error(master, err, disable_children=True)    
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
                    err = f"Failed to fetch orders for master {master_id}: {str(e)}"
                    logger.error(err)
                    log_connection_error(master, err, disable_children=True)
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
                # Added try-except around database access to catch connection issues
                try:
                    children = active_children_for_master(master)
                except Exception as e:
                    logger.error(f"Failed to fetch active children for master {master_id} from DB: {str(e)}")
                    # Continue to next master or exit if this is a critical error
                    continue


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
                                or first.get("norenordno")  # Finvasia
                            )
                            if init_id:
                                try:
                                    child.last_copied_trade_id = str(init_id)
                                    db.session.commit()
                                    logger.info(f"Initialized marker for child {child_id} to {init_id}")
                                except Exception as e:
                                    logger.error(f"Failed to commit initial marker for child {child_id}: {e}")
                                    db.session.rollback()
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
                            or order.get("tradingsymbol")  # Zerodha uses lowercase key
                            or order.get("symbol")
                            or order.get("stock")
                            or order.get("scripCode")
                            or order.get("Tsym")  # AliceBlue trade book
                            or order.get("tsym")
                        )
                        if not symbol:
                            token_val = (
                                order.get("instrument_token")
                                or order.get("instrumentToken")
                                or order.get("token")
                            )
                            if token_val:
                                symbol = get_symbol_by_token(token_val, master_broker)
                        if not symbol:
                            continue

                        master_product = extract_product_type(order)

                        # Determine quantity based on value limit or multiplier
                        try:
                            value_limit = child.copy_value_limit
                            copied_total = float(child.copied_value or 0)
                            if value_limit is not None:
                                remaining = float(value_limit) - copied_total
                                if remaining < price:
                                    child.copy_status = "Off"
                                    db.session.commit()
                                    logger.info(
                                        f"[{master_id}->{child_id}] Value limit reached")
                                    break
                                qty_limit = int(remaining // price)
                                if qty_limit <= 0:
                                    child.copy_status = "Off"
                                    db.session.commit()
                                    logger.info(
                                        f"[{master_id}->{child_id}] Value limit reached")
                                    break
                                copied_qty = min(int(filled_qty), qty_limit)
                            else:
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
                            child_credentials = child.credentials or {}
                            ChildBrokerClass = get_broker_class(child_broker)
                            
                            if child_broker == "aliceblue":
                                api_key = child_credentials.get("api_key")
                                if not api_key:
                                    logger.warning(f"Missing API key for AliceBlue child {child_id}")
                                    continue
                                device_number = child_credentials.get("device_number") or child.device_number
                                rest_child = {
                                    k: v
                                    for k, v in child_credentials.items()
                                    if k
                                    not in (
                                        "access_token",
                                        "api_key",
                                        "device_number",
                                        "client_id",
                                    )
                                }
                                child_api = ChildBrokerClass(
                                    child.client_id,
                                    api_key,
                                    device_number=device_number,
                                    **rest_child
                                )
                                clear_init_error_logs(child, is_master=False)
                            elif child_broker == "finvasia":
                                required = ['password', 'totp_secret', 'vendor_code', 'api_key']
                                if not all(child_credentials.get(r) for r in required):
                                    logger.warning(f"Missing credentials for Finvasia child {child_id}")
                                    continue
                                imei = child_credentials.get('imei')
                                if not imei:
                                    logger.warning("Missing IMEI for Finvasia child %s", child_id)
                                    continue
                                child_api = ChildBrokerClass(
                                    client_id=child.client_id,
                                    password=child_credentials['password'],
                                    totp_secret=child_credentials['totp_secret'],
                                    vendor_code=child_credentials['vendor_code'],
                                    api_key=child_credentials['api_key'],
                                    imei=imei
                                )
                                clear_init_error_logs(child, is_master=False)
                            else:
                                access_token = child_credentials.get("access_token")
                                if not access_token:
                                    logger.warning(f"Missing access token for {child_broker} child {child_id}")
                                    continue
                                rest_child = {
                                    k: v
                                    for k, v in child_credentials.items()
                                    if k not in ("access_token", "client_id")
                                }
                                child_api = ChildBrokerClass(
                                    client_id=child.client_id,
                                    access_token=access_token,
                                    **rest_child
                                )
                                clear_init_error_logs(child, is_master=False)
                        except Exception as e:
                            err = f"Failed to initialize child API for {child_id}: {e}"
                            logger.error(err)
                            log_connection_error(child, err)
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
                                    "product_type": map_product_for_broker(master_product, child_broker)
                                    or getattr(child_api, "INTRA", "INTRADAY"),
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
                                    "product": map_product_for_broker(master_product, child_broker) or "MIS",
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
                                    "product": map_product_for_broker(master_product, child_broker) or "MIS",
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
                                    "product": map_product_for_broker(master_product, child_broker) or "MIS",
                                    "price": price or 0
                                }

                            # Place the order
                            logger.info(f"Placing {transaction_type} order for {copied_qty} {symbol} on {child_broker} for child {child_id}")
                            response = child_api.place_order(**order_params)

                        except Exception as e:
                            err = f"Failed to place order for child {child_id}: {e}"
                            logger.error(err)
                            log_connection_error(child, err)
                            continue

                        # Handle order response and record trade
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
                                    
                                    
                                    # Log the failure
                                    save_log(
                                        child_id,
                                        symbol,
                                        transaction_type,
                                        copied_qty,
                                        "FAILED",
                                        error_msg
                                    )
                                    log_connection_error(child, f"Order failed: {error_msg}")    
                                    record_trade(
                                        user_email,
                                        symbol,
                                        transaction_type,
                                        copied_qty,
                                        price,
                                        'FAILED',
                                        broker=child_broker,
                                        client_id=child_id,
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
                                        user_email,
                                        symbol,
                                        transaction_type,
                                        copied_qty,
                                        price,
                                        'SUCCESS',
                                        broker=child_broker,
                                        client_id=child_id,
                                    )
                                    if child.copy_value_limit is not None:
                                        try:
                                            child.copied_value = float(child.copied_value or 0) + (copied_qty * price)
                                            if child.copied_value >= child.copy_value_limit:
                                                child.copy_status = 'Off'
                                        except Exception:
                                            pass
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

            logger.info("Poll and copy trades cycle completed")
            
    except Exception as e:
        logger.error(f"Failed to start scheduler: {str(e)}")
        # This catch-all should ideally not happen if internal exceptions are caught more specifically.
        # However, for a high-level scheduler, it ensures the job doesn't completely die silently.

def start_scheduler():
    """Start a background scheduler for ``poll_and_copy_trades``."""
    if app.config.get("TESTING") or os.environ.get("RUN_SCHEDULER", "1") == "0":
        logger.info("Scheduler disabled")
        return

    global _scheduler
    if _scheduler is not None and _scheduler.running:
        return
    try:
        _scheduler = BackgroundScheduler(timezone="UTC", daemon=True)

        def _job():
            with app.app_context():
                poll_and_copy_trades()

        _scheduler.add_job(
            _job,
            "interval",
            seconds=10,
            id="poll_trades",
            replace_existing=True,
        )
        _scheduler.start()
        logger.info("Scheduler started")
    except Exception as e:
        logger.error(f"Failed to start scheduler: {e}")
        _scheduler = None




@app.route("/connect-zerodha", methods=["POST"])
@login_required
def connect_zerodha():
    """Connect a Zerodha account using provided credentials.
    
    Expected JSON payload:
    {
        "client_id": "string",
        "api_key": "string",
        "api_secret": "string",
        "request_token": "string"
    }
    
    Returns:
        JSON response with access token or error message
    """
    try:
        # Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        required_fields = ["client_id", "api_key", "api_secret", "request_token"]
        missing_fields = [field for field in required_fields if not data.get(field)]
        
        if missing_fields:
            return jsonify({
                "error": f"Missing required fields: {', '.join(missing_fields)}"
            }), 400
            
        # Initialize broker with credentials
        try:
            broker = ZerodhaBroker(
                client_id=data["client_id"],
                api_key=data["api_key"],
                api_secret=data["api_secret"],
                request_token=data["request_token"]
            )
        except Exception as e:
            logger.error(f"Failed to initialize Zerodha broker: {str(e)}")
            return jsonify({
                "error": "Failed to initialize broker connection",
                "details": str(e)
            }), 500
            
        # Validate access token
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
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500

@app.route('/api/order-book/<client_id>', methods=['GET'])
@login_required
def get_order_book(client_id):
    """Get order book for a master account - Complete Database Version.
    
    Args:
        client_id (str): Client ID of the master account
        
    Returns:
        JSON response with formatted order list or error message
    """
    logger.info(f"Fetching order book for client {client_id}")
    
    try:
        user = current_user()
        account = Account.query.filter_by(
            client_id=client_id,
            user_id=user.id
        ).first()

        if not account:
            logger.warning(f"Account not found for client {client_id}")
            return jsonify({
               "error": "Account not found"
            }), 404

        # ✅ STEP 2: Convert database model to dict for broker_api compatibility
        account_data = _account_to_dict(account)
        logger.info(f"Found account: {account_data['broker']} - {account_data['username']}")

        # ✅ STEP 3: Initialize broker API
        try:
            api = broker_api(account_data)
            logger.debug(f"Initialized {account_data['broker']} API for {client_id}")
        except Exception as e:
            logger.error(f"Failed to initialize broker API: {str(e)}")
            return jsonify({
                "error": "Failed to initialize broker connection",
                "details": str(e)
            }), 500

        # ✅ STEP 4: Fetch orders from broker
        try:
            broker_name = account_data.get('broker', '').lower()

            # Use appropriate method based on broker
            if broker_name == "aliceblue" and hasattr(api, "get_trade_book"):
                logger.debug("Using AliceBlue trade book")
                orders_resp = api.get_trade_book()
                if isinstance(orders_resp, dict) and orders_resp.get("status") == "failure":
                    logger.warning(
                        "Trade book failed for AliceBlue, falling back to order list"
                    )
                    orders_resp = api.get_order_list()
            else:
                logger.debug("Using standard order list")
                orders_resp = api.get_order_list()

            # Check for API errors
            if isinstance(orders_resp, dict) and orders_resp.get("status") == "failure":
                error_msg = orders_resp.get("error", "Failed to fetch orders")
                logger.error(f"API error: {error_msg}")
                return jsonify({"error": error_msg}), 500
                
            # Parse the response
            orders = parse_order_list(orders_resp)
            logger.info(f"Fetched {len(orders)} raw orders from {broker_name}")
            
        except Exception as e:
            logger.error(f"Failed to fetch orders: {str(e)}")
            return jsonify({
                "error": "Failed to fetch orders",
                "details": str(e)
            }), 500
            
        # ✅ STEP 5: Clean and sanitize data
        orders = strip_emojis_from_obj(orders)
        
        if not isinstance(orders, list):
            logger.warning(f"Invalid orders format: {type(orders)}")
            orders = []
        
        # ✅ STEP 6: Format orders for frontend
        formatted = []
        for order in orders:
            if not isinstance(order, dict):
                logger.warning(f"Skipping invalid order format: {type(order)}")
                continue
                
            try:
                # Extract order ID with multiple fallbacks
                order_id = (
                    order.get("orderId")
                    or order.get("order_id") 
                    or order.get("id")
                    or order.get("orderNumber")
                    or order.get("NOrdNo")
                    or order.get("Nstordno")
                    or order.get("nestOrderNumber")
                    or order.get("ExchOrdID")
                    or order.get("norenordno")  # Finvasia
                    or "N/A"
                )

                # Extract transaction side
                side = (
                    order.get("transactionType")
                    or order.get("transaction_type")
                    or order.get("side")
                    or order.get("Trantype")
                    or order.get("tran_side")
                    or order.get("buyOrSell")
                    or order.get("bs")
                    or "N/A"
                )
                
                # Extract status with fallbacks
                status_raw = (
                    order.get("orderStatus")
                    or order.get("report_type")
                    or order.get("status")
                    or order.get("Status")
                    or ("FILLED" if order.get("tradedQty") else "PENDING")
                )
                
                # Normalize status
                status = str(status_raw).upper() if status_raw else "UNKNOWN"
                
                # Extract symbol with fallbacks
                symbol = (
                    order.get("tradingSymbol")
                    or order.get("tradingsymbol")  # Zerodha uses lowercase key
                    or order.get("symbol")
                    or order.get("Tsym")
                    or order.get("tsym")
                    or order.get("Trsym")
                    or "—"
                )
                if symbol == "—":
                    token_val = (
                        order.get("instrument_token")
                        or order.get("instrumentToken")
                        or order.get("token")
                    )
                    if token_val:
                        sym_by_token = get_symbol_by_token(token_val, broker_name)
                        if sym_by_token:
                            symbol = sym_by_token

                # Extract product type
                product_type = (
                    order.get("productType")
                    or order.get("product") 
                    or order.get("Pcode") 
                    or order.get("prd")
                    or "—"
                )

                # Extract quantities with validation
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
                        or order.get("Filledqty")     # AliceBlue
                        or order.get("Fillshares")    # AliceBlue
                        or order.get("fillshares")    # Finvasia
                        or order.get("tradedQty")
                        or order.get("executedQty")
                        or (placed_qty if status in ["FILLED", "COMPLETE", "TRADED"] else 0)
                    )
                except (TypeError, ValueError):
                    filled_qty = 0

                # Extract average price
                try:
                    avg_price = float(
                        order.get("averagePrice")
                        or order.get("avg_price")
                        or order.get("Avgprc")        # AliceBlue
                        or order.get("avgprc")        # Finvasia
                        or order.get("Prc")
                        or order.get("tradePrice")
                        or order.get("tradedPrice")
                        or order.get("executedPrice")
                        or 0
                    )
                except (TypeError, ValueError):
                    avg_price = 0.0

                # Extract and format order time
                order_time_raw = (
                    order.get("orderTimestamp")
                    or order.get("order_timestamp")
                    or order.get("order_time")
                    or order.get("create_time")
                    or order.get("createTime")
                    or order.get("orderDateTime")
                    or order.get("ExchConfrmtime")  # AliceBlue
                    or order.get("norentm")         # Finvasia
                    or order.get("exchtime")        # Finvasia
                    or order.get("ft")              # Finvasia/AliceBlue
                    or ""
                )
                
                parsed_dt = parse_timestamp(order_time_raw)
                if parsed_dt:
                   order_time = parsed_dt.isoformat(timespec="seconds")
                else:
                    try:
                        order_time = datetime.fromisoformat(str(order_time_raw)).isoformat(timespec="seconds")
                    except Exception:
                        order_time = (
                            str(order_time_raw).replace("T", " ").split(".")[0]
                            if order_time_raw
                            else None
                        )

                # Extract remarks
                remarks = (
                    order.get("remarks")
                    or order.get("Remark")
                    or order.get("orderTag")
                    or order.get("usercomment")
                    or order.get("Usercomments")
                    or order.get("remarks1")
                )
                
                if status.startswith("REJECT"):
                    remarks = order.get("rejreason") or remarks or "Order rejected"
                elif status in ["TRADED", "FILLED", "COMPLETE", "SUCCESS"]:
                    remarks = remarks or "Trade successful"
                else:
                    remarks = remarks or "—"

                # ✅ Create formatted order entry
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
                    "remarks": str(remarks)[:100]  # Limit remarks length
                }
                
                formatted.append(formatted_order)
                
            except Exception as e:
                logger.error(f"Error formatting order {order.get('orderId', 'unknown')}: {str(e)}")
                continue

        # ✅ STEP 7: Sort orders by time (newest first)
        try:
            formatted.sort(key=lambda x: x.get('order_time', ''), reverse=True)
        except Exception as e:
            logger.warning(f"Failed to sort orders: {str(e)}")

        logger.info(f"Successfully formatted {len(formatted)} orders for client {client_id}")
        
        # ✅ STEP 8: Return formatted response
        return jsonify(formatted), 200

    except Exception as e:
        logger.error(f"Unexpected error in get_order_book: {str(e)}")
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500

@app.route("/zerodha_redirects/<client_id>")
def zerodha_redirect_handler(client_id):
    """Handle OAuth redirect from Zerodha login flow.
    
    Args:
        client_id (str): Client ID from the redirect URL
        
    Returns:
        Redirect to account page on success, or error page on failure
    """
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

        # Load pending authentication data from the database
        try:
            pending = get_pending_zerodha()
        except Exception as e:
            logger.error(f"Failed to load pending auth data: {str(e)}")
            return render_template(
                "error.html",
                error="Authentication Error",
                message="Failed to load authentication data."
            ), 500

        # Validate pending credentials
        cred = pending.pop(client_id, None)
        if not cred:
            logger.error(f"No pending auth found for client {client_id}")
            return render_template(
                "error.html",
                error="Invalid Request",
                message="No pending authentication found for this client."
            ), 400

        # Extract and validate required credentials
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
            # Initialize Kite Connect
            from kiteconnect import KiteConnect
            kite = KiteConnect(api_key=api_key)
            
            # Generate session
            session_data = kite.generate_session(request_token, api_secret)
            if not session_data or "access_token" not in session_data:
                raise ValueError("Failed to generate valid session")
                
            access_token = session_data["access_token"]

            cred_fields = {
                k: v
                for k, v in cred.items()
                if k not in {"owner", "username"}
            }
            cred_fields.update(
                {
                    "access_token": access_token,
                    "api_key": api_key,
                    "api_secret": api_secret,
                }
            )
            account = {
                "broker": "zerodha",
                "client_id": client_id,
                "username": username,
                "credentials": cred_fields,
                "status": "Connected",
                "auto_login": True,
                "last_login": datetime.now().isoformat(),
                "role": None,
                "linked_master_id": None,
                "multiplier": 1,
                "copy_status": "Off",
            }

            # Save account data
            try:
                save_account_to_user(cred.get("owner", username), account)

                # Clear any previous connection error logs for this account
                
                user_obj = User.query.filter_by(email=cred.get("owner", username)).first()
                if user_obj:
                    acc_obj = Account.query.filter_by(user_id=user_obj.id, client_id=client_id).first()
                    if acc_obj:
                        clear_connection_error_logs(acc_obj)

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
def webhook(user_id):
    """Handle incoming webhook requests for order placement.
    
    Args:
        user_id (str): User ID to identify the account
        
    Expected POST data examples:
        # using "symbol"
        {"symbol": "NSE:SBIN", "action": "BUY", "quantity": 10, "secret": "..."}
        # using "ticker"
        {"ticker": "NSE:SBIN", "action": "BUY", "quantity": 10, "secret": "..."}
        
    Returns:
        JSON response with order status or error message
    """
    logger.info(f"Received webhook request for user {user_id}")
    
    try:
        # Parse and validate request data
        try:
            data = request.get_json(force=True)
        except Exception as e:
            logger.error(f"Failed to parse JSON data: {str(e)}")
            return jsonify({
                "error": "Invalid JSON data",
                "details": str(e)
            }), 400

        # Handle alert messages
        if isinstance(data, str) or "message" in data:
            message = data if isinstance(data, str) else data.get("message")
            logger.info(f"Alert received for {user_id}: {message}")
            return jsonify({
                "status": "Alert logged",
                "message": message
            }), 200

        secret = (
            request.headers.get("X-Webhook-Secret")
            or (data.get("secret") if isinstance(data, dict) else None)
            or request.args.get("secret")
        )
        user_obj = get_user_by_token(user_id)
        if not user_obj:
            logger.error(f"Invalid webhook token: {user_id}")
            return jsonify({"error": "Invalid webhook ID"}), 403
        strategy = None
        if secret:
            strategy = Strategy.query.filter_by(
                user_id=user_obj.id, webhook_secret=secret
            ).first()
            if not strategy:
                logger.error("Webhook secret mismatch")
                return jsonify({"error": "Invalid webhook secret"}), 403
            if not strategy.is_active:
                logger.error("Strategy inactive")
                return jsonify({"error": "Strategy inactive"}), 403
        else:
            if (
                Strategy.query.filter(
                    Strategy.user_id == user_obj.id,
                    Strategy.webhook_secret.isnot(None),
                ).count()
                > 0
            ):
                logger.error("Webhook secret required but missing")
                return jsonify({"error": "Webhook secret required"}), 403

        # Validate required fields
        symbols = []
        base_symbol = data.get("symbol") or data.get("ticker")
        trading_symbols = data.get("tradingSymbols") or data.get("tradingSymbol")
        if base_symbol:
            symbols = [base_symbol]
        elif trading_symbols:
            if isinstance(trading_symbols, list):
               symbols = trading_symbols
            else:
                symbols = [trading_symbols]
        exchange = data.get("exchange")
        if exchange:
            symbols = [s if ":" in str(s) else f"{exchange}:{s}" for s in symbols]
        symbol = symbols[0] if symbols else None

        action = data.get("action")
        if not action:
            ttype = data.get("transactionType")
            if ttype:
                ttype = str(ttype).lower()
                mapping = {
                    "le": "BUY",
                    "sxle": "BUY",
                    "lx": "SELL",
                    "se": "SELL",
                    "lxse": "SELL",
                    "sx": "BUY",
                    "buy": "BUY",
                    "sell": "SELL",    
                }
                action = mapping.get(ttype)

        quantity = data.get("quantity")
        if quantity is None:
            quantity = data.get("orderQty")
        try:
            if quantity is not None:
                quantity = int(float(quantity))
        except (TypeError, ValueError):
            quantity = None

        if not symbols or action is None or quantity is None:
            logger.error(f"Missing required fields: {data}")
            return jsonify({
                "error": "Missing required fields",
                "required": ["symbol/ticker", "action", "quantity"],
                "received": {
                    "symbol_or_ticker": bool(symbols),
                    "action": bool(action),
                    "quantity": bool(quantity)
                }
            }), 400
            

        # Determine account credentials
        sub_count = 0
        if strategy:
            sub_count = StrategySubscription.query.filter_by(strategy_id=strategy.id, approved=True).count()

        accounts = []
        if sub_count == 0:
            if strategy and strategy.master_accounts:
                allowed_ids = [int(a) for a in strategy.master_accounts.split(',') if a.strip()]
                if allowed_ids:
                    accounts = Account.query.filter(Account.user_id==user_obj.id, Account.id.in_(allowed_ids)).all()
            if not accounts:
                default_acc = None
                if strategy and strategy.account_id:
                    default_acc = Account.query.filter_by(id=strategy.account_id, user_id=user_obj.id).first()
                if not default_acc:
                    default_acc = user_obj.accounts[0] if user_obj.accounts else None
                if default_acc:
                    accounts = [default_acc]
            if not accounts or any(not a or not a.credentials for a in accounts):
                logger.error("Account not configured")
                return jsonify({"error": "Account not configured"}), 403

        def process_account(account, symbol):
            broker_name = (account.broker or "dhan").lower()
            if strategy and strategy.brokers:
                allowed = [b.strip().lower() for b in strategy.brokers.split(',') if b.strip()]
                if allowed and broker_name not in allowed:
                    return {"account_id": account.id, "symbol": symbol, "status": "SKIPPED", "reason": "Broker not allowed"}

            try:
                acc_dict = _account_to_dict(account)
                broker_api_instance = broker_api(acc_dict)
            except Exception as e:
                logger.error(f"Failed to initialize broker {broker_name}: {str(e)}")
                return {"account_id": account.id, "symbol": symbol, "status": "ERROR", "reason": "Failed to initialize broker"}
            try:
                mapping = get_symbol_for_broker(symbol, broker_name)
                order_params = build_order_params(broker_name, mapping, symbol, action, int(quantity), "MARKET", broker_api_instance)
            except Exception as e:
                logger.error(f"Error building order parameters: {str(e)}")
                return {"account_id": account.id, "symbol": symbol, "status": "ERROR", "reason": "Failed to build order parameters"}

            if strategy:
                if strategy.schedule:
                    try:
                        start, end = strategy.schedule.split("-")
                        now = datetime.utcnow().time()
                        start_t = datetime.strptime(start.strip(), "%H:%M").time()
                        end_t = datetime.strptime(end.strip(), "%H:%M").time()
                        if not (start_t <= now <= end_t):
                            return {"account_id": account.id, "symbol": symbol, "status": "ERROR", "reason": "Outside allowed schedule"}
                    except Exception as e:
                        logger.error(f"Schedule parse error: {e}")

                if strategy.risk_max_positions is not None or strategy.risk_max_allocation is not None:
                    try:
                        pos_resp = broker_api_instance.get_positions()
                        positions = _find_position_list(pos_resp)
                        if isinstance(positions, dict):
                            positions = [positions]
                        elif not isinstance(positions, list):
                            positions = []
                        count = 0
                        allocation = 0.0
                        for p in positions:
                            norm = normalize_position(p, account.broker)
                            if not norm:
                                continue
                            count += 1
                            qtyp = abs(norm.get("netQty", 0))
                            pricep = norm.get("ltp") or norm.get("buyAvg") or 0
                            allocation += qtyp * pricep
                        if strategy.risk_max_positions is not None and count >= strategy.risk_max_positions:
                            return {"account_id": account.id, "symbol": symbol, "status": "ERROR", "reason": "Max positions exceeded"}
                        if strategy.risk_max_allocation is not None and allocation >= strategy.risk_max_allocation:
                            return {"account_id": account.id, "symbol": symbol, "status": "ERROR", "reason": "Max allocation exceeded"}
                    except Exception as e:
                        logger.error(f"Risk check failed: {e}")

            try:
                response = broker_api_instance.place_order(**order_params)
                if isinstance(response, dict) and response.get("status") == "failure":
                    reason = response.get("remarks") or response.get("error_message") or response.get("error") or "Unknown error"
                    record_trade(
                        user_obj.id,
                        symbol,
                        action.upper(),
                        quantity,
                        order_params.get('price'),
                        "FAILED",
                        broker=account.broker,
                        client_id=account.client_id,
                    )
                    return {"account_id": account.id, "symbol": symbol, "status": "FAILED", "reason": reason}
                record_trade(
                    user_obj.id,
                    symbol,
                    action.upper(),
                    quantity,
                    order_params.get('price'),
                    "SUCCESS",
                    broker=account.broker,
                    client_id=account.client_id,
                )
                try:
                    poll_and_copy_trades()
                except Exception as e:
                    logger.error(f"Failed to trigger copy trading: {str(e)}")
                return {"account_id": account.id, "symbol": symbol, "status": "SUCCESS", "order_id": response.get("order_id"), "result": response.get("remarks", "Trade placed successfully")}
            except Exception as e:
                logger.error(f"Error placing order: {str(e)}")
                return {"account_id": account.id, "symbol": symbol, "status": "ERROR", "reason": str(e)}

        all_results = []
        for sym in symbols:
            if sub_count:
                all_results.extend(execute_for_subscriptions(strategy, sym, action, int(quantity)))
                continue
            results = [process_account(acc, sym) for acc in accounts]
            all_results.extend(results)

        if len(all_results) == 1:
            r = all_results[0]
            if r.get("status") in ("ERROR", "FAILED", "SKIPPED"):
                reason = r.get("reason", "")
                if reason in (
                    "Outside allowed schedule",
                    "Max positions exceeded",
                    "Max allocation exceeded",
                ):
                    return jsonify({"error": reason}), 403
                return jsonify(r), 400
            return jsonify(r), 200
        return jsonify({"results": all_results}), 200

    except Exception as e:
        logger.error(f"Unexpected error in webhook: {str(e)}")
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500

@app.route('/api/master-squareoff', methods=['POST'])
@login_required
def master_squareoff():
    """Square off child orders for a master order - Complete Database Version.
    
    Expected POST data:
        {
            "master_order_id": "string"
        }
        
    Returns:
        JSON response with square-off results for each child
    """
    logger.info("Processing master square-off request")
    
    try:
        # ✅ STEP 1: Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        master_order_id = data.get("master_order_id")
        if not master_order_id:
            logger.error("Missing master_order_id in request")
            return jsonify({"error": "Missing master_order_id"}), 400

        user = current_user()
        active_mappings = order_mappings_for_user(user).filter_by(
            master_order_id=master_order_id,
            status="ACTIVE"
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

        # ✅ STEP 3: Group mappings by child account for processing
        child_mappings = {}
        for mapping in active_mappings:
            child_id = mapping.child_client_id
            if child_id not in child_mappings:
                child_mappings[child_id] = []
            child_mappings[child_id].append(mapping)

        # ✅ STEP 4: Process each child account
        results = []
        successful_squareoffs = 0
        failed_squareoffs = 0

        for child_id, mappings in child_mappings.items():
            logger.info(f"Processing square-off for child {child_id} with {len(mappings)} positions")
            
            # Find child account in database
            child_account = Account.query.filter_by(
                client_id=child_id,
                user_id=user.id
            ).first()
            
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
                child_broker = child_account.broker.lower()
                # ✅ STEP 5: Convert account to dict for broker_api compatibility
                child_dict = _account_to_dict(child_account)
                broker_api_instance = broker_api(child_dict)
                
                # ✅ STEP 6: Get current positions for this child
                try:
                    positions_response = broker_api_instance.get_positions()
                    
                    # Handle different response formats
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

                # ✅ STEP 7: Process each symbol mapping for this child
                for mapping in mappings:
                    symbol = mapping.symbol
                    logger.debug(f"Processing symbol {symbol} for child {child_id}")
                    
                    # Find matching position
                    matching_position = None
                    for position in positions:
                        pos_symbol = (
                            position.get("tradingSymbol")
                            or position.get("tradingsymbol")  # Zerodha lowercase
                            or position.get("symbol")
                            or position.get("tsym")
                            or position.get("Tsym")
                            or ""
                        ).upper()

                        if not pos_symbol and (
                            position.get("instrument_token")
                            or position.get("instrumentToken")
                            or position.get("token")
                        ):
                            pos_symbol = (
                                get_symbol_by_token(
                                    position.get("instrument_token")
                                    or position.get("instrumentToken")
                                    or position.get("token"),
                                    child_broker,
                                )
                                or ""
                            ).upper()    
                        
                        if pos_symbol == symbol.upper():
                            # Check if there's a net position
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

                    # ✅ STEP 8: Calculate square-off parameters
                    net_qty = int(
                        matching_position.get("netQty") or
                        matching_position.get("net_quantity") or
                        matching_position.get("netQuantity") or
                        matching_position.get("Netqty") or
                        0
                    )
                    
                    direction = "SELL" if net_qty > 0 else "BUY"
                    abs_qty = abs(net_qty)
                    
                    # ✅ STEP 9: Build broker-specific order parameters
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
                    else:  # Default format for other brokers
                        order_params = {
                            "tradingsymbol": symbol,
                            "exchange": "NSE",
                            "transaction_type": direction,
                            "quantity": abs_qty,
                            "order_type": "MARKET",
                            "product": "MIS",
                            "price": 0
                        }

                    # ✅ STEP 10: Place square-off order
                    try:
                        logger.info(f"Placing square-off order for {child_id}: {direction} {abs_qty} {symbol}")
                        square_off_response = broker_api_instance.place_order(**order_params)
                        
                        # Check for API errors
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
                            # Success - update mapping status
                            mapping.status = "SQUARED_OFF"
                            mapping.remarks = f"Squared off on {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}"
                            
                            # Extract order ID from response
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

        # ✅ STEP 11: Commit all mapping status updates
        try:
            db.session.commit()
            logger.info(f"Successfully updated {successful_squareoffs} mappings to SQUARED_OFF status")
        except Exception as e:
            logger.error(f"Failed to commit mapping updates: {str(e)}")
            db.session.rollback()
            return jsonify({
                "error": "Failed to update mapping statuses",
                "details": str(e),
                "results": results
            }), 500

        # ✅ STEP 12: Log the bulk square-off action
        try:
            log_entry = SystemLog(
                timestamp=datetime.utcnow().isoformat(),
                level="INFO",
                message=f"Master square-off completed: {master_order_id} - {successful_squareoffs} success, {failed_squareoffs} failed",
                user_id=session.get("user", "system"), # Use session.get('user') which is an email string
                details=json.dumps({
                    "action": "master_squareoff",
                    "master_order_id": master_order_id,
                    "total_mappings": len(active_mappings),
                    "successful_squareoffs": successful_squareoffs,
                    "failed_squareoffs": failed_squareoffs,
                    "children_processed": len(child_mappings),
                    "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    "results_summary": {
                        "success": len([r for r in results if r["status"] == "SUCCESS"]),
                        "failed": len([r for r in results if r["status"] == "FAILED"]),
                        "skipped": len([r for r in results if r["status"] == "SKIPPED"]),
                        "error": len([r for r in results if r["status"] == "ERROR"])
                    }
                })
            )
            db.session.add(log_entry)
            db.session.commit()
        except Exception as e:
            logger.warning(f"Failed to log master square-off action: {str(e)}")

        # ✅ STEP 13: Prepare comprehensive response
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

        # ✅ STEP 14: Return appropriate status code
        if failed_squareoffs == 0:
            return jsonify(response_data), 200  # Complete success
        elif successful_squareoffs > 0:
            return jsonify(response_data), 207  # Partial success
        else:
            response_data["error"] = "All square-off attempts failed"
            return jsonify(response_data), 500  # Complete failure

    except Exception as e:
        logger.error(f"Unexpected error in master_squareoff: {str(e)}")
        # Ensure rollback in case of an unexpected error before commit
        db.session.rollback() 
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500

@app.route('/api/master-orders', methods=['GET'])
@login_required
def get_master_orders():
    """Get all master orders with their child order details.
    
    Query Parameters:
        master_id (optional): Filter orders by master account ID
        status (optional): Filter orders by status (ACTIVE, COMPLETED, FAILED, etc)
        
    Returns:
        JSON response with master orders and summary statistics
    """
    logger.info("Fetching master orders")
    
    try:
        # Fetch order mappings from the database
        master_id_filter = request.args.get("master_id")
        status_filter = request.args.get("status", "").upper()
        
        user = current_user()
        query = order_mappings_for_user(user)
        if master_id_filter:
            query = query.filter_by(master_client_id=master_id_filter)
        if status_filter:
            query = query.filter_by(status=status_filter)
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
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500

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
@login_required
def init_zerodha_login():
    data = request.json
    client_id = data.get('client_id')
    api_key = data.get('api_key')
    api_secret = data.get('api_secret')
    username = data.get('username')
    if not all([client_id, api_key, api_secret, username]):
        return jsonify({'error': 'Missing fields'}), 400

    pending = get_pending_zerodha()

    cred_data = {k: v for k, v in data.items() if k != 'broker'}
    cred_data['owner'] = session.get('user')
    pending[client_id] = cred_data
    set_pending_zerodha(pending)

    redirect_uri = f"https://dhan-trading.onrender.com/zerodha_redirects/{client_id}"
    login_url = f"https://kite.zerodha.com/connect/login?api_key={api_key}&v=3&redirect_uri={quote(redirect_uri, safe='')}"
    return jsonify({'login_url': login_url})

# ======== FYERS Authentication ========
@app.route('/api/init-fyers-login', methods=['POST'])
@login_required
def init_fyers_login():
    """Start the Fyers OAuth login flow and return the login URL."""
    data = request.json
    client_id = data.get('client_id')
    secret_key = data.get('secret_key')
    username = data.get('username')
    if not all([client_id, secret_key, username]):
        return jsonify({'error': 'Missing fields'}), 400

    pending = get_pending_fyers()

    state = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    redirect_uri = f"https://dhan-trading.onrender.com/fyers_redirects/{client_id}"

    cred_data = {k: v for k, v in data.items() if k != 'broker'}
    cred_data.update({'redirect_uri': redirect_uri, 'state': state, 'owner': session.get('user')})

    pending[client_id] = cred_data
    set_pending_fyers(pending)

    login_url = FyersBroker.login_url(client_id, redirect_uri, state)
    return jsonify({'login_url': login_url})


@app.route('/fyers_redirects/<client_id>')
def fyers_redirect_handler(client_id):
    """Handle redirect from Fyers OAuth flow and store tokens."""
    auth_code = request.args.get('auth_code')
    state = request.args.get('state')
    if not auth_code:
        return "No auth_code received", 400

    pending = get_pending_fyers()

    cred = pending.pop(client_id, None)
    if not cred or cred.get('state') != state:
        return "No pending auth for this client", 400

    secret_key = cred.get('secret_key')
    username = cred.get('username') or client_id
    redirect_uri = cred.get('redirect_uri')

    token_resp = FyersBroker.exchange_code_for_token(client_id, secret_key, auth_code)
    if token_resp.get('s') != 'ok':
        msg = token_resp.get('message', 'Failed to generate token')
        return f"Error: {msg}", 500

    access_token = token_resp.get('access_token')
    refresh_token = token_resp.get('refresh_token')

    cred_fields = {
        k: v
        for k, v in cred.items()
        if k not in {'owner', 'username', 'redirect_uri', 'state'}
    }
    cred_fields.update({
        'access_token': access_token,
        'refresh_token': refresh_token,
        'secret_key': secret_key,
    })

    account = {
        'broker': 'fyers',
        'client_id': client_id,
        'username': username,
        'credentials': cred_fields,
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

    # Save account
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

    # Save to accounts.json or DB
    save_account_to_user(username or client_id, account)

    return "✅ Zerodha account connected!"


@app.route('/api/square-off', methods=['POST'])
@login_required
def square_off():
    """Square off positions for a master or its children."""
    logger.info("Processing square-off request")
    try:
        data = request.json
        client_id = data.get("client_id")
        symbol = data.get("symbol")
        is_master_squareoff = data.get("is_master", False)

        if not client_id or not symbol:
            return jsonify({"error": "Missing client_id or symbol"}), 400

        # Find the account that was clicked on
        clicked_account, parent_master = find_account_by_client_id(client_id)
        if not clicked_account:
            return jsonify({"error": "Client not found"}), 404

        # Determine the actual master account for the operation
        master_account = parent_master if parent_master else clicked_account
        if master_account.role != 'master':
             return jsonify({"error": f"Account {master_account.client_id} is not a master account."}), 400

        # This block handles squaring off ONLY the master's personal position
        if is_master_squareoff:
            logger.info(f"Squaring off master account {master_account.client_id} position for {symbol}")
            try:
                master_api = broker_api(_account_to_dict(master_account))
                positions_resp = master_api.get_positions()
                positions = positions_resp.get("data", [])
                match = None
                for p in positions:
                    p_symbol = (
                        p.get("tradingSymbol", "")
                        or p.get("tradingsymbol", "")
                    )
                    if not p_symbol and (
                        p.get("instrument_token")
                        or p.get("instrumentToken")
                        or p.get("token")
                    ):
                        p_symbol = get_symbol_by_token(
                            p.get("instrument_token")
                            or p.get("instrumentToken")
                            or p.get("token"),
                            master_account.broker,
                        )
                    if p_symbol and str(p_symbol).upper() == symbol.upper():
                        match = p
                        break
                
                if not match or int(match.get("netQty", 0)) == 0:
                    return jsonify({"message": f"Master → No active position in {symbol} (already squared off)"}), 200

                qty = abs(int(match["netQty"]))
                direction = "SELL" if int(match["netQty"]) > 0 else "BUY"

                # Build order params dynamically
                order_params = {
                    "tradingsymbol": symbol,
                    "security_id": match.get("securityId"),
                    "exchange_segment": match.get("exchangeSegment"),
                    "transaction_type": direction,
                    "quantity": qty,
                    "order_type": "MARKET",
                    "product_type": match.get("productType", "INTRADAY"),
                    "price": 0
                }
                
                resp = master_api.place_order(**order_params)
                # Ensure the user_id for logging is correct. If admin is logged in, session['user'] won't be set.
                user_id_for_log = session.get('user') or session.get('admin') or 'system'
                save_log(user_id_for_log, symbol, "SQUARE_OFF", qty, "SUCCESS", str(resp))
                return jsonify({"message": "Master square-off placed", "details": str(resp)}), 200
            except Exception as e:
                logger.error(f"Failed to square off master position: {str(e)}")
                # Ensure rollback on error
                db.session.rollback()
                return jsonify({"error": str(e)}), 500
        
        # This block handles squaring off ALL linked children's positions
        else:
            logger.info(f"Squaring off all children under master {master_account.client_id} for {symbol}")
            
            # --- CORRECTED LOGIC ---
            # Query the database for all active children linked to this master
            # Ensure `user` is defined from `current_user()` here
            user = current_user()
            children_accounts = Account.query.filter_by(
                linked_master_id=master_account.client_id,
                role='child',
                user_id=user.id,
                copy_status='On'
            ).all()

            if not children_accounts:
                return jsonify({"message": "No active children found to square off."}), 200

            results = []
            for child in children_accounts:
                try:
                    child_api = broker_api(_account_to_dict(child))
                    positions_resp = child_api.get_positions()
                    positions = positions_resp.get('data', [])
                    match = None
                    for p in positions:
                        p_symbol = (
                            p.get('tradingSymbol', '')
                            or p.get('tradingsymbol', '')
                        )
                        if not p_symbol and (
                            p.get('instrument_token')
                            or p.get('instrumentToken')
                            or p.get('token')
                        ):
                            p_symbol = get_symbol_by_token(
                                p.get('instrument_token')
                                or p.get('instrumentToken')
                                or p.get('token'),
                                child.broker,
                            )
                        if p_symbol and str(p_symbol).upper() == symbol.upper():
                            match = p
                            break

                    if not match or int(match.get('netQty', 0)) == 0:
                        results.append(f"Child {child.client_id} → Skipped (no active position in {symbol})")
                        continue

                    quantity = abs(int(match['netQty']))
                    direction = "SELL" if int(match['netQty']) > 0 else "BUY"

                    order_params = {
                        "tradingsymbol": symbol,
                        "security_id": match.get("securityId"),
                        "exchange_segment": match.get("exchangeSegment"),
                        "transaction_type": direction,
                        "quantity": quantity,
                        "order_type": "MARKET",
                        "product_type": match.get("productType", "INTRADAY"),
                        "price": 0
                    }

                    response = child_api.place_order(**order_params)

                    if isinstance(response, dict) and response.get("status") == "failure":
                        msg = clean_response_message(response)
                        results.append(f"Child {child.client_id} → FAILED: {msg}")
                        save_log(child.client_id, symbol, "SQUARE_OFF", quantity, "FAILED", msg)
                    else:
                        results.append(f"Child {child.client_id} → SUCCESS")
                        save_log(child.client_id, symbol, "SQUARE_OFF", quantity, "SUCCESS", str(response))

                except Exception as e:
                    error_msg = str(e)
                    results.append(f"Child {child.client_id} → ERROR: {error_msg}")
                    save_log(child.client_id, symbol, "SQUARE_OFF", 0, "ERROR", error_msg)

            return jsonify({"message": "Square-off for all children completed", "details": results}), 200

    except Exception as e:
        logger.error(f"Unexpected error in square_off: {str(e)}")
        db.session.rollback()
        return jsonify({"error": "Internal server error", "details": str(e)}), 500


# --- Cancel Order Endpoint ---
# --- Corrected and Optimized Cancel Order Endpoint ---
@app.route('/api/cancel-order', methods=['POST'])
@login_required
def cancel_order():
    try:
        data = request.json
        master_order_id = data.get("master_order_id")
        
        # Find all active child orders linked to the master order
        user = current_user()
        mappings = order_mappings_for_user(user).filter_by(
            master_order_id=master_order_id, status="ACTIVE"
        ).all()

        if not mappings:
            return jsonify({"message": "No active child orders found for this master order."}), 200

        results = []
        
        # --- EFFICIENCY IMPROVEMENT ---
        # 1. Get only the child_ids we need to process
        child_ids_to_find = {m.child_client_id for m in mappings}
        
        # 2. Fetch only those specific accounts in a single query
        accounts = {
            acc.client_id: acc
            for acc in Account.query.filter(
                Account.client_id.in_(child_ids_to_find),
                Account.user_id == user.id
            ).all()
        }

        for mapping in mappings:
            # --- CORRECTNESS FIX ---
            # Use dot notation to access attributes of the SQLAlchemy object
            child_id = mapping.child_client_id
            child_order_id = mapping.child_order_id
            
            found_account = accounts.get(child_id)

            if not found_account:
                results.append(f"{child_id} → Client account not found")
                continue

            try:
                # Use the fetched account object to initialize the broker API
                api = broker_api({
                    "broker": found_account.broker,
                    "client_id": found_account.client_id,
                    "credentials": found_account.credentials,
                })
                
                cancel_resp = api.cancel_order(child_order_id)

                if isinstance(cancel_resp, dict) and cancel_resp.get("status") == "failure":
                    # Handle API-level failure
                    error_message = clean_response_message(cancel_resp)
                    results.append(f"{child_id} → Cancel failed: {error_message}")
                else:
                    # On success, update the mapping's status
                    mapping.status = "CANCELLED"
                    mapping.remarks = f"Cancelled by user on {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}"

            except Exception as e:
                # Handle exceptions during the API call
                results.append(f"{child_id} → ERROR: {str(e)}")

        # Commit all status changes to the database at once
        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            logger.error(f"Failed to commit cancel order mapping updates: {str(e)}")
            return jsonify({"error": "Failed to update order mapping statuses", "details": str(e)}), 500


        return jsonify({"message": "Cancel process completed", "details": results}), 200

    except Exception as e:
        # Rollback in case of an unexpected error during the process
        db.session.rollback()
        logger.error(f"Unexpected error in cancel_order: {str(e)}")
        return jsonify({"error": "An internal server error occurred.", "details": str(e)}), 500

@app.route('/api/change-master', methods=['POST'])
@login_required
def change_master():
    """Change master for a child account - Complete Database Version.
    
    Expected POST data:
        {
            "child_id": "string",
            "new_master_id": "string"
        }
        
    Returns:
        JSON response with change confirmation or error
    """
    logger.info("Processing change master request")
    
    try:
        # ✅ STEP 1: Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        child_id = data.get("child_id")
        new_master_id = data.get("new_master_id")
        
        if not child_id or not new_master_id:
            logger.error(f"Missing required fields: child_id={bool(child_id)}, new_master_id={bool(new_master_id)}")
            return jsonify({"error": "Missing child_id or new_master_id"}), 400

        # ✅ STEP 2: Get current user from session
        user_email = session.get("user")
        if not user_email:
            return jsonify({"error": "User not logged in"}), 401
            
        user = current_user() # Use current_user() helper
        if not user:
            logger.error(f"User not found: {user_email}")
            return jsonify({"error": "User not found"}), 404

        # ✅ STEP 3: Find and validate child account
        child_account = Account.query.filter_by(
            user_id=user.id,
            client_id=child_id
        ).first()
        
        if not child_account:
            logger.error(f"Child account not found: {child_id}")
            return jsonify({"error": "Child account not found"}), 404

        # ✅ STEP 4: Validate child account is actually a child
        if child_account.role != "child":
            logger.error(f"Account {child_id} is not configured as child (role: {child_account.role})")
            return jsonify({
                "error": "Account is not configured as a child",
                "current_role": child_account.role
            }), 400

        # ✅ STEP 5: Get current master details for comparison
        old_master_id = child_account.linked_master_id
        old_master_account = None
        
        if old_master_id:
            old_master_account = Account.query.filter_by(
                client_id=old_master_id,
                user_id=user.id
            ).first()

        # ✅ STEP 6: Validate new master account exists and is accessible
        new_master_account = Account.query.filter_by(
            user_id=user.id,  # Must belong to same user
            client_id=new_master_id,
            role='master'
        ).first()
        
        if not new_master_account:
            logger.error(f"New master account not found or not accessible: {new_master_id}")
            return jsonify({
                "error": "New master account not found or not accessible",
                "master_id": new_master_id
            }), 404

        # ✅ STEP 7: Check if already linked to the same master
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

        # ✅ STEP 8: Store previous state for logging and response
        previous_state = {
            "linked_master_id": child_account.linked_master_id,
            "copy_status": child_account.copy_status,
            "last_copied_trade_id": child_account.last_copied_trade_id
        }

        # ✅ STEP 9: Update child account with new master
        logger.info(f"Changing master for {child_id}: {old_master_id} -> {new_master_id}")
        
        child_account.linked_master_id = new_master_id
        
        # Reset marker to prevent copying historical orders from new master
        child_account.last_copied_trade_id = "NONE"
        
        # If copying was active, temporarily turn it off for safety
        was_copying = child_account.copy_status == "On"
        if was_copying:
            child_account.copy_status = "Off"
            logger.info(f"Temporarily disabled copying during master change for {child_id}")

        # ✅ STEP 10: Get latest order from new master for marker
        new_latest_order_id = "NONE"
        
        try:
            # Convert new master to dict for broker_api
            new_master_dict = _account_to_dict(new_master_account)
            new_master_api = broker_api(new_master_dict)
            
            # Get orders based on broker type
            broker_name = new_master_account.broker.lower() if new_master_account.broker else "unknown"
            
            if broker_name == "aliceblue" and hasattr(new_master_api, "get_trade_book"):
                logger.debug("Using AliceBlue trade book for new master marker")
                orders_resp = new_master_api.get_trade_book()
                order_list = parse_order_list(orders_resp)
                
                # Fallback to order list if trade book is empty
                if not order_list and hasattr(new_master_api, "get_order_list"):
                    orders_resp = new_master_api.get_order_list()
                    order_list = parse_order_list(orders_resp)
            else:
                orders_resp = new_master_api.get_order_list()
                order_list = parse_order_list(orders_resp)
            
            # Process orders
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

        # Set the new marker
        child_account.last_copied_trade_id = new_latest_order_id

        # ✅ STEP 11: Commit changes to database
        try:
            db.session.commit()
            logger.info(f"Successfully changed master for {child_id}: {old_master_id} -> {new_master_id}")
        except Exception as e:
            logger.error(f"Failed to commit master change: {str(e)}")
            db.session.rollback()
            return jsonify({
                "error": "Failed to save master change",
                "details": str(e)
            }), 500

        # ✅ STEP 12: Log the action for audit trail
        try:
            log_entry = SystemLog(
                timestamp=datetime.utcnow().isoformat(),
                level="INFO",
                message=f"Master changed: {child_id} from {old_master_id} to {new_master_id}",
                user_id=str(user.id),
                details=json.dumps({
                    "action": "change_master",
                    "child_id": child_id,
                    "old_master_id": old_master_id,
                    "new_master_id": new_master_id,
                    "user": user_email,
                    "was_copying": was_copying,
                    "new_marker": new_latest_order_id,
                    "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    "previous_state": previous_state
                })
            )
            db.session.add(log_entry)
            db.session.commit()
        except Exception as e:
            logger.warning(f"Failed to log master change: {str(e)}")

        # ✅ STEP 13: Prepare comprehensive response
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

        # Add restart instruction if copying was active
        if was_copying:
            response_data["next_action"] = "Please restart copying to begin following the new master"

        return jsonify(response_data), 200

    except Exception as e:
        logger.error(f"Unexpected error in change_master: {str(e)}")
        # Ensure rollback on unexpected error before commit
        db.session.rollback()
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500
        

@app.route('/api/remove-child', methods=['POST'])
@login_required
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

        user_email = session.get("user")
        user = current_user() # Use current_user() helper
        if not user:
            logger.error(f"User not found: {user_email}")
            return jsonify({"error": "User not found"}), 404

        child_account = Account.query.filter_by(user_id=user.id, client_id=client_id, role='child').first()
        if not child_account:
            logger.error(f"Child account not found: {client_id}")
            return jsonify({"error": "Child account not found or not configured as child"}), 404

        master_id = child_account.linked_master_id
        master_account = (
            Account.query.filter_by(user_id=user.id, client_id=master_id, role="master").first()
            if master_id else None
        )

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
        
        active_mappings = OrderMapping.query.filter_by(child_client_id=client_id, status="ACTIVE").all()
        mapping_count = len(active_mappings)

        child_account.role = None
        child_account.linked_master_id = None
        child_account.copy_status = "Off"
        child_account.multiplier = 1.0
        child_account.copy_value_limit = None
        child_account.copied_value = 0.0
        child_account.last_copied_trade_id = None

        current_time_iso = datetime.utcnow().isoformat()
        current_time_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        mappings_updated = 0
        for mapping in active_mappings:
            mapping.status = "CHILD_REMOVED"
            mapping.remarks = f"Child account removed on {current_time_str}"
            mappings_updated += 1

        db.session.commit()

        log_entry = SystemLog(
            timestamp=current_time_iso,
            level="INFO",
            message=f"Child removed: {client_id} from master {master_id}",
            user_id=str(user.id),
            details=json.dumps({
                "action": "remove_child",
                "client_id": client_id,
                "master_id": master_id,
                "user": user_email,
                "was_copying": was_copying,
                "active_mappings": mapping_count,
                "mappings_updated": mappings_updated,
                "timestamp": current_time_iso, # Corrected Timestamp
                "previous_state": previous_state
            })
        )
        db.session.add(log_entry)
        db.session.commit()

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
                "removed_at": current_time_iso # Corrected Timestamp
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
        db.session.rollback()
        return jsonify({"error": "Internal server error", "details": str(e)}), 500
        

@app.route('/api/remove-master', methods=['POST'])
@login_required
def remove_master():
    """Remove master role from account - Complete Database Version.
    
    Expected POST data:
        {
            "client_id": "string"
        }
        
    Returns:
        JSON response with removal confirmation or error
    """
    logger.info("Processing remove master request")
    
    try:
        # ✅ STEP 1: Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        client_id = data.get("client_id")
        
        if not client_id:
            logger.error("Missing client_id in request")
            return jsonify({"error": "Missing client_id"}), 400

        # ✅ STEP 2: Get current user from session
        user_email = session.get("user")
        user = current_user() # Use current_user() helper
        if not user:
            logger.error(f"User not found: {user_email}")
            return jsonify({"error": "User not found"}), 404

        # ✅ STEP 3: Find and validate master account
        master_account = Account.query.filter_by(
            user_id=user.id,
            client_id=client_id,
            role='master'  # Must be a master to remove
        ).first()
        
        if not master_account:
            logger.error(f"Master account not found: {client_id}")
            return jsonify({"error": "Master account not found or not configured as master"}), 404

        # ✅ STEP 4: Find all child accounts linked to this master
        linked_children = Account.query.filter_by(
            role='child',
            linked_master_id=client_id,
            user_id=user.id
        ).all()

        children_count = len(linked_children)
        active_children = [child for child in linked_children if child.copy_status == "On"]
        active_count = len(active_children)

        logger.info(f"Master {client_id} has {children_count} linked children ({active_count} actively copying)")

        # ✅ STEP 5: Store current state for logging and response
        previous_state = {
            "role": master_account.role,
            "copy_status": master_account.copy_status,
            "multiplier": master_account.multiplier,
            "broker": master_account.broker,
            "username": master_account.username,
            "linked_children": [child.client_id for child in linked_children],
            "active_children": [child.client_id for child in active_children]
        }

        # ✅ STEP 6: Find all active order mappings for this master
        active_mappings = OrderMapping.query.filter_by(
            master_client_id=client_id,
            status="ACTIVE"
        ).all()

        mapping_count = len(active_mappings)
        if mapping_count > 0:
            logger.info(f"Found {mapping_count} active order mappings for master {client_id}")

        # ✅ STEP 7: Process each linked child (orphan them)
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
                child.copy_value_limit = None
                child.copied_value = 0.0
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

        # ✅ STEP 8: Remove master role from account
        logger.info(f"Removing master role from account {client_id}")
        
        master_account.role = None  # Remove role completely
        master_account.copy_status = "Off"  # Ensure status is off
        master_account.multiplier = 1.0  # Reset to default
        # Note: linked_master_id is not applicable for masters

        # ✅ STEP 9: Update all active order mappings to mark them as orphaned
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

        # ✅ STEP 10: Commit all changes to database
        try:
            db.session.commit()
            logger.info(f"Successfully removed master role from {client_id} and orphaned {len(children_processed)} children")
        except Exception as e:
            logger.error(f"Failed to commit master removal: {str(e)}")
            db.session.rollback()
            return jsonify({
                "error": "Failed to save master removal",
                "details": str(e)
            }), 500

        # ✅ STEP 11: Log the action for audit trail
        try:
            log_entry = SystemLog(
                timestamp=datetime.utcnow().isoformat(),
                level="INFO",
                message=f"Master removed: {client_id} with {children_count} children orphaned",
                user_id=str(user.id),
                details=json.dumps({
                    "action": "remove_master",
                    "master_id": client_id,
                    "user": user_email,
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

        # ✅ STEP 12: Prepare comprehensive response
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

        # Add failure details if any children failed
        if children_failed:
            response_data["children_affected"]["failures"] = children_failed

        # Add warning if there were many mappings
        if mapping_count > 10:
            response_data["order_mappings"]["note"] = f"Showing first 10 of {mapping_count} total mappings"

        return jsonify(response_data), 200

    except Exception as e:
        logger.error(f"Unexpected error in remove_master: {str(e)}")
        # Ensure rollback on unexpected error before commit
        db.session.rollback()
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500

# PATCH for /api/update-multiplier
@app.route('/api/update-multiplier', methods=['POST'])
@login_required
def update_multiplier():
    """Update multiplier for account - Complete Database Version.
    
    Expected POST data:
        {
            "client_id": "string",
            "multiplier": float
        }
        
    Returns:
        JSON response with update confirmation or error
    """
    logger.info("Processing update multiplier request")
    
    try:
        # ✅ STEP 1: Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        client_id = data.get("client_id")
        new_multiplier = data.get("multiplier")
        
        if not client_id or new_multiplier is None:
            logger.error(f"Missing required fields: client_id={bool(client_id)}, multiplier={new_multiplier is not None}")
            return jsonify({"error": "Missing client_id or multiplier"}), 400

        # ✅ STEP 2: Validate multiplier value
        try:
            new_multiplier = float(new_multiplier)
            if new_multiplier < 0.1:
                return jsonify({"error": "Multiplier must be at least 0.1"}), 400
            if new_multiplier > 100.0:  # Reasonable upper limit
                return jsonify({"error": "Multiplier cannot exceed 100.0"}), 400
        except (ValueError, TypeError):
            return jsonify({"error": "Invalid multiplier format - must be a number"}), 400

        # ✅ STEP 3: Get current user from session
        user_email = session.get("user")
        user = current_user() # Use current_user() helper
        if not user:
            logger.error(f"User not found: {user_email}")
            return jsonify({"error": "User not found"}), 404

        # ✅ STEP 4: Find and validate account
        account = Account.query.filter_by(
            user_id=user.id,
            client_id=client_id
        ).first()
        
        if not account:
            logger.error(f"Account not found: {client_id} for user {user_email}")
            return jsonify({"error": "Account not found"}), 404

        # ✅ STEP 5: Store previous state for logging and response
        previous_state = {
            "multiplier": account.multiplier,
            "role": account.role,
            "copy_status": account.copy_status,
            "linked_master_id": account.linked_master_id,
            "broker": account.broker,
            "username": account.username
        }

        # ✅ STEP 6: Check if multiplier is actually changing
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

        # ✅ STEP 7: Get master account details if account is a child
        master_account = None
        if account.role == "child" and account.linked_master_id:
            master_account = Account.query.filter_by(
                client_id=account.linked_master_id,
                user_id=user.id
            ).first()

        # ✅ STEP 8: Update multiplier
        logger.info(f"Updating multiplier for {client_id}: {previous_state['multiplier']} -> {new_multiplier}")
        
        account.multiplier = new_multiplier

        # ✅ STEP 9: Handle special cases based on account role
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

        # ✅ STEP 10: Check for any active order mappings that might be affected
        active_mappings = []
        if account.role == "child":
            active_mappings = OrderMapping.query.filter_by(
                child_client_id=client_id,
                status="ACTIVE"
            ).all()

        active_mapping_count = len(active_mappings)
        
        if active_mapping_count > 0:
            warnings.append(f"{active_mapping_count} active order mappings found - multiplier change affects future orders only")

        # ✅ STEP 11: Commit changes to database
        try:
            db.session.commit()
            logger.info(f"Successfully updated multiplier for {client_id} to {new_multiplier}")
        except Exception as e:
            logger.error(f"Failed to commit multiplier update: {str(e)}")
            db.session.rollback()
            return jsonify({
                "error": "Failed to save multiplier update",
                "details": str(e)
            }), 500

        # ✅ STEP 12: Log the action for audit trail
        try:
            log_entry = SystemLog(
                timestamp=datetime.utcnow().isoformat(),
                level="INFO",
                message=f"Multiplier updated: {client_id} from {previous_state['multiplier']} to {new_multiplier}",
                user_id=str(user.id),
                details=json.dumps({
                    "action": "update_multiplier",
                    "client_id": client_id,
                    "user": user_email,
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

        # ✅ STEP 13: Calculate impact estimation if child account
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

        # ✅ STEP 14: Prepare comprehensive response
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

        # Add impact estimation if available
        if impact_estimation:
            response_data["impact_estimation"] = impact_estimation

        return jsonify(response_data), 200

    except Exception as e:
        logger.error(f"Unexpected error in update_multiplier: {str(e)}")
        # Ensure rollback on unexpected error before commit
        db.session.rollback()
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500

# Delete an account entirely
@app.route('/api/delete-account', methods=['POST'])
@login_required
def delete_account():
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
        
    try:
        # If deleting a master, orphan all linked children so they appear as unassigned
        if acc_db.role == "master":
            linked_children = Account.query.filter_by(
                role="child",
                linked_master_id=client_id,
                user_id=db_user.id,
            ).all()
            for child in linked_children:
                child.role = None
                child.linked_master_id = None
                child.copy_status = "Off"
                child.multiplier = 1.0
                child.last_copied_trade_id = None

            # Mark any active order mappings for this master as removed
            mappings = OrderMapping.query.filter_by(
                master_client_id=client_id,
                status="ACTIVE",
            ).all()
            for mapping in mappings:
                mapping.status = "MASTER_REMOVED"

        elif acc_db.role == "child":
            # Remove references if deleting a child account
            mappings = OrderMapping.query.filter_by(
                child_client_id=client_id,
                status="ACTIVE",
            ).all()
            for mapping in mappings:
                mapping.status = "CHILD_REMOVED"

        db.session.delete(acc_db)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.error(f"Failed to delete account {client_id}: {str(e)}")
        return jsonify({"error": f"Failed to delete account: {str(e)}"}), 500

    return jsonify({"message": f"Account {client_id} deleted."})

# Reconnect an existing account by validating stored credentials
@app.route('/api/reconnect-account', methods=['POST'])
@login_required
def reconnect_account():
    """Reconnect an account using stored or provided credentials."""
    data = request.json or {}
    client_id = data.get("client_id")
    if not client_id:
        return jsonify({"error": "Missing client_id"}), 400

    user_email = session.get("user")
    db_user = User.query.filter_by(email=user_email).first()
    if not db_user:
        return jsonify({"error": "Account not found"}), 404

    acc_db = Account.query.filter_by(user_id=db_user.id, client_id=client_id).first()
    if not acc_db:
        return jsonify({"error": "Account not found"}), 404
    # Merge any credentials provided in the request with stored ones
    new_creds = dict(acc_db.credentials or {})
    for k, v in data.items():
        if k not in ("client_id", "broker", "username") and v is not None:
            new_creds[k] = v

    acc_db.username = data.get("username", acc_db.username)
    acc_db.credentials = dict(new_creds)


    try:
        acc_dict = _account_to_dict(acc_db)
        acc_dict["credentials"] = new_creds
        api = broker_api(acc_dict)
        valid = True
        if hasattr(api, 'check_token_valid'):
            valid = api.check_token_valid()
        if not valid:
            acc_db.status = 'Failed'
            db.session.commit()
            return jsonify({"error": "Failed to reconnect with stored credentials"}), 400

        # Update stored access token and token_time if available
        if getattr(api, 'access_token', None):
            new_creds['access_token'] = api.access_token
        if getattr(api, 'token_time', None):
            new_creds['token_time'] = api.token_time
        acc_db.credentials = dict(new_creds)

        acc_db.status = 'Connected'
        acc_db.last_login_time = datetime.utcnow()
        db.session.commit()
        clear_connection_error_logs(acc_db)
    except Exception as e:
        db.session.rollback()
        logger.error(f"Failed to reconnect account {client_id}: {str(e)}")
        return jsonify({"error": f"Failed to reconnect: {str(e)}"}), 500

    return jsonify({"message": f"Account {client_id} reconnected."})

# Fetch the latest opening balance for a specific account
@app.route('/api/account-balance', methods=['POST'])
@login_required
def account_balance():
    data = request.json or {}
    client_id = data.get("client_id")
    if not client_id:
        return jsonify({"error": "Missing client_id"}), 400

    user_email = session.get("user")
    db_user = User.query.filter_by(email=user_email).first()
    if not db_user:
        return jsonify({"error": "Account not found"}), 404

    acc_db = Account.query.filter_by(user_id=db_user.id, client_id=client_id).first()
    if not acc_db:
        return jsonify({"error": "Account not found"}), 404

    bal = get_opening_balance_for_account(_account_to_dict(acc_db))
    return jsonify({"balance": bal})


# Check auto-login status for all accounts that have auto_login enabled
@app.route('/api/check-auto-logins', methods=['POST'])
@login_required
def check_auto_logins():
    """Attempt to reconnect all accounts with auto_login enabled."""
    user = current_user()
    if not user:
        return jsonify({"error": "User not found"}), 404

    results = []
    for acc_db in user.accounts:
        if not acc_db.auto_login:
            continue
        try:
            acc_dict = _account_to_dict(acc_db)
            acc_dict["credentials"] = acc_db.credentials
            api = broker_api(acc_dict)
            valid = True
            if hasattr(api, "check_token_valid"):
                valid = api.check_token_valid()
            if not valid:
                acc_db.status = "Failed"
            else:
                acc_db.status = "Connected"
                acc_db.last_login_time = datetime.utcnow()
                if getattr(api, "access_token", None):
                    creds = dict(acc_db.credentials or {})
                    creds["access_token"] = api.access_token
                    if getattr(api, "token_time", None):
                        creds["token_time"] = api.token_time
                    acc_db.credentials = creds
            db.session.commit()
            if valid:
                clear_connection_error_logs(acc_db)
            results.append({"client_id": acc_db.client_id, "status": acc_db.status})
        except Exception as e:
            db.session.rollback()
            results.append({"client_id": acc_db.client_id, "error": str(e)})

    return jsonify({"results": results})


# Update credentials for an existing account
@app.route('/api/update-account', methods=['POST'])
@login_required
def update_account():
    data = request.json or {}
    client_id = data.get("client_id")
    if not client_id:
        return jsonify({"error": "Missing client_id"}), 400

    user_email = session.get("user")
    db_user = User.query.filter_by(email=user_email).first()
    if not db_user:
        return jsonify({"error": "Account not found"}), 404

    acc_db = Account.query.filter_by(user_id=db_user.id, client_id=client_id).first()
    if not acc_db:
        return jsonify({"error": "Account not found"}), 404

    broker = acc_db.broker
    if data.get("broker") and data.get("broker") != broker:
        return jsonify({"error": "Broker cannot be changed"}), 400

    new_creds = dict(acc_db.credentials or {})
    for k, v in data.items():
        if k not in ("client_id", "broker", "username") and v is not None:
            new_creds[k] = v

    acc_db.username = data.get("username", acc_db.username)
    acc_db.credentials = new_creds

    try:
        acc_dict = _account_to_dict(acc_db)
        acc_dict["credentials"] = new_creds
        api = broker_api(acc_dict)
        valid = True
        if hasattr(api, "check_token_valid"):
            valid = api.check_token_valid()
        if not valid:
            acc_db.status = "Failed"
            db.session.commit()
            return jsonify({"error": "Credential validation failed"}), 400

        if getattr(api, "access_token", None):
            new_creds["access_token"] = api.access_token
        if getattr(api, "token_time", None):
            new_creds["token_time"] = api.token_time
        acc_db.credentials = new_creds

        acc_db.status = "Connected"
        acc_db.last_login_time = datetime.utcnow()
        db.session.commit()
        clear_connection_error_logs(acc_db)
    except Exception as e:
        db.session.rollback()
        logger.error(f"Failed to update account {client_id}: {str(e)}")
        return jsonify({"error": f"Failed to update account: {str(e)}"}), 500

    return jsonify({"message": f"Account {client_id} updated."})


@app.route("/marketwatch")
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
    
    broker_obj = None # Initialize broker_obj to None
    error_message = None # Initialize error_message

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
            imei = credentials.get('imei')
            if not imei:
                return jsonify({'error': 'Missing IMEI'}), 400
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
            # For other brokers, assume access_token based authentication
            access_token = credentials.get('access_token')
            rest = {
                k: v
                for k, v in credentials.items()
                if k not in ('access_token', 'client_id')
            }
            broker_obj = BrokerClass(client_id, access_token, **rest)

        # After instantiation, check token validity if the method exists
        if broker_obj and hasattr(broker_obj, 'check_token_valid'):
            valid = broker_obj.check_token_valid()
            if not valid:
                # If validation fails, try to get a more specific error message
                error_message = broker_obj.last_auth_error() or 'Invalid broker credentials'
                return jsonify({'error': error_message}), 400
        elif not broker_obj:
            return jsonify({'error': 'Broker object could not be initialized.'}), 400
        
        return jsonify({'valid': True})

    except Exception as e:
        # Catch any exceptions during broker instantiation or the check_token_valid call
        if broker_obj and hasattr(broker_obj, 'last_auth_error') and broker_obj.last_auth_error():
            error_message = broker_obj.last_auth_error()
        else:
            error_message = str(e)
        return jsonify({'error': f'Credential validation failed: {error_message}'}), 400


@app.route('/api/add-account', methods=['POST'])
@login_required
def add_account():
    """Add account - Complete Database Version.
    
    Expected POST data:
        {
            "broker": "string",
            "client_id": "string", 
            "username": "string",
            "access_token": "string",  # For most brokers
            "api_key": "string",       # For AliceBlue/Finvasia
            "password": "string",      # For Finvasia
            "totp_secret": "string",   # For Finvasia
            "vendor_code": "string",   # For Finvasia
            "imei": "string"           # For Finvasia (optional)
        }
        
    Returns:
        JSON response with account addition confirmation or error
    """
    logger.info("Processing add account request")
    
    try:
        # ✅ STEP 1: Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        broker = data.get('broker')
        client_id = data.get('client_id')
        username = data.get('username')
        
        if not broker or not client_id or not username:
            logger.error(f"Missing required fields: broker={bool(broker)}, client_id={bool(client_id)}, username={bool(username)}")
            return jsonify({"error": "Missing broker, client_id or username"}), 400

        # Normalize broker name
        broker = broker.lower().strip()
        
        # Validate broker is supported
        supported_brokers = ['aliceblue', 'finvasia', 'dhan', 'zerodha', 'groww', 'upstox', 'fyers']
        if broker not in supported_brokers:
            return jsonify({
                "error": f"Unsupported broker: {broker}",
                "supported_brokers": supported_brokers
            }), 400

        # ✅ STEP 2: Get current user from session
        user_email = session.get("user")
        user = current_user() # Use current_user() helper
        if not user:
            logger.error(f"User not found: {user_email}")
            return jsonify({"error": "User not found"}), 404

        # ✅ STEP 3: Check for duplicate account
        existing_account = Account.query.filter_by(
            user_id=user.id,
            client_id=client_id,
            broker=broker
        ).first()
        
        if existing_account:
            logger.warning(f"Account already exists: {client_id} ({broker}) for user {user_email}")
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

        # ✅ STEP 4: Extract and validate broker-specific credentials
        credentials = {}
        
        # Copy all credential fields except the basic ones
        for key, value in data.items():
            if key not in ('broker', 'client_id', 'username') and value is not None:
                credentials[key] = value

        # ✅ STEP 5: Broker-specific validation and setup
        validation_error = None
        
        if broker == 'aliceblue':
            api_key = credentials.get('api_key')
            if not api_key:
                validation_error = "Missing API Key for AliceBlue"
            else:
                # Generate device number if not provided
                if 'device_number' not in credentials:
                    # Check if user has existing AliceBlue account with device_number
                    existing_alice = Account.query.filter_by(
                        user_id=user.id,
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
                # Set default IMEI if not provided
                if not credentials.get('imei'):
                    validation_error = "Missing IMEI for Finvasia"
                    
        elif broker == 'groww':
            access_token = credentials.get('access_token')
            if not access_token:
                validation_error = "Missing Access Token for Groww"
                
        else:  # dhan, zerodha, upstox
            access_token = credentials.get('access_token')
            if not access_token:
                validation_error = f"Missing Access Token for {broker.title()}"

        if validation_error:
            logger.error(f"Credential validation failed: {validation_error}")
            return jsonify({"error": validation_error}), 400

        # ✅ STEP 6: Test broker connection and validate credentials
        broker_obj = None  # ensure variable defined for error handling
        try:
            BrokerClass = get_broker_class(broker)
            if not BrokerClass:
                return jsonify({"error": f"Broker class not found for {broker}"}), 500
            
            # Initialize broker object based on type
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
                        imei=credentials['imei']
                    )
                
            elif broker == 'groww':
                broker_obj = BrokerClass(client_id, credentials['access_token'])
                
            else:  # dhan, zerodha, upstox
                access_token = credentials.get('access_token')
                # Pass additional credentials as kwargs
                other_creds = {k: v for k, v in credentials.items() if k != 'access_token'}
                broker_obj = BrokerClass(client_id, access_token, **other_creds)

            # Store any updated token information from the broker object
            if getattr(broker_obj, 'access_token', None):
                credentials['access_token'] = broker_obj.access_token
            if getattr(broker_obj, 'token_time', None):
                credentials['token_time'] = broker_obj.token_time
                
            # Test connection if validation method exists
            if broker != 'zerodha' and hasattr(broker_obj, 'check_token_valid'):
                logger.info(f"Validating credentials for {broker} account {client_id}")
                is_valid = broker_obj.check_token_valid()
                
                if not is_valid:
                    error_message = "Invalid broker credentials"
                    if hasattr(broker_obj, 'last_auth_error') and broker_obj.last_auth_error():
                        error_message = broker_obj.last_auth_error()
                    
                    logger.error(f"Credential validation failed for {broker}: {error_message}")
                    return jsonify({"error": f"Credential validation failed: {error_message}"}), 400
                    
                logger.info(f"Credentials validated successfully for {broker} account {client_id}")
            else:
                logger.info(f"No validation method available for {broker}, skipping credential test")
                
        except Exception as e:
            error_message = str(e)
            if hasattr(broker_obj, 'last_auth_error') and broker_obj.last_auth_error():
                error_message = broker_obj.last_auth_error()
            
            logger.error(f"Failed to initialize/validate {broker} broker: {error_message}")
            return jsonify({"error": f"Broker initialization failed: {error_message}"}), 400

        # ✅ STEP 7: Create new account in database
        logger.info(f"Creating new account: {client_id} ({broker}) for user {user_email}")
        
        new_account = Account(
            user_id=user.id,
            broker=broker,
            client_id=client_id,
            username=username,
            credentials=credentials,  # Store as JSON
            status='Connected',
            auto_login=True,
            last_login_time=datetime.utcnow(),
            role=None,  # Initially unassigned
            linked_master_id=None,
            multiplier=1.0,
            copy_status='Off',
            copy_value_limit=None,
            copied_value=0.0,
            device_number=credentials.get('device_number') if broker == 'aliceblue' else None
        )

        # ✅ STEP 8: Add and commit to database
        try:
            db.session.add(new_account)
            db.session.commit()
            logger.info(f"Successfully added account {client_id} ({broker}) to database")
        except Exception as e:
            logger.error(f"Failed to save account to database: {str(e)}")
            db.session.rollback()
            return jsonify({
                "error": "Failed to save account",
                "details": str(e)
            }), 500

        # ✅ STEP 9: Log the action for audit trail
        try:
            log_entry = SystemLog(
                timestamp=datetime.utcnow(),
                level="INFO",
                message=f"Account added: {client_id} ({broker}) by {user_email}",
                user_id=str(user.id),
                details=json.dumps({
                    "action": "add_account",
                    "client_id": client_id,
                    "broker": broker,
                    "username": username,
                    "user": user_email,
                    "credentials_provided": list(credentials.keys()),
                    "validation_performed": hasattr(broker_obj, 'check_token_valid'),
                    "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                })
            )
            db.session.add(log_entry)
            db.session.commit()
        except Exception as e:
            logger.warning(f"Failed to log account addition: {str(e)}")

        # ✅ STEP 10: Prepare success response
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
        # Ensure rollback on unexpected error before commit
        db.session.rollback()
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500
    
    
@app.route('/api/accounts')
@login_required
def get_accounts():
    try:
        user_email = session.get("user")
        user = current_user()  # Use current_user() helper
        if not user:
            return jsonify({"error": "User not found"}), 404

        # Ensure any children whose master was deleted are marked unassigned
        orphan_children_without_master(user)
            
        cache_only = request.args.get("cache_only") in ("1", "true", "True")
        
        accounts = [_account_to_dict(acc) for acc in user.accounts]

        # Add opening balances
        for acc in accounts:
            bal = get_opening_balance_for_account(acc, cache_only=cache_only)
            if bal is not None:
                acc["opening_balance"] = bal

        # Group masters with their children
        masters = []
        for acc in accounts:
            if acc.get("role") == "master":
                children = Account.query.filter_by(
                    role="child",
                    linked_master_id=acc.get("client_id"),
                    user_id=user.id
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
        # Ensure rollback on error
        db.session.rollback()
        return jsonify({"error": str(e)}), 500
        
@app.route('/api/groups', methods=['GET'])
@login_required
def get_groups():
    """Return all account groups for the logged-in user."""
    user_email = session.get("user")
    user_obj = current_user() # Use current_user() helper
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
    user_obj = current_user() # Use current_user() helper
    if not user_obj:
        return jsonify({"error": "User not found"}), 400
    if Group.query.filter_by(user_id=user_obj.id, name=name).first():
        return jsonify({"error": "Group already exists"}), 400

    group = Group(name=name, user_id=user_obj.id)
    for cid in members:
        acc = Account.query.filter_by(user_id=user_obj.id, client_id=cid).first()
        if acc:
            group.accounts.append(acc)
    try:
        db.session.add(group)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.error(f"Failed to create group: {str(e)}")
        return jsonify({"error": f"Failed to create group: {str(e)}"}), 500

    return jsonify({"message": f"Group '{name}' created"})


@app.route('/api/groups/<group_name>/add', methods=['POST'])
@login_required
def add_account_to_group(group_name):
    """Add an account to an existing group."""
    client_id = request.json.get("client_id")
    if not client_id:
        return jsonify({"error": "Missing client_id"}), 400

    user_email = session.get("user")
    user_obj = current_user() # Use current_user() helper
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
    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.error(f"Failed to add account to group: {str(e)}")
        return jsonify({"error": f"Failed to add account to group: {str(e)}"}), 500
    return jsonify({"message": f"Added {client_id} to {group_name}"})

@app.route('/api/groups/<group_name>/remove', methods=['POST'])
@login_required
def remove_account_from_group(group_name):
    """Remove an account from a group."""
    client_id = request.json.get("client_id")
    if not client_id:
        return jsonify({"error": "Missing client_id"}), 400

    user_email = session.get("user")
    user_obj = current_user() # Use current_user() helper
    if not user_obj:
        return jsonify({"error": "User not found"}), 400
    group = Group.query.filter_by(user_id=user_obj.id, name=group_name).first()
    if not group:
        return jsonify({"error": "Group not found"}), 404
    acc = Account.query.filter_by(user_id=user_obj.id, client_id=client_id).first()
    if not acc or acc not in group.accounts:
        return jsonify({"error": "Account not in group"}), 400
    group.accounts.remove(acc)
    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.error(f"Failed to remove account from group: {str(e)}")
        return jsonify({"error": f"Failed to remove account from group: {str(e)}"}), 500
    return jsonify({"message": f"Removed {client_id} from {group_name}"})


@app.route('/api/groups/<group_name>/rename', methods=['POST'])
@login_required
def rename_group(group_name):
    """Rename an existing group."""
    new_name = request.json.get('new_name')
    if not new_name:
        return jsonify({'error': 'Missing new_name'}), 400

    user_obj = current_user()
    if not user_obj:
        return jsonify({'error': 'User not found'}), 400

    group = Group.query.filter_by(user_id=user_obj.id, name=group_name).first()
    if not group:
        return jsonify({'error': 'Group not found'}), 404

    if Group.query.filter_by(user_id=user_obj.id, name=new_name).first():
        return jsonify({'error': 'Group name already exists'}), 400

    group.name = new_name
    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.error(f"Failed to rename group: {str(e)}")
        return jsonify({'error': f"Failed to rename group: {str(e)}"}), 500
    return jsonify({'message': f"Group renamed to {new_name}"})


@app.route('/api/groups/<group_name>', methods=['DELETE'])
@login_required
def delete_group(group_name):
    """Delete a group."""
    user_obj = current_user()
    if not user_obj:
        return jsonify({'error': 'User not found'}), 400

    group = Group.query.filter_by(user_id=user_obj.id, name=group_name).first()
    if not group:
        return jsonify({'error': 'Group not found'}), 404

    try:
        db.session.delete(group)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.error(f"Failed to delete group: {str(e)}")
        return jsonify({'error': f"Failed to delete group: {str(e)}"}), 500

    return jsonify({'message': f"Group {group_name} deleted"})


@app.route('/api/symbols')
@login_required
def symbols_list():
    """Return list of symbols with token mappings for common brokers."""
    return jsonify(get_symbols())


@app.route('/api/quote/<symbol>')
@login_required
def quote_price(symbol):
    """Return latest price for a symbol using the primary account broker."""
    user = current_user()
    account = get_primary_account(user)
    if not account or not account.credentials:
        return jsonify({'error': 'Account not configured'}), 400

    try:
        api = broker_api(_account_to_dict(account))
        broker = (account.broker or 'dhan').lower()
        mapping = get_symbol_for_broker(symbol, broker)

        price = None
        if broker == 'dhan':
            sid = mapping.get('security_id')
            seg = mapping.get('exchange_segment', api.NSE)
            if sid:
                resp = api._request(
                    'post',
                    f"{api.api_base}/marketfeed/ltp",
                    json={seg: [int(sid)]},
                    headers=api.headers,
                    timeout=api.timeout,
                )
                data = resp.json()
                price = (
                    data.get('data', {})
                    .get(seg, {})
                    .get(str(sid), {})
                    .get('ltp')
                )

        if price is None:
            return jsonify({'error': 'Price not available'}), 400
        return jsonify({'price': float(price)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


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
    user_obj = current_user() # Use current_user() helper
    if not user_obj:
        return jsonify({"error": "User not found"}), 400
    group = Group.query.filter_by(user_id=user_obj.id, name=group_name).first()
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
                # Assuming your symbol_map provides 'symbol', 'token', and 'exchange' for Finvasia
                finvasia_symbol = mapping.get("symbol", symbol) # Use 'symbol' key from your mapping
                finvasia_token = mapping.get("token") # Extract the Finvasia token
                finvasia_exchange = mapping.get("exchange", "NSE") # Use 'exchange' key from your mapping

                if not finvasia_token:
                    raise ValueError(f"Finvasia token not found in symbol map for {symbol}")

                order_params = dict(
                    tradingsymbol=finvasia_symbol,
                    exchange=finvasia_exchange,
                    transaction_type=action.upper(),
                    quantity=int(quantity),
                    order_type=map_order_type("MARKET", broker_name),
                    product="MIS", # Or map to 'C' for CNC, 'H' for NRML based on your needs
                    price=0, # Market orders typically have price=0, or adjust for Limit orders
                    token=finvasia_token, # <<< CRITICAL: Pass the extracted token here
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

            record_trade(
                user_email,
                symbol,
                action.upper(),
                quantity,
                order_params.get('price'),
                status,
                broker=broker_name,
                client_id=acc.get('client_id'),
            )
        except Exception as e:
            results.append({"client_id": acc.get("client_id"), "status": "ERROR", "reason": str(e)})

    return jsonify(results)
    
# Set master account
@app.route('/api/set-master', methods=['POST'])
@login_required
def set_master():
    try:
        client_id = request.json.get('client_id')
        if not client_id:
            return jsonify({"error": "Missing client_id"}), 400

        user_email = session.get("user")
        user = current_user() # Use current_user() helper
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
        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            logger.error(f"Failed to set master role: {str(e)}")
            return jsonify({"error": f"Failed to set master role: {str(e)}"}), 500

        return jsonify({"message": "Set as master successfully"})
    except Exception as e:
        # Catch all for unexpected errors
        logger.error(f"Unexpected error in set_master: {str(e)}")
        db.session.rollback()
        return jsonify({"error": "Internal server error", "details": str(e)}), 500

@app.route('/api/set-child', methods=['POST'])
@login_required
def set_child():
    try:
        client_id = request.json.get('client_id')
        linked_master_id = request.json.get('linked_master_id')
        value_limit = request.json.get('value_limit')
        multiplier = request.json.get('multiplier')
        
        if not client_id or not linked_master_id:
            return jsonify({"error": "Missing client_id or linked_master_id"}), 400

        user_email = session.get("user")
        user = current_user() # Use current_user() helper
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
        # Value limit takes priority over multiplier. If a value limit is
        # provided we store it and reset the multiplier to 1.0 so it is
        # effectively ignored during copying.
        if value_limit is not None:
            try:
                account.copy_value_limit = float(value_limit)
            except (TypeError, ValueError):
                account.copy_value_limit = None
            account.multiplier = 1.0
        else:
            account.copy_value_limit = None
            try:
                account.multiplier = float(multiplier or 1.0)
            except (TypeError, ValueError):
                account.multiplier = 1.0
        account.copied_value = 0.0
        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            logger.error(f"Failed to set child role: {str(e)}")
            return jsonify({"error": f"Failed to set child role: {str(e)}"}), 500

        return jsonify({"message": "Set as child successfully"})
    except Exception as e:
        # Catch all for unexpected errors
        logger.error(f"Unexpected error in set_child: {str(e)}")
        db.session.rollback()
        return jsonify({"error": "Internal server error", "details": str(e)}), 500

# Start copying for a child account
@app.route('/api/start-copy', methods=['POST'])
@login_required
def start_copy():
    """Start copying for a child account - Complete Database Version.
    
    Expected POST data:
        {
            "client_id": "string",
            "master_id": "string"
        }
        
    Returns:
        JSON response with success message or error
    """
    logger.info("Processing start copy request")
    
    try:
        # ✅ STEP 1: Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        client_id = data.get("client_id")
        master_id = data.get("master_id")
        
        if not client_id or not master_id:
            logger.error(f"Missing required fields: client_id={bool(client_id)}, master_id={bool(master_id)}")
            return jsonify({"error": "Missing client_id or master_id"}), 400

        # ✅ STEP 2: Get current user from session
        user_email = session.get("user")
        user = current_user() # Use current_user() helper
        if not user:
            return jsonify({"error": "User not logged in"}), 401
            
        # ✅ STEP 3: Find and validate child account
        child_account = Account.query.filter_by(
            user_id=user.id, 
            client_id=client_id
        ).first()
        
        if not child_account:
            logger.error(f"Child account not found: {client_id}")
            return jsonify({"error": "Child account not found"}), 404

        # ✅ STEP 4: Find and validate master account
        master_account = Account.query.filter_by(
            client_id=master_id,
            role='master',
            user_id=user.id
        ).first()
        
        if not master_account:
            logger.error(f"Master account not found: {master_id}")
            return jsonify({"error": "Master account not found"}), 404

        # ✅ STEP 5: Check if master belongs to same user or is accessible
        if master_account.user_id != user.id:
            logger.warning(f"User {user_email} trying to access master {master_id} owned by different user")
            return jsonify({"error": "Master account not accessible"}), 403

        # ✅ STEP 6: Update child account configuration
        previous_status = child_account.copy_status
        child_account.role = "child"
        child_account.linked_master_id = master_id
        child_account.copy_status = "On"

        # Reset copied value when copying is re-enabled
        if previous_status != "On":
            child_account.copied_value = 0.0
            logger.debug(
                f"Reset copied value for {client_id} as copying was re-enabled"
            )

        logger.info(f"Setting up child {client_id} to copy from master {master_id}")

        # ✅ STEP 7: Get latest order ID from master to set initial marker
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

        # ✅ STEP 8: Set the marker to prevent copying historical orders
        child_account.last_copied_trade_id = latest_order_id
        
        # ✅ STEP 9: Commit changes to database
        try:
            db.session.commit()
            logger.info(f"Successfully started copying: {client_id} -> {master_id}, marker: {latest_order_id}")
        except Exception as e:
            logger.error(f"Failed to commit changes: {str(e)}")
            db.session.rollback()
            return jsonify({
                "error": "Failed to save configuration",
                "details": str(e)
            }), 500

        # ✅ STEP 10: Return success response
        return jsonify({
            'message': f"Started copying for {client_id}",
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
        # Ensure rollback on unexpected error before commit
        db.session.rollback()
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500
    
@app.route('/api/stop-copy', methods=['POST'])
@login_required
def stop_copy():
    """Stop copying for a child account - Complete Database Version.
    
    Expected POST data:
        {
            "client_id": "string",
            "master_id": "string" (optional, for validation)
        }
        
    Returns:
        JSON response with success message or error
    """
    logger.info("Processing stop copy request")
    
    try:
        # ✅ STEP 1: Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        client_id = data.get("client_id")
        master_id = data.get("master_id")  # Optional for validation
        
        if not client_id:
            logger.error("Missing client_id in request")
            return jsonify({"error": "Missing client_id"}), 400

        # ✅ STEP 2: Get current user from session
        user_email = session.get("user")
        user = current_user() # Use current_user() helper
        if not user:
            return jsonify({"error": "User not logged in"}), 401
            
        # ✅ STEP 3: Find and validate child account
        child_account = Account.query.filter_by(
            user_id=user.id, 
            client_id=client_id
        ).first()
        
        if not child_account:
            logger.error(f"Child account not found: {client_id}")
            return jsonify({"error": "Child account not found"}), 404

        # ✅ STEP 4: Validate current state
        if child_account.role != "child":
            logger.warning(f"Account {client_id} is not configured as child (role: {child_account.role})")
            return jsonify({"error": "Account is not configured as a child"}), 400

        if child_account.copy_status != "On":
            logger.info(f"Copy already stopped for {client_id} (status: {child_account.copy_status})")
            return jsonify({
                "message": f"Copy trading is already stopped for {client_id}",
                "current_status": child_account.copy_status
            }), 200

        # ✅ STEP 5: Optional master validation
        current_master_id = child_account.linked_master_id
        if master_id and master_id != current_master_id:
            logger.warning(f"Master ID mismatch: expected {master_id}, current {current_master_id}")
            return jsonify({
                "error": "Master ID mismatch",
                "expected": master_id,
                "current": current_master_id
            }), 400

        # ✅ STEP 6: Get master account details for logging
        master_account = None
        if current_master_id:
            master_account = Account.query.filter_by(
                client_id=current_master_id,
                user_id=user.id
            ).first()

        # ✅ STEP 7: Stop copying configuration
        logger.info(f"Stopping copy trading for {client_id} from master {current_master_id}")
        
        # Store previous state for response
        previous_state = {
            "role": child_account.role,
            "linked_master_id": child_account.linked_master_id,
            "copy_status": child_account.copy_status,
            "multiplier": child_account.multiplier,
            "last_copied_trade_id": child_account.last_copied_trade_id
        }
        
        # Update child account - stop copying but keep role and relationship
        child_account.copy_status = "Off"
        # Note: We keep role="child" and linked_master_id for potential restart
        
        # ✅ STEP 8: Commit changes to database
        try:
            db.session.commit()
            logger.info(f"Successfully stopped copying for {client_id}")
        except Exception as e:
            logger.error(f"Failed to commit changes: {str(e)}")
            db.session.rollback()
            return jsonify({
                "error": "Failed to save configuration",
                "details": str(e)
            }), 500

        # ✅ STEP 9: Log the action for audit trail
        try:
            log_entry = SystemLog(
                timestamp=datetime.now().isoformat(),
                level="INFO",
                message=f"Copy trading stopped: {client_id} -> {current_master_id}",
                user_id=str(user.id),
                details=json.dumps({
                    "action": "stop_copy",
                    "child_id": client_id,
                    "master_id": current_master_id,
                    "user": user_email,
                    "previous_state": previous_state
                })
            )
            db.session.add(log_entry)
            db.session.commit()
        except Exception as e:
            logger.warning(f"Failed to log action: {str(e)}")
            # Don't fail the request if logging fails

        # ✅ STEP 10: Return success response with details
        response_data = {
            'message': f"Stopped copying for {client_id}",
            'details': {
                'child_account': client_id,
                'master_account': current_master_id,
                'copy_status': 'Off',
                'role': child_account.role,  # Still "child"
                'broker': child_account.broker,
                'stopped_at': datetime.now().isoformat()
            }
        }
        
        # Add master details if available
        if master_account:
            response_data['details']['master_broker'] = master_account.broker
            response_data['details']['master_username'] = master_account.username

        return jsonify(response_data), 200

    except Exception as e:
        logger.error(f"Unexpected error in stop_copy: {str(e)}")
        # Ensure rollback on unexpected error before commit
        db.session.rollback()
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500


# Bulk start copying for all children of a master
@app.route('/api/start-copy-all', methods=['POST'])
@login_required
def start_copy_all():
    """Start copying for all children of a master - Complete Database Version.
    
    Expected POST data:
        {
            "master_id": "string"
        }
        
    Returns:
        JSON response with bulk operation results
    """
    logger.info("Processing bulk start copy request")
    
    try:
        # ✅ STEP 1: Validate request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        master_id = data.get("master_id")
        
        if not master_id:
            logger.error("Missing master_id in request")
            return jsonify({"error": "Missing master_id"}), 400

        # ✅ STEP 2: Get current user from session
        user_email = session.get("user")
        user = current_user() # Use current_user() helper
        if not user:
            return jsonify({"error": "User not logged in"}), 401
            
        # ✅ STEP 3: Validate master account exists and belongs to user
        master_account = Account.query.filter_by(
            user_id=user.id,
            client_id=master_id,
            role='master'
        ).first()
        
        if not master_account:
            logger.error(f"Master account not found or not accessible: {master_id}")
            return jsonify({"error": "Master account not found or not accessible"}), 404

        # ✅ STEP 4: Find all child accounts linked to this master
        linked_children = Account.query.filter_by(
            user_id=user.id,
            role='child',
            linked_master_id=master_id
        ).all()

        # ✅ STEP 5: Filter only children that are currently stopped
        stopped_children = [child for child in linked_children if child.copy_status == "Off"]

        # ✅ STEP 6: Check if there are any stopped children to start
        if not stopped_children:
            logger.info(f"No stopped children found for master {master_id}")
            
            # Check if all are already copying
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

        # ✅ STEP 7: Get latest order from master for initial marker
        master_latest_order_id = "NONE"
        
        try:
            # Convert master account to dict for broker_api compatibility
            master_dict = _account_to_dict(master_account)
            master_api = broker_api(master_dict)
            
            # Get orders based on broker type
            broker_name = master_account.broker.lower() if master_account.broker else "unknown"
            
            if broker_name == "aliceblue" and hasattr(master_api, "get_trade_book"):
                logger.debug("Using AliceBlue trade book for bulk marker")
                orders_resp = master_api.get_trade_book()
                order_list = parse_order_list(orders_resp)
                
                # Fallback to order list if trade book is empty
                if not order_list and hasattr(master_api, "get_order_list"):
                    logger.debug("Trade book empty, falling back to order list")
                    orders_resp = master_api.get_order_list()
                    order_list = parse_order_list(orders_resp)
            else:
                logger.debug("Using standard order list for bulk marker")
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
                        master_latest_order_id = (
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

        # ✅ STEP 8: Process each stopped child account
        results = []
        started_count = 0
        failed_count = 0

        for child in stopped_children:
            try:
                # Store previous state for logging
                previous_state = {
                    "client_id": child.client_id,
                    "copy_status": child.copy_status,
                    "multiplier": child.multiplier,
                    "last_copied_trade_id": child.last_copied_trade_id
                }

                # Start copying for this child
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

        # ✅ STEP 9: Commit all changes to database
        try:
            db.session.commit()
            logger.info(f"Successfully started copying for {started_count} children of master {master_id}")
        except Exception as e:
            logger.error(f"Failed to commit bulk changes: {str(e)}")
            db.session.rollback()
            return jsonify({
                "error": "Failed to save bulk configuration changes",
                "details": str(e),
                "partial_success": False
            }), 500

        # ✅ STEP 10: Log the bulk action for audit trail
        try:
            log_entry = SystemLog(
                timestamp=datetime.utcnow().isoformat(),
                level="INFO",
                message=f"Bulk start copy: {started_count} children started for master {master_id}",
                user_id=str(user.id),
                details=json.dumps({
                    "action": "start_copy_all",
                    "master_id": master_id,
                    "user": user_email,
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
            # Don't fail the request if logging fails

        # ✅ STEP 11: Prepare comprehensive response
        response_data = {
            "message": f"Bulk start completed for master {master_id}",
            "master_id": master_id,
            "master_broker": master_account.broker,
            "master_username": master_account.username,
            "master_marker": master_latest_order_id,
            "summary": {
                "total_children": len(linked_children),
                "eligible_to_start": len(stopped_children),
                "started_successfully": started_count,
                "failed": failed_count,
                "already_copying": len(linked_children) - len(stopped_children),
                "success_rate": f"{(started_count/len(stopped_children)*100):.1f}%" if stopped_children else "0%"
            },
            "details": results,
            "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        }

        # ✅ STEP 12: Return appropriate status code based on results
        if failed_count == 0:
            # Complete success
            return jsonify(response_data), 200
        elif started_count > 0:
            # Partial success
            response_data["warning"] = f"{failed_count} accounts failed to start"
            return jsonify(response_data), 207  # Multi-Status
        else:
            # Complete failure
            response_data["error"] = "Failed to start any child accounts"
            return jsonify(response_data), 500

    except Exception as e:
        logger.error(f"Unexpected error in start_copy_all: {str(e)}")
        # Ensure rollback on unexpected error before commit
        db.session.rollback()
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500

# Bulk stop copying for all children of a master
@app.route('/api/stop-copy-all', methods=['POST'])
@login_required
def stop_copy_all():
    """Bulk stop copying for all children of a master - Complete Database Version."""
    logger.info("Processing bulk stop copy request")
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        master_id = data.get("master_id")
        if not master_id:
            logger.error("Missing master_id in request")
            return jsonify({"error": "Missing master_id"}), 400

        user_email = session.get("user")
        user = current_user() # Use current_user() helper
        if not user:
            logger.error(f"User not found: {user_email}")
            return jsonify({"error": "User not found"}), 404

        master_account = Account.query.filter_by(user_id=user.id, client_id=master_id, role='master').first()
        if not master_account:
            logger.error(f"Master account not found or not accessible: {master_id}")
            return jsonify({"error": "Master account not found or not accessible"}), 404

        active_children = Account.query.filter_by(user_id=user.id, role='child', linked_master_id=master_id, copy_status='On').all()
        if not active_children:
            logger.info(f"No active children found for master {master_id}")
            return jsonify({"message": f"No active child accounts found for master {master_id}", "master_id": master_id, "stopped_count": 0, "details": []}), 200

        results = []
        stopped_count = 0
        failed_count = 0
        
        current_time_iso = datetime.utcnow().isoformat()

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
                logger.error(f"Failed to stop copying for child {child.client_id}: {str(e)}")
                results.append({"client_id": child.client_id, "status": "ERROR", "message": f"Failed to stop: {str(e)}"})
                failed_count += 1

        db.session.commit()

        log_entry = SystemLog(
            timestamp=current_time_iso,
            level="INFO",
            message=f"Bulk stop copy: {stopped_count} children stopped for master {master_id}",
            user_id=str(user.id),
            details=json.dumps({
                "action": "stop_copy_all",
                "master_id": master_id,
                "user": user_email,
                "stopped_count": stopped_count,
                "failed_count": failed_count,
                "timestamp": current_time_iso, # Corrected Timestamp
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
            "details": results,
            "timestamp": current_time_iso # Corrected Timestamp
        }

        if failed_count == 0:
            return jsonify(response_data), 200
        elif stopped_count > 0:
            return jsonify(response_data), 207
        else:
            return jsonify(response_data), 500

    except Exception as e:
        logger.error(f"Unexpected error in stop_copy_all: {str(e)}")
        db.session.rollback()
        return jsonify({"error": "Internal server error", "details": str(e)}), 500

# Exit all open positions for a single child account
@app.route('/api/exit-child-positions', methods=['POST'])
@login_required
def exit_child_positions():
    logger.info("Processing exit child positions request")
    try:
        data = request.get_json(silent=True)
        if isinstance(data, str):
            try:
                parsed = json.loads(data)
                data = parsed if isinstance(parsed, dict) else {}
            except Exception:
                data = {}
        elif not isinstance(data, dict):
            data = {}
        if data is None:
            data = {}
        child_id = data.get('child_id')
        if not child_id:
            return jsonify({'error': 'Missing child_id'}), 400

        user = current_user()
        child = Account.query.filter_by(user_id=user.id, client_id=child_id, role='child').first()
        if not child:
            return jsonify({'error': 'Child account not found'}), 404

        results = exit_all_positions_for_account(child)
        successes = [
            r for r in results
            if str(r.get('status', '')).upper() == 'SUCCESS'
        ]
        failures = [
            r for r in results
            if str(r.get('status', '')).upper() != 'SUCCESS'
        ]
        exited = bool(successes)
        message = (
            f"Exited {len(successes)} of {len(results)} positions for {child_id}"
            if results and successes
            else f"No positions exited for {child_id}"
        )
        return jsonify({
            'message': message,
            'child_id': child_id,
            'results': results,
            'exited': exited,
            'failed_count': len(failures)
        }), 200
    except Exception as e:
        logger.error(f"Unexpected error in exit_child_positions: {str(e)}")
        return jsonify({'error': 'Internal server error', 'details': str(e)}), 500


# Exit all open positions for every child under a master
@app.route('/api/exit-all-children', methods=['POST'])
@login_required
def exit_all_children():
    logger.info("Processing exit all children request")
    try:
        data = request.get_json(silent=True)
        if isinstance(data, str):
            try:
                parsed = json.loads(data)
                data = parsed if isinstance(parsed, dict) else {}
            except Exception:
                data = {}
        elif not isinstance(data, dict):
            data = {}
        if data is None:
            data = {}
        master_id = data.get('master_id')
        if not master_id:
            return jsonify({'error': 'Missing master_id'}), 400
        child_ids = data.get('child_ids') or []

        user = current_user()
        master = Account.query.filter_by(user_id=user.id, client_id=master_id, role='master').first()
        if not master:
            return jsonify({'error': 'Master account not found'}), 404

        query = Account.query.filter_by(user_id=user.id, role='child', linked_master_id=master_id)
        if child_ids:
            query = query.filter(Account.client_id.in_(child_ids))
        children = query.all()
        if not children:
            return jsonify({'message': 'No child accounts found', 'master_id': master_id, 'exited_children': []}), 200

        all_results = {}
        exited = []
        failed = []
        for child in children:
            res = exit_all_positions_for_account(child)
            all_results[child.client_id] = res
            if any(str(r.get('status', '')).upper() == 'SUCCESS' for r in res):
                exited.append(child.client_id)
            else:
                failed.append(child.client_id)

        message = (
            f"Exited positions for {len(exited)} of {len(children)} children"
            if exited else
            "No positions exited for any child"
        )

        return jsonify({
            'message': message,
            'master_id': master_id,
            'results': all_results,
            'exited_children': exited,
            'failed_children': failed
        }), 200
    except Exception as e:
        logger.error(f"Unexpected error in exit_all_children: {str(e)}")
        return jsonify({'error': 'Internal server error', 'details': str(e)}), 500



# === Endpoint to fetch passive alert logs ===
@app.route("/api/alerts")
def get_alerts():
    user_id = request.args.get("user_id")
    # It's better to fetch the actual User object to ensure the user_id is valid and belongs to the current session user
    current_logged_in_user = current_user()
    if not current_logged_in_user or str(current_logged_in_user.id) != user_id:
        return jsonify({"error": "Unauthorized"}), 401

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

# === API to save new user from login form ===
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
        # db.session.commit() # Commit after adding account for a single transaction

    account = Account.query.filter_by(user_id=user.id, client_id=client_id).first()
    creds = {"access_token": access_token}
    if not account:
        account = Account(user_id=user.id, broker=broker, client_id=client_id, credentials=creds)
        db.session.add(account)
    else:
        account.credentials = creds
    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.error(f"Failed to register user/account: {str(e)}")
        return jsonify({"error": f"Failed to register: {str(e)}"}), 500

    return jsonify({"status": "User registered successfully", "webhook": f"/webhook/{token}"})


# === API to fetch logs for a user ===
@app.route("/logs")
@login_required
def get_logs():
    user = current_user()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401
    rows = (
        TradeLog.query.filter_by(user_id=str(user.id))
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
                # Read the file data and store as a data URL so it persists in the
                # database even if the filesystem is wiped during redeploys.
                import base64
                data = file.read()
                encoded = base64.b64encode(data).decode("utf-8")
                mime_type = file.mimetype or "application/octet-stream"
                user.profile_image = f"data:{mime_type};base64,{encoded}"
            message = "Profile updated"

            try:
                db.session.commit()
            except Exception as e:
                db.session.rollback()
                flash(f"Failed to update profile: {str(e)}", "error")
                logger.error(f"Failed to update user profile {username}: {str(e)}")


    profile_data = {
        "email": username,
        "first_name": (user.name or "").split(" ")[0] if user.name else "",
        "last_name": (user.name or "").split(" ")[1] if user.name and len(user.name.split(" ")) > 1 else "",
        "plan": user.plan,
        "profile_image": user.profile_image or "user.png",
    }

    return render_template("user.html", user=profile_data, message=message)

@app.route('/profile-images/<path:filename>')
def profile_image_file(filename):
    """Serve uploaded profile images from the persistent directory."""
    return send_from_directory(app.config["PROFILE_IMAGE_DIR"], filename)

# === Page routes ===
@app.route('/')
def home():
    return render_template("index.html")

@app.route('/dhan-dashboard')
@login_required
def dhan_dashboard():
    return render_template("dhan-dashboard.html")

@app.route('/demat-dashboard')
@login_required
def demat_dashboard():
    return render_template("demat-dashboard.html")

@app.route('/demat-notifications')
@login_required
def demat_notifications():
    return render_template("demat-notifications.html")

@app.route('/demat-strategies')
@login_required
def demat_strategies():
    return render_template("demat-strategies.html")

@app.route("/strategy-performance/<int:strategy_id>")
@login_required
def strategy_performance(strategy_id):
    user = current_user()
    strategy = Strategy.query.filter_by(id=strategy_id, user_id=user.id).first_or_404()
    return render_template("strategy-performance.html", strategy=strategy)

@app.route("/strategy-marketplace")
@login_required
def strategy_marketplace():
    strategies = Strategy.query.filter_by(is_public=True, is_active=True).all()
    return render_template("strategy-marketplace.html", strategies=strategies)
    
@app.route('/demat-subscriptions')
@login_required
def demat_subscriptions():
    return render_template("demat-subscriptions.html")

@app.route('/demat-webhooks')
@login_required
def demat_webhooks():
    return render_template("demat-webhooks.html")

@app.route('/demat-brokers')
@login_required
def demat_brokers():
    return render_template("demat-brokers.html")

@app.route('/demat-account')
@login_required
def demat_account():
    return render_template("demat-account.html")

@app.route('/api/account-info')
def account_info():
    """API endpoint for account information"""
    # In a real application, this would fetch from a database or external API
    # For now, returning empty data structure
    return jsonify({
        'accounts': [],
        'balance': 0.00,
        'status': 'No account data available'
    })

@app.route('/api/dashboard-data')
def dashboard_data():
    """API endpoint for dashboard data"""
    return jsonify({
        'notifications': [],
        'strategies': [],
        'recent_activity': [],
        'status': 'No data available'
    })

@app.route("/Summary")
@login_required
def summary():
    if 'username' not in session:
        user = current_user()
        if user:
            session['username'] = user.name or (user.email.split('@')[0] if user.email else user.phone)
    return render_template("Summary.html")  # or "Summary.html" if that's your file name

@app.route("/copy-trading")
@login_required
def copytrading():
    return render_template("copy-trading.html")

@app.route("/Add-Account")
@login_required
def AddAccount():
    return render_template("Add-Account.html")

@app.route("/groups")
@login_required
def groups_page():
    return render_template("groups.html")

# === Admin routes ===
@app.route('/adminlogin', methods=['GET', 'POST'])
def admin_login():
    import os
    error = None

    if request.method == 'POST':
        input_email = request.form.get('email')
        input_password = request.form.get('password')

        # Compare with Render env vars
        # Use configured admin credentials
        admin_email = ADMIN_EMAIL
        admin_password = ADMIN_PASSWORD


        if input_email == admin_email and input_password == admin_password:
            session['admin'] = admin_email
            return redirect(url_for('admin_dashboard'))  # Replace with your dashboard route
        else:
            error = 'Invalid credentials'

    return render_template('login.html', error=error)


@app.route('/adminlogout')
def admin_logout():
    session.pop('admin', None)
    return redirect(url_for('admin_login'))

@app.route('/admindashboard')
@admin_login_required
def admin_dashboard():
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
    return render_template('dashboard.html', metrics=metrics, api_status=api_status, trade_chart=trade_chart, signup_chart=signup_chart)

@app.route('/adminusers')
@admin_login_required
def admin_users():
    users = load_users()
    return render_template('users.html', users=users)

@app.route('/adminbrokers')
@admin_login_required
def admin_brokers():
    accounts = load_accounts()
    broker_names = sorted({acc.broker for acc in accounts if acc.broker})
    return render_template('brokers.html', accounts=accounts, broker_names=broker_names)

@app.route('/admintrades')
@admin_login_required
def admin_trades():
    trades = load_trades()
    return render_template('trades.html', trades=trades)

@app.route('/adminsubscriptions')
@admin_login_required
def admin_subscriptions():
    users = load_users()
    subs = [u for u in users]
    return render_template('subscriptions.html', subscriptions=subs)

@app.route('/adminlogs')
@admin_login_required
def admin_logs():
    webhook_logs, system_logs = load_logs()
    return render_template('logs.html', webhook_logs=webhook_logs, system_logs=system_logs)

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
        try:
            save_settings(settings)
            flash("Settings updated successfully!", "success")
        except Exception as e:
            flash(f"Failed to save settings: {str(e)}", "error")
            logger.error(f"Failed to save admin settings: {str(e)}")

    return render_template('settings.html', settings=settings)

@app.route('/adminprofile')
@admin_login_required
def admin_profile():
    return render_template('profile.html', admin={'email': session.get('admin')})

# ---- Admin action routes ----

@app.route('/adminusers/<user_id>/suspend', methods=['POST'])
@admin_login_required
def admin_suspend_user(user_id):
    user = User.query.get_or_404(user_id)
    user.plan = 'Suspended'
    try:
        db.session.commit()
        flash(f'User {user.email} suspended.')
    except Exception as e:
        db.session.rollback()
        flash(f"Failed to suspend user: {str(e)}", "error")
        logger.error(f"Failed to suspend user {user_id}: {str(e)}")
    return redirect(url_for('admin_users'))


@app.route('/adminusers/<user_id>/reset', methods=['POST'])
@admin_login_required
def admin_reset_password(user_id):
    user = User.query.get_or_404(user_id)
    new_pass = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    user.set_password(new_pass)
    try:
        db.session.commit()
        flash(f'New password for {user.email}: {new_pass}')
    except Exception as e:
        db.session.rollback()
        flash(f"Failed to reset password: {str(e)}", "error")
        logger.error(f"Failed to reset password for user {user_id}: {str(e)}")
    return redirect(url_for('admin_users'))


@app.route('/adminusers/<user_id>')
@admin_login_required
def admin_view_user(user_id):
    user = User.query.get_or_404(user_id)
    return render_template('user_detail.html', user=user)


@app.route('/adminbrokers/<account_id>/revoke', methods=['POST'])
@admin_login_required
def admin_revoke_account(account_id):
    account = Account.query.get_or_404(account_id)
    account.status = 'Revoked'
    try:
        db.session.commit()
        flash('Account revoked')
    except Exception as e:
        db.session.rollback()
        flash(f"Failed to revoke account: {str(e)}", "error")
        logger.error(f"Failed to revoke account {account_id}: {str(e)}")
    return redirect(url_for('admin_brokers'))


@app.route('/admintrades/<trade_id>/retry', methods=['POST'])
@admin_login_required
def admin_retry_trade(trade_id):
    trade = Trade.query.get_or_404(trade_id)
    trade.status = 'Pending'
    try:
        db.session.commit()
        flash('Trade marked for retry')
    except Exception as e:
        db.session.rollback()
        flash(f"Failed to mark trade for retry: {str(e)}", "error")
        logger.error(f"Failed to retry trade {trade_id}: {str(e)}")
    return redirect(url_for('admin_trades'))


@app.route('/adminsubscriptions/<user_id>/change', methods=['POST'])
@admin_login_required
def admin_change_subscription(user_id):
    user = User.query.get_or_404(user_id)
    user.plan = 'Pro' if user.plan != 'Pro' else 'Free'
    try:
        db.session.commit()
        flash(f'Plan updated to {user.plan} for {user.email}')
    except Exception as e:
        db.session.rollback()
        flash(f"Failed to change subscription: {str(e)}", "error")
        logger.error(f"Failed to change subscription for user {user_id}: {str(e)}")
    return redirect(url_for('admin_subscriptions'))

start_scheduler()

if __name__ == '__main__':
    # It's generally better to run `ensure_system_log_schema()`
    # within an app context, perhaps before app.run() or as part of a setup script.
    # It's already called at the module level in the original code,
    # which is fine as long as `app` and `db` are initialized by then.
    debug = os.environ.get("FLASK_DEBUG") == "1"
    app.run(debug=debug)
