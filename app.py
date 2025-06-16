from brokers.factory import get_broker_class
import uuid
from brokers.zerodha import ZerodhaBroker, KiteConnect
try:
    from brokers.symbol_map import get_symbol_for_broker
except Exception:  # pragma: no cover - fallback if import fails
    def get_symbol_for_broker(symbol: str, broker: str) -> dict:
        """Fallback stub returning empty mapping."""
        return {}
from flask import Flask, request, jsonify, render_template, session, redirect, url_for, flash
from dhanhq import dhanhq
import sqlite3
import os
import json
import pandas as pd
from flask_cors import CORS
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
from models import db, User, Account, Trade, WebhookLog, SystemLog, Setting
from werkzeug.security import generate_password_hash, check_password_hash
import re

app = Flask(__name__)
app.secret_key = "change-me"
CORS(app)
DB_PATH = os.path.join("/tmp", "quantbot.db")
app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{DB_PATH}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)
start_time = datetime.utcnow()
device_number = None

BROKER_STATUS_URLS = {
    "dhan": "https://api.dhan.co",
    "zerodha": "https://api.kite.trade",
    "aliceblue": "https://ant.aliceblueonline.com",
    "finvasia": "https://api.shoonya.com",
    "fyers": "https://api.fyers.in",
}


def map_order_type(order_type: str, broker: str) -> str:
    """Convert generic order types to broker specific codes."""
    if not order_type:
        return ""
    if broker and broker.lower() == "aliceblue" and order_type.upper() == "MARKET":
        return "MKT"
    return str(order_type)

def safe_write_json(path, data):
    dirpath = os.path.dirname(path) or '.'
    with tempfile.NamedTemporaryFile('w', delete=False, dir=dirpath) as tmp:
        json.dump(data, tmp, indent=2)
        tmp.flush()
        os.fsync(tmp.fileno())
    shutil.move(tmp.name, path)

def safe_read_json(path):
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error reading {path}: {e}")
        return {}

EMOJI_RE = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)

def strip_emojis_from_obj(obj):
    """Recursively remove emoji characters from strings in lists/dicts."""
    if isinstance(obj, dict):
        return {k: strip_emojis_from_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [strip_emojis_from_obj(v) for v in obj]
    if isinstance(obj, str):
        return EMOJI_RE.sub('', obj)
    return obj

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
        return BrokerClass(client_id, password, totp_secret, vendor_code, api_key, imei, **rest)
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
        print(f"Failed to fetch balance for {acc.get('client_id')}: {e}")
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

def save_account_to_user(owner, account):
    """Persist account credentials in accounts.json."""
    path = "accounts.json"
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                data = json.load(f)
        except Exception:
            data = {"accounts": []}
    else:
        data = {"accounts": []}

    entry = dict(account)
    entry.setdefault("owner", owner)
    data.setdefault("accounts", []).append(entry)
    safe_write_json(path, data)

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


def find_account_by_client_id(accounts, client_id):
    """Return ``(account, parent_master)`` for the provided ``client_id``.

    ``accounts.json`` stores all records in a flat ``accounts`` list.  Values
    might be saved as strings or numbers depending on how they were imported.
    To make lookups robust we always compare the string version of the IDs.  If
    the located account has ``role == 'child'`` the corresponding master is
    returned as the second tuple element.
    """

    cid = str(client_id)
    account = next(
        (acc for acc in accounts.get("accounts", []) if str(acc.get("client_id")) == cid),
        None,
    )
    if not account:
        return None, None

    if account.get("role") == "child":
        master_id = account.get("linked_master_id")
        master = next(
            (acc for acc in accounts.get("accounts", []) if str(acc.get("client_id")) == str(master_id)),
            None,
        )
        return account, master

    return account, None


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
            response.get("message"),
            response.get("reason"),
        ]
        for field in error_fields:
            if field:
                return str(field)

        # Fallback to raw JSON dump for debugging
        return json.dumps(response)

    return str(response)


# === Initialize SQLite DB ===
def init_db():
    conn = sqlite3.connect("tradelogs.db")
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            user_id TEXT,
            symbol TEXT,
            action TEXT,
            quantity INTEGER,
            status TEXT,
            response TEXT
        )
    """)
    conn.commit()
    conn.close()

init_db()

# === Save logs ===
def save_log(user_id, symbol, action, quantity, status, response):
    conn = sqlite3.connect("tradelogs.db")
    c = conn.cursor()
    c.execute("""
        INSERT INTO logs (timestamp, user_id, symbol, action, quantity, status, response)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (datetime.now().isoformat(), user_id, symbol, action, quantity, status, response))
    conn.commit()
    conn.close()

def save_order_mapping(master_order_id, child_order_id, master_id, master_broker, child_id, child_broker, symbol):
    path = "order_mappings.json"
    mappings = []

    if os.path.exists(path):
        with open(path, "r") as f:
            mappings = json.load(f)

    mappings.append({
        "master_order_id": master_order_id,
        "child_order_id": child_order_id,
        "master_client_id": master_id,
        "master_broker": master_broker,
        "child_client_id": child_id,
        "child_broker": child_broker,
        "symbol": symbol,
        "status": "ACTIVE"
    })

    with open(path, "w") as f:
        json.dump(mappings, f, indent=2)


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

# Admin authentication
ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "admin@example.com")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "admin")

def admin_login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("admin"):
            return redirect(url_for("admin_login"))
        return view(*args, **kwargs)
    return wrapped
    
def poll_and_copy_trades():
    """Run trade copying logic with application context for DB access."""
    with app.app_context():
        print("🔄 poll_and_copy_trades() triggered...")
        try:
            if not os.path.exists("accounts.json"):
                print("⚠️ No accounts.json file found.")
                return

            with open("accounts.json", "r") as f:
                accounts_data = json.load(f)
            all_accounts = accounts_data.get("accounts", [])

            # Find all masters
            masters = [acc for acc in all_accounts if acc.get("role") == "master"]

            if not masters:
                print("⚠️ No master accounts configured.")
                return

            for master in masters:
                master_id = master.get("client_id")
                master_broker = master.get("broker", "Unknown").lower()
                credentials = master.get("credentials", {})
                BrokerClass = get_broker_class(master_broker)
                try:
                    rest = {k: v for k, v in credentials.items() if k != "access_token"}
                    if master_broker == "aliceblue":
                        master_api = BrokerClass(
                            master.get("client_id"),
                            credentials.get("api_key"),
                            device_number=credentials.get("device_number"),
                            **rest
                        )
                    else:
                        master_api = BrokerClass(
                            client_id=master.get("client_id"),
                            access_token=credentials.get("access_token"),
                            **rest
                        )
                except Exception as e:
                    print(f"❌ Could not initialize master API ({master_broker}): {e}")
                    continue

                last_copied_key = f"last_copied_trade_id_{master_id}"
                last_copied_trade_id = accounts_data.get(last_copied_key)
                new_last_trade_id = None

                # Get master orders using standard interface
                try:
                    orders_resp = master_api.get_order_list()
                    order_list = orders_resp.get("data", orders_resp.get("orders", []))
                    order_list = strip_emojis_from_obj(order_list)
                except Exception as e:
                    print(f"❌ Error fetching orders for master {master_id}: {e}")
                    continue

                if not order_list:
                    print(f"ℹ️ No orders found for master {master_id}.")
                    continue
                order_list = sorted(order_list, key=lambda x: x.get("orderTimestamp", x.get("order_time", "")), reverse=True)
                children = [acc for acc in all_accounts if acc.get("role") == "child" and acc.get("linked_master_id") == master_id]

                for order in order_list:
                    order_id = order.get("orderId") or order.get("order_id")
                    if not order_id:
                        continue

                    if order_id == last_copied_trade_id:
                        print(f"✅ [{master_id}] Reached last copied trade. Stopping here.")
                        break

                    order_status = order.get("orderStatus") or order.get("status") or ""
                    if order_status.upper() not in ["TRADED", "FILLED", "COMPLETE"]:
                        print(f"⏩ [{master_id}] Skipping order {order_id} (Status: {order_status})")
                        continue

                    print(f"✅ [{master_id}] New TRADED/FILLED order: {order_id}")
                    new_last_trade_id = new_last_trade_id or order_id
                    base_qty = (
                        order.get("quantity") or
                        order.get("orderQuantity") or
                        order.get("qty") or 1
                    )
                    price = float(order.get("price") or order.get("orderPrice") or order.get("avg_price") or 0)
                    transaction_type = (
                        order.get("transactionType") or
                        order.get("transaction_type") or
                        order.get("side") or
                        "BUY"
                    ).upper()
                    symbol = order.get("tradingSymbol") or order.get("symbol") or order.get("stock") or "UNKNOWN"
                    master_owner = master.get("owner")
                    record_trade(master_owner, symbol, transaction_type, base_qty, price, order_status.upper())
                    if not children:
                        print(f"ℹ️ [{master_id}] No children to copy trades to.")
                        continue
                    for child in children:
                        if child.get("copy_status") != "On":
                            print(f"➡️ Skipping child {child['client_id']} (copy_status is Off)")
                            continue

                        child_broker = child.get("broker", "Unknown").lower()
                        child_credentials = child.get("credentials", {})
                        try:
                            ChildBrokerClass = get_broker_class(child_broker)
                            rest_child = {k: v for k, v in child_credentials.items() if k != "access_token"}
                            if child_broker == "aliceblue":
                                child_api = ChildBrokerClass(
                                    child.get("client_id"),
                                    child_credentials.get("api_key"),
                                    device_number=child_credentials.get("device_number"),
                                    **rest_child
                                )
                            else:
                                child_api = ChildBrokerClass(
                                    client_id=child.get("client_id"),
                                    access_token=child_credentials.get("access_token"),
                                    **rest_child
                                )
                        except Exception as e:
                            print(f"❌ Could not initialize child API ({child_broker}): {e}")
                            continue

                        multiplier = float(child.get("multiplier", 1))
                        copied_qty = max(1, int(float(base_qty) * multiplier))

                        symbol = order.get("tradingSymbol") or order.get("symbol") or order.get("stock") or "UNKNOWN"
                        exchange = order.get("exchange") or order.get("exchangeSegment") or order.get("exchange_segment") or "NSE"
                        transaction_type = (
                            order.get("transactionType") or
                            order.get("transaction_type") or
                            order.get("side") or
                            "BUY"
                        ).upper()
                        order_type = (
                            order.get("orderType") or
                            order.get("order_type") or
                            "MARKET"
                        ).upper()
                        product_type = (
                            order.get("productType") or
                            order.get("ProductType") or
                            order.get("product_type") or
                            "INTRADAY"
                        ).upper()
                        price = float(order.get("price") or order.get("orderPrice") or order.get("avg_price") or 0)
                         # --- Map values between different broker conventions ---
                        if child_broker == "zerodha":
                            exchange = {
                                "NSE_EQ": "NSE",
                                "BSE_EQ": "BSE",
                                "NFO": "NFO",
                            }.get(str(exchange).upper(), str(exchange).upper())

                            product_type = {
                                "INTRADAY": "MIS",
                                "MARGIN": "MIS",
                                "DELIVERY": "CNC",
                                "CNC": "CNC",
                                "NRML": "NRML",
                            }.get(str(product_type).upper(), str(product_type).upper())


                        try:
                            mapping_child = get_symbol_for_broker(symbol, child_broker)
                            if child_broker == "dhan":
                                security_id = (
                                    order.get("securityId")
                                    or order.get("security_id")
                                    or mapping_child.get("security_id")
                                )
                                response = child_api.place_order(
                                    security_id=security_id,
                                    exchange_segment=exchange,
                                    transaction_type=transaction_type,
                                    quantity=copied_qty,
                                    order_type=order_type,
                                    product_type=product_type,
                                    price=price or 0
                                )
                            else:
                                tradingsymbol = mapping_child.get("tradingsymbol", symbol)
                                response = child_api.place_order(
                                    tradingsymbol=tradingsymbol,
                                    exchange=exchange,
                                    transaction_type=transaction_type,
                                    quantity=copied_qty,
                                    order_type=order_type,
                                    product=product_type,
                                    price=price or 0  # Always pass price, even if 0 for MARKET
                                )

                            if isinstance(response, dict) and response.get("status") == "failure":
                                error_msg = response.get("error") or response.get("remarks") or "Unknown error"
                                print(f"❌ Trade FAILED for {child['client_id']} (Reason: {error_msg})")
                                save_log(child['client_id'], symbol, transaction_type, copied_qty, "FAILED", error_msg)
                                record_trade(child.get('owner'), symbol, transaction_type, copied_qty, price, 'FAILED')
                            else:
                                order_id_child = response.get("order_id") or response.get("orderId")
                                print(f"✅ Copied to {child['client_id']} (Order ID: {order_id_child})")
                                save_log(child['client_id'], symbol, transaction_type, copied_qty, "SUCCESS", str(response))
                                save_order_mapping(
                                    master_order_id=order_id,
                                    child_order_id=order_id_child,
                                    master_id=master_id,
                                    master_broker=master_broker,
                                    child_id=child["client_id"],
                                    child_broker=child_broker,
                                    symbol=symbol
                                )
                                record_trade(child.get('owner'), symbol, transaction_type, copied_qty, price, 'SUCCESS')
                        except Exception as e:
                            print(f"❌ Error copying to {child['client_id']}: {e}")
                            save_log(child['client_id'], symbol, transaction_type, copied_qty, "FAILED", str(e))

                if new_last_trade_id:
                    print(f"✅ Updating last_copied_trade_id for {master_id} to {new_last_trade_id}")
                    accounts_data[last_copied_key] = new_last_trade_id

            safe_write_json("accounts.json", accounts_data)
        except Exception as e:
            print(f"❌ poll_and_copy_trades encountered an error: {e}")



def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=poll_and_copy_trades, trigger="interval", seconds=10)
    scheduler.start()
    print("✅ Background copy trader scheduler is running...")
    return scheduler

@app.route("/connect-zerodha", methods=["POST"])
def connect_zerodha():
    data = request.json
    broker = ZerodhaBroker(
        client_id=data.get("client_id"),
        api_key=data.get("api_key"),
        api_secret=data.get("api_secret"),
        request_token=data.get("request_token"),
    )
    return jsonify({"access_token": broker.access_token})

# --- Order Book Endpoint ---
@app.route('/api/order-book/<client_id>', methods=['GET'])
def get_order_book(client_id):
    try:
        with open("accounts.json", "r") as f:
            accounts_data = json.load(f)
        masters = [acc for acc in accounts_data.get("accounts", []) if acc.get("role") == "master"]
        master = next((m for m in masters if m.get("client_id") == client_id), None)
        if not master:
            return jsonify({"error": "Master not found"}), 404

        api = broker_api(master)
        orders_resp = api.get_order_list()
        orders = orders_resp.get("data", orders_resp.get("orders", []))
        orders = strip_emojis_from_obj(orders)

        formatted = []
        for order in orders:
            formatted.append({
                "order_id": order.get("orderId") or order.get("order_id"),
                "side": order.get("transactionType", order.get("side", "NA")),
                "status": order.get("orderStatus", order.get("status", "NA")),
                "symbol": order.get("tradingSymbol", order.get("symbol", "—")),
                "product_type": order.get("productType", order.get("product", "—")),
                "placed_qty": order.get("orderQuantity", order.get("qty", 0)),
                "filled_qty": order.get("filledQuantity", order.get("filled_qty", 0)),
                "avg_price": order.get("averagePrice", order.get("avg_price", 0)),
                "order_time": order.get("orderTimestamp", order.get("order_time", "")).replace("T", " ").split(".")[0],
                "remarks": order.get("remarks", "—")
            })

        return jsonify(formatted), 200

    except Exception as e:
        print(f"❌ Error in get_order_book(): {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/zerodha_redirects/<client_id>")
def zerodha_redirect_handler(client_id):
    from kiteconnect import KiteConnect

    request_token = request.args.get("request_token")

    if not request_token:
        return "❌ No request_token received", 400

    pending_path = "pending_zerodha.json"
    if os.path.exists(pending_path):
        try:
            with open(pending_path) as f:
                pending = json.load(f)
        except Exception:
            pending = {}
    else:
        pending = {}

    cred = pending.pop(client_id, None)
    if not cred:
        return "❌ No pending auth for this client", 400

    api_key = cred.get("api_key")
    api_secret = cred.get("api_secret")
    username = cred.get("username") or client_id

    kite = KiteConnect(api_key=api_key)

    try:
        session_data = kite.generate_session(request_token, api_secret)
        access_token = session_data["access_token"]

        # Save it in DB or accounts.json
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

        # Store using your own helper
        save_account_to_user(cred.get("owner", username), account)
        safe_write_json(pending_path, pending)
        return redirect(url_for("AddAccount"))

    except Exception as e:
        return f"❌ Error: {str(e)}", 500



# === Webhook to place orders using stored user credentials ===
@app.route("/webhook/<user_id>", methods=["POST"])
def webhook(user_id):
    try:
        data = request.get_json(force=True)
    except Exception:
        data = {}

    # ALERT HANDLING (unchanged)
    if isinstance(data, str):
        return jsonify({"status": "Alert logged", "message": data}), 200
    if "message" in data:
        return jsonify({"status": "Alert logged", "message": data["message"]}), 200

    symbol = data.get("symbol")
    action = data.get("action")
    quantity = data.get("quantity")

    if not all([symbol, action, quantity]):
        return jsonify({"error": "Missing required fields (symbol, action, quantity)"}), 400

    # Load users.json (or however you store users)
    try:
        with open("users.json", "r") as f:
            users = json.load(f)
    except FileNotFoundError:
        return jsonify({"error": "User database not found"}), 500

    if user_id not in users:
        return jsonify({"error": "Invalid webhook ID"}), 403

    user = users[user_id]
    broker_name = user.get("broker", "dhan")
    client_id = user["client_id"]
    access_token = user["access_token"]

    # === Dynamic Broker Adapter ===
    try:
        BrokerClass = get_broker_class(broker_name)
        broker_api = BrokerClass(client_id, access_token)
    except Exception as e:
        return jsonify({"error": f"Could not initialize broker: {e}"}), 500

    # === Broker-agnostic order parameter builder ===
    order_params = {}
    mapping = get_symbol_for_broker(symbol, broker_name)
    if broker_name.lower() == "dhan":
        security_id = mapping.get("security_id")
        if not security_id:
            return jsonify({"error": f"Symbol '{symbol}' not found in symbol map."}), 400
        order_params = dict(
            security_id=security_id,
            exchange_segment=broker_api.NSE,
            transaction_type=broker_api.BUY if action.upper() == "BUY" else broker_api.SELL,
            quantity=int(quantity),
            order_type=broker_api.MARKET,
            product_type=broker_api.INTRA,
            price=0
        )
    elif broker_name.lower() == "zerodha":
        tradingsymbol = mapping.get("tradingsymbol", symbol)
        order_params = dict(
            tradingsymbol=tradingsymbol,
            exchange="NSE",
            transaction_type=action.upper(),
            quantity=int(quantity),
            order_type="MARKET",
            product="MIS",  # Or "CNC"
            price=None,
        )
    # Add more brokers here...

    # === Place order ===
    try:
        response = broker_api.place_order(**order_params)
        if isinstance(response, dict) and response.get("status") == "failure":
            status = "FAILED"
            reason = (
                response.get("remarks") or response.get("error_message") or
                response.get("errorMessage") or response.get("error") or "Unknown error"
            )
            record_trade(user_id, symbol, action.upper(), quantity, order_params.get('price'), status)
            return jsonify({"status": status, "reason": reason}), 400

        # If order was SUCCESSFUL, trigger instant copying for all children!
        poll_and_copy_trades()
        status = "SUCCESS"

        success_msg = response.get("remarks", "Trade placed successfully")
        record_trade(user_id, symbol, action.upper(), quantity, order_params.get('price'), status)
        return jsonify({"status": status, "result": str(success_msg)}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/master-squareoff', methods=['POST'])
def master_squareoff():
    data = request.json
    master_order_id = data.get("master_order_id")
    if not master_order_id:
        return jsonify({"error": "Missing master_order_id"}), 400

    try:
        with open("order_mappings.json", "r") as f:
            mappings = json.load(f)
        targets = [m for m in mappings if m["master_order_id"] == master_order_id and m["status"] == "ACTIVE"]
        if not targets:
            return jsonify({"message": "No active child orders found for this master order."}), 200

        results = []
        with open("accounts.json", "r") as f:
            accounts = json.load(f)

        for mapping in targets:
            child_id = mapping["child_client_id"]
            symbol = mapping["symbol"]
            found = None
            for master in accounts.get("masters", []):
                for child in master.get("children", []):
                    if child["client_id"] == child_id:
                        found = child
                        break
                if found: break

            if not found:
                results.append(f"{child_id} → ❌ Credentials not found")
                continue

            try:
                api = broker_api(found)
                positions_resp = api.get_positions()
                positions = positions_resp.get("data", positions_resp.get("positions", []))
                match = next((p for p in positions if (p.get("tradingSymbol") or p.get("symbol", "")).upper() == symbol.upper() and int(p.get("netQty", p.get("net_quantity", 0))) != 0), None)
                if not match:
                    results.append(f"{child_id} → ℹ️ No open position in {symbol}")
                    continue

                direction = "SELL" if match.get("netQty", match.get("net_quantity", 0)) > 0 else "BUY"
                response = api.place_order(
                    security_id=match.get("securityId", match.get("security_id")),
                    exchange_segment=match.get("exchangeSegment", match.get("exchange_segment")),
                    transaction_type=direction,
                    quantity=abs(int(match.get("netQty", match.get("net_quantity", 0)))),
                    order_type="MARKET",
                    product_type="INTRADAY",
                    price=0
                )
                if isinstance(response, dict) and response.get("status") == "failure":
                    results.append(f"{child_id} → ❌ Square-off failed: {response.get('remarks', response.get('error', 'Unknown error'))}")
                else:
                    mapping["status"] = "SQUARED_OFF"
                    results.append(f"{child_id} → ✅ Square-off done")

            except Exception as e:
                results.append(f"{child_id} → ❌ ERROR: {str(e)}")

        with open("order_mappings.json", "w") as f:
            json.dump(mappings, f, indent=2)

        return jsonify({"message": "Square-off complete", "details": results}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/master-orders', methods=['GET'])
def get_master_orders():
    try:
        path = "order_mappings.json"
        if not os.path.exists(path):
            return jsonify([]), 200

        with open(path, "r") as f:
            mappings = json.load(f)

        master_id_filter = request.args.get("master_id")

        master_summary = {}

        for entry in mappings:
            master_id = entry["master_client_id"]
            if master_id_filter and master_id != master_id_filter:
                continue  # ⛔ skip non-matching masters

            mid = entry["master_order_id"]
            if mid not in master_summary:
                master_summary[mid] = {
                    "master_order_id": mid,
                    "symbol": entry["symbol"],
                    "master_client_id": master_id,
                    "master_broker": entry.get("master_broker", "Unknown"),
                    "status": "ACTIVE",
                    "total_children": 0,
                    "child_statuses": [],
                    "timestamp": entry.get("timestamp", "—")
                }

            master_summary[mid]["total_children"] += 1
            master_summary[mid]["child_statuses"].append(entry["status"])

        for summary in master_summary.values():
            if all(s != "ACTIVE" for s in summary["child_statuses"]):
                summary["status"] = "CANCELLED"

        return jsonify(list(master_summary.values())), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

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

    pending_path = 'pending_zerodha.json'
    if os.path.exists(pending_path):
        try:
            with open(pending_path) as f:
                pending = json.load(f)
        except Exception:
            pending = {}
    else:
        pending = {}

    pending[client_id] = {
        'api_key': api_key,
        'api_secret': api_secret,
        'username': username,
        'owner': session.get('user')
    }
    safe_write_json(pending_path, pending)

    redirect_uri = f"https://dhan-trading.onrender.com/zerodha_redirects/{client_id}"
    login_url = f"https://kite.zerodha.com/connect/login?api_key={api_key}&v=3&redirect_uri={quote(redirect_uri, safe='')}"
    return jsonify({'login_url': login_url})

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
def square_off():
    data = request.json
    client_id = data.get("client_id")
    symbol = data.get("symbol")
    is_master = data.get("is_master", False)

    if not client_id or not symbol:
        return jsonify({"error": "Missing client_id or symbol"}), 400

    try:
        with open("accounts.json", "r") as f:
            accounts = json.load(f)
    except Exception:
        return jsonify({"error": "Failed to load accounts file"}), 500

    found, parent = find_account_by_client_id(accounts, client_id)
    if not found:
        return jsonify({"error": "Client not found"}), 404
    if parent is None:
        master = found
    else:
        master = parent

    if is_master and parent is None:
        # Square off master only
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
        # Square off all children under master (parent==None means master, else parent)
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
def get_order_mappings():
    try:
        path = "order_mappings.json"
        if not os.path.exists(path):
            return jsonify([]), 200

        with open(path, "r") as f:
            mappings = json.load(f)

        return jsonify(mappings), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/child-orders')
def child_orders():
    master_order_id = request.args.get('master_order_id')
    path = 'order_mappings.json'
    if not os.path.exists(path):
        return jsonify([])
    with open(path, 'r') as f:
        mappings = json.load(f)
    if master_order_id:
        mappings = [m for m in mappings if m.get('master_order_id') == master_order_id]
    return jsonify(mappings)

# --- Cancel Order Endpoint ---
@app.route('/api/cancel-order', methods=['POST'])
def cancel_order():
    data = request.json
    master_order_id = data.get("master_order_id")
    if not master_order_id:
        return jsonify({"error": "Missing master_order_id"}), 400

    try:
        if not os.path.exists("order_mappings.json"):
            return jsonify({"error": "No order mappings found"}), 404

        with open("order_mappings.json", "r") as f:
            mappings = json.load(f)

        relevant = [m for m in mappings if m["master_order_id"] == master_order_id and m["status"] == "ACTIVE"]
        if not relevant:
            return jsonify({"message": "No active child orders found for this master order."}), 200

        results = []
        with open("accounts.json", "r") as f:
            accounts = json.load(f)

        for mapping in relevant:
            child_id = mapping["child_client_id"]
            child_order_id = mapping["child_order_id"]
            found = None
            for m in accounts.get("masters", []):
                for c in m.get("children", []):
                    if c["client_id"] == child_id:
                        found = c
                        break
                if found: break

            if not found:
                results.append(f"{child_id} → ❌ Client not found")
                continue

            try:
                api = broker_api(found)
                cancel_resp = api.cancel_order(child_order_id)

                if isinstance(cancel_resp, dict) and cancel_resp.get("status") == "failure":
                    results.append(f"{child_id} → ❌ Cancel failed: {cancel_resp.get('remarks', cancel_resp.get('error', 'Unknown error'))}")
                else:
                    results.append(f"{child_id} → ✅ Cancelled")
                    mapping["status"] = "CANCELLED"

            except Exception as e:
                results.append(f"{child_id} → ❌ ERROR: {str(e)}")

        with open("order_mappings.json", "w") as f:
            json.dump(mappings, f, indent=2)

        return jsonify({"message": "Cancel process completed", "details": results}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/change-master', methods=['POST'])
def change_master():
    data = request.json
    child_id = data.get("child_id")
    new_master_id = data.get("new_master_id")
    if not child_id or not new_master_id:
        return jsonify({"error": "Missing child_id or new_master_id"}), 400
    if os.path.exists("accounts.json"):
        with open("accounts.json", "r") as f:
            accounts_data = json.load(f)
    else:
        return jsonify({"error": "No accounts file found"}), 500
    found = None
    for acc in accounts_data["accounts"]:
        if acc["client_id"] == child_id:
            acc["linked_master_id"] = new_master_id
            found = acc
    if not found:
        return jsonify({"error": "Child not found."}), 404
    safe_write_json("accounts.json", accounts_data)

    return jsonify({"message": f"Child {child_id} now linked to master {new_master_id}."}), 200

@app.route('/api/remove-child', methods=['POST'])
def remove_child():
    data = request.json
    client_id = data.get("client_id")
    if not client_id:
        return jsonify({"error": "Missing client_id"}), 400
    if os.path.exists("accounts.json"):
        with open("accounts.json", "r") as f:
            accounts_data = json.load(f)
    else:
        return jsonify({"error": "No accounts file found"}), 500
    found = None
    for acc in accounts_data["accounts"]:
        if acc["client_id"] == client_id and acc.get("role") == "child":
            acc["role"] = None
            acc["linked_master_id"] = None
            acc["copy_status"] = "Off"
            acc["multiplier"] = 1
            found = acc
    if not found:
        return jsonify({"error": "Child not found."}), 404
    safe_write_json("accounts.json", accounts_data)

    return jsonify({"message": f"Child {client_id} removed from master."}), 200

@app.route('/api/remove-master', methods=['POST'])
def remove_master():
    data = request.json
    client_id = data.get("client_id")
    if not client_id:
        return jsonify({"error": "Missing client_id"}), 400
    if os.path.exists("accounts.json"):
        with open("accounts.json", "r") as f:
            accounts_data = json.load(f)
    else:
        return jsonify({"error": "No accounts file found"}), 500
    found = None
    for acc in accounts_data["accounts"]:
        if acc["client_id"] == client_id and acc.get("role") == "master":
            acc["role"] = None
            acc["copy_status"] = "Off"
            acc.pop("linked_master_id", None)
            acc["multiplier"] = 1
            found = acc
    if not found:
        return jsonify({"error": "Master not found."}), 404
    safe_write_json("accounts.json", accounts_data)
    return jsonify({"message": f"Master {client_id} removed."}), 200

# PATCH for /api/update-multiplier
@app.route('/api/update-multiplier', methods=['POST'])
def update_multiplier():
    data = request.json
    client_id = data.get("client_id")
    new_multiplier = data.get("multiplier")
    if not client_id or new_multiplier is None:
        return jsonify({"error": "Missing required fields"}), 400
    try:
        new_multiplier = float(new_multiplier)
        if new_multiplier < 0.1:
            return jsonify({"error": "Multiplier must be at least 0.1"}), 400
    except ValueError:
        return jsonify({"error": "Invalid multiplier format"}), 400
    if os.path.exists("accounts.json"):
        with open("accounts.json", "r") as f:
            accounts_data = json.load(f)
    else:
        return jsonify({"error": "No accounts found"}), 400
    found = None
    for acc in accounts_data["accounts"]:
        if acc["client_id"] == client_id:
            acc["multiplier"] = new_multiplier
            found = acc
    if not found:
        return jsonify({"error": "Child account not found"}), 404
    safe_write_json("accounts.json", accounts_data)

    return jsonify({"message": f"Multiplier updated to {new_multiplier} for {client_id}"}), 200


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
            imei = credentials.get('imei') or 'abc1234'
            credentials['imei'] = imei # Ensure imei is in credentials if not already
            broker_obj = BrokerClass(
                client_id=client_id,
                password=credentials['password'],
                totp_secret=credentials['totp_secret'],
                vendor_code=credentials['vendor_code'],
                api_key=credentials['api_key'],
                imei=imei
            )
        else:
            # For other brokers, assume access_token based authentication
            access_token = credentials.get('access_token')
            rest = {k: v for k, v in credentials.items() if k != 'access_token'}
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
    user = session.get("user")
    data = request.json
    broker = data.get('broker')
    client_id = data.get('client_id')
    username = data.get('username')

    credentials = {k: v for k, v in data.items() if k not in ('broker', 'client_id', 'username')}

    if not broker or not client_id or not username:
        return jsonify({'error': 'Missing broker, client_id or username'}), 400

    # Load DB
    if os.path.exists("accounts.json"):
        with open("accounts.json", "r") as f:
            accounts_data = json.load(f)
    else:
        accounts_data = {"accounts": []}

    # Check for duplicates
    for acc in accounts_data["accounts"]:
        if (
            acc.get("client_id") == client_id
            and acc.get("broker") == broker
            and acc.get("owner") == user
        ):
            return jsonify({'error': 'Account already exists'}), 400

    # For Alice Blue, generate and add device_number if not already present
    device_number = None
    if broker == 'aliceblue':
        # Use a persistent device_number for this client_id (if already exists, reuse it)
        for acc in accounts_data["accounts"]:
            if (
                acc.get("client_id") == client_id
                and acc.get("broker") == broker
                and acc.get("owner") == user
            ) and acc.get("device_number"):
                device_number = acc.get("device_number")
                break
        if not device_number:
            device_number = str(uuid.uuid4())
        credentials['device_number'] = device_number

    # Validate credentials using broker adapter
    broker_obj = None # Initialize broker_obj to None
    error_message = None # Initialize error_message

    try:
        BrokerClass = get_broker_class(broker)
        
        if broker == 'aliceblue':
            api_key = credentials.get('api_key')
            if not api_key:
                return jsonify({'error': 'Missing API Key'}), 400
            # Pass device_number into the BrokerClass if needed, or ensure it's used in place_order
            broker_obj = BrokerClass(client_id, api_key, device_number=device_number)
            
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
        else:
            access_token = credentials.get('access_token')
            rest = {k: v for k, v in credentials.items() if k != 'access_token'}
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

    except Exception as e:
        if broker_obj and hasattr(broker_obj, 'last_auth_error') and broker_obj.last_auth_error():
            error_message = broker_obj.last_auth_error()
        else:
            error_message = str(e)
        return jsonify({'error': f'Credential validation failed: {error_message}'}), 400

    # Add to accounts.json
    account_record = {
        "broker": broker,
        "client_id": client_id,
        "username": username,
        "credentials": credentials,
        "owner": user,
        "status": "Connected",
        "auto_login": True,
        "last_login": datetime.now().isoformat(),
        "role": None,
        "linked_master_id": None,
        "multiplier": 1,
        "copy_status": "Off"
    }
    if broker == "aliceblue":
        account_record["device_number"] = device_number
    accounts_data["accounts"].append(account_record)

    # Add to SQL DB if available
    db_user = User.query.filter_by(email=user).first()
    if db_user:
        existing = Account.query.filter_by(user_id=db_user.id, client_id=client_id).first()
        if not existing:
            account_obj = Account(
                user_id=db_user.id,
                broker=broker,
                client_id=client_id,
                token_expiry=credentials.get('token_expiry'), 
                status='Connected'
            )
            db.session.add(account_obj)
        else:
            existing.broker = broker
            existing.token_expiry = credentials.get('token_expiry')
            existing.status = 'Connected'
        db.session.commit()

    safe_write_json("accounts.json", accounts_data)
    return jsonify({'message': f"✅ Account {username} ({broker}) added."}), 200
    
    
@app.route('/api/accounts')
@login_required
def get_accounts():
    try:
        if os.path.exists("accounts.json"):
            with open("accounts.json", "r") as f:
                accounts_data = json.load(f)
            if "accounts" not in accounts_data or not isinstance(accounts_data["accounts"], list):
                raise ValueError("Corrupt accounts.json: missing 'accounts' list")
        else:
            accounts_data = {'accounts': []}

        user = session.get("user")
        accounts = [a for a in accounts_data["accounts"] if a.get("owner") == user]

        for acc in accounts:
            bal = get_opening_balance_for_account(acc)
            if bal is not None:
                acc["opening_balance"] = bal

        masters = []
        for acc in accounts:
            if acc.get("role") == "master":
                # Attach children to each master
                children = [child for child in accounts if child.get("role") == "child" and child.get("linked_master_id") == acc.get("client_id")]
                acc_copy = dict(acc)
                acc_copy["children"] = children
                masters.append(acc_copy)
        return jsonify({
            "masters": masters,
            "accounts": accounts
        })
    except Exception as e:
        print(f"❌ Error in /api/accounts: {str(e)}")
        return jsonify({"error": str(e)}), 500
        
@app.route('/api/groups', methods=['GET'])
@login_required
def get_groups():
    """Return all account groups for the logged-in user."""
    user = session.get("user")
    groups_path = "groups.json"
    if os.path.exists(groups_path):
        with open(groups_path, "r") as f:
            groups_data = json.load(f)
    else:
        groups_data = {"groups": []}
    groups = [g for g in groups_data.get("groups", []) if g.get("owner") == user]
    return jsonify(groups)


@app.route('/api/create-group', methods=['POST'])
@login_required
def create_group():
    """Create a new account group."""
    data = request.json
    name = data.get("name")
    members = data.get("members", [])
    if not name:
        return jsonify({"error": "Missing group name"}), 400

    groups_path = "groups.json"
    if os.path.exists(groups_path):
        with open(groups_path, "r") as f:
            groups_data = json.load(f)
    else:
        groups_data = {"groups": []}

    user = session.get("user")
    for g in groups_data.get("groups", []):
        if g.get("name") == name and g.get("owner") == user:
            return jsonify({"error": "Group already exists"}), 400

    groups_data.setdefault("groups", []).append({
        "name": name,
        "owner": user,
        "members": members
    })
    safe_write_json(groups_path, groups_data)
    return jsonify({"message": f"Group '{name}' created"})


@app.route('/api/groups/<group_name>/add', methods=['POST'])
@login_required
def add_account_to_group(group_name):
    """Add an account to an existing group."""
    client_id = request.json.get("client_id")
    if not client_id:
        return jsonify({"error": "Missing client_id"}), 400

    groups_path = "groups.json"
    if os.path.exists(groups_path):
        with open(groups_path, "r") as f:
            groups_data = json.load(f)
    else:
        return jsonify({"error": "Group database not found"}), 500

    user = session.get("user")
    for g in groups_data.get("groups", []):
        if g.get("name") == group_name and g.get("owner") == user:
            if client_id not in g.get("members", []):
                g.setdefault("members", []).append(client_id)
                safe_write_json(groups_path, groups_data)
                return jsonify({"message": f"Added {client_id} to {group_name}"})
            return jsonify({"message": "Account already in group"})

    return jsonify({"error": "Group not found"}), 404


@app.route('/api/groups/<group_name>/remove', methods=['POST'])
@login_required
def remove_account_from_group(group_name):
    """Remove an account from a group."""
    client_id = request.json.get("client_id")
    if not client_id:
        return jsonify({"error": "Missing client_id"}), 400

    groups_path = "groups.json"
    if os.path.exists(groups_path):
        with open(groups_path, "r") as f:
            groups_data = json.load(f)
    else:
        return jsonify({"error": "Group database not found"}), 500

    user = session.get("user")
    for g in groups_data.get("groups", []):
        if g.get("name") == group_name and g.get("owner") == user:
            if client_id in g.get("members", []):
                g["members"].remove(client_id)
                safe_write_json(groups_path, groups_data)
                return jsonify({"message": f"Removed {client_id} from {group_name}"})
            return jsonify({"error": "Account not in group"}), 400

    return jsonify({"error": "Group not found"}), 404


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

    groups_path = "groups.json"
    if os.path.exists(groups_path):
        with open(groups_path, "r") as f:
            groups_data = json.load(f)
    else:
        return jsonify({"error": "No groups configured"}), 400

    group = None
    user = session.get("user")
    for g in groups_data.get("groups", []):
        if g.get("name") == group_name and g.get("owner") == user:
            group = g
            break
    if not group:
        return jsonify({"error": "Group not found"}), 404

    if os.path.exists("accounts.json"):
        with open("accounts.json", "r") as f:
            accounts_data = json.load(f)
    else:
        return jsonify({"error": "No accounts configured"}), 500

    accounts = [a for a in accounts_data.get("accounts", []) if a.get("client_id") in group.get("members", [])]

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

            record_trade(user, symbol, action.upper(), quantity, order_params.get('price'), status)
        except Exception as e:
            results.append({"client_id": acc.get("client_id"), "status": "ERROR", "reason": str(e)})

    return jsonify(results)
    
# Set master account
@app.route('/api/set-master', methods=['POST'])
def set_master():
    try:
        client_id = request.json.get('client_id')
        if not client_id:
            return jsonify({"error": "Missing client_id"}), 400

        if os.path.exists("accounts.json"):
            with open("accounts.json", "r") as f:
                accounts_data = json.load(f)
        else:
            accounts_data = {"accounts": []}
            
        user = session.get("user")
        found = False
        for acc in accounts_data["accounts"]:
            if acc.get("client_id") == client_id and acc.get("owner") == user:
                acc["role"] = "master"
                acc.pop("linked_master_id", None)
                acc["copy_status"] = "Off"
                acc["multiplier"] = 1
                found = True    # <-- This line was missing!

        if not found:
            return jsonify({"error": "Account not found"}), 404

        safe_write_json("accounts.json", accounts_data)

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

        if os.path.exists("accounts.json"):
            with open("accounts.json", "r") as f:
                accounts_data = json.load(f)
        else:
            accounts_data = {"accounts": []}

        user = session.get("user")
        found = False
        for acc in accounts_data["accounts"]:
            if acc.get("client_id") == client_id and acc.get("owner") == user:
                acc["role"] = "child"
                acc["linked_master_id"] = linked_master_id
                acc["copy_status"] = "Off"
                acc["multiplier"] = 1
                found = True   # <-- This line was missing!

        if not found:
            return jsonify({"error": "Account not found"}), 404

        safe_write_json("accounts.json", accounts_data)

        return jsonify({"message": "Set as child successfully"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Start copying for a child account
@app.route('/api/start-copy', methods=['POST'])
@login_required
def start_copy():
    data = request.json
    client_id = data.get("client_id")
    master_id = data.get("master_id")
    if not client_id or not master_id:
        return jsonify({"error": "Missing client_id or master_id"}), 400
    if os.path.exists("accounts.json"):
        with open("accounts.json", "r") as f:
            accounts_data = json.load(f)
    else:
        return jsonify({"error": "No accounts file found"}), 500

    user = session.get("user")

    found = False
    for acc in accounts_data["accounts"]:
        if acc["client_id"] == client_id and acc.get("owner") == user:
            acc["role"] = "child"
            acc["linked_master_id"] = master_id
            acc["copy_status"] = "On"
            found = True
    if not found:
        return jsonify({"error": "Child account not found."}), 404

    safe_write_json("accounts.json", accounts_data)
    return jsonify({'message': f"✅ Started copying for {client_id} under master {master_id}."})


@app.route('/api/stop-copy', methods=['POST'])
@login_required
def stop_copy():
    data = request.json
    client_id = data.get("client_id")
    master_id = data.get("master_id")
    if not client_id or not master_id:
        return jsonify({"error": "Missing client_id or master_id"}), 400
    if os.path.exists("accounts.json"):
        with open("accounts.json", "r") as f:
            accounts_data = json.load(f)
    else:
        return jsonify({"error": "No accounts file found"}), 500

    user = session.get("user")
    found = False
    for acc in accounts_data["accounts"]:
        if acc["client_id"] == client_id and acc.get("linked_master_id") == master_id and acc.get("owner") == user:
            acc["copy_status"] = "Off"
            found = True
    if not found:
        return jsonify({"error": "Child account not found."}), 404

    safe_write_json("accounts.json", accounts_data)
    return jsonify({'message': f"🛑 Stopped copying for {client_id} under master {master_id}."})

# === Endpoint to fetch passive alert logs ===
@app.route("/api/alerts")
def get_alerts():
    user_id = request.args.get("user_id")
    conn = sqlite3.connect("tradelogs.db")
    c = conn.cursor()
    c.execute("SELECT timestamp, response FROM logs WHERE user_id = ? AND status = 'ALERT' ORDER BY id DESC LIMIT 20", (user_id,))
    rows = c.fetchall()
    conn.close()

    alerts = [{"time": row[0], "message": row[1]} for row in rows]
    return jsonify(alerts)



# === API to save new user from login form ===
@app.route("/register", methods=["POST"])
def register_user():
    data = request.json
    user_id = data.get("user_id")
    client_id = data.get("client_id")
    access_token = data.get("access_token")

    if not all([user_id, client_id, access_token]):
        return jsonify({"error": "Missing required fields"}), 400

    try:
        with open("users.json", "r") as f:
            users = json.load(f)
    except FileNotFoundError:
        users = {}

    users[user_id] = {
        "client_id": client_id,
        "access_token": access_token
    }

    with open("users.json", "w") as f:
        json.dump(users, f, indent=2)

    return jsonify({"status": "User registered successfully", "webhook": f"/webhook/{user_id}"})

# === API to fetch logs for a user ===
@app.route("/logs")
def get_logs():
    user_id = request.args.get("user_id")
    conn = sqlite3.connect("tradelogs.db")
    c = conn.cursor()
    c.execute("SELECT * FROM logs WHERE user_id = ? ORDER BY id DESC LIMIT 100", (user_id,))
    rows = c.fetchall()
    conn.close()

    logs = []
    for row in rows:
        logs.append({
            "timestamp": row[1],
            "user_id": row[2],
            "symbol": row[3],
            "action": row[4],
            "quantity": row[5],
            "status": row[6],
            "response": row[7]
        })

    return jsonify(logs)

# === API to get live portfolio snapshot (holdings) ===
def get_portfolio(user_id):
    """Return live positions for any stored account."""
    # Check users.json (external registered users)
    try:
        with open("users.json", "r") as f:
            users = json.load(f)
    except Exception:
        users = {}

    user_obj = users.get(user_id)
    if not user_obj:
        # Maybe user_id is actually the broker client_id
        user_obj = next((u for u in users.values() if u.get("client_id") == user_id), None)

    if user_obj:
        client_id = user_obj["client_id"]
        access_token = user_obj["access_token"]
        dhan = dhanhq(client_id, access_token)
        try:
            positions_resp = dhan.get_positions()
            return jsonify(positions_resp)
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    accounts = safe_read_json("accounts.json")
    found, _ = find_account_by_client_id(accounts, user_id)
    if not found:
        return jsonify({"error": "Invalid user ID"}), 403

    # Check in accounts.json using utility (for dashboard accounts)
    try:
        api = broker_api(found)
        positions_resp = api.get_positions()
        return jsonify(positions_resp)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# === API to get trade summary and open orders ===
@app.route("/api/orders/<user_id>")
def get_orders(user_id):
    """Return recent orders for a stored account."""
    try:
        with open("users.json", "r") as f:
            users = json.load(f)
    except Exception as e:
        print(f"❌ Failed to load users.json: {str(e)}")
        return jsonify({"error": "User DB not found"}), 500

    user_obj = users.get(user_id)
    if not user_obj:
        user_obj = next((u for u in users.values() if u.get("client_id") == user_id), None)
    if not user_obj:
        return jsonify({"error": "Invalid user ID"}), 403

    user = user_obj
    dhan = dhanhq(user["client_id"], user["access_token"])

    try:
        resp = dhan.get_order_list()
        print(f"👉 Full Dhan API response for {user_id}: {resp}")

        # Defensive check: is it the expected dict?
        if not isinstance(resp, dict) or "data" not in resp:
            return jsonify({"error": "Unexpected response format", "details": resp}), 500

        orders = strip_emojis_from_obj(resp["data"])  # ✅ sanitized orders

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
        print(f"❌ Error while fetching orders for {user_id}: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/account/<user_id>")
def get_account_stats(user_id):
    """Return account margin/fund stats."""
    try:
        with open("users.json", "r") as f:
            users = json.load(f)
    except Exception:
        return jsonify({"error": "User DB not found"}), 500

    user_obj = users.get(user_id)
    if not user_obj:
        user_obj = next((u for u in users.values() if u.get("client_id") == user_id), None)
    if not user_obj:
        return jsonify({"error": "Invalid user ID"}), 403

    user = user_obj
    dhan = dhanhq(user["client_id"], user["access_token"])

    try:
        stats_resp = dhan.get_fund_limits()
        print(f"👉 Fund stats for {user_id}: {stats_resp}")

        if not isinstance(stats_resp, dict) or "data" not in stats_resp:
            return jsonify({"error": "Unexpected response format", "details": stats_resp}), 500

        stats = stats_resp["data"]

        # Map to clean keys:
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
    users = {}
    if os.path.exists("users.json"):
        with open("users.json", "r") as f:
            try:
                users = json.load(f)
            except Exception:
                users = {}

    user = users.get(username, {})
    message = ""

    if request.method == "POST":
        action = request.form.get("action")

        if action == "save_profile":
            first_name = request.form.get("first_name", "")
            last_name = request.form.get("last_name", "")

            user["first_name"] = first_name
            user["last_name"] = last_name

            file = request.files.get("profile_image")
            if file and file.filename:
                image_dir = os.path.join("static", "profile_images")
                os.makedirs(image_dir, exist_ok=True)
                filename = secure_filename(username + "_" + file.filename)
                file.save(os.path.join(image_dir, filename))
                user["profile_image"] = os.path.join("profile_images", filename)
            message = "Profile updated"

        elif action == "send_otp" and not user.get("mobile"):
            mobile = request.form.get("mobile")
            if mobile:
                otp = "".join(random.choices(string.digits, k=6))
                user["pending_mobile"] = mobile
                user["otp"] = otp
                print(f"OTP for {mobile}: {otp}")
                message = f"OTP sent to {mobile}"

        elif action == "verify_otp" and user.get("pending_mobile"):
            otp_input = request.form.get("otp")
            if otp_input == user.get("otp"):
                user["mobile"] = user.get("pending_mobile")
                user.pop("pending_mobile", None)
                user.pop("otp", None)
                user["mobile_verified"] = True
                message = "Mobile number verified"
            else:
                message = "Invalid OTP"

        users[username] = user
        with open("users.json", "w") as f:
            json.dump(users, f, indent=2)

    profile_data = {
        "email": username,
        "first_name": user.get("first_name", ""),
        "last_name": user.get("last_name", ""),
        "plan": user.get("plan", "Free"),
        "profile_image": user.get("profile_image", "user.png"),
        "mobile": user.get("mobile"),
        "pending_mobile": user.get("pending_mobile"),
        "mobile_verified": user.get("mobile_verified", False),
    }

    return render_template("user.html", user=profile_data, message=message)


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("email") or request.form.get("username")
        password = request.form.get("password")
        user = User.query.filter_by(email=username).first()
        if user and user.check_password(password):
            session["user"] = user.email
            user.last_login = datetime.now().strftime('%Y-%m-%d')
            db.session.commit()
            return redirect(url_for("summary"))
        return render_template("log-in.html", error="Invalid credentials")
    return render_template("log-in.html")

@app.route("/signup", methods=["GET", "POST"])
def signup():
    if request.method == "POST":
        username = request.form.get("email") or request.form.get("username")
        password = request.form.get("password")
        if User.query.filter_by(email=username).first():
            return render_template("sign-up.html", error="User already exists")
        user = User(email=username)
        user.set_password(password)
        db.session.add(user)
        db.session.commit()
        session["user"] = user.email
        return redirect(url_for("summary"))
    return render_template("sign-up.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("home"))



# === Page routes ===
@app.route('/')
def home():
    return render_template("index.html")

@app.route('/dhan-dashboard')
@login_required
def dhan_dashboard():
    return render_template("dhan-dashboard.html")

@app.route("/Summary")
@login_required
def summary():
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
        admin_email = os.environ.get('ADMIN_EMAIL')
        admin_password = os.environ.get('ADMIN_PASSWORD')

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
        save_settings(settings)
    return render_template('settings.html', settings=settings)

@app.route('/adminprofile')
@admin_login_required
def admin_profile():
    return render_template('profile.html', admin={'email': session.get('admin')})

# ---- Admin action routes ----

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
    user.set_password(new_pass)
    db.session.commit()
    flash(f'New password for {user.email}: {new_pass}')
    return redirect(url_for('admin_users'))


@app.route('/adminusers/<int:user_id>')
@admin_login_required
def admin_view_user(user_id):
    user = User.query.get_or_404(user_id)
    return render_template('user_detail.html', user=user)


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

with app.app_context():
    db.create_all()

scheduler = start_scheduler()

if __name__ == '__main__':
    app.run(debug=True)
