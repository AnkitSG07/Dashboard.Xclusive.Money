from brokers.factory import get_broker_class
from flask import Flask, request, jsonify, render_template
from dhanhq import dhanhq
import sqlite3
import os
import json
import pandas as pd
from flask_cors import CORS
import io
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)
CORS(app)

RAPIDAPI_KEY = "1c99b13c79msh266bd26283ae7f3p1ded7djsn92d495c38bab"  # 👉 Replace this with your real key
RAPIDAPI_HOST = "apidojo-yahoo-finance-v1.p.rapidapi.com"

def broker_api(obj):
    """
    Returns a broker API instance for the given account/child/master dict.
    """
    broker = obj.get("broker", "Unknown").lower()
    credentials = obj.get("credentials", {})
    BrokerClass = get_broker_class(broker)
    return BrokerClass(**credentials)

def find_account_by_client_id(accounts, client_id):
    """
    Returns (account_object, parent_master_object or None) for a given client_id, or (None, None) if not found.
    """
    for master in accounts.get("masters", []):
        if master.get("client_id") == client_id:
            return master, None
        for child in master.get("children", []):
            if child.get("client_id") == client_id:
                return child, master
    return None, None


def clean_response_message(response):
    if isinstance(response, dict):
        remarks = response.get("remarks")
        if isinstance(remarks, dict):
            return remarks.get("errorMessage") or remarks.get("error_message") or str(remarks)
        return str(remarks) or str(response.get("status")) or str(response)
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

def poll_and_copy_trades():
    print("🔄 poll_and_copy_trades() triggered...")

    try:
        if not os.path.exists("accounts.json"):
            print("⚠️ No accounts.json file found.")
            return

        with open("accounts.json", "r") as f:
            accounts = json.load(f)

        masters = accounts.get("masters", [])
        if not masters:
            print("⚠️ No master accounts configured.")
            return

        for master in masters:
            master_id = master.get("client_id")
            master_broker = master.get("broker", "Unknown").lower()
            credentials = master.get("credentials", {})
            BrokerClass = get_broker_class(master_broker)
            try:
                master_api = BrokerClass(**credentials)
            except Exception as e:
                print(f"❌ Could not initialize master API ({master_broker}): {e}")
                continue

            last_copied_key = f"last_copied_trade_id_{master_id}"
            last_copied_trade_id = accounts.get(last_copied_key)
            new_last_trade_id = None

            # Get master orders using standard interface
            try:
                orders_resp = master_api.get_order_list()
                order_list = orders_resp.get("data", orders_resp.get("orders", []))  # support for different APIs
            except Exception as e:
                print(f"❌ Error fetching orders for master {master_id}: {e}")
                continue

            if not order_list:
                print(f"ℹ️ No orders found for master {master_id}.")
                continue

            order_list = sorted(order_list, key=lambda x: x.get("orderTimestamp", x.get("order_time", "")), reverse=True)

            for order in order_list:
                order_id = order.get("orderId") or order.get("order_id")
                if not order_id:
                    continue

                if order_id == last_copied_trade_id:
                    print(f"✅ [{master_id}] Reached last copied trade. Stopping here.")
                    break

                # Status check (try to use most general key)
                order_status = order.get("orderStatus") or order.get("status") or ""
                if order_status.upper() not in ["TRADED", "FILLED", "COMPLETE"]:
                    print(f"⏩ [{master_id}] Skipping order {order_id} (Status: {order_status})")
                    continue

                print(f"✅ [{master_id}] New TRADED/FILLED order: {order_id}")
                new_last_trade_id = new_last_trade_id or order_id

                children = master.get("children", [])
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
                        child_api = ChildBrokerClass(**child_credentials)
                    except Exception as e:
                        print(f"❌ Could not initialize child API ({child_broker}): {e}")
                        continue

                    multiplier = float(child.get("multiplier", 1))
                    master_qty = (
                        order.get("quantity") or
                        order.get("orderQuantity") or
                        order.get("qty") or
                        1
                    )
                    copied_qty = max(1, int(float(master_qty) * multiplier))

                    # Broker-independent field mapping:
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

                    # You may add broker-specific mapping here if absolutely needed
                    # but generally above should be enough for standardized brokers

                    # Place order using child's broker interface
                    try:
                        response = child_api.place_order(
                            tradingsymbol=symbol,
                            exchange=exchange,
                            transaction_type=transaction_type,
                            quantity=copied_qty,
                            order_type=order_type,
                            product=product_type,
                            price=price if order_type == "LIMIT" else None
                        )
                        if isinstance(response, dict) and response.get("status") == "failure":
                            error_msg = response.get("error") or response.get("remarks") or "Unknown error"
                            print(f"❌ Trade FAILED for {child['client_id']} (Reason: {error_msg})")
                            save_log(child['client_id'], symbol, transaction_type, copied_qty, "FAILED", error_msg)
                        else:
                            order_id_child = response.get("order_id") or response.get("orderId")
                            print(f"✅ Copied to {child['client_id']} (Order ID: {order_id_child})")
                            save_log(child['client_id'], symbol, transaction_type, copied_qty, "SUCCESS", str(response))
                            # Save broker-aware order mapping (for square-off etc.)
                            save_order_mapping(
                                master_order_id=order_id,
                                child_order_id=order_id_child,
                                master_id=master_id,
                                master_broker=master_broker,
                                child_id=child["client_id"],
                                child_broker=child_broker,
                                symbol=symbol
                            )
                    except Exception as e:
                        print(f"❌ Error copying to {child['client_id']}: {e}")
                        save_log(child['client_id'], symbol, transaction_type, copied_qty, "FAILED", str(e))

            if new_last_trade_id:
                print(f"✅ Updating last_copied_trade_id for {master_id} to {new_last_trade_id}")
                accounts[last_copied_key] = new_last_trade_id

        with open("accounts.json", "w") as f:
            json.dump(accounts, f, indent=2)

    except Exception as e:
        print(f"❌ poll_and_copy_trades encountered an error: {e}")

scheduler = BackgroundScheduler()
scheduler.add_job(func=poll_and_copy_trades, trigger="interval", seconds=10)
scheduler.start()
print("✅ Background copy trader scheduler is running...")

# --- Order Book Endpoint ---
@app.route('/api/order-book/<client_id>', methods=['GET'])
def get_order_book(client_id):
    try:
        with open("accounts.json", "r") as f:
            accounts = json.load(f)

        masters = accounts.get("masters", [])
        master = next((m for m in masters if m.get("client_id") == client_id), None)
        if not master:
            return jsonify({"error": "Master not found"}), 404

        api = broker_api(master)
        orders_resp = api.get_order_list()
        orders = orders_resp.get("data", orders_resp.get("orders", []))

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


# === Webhook to place orders using stored user credentials ===
@app.route("/webhook/<user_id>", methods=["POST"])
def webhook(user_id):
    try:
        data = request.get_json(force=True)
    except Exception:
        data = {}

    # 🔔 Passive Alert Handling - Raw String Support
    if isinstance(data, str):
        save_log(user_id, "-", "-", 0, "ALERT", data)
        return jsonify({"status": "Alert logged", "message": data}), 200

    # 🔔 Passive Alert Handling - Normal JSON Support
    if "message" in data:
        message = data["message"]
        save_log(user_id, "-", "-", 0, "ALERT", message)
        return jsonify({"status": "Alert logged", "message": message}), 200

    # 🛒 Live Trade Handling
    symbol = data.get("symbol")
    action = data.get("action")
    quantity = data.get("quantity")

    if not all([symbol, action, quantity]):
        return jsonify({"error": "Missing required fields (symbol, action, quantity)"}), 400

    # 🚪 Load User Credentials
    try:
        with open("users.json", "r") as f:
            users = json.load(f)
    except FileNotFoundError:
        return jsonify({"error": "User database not found"}), 500

    if user_id not in users:
        return jsonify({"error": "Invalid webhook ID"}), 403

    user = users[user_id]
    client_id = user["client_id"]
    access_token = user["access_token"]
    dhan = dhanhq(client_id, access_token)

    # 🔥 Full SYMBOL_MAP (your 100+ symbols loaded)
    SYMBOL_MAP = {
        "RELIANCE": "2885", "TCS": "11536", "INFY": "10999", "ADANIPORTS": "15083", "HDFCBANK": "1333",
        "SBIN": "3045", "ICICIBANK": "4963", "AXISBANK": "1343", "ITC": "1660", "HINDUNILVR": "1394",
        "KOTAKBANK": "1922", "LT": "11483", "BAJFINANCE": "317", "HCLTECH": "7229", "ASIANPAINT": "236",
        "MARUTI": "1095", "M&M": "2031", "SUNPHARMA": "3046", "TATAMOTORS": "3432", "WIPRO": "3787",
        "ULTRACEMCO": "11532", "TITAN": "3506", "NESTLEIND": "11262", "BAJAJFINSV": "317",
        "POWERGRID": "14977", "NTPC": "2886", "JSWSTEEL": "11723", "HDFCLIFE": "11915",
        "DRREDDY": "881", "TECHM": "11534", "BRITANNIA": "293", "TATASTEEL": "3505", "CIPLA": "694",
        "SBILIFE": "11916", "BAJAJ-AUTO": "317", "HINDALCO": "1393", "DIVISLAB": "881",
        "GRASIM": "1147", "ADANIENT": "15083", "COALINDIA": "694", "INDUSINDBK": "1393",
        "TATACONSUM": "3505", "EICHERMOT": "881", "SHREECEM": "1147", "HEROMOTOCO": "15083",
        "BAJAJHLDNG": "694", "SBICARD": "1393", "DLF": "3505", "DMART": "881", "UPL": "1147",
        "ICICIPRULI": "15083", "HDFCAMC": "694", "HDFC": "1393", "GAIL": "3505", "HAL": "881",
        "TATAPOWER": "1147", "VEDL": "15083", "BPCL": "694", "IOC": "1393", "ONGC": "3505",
        "LICHSGFIN": "881", "BANKBARODA": "1147", "PNB": "15083", "CANBK": "694", "UNIONBANK": "1393",
        "IDFCFIRSTB": "3505", "BANDHANBNK": "881", "FEDERALBNK": "1147", "RBLBANK": "15083",
        "YESBANK": "694", "IGL": "1393", "PETRONET": "3505", "GUJGASLTD": "881", "MGL": "1147",
        "TORNTPHARM": "15083", "LUPIN": "694", "AUROPHARMA": "1393", "BIOCON": "3505",
        "GLENMARK": "881", "CADILAHC": "1147", "ALKEM": "15083", "APOLLOHOSP": "694",
        "MAXHEALTH": "1393", "FORTIS": "3505", "JUBLFOOD": "881", "UBL": "1147", "MCDOWELL-N": "15083",
        "COLPAL": "694", "DABUR": "1393", "GODREJCP": "3505", "MARICO": "881", "EMAMILTD": "1147",
        "PGHH": "15083", "GILLETTE": "694", "TATACHEM": "1393", "PIDILITIND": "3505",
        "BERGEPAINT": "881", "KANSAINER": "1147", "JSWENERGY": "15083", "ADANIGREEN": "694",
        "ADANITRANS": "1393", "NHPC": "3505", "SJVN": "881", "RECLTD": "1147", "PFC": "15083"
    }

    security_id = SYMBOL_MAP.get(symbol.strip().upper())
    if not security_id:
        return jsonify({"error": f"Symbol '{symbol}' not found in symbol map."}), 400

    try:
        # 🚀 Place Order
        response = dhan.place_order(
            security_id=security_id,
            exchange_segment=dhan.NSE,
            transaction_type=dhan.BUY if action.upper() == "BUY" else dhan.SELL,
            quantity=int(quantity),
            order_type=dhan.MARKET,
            product_type=dhan.INTRA,
            price=0
        )

        # 🧹 Clean and classify the result
        if isinstance(response, dict) and response.get("status") == "failure":
            reason = (
                response.get("remarks") or
                response.get("error_message") or
                response.get("errorMessage") or
                "Unknown error"
            )

            reason_str = str(reason)
            if "market" in reason_str.lower() or "closed" in reason_str.lower():
                save_log(user_id, symbol, action, quantity, "MARKET_CLOSED", reason_str)
                return jsonify({"status": "MARKET_CLOSED", "reason": reason_str}), 400
            else:
                save_log(user_id, symbol, action, quantity, "FAILED", reason_str)
                return jsonify({"status": "FAILED", "reason": reason_str}), 400

        # ✅ Successful trade
        success_msg = response.get("remarks", "Trade placed successfully")
        save_log(user_id, symbol, action, quantity, "SUCCESS", str(success_msg))
        return jsonify({"status": "SUCCESS", "result": str(success_msg)}), 200

    except Exception as e:
        error_msg = str(e)
        save_log(user_id, symbol, action, quantity, "FAILED", error_msg)
        return jsonify({"error": error_msg}), 500

@app.route('/api/child-orders', methods=['GET'])
def get_child_orders():
    master_order_id = request.args.get("master_order_id")

    if not master_order_id:
        return jsonify({"error": "Missing master_order_id"}), 400

    try:
        with open("order_mappings.json", "r") as f:
            mappings = json.load(f)

        child_rows = [m for m in mappings if m["master_order_id"] == master_order_id]

        return jsonify(child_rows), 200

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

    try:
        with open("accounts.json", "r") as f:
            accounts = json.load(f)

        child, old_master = find_account_by_client_id(accounts, child_id)
        if not child or not old_master:
            return jsonify({"error": "Child not found"}), 404

        # Remove from old master
        old_master["children"] = [c for c in old_master.get("children", []) if c["client_id"] != child_id]

        # Add to new master
        new_master, _ = find_account_by_client_id(accounts, new_master_id)
        if not new_master:
            return jsonify({"error": "New master not found"}), 404
        if "children" not in new_master:
            new_master["children"] = []
        new_master["children"].append(child)

        with open("accounts.json", "w") as f:
            json.dump(accounts, f, indent=2)

        return jsonify({"message": f"✅ Child {child_id} moved to new master {new_master_id}"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

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
            accounts = json.load(f)
    else:
        return jsonify({"error": "No accounts found"}), 400

    child, _ = find_account_by_client_id(accounts, client_id)
    if not child:
        return jsonify({"error": "Child account not found"}), 404

    child["multiplier"] = new_multiplier
    with open("accounts.json", "w") as f:
        json.dump(accounts, f, indent=2)
    return jsonify({"message": f"Multiplier updated to {new_multiplier} for {client_id}"}), 200


@app.route("/marketwatch")
def market_watch():
    return render_template("marketwatch.html")

@app.route('/api/add-account', methods=['POST'])
def add_account():
    data = request.json
    client_id = data.get("client_id")
    username = data.get("username")
    broker = data.get("broker")
    role = data.get("role")
    multiplier = float(data.get("multiplier", 1))
    linked_master_id = data.get("linked_master_id")

    if not all([client_id, broker, username, role]):
        return jsonify({"error": "Missing required fields"}), 400

    # Collect credentials (all extra keys except role, client_id, username, broker, multiplier, linked_master_id)
    exclude_keys = {"client_id", "username", "broker", "role", "multiplier", "linked_master_id"}
    credentials = {k: v for k, v in data.items() if k not in exclude_keys}

    try:
        if os.path.exists("accounts.json"):
            with open("accounts.json", "r") as f:
                accounts = json.load(f)
        else:
            accounts = {"masters": []}

        if "masters" not in accounts:
            accounts["masters"] = []

        if role.lower() == "master":
            accounts["masters"].append({
                "broker": broker,
                "client_id": client_id,
                "username": username,
                "status": "Connected",
                "credentials": credentials,
                "children": []
            })
            message = f"✅ Master account {username} ({broker}) added."

        elif role.lower() == "child":
            if not linked_master_id:
                return jsonify({"error": "Missing linked_master_id for child"}), 400

            found_master = None
            for master in accounts["masters"]:
                if master["client_id"] == linked_master_id or master["username"] == linked_master_id:
                    found_master = master
                    break

            if not found_master:
                return jsonify({"error": "Linked master not found"}), 400

            found_master["children"].append({
                "broker": broker,
                "client_id": client_id,
                "username": username,
                "status": "Connected",
                "credentials": credentials,
                "copy_status": "Off",
                "multiplier": multiplier
            })
            message = f"✅ Child account {username} ({broker}) added under master {found_master['username']}."

        else:
            return jsonify({"error": "Invalid role (must be 'master' or 'child')"}), 400

        # Save back to file
        with open("accounts.json", "w") as f:
            json.dump(accounts, f, indent=2)

        return jsonify({"message": message}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Get all trading accounts (sample data for now)
@app.route('/api/accounts')
def get_accounts():
    try:
        if os.path.exists("accounts.json"):
            with open("accounts.json", "r") as f:
                accounts = json.load(f)
        else:
            accounts = {"masters": []}

        # Don't leak tokens! Mask credentials (remove or mask tokens)
        def mask_creds(credentials):
            # Only keep non-sensitive fields (or mask sensitive)
            # Example: Only show api_key partially
            result = {}
            for k, v in credentials.items():
                if 'token' in k or 'secret' in k:
                    result[k] = '****'
                elif 'api_key' in k:
                    result[k] = v[:4] + '****' if len(v) > 4 else '****'
                else:
                    result[k] = v
            return result

        formatted = []
        for master in accounts.get("masters", []):
            formatted.append({
                "role": "master",
                "broker": master.get("broker"),
                "client_id": master.get("client_id"),
                "username": master.get("username"),
                "status": master.get("status"),
                "credentials": mask_creds(master.get("credentials", {})),
                "children": [
                    {
                        "broker": child.get("broker"),
                        "client_id": child.get("client_id"),
                        "username": child.get("username"),
                        "status": child.get("status"),
                        "credentials": mask_creds(child.get("credentials", {})),
                        "copy_status": child.get("copy_status"),
                        "multiplier": child.get("multiplier")
                    }
                    for child in master.get("children", [])
                ]
            })

        return jsonify({"masters": formatted}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Set master account
@app.route('/api/set-master', methods=['POST'])
def set_master():
    data = request.json
    client_id = data.get("client_id")

    if not client_id:
        return jsonify({"error": "Missing client_id"}), 400

    if os.path.exists("accounts.json"):
        with open("accounts.json", "r") as f:
            accounts = json.load(f)
    else:
        return jsonify({"error": "No accounts file found"}), 500

    found = None
    for master in accounts.get("masters", []):
        if master["client_id"] == client_id:
            found = master
            break
        for child in master.get("children", []):
            if child["client_id"] == client_id:
                found = child
                break

    if not found:
        return jsonify({"error": f"Client ID {client_id} not found in accounts."}), 404

    # Store entire account as master (can be master or child)
    accounts["master"] = found

    with open("accounts.json", "w") as f:
        json.dump(accounts, f, indent=2)

    return jsonify({'message': f"✅ Set {client_id} as master successfully."})



# Start copying for a child account
@app.route('/api/start-copy', methods=['POST'])
def start_copy():
    data = request.json
    client_id = data.get("client_id")
    master_id = data.get("master_id")

    if not client_id or not master_id:
        return jsonify({"error": "Missing client_id or master_id"}), 400

    try:
        if os.path.exists("accounts.json"):
            with open("accounts.json", "r") as f:
                accounts = json.load(f)
        else:
            return jsonify({"error": "No accounts file found"}), 500

        child, master = find_account_by_client_id(accounts, client_id)
        if not child or not master or master.get("client_id") != master_id:
            return jsonify({"error": "❌ Child account or master not found."}), 404

        child["copy_status"] = "On"

        with open("accounts.json", "w") as f:
            json.dump(accounts, f, indent=2)
        return jsonify({'message': f"✅ Started copying for {client_id} under master {master_id}."}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/stop-copy', methods=['POST'])
def stop_copy():
    data = request.json
    client_id = data.get("client_id")
    master_id = data.get("master_id")

    if not client_id or not master_id:
        return jsonify({"error": "Missing client_id or master_id"}), 400

    try:
        if os.path.exists("accounts.json"):
            with open("accounts.json", "r") as f:
                accounts = json.load(f)
        else:
            return jsonify({"error": "No accounts file found"}), 500

        child, master = find_account_by_client_id(accounts, client_id)
        if not child or not master or master.get("client_id") != master_id:
            return jsonify({"error": "❌ Child account or master not found."}), 404

        child["copy_status"] = "Off"

        with open("accounts.json", "w") as f:
            json.dump(accounts, f, indent=2)
        return jsonify({'message': f"🛑 Stopped copying for {client_id} under master {master_id}."}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
@app.route("/api/portfolio/<user_id>")
def get_portfolio(user_id):
    # Check users.json (external registered users)
    try:
        with open("users.json", "r") as f:
            users = json.load(f)
    except:
        users = {}

    if user_id in users:
        client_id = users[user_id]["client_id"]
        access_token = users[user_id]["access_token"]
        dhan = dhanhq(client_id, access_token)
        try:
            positions_resp = dhan.get_positions()
            return jsonify(positions_resp)
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    # Check in accounts.json using utility (for dashboard accounts)
    try:
        with open("accounts.json", "r") as f:
            accounts = json.load(f)
        found, _ = find_account_by_client_id(accounts, user_id)
        if not found:
            return jsonify({"error": "Invalid user ID"}), 403
        api = broker_api(found)
        positions_resp = api.get_positions()
        return jsonify(positions_resp)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# === API to get trade summary and open orders ===
@app.route("/api/orders/<user_id>")
def get_orders(user_id):
    try:
        with open("users.json", "r") as f:
            users = json.load(f)
    except Exception as e:
        print(f"❌ Failed to load users.json: {str(e)}")
        return jsonify({"error": "User DB not found"}), 500

    if user_id not in users:
        return jsonify({"error": "Invalid user ID"}), 403

    user = users[user_id]
    dhan = dhanhq(user["client_id"], user["access_token"])

    try:
        resp = dhan.get_order_list()
        print(f"👉 Full Dhan API response for {user_id}: {resp}")

        # Defensive check: is it the expected dict?
        if not isinstance(resp, dict) or "data" not in resp:
            return jsonify({"error": "Unexpected response format", "details": resp}), 500

        orders = resp["data"]  # ✅ the real list of orders now

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
    try:
        with open("users.json", "r") as f:
            users = json.load(f)
    except:
        return jsonify({"error": "User DB not found"}), 500

    if user_id not in users:
        return jsonify({"error": "Invalid user ID"}), 403

    user = users[user_id]
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



# === Page routes ===
@app.route('/')
def home():
    return render_template("index.html")

@app.route('/register')
def dashboard():
    return render_template("login.html")

@app.route('/dhan-dashboard')
def dhan_dashboard():
    return render_template("dhan-dashboard.html")

@app.route("/Summary")
def summary():
    return render_template("Summary.html")  # or "Summary.html" if that's your file name

@app.route("/copy-trading")
def copytrading():
    return render_template("copy-trading.html")

@app.route("/Add-Account")
def AddAccount():
    return render_template("Add-Account.html")

if __name__ == '__main__':
        app.run(debug=True)
