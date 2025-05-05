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

def poll_and_copy_trades():
    print("🔄 poll_and_copy_trades() triggered...")

    try:
        # ✅ Load accounts.json
        if os.path.exists("accounts.json"):
            with open("accounts.json", "r") as f:
                accounts = json.load(f)
        else:
            print("⚠️ No accounts.json file found.")
            return

        master = accounts.get("master")
        if not master or not master.get("access_token"):
            print("⚠️ No master account is configured or missing access token.")
            return

        # ✅ Get the last copied trade from accounts.json
        last_copied_trade_id = accounts.get("last_copied_trade_id")

        dhan_master = dhanhq(master["client_id"], master["access_token"])
        orders_resp = dhan_master.get_order_list()

        # ✅ Extract orders from Dhan API
        order_list = orders_resp.get("data", [])
        if not order_list:
            print("ℹ️ No orders found for master account.")
            return

        # ✅ Grab the latest order
        latest_order = order_list[0]
        print(f"🛠 Latest order details:\n{json.dumps(latest_order, indent=2)}")

        order_id = latest_order.get("orderId") or latest_order.get("order_id")  # handle both cases

        if not order_id:
            print("⚠️ Could not find order_id in the latest order. Skipping.")
            return

        # ✅ Check if already copied
        if order_id == last_copied_trade_id:
            print("✅ No new trades to copy. Latest already processed.")
            return

        print(f"✅ New master trade detected: Order ID {order_id}")

        # 🔥 Update accounts.json
        accounts["last_copied_trade_id"] = order_id
        with open("accounts.json", "w") as f:
            json.dump(accounts, f, indent=2)

        # ✅ Start copying to child accounts
        children = accounts.get("children", [])
        if not children:
            print("ℹ️ No child accounts found.")
            return

        for child in children:
            if child.get("copy_status") != "On":
                print(f"➡️ Skipping child {child['client_id']} (copy_status is Off)")
                continue

            try:
                dhan_child = dhanhq(child["client_id"], child["access_token"])
                multiplier = float(child.get("multiplier", 1))
                master_qty = latest_order.get("quantity") or latest_order.get("orderQuantity") or 1
                copied_qty = max(1, int(float(master_qty) * multiplier))

                print(f"➡️ Copying to child {child['client_id']} | Qty: {copied_qty} (Multiplier: {multiplier})")

                # ✅ Extract required fields safely
                security_id = latest_order.get("securityId") or latest_order.get("security_id")
                exchange_segment = latest_order.get("exchangeSegment") or latest_order.get("exchange_segment")
                transaction_type = latest_order.get("transactionType") or latest_order.get("transaction_type")
                order_type = latest_order.get("orderType") or latest_order.get("order_type")
                product_type = latest_order.get("productType") or latest_order.get("product_type")
                price = latest_order.get("price") or latest_order.get("orderPrice") or 0

                # 🛠 Log field checks
                print(f"""🔎 Fields being sent:
    security_id: {security_id}
    exchange_segment: {exchange_segment}
    transaction_type: {transaction_type}
    order_type: {order_type}
    product_type: {product_type}
    price: {price}""")

                # 🚀 Place the order
                response = dhan_child.place_order(
                    security_id=security_id,
                    exchange_segment=exchange_segment,
                    transaction_type=transaction_type,
                    quantity=copied_qty,
                    order_type=order_type,
                    product_type=product_type,
                    price=price
                )

                # ✅ Check if FAILED or SUCCESS
                if isinstance(response, dict) and response.get("status") == "failure":
                    error_msg = response.get("omsErrorDescription") or response.get("remarks") or "Unknown error"
                    print(f"❌ Trade FAILED for {child['client_id']} (Reason: {error_msg})")
                    save_log(
                        child["client_id"],
                        latest_order.get("tradingSymbol") or latest_order.get("symbol", ""),
                        transaction_type,
                        copied_qty,
                        "FAILED",
                        error_msg
                    )
                else:
                    print(f"✅ Successfully copied to {child['client_id']} (Order Response: {response})")
                    save_log(
                        child["client_id"],
                        latest_order.get("tradingSymbol") or latest_order.get("symbol", ""),
                        transaction_type,
                        copied_qty,
                        "SUCCESS",
                        str(response)
                    )

            except Exception as e:
                print(f"❌ Error copying to {child['client_id']}: {e}")
                save_log(
                    child["client_id"],
                    latest_order.get("tradingSymbol") or latest_order.get("symbol", ""),
                    transaction_type,
                    copied_qty,
                    "FAILED",
                    str(e)
                )

    except Exception as e:
        print(f"❌ poll_and_copy_trades encountered an error: {e}")


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


@app.route('/api/update-multiplier', methods=['POST'])
def update_multiplier():
    data = request.json
    client_id = data.get("client_id")
    new_multiplier = data.get("multiplier")

    if not client_id or new_multiplier is None:
        return jsonify({"error": "Missing required fields"}), 400

    try:
        # Validate multiplier is a float and >= 0.1
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

        if not isinstance(accounts.get("children"), list):
            return jsonify({"error": "Invalid accounts file format"}), 500

        updated = False
        for child in accounts["children"]:
            if child["client_id"] == client_id:
                child["multiplier"] = new_multiplier
                updated = True
                break

        if updated:
            with open("accounts.json", "w") as f:
                json.dump(accounts, f, indent=2)
            return jsonify({"message": f"Multiplier updated to {new_multiplier} for {client_id}"}), 200
        else:
            return jsonify({"error": "Child account not found"}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/marketwatch")
def market_watch():
    return render_template("marketwatch.html")

@app.route('/api/add-account', methods=['POST'])
def add_account():
    data = request.json
    client_id = data.get("client_id")
    access_token = data.get("access_token")
    username = data.get("username")
    role = data.get("role")
    multiplier = float(data.get("multiplier", 1))

    if not all([client_id, username, role]):
        return jsonify({"error": "Missing required fields"}), 400

    try:
        if os.path.exists("accounts.json"):
            with open("accounts.json", "r") as f:
                accounts = json.load(f)
        else:
            accounts = {"master": None, "children": []}

        if role == "master":
            accounts["master"] = {
                "broker": "Dhan",
                "client_id": client_id,
                "username": username,
                "access_token": access_token,
                "status": "Connected"
            }
        else:
            accounts["children"].append({
                "broker": "Dhan",
                "client_id": client_id,
                "username": username,
                "access_token": access_token,
                "status": "Connected",
                "copy_status": "Off",
                "multiplier": multiplier
            })

        with open("accounts.json", "w") as f:
            json.dump(accounts, f, indent=2)

        return jsonify({"message": "Account added successfully"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Get all trading accounts (sample data for now)
@app.route('/api/accounts')
def get_accounts():
    if os.path.exists("accounts.json"):
        with open("accounts.json", "r") as f:
            accounts = json.load(f)
    else:
        accounts = {"master": None, "children": []}
    return jsonify(accounts)

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

    # Find the new master in children or existing master
    found = None
    if accounts.get("master") and accounts["master"]["client_id"] == client_id:
        found = accounts["master"]
    else:
        for child in accounts.get("children", []):
            if child["client_id"] == client_id:
                found = {
                    "broker": child["broker"],
                    "client_id": child["client_id"],
                    "username": child["username"],
                    "access_token": child["access_token"],
                    "status": child["status"]
                }
                break

    if not found:
        return jsonify({"error": f"Client ID {client_id} not found in accounts."}), 404

    accounts["master"] = found

    with open("accounts.json", "w") as f:
        json.dump(accounts, f, indent=2)

    return jsonify({'message': f"✅ Set {client_id} as master successfully."})


# Start copying for a child account
@app.route('/api/start-copy', methods=['POST'])
def start_copy():
    data = request.json
    client_id = data.get("client_id")

    if not client_id:
        return jsonify({"error": "Missing client_id"}), 400

    try:
        if os.path.exists("accounts.json"):
            with open("accounts.json", "r") as f:
                accounts = json.load(f)
        else:
            return jsonify({"error": "No accounts file found"}), 500

        updated = False
        for child in accounts.get("children", []):
            if child["client_id"] == client_id:
                child["copy_status"] = "On"
                updated = True
                break

        if updated:
            with open("accounts.json", "w") as f:
                json.dump(accounts, f, indent=2)
            return jsonify({'message': f"✅ Started copying for {client_id}."}), 200
        else:
            return jsonify({"error": "❌ Child account not found."}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/stop-copy', methods=['POST'])
def stop_copy():
    data = request.json
    client_id = data.get("client_id")

    if not client_id:
        return jsonify({"error": "Missing client_id"}), 400

    try:
        if os.path.exists("accounts.json"):
            with open("accounts.json", "r") as f:
                accounts = json.load(f)
        else:
            return jsonify({"error": "No accounts file found"}), 500

        updated = False
        for child in accounts.get("children", []):
            if child["client_id"] == client_id:
                child["copy_status"] = "Off"
                updated = True
                break

        if updated:
            with open("accounts.json", "w") as f:
                json.dump(accounts, f, indent=2)
            return jsonify({'message': f"🛑 Stopped copying for {client_id}."}), 200
        else:
            return jsonify({"error": "❌ Child account not found."}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500



# Remove a child account
@app.route('/api/remove-child', methods=['POST'])
def remove_child():
    data = request.json
    client_id = data.get("client_id")

    if not client_id:
        return jsonify({"error": "Missing client_id"}), 400

    try:
        if os.path.exists("accounts.json"):
            with open("accounts.json", "r") as f:
                accounts = json.load(f)
        else:
            return jsonify({"error": "No accounts file found"}), 500

        original_count = len(accounts.get("children", []))
        accounts["children"] = [child for child in accounts.get("children", []) if child["client_id"] != client_id]

        if len(accounts["children"]) < original_count:
            with open("accounts.json", "w") as f:
                json.dump(accounts, f, indent=2)
            return jsonify({'message': f"✅ Removed child {client_id}."}), 200
        else:
            return jsonify({"error": "❌ Child account not found."}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route('/copy-trading')
def copy_trading():
    # This renders the copy-trading.html you showed
    return render_template('copy-trading.html')

@app.route('/api/market/gainers')
def market_gainers():
    try:
        url = f"https://{RAPIDAPI_HOST}/market/v2/get-movers"
        querystring = {"region": "US", "lang": "en", "count": "6", "start": "0"}
        headers = {
            "X-RapidAPI-Key": RAPIDAPI_KEY,
            "X-RapidAPI-Host": RAPIDAPI_HOST
        }
        response = requests.get(url, headers=headers, params=querystring)
        data = response.json()

        quotes = data.get("finance", {}).get("result", [])[0].get("quotes", [])
        gainers = []
        for stock in quotes:
            current_price = stock.get('regularMarketPrice', 0)
            previous_close = stock.get('regularMarketPreviousClose', 0)
            if previous_close and current_price:
                pChange = ((current_price - previous_close) / previous_close) * 100
            else:
                pChange = 0

            gainers.append({
                "symbol": stock.get('symbol', 'N/A'),
                "price": current_price,
                "pChange": pChange
            })

        return jsonify(gainers)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/market/losers')
def market_losers():
    try:
        url = f"https://{RAPIDAPI_HOST}/market/v2/get-movers"
        querystring = {"region": "US", "lang": "en", "count": "6", "start": "0"}
        headers = {
            "X-RapidAPI-Key": RAPIDAPI_KEY,
            "X-RapidAPI-Host": RAPIDAPI_HOST
        }
        response = requests.get(url, headers=headers, params=querystring)
        data = response.json()

        quotes = data.get("finance", {}).get("result", [])[1].get("quotes", [])
        losers = []
        for stock in quotes:
            current_price = stock.get('regularMarketPrice', 0)
            previous_close = stock.get('regularMarketPreviousClose', 0)
            if previous_close and current_price:
                pChange = ((current_price - previous_close) / previous_close) * 100
            else:
                pChange = 0

            losers.append({
                "symbol": stock.get('symbol', 'N/A'),
                "price": current_price,
                "pChange": pChange
            })

        return jsonify(losers)
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
        holdings = dhan.get_holdings()
        return jsonify(holdings)
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


@app.route("/api/chart/pnl")
def chart_pnl():
    user_id = request.args.get("user_id")
    if not user_id:
        return jsonify({"error": "Missing user_id"}), 400

    conn = sqlite3.connect("tradelogs.db")
    c = conn.cursor()

    # Fetch last 5 PnL (unrealized PnL) entries
    c.execute("""
        SELECT timestamp, response 
        FROM logs 
        WHERE user_id = ? AND status = 'SUCCESS' 
        ORDER BY id DESC LIMIT 5
    """, (user_id,))
    rows = c.fetchall()
    conn.close()

    labels = []
    pnl_values = []

    for row in rows:
        labels.append(row[0][:10])  # Use date only (first 10 chars of timestamp)

        try:
            # Try to extract any "unrealized_pl" or "pnl" if present inside response JSON
            resp = eval(row[1]) if isinstance(row[1], str) else {}
            pnl = 0
            if isinstance(resp, dict):
                pnl = float(resp.get("unrealizedPnL", 0))
            pnl_values.append(pnl)
        except:
            pnl_values.append(0)

    if not labels:
        labels = ["Day1", "Day2", "Day3", "Day4", "Day5"]
        pnl_values = [0, 0, 0, 0, 0]

    return jsonify({"labels": labels[::-1], "values": pnl_values[::-1]})


# === Real API for Trades Chart (Buy vs Sell from logs) ===
@app.route("/api/chart/trades")
def chart_trades():
    user_id = request.args.get("user_id")
    if not user_id:
        return jsonify({"error": "Missing user_id"}), 400

    conn = sqlite3.connect("tradelogs.db")
    c = conn.cursor()

    # Count Buy and Sell from logs
    c.execute("""
        SELECT action, COUNT(*) 
        FROM logs 
        WHERE user_id = ? AND status = 'SUCCESS'
        GROUP BY action
    """, (user_id,))
    rows = c.fetchall()
    conn.close()

    buy_count = 0
    sell_count = 0

    for action, count in rows:
        if action.upper() == "BUY":
            buy_count += count
        elif action.upper() == "SELL":
            sell_count += count

    return jsonify({
        "labels": ["Buy", "Sell"],
        "values": [buy_count, sell_count],
        "colors": ["#28a745", "#dc3545"]
    })


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
    return render_template("login.html")

@app.route('/dashboard')
def dashboard():
    return render_template("dhan-dashboard.html")

if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=poll_and_copy_trades, trigger="interval", seconds=10)
    scheduler.start()
    print("Background copy trader is running...")

    try:
        app.run(debug=True, use_reloader=False)  # use_reloader=False is IMPORTANT
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()

