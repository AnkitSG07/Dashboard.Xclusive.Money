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
        if not os.path.exists("accounts.json"):
            print("⚠️ No accounts.json file found.")
            return

        with open("accounts.json", "r") as f:
            accounts = json.load(f)

        masters = accounts.get("masters", [])
        if not masters:
            print("⚠️ No master accounts configured.")
            return

        # Loop through each master account
        for master in masters:
            master_id = master.get("client_id")
            access_token = master.get("access_token")
            if not access_token:
                print(f"⚠️ Master {master_id} missing access token. Skipping.")
                continue

            last_copied_key = f"last_copied_trade_id_{master_id}"
            last_copied_trade_id = accounts.get(last_copied_key)
            new_last_trade_id = None

            dhan_master = dhanhq(master_id, access_token)
            orders_resp = dhan_master.get_order_list()
            order_list = orders_resp.get("data", [])
            if not order_list:
                print(f"ℹ️ No orders found for master {master_id}.")
                continue

            print(f"🛠 [{master_id}] Orders fetched: {len(order_list)}")
            order_list = sorted(order_list, key=lambda x: x.get("orderTimestamp", ""), reverse=True)

            for order in order_list:
                order_id = order.get("orderId") or order.get("order_id")
                if not order_id:
                    continue

                if order_id == last_copied_trade_id:
                    print(f"✅ [{master_id}] Reached last copied trade. Stopping here.")
                    break

                if order.get("orderStatus", "").upper() != "TRADED":
                    print(f"⏩ [{master_id}] Skipping order {order_id} (Status: {order.get('orderStatus')})")
                    continue

                print(f"✅ [{master_id}] New TRADED order: {order_id}")
                new_last_trade_id = new_last_trade_id or order_id

                children = master.get("children", [])
                if not children:
                    print(f"ℹ️ [{master_id}] No children to copy trades to.")
                    continue

                for child in children:
                    if child.get("copy_status") != "On":
                        print(f"➡️ Skipping child {child['client_id']} (copy_status is Off)")
                        continue

                    try:
                        dhan_child = dhanhq(child["client_id"], child["access_token"])
                        multiplier = float(child.get("multiplier", 1))
                        master_qty = order.get("quantity") or order.get("orderQuantity") or 1
                        copied_qty = max(1, int(float(master_qty) * multiplier))

                        print(f"➡️ Copying to child {child['client_id']} | Qty: {copied_qty} (Multiplier: {multiplier})")

                        security_id = order.get("securityId") or order.get("security_id")
                        exchange_segment = order.get("exchangeSegment") or order.get("exchange_segment")
                        transaction_type = order.get("transactionType") or order.get("transaction_type")
                        order_type = order.get("orderType") or order.get("order_type")
                        product_type = order.get("productType") or order.get("product_type")
                        price = order.get("price") or order.get("orderPrice") or 0

                        response = dhan_child.place_order(
                            security_id=security_id,
                            exchange_segment=exchange_segment,
                            transaction_type=transaction_type,
                            quantity=copied_qty,
                            order_type=order_type,
                            product_type=product_type,
                            price=price
                        )

                        if isinstance(response, dict) and response.get("status") == "failure":
                            error_msg = response.get("omsErrorDescription") or response.get("remarks") or "Unknown error"
                            print(f"❌ Trade FAILED for {child['client_id']} (Reason: {error_msg})")
                            save_log(
                                child["client_id"],
                                order.get("tradingSymbol") or order.get("symbol", ""),
                                transaction_type,
                                copied_qty,
                                "FAILED",
                                error_msg
                            )
                        else:
                            print(f"✅ Successfully copied to {child['client_id']} (Order Response: {response})")
                            save_log(
                                child["client_id"],
                                order.get("tradingSymbol") or order.get("symbol", ""),
                                transaction_type,
                                copied_qty,
                                "SUCCESS",
                                str(response)
                            )

                    except Exception as e:
                        print(f"❌ Error copying to {child['client_id']}: {e}")
                        save_log(
                            child["client_id"],
                            order.get("tradingSymbol") or order.get("symbol", ""),
                            transaction_type,
                            copied_qty,
                            "FAILED",
                            str(e)
                        )

            # After processing: update last_copied_trade_id for this master
            if new_last_trade_id:
                print(f"✅ Updating last_copied_trade_id for {master_id} to {new_last_trade_id}")
                accounts[last_copied_key] = new_last_trade_id

        # Save back the updated last_copied IDs
        with open("accounts.json", "w") as f:
            json.dump(accounts, f, indent=2)

    except Exception as e:
        print(f"❌ poll_and_copy_trades encountered an error: {e}")


scheduler = BackgroundScheduler()
scheduler.add_job(func=poll_and_copy_trades, trigger="interval", seconds=10)
scheduler.start()
print("✅ Background copy trader scheduler is running...")

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

@app.route('/api/square-off', methods=['POST'])
def square_off():
    data = request.json
    master_id = data.get("master_id")
    symbol = data.get("symbol")

    if not master_id or not symbol:
        return jsonify({"error": "Missing master_id or symbol"}), 400

    try:
        with open("accounts.json", "r") as f:
            accounts = json.load(f)
    except Exception as e:
        return jsonify({"error": "Failed to load accounts file"}), 500

    master = next((m for m in accounts.get("masters", []) if m["client_id"] == master_id), None)
    if not master:
        return jsonify({"error": "Master not found"}), 404

    results = []
    for child in master.get("children", []):
        if child.get("copy_status") != "On":
            results.append(f"Child {child['client_id']} → Skipped (copy OFF)")
            continue

        try:
            dhan_child = dhanhq(child["client_id"], child["access_token"])
            
            positions_resp = dhan_child.get_positions()
            positions = positions_resp.get('data', [])
            has_position = any(pos['tradingSymbol'].upper() == symbol.upper() and pos['netQty'] != 0 for pos in positions)

            if not has_position:
                results.append(f"Child {child['client_id']} → Skipped (no position in {symbol})")
                continue

            response = dhan_child.square_off_position(symbol)

            if isinstance(response, dict) and response.get("status") == "failure":
                msg = response.get("remarks", "Unknown error")
                results.append(f"Child {child['client_id']} → FAILED: {msg}")
                save_log(child['client_id'], symbol, "SQUARE_OFF", 0, "FAILED", msg)
            else:
                results.append(f"Child {child['client_id']} → SUCCESS")
                save_log(child['client_id'], symbol, "SQUARE_OFF", 0, "SUCCESS", str(response))

        except Exception as e:
            error_msg = str(e)
            results.append(f"Child {child['client_id']} → ERROR: {error_msg}")
            save_log(child['client_id'], symbol, "SQUARE_OFF", 0, "ERROR", error_msg)

    return jsonify({"message": "Square-off completed", "details": results}), 200



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

    updated = False
    for master in accounts.get("masters", []):
        for child in master.get("children", []):
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
    linked_master_id = data.get("linked_master_id")  # NEW: Get master link

    if not all([client_id, username, role]):
        return jsonify({"error": "Missing required fields"}), 400

    try:
        if os.path.exists("accounts.json"):
            with open("accounts.json", "r") as f:
                accounts = json.load(f)
        else:
            accounts = {"masters": []}  # NEW structure

        if "masters" not in accounts:
            accounts["masters"] = []

        if role.lower() == "master":
            # Add new master
            accounts["masters"].append({
                "broker": "Dhan",
                "client_id": client_id,
                "username": username,
                "access_token": access_token,
                "status": "Connected",
                "children": []
            })
            message = f"✅ Master account {username} added."

        elif role.lower() == "child":
            if not linked_master_id:
                return jsonify({"error": "Missing linked_master_id for child"}), 400

            # Find the master by client_id or username
            found_master = None
            for master in accounts["masters"]:
                if master["client_id"] == linked_master_id or master["username"] == linked_master_id:
                    found_master = master
                    break

            if not found_master:
                return jsonify({"error": "Linked master not found"}), 400

            # Add child under the found master
            found_master["children"].append({
                "broker": "Dhan",
                "client_id": client_id,
                "username": username,
                "access_token": access_token,
                "status": "Connected",
                "copy_status": "Off",
                "multiplier": multiplier
            })
            message = f"✅ Child account {username} added under master {found_master['username']}."

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

        # Format the response cleanly
        formatted = []
        for master in accounts.get("masters", []):
            formatted.append({
                "role": "master",
                "broker": master.get("broker"),
                "client_id": master.get("client_id"),
                "username": master.get("username"),
                "status": master.get("status"),
                "children": master.get("children", [])
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

    accounts["master"] = found

    with open("accounts.json", "w") as f:
        json.dump(accounts, f, indent=2)

    return jsonify({'message': f"✅ Set {client_id} as master successfully."})



# Start copying for a child account
@app.route('/api/start-copy', methods=['POST'])
def start_copy():
    data = request.json
    client_id = data.get("client_id")
    master_id = data.get("master_id")  # NEW: to specify which master this child belongs to

    if not client_id or not master_id:
        return jsonify({"error": "Missing client_id or master_id"}), 400

    try:
        if os.path.exists("accounts.json"):
            with open("accounts.json", "r") as f:
                accounts = json.load(f)
        else:
            return jsonify({"error": "No accounts file found"}), 500

        updated = False
        for master in accounts.get("masters", []):
            if master["client_id"] == master_id:
                for child in master.get("children", []):
                    if child["client_id"] == client_id:
                        child["copy_status"] = "On"
                        updated = True
                        break

        if updated:
            with open("accounts.json", "w") as f:
                json.dump(accounts, f, indent=2)
            return jsonify({'message': f"✅ Started copying for {client_id} under master {master_id}."}), 200
        else:
            return jsonify({"error": "❌ Child account or master not found."}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/stop-copy', methods=['POST'])
def stop_copy():
    data = request.json
    client_id = data.get("client_id")
    master_id = data.get("master_id")  # NEW: to specify which master this child belongs to

    if not client_id or not master_id:
        return jsonify({"error": "Missing client_id or master_id"}), 400

    try:
        if os.path.exists("accounts.json"):
            with open("accounts.json", "r") as f:
                accounts = json.load(f)
        else:
            return jsonify({"error": "No accounts file found"}), 500

        updated = False
        for master in accounts.get("masters", []):
            if master["client_id"] == master_id:
                for child in master.get("children", []):
                    if child["client_id"] == client_id:
                        child["copy_status"] = "Off"
                        updated = True
                        break

        if updated:
            with open("accounts.json", "w") as f:
                json.dump(accounts, f, indent=2)
            return jsonify({'message': f"🛑 Stopped copying for {client_id} under master {master_id}."}), 200
        else:
            return jsonify({"error": "❌ Child account or master not found."}), 404

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

        updated = False
        for master in accounts.get("masters", []):
            original_count = len(master.get("children", []))
            master["children"] = [child for child in master.get("children", []) if child["client_id"] != client_id]
            if len(master["children"]) < original_count:
                updated = True
                break

        if updated:
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
        users = {}

    client_id = None
    access_token = None

    if user_id in users:
        client_id = users[user_id]["client_id"]
        access_token = users[user_id]["access_token"]
    else:
        try:
            with open("accounts.json", "r") as f:
                accounts = json.load(f)
            for master in accounts.get("masters", []):
                for child in master.get("children", []):
                    if child["client_id"] == user_id:
                        client_id = child["client_id"]
                        access_token = child["access_token"]
                        break
                if client_id:
                    break
        except:
            return jsonify({"error": "accounts.json not found"}), 500

    if not client_id or not access_token:
        return jsonify({"error": "Invalid user ID"}), 403

    dhan = dhanhq(client_id, access_token)

    try:
        positions_resp = dhan.get_positions()
        positions = positions_resp.get('data', [])
        return jsonify({"positions": positions})
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
        app.run(debug=True)
