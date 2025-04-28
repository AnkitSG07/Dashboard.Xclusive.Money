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

app = Flask(__name__)
CORS(app)

RAPIDAPI_KEY = "1c99b13c79msh266bd26283ae7f3p1ded7djsn92d495c38bab"  # 👉 Replace this with your real key
RAPIDAPI_HOST = "yahoo-finance166.p.rapidapi.com"


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

@app.route("/marketwatch")
def market_watch():
    return render_template("marketwatch.html")

@app.route('/api/market/gainers')
def market_gainers():
    try:
        url = f"https://{RAPIDAPI_HOST}/market/get-movers"
        querystring = {"region":"IN","lang":"en","count":"10","start":"0","sortBy":"GAINER"}
        headers = {
            "X-RapidAPI-Key": RAPIDAPI_KEY,
            "X-RapidAPI-Host": RAPIDAPI_HOST
        }
        response = requests.get(url, headers=headers, params=querystring)
        data = response.json()

        gainers = []
        # Correct path: finance.result[0].quotes
        for stock in data.get('finance', {}).get('result', [])[0].get('quotes', []):
            gainers.append({
                "symbol": stock.get('symbol', 'N/A'),
                "price": stock.get('regularMarketPrice', 0),
                "pChange": stock.get('regularMarketChangePercent', 0)
            })

        return jsonify(gainers)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# 📉 Similarly for Top Losers:
@app.route('/api/market/losers')
def market_losers():
    try:
        url = f"https://{RAPIDAPI_HOST}/market/get-movers"
        querystring = {"region":"IN","lang":"en","count":"10","start":"0","sortBy":"LOSER"}
        headers = {
            "X-RapidAPI-Key": RAPIDAPI_KEY,
            "X-RapidAPI-Host": RAPIDAPI_HOST
        }
        response = requests.get(url, headers=headers, params=querystring)
        data = response.json()

        losers = []
        # Correct path: finance.result[0].quotes
        for stock in data.get('finance', {}).get('result', [])[0].get('quotes', []):
            losers.append({
                "symbol": stock.get('symbol', 'N/A'),
                "price": stock.get('regularMarketPrice', 0),
                "pChange": stock.get('regularMarketChangePercent', 0)
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
    except:
        return jsonify({"error": "User DB not found"}), 500

    if user_id not in users:
        return jsonify({"error": "Invalid user ID"}), 403

    user = users[user_id]
    dhan = dhanhq(user["client_id"], user["access_token"])

    try:
        orders = dhan.get_order_list()
        total_trades = len(orders)
        last_order = orders[0] if orders else {}
        total_qty = sum(int(o.get("quantity", 0)) for o in orders)

        return jsonify({
            "orders": orders,
            "summary": {
                "total_trades": total_trades,
                "last_status": last_order.get("order_status", "N/A"),
                "total_quantity": total_qty
            }
        })
    except Exception as e:
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
        stats = dhan.get_fund_limits()
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# === Page routes ===
@app.route('/')
def home():
    return render_template("login.html")

@app.route('/dashboard')
def dashboard():
    return render_template("dhan-dashboard.html")

if __name__ == "__main__":
    app.run(debug=True)
