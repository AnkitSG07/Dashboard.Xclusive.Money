from flask import Flask, request, jsonify, render_template
from dhanhq import dhanhq
import sqlite3
import os
import json
import pandas as pd
from flask_cors import CORS
import io
from datetime import datetime

app = Flask(__name__)
CORS(app)

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
    data = request.json
    message = data.get("message")  # Passive alert support

    if message:
        save_log(user_id, "-", "-", 0, "ALERT", message)
        return jsonify({"status": "Alert logged", "message": message}), 200

    symbol = data.get("symbol")
    action = data.get("action")
    quantity = data.get("quantity")

    if not all([symbol, action, quantity]):
        return jsonify({"error": "Missing required fields"}), 400

    # Load user credentials
    try:
        with open("users.json", "r") as f:
            users = json.load(f)
    except FileNotFoundError:
        return jsonify({"error": "User DB not found"}), 500

    if user_id not in users:
        return jsonify({"error": "Invalid webhook ID"}), 403

    user = users[user_id]
    client_id = user["client_id"]
    access_token = user["access_token"]
    dhan = dhanhq(client_id, access_token)

    SYMBOL_MAP = {
        "RELIANCE": "2885",
        "TCS": "11536",
        "INFY": "10999",
        "ADANIPORTS": "15083",
        "HDFCBANK": "1333",
        "SBIN": "3045",
        "ICICIBANK": "4963",
        "AXISBANK": "1343",
        "ITC": "1660",
        "HINDUNILVR": "1394",
        "KOTAKBANK": "1922",
        "LT": "11483",
        "BAJFINANCE": "317",
        "HCLTECH": "7229",
        "ASIANPAINT": "236",
        "MARUTI": "1095",
        "M&M": "2031",
        "SUNPHARMA": "3046",
        "TATAMOTORS": "3432",
        "WIPRO": "3787",
        "ULTRACEMCO": "11532",
        "TITAN": "3506",
        "NESTLEIND": "11262",
        "BAJAJFINSV": "317",
        "POWERGRID": "14977",
        "NTPC": "2886",
        "JSWSTEEL": "11723",
        "HDFCLIFE": "11915",
        "DRREDDY": "881",
        "TECHM": "11534",
        "BRITANNIA": "293",
        "TATASTEEL": "3505",
        "CIPLA": "694",
        "SBILIFE": "11916",
        "BAJAJ-AUTO": "317",
        "HINDALCO": "1393",
        "DIVISLAB": "881",
        "GRASIM": "1147",
        "ADANIENT": "15083",
        "COALINDIA": "694",
        "INDUSINDBK": "1393",
        "TATACONSUM": "3505",
        "EICHERMOT": "881",
        "SHREECEM": "1147",
        "HEROMOTOCO": "15083",
        "BAJAJHLDNG": "694",
        "SBICARD": "1393",
        "DLF": "3505",
        "DMART": "881",
        "UPL": "1147",
        "ICICIPRULI": "15083",
        "HDFCAMC": "694",
        "HDFC": "1393",
        "GAIL": "3505",
        "HAL": "881",
        "TATAPOWER": "1147",
        "VEDL": "15083",
        "BPCL": "694",
        "IOC": "1393",
        "ONGC": "3505",
        "LICHSGFIN": "881",
        "BANKBARODA": "1147",
        "PNB": "15083",
        "CANBK": "694",
        "UNIONBANK": "1393",
        "IDFCFIRSTB": "3505",
        "BANDHANBNK": "881",
        "FEDERALBNK": "1147",
        "RBLBANK": "15083",
        "YESBANK": "694",
        "IGL": "1393",
        "PETRONET": "3505",
        "GUJGASLTD": "881",
        "MGL": "1147",
        "TORNTPHARM": "15083",
        "LUPIN": "694",
        "AUROPHARMA": "1393",
        "BIOCON": "3505",
        "GLENMARK": "881",
        "CADILAHC": "1147",
        "ALKEM": "15083",
        "APOLLOHOSP": "694",
        "MAXHEALTH": "1393",
        "FORTIS": "3505",
        "JUBLFOOD": "881",
        "UBL": "1147",
        "MCDOWELL-N": "15083",
        "COLPAL": "694",
        "DABUR": "1393",
        "GODREJCP": "3505",
        "MARICO": "881",
        "EMAMILTD": "1147",
        "PGHH": "15083",
        "GILLETTE": "694",
        "TATACHEM": "1393",
        "PIDILITIND": "3505",
        "BERGEPAINT": "881",
        "KANSAINER": "1147",
        "JSWENERGY": "15083",
        "ADANIGREEN": "694",
        "ADANITRANS": "1393",
        "NHPC": "3505",
        "SJVN": "881",
        "RECLTD": "1147",
        "PFC": "15083"
    }

    security_id = SYMBOL_MAP.get(symbol.strip().upper())

    if not security_id:
        return jsonify({"error": f"Symbol '{symbol}' not found in SYMBOL_MAP"}), 400

    try:
        response = dhan.place_order(
            security_id=security_id,
            exchange_segment=dhan.NSE,
            transaction_type=dhan.BUY if action.upper() == "BUY" else dhan.SELL,
            quantity=int(quantity),
            order_type=dhan.MARKET,
            product_type=dhan.INTRA,
            price=0
        )
        save_log(user_id, symbol, action, quantity, "SUCCESS", str(response))
        return jsonify({"status": "Trade Placed", "result": response})
    except Exception as e:
        save_log(user_id, symbol, action, quantity, "FAILED", str(e))
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

@app.route("/api/chart/trades")
def chart_trades():
    user_id = request.args.get("user_id")

    try:
        with open("users.json", "r") as f:
            users = json.load(f)
        if user_id not in users:
            return jsonify({"error": "Invalid user ID"}), 403
    except:
        return jsonify({"error": "User DB not found"}), 500

    user = users[user_id]
    dhan = dhanhq(user["client_id"], user["access_token"])

    try:
        trades = dhan.get_order_list()
        buy_count = sum(1 for t in trades if t["transactionType"] == "BUY")
        sell_count = sum(1 for t in trades if t["transactionType"] == "SELL")

        return jsonify({
            "labels": ["Buy", "Sell"],
            "values": [buy_count, sell_count],
            "colors": ["#007bff", "#ffc107"]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
        
@app.route("/api/chart/pnl")
def chart_pnl():
    user_id = request.args.get("user_id")

    try:
        with open("users.json", "r") as f:
            users = json.load(f)
        if user_id not in users:
            return jsonify({"error": "Invalid user ID"}), 403
    except:
        return jsonify({"error": "User DB not found"}), 500

    user = users[user_id]
    dhan = dhanhq(user["client_id"], user["access_token"])

    try:
        holdings = dhan.get_holdings()
        labels = []
        values = []

        for holding in holdings[:5]:  # top 5
            labels.append(holding["tradingSymbol"])
            pnl = float(holding.get("unrealizedProfitLossAmount", 0))
            values.append(round(pnl, 2))

        return jsonify({"labels": labels, "values": values})
    except Exception as e:
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
