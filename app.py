from flask import Flask, request, jsonify, render_template
from dhanhq import dhanhq
import sqlite3
import os
import json
import pandas as pd
import io
from datetime import datetime

app = Flask(__name__)

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

    # ✅ Static SYMBOL_MAP
    SYMBOL_MAP = {
        "RELIANCE": "2885",
        "TCS": "11536",
        "INFY": "10999",
        "ADANIPORTS": "15083",
        "HDFCBANK": "1333",
        "SBIN": "3045",
        "ICICIBANK": "4963",
        "AXISBANK": "1343",
        "ITC": "1660"
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
