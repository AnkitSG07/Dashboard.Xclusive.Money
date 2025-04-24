from flask import Flask, request, jsonify, render_template
from dhanhq import dhanhq
from datetime import datetime
import sqlite3
import os

app = Flask(__name__)

# Create logs table if not exists
def init_db():
    conn = sqlite3.connect("tradelogs.db")
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            client_id TEXT,
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

@app.route("/dhan-webhook", methods=["POST"])
def dhan_webhook():
    data = request.json

    client_id = data.get("client_id")
    access_token = data.get("access_token")
    symbol = data.get("symbol")
    action = data.get("action")
    quantity = data.get("quantity")

    if not all([client_id, access_token, symbol, action, quantity]):
        return jsonify({"error": "Missing fields"}), 400

    dhan = dhanhq(client_id, access_token)

    # Sample static symbol map
    SYMBOL_MAP = {
        "RELIANCE": "1333",
        "TCS": "11536",
        "INFY": "10999"
    }

    security_id = SYMBOL_MAP.get(symbol.upper())
    if not security_id:
        return jsonify({"error": "Symbol not mapped"}), 400

    try:
        result = dhan.place_order(
            security_id=security_id,
            exchange_segment=dhan.NSE,
            transaction_type=dhan.BUY if action.upper() == "BUY" else dhan.SELL,
            quantity=int(quantity),
            order_type=dhan.MARKET,
            product_type=dhan.INTRA,
            price=0
        )

        save_log(client_id, symbol, action, quantity, "SUCCESS", str(result))
        return jsonify({"status": "Order placed", "result": result})

    except Exception as e:
        save_log(client_id, symbol, action, quantity, "FAILED", str(e))
        return jsonify({"error": str(e)}), 500

def save_log(client_id, symbol, action, quantity, status, response):
    conn = sqlite3.connect("tradelogs.db")
    c = conn.cursor()
    c.execute("""
        INSERT INTO logs (timestamp, client_id, symbol, action, quantity, status, response)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (datetime.now().isoformat(), client_id, symbol, action, quantity, status, response))
    conn.commit()
    conn.close()

@app.route("/logs")
def get_logs():
    client_id = request.args.get("client_id")
    conn = sqlite3.connect("tradelogs.db")
    c = conn.cursor()
    if client_id:
        c.execute("SELECT * FROM logs WHERE client_id = ? ORDER BY id DESC LIMIT 100", (client_id,))
    else:
        c.execute("SELECT * FROM logs ORDER BY id DESC LIMIT 100")
    rows = c.fetchall()
    conn.close()

    logs = []
    for row in rows:
        logs.append({
            "timestamp": row[1],
            "client_id": row[2],
            "symbol": row[3],
            "action": row[4],
            "quantity": row[5],
            "status": row[6],
            "response": row[7]
        })

    return jsonify(logs)

@app.route('/')
def home():
    return render_template('login.html')

@app.route('/dashboard')
def dashboard():
    return render_template('dhan-dashboard.html')

if __name__ == "__main__":
    app.run(debug=True)
