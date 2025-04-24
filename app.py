from flask import Flask, request, jsonify
from dhanhq import dhanhq
import os
import sqlite3
from datetime import datetime

app = Flask(__name__)

# Static symbol to security_id mapping (expand or fetch dynamically as needed)
SYMBOL_MAP = {
    "RELIANCE": "1333",
    "TCS": "11536",
    "INFY": "10999"
}

# Initialize SQLite DB for logs
conn = sqlite3.connect('tradelogs.db', check_same_thread=False)
cursor = conn.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    client_id TEXT,
                    symbol TEXT,
                    action TEXT,
                    quantity INTEGER,
                    status TEXT,
                    response TEXT
                )''')
conn.commit()

@app.route("/dhan-webhook", methods=["POST"])
def dhan_webhook():
    data = request.json
    client_id = data.get("client_id")
    access_token = data.get("access_token")
    symbol = data.get("symbol")
    action = data.get("action")
    quantity = data.get("quantity")

    if not all([client_id, access_token, symbol, action, quantity]):
        return jsonify({"error": "Missing required fields: client_id, access_token, symbol, action, or quantity."}), 400

    security_id = SYMBOL_MAP.get(symbol.upper())
    if not security_id:
        return jsonify({"error": "Unsupported symbol or missing mapping."}), 400

    try:
        user_dhan = dhanhq(client_id, access_token)
        order = user_dhan.place_order(
            security_id=security_id,
            exchange_segment=user_dhan.NSE,
            transaction_type=user_dhan.BUY if action.upper() == "BUY" else user_dhan.SELL,
            quantity=int(quantity),
            order_type=user_dhan.MARKET,
            product_type=user_dhan.INTRA,
            price=0
        )

        # Save log to SQLite
        cursor.execute("INSERT INTO logs (timestamp, client_id, symbol, action, quantity, status, response) VALUES (?, ?, ?, ?, ?, ?, ?)",
                       (datetime.now().isoformat(), client_id, symbol, action, quantity, 'SUCCESS', str(order)))
        conn.commit()

        return jsonify(order)
    except Exception as e:
        cursor.execute("INSERT INTO logs (timestamp, client_id, symbol, action, quantity, status, response) VALUES (?, ?, ?, ?, ?, ?, ?)",
                       (datetime.now().isoformat(), client_id, symbol, action, quantity, 'FAILED', str(e)))
        conn.commit()
        return jsonify({"error": str(e)}), 500

@app.route("/logs", methods=["GET"])
def get_logs():
    cursor.execute("SELECT timestamp, client_id, symbol, action, quantity, status, response FROM logs ORDER BY id DESC LIMIT 100")
    rows = cursor.fetchall()
    logs = [
        {
            "timestamp": r[0],
            "client_id": r[1],
            "symbol": r[2],
            "action": r[3],
            "quantity": r[4],
            "status": r[5],
            "response": r[6]
        } for r in rows
    ]
    return jsonify(logs)

if __name__ == "__main__":
    app.run(debug=True)
