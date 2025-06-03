# brokers/aliceblue.py

import requests
from .base import BrokerBase

class AliceBlueBroker(BrokerBase):
    # Constants for Alice Blue
    EXCHANGE = "NSE"       # or "BSE", "MCX"
    PRODUCT = "MIS"        # "CNC" for delivery, "MIS" for intraday
    ORDER_TYPE = "MARKET"  # "LIMIT" or "MARKET"
    BUY = "BUY"
    SELL = "SELL"

    def __init__(self, client_id, access_token, **kwargs):
        super().__init__(client_id, access_token, **kwargs)
        self.base_url = "https://ant.aliceblueonline.com/rest/AliceBlueAPIService/api"
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

    def place_order(self, tradingsymbol, exchange="NSE", transaction_type="BUY",
                    quantity=1, order_type="MARKET", product="MIS", price=0, **extra):
        payload = {
            "com_id": tradingsymbol,               # symbol e.g. "RELIANCE-EQ"
            "exch": exchange,                      # "NSE", "BSE", "MCX"
            "transtype": transaction_type,         # "BUY"/"SELL"
            "qty": int(quantity),
            "prctyp": order_type,                  # "MARKET"/"LIMIT"
            "prc": float(price) if price else 0,
            "prd": product                        # "MIS"/"CNC"
        }
        r = requests.post(f"{self.base_url}/placeOrder", json=payload, headers=self.headers, timeout=10)
        try:
            resp = r.json()
        except Exception:
            resp = {"status": "failure", "error": r.text}
        # Alice Blue returns {"stat": "Ok", "norenordno": "..."} on success
        if resp.get("stat", "").upper() == "OK":
            return {"status": "success", "order_id": resp.get("norenordno"), **resp}
        return {"status": "failure", **resp}

    def get_order_list(self):
        # Gets all orders for the day
        r = requests.get(f"{self.base_url}/orderBook", headers=self.headers, timeout=10)
        try:
            data = r.json()
        except Exception:
            return {"status": "failure", "error": r.text}
        if isinstance(data, dict) and "orders" in data:
            return {"status": "success", "orders": data["orders"]}
        # Sometimes it's a list
        if isinstance(data, list):
            return {"status": "success", "orders": data}
        return {"status": "failure", "error": data}

    def cancel_order(self, order_id):
        payload = {"norenordno": order_id}
        r = requests.post(f"{self.base_url}/cancelOrder", json=payload, headers=self.headers, timeout=10)
        try:
            resp = r.json()
        except Exception:
            resp = {"status": "failure", "error": r.text}
        if resp.get("stat", "").upper() == "OK":
            return {"status": "success", **resp}
        return {"status": "failure", **resp}

    def get_positions(self):
        r = requests.get(f"{self.base_url}/positionBook", headers=self.headers, timeout=10)
        try:
            data = r.json()
        except Exception:
            return {"status": "failure", "error": r.text}
        if isinstance(data, list):
            return {"status": "success", "positions": data}
        return {"status": "failure", "error": data}

    # ... Implement more endpoints as needed ...

