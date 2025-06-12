import os
import requests
import hashlib
from .base import BrokerBase

ALICEBLUE_API_KEY = os.environ.get("ALICEBLUE_API_KEY")

class AliceBlueBroker(BrokerBase):
    BASE_URL = "https://ant.aliceblueonline.com/rest/AliceBlueAPIService/api"

    def __init__(self, client_id, **kwargs):
        super().__init__(client_id, None, **kwargs)
        self.api_key = ALICEBLUE_API_KEY
        self.session_id = None
        self.headers = None
        self.login()

    def get_encryption_key(self):
        url = f"{self.BASE_URL}/customer/getAPIEncpkey"
        payload = {"userId": self.client_id}
        r = requests.post(url, json=payload, timeout=15)
        try:
            resp = r.json()
        except Exception:
            raise Exception(f"Failed to get encryption key: {r.text}")
        if resp.get("stat") != "Ok":
            raise Exception(f"Failed to get encryption key: {resp.get('emsg') or resp}")
        return resp["encKey"]

    def login(self):
        # Step 1: Get encryption key
        encKey = self.get_encryption_key()

        # Step 2: SHA-256(userId + apiKey + encKey)
        concat = self.client_id + self.api_key + encKey
        userData = hashlib.sha256(concat.encode()).hexdigest()

        # Step 3: Get session ID
        url = f"{self.BASE_URL}/customer/getUserSID"
        payload = {"userId": self.client_id, "userData": userData}
        r = requests.post(url, json=payload, timeout=15)
        try:
            resp = r.json()
        except Exception:
            raise Exception(f"AliceBlue login failed: {r.text}")
        if resp.get("stat") != "Ok":
            raise Exception(f"AliceBlue login failed: {resp.get('emsg') or resp}")
        self.session_id = resp["sessionID"]
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.client_id} {self.session_id}"
        }

    def ensure_session(self):
        if not self.session_id or not self.headers:
            self.login()

    def safe_json(self, r):
        try:
            return r.json()
        except Exception:
            return {"status": "failure", "error": r.text}

    def place_order(self, tradingsymbol, exchange="NSE", transaction_type="BUY", quantity=1,
                   order_type="MARKET", product="MIS", price=0, **kwargs):
        self.ensure_session()
        url = f"{self.BASE_URL}/placeOrder"
        payload = {
            "exchange": exchange,
            "symbol": tradingsymbol,
            "transaction_type": transaction_type,
            "quantity": int(quantity),
            "order_type": order_type,
            "product_type": product,
            "price": float(price) if price else 0,
            "trigger_price": 0,
            "disclosed_quantity": 0,
            "validity": "DAY"
        }
        r = requests.post(url, json=payload, headers=self.headers, timeout=10)
        resp = self.safe_json(r)
        if resp.get("stat") == "Ok" or "NOrdNo" in resp:
            return {"status": "success", "order_id": resp.get("NOrdNo"), **resp}
        return {"status": "failure", **resp}

    def get_order_list(self):
        self.ensure_session()
        url = f"{self.BASE_URL}/orderBook"
        r = requests.get(url, headers=self.headers, timeout=10)
        resp = self.safe_json(r)
        orders = resp.get("data") or resp.get("OrderBookDetail", []) or []
        return {"status": "success", "orders": orders}

    def cancel_order(self, order_id):
        self.ensure_session()
        url = f"{self.BASE_URL}/cancelOrder"
        payload = {"NOrdNo": order_id}
        r = requests.post(url, json=payload, headers=self.headers, timeout=10)
        resp = self.safe_json(r)
        if resp.get("stat") == "Ok":
            return {"status": "success", **resp}
        return {"status": "failure", **resp}

    def get_positions(self):
        self.ensure_session()
        url = f"{self.BASE_URL}/positions"
        r = requests.get(url, headers=self.headers, timeout=10)
        resp = self.safe_json(r)
        positions = resp.get("data") or resp.get("positions", []) or []
        return {"status": "success", "positions": positions}

    def get_opening_balance(self):
        self.ensure_session()
        url = f"{self.BASE_URL}/balance"
        try:
            r = requests.get(url, headers=self.headers, timeout=10)
            data = self.safe_json(r)
            for key in ["available_balance", "cash", "opening_balance"]:
                if key in data:
                    return float(data[key])
            return None
        except Exception:
            return None
    def check_token_valid(self):
        # Always valid if instantiated
        return True
