import requests
import hashlib
from .base import BrokerBase

class AliceBlueBroker(BrokerBase):
    BASE_URL = "https://ant.aliceblueonline.com/rest/AliceBlueAPIService/api/"

    def __init__(self, client_id, api_key, **kwargs):
        super().__init__(client_id, api_key, **kwargs)
        self.client_id = client_id
        self.api_key = api_key
        self.session_id = None
        self.headers = None
        self.authenticate()

    def authenticate(self):
        # Step 1: Get Encryption Key
        url = self.BASE_URL + "customer/getAPIEncpkey"
        resp = requests.post(url, json={"userId": self.client_id}, timeout=10)
        try:
            data = resp.json()
        except Exception:
            raise Exception(f"AliceBlue getAPIEncpkey HTTP error: {resp.text}")
        if data.get("stat") != "Ok" or not data.get("encKey"):
            msg = data.get("emsg") or data.get("stat") or data
            self._last_auth_error = f"getAPIEncpkey failed: {msg}"
            raise Exception(f"AliceBlue getAPIEncpkey failed: {msg}")
        enc_key = data["encKey"]

        # Step 2: Generate SHA256(userId + apiKey + encKey)
        to_hash = f"{self.client_id}{self.api_key}{enc_key}"
        user_data = hashlib.sha256(to_hash.encode()).hexdigest()

        # Step 3: Get Session ID
        url = self.BASE_URL + "customer/getUserSID"
        resp = requests.post(url, json={"userId": self.client_id, "userData": user_data}, timeout=10)
        try:
            data = resp.json()
        except Exception:
            raise Exception(f"AliceBlue getUserSID HTTP error: {resp.text}")
        if data.get("stat") != "Ok" or not data.get("sessionID"):
            msg = data.get("emsg") or data.get("stat") or data
            self._last_auth_error = f"getUserSID failed: {msg}"
            raise Exception(f"AliceBlue getUserSID failed: {msg}")
        self.session_id = data["sessionID"]
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.client_id} {self.session_id}"
        }
        self._last_auth_error = None

    def ensure_session(self):
        if not self.session_id or not self.headers:
            self.authenticate()

    def place_order(self, tradingsymbol, exchange="NSE", transaction_type="BUY", quantity=1,
                   order_type="MARKET", product="MIS", price=0, **kwargs):
        self.ensure_session()
        url = self.BASE_URL + "placeOrder"
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
        try:
            resp = r.json()
        except Exception:
            resp = {"status": "failure", "error": r.text}
        if resp.get("stat") == "Ok" or "NOrdNo" in resp:
            return {"status": "success", "order_id": resp.get("NOrdNo"), **resp}
        return {"status": "failure", **resp}

    def get_order_list(self):
        self.ensure_session()
        url = self.BASE_URL + "orderBook"
        r = requests.get(url, headers=self.headers, timeout=10)
        try:
            resp = r.json()
        except Exception:
            return {"status": "failure", "error": r.text}
        orders = resp.get("data") or resp.get("OrderBookDetail", []) or []
        return {"status": "success", "orders": orders}

    def cancel_order(self, order_id):
        self.ensure_session()
        url = self.BASE_URL + "cancelOrder"
        payload = {"NOrdNo": order_id}
        r = requests.post(url, json=payload, headers=self.headers, timeout=10)
        try:
            resp = r.json()
        except Exception:
            return {"status": "failure", "error": r.text}
        if resp.get("stat") == "Ok":
            return {"status": "success", **resp}
        return {"status": "failure", **resp}

    def get_positions(self):
        self.ensure_session()
        url = self.BASE_URL + "positions"
        r = requests.get(url, headers=self.headers, timeout=10)
        try:
            resp = r.json()
        except Exception:
            return {"status": "failure", "error": r.text}
        positions = resp.get("data") or resp.get("positions", []) or []
        return {"status": "success", "positions": positions}

    def get_opening_balance(self):
        self.ensure_session()
        url = self.BASE_URL + "balance"
        try:
            r = requests.get(url, headers=self.headers, timeout=10)
            data = r.json()
            for key in ["available_balance", "cash", "opening_balance"]:
                if key in data:
                    return float(data[key])
            return None
        except Exception:
            return None

    def check_token_valid(self):
        try:
            self.ensure_session()
            url = self.BASE_URL + "profile"
            r = requests.get(url, headers=self.headers, timeout=10)
            return r.status_code == 200 and "stat" in r.json() and r.json().get("stat") == "Ok"
        except Exception:
            return False
