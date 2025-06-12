import requests
import pyotp
from .base import BrokerBase

class AliceBlueBroker(BrokerBase):
    BASE_URL = "https://ant.aliceblueonline.com/rest/AliceBlueAPIService/api/"

    def __init__(self, client_id, password, totp_secret, app_id, api_secret, **kwargs):
        super().__init__(client_id, password, **kwargs)
        self.password = password
        self.totp_secret = totp_secret
        self.app_id = app_id
        self.api_secret = api_secret
        self.session_id = None
        self.headers = None
        self.login()  # Auto-login on creation

    def get_totp(self):
        return pyotp.TOTP(self.totp_secret).now()

    def login(self):
        totp = self.get_totp()
        url = self.BASE_URL + "customerLogin"  # Correct endpoint (no slash)
        data = {
            "userId": self.client_id,
            "userData": self.password,
            "totp": totp,
            "appId": self.app_id,
            "apiSecret": self.api_secret
        }
        try:
            r = requests.post(url, json=data, timeout=15)
            if r.status_code != 200:
                raise Exception(f"Login request failed: {r.status_code} {r.text}")
            resp = r.json()
        except Exception as e:
            raise Exception(f"AliceBlue login failed: {e}")
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
        resp = self.safe_json(r)
        if resp.get("stat") == "Ok" or "NOrdNo" in resp:
            return {"status": "success", "order_id": resp.get("NOrdNo"), **resp}
        return {"status": "failure", **resp}

    def get_order_list(self):
        self.ensure_session()
        url = self.BASE_URL + "orderBook"
        r = requests.get(url, headers=self.headers, timeout=10)
        resp = self.safe_json(r)
        orders = resp.get("data") or resp.get("OrderBookDetail", []) or []
        return {"status": "success", "orders": orders}

    def cancel_order(self, order_id):
        self.ensure_session()
        url = self.BASE_URL + "cancelOrder"
        payload = {"NOrdNo": order_id}
        r = requests.post(url, json=payload, headers=self.headers, timeout=10)
        resp = self.safe_json(r)
        if resp.get("stat") == "Ok":
            return {"status": "success", **resp}
        return {"status": "failure", **resp}

    def get_positions(self):
        self.ensure_session()
        url = self.BASE_URL + "positions"
        r = requests.get(url, headers=self.headers, timeout=10)
        resp = self.safe_json(r)
        positions = resp.get("data") or resp.get("positions", []) or []
        return {"status": "success", "positions": positions}

    def get_opening_balance(self):
        self.ensure_session()
        url = self.BASE_URL + "balance"
        try:
            r = requests.get(url, headers=self.headers, timeout=10)
            data = self.safe_json(r)
            for key in ["available_balance", "cash", "opening_balance"]:
                if key in data:
                    return float(data[key])
            return None
        except Exception:
            return None
