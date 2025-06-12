import requests
import pyotp
from .base import BrokerBase

class AliceBlueBroker(BrokerBase):
    BASE_URL = "https://ant.aliceblueonline.com/rest/AliceBlueAPIService/api/"

    def __init__(self, client_id, password, totp_secret, **kwargs):
        """Initialize the broker and perform login."""
        # Base class expects an access token. We don't have one yet so pass "".
        super().__init__(client_id, "", **kwargs)
        # Store password for the login call
        self.password = password
        self.totp_secret = totp_secret
        self.session_id = None
        self.headers = None
        self.login()  # Auto-login on creation

    def get_totp(self):
        return pyotp.TOTP(self.totp_secret).now()

    def login(self):
        totp = self.get_totp()
        url = self.BASE_URL + "customer/login"
        data = {
            "userId": self.client_id,
            "userData": self.password,
            "totp": totp
        }
        r = requests.post(url, json=data, timeout=10)
        resp = r.json()
        if resp.get("stat") != "Ok":
            raise Exception(f"AliceBlue login failed: {resp.get('emsg')}")
        self.session_id = resp["sessionID"]
        # Set Authorization header for future API calls
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.client_id} {self.session_id}"
        }

    def ensure_session(self):
        """Ensure we are logged in, otherwise re-login (can add checks later)"""
        if not self.session_id or not self.headers:
            self.login()

    def place_order(self, tradingsymbol, exchange="NSE", transaction_type="BUY", quantity=1,
                   order_type="MARKET", product="MIS", price=0, **kwargs):
        self.ensure_session()
        url = self.BASE_URL + "placeOrder"
        # Alice Blue expects "action" as "BUY" or "SELL"
        payload = {
            "exchange": exchange,
            "symbol": tradingsymbol,
            "transaction_type": transaction_type,  # "BUY" or "SELL"
            "quantity": int(quantity),
            "order_type": order_type,              # "MARKET" or "LIMIT"
            "product_type": product,               # "MIS" or "CNC"
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
        # Orders are in 'data' or list directly
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
        # Positions are in 'data' or list directly
        positions = resp.get("data") or resp.get("positions", []) or []
        return {"status": "success", "positions": positions}

    def get_opening_balance(self):
        """Return available cash balance if API provides it."""
        self.ensure_session()
        try:
            url = self.BASE_URL + "balance"
            r = requests.get(url, headers=self.headers, timeout=10)
            data = r.json()
            for key in ["available_balance", "cash", "opening_balance"]:
                if key in data:
                    return float(data[key])
            return None
        except Exception:
            return None


    # ... Add more methods if needed (holdings, funds etc.)
