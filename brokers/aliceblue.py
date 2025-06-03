# brokers/aliceblue.py

import requests
import pyotp
from .base import BrokerBase

class AliceBlueBroker(BrokerBase):
    BASE_URL = "https://ant.aliceblueonline.com/rest/AliceBlueAPIService/api/"
    
    def __init__(self, client_id, password, totp_secret, **kwargs):
        super().__init__(client_id, password, totp_secret=totp_secret, **kwargs)
        self.client_id = client_id
        self.password = password
        self.totp_secret = totp_secret
        self.session_id = None

    def _get_totp(self):
        return pyotp.TOTP(self.totp_secret).now()

    def login(self):
        """Obtain session ID using client_id, password, and TOTP code."""
        url = self.BASE_URL + "customer/login"
        totp_code = self._get_totp()
        data = {
            "userId": self.client_id,
            "userData": self.password,
            "totp": totp_code
        }
        resp = requests.post(url, json=data, timeout=10)
        r = resp.json()
        if r.get("stat") != "Ok" or "sessionID" not in r:
            raise Exception(f"AliceBlue login failed: {r.get('emsg') or r}")
        self.session_id = r["sessionID"]
        return self.session_id

    def _headers(self):
        # Login if needed
        if not self.session_id:
            self.login()
        # The header format: Authorization: Bearer <userId> <sessionID>
        return {
            "Authorization": f"Bearer {self.client_id} {self.session_id}",
            "Content-Type": "application/json"
        }

    def place_order(self, tradingsymbol, exchange="NSE", transaction_type="BUY", quantity=1, order_type="MARKET", product="MIS", price=None, **kwargs):
        url = self.BASE_URL + "placeOrder"
        data = {
            "symbol": tradingsymbol,
            "exchange": exchange,
            "transaction_type": transaction_type,  # "BUY" or "SELL"
            "quantity": int(quantity),
            "order_type": order_type,              # "MARKET" or "LIMIT"
            "product_type": product,               # "MIS", "CNC", etc.
            "price": float(price) if price else 0
        }
        # AliceBlue API might expect slightly different keys—update if needed as per latest docs.
        resp = requests.post(url, json=data, headers=self._headers(), timeout=10)
        try:
            r = resp.json()
        except Exception:
            return {"status": "failure", "error": resp.text}
        if r.get("stat") == "Ok" and r.get("NOrdNo"):
            return {"status": "success", "order_id": r["NOrdNo"], **r}
        return {"status": "failure", **r}

    def get_order_list(self):
        url = self.BASE_URL + "orderBook"
        resp = requests.get(url, headers=self._headers(), timeout=10)
        try:
            data = resp.json()
        except Exception:
            return {"status": "failure", "error": resp.text}
        # AliceBlue returns list of orders in "orders" key or directly as list
        return {"status": "success", "orders": data.get("OrderBook", data)}

    def cancel_order(self, order_id):
        url = self.BASE_URL + "cancelOrder"
        data = {"NOrdNo": order_id}
        resp = requests.post(url, json=data, headers=self._headers(), timeout=10)
        try:
            r = resp.json()
        except Exception:
            return {"status": "failure", "error": resp.text}
        if r.get("stat") == "Ok":
            return {"status": "success", **r}
        return {"status": "failure", **r}

    def get_positions(self):
        url = self.BASE_URL + "positions"
        resp = requests.get(url, headers=self._headers(), timeout=10)
        try:
            data = resp.json()
        except Exception:
            return {"status": "failure", "error": resp.text}
        return {"status": "success", "positions": data.get("Positions", data)}

    def get_fund_limits(self):
        url = self.BASE_URL + "limits"
        resp = requests.get(url, headers=self._headers(), timeout=10)
        try:
            data = resp.json()
        except Exception:
            return {"status": "failure", "error": resp.text}
        return {"status": "success", "data": data}

# Factory support:
# In your accounts.json, store:
# {
#   "broker": "aliceblue",
#   "client_id": "AB12345",
#   "credentials": {
#       "password": "yourpassword",
#       "totp_secret": "32CHARS"
#   }
# }
# Factory: get_broker_class("aliceblue")(client_id, password, totp_secret)
