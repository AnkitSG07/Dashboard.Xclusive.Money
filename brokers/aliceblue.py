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
    url = self.BASE_URL + "customer/getAPIEncpkey"
    print(f"Requesting encKey for userId={self.client_id}")
    resp = requests.post(url, json={"userId": self.client_id}, timeout=10)
    print(f"getAPIEncpkey response: {resp.text}")
    resp = resp.json()
    if resp.get("stat") != "Ok":
        raise Exception(f"AliceBlue getAPIEncpkey failed: {resp.get('emsg')}")
    enc_key = resp["encKey"]
    print("enc_key:", repr(enc_key))
    concat_string = f"{self.client_id}{self.api_key}{enc_key}"
    print("concat_string:", repr(concat_string))
    user_data = hashlib.sha256(concat_string.encode()).hexdigest()
    print("user_data hash:", user_data)
    url = self.BASE_URL + "customer/getUserSID"
    print(f"Requesting SID with userData={user_data}")
    resp = requests.post(url, json={"userId": self.client_id, "userData": user_data}, timeout=10)
    print(f"getUserSID response: {resp.text}")
    resp = resp.json()
    if resp.get("stat") != "Ok":
        raise Exception(f"AliceBlue getUserSID failed: {resp.get('emsg')}")
    self.session_id = resp["sessionID"]
    self.headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {self.client_id} {self.session_id}"
    }

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
