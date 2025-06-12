import os
import requests
import hashlib
from .base import BrokerBase

class AliceBlueBroker(BrokerBase):
    BASE_URL = "https://ant.aliceblueonline.com/rest/AliceBlueAPIService"

    def __init__(self, client_id, **kwargs):
        super().__init__(client_id, None, **kwargs)
        self.client_id = client_id
        self.api_key = os.environ.get("ALICEBLUE_API_KEY")  # Set your App ID in env
        if not self.api_key:
            raise Exception("ALICEBLUE_API_KEY not set in environment")
        self.session_id = None
        self.headers = None
        self.login()

    def get_encryption_key(self):
        url = f"{self.BASE_URL}/api/customer/getAPIEncpkey"
        payload = {"userId": self.client_id}
        r = requests.post(url, json=payload, timeout=10)
        resp = r.json()
        if resp.get("stat") != "Ok" or not resp.get("encKey"):
            emsg = resp.get("emsg") or "No encryption key returned"
            raise Exception(f"Failed to get encryption key: {emsg}")
        return resp["encKey"]

    def login(self):
        encKey = self.get_encryption_key()
        if not encKey:
            raise Exception("Encryption key unavailable")
        concat = self.client_id + self.api_key + encKey
        userData = hashlib.sha256(concat.encode()).hexdigest()
        url = f"{self.BASE_URL}/api/customer/getUserSID"
        payload = {"userId": self.client_id, "userData": userData}
        r = requests.post(url, json=payload, timeout=10)
        resp = r.json()
        if resp.get("stat") != "Ok":
            raise Exception(f"AliceBlue login failed: {resp.get('emsg')}")
        self.session_id = resp["sessionID"]
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.client_id} {self.session_id}"
        }

    def ensure_session(self):
        if not self.session_id or not self.headers:
            self.login()

    def place_order(
        self,
        tradingsymbol=None,
        security_id=None,
        exchange_segment="NSE",
        transaction_type="BUY",
        quantity=1,
        order_type="MKT",
        product_type="MIS",
        price=0,
        **kwargs,
    ):
        """Place an order using Alice Blue REST API."""
        self.ensure_session()
        if security_id is None:
            security_id = kwargs.get("symbol_id")

        order = {
            "discqty": "0",
            "trading_symbol": tradingsymbol,
            "exch": exchange_segment,
            "transtype": transaction_type,
            "ret": "DAY",
            "prctyp": order_type,
            "qty": str(quantity),
            "symbol_id": str(security_id) if security_id is not None else "",
            "price": str(price or 0),
            "trigPrice": str(kwargs.get("trigger_price", 0)),
            "pCode": product_type,
            "complexty": kwargs.get("complexty", "REGULAR"),
            "orderTag": kwargs.get("order_tag", ""),
            "deviceNumber": kwargs.get("device_number", "python-device"),
        }
        url = f"{self.BASE_URL}/api/placeOrder/executePlaceOrder"
        r = requests.post(url, json=[order], headers=self.headers, timeout=10)
        try:
            resp = r.json()
        except Exception:
            resp = {"status": "failure", "error": r.text}
        if resp.get("stat") == "Ok" or "NOrdNo" in resp:
            return {"status": "success", "order_id": resp.get("NOrdNo"), **resp}
        return {"status": "failure", **resp}

    def get_order_list(self):
        self.ensure_session()
        url = f"{self.BASE_URL}/api/positionAndHoldings/positionBook"
        r = requests.post(url, json={"ret": "DAY"}, headers=self.headers, timeout=10)
        try:
            resp = r.json()
        except Exception:
            return {"status": "failure", "error": r.text}
        orders = resp.get("data") or resp.get("OrderBookDetail", []) or []
        return {"status": "success", "orders": orders}

    def cancel_order(self, order_id):
        self.ensure_session()
        url = f"{self.BASE_URL}/api/cancelOrder"
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
        url = f"{self.BASE_URL}/api/positions"
        r = requests.get(url, headers=self.headers, timeout=10)
        try:
            resp = r.json()
        except Exception:
            return {"status": "failure", "error": r.text}
        positions = resp.get("data") or resp.get("positions", []) or []
        return {"status": "success", "positions": positions}

    def get_opening_balance(self):
        self.ensure_session()
        url = f"{self.BASE_URL}/api/balance"
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
        # Always valid if instantiated
        return True
