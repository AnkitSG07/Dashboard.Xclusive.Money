import requests
import hashlib
import json
from .base import BrokerBase

class AliceBlueBroker(BrokerBase):
    BASE_URL = "https://ant.aliceblueonline.com/rest/AliceBlueAPIService/api/"

    def __init__(self, client_id, api_key, **kwargs):
        super().__init__(client_id, api_key, **kwargs)
        self.client_id = str(client_id).strip()
        self.api_key = str(api_key).strip()
        self.session_id = None
        self.headers = None
        self._last_auth_error = None
        self.authenticate()

    def authenticate(self):
        # Step 1: Get Encryption Key
        url = self.BASE_URL + "customer/getAPIEncpkey"
        payload = {"userId": self.client_id}
        headers = {'Content-Type': 'application/json'}
        resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=10)
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

        client_id = str(self.client_id).strip()
        api_key = str(self.api_key).strip()
        enc_key = str(enc_key).strip()  # from previous step
        
        print("client_id:", repr(client_id))
        print("api_key:", repr(api_key))
        print("enc_key:", repr(enc_key))
        
        to_hash = f"{client_id}{api_key}{enc_key}"
        print("concat string:", repr(to_hash))
        
        user_data = hashlib.sha256(to_hash.encode()).hexdigest()
        print("sha256:", user_data)
        
        payload = {
            "userId": client_id,
            "userData": user_data
        }
        print("payload:", json.dumps(payload))
        
        url = "https://ant.aliceblueonline.com/rest/AliceBlueAPIService/api/customer/getUserSID"
        headers = {'Content-Type': 'application/json'}
        resp = requests.post(url, headers=headers, data=json.dumps(payload))
        print(resp.text)

        # Step 3: Get Session ID
        url = self.BASE_URL + "customer/getUserSID"
        payload = {
            "userId": self.client_id,
            "userData": user_data
        }
        headers = {'Content-Type': 'application/json'}
        resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=10)
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

    def place_order(self, tradingsymbol, exchange="NSE", transaction_type="BUY", quantity=1, order_type="L", product="MIS", price=0, **kwargs):
        self.ensure_session()
        url = self.BASE_URL + "placeOrder/executePlaceOrder"
        payload = [{
            "discqty": "0",
            "trading_symbol": tradingsymbol,
            "exch": exchange,
            "transtype": transaction_type,
            "ret": "DAY",
            "prctyp": order_type,
            "qty": str(quantity),
            "price": str(price),
            "pCode": product,
            "symbol_id": kwargs.get("symbol_id", ""),
            "trigPrice": kwargs.get("trigPrice", "0.00"),
            "complexty": kwargs.get("complexty", "REGULAR"),
            "orderTag": kwargs.get("orderTag", ""),
            "deviceNumber": kwargs.get("deviceNumber", "default")
        }]
        r = requests.post(url, data=json.dumps(payload), headers=self.headers, timeout=10)
        try:
            resp = r.json()
        except Exception:
            resp = {"status": "failure", "error": r.text}
        if resp.get("stat") == "Ok" or "nestOrderNumber" in resp:
            return {"status": "success", "order_id": resp.get("nestOrderNumber"), **resp}
        return {"status": "failure", **resp}

    def get_order_list(self):
        self.ensure_session()
        url = self.BASE_URL + "placeOrder/fetchOrderBook"
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
        url = self.BASE_URL + "positionAndHoldings/positionBook"
        r = requests.get(url, headers=self.headers, timeout=10)
        try:
            resp = r.json()
        except Exception:
            return {"status": "failure", "error": r.text}
        positions = resp.get("data") or resp.get("positions", []) or []
        return {"status": "success", "positions": positions}

    def get_opening_balance(self):
        self.ensure_session()
        url = self.BASE_URL + "positionAndHoldings/holdings"
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
            url = self.BASE_URL + "customer/accountDetails"
            r = requests.post(url, headers=self.headers, timeout=10)
            content_type = r.headers.get("Content-Type", "").lower()
            data = None
            if "json" in content_type:
                try:
                    data = r.json()
                except Exception:
                    snippet = r.text[:100]
                    self._last_auth_error = f"HTTP {r.status_code}: {snippet}"
                    return False
            else:
                snippet = r.text[:100]
                self._last_auth_error = f"HTTP {r.status_code}: {snippet}"
                return False
            # The correct check for Alice Blue account detail success:
            if r.status_code == 200 and data.get("accountStatus", "").lower() == "activated":
                return True
            self._last_auth_error = (
                data.get("accountStatus")
                or data.get("emsg")
                or data.get("stat")
                or str(data)
            )
            return False
        except Exception as e:
            self._last_auth_error = str(e)
            return False
            
    def last_auth_error(self):
        return self._last_auth_error
