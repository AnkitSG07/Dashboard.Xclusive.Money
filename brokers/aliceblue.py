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
        self.device_number = device_number
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

    def place_order(
        self,
        tradingsymbol,
        exchange="NSE",
        transaction_type="BUY",
        quantity=1,
        order_type="MKT",
        product="MIS",
        price=0,
        symbol_id="",
        deviceNumber=None,
        orderTag="order1",
        complexty="regular",
        disclosed_qty=0,
        retention="DAY",
        trigger_price=""
    ):
        """
        Place an order on Alice Blue using the official API contract.
    
        Args:
            tradingsymbol (str): Trading symbol, e.g., 'ASHOKLEY-EQ'
            exchange (str): Exchange, e.g., 'NSE'
            transaction_type (str): 'BUY' or 'SELL'
            quantity (int): Order quantity
            order_type (str): 'L', 'MKT', 'SL', 'SL-M'
            product (str): Product code, e.g., 'MIS', 'CNC', 'NRML', etc.
            price (float/int/str): Price as required by Alice Blue (string or float)
            symbol_id (str): Token/ID for the symbol
            deviceNumber (str): Device ID string (should be unique and persistent per account)
            orderTag (str): Order tag/remark
            complexty (str): 'regular', 'BO', 'CO', etc.
            disclosed_qty (int): Disclosed quantity
            retention (str): 'DAY' by default
            trigger_price (str/int): Trigger price for SL/SL-M
    
        Returns:
            dict: Result with status, order_id (if successful), or error message.
        """
        # Use stored device_number if not supplied
        if not deviceNumber and hasattr(self, "device_number"):
            deviceNumber = self.device_number
        elif not deviceNumber:
            deviceNumber = "device123"  # Fallback (should not happen in production)
    
        url = self.BASE_URL + "placeOrder/executePlaceOrder"
        payload = [{
            "complexty": complexty,
            "discqty": str(disclosed_qty),
            "exch": exchange,
            "pCode": product,
            "prctyp": order_type,
            "price": str(price) if price else "0",
            "qty": int(quantity),
            "ret": retention,
            "symbol_id": str(symbol_id),
            "trading_symbol": tradingsymbol,
            "transtype": transaction_type.upper(),
            "trigPrice": str(trigger_price) if trigger_price else "",
            "orderTag": orderTag,
            "deviceNumber": deviceNumber
        }]
        headers = {
            'Authorization': f'Bearer {self.client_id} {self.session_id}',
            'Content-Type': 'application/json'
        }
        try:
            r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=10)
            try:
                resp = r.json()
            except Exception:
                return {"status": "failure", "error": r.text}
            # If response is a list, extract the first dict
            if isinstance(resp, list):
                resp = resp[0] if resp else {}
            if resp.get("stat", "").lower() == "ok" and "nestOrderNumber" in resp:
                return {"status": "success", "order_id": resp["nestOrderNumber"], **resp}
            return {"status": "failure", **resp}
        except Exception as e:
            return {"status": "failure", "error": str(e)}
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
            # According to Alice Blue docs, GET is supported, but POST is safer if you ever add a body.
            # We'll use GET since the docs specify it and it works for most users.
            r = requests.get(url, headers=self.headers, timeout=10)
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
    
            # Success: stat == "Ok" and accountStatus == "Activated"
            if (
                r.status_code == 200
                and data.get("accountStatus", "").lower() == "activated"
            ):
                return True
    
            # Error: stat == "Not_Ok" or emsg present
            self._last_auth_error = (
                data.get("emsg")
                or data.get("stat")
                or data.get("accountStatus")
                or str(data)
            )
            return False
            
        except Exception as e:
            self._last_auth_error = str(e)
            return False
            
    def last_auth_error(self):
        return self._last_auth_error
