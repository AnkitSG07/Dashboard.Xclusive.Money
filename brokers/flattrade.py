import requests
import time
from .base import BrokerBase

class FlattradeBroker(BrokerBase):
    BASE_URL = "https://apigw.flattrade.in/trade"
    CLIENT_CODE = None
    ACCESS_TOKEN = None
    SESSION_TOKEN = None

    def __init__(self, client_id, access_token=None, api_key=None, api_secret=None, password=None, totp_secret=None, **kwargs):
        """
        Initialize the Flattrade broker adapter.
        """
        super().__init__(client_id, access_token, **kwargs)
        self.client_id = client_id
        self.api_key = api_key
        self.api_secret = api_secret
        self.password = password
        self.totp_secret = totp_secret
        self.access_token = access_token
        self.vendor_code = kwargs.get("vendor_code", None)  # Optional if needed
        self.session_expiry = 0

        if not self.access_token:
            self.login()
        else:
            FlattradeBroker.ACCESS_TOKEN = self.access_token
            FlattradeBroker.CLIENT_CODE = self.client_id

    def login(self):
        """
        Login to Flattrade and set access_token/session_token.
        """
        url = f"{self.BASE_URL}/auth/login"
        payload = {
            "userId": self.client_id,
            "password": self.password,
            "factor2": self.totp_secret,       # TOTP or 2FA code
            "vendorCode": self.vendor_code or "YOUR_VENDOR_CODE",  # Put your vendor code if required
            "apiKey": self.api_key,
            "imei": "1234567890",    # As per API, any random string is accepted
        }
        resp = requests.post(url, json=payload)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "Success":
            raise Exception("Flattrade login failed: " + str(data))
        self.access_token = data["susertoken"]
        self.session_expiry = time.time() + 6.5 * 3600   # 6.5 hour session
        FlattradeBroker.ACCESS_TOKEN = self.access_token
        FlattradeBroker.CLIENT_CODE = self.client_id

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

    def _check_session(self):
        if not self.access_token or time.time() > self.session_expiry:
            self.login()

    def get_order_list(self):
        """
        Get order book.
        """
        self._check_session()
        url = f"{self.BASE_URL}/orders"
        resp = requests.get(url, headers=self._headers())
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") == "Success":
            return {"status": "success", "data": data.get("result", [])}
        return {"status": "failure", "error": data.get("emsg") or data.get("result")}

    def place_order(
        self,
        tradingsymbol=None,
        exchange=None,
        transaction_type=None,
        quantity=None,
        order_type=None,
        product=None,
        price=None,
        **kwargs,
    ):
        """Place a regular order (NRML/INTRADAY)."""
        self._check_session()
        tradingsymbol = tradingsymbol or kwargs.pop("symbol", None)
        transaction_type = transaction_type or kwargs.pop("action", None)
        quantity = quantity or kwargs.pop("qty", None)
        product = product or kwargs.pop("product_type", None)
        exchange = exchange or kwargs.pop("exchange", None)
        if isinstance(transaction_type, str):
            transaction_type = transaction_type.upper()
        if isinstance(order_type, str):
            order_type = order_type.upper()
        if isinstance(product, str):
            product = product.upper()
        url = f"{self.BASE_URL}/orders"
        payload = {
            "exchange": exchange,
            "tradingsymbol": tradingsymbol,
            "transactiontype": transaction_type,
            "quantity": int(quantity),
            "ordertype": order_type,
            "producttype": product,
            "price": float(price) if price else 0,
            "duration": "DAY",
            "triggerprice": 0,
            "ret": "DAY",
        }
        # Remove unnecessary keys if not required
        for k in ["disclosedquantity", "triggerprice"]:
            if k in payload and not payload[k]:
                payload.pop(k)
        resp = requests.post(url, json=payload, headers=self._headers())
        try:
            resp.raise_for_status()
        except Exception:
            # Flattrade always returns 200 even for error, so ignore for now
            pass
        result = resp.json()
        # Example success: {"status":"Success","result":"Order Placed Successfully","orderid":"23062600010479"}
        if result.get("status") == "Success":
            return {"status": "success", "order_id": result.get("orderid"), "remarks": result.get("result")}
        else:
            return {"status": "failure", "error": result.get("emsg") or result.get("result"), "remarks": result.get("result")}

    def cancel_order(self, order_id):
        """
        Cancel an order.
        """
        self._check_session()
        url = f"{self.BASE_URL}/orders/cancel"
        payload = {
            "orderid": order_id,
            "userId": self.client_id
        }
        resp = requests.post(url, json=payload, headers=self._headers())
        resp.raise_for_status()
        result = resp.json()
        # Example: {"status":"Success","result":"Order Cancelled Successfully"}
        if result.get("status") == "Success":
            return {"status": "success", "remarks": result.get("result")}
        else:
            return {"status": "failure", "error": result.get("emsg") or result.get("result"), "remarks": result.get("result")}

    def get_positions(self):
        """
        Get position book.
        """
        self._check_session()
        url = f"{self.BASE_URL}/positions"
        resp = requests.get(url, headers=self._headers())
        resp.raise_for_status()
        data = resp.json()
        return {"data": data.get("result", [])}

    def get_holdings(self):
        """
        Get holding book.
        """
        self._check_session()
        url = f"{self.BASE_URL}/holdings"
        resp = requests.get(url, headers=self._headers())
        resp.raise_for_status()
        data = resp.json()
        return {"data": data.get("result", [])}

    # Utility constants to match your backend style (like in dhan/zerodha adapters)
    NSE = "NSE"
    BSE = "BSE"
    MCX = "MCX"
    BUY = "BUY"
    SELL = "SELL"
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    INTRA = "INTRADAY"
    CNC = "DELIVERY"
    DAY = "DAY"

# Required for your broker factory logic
def Flattrade(client_id, access_token=None, **kwargs):
    return FlattradeBroker(client_id, access_token=access_token, **kwargs)

def get_opening_balance(self):
    """Return cash balance using limits API if available."""
    self._check_session()
    try:
        url = f"{self.BASE_URL}/limits"
        resp = requests.get(url, headers=self._headers())
        resp.raise_for_status()
        data = resp.json()
        for key in ["cash", "available_balance", "opening_balance"]:
            if key in data:
                return float(data[key])
        if "result" in data and isinstance(data["result"], dict):
            for key in ["cash", "available_balance", "opening_balance"]:
                if key in data["result"]:
                    return float(data["result"][key])
        return None
    except Exception:
        return None
