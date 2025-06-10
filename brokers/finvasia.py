from .base import BrokerBase

try:
    from SmartApi import SmartConnect
except ImportError:
    SmartConnect = None


class FinvasiaBroker(BrokerBase):
    def __init__(self, client_id, password=None, vendor_code=None, api_key=None,
                 imei=None, totp_key=None, access_token=None, **kwargs):
        super().__init__(client_id, access_token, **kwargs)
        if SmartConnect is None:
            raise ImportError("SmartApi not installed")

        self.api_key = api_key
        self.api = SmartConnect(api_key=api_key)

        # Case 1: Use access token if provided
        if access_token:
            self.api.setAccessToken(access_token)
            self.access_token = access_token
        # Case 2: Perform full login with password, TOTP, etc.
        elif all([password, vendor_code, totp_key]):
            session = self.api.generateSession(
                client_id=client_id,
                password=password,
                totp=totp_key,
                vendor_code=vendor_code,
                imei=imei or "web-client-01"
            )
            self.access_token = session["data"]["access_token"]
        else:
            raise ValueError("Insufficient login credentials provided for Finvasia.")

    def place_order(self, tradingsymbol, exchange, transaction_type, quantity,
                    order_type="MARKET", product="INTRADAY", price=None):
        try:
            data = {
                "variety": "NORMAL",
                "tradingsymbol": tradingsymbol,
                "symboltoken": "",  # You will need to handle token lookup
                "transactiontype": transaction_type.upper(),
                "exchange": exchange.upper(),
                "ordertype": order_type.upper(),
                "producttype": product.upper(),
                "duration": "DAY",
                "price": float(price) if price else 0,
                "quantity": int(quantity)
            }
            result = self.api.placeOrder(orderparams=data)
            return {"status": "success", "order_id": result["data"]["orderid"]}
        except Exception as e:
            return {"status": "failure", "error": str(e)}

    def get_order_list(self):
        try:
            orders = self.api.orderBook()
            return {"status": "success", "data": orders["data"]}
        except Exception as e:
            return {"status": "failure", "error": str(e), "data": []}

    def get_positions(self):
        try:
            positions = self.api.position()
            return {"status": "success", "data": positions["data"]}
        except Exception as e:
            return {"status": "failure", "error": str(e), "data": []}

    def cancel_order(self, order_id):
        try:
            result = self.api.cancelOrder(order_id=order_id, variety="NORMAL")
            return {"status": "success", "order_id": order_id}
        except Exception as e:
            return {"status": "failure", "error": str(e)}

    def get_profile(self):
        return {"status": "success", "data": {"client_id": self.client_id}}

    def check_token_valid(self):
        try:
            self.api.orderBook()
            return True
        except Exception:
            return False

    def get_opening_balance(self):
        try:
            data = self.api.rmsLimit()
            info = data.get("data", data)
            for key in ["net", "availablecash", "available_margin", "cash"]:
                if key in info:
                    return float(info[key])
            return None
        except Exception:
            return None
