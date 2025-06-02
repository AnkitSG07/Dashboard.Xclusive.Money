from .base import BrokerBase

try:
    from kiteconnect import KiteConnect
except ImportError:
    KiteConnect = None

class ZerodhaBroker(BrokerBase):
    def __init__(self, client_id, access_token, api_key=None, **kwargs):
        super().__init__(client_id, access_token, **kwargs)
        if KiteConnect is None:
            raise ImportError("kiteconnect not installed")
        if not api_key:
            raise ValueError("api_key is required for Zerodha.")
        self.kite = KiteConnect(api_key=api_key)
        self.kite.set_access_token(access_token)

    def place_order(self, tradingsymbol, exchange, transaction_type, quantity, order_type="MARKET", product="MIS", price=None):
        params = {
            "tradingsymbol": tradingsymbol,
            "exchange": exchange,
            "transaction_type": transaction_type.upper(),
            "quantity": int(quantity),
            "order_type": order_type.upper(),
            "product": product.upper(),
        }
        if order_type.upper() == "LIMIT" and price is not None:
            params["price"] = float(price)
        try:
            order_id = self.kite.place_order(variety=self.kite.VARIETY_REGULAR, **params)
            return {"status": "success", "order_id": order_id}
        except Exception as e:
            return {"status": "failure", "error": str(e)}

    def get_order_list(self):
        try:
            orders = self.kite.orders()
            return {"status": "success", "data": orders}
        except Exception as e:
            return {"status": "failure", "error": str(e), "data": []}

    def get_positions(self):
        try:
            positions = self.kite.positions()
            return {"status": "success", "data": positions.get('net', [])}
        except Exception as e:
            return {"status": "failure", "error": str(e), "data": []}

    def cancel_order(self, order_id):
        try:
            self.kite.cancel_order(variety=self.kite.VARIETY_REGULAR, order_id=order_id)
            return {"status": "success", "order_id": order_id}
        except Exception as e:
            return {"status": "failure", "error": str(e)}

    def get_profile(self):
        try:
            profile = self.kite.profile()
            return {"status": "success", "data": profile}
        except Exception as e:
            return {"status": "failure", "error": str(e), "data": None}

    def check_token_valid(self):
        try:
            self.kite.profile()
            return True
        except Exception:
            return False
