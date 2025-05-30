# brokers/zerodha.py

from .base import BrokerBase

try:
    from kiteconnect import KiteConnect
except ImportError:
    KiteConnect = None

class ZerodhaBroker(BrokerBase):
    def __init__(self, client_id, access_token, **kwargs):
        super().__init__(client_id, access_token, **kwargs)
        if KiteConnect is None:
            raise ImportError("kiteconnect library not installed.")
        # You might need to set your API key as well, e.g.:
        api_key = kwargs.get("api_key")  # Pass via kwargs when creating the instance
        if not api_key:
            raise ValueError("Zerodha requires 'api_key' as well.")
        self.kite = KiteConnect(api_key=api_key)
        self.kite.set_access_token(access_token)
        self.client_id = client_id

    def place_order(self, tradingsymbol, exchange, transaction_type, quantity, order_type="MARKET", product="MIS", price=None):
        """
        Place a Zerodha order.
        Args:
            tradingsymbol: e.g. 'RELIANCE'
            exchange: e.g. 'NSE'
            transaction_type: 'BUY' or 'SELL'
            quantity: integer
            order_type: 'MARKET', 'LIMIT', etc.
            product: 'MIS', 'CNC', etc.
            price: required if order_type is 'LIMIT'
        """
        params = {
            "tradingsymbol": tradingsymbol,
            "exchange": exchange,
            "transaction_type": transaction_type.upper(),
            "quantity": int(quantity),
            "order_type": order_type.upper(),
            "product": product.upper(),
        }
        if order_type.upper() == "LIMIT" and price:
            params["price"] = float(price)
        try:
            order_id = self.kite.place_order(
                variety=self.kite.VARIETY_REGULAR,
                **params
            )
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
            return {"status": "success", "data": positions}
        except Exception as e:
            return {"status": "failure", "error": str(e), "data": []}

    def cancel_order(self, order_id):
        try:
            self.kite.cancel_order(variety=self.kite.VARIETY_REGULAR, order_id=order_id)
            return {"status": "success", "order_id": order_id}
        except Exception as e:
            return {"status": "failure", "error": str(e)}

    # Add more helper methods as needed

