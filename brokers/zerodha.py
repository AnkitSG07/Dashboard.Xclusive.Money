# brokers/zerodha.py

from .base import BrokerBase

try:
    from kiteconnect import KiteConnect
except ImportError:
    KiteConnect = None

class ZerodhaBroker(BrokerBase):
    """
    Adapter for Zerodha KiteConnect.
    Usage:
        broker = ZerodhaBroker(
            client_id="userid", 
            access_token="token", 
            api_key="api_key"    # required
        )
    """
    def __init__(self, client_id, access_token, **kwargs):
        super().__init__(client_id, access_token, **kwargs)
        if KiteConnect is None:
            raise ImportError("kiteconnect library not installed.")
        api_key = kwargs.get("api_key")
        if not api_key:
            raise ValueError("Zerodha requires 'api_key' as well.")
        self.kite = KiteConnect(api_key=api_key)
        self.kite.set_access_token(access_token)
        self.client_id = client_id
        self.api_key = api_key
        self.access_token = access_token

    def place_order(
        self, tradingsymbol, exchange, transaction_type, quantity, 
        order_type="MARKET", product="MIS", price=None
    ):
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
        Returns:
            dict: {status: "success"/"failure", order_id/error}
        """
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
            order_id = self.kite.place_order(
                variety=self.kite.VARIETY_REGULAR,
                **params
            )
            return {"status": "success", "order_id": order_id}
        except Exception as e:
            return {"status": "failure", "error": str(e)}

    def get_order_list(self):
        """
        Returns:
            dict: {status: "success"/"failure", data: [orders]}
        """
        try:
            orders = self.kite.orders()
            return {"status": "success", "data": orders}
        except Exception as e:
            return {"status": "failure", "error": str(e), "data": []}

    def get_positions(self):
        """
        Returns:
            dict: {status: "success"/"failure", data: positions}
        """
        try:
            positions = self.kite.positions()
            return {"status": "success", "data": positions}
        except Exception as e:
            return {"status": "failure", "error": str(e), "data": []}

    def cancel_order(self, order_id):
        """
        Cancel a placed order.
        Returns:
            dict: {status: "success"/"failure", order_id/error}
        """
        try:
            self.kite.cancel_order(variety=self.kite.VARIETY_REGULAR, order_id=order_id)
            return {"status": "success", "order_id": order_id}
        except Exception as e:
            return {"status": "failure", "error": str(e)}

    def get_profile(self):
        """
        Fetch user profile (can be used for validation or UI).
        Returns:
            dict: {status: "success"/"failure", data: profile}
        """
        try:
            profile = self.kite.profile()
            return {"status": "success", "data": profile}
        except Exception as e:
            return {"status": "failure", "error": str(e), "data": None}

    def check_token_valid(self):
        """
        Optional: Check if access token is valid.
        Returns:
            bool
        """
        try:
            self.kite.profile()
            return True
        except Exception:
            return False
