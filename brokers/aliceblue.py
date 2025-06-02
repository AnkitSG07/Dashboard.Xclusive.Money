from .base import BrokerBase

try:
    from alice_blue import AliceBlue
except ImportError:
    AliceBlue = None

class AliceBlueBroker(BrokerBase):
    def __init__(self, client_id, access_token, api_key=None, user_id=None, **kwargs):
        super().__init__(client_id, access_token, **kwargs)
        if AliceBlue is None:
            raise ImportError("alice_blue not installed")
        self.api = AliceBlue(username=client_id, password=access_token, api_key=api_key)
        self.api.get_session_id() # login

    def place_order(self, tradingsymbol, exchange, transaction_type, quantity, order_type="MARKET", product="MIS", price=None):
        try:
            order_id = self.api.place_order(
                transaction_type=transaction_type.upper(),
                instrument=self.api.get_instrument_by_symbol(exchange.upper(), tradingsymbol.upper()),
                quantity=int(quantity),
                order_type=order_type.upper(),
                product_type=product.upper(),
                price=float(price) if price else 0.0
            )
            return {"status": "success", "order_id": order_id}
        except Exception as e:
            return {"status": "failure", "error": str(e)}

    def get_order_list(self):
        try:
            orders = self.api.get_order_history()
            return {"status": "success", "data": orders}
        except Exception as e:
            return {"status": "failure", "error": str(e), "data": []}

    def get_positions(self):
        try:
            positions = self.api.get_netwise_positions()
            return {"status": "success", "data": positions}
        except Exception as e:
            return {"status": "failure", "error": str(e), "data": []}

    def cancel_order(self, order_id):
        try:
            self.api.cancel_order(order_id=order_id)
            return {"status": "success", "order_id": order_id}
        except Exception as e:
            return {"status": "failure", "error": str(e)}

    def get_profile(self):
        return {"status": "success", "data": {"client_id": self.client_id}}

    def check_token_valid(self):
        try:
            self.api.get_profile()
            return True
        except Exception:
            return False

