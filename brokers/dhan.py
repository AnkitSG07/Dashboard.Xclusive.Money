from dhanhq import dhanhq

class DhanApi:
    def __init__(self, client_id, access_token, **kwargs):
        # These must be provided in your accounts.json as credentials for each account
        self.client_id = client_id
        self.access_token = access_token
        self.api = dhanhq.DhanHQ(client_id, access_token)

    def get_order_list(self):
        try:
            orders = self.api.get_orders()
            # The wrapper may return as a list. Make it a dict for compatibility.
            return {"data": orders}
        except Exception as e:
            return {"data": [], "error": str(e)}

    def place_order(self, tradingsymbol, exchange, transaction_type, quantity, order_type, product, price=None):
        try:
            # Map arguments to DhanHQ place_order params:
            params = {
                "security_id": tradingsymbol,
                "exchange_segment": exchange,
                "transaction_type": transaction_type,
                "quantity": int(quantity),
                "order_type": order_type,
                "product_type": product,
            }
            if price is not None:
                params["price"] = float(price)
            order_resp = self.api.place_order(**params)
            return order_resp
        except Exception as e:
            return {"status": "failure", "error": str(e)}
