from dhanhq import dhanhq

class DhanApi:
    def __init__(self, client_id, access_token, **kwargs):
        self.client_id = client_id
        self.access_token = access_token
        self.api = dhanhq(client_id, access_token)

    def get_order_list(self):
        try:
            # DhanHQ returns a dict with "data"
            orders = self.api.get_order_list()
            return orders  # Already a dict with "data"
        except Exception as e:
            return {"data": [], "error": str(e)}

    def get_positions(self):
        try:
            positions = self.api.get_positions()
            return positions  # Already a dict with "data"
        except Exception as e:
            return {"data": [], "error": str(e)}

    def place_order(self, tradingsymbol, exchange, transaction_type, quantity, order_type, product, price=None):
        try:
            params = {
                "security_id": tradingsymbol,         # In your backend, tradingsymbol == security_id
                "exchange_segment": exchange,         # Example: 'NSE'
                "transaction_type": transaction_type, # 'BUY' or 'SELL'
                "quantity": int(quantity),
                "order_type": order_type,             # 'MARKET' or 'LIMIT'
                "product_type": product,              # 'INTRADAY' or 'CNC'
            }
            if price is not None and order_type == "LIMIT":
                params["price"] = float(price)
            resp = self.api.place_order(**params)
            return resp  # Should include order id etc.
        except Exception as e:
            return {"status": "failure", "error": str(e)}
