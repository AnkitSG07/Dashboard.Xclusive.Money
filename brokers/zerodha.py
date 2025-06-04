from kiteconnect import KiteConnect
import pyotp
import requests
from .base import BrokerBase

class ZerodhaBroker(BrokerBase):
    def __init__(self, client_id, api_key, api_secret, password=None, totp_key=None, **kwargs):
        super().__init__(client_id, api_key, **kwargs)
        self.client_id = client_id
        self.api_key = api_key
        self.api_secret = api_secret
        self.password = password
        self.totp_key = totp_key
        self.access_token = None
        self.kite = KiteConnect(api_key=self.api_key)

        # If you already have a valid access_token, load it here
        # If not, do TOTP login
        self._auto_login()

    def _auto_login(self):
        """
        Use TOTP + password to login and obtain request_token automatically.
        Exchange for access_token.
        """
        try:
            # 1. Generate TOTP
            totp = pyotp.TOTP(self.totp_key).now() if self.totp_key else None

            # 2. Start a session and do POST to Zerodha's login
            s = requests.Session()
            login_url = "https://kite.zerodha.com/api/login"
            resp = s.post(login_url, data={"user_id": self.client_id, "password": self.password})
            if resp.status_code != 200 or not resp.json().get("data"):
                raise Exception(f"Login failed: {resp.text}")
            # 3. Authenticate with TOTP (2FA)
            twofa_url = "https://kite.zerodha.com/api/twofa"
            resp2 = s.post(twofa_url, data={"user_id": self.client_id, "request_id": resp.json()["data"]["request_id"], "twofa_value": totp})
            if resp2.status_code != 200 or not resp2.json().get("data"):
                raise Exception(f"TOTP 2FA failed: {resp2.text}")

            # 4. Get request_token by following the redirect after 2FA
            # (Skip this if you handle this on frontend, or use kiteconnect's web flow)
            # Otherwise, use KiteConnect's manual method if you have the request_token:
            raise Exception("Please use kiteconnect's web login flow to obtain request_token, or automate with Selenium/Playwright.")
        except Exception as e:
            # In real prod, you'd store and refresh access_token via web flow.
            # For now, expect manual request_token via web flow.
            pass

    def set_access_token(self, request_token):
        """
        Once you have the request_token (from web flow, or headless), exchange for access_token.
        """
        data = self.kite.generate_session(request_token, api_secret=self.api_secret)
        self.access_token = data["access_token"]
        self.kite.set_access_token(self.access_token)

    def place_order(self, tradingsymbol, exchange, transaction_type, quantity, order_type, product, price=None, **kwargs):
        """
        Place order using access_token
        """
        if not self.access_token:
            raise Exception("No access_token, please login.")
        try:
            order_id = self.kite.place_order(
                variety="regular",
                exchange=exchange,
                tradingsymbol=tradingsymbol,
                transaction_type=transaction_type,
                quantity=int(quantity),
                order_type=order_type,
                product=product,
                price=float(price) if price else None,
                validity="DAY"
            )
            return {"status": "success", "order_id": order_id}
        except Exception as e:
            return {"status": "failure", "error": str(e)}

    def get_order_list(self):
        """
        Fetch orders using access_token
        """
        if not self.access_token:
            raise Exception("No access_token, please login.")
        try:
            orders = self.kite.orders()
            return {"status": "success", "data": orders}
        except Exception as e:
            return {"status": "failure", "error": str(e)}

    def cancel_order(self, order_id):
        """
        Cancel a regular order
        """
        if not self.access_token:
            raise Exception("No access_token, please login.")
        try:
            result = self.kite.cancel_order(variety="regular", order_id=order_id)
            return {"status": "success", "result": result}
        except Exception as e:
            return {"status": "failure", "error": str(e)}
