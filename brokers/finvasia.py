import pyotp

# Shoonya API wrapper import varies by package version
try:
    from shoonya import ShoonyaApiPy  # type: ignore
except Exception:  # pragma: no cover - fallback for older packages
    try:
        from shoonya.apis import ShoonyaApiPy  # type: ignore
    except Exception:
        from shoonya import Shoonya as ShoonyaApiPy  # type: ignore
from .base import BrokerBase

class FinvasiaBroker(BrokerBase):
    def __init__(self, client_id, password=None, totp_secret=None, vendor_code=None, api_key=None, imei="abc1234", **kwargs):
        super().__init__(client_id, "", **kwargs)
        self.password = password
        self.totp_secret = totp_secret
        self.vendor_code = vendor_code
        self.api_key = api_key
        self.imei = imei
        self.api = ShoonyaApiPy()
        self.session = None
        self._last_auth_error = None
        if all([password, totp_secret, vendor_code, api_key]):
            self.login()

    def login(self):
        """Authenticate to Shoonya using the stored credentials."""
        totp = pyotp.TOTP(self.totp_secret).now()
        ret = self.api.login(
            userid=self.client_id,
            password=self.password,
            twoFA=totp,
            vendor_code=self.vendor_code,
            api_secret=self.api_key,
            imei=self.imei,
        )
        if not ret or ret.get("stat") != "Ok":
            msg = ret.get("emsg", "Finvasia login failed") if isinstance(ret, dict) else "Finvasia login failed"
            if "expir" in msg.lower():
                self._last_auth_error = "API key expired. Please regenerate API key from Prism and retry."
            else:
                self._last_auth_error = "Login failed. Please check your password, TOTP, API key, or vendor code."
            raise Exception(self._last_auth_error)
        self.session = ret
        self._last_auth_error = None


    def check_token_valid(self):
        try:
            resp = self.api.get_limits()
            if resp.get("stat") == "Ok":
                self._last_auth_error = None
                return True
            self._last_auth_error = resp.get("emsg") or resp.get("stat")
            return False
        except Exception as e:
            self._last_auth_error = str(e)
            return False

    def get_opening_balance(self):
        data = self.api.get_limits()
        return data.get("cash")

    def place_order(self, tradingsymbol, exchange, transaction_type, quantity, order_type="MKT", product="C", price=0, **kwargs):
        return self.api.place_order(
            buy_or_sell="B" if transaction_type.upper() == "BUY" else "S",
            product_type=product,
            exchange=exchange,
            tradingsymbol=tradingsymbol,
            quantity=int(quantity),
            discloseqty=0,
            price_type=order_type,
            price=float(price),
            trigger_price=kwargs.get("trigger_price"),
            retention="DAY",
            amo="NO",
            remarks=kwargs.get("remarks"),
        )
        
    def get_order_list(self):
        return self.api.get_order_book()

    def cancel_order(self, order_id):
        return self.api.cancel_order(orderno=order_id)

    def get_positions(self):
        return self.api.get_positions()

    def last_auth_error(self):
        return self._last_auth_error
