import pyotp
from api_helper import ShoonyaApiPy  # From ShoonyaApi-py cloned from GitHub (AnkitSG07/ShoonyaApi-py)
from .base import BrokerBase # Assuming BrokerBase is in the same package/module

class FinvasiaBroker(BrokerBase):
    """
    Adapter for Finvasia (Shoonya) API using ShoonyaApiPy from the correct GitHub repo.
    Required:
        - client_id, password, totp_secret, vendor_code, api_key, imei
    """
    def __init__(self, client_id, password=None, totp_secret=None, vendor_code=None, api_key=None, imei="abc1234", **kwargs):
        kwargs.pop("access_token", None) # Remove access_token as Finvasia doesn't use it directly
        super().__init__(client_id, "", **kwargs) # Pass an empty string for access_token as Finvasia uses susertoken internally
        self.password = password
        self.totp_secret = totp_secret
        self.vendor_code = vendor_code
        self.api_key = api_key
        self.imei = imei or "abc1234"
        self.api = ShoonyaApiPy()
        self.session = None
        self._last_auth_error = None
        
        # Initialize API with session details if provided in kwargs (e.g., for persistent session)
        if kwargs.get("api_session"):
            self.api.set_session(kwargs["api_session"])
            self.session = kwargs["api_session"]
            if not self.check_token_valid(): # Verify if the provided session token is still valid
                print("Provided API session token is invalid, attempting re-login.")
                if all([password, totp_secret, vendor_code, api_key]):
                    self.login()
                else:
                    raise Exception("Invalid session and insufficient credentials to re-login.")
        elif all([password, totp_secret, vendor_code, api_key]):
            self.login()
        else:
            raise Exception("Insufficient credentials for Finvasia broker initialization. "
                            "Please provide password, TOTP secret, vendor code, and API key or a valid api_session.")

    def login(self):
        totp = pyotp.TOTP(self.totp_secret).now()
        try:
            ret = self.api.login(
                userid=self.client_id,
                password=self.password,
                twoFA=totp,
                vendor_code=self.vendor_code,
                api_secret=self.api_key,
                imei=self.imei,
            )
        except Exception as e:
            # Catch network errors or exceptions from the ShoonyaApiPy login call
            self._last_auth_error = f"Network or API error during login: {e}"
            raise Exception(self._last_auth_error)

        if not isinstance(ret, dict) or ret.get("stat") != "Ok":
            msg = ret.get("emsg", "Finvasia login failed: Unknown error") if isinstance(ret, dict) else "Finvasia login failed: No valid response received."
            if "expir" in msg.lower() or "token invalid" in msg.lower():
                self._last_auth_error = "API key or session expired/invalid. Please regenerate API key from Prism or ensure credentials are correct."
            elif "not match" in msg.lower() or "incorrect" in msg.lower():
                 self._last_auth_error = "Login failed: Credentials (password, TOTP, API key, or vendor code) are incorrect."
            else:
                self._last_auth_error = f"Finvasia login failed: {msg}"
            raise Exception(self._last_auth_error)
        
        self.session = ret # Store the full session response
        self.api.set_session(ret) # Crucial: Set the session token in the API helper
        self._last_auth_error = None
        print(f"Successfully logged in to Finvasia for {self.client_id}.")

    def check_token_valid(self):
        """Checks if the current session token is valid by making a simple API call."""
        try:
            # Use get_limits which is a simple, common call to validate session
            resp = self.api.get_limits() 
            if resp and resp.get("stat") == "Ok":
                self._last_auth_error = None
                return True
            self._last_auth_error = resp.get("emsg", "Session invalid (no specific error message).") if resp else "No response from get_limits API."
            return False
        except Exception as e:
            self._last_auth_error = f"Error checking token validity: {e}"
            return False

    def get_opening_balance(self):
        """Returns the full limits data, as it contains more than just cash."""
        data = self.api.get_limits()
        if data and data.get("stat") == "Ok":
            return {"status": "success", **data}
        return {"status": "failure", "error": data.get("emsg", "Failed to retrieve limits.") if data else "No response from get_limits."}

    def _normalize_product(self, product: str) -> str:
        """Convert common product names to Finvasia codes."""
        if not product:
            return ""
        p = str(product).strip().upper()
        mapping = {
            "MIS": "M",
            "INTRADAY": "M",
            "CNC": "C",
            "DELIVERY": "C",
            "NRML": "H",
            "NORMAL": "H",
        }
        return mapping.get(p, p)

    def place_order(self, tradingsymbol, exchange, transaction_type, quantity, order_type="MKT", product="C", price=0, token=None, **kwargs):
        """
        Places an order with Finvasia.
        Args:
            tradingsymbol (str): The trading symbol (e.g., "IDEA").
            exchange (str): The exchange (e.g., "NSE").
            transaction_type (str): "BUY" or "SELL".
            quantity (int): Order quantity.
            order_type (str): "MKT", "LMT", "SL", "SL-M".
            product (str): "CNC", "MIS", "NRML" (will be normalized to "C", "M", "H").
            price (float): Price for LIMIT/SL orders. **Assumed to be in Rupees; converted to Paise.**
            token (str): Finvasia's unique instrument token (e.g., "926241" for IDEA-EQ). **REQUIRED for Finvasia.**
        """
        if token is None:
            return {"status": "failure", "error": "Finvasia requires 'token' for order placement."}

        product_code = self._normalize_product(product)
        
        # **CRITICAL: Convert price to paise**
        price_in_paise = int(float(price) * 100) if order_type.upper() in ["LMT", "SL"] and price is not None else 0
        
        trigger_price_in_paise = 0
        if kwargs.get("trigger_price") is not None:
            trigger_price_in_paise = int(float(kwargs["trigger_price"]) * 100)

        order_params = dict(
            buy_or_sell="B" if transaction_type.upper() == "BUY" else "S",
            product_type=product_code,
            exchange=exchange,
            tradingsymbol=tradingsymbol,
            quantity=int(quantity),
            discloseqty=0, # Typically 0 for most orders
            price_type=order_type,
            price=price_in_paise,
            trigger_price=trigger_price_in_paise,
            retention="DAY", # or "IOC"
            amo="NO", # After Market Order
            remarks=kwargs.get("remarks"),
            token=token # **Pass the Finvasia instrument token**
        )
        print("Finvasia place_order params:", order_params)

        try:
            resp = self.api.place_order(**order_params)
        except Exception as e:
            # Attempt re-login on session expiry and retry the order
            if "Session Expired" in str(e) or "Invalid session" in str(e) or "Login again" in str(e):
                print(f"Session expired or invalid. Attempting re-login for {self.client_id}...")
                try:
                    self.login()
                    print("Re-login successful. Retrying order.")
                    resp = self.api.place_order(**order_params) # Retry order after successful re-login
                except Exception as retry_e:
                    msg = f"Re-login failed or order retry failed after re-login: {retry_e}"
                    return {"status": "failure", "error": msg}
            else:
                msg = str(e) if str(e) else "Unknown error during place_order"
                return {"status": "failure", "error": msg}
        
        # Handle cases where ShoonyaApiPy might return None for failed calls
        if resp is None:
            return {"status": "failure", "error": "No response received from Finvasia API (place_order)."}

        if isinstance(resp, dict):
            if resp.get("stat") == "Ok":
                # Finvasia returns orderno on success
                return {"status": "success", "order_id": resp.get("norenordno"), **resp}
            return {"status": "failure", "error": resp.get("emsg", "Order placement failed with unknown error."), **resp}

        return {"status": "failure", "error": str(resp) if resp is not None else "Unknown response type from API."}

    def modify_order(self, order_id, exchange, tradingsymbol, new_quantity=None, new_price=None, new_order_type=None, new_trigger_price=None, token=None, **kwargs):
        """
        Modifies an existing order with Finvasia.
        Args:
            order_id (str): The order number (norenordno) of the order to modify.
            exchange (str): The exchange.
            tradingsymbol (str): The trading symbol.
            new_quantity (int, optional): New quantity.
            new_price (float, optional): New price for LIMIT/SL orders. **Assumed to be in Rupees; converted to Paise.**
            new_order_type (str, optional): New order type ("MKT", "LMT", "SL", "SL-M").
            new_trigger_price (float, optional): New trigger price. **Assumed to be in Rupees; converted to Paise.**
            token (str): Finvasia's unique instrument token. **REQUIRED.**
        """
        if token is None:
            return {"status": "failure", "error": "Finvasia requires 'token' for order modification."}

        modify_params = {
            "orderno": order_id,
            "exchange": exchange,
            "tradingsymbol": tradingsymbol,
            "token": token, # **Pass the Finvasia instrument token**
        }
        if new_quantity is not None:
            modify_params["newquantity"] = int(new_quantity)
        if new_price is not None:
            # **CRITICAL: Convert price to paise for modify**
            modify_params["newprice"] = int(float(new_price) * 100)
        if new_order_type is not None:
            modify_params["newprice_type"] = new_order_type
        if new_trigger_price is not None:
            # **CRITICAL: Convert trigger price to paise for modify**
            modify_params["newtriggerprice"] = int(float(new_trigger_price) * 100)
        
        # Add other potential modification parameters if needed (e.g., new_product_type)
        # For now, product_type is usually not changed in modify, only price/qty/order_type

        print("Finvasia modify_order params:", modify_params)
        try:
            resp = self.api.modify_order(**modify_params)
        except Exception as e:
            if "Session Expired" in str(e) or "Invalid session" in str(e) or "Login again" in str(e):
                print(f"Session expired or invalid during modify. Attempting re-login for {self.client_id}...")
                try:
                    self.login()
                    print("Re-login successful. Retrying modify order.")
                    resp = self.api.modify_order(**modify_params)
                except Exception as retry_e:
                    msg = f"Re-login failed or modify retry failed after re-login: {retry_e}"
                    return {"status": "failure", "error": msg}
            else:
                msg = str(e) if str(e) else "Unknown error during modify_order"
                return {"status": "failure", "error": msg}
        
        if resp is None:
            return {"status": "failure", "error": "No response received from Finvasia API (modify_order)."}

        if isinstance(resp, dict):
            if resp.get("stat") == "Ok":
                return {"status": "success", "order_id": resp.get("norenordno"), **resp}
            return {"status": "failure", "error": resp.get("emsg", "Order modification failed with unknown error."), **resp}
        
        return {"status": "failure", "error": str(resp) if resp is not None else "Unknown response type from API."}


    def get_order_list(self):
        """Fetches the full order book."""
        resp = self.api.get_order_book()
        if resp and resp.get("stat") == "Ok":
            return {"status": "success", "orders": resp.get("items", [])} # Finvasia returns a list of orders in "items" key
        return {"status": "failure", "error": resp.get("emsg", "Failed to retrieve order list.") if resp else "No response from get_order_book."}

    def get_order_status(self, order_id):
        """
        Fetches the status of a specific order.
        Note: Finvasia's get_order_book returns all orders. You might need to filter or
        if a direct single-order status API is available, use that.
        For now, this fetches all and filters.
        """
        order_book_resp = self.get_order_list()
        if order_book_resp.get("status") == "success":
            for order in order_book_resp.get("orders", []):
                if order.get("norenordno") == str(order_id): # Ensure type match
                    return {"status": "success", "order": order}
            return {"status": "failure", "error": f"Order with ID {order_id} not found."}
        return {"status": "failure", "error": order_book_resp.get("error", "Failed to retrieve order status.")}

    def get_trade_list_for_order(self, order_id):
        """
        Fetches trade details for a specific order.
        Note: ShoonyaApiPy's get_trade_book returns all trades. This method filters for a specific order.
        """
        trade_book_resp = self.api.get_trade_book()
        if trade_book_resp and trade_book_resp.get("stat") == "Ok":
            trades = [trade for trade in trade_book_resp.get("items", []) if trade.get("norenordno") == str(order_id)]
            if trades:
                return {"status": "success", "trades": trades}
            return {"status": "success", "trades": [], "message": f"No trades found for order ID {order_id}."}
        return {"status": "failure", "error": trade_book_resp.get("emsg", "Failed to retrieve trade list.") if trade_book_resp else "No response from get_trade_book."}

    def cancel_order(self, order_id):
        """Cancels a specific order by its order ID."""
        try:
            resp = self.api.cancel_order(orderno=order_id)
        except Exception as e:
            if "Session Expired" in str(e) or "Invalid session" in str(e) or "Login again" in str(e):
                print(f"Session expired or invalid during cancel. Attempting re-login for {self.client_id}...")
                try:
                    self.login()
                    print("Re-login successful. Retrying cancel order.")
                    resp = self.api.cancel_order(orderno=order_id)
                except Exception as retry_e:
                    msg = f"Re-login failed or cancel retry failed after re-login: {retry_e}"
                    return {"status": "failure", "error": msg}
            else:
                msg = str(e) if str(e) else "Unknown error during cancel_order"
                return {"status": "failure", "error": msg}

        if resp is None:
            return {"status": "failure", "error": "No response received from Finvasia API (cancel_order)."}

        if isinstance(resp, dict):
            if resp.get("stat") == "Ok":
                return {"status": "success", "order_id": order_id, **resp}
            return {"status": "failure", "error": resp.get("emsg", "Order cancellation failed with unknown error."), **resp}
        
        return {"status": "failure", "error": str(resp) if resp is not None else "Unknown response type from API."}

    def get_positions(self):
        """Fetches all open positions."""
        resp = self.api.get_positions()
        if resp and resp.get("stat") == "Ok":
            return {"status": "success", "positions": resp.get("items", [])} # Positions are in "items" list
        return {"status": "failure", "error": resp.get("emsg", "Failed to retrieve positions.") if resp else "No response from get_positions."}

    def get_holdings(self):
        """Fetches all holdings."""
        resp = self.api.get_holdings()
        if resp and resp.get("stat") == "Ok":
            return {"status": "success", "holdings": resp.get("items", [])} # Holdings are in "items" list
        return {"status": "failure", "error": resp.get("emsg", "Failed to retrieve holdings.") if resp else "No response from get_holdings."}

    def last_auth_error(self):
        return self._last_auth_error
