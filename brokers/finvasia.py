import pyotp
import logging
from api_helper import ShoonyaApiPy  # From ShoonyaApi-py cloned from GitHub (AnkitSG07/ShoonyaApi-py)
from .base import BrokerBase

logger = logging.getLogger(__name__)

class FinvasiaBroker(BrokerBase):
    """
    Adapter for Finvasia (Shoonya) API using ShoonyaApiPy from the correct GitHub repo.
    Required:
        - client_id, password, totp_secret, vendor_code, api_key, imei
    """
    def __init__(self, client_id, password=None, totp_secret=None, vendor_code=None, api_key=None, imei=None, **kwargs):
        kwargs.pop("access_token", None)  # Remove access_token if present from kwargs
        super().__init__(client_id, "", **kwargs)
        if not imei:
            raise ValueError("IMEI is required for Finvasia accounts")
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

    def _is_logged_in(self):
        """Check if the underlying Shoonya API is logged in."""
        if hasattr(self.api, "is_logged_in"):
            try:
                return self.api.is_logged_in()
            except Exception:
                pass
        return self.session is not None

    def login(self):
        try:
            totp = pyotp.TOTP(self.totp_secret).now()
            try:
                ret = self.api.login(
                    userid=self.client_id,
                    password=self.password,
                    twoFA=totp,
                    vendor_code=self.vendor_code,
                    api_secret=self.api_key,
                    imei=self.imei
                )
            except ValueError as e:
                # ShoonyaApiPy can raise JSON decoding errors when the API
                # returns an empty or non-JSON response. Handle it gracefully
                error_msg = f"Invalid response from Finvasia login API: {e}"
                self._last_auth_error = error_msg
                logger.error(error_msg)
                return {"status": "failure", "error": error_msg}

            if ret and ret.get("stat") == "Ok":
                self.session = ret  # Store the session info
                self._last_auth_error = None
                logger.info("Finvasia login successful for %s", self.client_id)
                return {"status": "success", "message": "Login successful"}
                
            error_msg = ret.get("emsg", "Unknown login error") if ret else "No response from login API"
            self._last_auth_error = error_msg
            logger.error("Finvasia login failed for %s: %s", self.client_id, error_msg)
            return {"status": "failure", "error": error_msg}

        except Exception as e:
            error_msg = f"Exception during Finvasia login: {e}"
            self._last_auth_error = error_msg
            logger.error(error_msg)
            return {"status": "failure", "error": error_msg}

    def _normalize_product_type(self, product_type):
        """Normalize product type for Finvasia API."""
        product_type = str(product_type).strip().upper()
        if product_type in ["MIS", "INTRA", "INTRADAY", "INTRA DAY"]:
            return "M"  # Margin Intraday Square Off
        if product_type in ["CNC", "CARRYFORWARD"]:
            return "C"  # Cash & Carry / Delivery
        if product_type in ["NRML", "NORMAL"]:
            return "H"  # Normal/Holding (for F&O carryforward)
        return product_type  # Return as is if not in common aliases

    def _normalize_product(self, product):
        """Backward-compatible alias for product normalization."""
        return self._normalize_product_type(product)

    def _normalize_order_type(self, order_type):
        if not order_type:
            return order_type
        ot = str(order_type).upper()
        mapping = {
            "MARKET": "MKT",
            "MKT": "MKT",
            "LIMIT": "LMT",
            "L": "LMT",
            "SL": "SL",
            "SL-M": "SL-M",
        }
        return mapping.get(ot, ot)


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

    def place_order(
        self,
        tradingsymbol=None,
        exchange=None,
        transaction_type=None,
        quantity=None,
        order_type="MKT",
        product="C",
        price=0,
        token=None,
        **kwargs,
    ):
        """
        Places an order with Finvasia.
        Args:
            tradingsymbol (str): The trading symbol (e.g., "ADANIENT-EQ").
            exchange (str): The exchange (e.g., "NSE").
            transaction_type (str): "BUY" or "SELL".
            quantity (int): Order quantity.
            order_type (str): "MKT", "LMT", "SL", "SL-M".
            product (str): "CNC", "MIS", "NRML" (will be normalized to "C", "M", "H").
            price (float): Price for LIMIT/SL orders. **Assumed to be in Rupees; converted to Paise.**
            token (str): Finvasia's unique instrument token (e.g., "25"). This parameter
                         is accepted by this wrapper for potential internal use (e.g., if you need
                         to construct a specific tradingsymbol format like 'NSE|25'), but NOT
                         passed directly to ShoonyaApiPy's place_order method.
        """
        
        # Allow generic project-wide order fields if explicit Finvasia names were
        # omitted.  This mirrors the alias pattern used by other broker adapters
        # such as ``DhanBroker``.
        tradingsymbol = tradingsymbol or kwargs.pop("symbol", None)
        transaction_type = transaction_type or kwargs.pop("action", None)
        quantity = quantity or kwargs.pop("qty", None)
        product = kwargs.pop("product_type", product)

        if not self._is_logged_in():
            self.login()
            if not self._is_logged_in():
                return {"status": "failure", "error": "Finvasia API not logged in."}

        product_code = self._normalize_product_type(product)
        order_type = self._normalize_order_type(order_type)

        
        # Convert price to paise for Finvasia API
        price_in_paise = int(float(price) * 100) if order_type in ["LMT", "SL", "SL-M"] and price is not None else 0
        
        trigger_price_in_paise = 0
        if kwargs.get("trigger_price") is not None:
            trigger_price_in_paise = int(float(kwargs["trigger_price"]) * 100)

        order_params = dict(
            buy_or_sell="B" if transaction_type.upper() == "BUY" else "S",
            product_type=product_code,
            exchange=exchange,
            tradingsymbol=tradingsymbol, # Use the actual tradingsymbol (e.g., "ADANIENT-EQ")
            quantity=int(quantity),
            discloseqty=0, # Typically 0 for most orders unless specified
            price_type=order_type,
            price=price_in_paise,
            trigger_price=trigger_price_in_paise,
            retention="DAY", # "DAY", "IOC", "GTD", etc.
            amo="NO", # After Market Order: "YES" or "NO"
            remarks=kwargs.get("remarks", ""),
            # token parameter is NOT passed directly to self.api.place_order
            # as ShoonyaApiPy handles instrument identification via tradingsymbol and exchange.
        )
        logger.debug("Finvasia place_order params: %s", order_params)

        try:
            resp = self.api.place_order(**order_params)
        except Exception as e:
            # Attempt re-login on session expiry and retry the order
            if "Session Expired" in str(e) or "Invalid session" in str(e) or "Login again" in str(e) or "api_session_timeout" in str(e):
                logger.warning("Session expired or invalid. Attempting re-login for %s", self.client_id)
                try:
                    login_result = self.login()
                    if login_result.get("status") == "success":
                        logger.info("Re-login successful. Retrying order.")
                        resp = self.api.place_order(**order_params) # Retry order after successful re-login
                    else:
                        return {"status": "failure", "error": f"Re-login failed: {login_result.get('error')}"}
                except Exception as retry_e:
                    msg = f"Re-login failed or order retry failed after re-login: {retry_e}"
                    return {"status": "failure", "error": msg}
            else:
                msg = str(e) if str(e) else "Unknown error during place_order"
                return {"status": "failure", "error": msg}
        
        # Handle cases where ShoonyaApiPy might return None for failed calls or unexpected responses
        if resp is None:
            return {"status": "failure", "error": "No response received from Finvasia API (place_order). Check API status."}

        if isinstance(resp, dict):
            if resp.get("stat") == "Ok":
                # Finvasia returns orderno on success
                return {"status": "success", "order_id": resp.get("norenordno"), "message": resp.get("emsg", "Order placed successfully."), **resp}
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
            token (str): Finvasia's unique instrument token. Accepted by this method,
                         but NOT passed directly to ShoonyaApiPy's modify_order.
        """
        modify_params = {
            "orderno": order_id,
            "exchange": exchange,
            "tradingsymbol": tradingsymbol,
            # token parameter is NOT passed directly to self.api.modify_order
        }
        if new_quantity is not None:
            modify_params["newquantity"] = int(new_quantity)
        if new_price is not None:
            # Convert new_price to paise for modify_order
            modify_params["newprice"] = int(float(new_price) * 100)
        if new_order_type is not None:
            modify_params["newprice_type"] = self._normalize_order_type(new_order_type)
        if new_trigger_price is not None:
            # Convert new_trigger_price to paise for modify_order
            modify_params["newtriggerprice"] = int(float(new_trigger_price) * 100)
        
        # remarks for modify if applicable
        if kwargs.get("remarks"):
            modify_params["remarks"] = kwargs["remarks"]

        logger.debug("Finvasia modify_order params: %s", modify_params)
        try:
            resp = self.api.modify_order(**modify_params)
        except Exception as e:
            if "Session Expired" in str(e) or "Invalid session" in str(e) or "Login again" in str(e) or "api_session_timeout" in str(e):
                logger.warning("Session expired or invalid during modify. Attempting re-login for %s", self.client_id)
                try:
                    login_result = self.login()
                    if login_result.get("status") == "success":
                        logger.info("Re-login successful. Retrying modify order.")
                        resp = self.api.modify_order(**modify_params)
                    else:
                        return {"status": "failure", "error": f"Re-login failed: {login_result.get('error')}"}
                except Exception as retry_e:
                    msg = f"Re-login failed or modify retry failed after re-login: {retry_e}"
                    return {"status": "failure", "error": msg}
            else:
                msg = str(e) if str(e) else "Unknown error during modify_order"
                return {"status": "failure", "error": msg}
        
        if resp is None:
            return {"status": "failure", "error": "No response received from Finvasia API (modify_order). Check API status."}

        if isinstance(resp, dict):
            if resp.get("stat") == "Ok":
                return {"status": "success", "order_id": resp.get("norenordno"), "message": resp.get("emsg", "Order modified successfully."), **resp}
            return {"status": "failure", "error": resp.get("emsg", "Order modification failed with unknown error."), **resp}
        
        return {"status": "failure", "error": str(resp) if resp is not None else "Unknown response type from API."}

    def cancel_order(self, order_id):
        """Cancels an order."""
        if not self._is_logged_in():
            self.login()
            if not self._is_logged_in():
                return {"status": "failure", "error": "Finvasia API not logged in for cancellation."}
        
        try:
            resp = self.api.cancel_order(orderno=order_id)
        except Exception as e:
            if "Session Expired" in str(e) or "Invalid session" in str(e) or "Login again" in str(e) or "api_session_timeout" in str(e):
                logger.warning("Session expired or invalid during cancel. Attempting re-login for %s", self.client_id)
                try:
                    login_result = self.login()
                    if login_result.get("status") == "success":
                        logger.info("Re-login successful. Retrying cancel order.")
                        resp = self.api.cancel_order(orderno=order_id)
                    else:
                        return {"status": "failure", "error": f"Re-login failed: {login_result.get('error')}"}
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
                return {"status": "success", "message": resp.get("emsg", "Order cancelled successfully."), **resp}
            return {"status": "failure", "error": resp.get("emsg", "Order cancellation failed with unknown error."), **resp}
        
        return {"status": "failure", "error": str(resp) if resp is not None else "Unknown response type from API."}

    def get_order_list(self):
        """Retrieves the list of all orders."""
        if not self._is_logged_in():
            self.login()
            if not self._is_logged_in():
                return {"status": "failure", "error": "Finvasia API not logged in for order list."}
        try:
            resp = self.api.get_order_book()
        except Exception as e:
            if "Session Expired" in str(e) or "Invalid session" in str(e) or "Login again" in str(e) or "api_session_timeout" in str(e):
                logger.warning("Session expired or invalid during get_order_book. Attempting re-login for %s", self.client_id)
                try:
                    login_result = self.login()
                    if login_result.get("status") == "success":
                        logger.info("Re-login successful. Retrying get_order_book.")
                        resp = self.api.get_order_book()
                    else:
                        return {"status": "failure", "error": f"Re-login failed: {login_result.get('error')}"}
                except Exception as retry_e:
                    msg = f"Re-login failed or get_order_book retry failed after re-login: {retry_e}"
                    return {"status": "failure", "error": msg}
            else:
                msg = str(e) if str(e) else "Unknown error during get_order_book"
                return {"status": "failure", "error": msg}

        if resp is None:
            return {"status": "failure", "error": "No response received from Finvasia API (get_order_book)."}

        if isinstance(resp, list):  # Order book is usually a list of orders
            for o in resp:
                o["symbol"] = o.get("tsym")
                o["action"] = o.get("trantype")
                o["qty"] = o.get("qty")
                o["exchange"] = o.get("exch")
                o["order_type"] = o.get("prctyp")
                o["id"] = o.get("norenordno")
            return {"status": "success", "data": resp}
        elif isinstance(resp, dict) and resp.get("stat") == "Not_Ok":
            return {"status": "failure", "error": resp.get("emsg", "Failed to retrieve order book.")}

        return {"status": "failure", "error": str(resp) if resp is not None else "Unknown response type from API."}

    def list_orders(self, **kwargs):
        """Return a list of orders with canonical field names."""

        resp = self.get_order_list(**kwargs)
        if isinstance(resp, dict):
            if resp.get("status") != "success":
                raise RuntimeError(resp.get("error") or "failed to fetch order list")
            orders = resp.get("data", [])
        else:
            orders = resp

        normalized = []
        for o in orders:
            if not isinstance(o, dict):
                continue
            order = dict(o)

            symbol = order.get("symbol") or order.get("tsym")
            if symbol is not None:
                order["symbol"] = str(symbol)

            action = order.get("action") or order.get("trantype")
            if action is not None:
                order["action"] = str(action).upper()

            qty = order.get("qty") or order.get("quantity")
            try:
                order["qty"] = int(qty)
            except (TypeError, ValueError):
                pass

            exchange = order.get("exchange") or order.get("exch")
            if exchange is not None:
                order["exchange"] = str(exchange).upper()

            order_type = order.get("order_type") or order.get("prctyp")
            if order_type is not None:
                order["order_type"] = str(order_type).upper()

            status = order.get("status") or order.get("stat")
            if status is not None:
                order["status"] = str(status).upper()

            order_id = order.get("order_id") or order.get("id") or order.get("norenordno")
            if order_id is not None:
                order["order_id"] = str(order_id)

            normalized.append(order)

        return normalized

    def get_positions(self):
        """Retrieves current positions."""
        if not self._is_logged_in():
            self.login()
            if not self._is_logged_in():
                return {"status": "failure", "error": "Finvasia API not logged in for positions."}
        try:
            resp = self.api.get_positions()
        except Exception as e:
            if "Session Expired" in str(e) or "Invalid session" in str(e) or "Login again" in str(e) or "api_session_timeout" in str(e):
                logger.warning("Session expired or invalid during get_positions. Attempting re-login for %s", self.client_id)
                try:
                    login_result = self.login()
                    if login_result.get("status") == "success":
                        logger.info("Re-login successful. Retrying get_positions.")
                        resp = self.api.get_positions()
                    else:
                        return {"status": "failure", "error": f"Re-login failed: {login_result.get('error')}"}
                except Exception as retry_e:
                    msg = f"Re-login failed or get_positions retry failed after re-login: {retry_e}"
                    return {"status": "failure", "error": msg}
            else:
                msg = str(e) if str(e) else "Unknown error during get_positions"
                return {"status": "failure", "error": msg}
            
        if resp is None:
            return {"status": "failure", "error": "No response received from Finvasia API (get_positions)."}

        if isinstance(resp, list): # Positions list
            # Optionally, transform position structure
            return {"status": "success", "positions": resp}
        elif isinstance(resp, dict) and resp.get("stat") == "Not_Ok":
            return {"status": "failure", "error": resp.get("emsg", "Failed to retrieve positions.")}
        
        return {"status": "failure", "error": str(resp) if resp is not None else "Unknown response type from API."}

    def get_holdings(self):
        """Retrieves current holdings."""
        if not self._is_logged_in():
            self.login()
            if not self._is_logged_in():
                return {"status": "failure", "error": "Finvasia API not logged in for holdings."}
        try:
            resp = self.api.get_holdings()
        except Exception as e:
            if "Session Expired" in str(e) or "Invalid session" in str(e) or "Login again" in str(e) or "api_session_timeout" in str(e):
                logger.warning("Session expired or invalid during get_holdings. Attempting re-login for %s", self.client_id)
                try:
                    login_result = self.login()
                    if login_result.get("status") == "success":
                        logger.info("Re-login successful. Retrying get_holdings.")
                        resp = self.api.get_holdings()
                    else:
                        return {"status": "failure", "error": f"Re-login failed: {login_result.get('error')}"}
                except Exception as retry_e:
                    msg = f"Re-login failed or get_holdings retry failed after re-login: {retry_e}"
                    return {"status": "failure", "error": msg}
            else:
                msg = str(e) if str(e) else "Unknown error during get_holdings"
                return {"status": "failure", "error": msg}
            
        if resp is None:
            return {"status": "failure", "error": "No response received from Finvasia API (get_holdings)."}

        if isinstance(resp, list): # Holdings list
            # Optionally, transform holdings structure
            return {"status": "success", "holdings": resp}
        elif isinstance(resp, dict) and resp.get("stat") == "Not_Ok":
            return {"status": "failure", "error": resp.get("emsg", "Failed to retrieve holdings.")}
        
        return {"status": "failure", "error": str(resp) if resp is not None else "Unknown response type from API."}

    def get_limits(self):
        """Retrieves margin limits."""
        if not self._is_logged_in():
            self.login()
            if not self._is_logged_in():
                return {"status": "failure", "error": "Finvasia API not logged in for limits."}
        try:
            resp = self.api.get_limits()
        except Exception as e:
            if "Session Expired" in str(e) or "Invalid session" in str(e) or "Login again" in str(e) or "api_session_timeout" in str(e):
                logger.warning("Session expired or invalid during get_limits. Attempting re-login for %s", self.client_id)
                try:
                    login_result = self.login()
                    if login_result.get("status") == "success":
                        logger.info("Re-login successful. Retrying get_limits.")
                        resp = self.api.get_limits()
                    else:
                        return {"status": "failure", "error": f"Re-login failed: {login_result.get('error')}"}
                except Exception as retry_e:
                    msg = f"Re-login failed or get_limits retry failed after re-login: {retry_e}"
                    return {"status": "failure", "error": msg}
            else:
                msg = str(e) if str(e) else "Unknown error during get_limits"
                return {"status": "failure", "error": msg}
            
        if resp is None:
            return {"status": "failure", "error": "No response received from Finvasia API (get_limits)."}

        if isinstance(resp, dict) and resp.get("stat") == "Ok":
            # Finvasia's limits response structure needs to be checked, assuming it's a dict
            return {"status": "success", "limits": resp}
        elif isinstance(resp, dict) and resp.get("stat") == "Not_Ok":
            return {"status": "failure", "error": resp.get("emsg", "Failed to retrieve limits.")}
            
        return {"status": "failure", "error": str(resp) if resp is not None else "Unknown response type from API."}

    def get_opening_balance(self):
        """Return available cash balance using the limits API."""
        result = self.get_limits()
        if isinstance(result, dict):
            limits = result.get("limits") if result.get("status") == "success" else result
            if isinstance(limits, dict):
                first_non_zero = None
                balances = []
                for key in [
                    "cash",
                    "availablecash",
                    "available_balance",
                    "availableBalance",
                    "opening_balance",
                    "net",
                    "netCash",
                ]:
                    if key in limits:
                        try:
                            value = float(limits[key])
                        except (TypeError, ValueError):
                            pass

                        else:
                            balances.append(value)
                            if first_non_zero is None and value != 0:
                                first_non_zero = value

                if first_non_zero is not None:
                    return first_non_zero

                if balances:
                    return max(balances)

        return 0
 

    def last_auth_error(self):
        """Returns the last authentication error message."""
        return self._last_auth_error
