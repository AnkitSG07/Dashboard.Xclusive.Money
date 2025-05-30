import datetime
import logging

# brokers/base.py

class BrokerBase:
    """
    Abstract base class for all broker integrations.
    Each broker class (Zerodha, Dhan, AliceBlue, etc.) should inherit from this.
    """

    def __init__(self, client_id, access_token=None, api_key=None, secret_key=None, **kwargs):
        self.client_id = client_id
        self.access_token = access_token
        self.api_key = api_key
        self.secret_key = secret_key
        self.extra = kwargs  # For broker-specific keys (like app_id, app_secret, etc.)

    # --- Required Methods ---
    def authenticate(self):
        """
        Authenticate the user/session.
        For brokers that use tokens, this may be a no-op.
        """
        raise NotImplementedError("authenticate() not implemented for this broker.")

    def place_order(self, symbol, qty, order_type, side, product_type, price=None, validity='DAY', **kwargs):
        """
        Place an order.
        :param symbol: Trading symbol
        :param qty: Quantity
        :param order_type: 'MARKET', 'LIMIT', etc.
        :param side: 'BUY' or 'SELL'
        :param product_type: 'CNC', 'MIS', etc.
        :param price: Price if LIMIT order
        :param validity: Order validity
        """
        raise NotImplementedError("place_order() not implemented for this broker.")

    def get_order_list(self, status=None):
        """
        Get all orders (optionally filter by status: 'open', 'completed', etc.)
        """
        raise NotImplementedError("get_order_list() not implemented for this broker.")

    def get_positions(self):
        """
        Get current positions/holdings.
        """
        raise NotImplementedError("get_positions() not implemented for this broker.")

    def get_balance(self):
        """
        Get account balance/margin available.
        """
        raise NotImplementedError("get_balance() not implemented for this broker.")

    def square_off(self, symbol):
        """
        Square off (close) position for the given symbol.
        """
        raise NotImplementedError("square_off() not implemented for this broker.")

    def get_profile(self):
        """
        Fetch account profile details (name, email, etc.).
        """
        raise NotImplementedError("get_profile() not implemented for this broker.")

    def reconnect(self):
        """
        Reconnect or refresh session/token if needed.
        """
        raise NotImplementedError("reconnect() not implemented for this broker.")

    # --- Optional Helper/Utility Methods ---

    def is_token_expired(self):
        """
        Check if the access token is expired (implement if required for broker).
        """
        return False

    def refresh_token(self):
        """
        Refresh the access token (implement if required for broker).
        """
        pass


    def log_api_error(self, error, context=""):
        """
        Logs API errors with broker/client context.
        """
        msg = f"[{self.__class__.__name__}][{self.client_id}] API Error: {error}"
        if context:
            msg += f" | Context: {context}"
        print(msg)
        # You could also write to a log file:
        # logging.error(msg)

    def format_symbol(self, symbol):
        """
        Formats symbol string to a standard (override in child if needed).
        """
        return symbol.upper()

    def now(self):
        """
        Returns current UTC time as ISO string (helpful for timestamps).
        """
        return datetime.datetime.utcnow().isoformat()

    def safe_api_call(self, func, *args, **kwargs):
        """
        Wrapper to call broker API safely, catch errors and log them.
        Returns None if exception occurs.
        """
        try:
            return func(*args, **kwargs)
        except Exception as e:
            self.log_api_error(str(e), context=func.__name__)
            return None

    def status_message(self, status, message=""):
        """
        Standardizes status messages from brokers.
        """
        return {
            "broker": self.__class__.__name__,
            "client_id": self.client_id,
            "status": status,
            "message": message,
            "timestamp": self.now()
        }

