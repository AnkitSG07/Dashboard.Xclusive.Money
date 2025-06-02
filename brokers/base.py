# brokers/base.py

from abc import ABC, abstractmethod

class BrokerBase(ABC):
    """
    Abstract base class for all broker adapters.
    All broker classes should inherit from this and implement the required methods.
    """

    def __init__(self, client_id, access_token, **kwargs):
        self.client_id = client_id
        self.access_token = access_token
        self.kwargs = kwargs

    @abstractmethod
    def place_order(self, tradingsymbol, exchange, transaction_type, quantity, order_type="MARKET", product="INTRADAY", price=None):
        """
        Place a new order.

        Returns:
            dict: {status: "success"/"failure", order_id/error}
        """
        pass

    @abstractmethod
    def get_order_list(self):
        """
        Get all orders.

        Returns:
            dict: {status: "success"/"failure", data: [orders]}
        """
        pass

    @abstractmethod
    def get_positions(self):
        """
        Get all open positions/holdings.

        Returns:
            dict: {status: "success"/"failure", data: positions}
        """
        pass

    @abstractmethod
    def cancel_order(self, order_id):
        """
        Cancel an order by order_id.

        Returns:
            dict: {status: "success"/"failure", order_id/error}
        """
        pass

    def get_profile(self):
        """
        Optional: Fetch user profile details.

        Returns:
            dict: {status: "success"/"failure", data: profile}
        """
        raise NotImplementedError("get_profile not implemented for this broker.")

    def check_token_valid(self):
        """
        Optional: Validate access token.

        Returns:
            bool
        """
        raise NotImplementedError("check_token_valid not implemented for this broker.")
