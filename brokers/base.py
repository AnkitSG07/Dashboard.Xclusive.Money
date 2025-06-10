# brokers/base.py
from abc import ABC, abstractmethod

class BrokerBase(ABC):
    """
    Abstract base class for all broker adapters.
    """

    def __init__(self, client_id, access_token, **kwargs):
        self.client_id = client_id
        self.access_token = access_token
        # Accept optional symbol map for symbol → security_id mapping
        self.symbol_map = kwargs.get('symbol_map', {})

    @abstractmethod
    def place_order(
        self,
        tradingsymbol=None,
        security_id=None,
        exchange_segment=None,
        transaction_type=None,
        quantity=None,
        order_type="MARKET",
        product_type="INTRADAY",
        price=None,
        **kwargs
    ):
        """
        Place a new order.
        """
        pass

    @abstractmethod
    def get_order_list(self):
        pass

    @abstractmethod
    def get_positions(self):
        pass

    @abstractmethod
    def cancel_order(self, order_id):
        pass

    def get_profile(self):
        raise NotImplementedError("get_profile not implemented for this broker.")

    def check_token_valid(self):
        raise NotImplementedError("check_token_valid not implemented for this broker.")

    def get_opening_balance(self):
        """Return opening balance for this account if available."""
        raise NotImplementedError("get_opening_balance not implemented for this broker.")
