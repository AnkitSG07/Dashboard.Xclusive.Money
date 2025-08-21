# brokers/base.py
from abc import ABC, abstractmethod
import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from prometheus_client import Histogram

# Default request timeout for broker HTTP calls. Gunicorn workers will be
# killed after 30s by default, so we keep a small safety margin to ensure the
# request fails before the worker does.  This value can still be overridden via
# the ``BROKER_TIMEOUT`` environment variable.
DEFAULT_TIMEOUT = int(os.environ.get("BROKER_TIMEOUT", "25"))

# Prometheus metric capturing broker API HTTP request latency
BROKER_API_LATENCY = Histogram(
    "broker_api_request_duration_seconds",
    "Time spent performing broker API HTTP requests",
    ["broker", "method"],
)

class BrokerBase(ABC):
    """
    Abstract base class for all broker adapters.
    """

    def __init__(self, client_id, access_token, *, timeout=None, **kwargs):
        self.client_id = client_id
        self.access_token = access_token
        # Use the passed timeout or fall back to the default configurable value
        self.timeout = timeout or DEFAULT_TIMEOUT
        # HTTP session with retries for all network calls
        self.session = self._create_session()
        # Accept optional symbol map for symbol → security_id mapping
        self.symbol_map = kwargs.get("symbol_map", {})

    def _create_session(self):
        """Return a requests session configured with retries."""
        session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[408, 429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS"],
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session


    def _request(self, method, url, *, timeout=None, **kwargs):
        """Perform an HTTP request with a hard timeout.

        Gunicorn kills workers that run longer than its configured timeout
        (30s by default).  The previous implementation retried once with
        double the timeout on ``ReadTimeout`` which could easily exceed the
        worker limit and crash the process.  Instead we make a single request
        and propagate any ``requests`` exceptions so callers can handle them
        gracefully.
        """
        timeout = timeout or self.timeout
        try:
            with BROKER_API_LATENCY.labels(
                broker=self.__class__.__name__, method=method
            ).time():
                return self.session.request(method, url, timeout=timeout, **kwargs)
        except requests.exceptions.RequestException:
            # Propagate network errors (including Timeouts) to the caller so
            # they can decide how to handle failures without hanging.
            raise
            
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
