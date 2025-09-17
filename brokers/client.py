"""HTTP/RPC client for broker microservices.

This lightweight client mirrors the public API of the broker adapters but
forwards all calls to a remote service via HTTP.  It provides basic
retry/timeout handling and a simple in-process rate limiter so callers don't
accidentally overwhelm the service.
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class _RateLimiter:
    """Simple token bucket rate limiter."""

    def __init__(self, calls: int, period: float) -> None:
        self.calls = calls
        self.period = period
        self.allowance = calls
        self.last_check = time.monotonic()

    def wait(self) -> None:
        """Block until a new request may be performed."""

        current = time.monotonic()
        time_passed = current - self.last_check
        self.last_check = current
        self.allowance += time_passed * (self.calls / self.period)
        if self.allowance > self.calls:
            self.allowance = self.calls
        if self.allowance < 1:
            sleep_time = (1 - self.allowance) * (self.period / self.calls)
            time.sleep(sleep_time)
            self.allowance = 0
        else:
            self.allowance -= 1


class BrokerServiceClient:
    """Client used by the main application to talk to a broker service."""

    def __init__(
        self,
        base_url: str,
        client_id: str,
        access_token: str,
        *,
        timeout: Optional[int] = None,
        rate_limit: str = "5/1s",
        **_: Any,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.client_id = client_id
        self.access_token = access_token
        self.timeout = timeout or int(os.environ.get("BROKER_TIMEOUT", "5"))

        # Configure HTTP session with retries
        self.session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[408, 429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Parse rate limit "calls/period" format, e.g. "5/1s" -> 5 calls per 1s
        calls, period = 5, 1.0
        try:
            part_calls, part_period = rate_limit.split("/")
            calls = int(part_calls)
            period = float(part_period.rstrip("s"))
        except Exception:  # pragma: no cover - fallback to defaults
            pass
        self._limiter = _RateLimiter(calls, period)

    # ------------------------------------------------------------------
    # helpers
    def _post(self, path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        self._limiter.wait()
        data = {"client_id": self.client_id, "access_token": self.access_token}
        if payload:
            data.update(payload)
        resp = self.session.post(
            f"{self.base_url}{path}", json=data, timeout=self.timeout
        )
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # public API matching BrokerBase
    def place_order(self, **params: Any) -> Dict[str, Any]:
        return self._post("/place_order", params)

    def get_positions(self) -> Dict[str, Any]:
        return self._post("/positions")

    def get_order_list(self) -> Any:
        return self._post("/order_list")

    def list_orders(self) -> Any:
        return self._post("/list_orders")

    def get_ltp(self, symbol: str) -> Dict[str, Any]:
        return self._post("/ltp", {"symbol": symbol})

    def get_trade_book(self) -> Any:
        return self._post("/trade_book")


__all__ = ["BrokerServiceClient"]
