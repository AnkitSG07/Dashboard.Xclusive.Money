# brokers/groww.py
"""Groww broker adapter using the official `growwapi` SDK."""
from __future__ import annotations

import pyotp
from typing import Optional

from .base import BrokerBase

try:
    from growwapi import GrowwAPI
except ImportError:  # pragma: no cover - library might not be installed in tests
    GrowwAPI = None


class GrowwBroker(BrokerBase):
    """Adapter for Groww trading API."""

    BROKER = "groww"

    def __init__(
        self,
        client_id: str,
        access_token: Optional[str] = None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        totp_secret: Optional[str] = None,
        **kwargs,
    ) -> None:
        if GrowwAPI is None:
            raise ImportError("growwapi not installed")
        super().__init__(client_id, access_token or "", **kwargs)
        self.client_id = client_id

        if not access_token:
            if api_key and (api_secret or totp_secret):
                secret = api_secret or totp_secret
                token = pyotp.TOTP(secret).now()
                access_token = GrowwAPI.get_access_token(api_key, token)
                self.access_token = access_token
            else:
                raise ValueError(
                    "access_token or api_key and api_secret/totp_secret required"
                )
        self.api = GrowwAPI(access_token)

    # Convenience constants mirroring growwapi constants
    NSE = GrowwAPI.EXCHANGE_NSE
    SEGMENT_CASH = GrowwAPI.SEGMENT_CASH
    BUY = GrowwAPI.TRANSACTION_TYPE_BUY
    SELL = GrowwAPI.TRANSACTION_TYPE_SELL
    MARKET = GrowwAPI.ORDER_TYPE_MARKET
    LIMIT = GrowwAPI.ORDER_TYPE_LIMIT
    DAY = GrowwAPI.VALIDITY_DAY
    CNC = GrowwAPI.PRODUCT_CNC
    MIS = GrowwAPI.PRODUCT_MIS

    def place_order(
        self,
        tradingsymbol: str,
        exchange: str = NSE,
        transaction_type: str = BUY,
        quantity: int = 1,
        order_type: str = MARKET,
        product: str = MIS,
        price: Optional[float] = None,
        **kwargs,
    ):
        """Place an order using the Groww API."""
        segment = kwargs.get("segment", self.SEGMENT_CASH)
        validity = kwargs.get("validity", self.DAY)
        order_reference_id = kwargs.get("order_reference_id")
        trigger_price = kwargs.get("trigger_price")
        try:
            resp = self.api.place_order(
                validity=validity,
                exchange=exchange,
                order_type=order_type,
                product=product,
                quantity=int(quantity),
                segment=segment,
                trading_symbol=tradingsymbol,
                transaction_type=transaction_type,
                order_reference_id=order_reference_id,
                price=float(price) if price else 0.0,
                trigger_price=trigger_price,
            )
            if resp and resp.get("groww_order_id"):
                return {"status": "success", **resp}
            return {"status": "failure", **resp}
        except Exception as e:  # pragma: no cover - network call
            return {"status": "failure", "error": str(e)}

    def get_order_list(self, **kwargs):
        """Return list of orders."""
        page = kwargs.get("page", 0)
        page_size = kwargs.get("page_size", 25)
        segment = kwargs.get("segment")
        try:
            resp = self.api.get_order_list(page=page, page_size=page_size, segment=segment)
            return {"status": "success", "data": resp.get("order_list", [])}
        except Exception as e:  # pragma: no cover - network call
            return {"status": "failure", "error": str(e), "data": []}

    def cancel_order(self, order_id: str, segment: str = SEGMENT_CASH):
        """Cancel an existing order."""
        try:
            resp = self.api.cancel_order(groww_order_id=order_id, segment=segment)
            if resp and resp.get("groww_order_id"):
                return {"status": "success", **resp}
            return {"status": "failure", **resp}
        except Exception as e:  # pragma: no cover - network call
            return {"status": "failure", "error": str(e)}

    def get_positions(self, segment: Optional[str] = None):
        """Return current positions."""
        try:
            resp = self.api.get_positions_for_user(segment=segment)
            return {"status": "success", "data": resp.get("positions", [])}
        except Exception as e:  # pragma: no cover - network call
            return {"status": "failure", "error": str(e), "data": []}

    def get_profile(self):
        """Return available margin details as profile info."""
        try:
            resp = self.api.get_available_margin_details()
            return {"status": "success", "data": resp}
        except Exception as e:  # pragma: no cover - network call
            return {"status": "failure", "error": str(e), "data": None}

    def check_token_valid(self):
        try:
            self.api.get_order_list(page=0, page_size=1)
            return True
        except Exception:
            return False

    def get_opening_balance(self):
        try:
            resp = self.api.get_available_margin_details()
            for key in [
                "available_cash",
                "cash",
                "available_balance",
                "net_cash",
            ]:
                if key in resp:
                    return float(resp[key])
            return None
        except Exception:
            return None
