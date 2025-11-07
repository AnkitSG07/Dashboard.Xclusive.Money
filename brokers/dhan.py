import base64
import json
import logging
import re
import calendar
from datetime import datetime, date, timedelta, timezone
from hashlib import pbkdf2_hmac
from typing import Any, Dict, Optional

import pyotp
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from .base import BrokerBase
from .symbol_map import get_symbol_for_broker_lazy, refresh_symbol_slice

logger = logging.getLogger(__name__)


class DhanAuthError(RuntimeError):
    """Raised when the automated Dhan login flow fails."""

class DhanBroker(BrokerBase):
    BROKER = "dhan"
    NSE = "NSE_EQ"
    INTRA = "INTRADAY"
    BUY = "BUY"
    SELL = "SELL"
    MARKET = "MARKET"
    LIMIT = "LIMIT"

    AUTH_BASE = "https://auth.dhan.co"
    LOGIN_API_BASE = "https://login-api.dhan.co"
    _DHAN_APP_VERSION = "v1.3.0.0"
    _DHAN_APP_ID = "DH_WEB_V3"
    _DHAN_SOURCE = "W"
    _DHAN_PBKDF2_PASS = b"DHAN_WEB_V3"
    _DHAN_PBKDF2_SALT = bytes.fromhex("b3cfa2e8d0b64f1c2e5b7890a1c2d3e4")
    _DHAN_PBKDF2_ITERATIONS = 4096
    _DHAN_AES_IV = bytes.fromhex("1f2e3d4c5b6a79888796a5b4c3d2e1f0")
    _DHAN_ENC_VERSION = "v3"
    TOKEN_REFRESH_SKEW = timedelta(minutes=2)

    def __init__(self, client_id, access_token=None, **kwargs):
        timeout = kwargs.pop("timeout", 10)
        self.auth_base = (kwargs.pop("auth_base", self.AUTH_BASE) or self.AUTH_BASE).rstrip("/")
        self.login_id = kwargs.pop("login_id", kwargs.pop("dhan_login_id", None))
        login_password = kwargs.pop("login_password", None)
        if login_password is None:
            login_password = kwargs.pop("password", None)
        self.login_password = login_password
        self.api_key = kwargs.pop("api_key", kwargs.pop("app_id", None))
        self.api_secret = kwargs.pop("api_secret", kwargs.pop("app_secret", None))
        totp_secret = kwargs.pop("totp_secret", None)
        if isinstance(totp_secret, str):
            totp_secret = totp_secret.replace(" ", "")
        self.totp_secret = totp_secret
        self.refresh_token = kwargs.pop("refresh_token", None)
        self.token_time = kwargs.pop("token_time", None)
        symbol_map = kwargs.pop("symbol_map", {})
        self._token_expiry_dt: Optional[datetime] = None
        self.token_expiry: Optional[str] = None
        self.persist_credentials: Dict[str, Any] = {}
        self._last_auth_error_message: Optional[str] = None

        super().__init__(client_id, access_token or "", timeout=timeout, symbol_map=symbol_map)

        self.api_base = "https://api.dhan.co/v2"
        requests.packages.urllib3.util.connection.HAS_IPV6 = False
        self._update_headers(self.access_token)

        token_expiry = kwargs.pop("token_expiry", None)
        if token_expiry:
            self._set_token_expiry(token_expiry)

        if self.access_token and not self.token_time:
            self.token_time = datetime.now(timezone.utc).isoformat()

        if not self.access_token and self._has_login_material():
            self.generate_session_token()

    def _update_headers(self, access_token: Optional[str]) -> None:
        token = access_token or ""
        self.access_token = token
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if token:
            headers["access-token"] = token
        self.headers = headers

    def _has_login_material(self) -> bool:
        return all(
            [
                self.login_id,
                self.login_password,
                self.api_key,
                self.api_secret,
                self.totp_secret,
            ]
        )

    def _auth_headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        if self.api_key:
            headers["app_id"] = self.api_key
        if self.api_secret:
            headers["app_secret"] = self.api_secret
        return headers

    def _set_token_expiry(self, value: Any) -> None:
        dt = self._parse_timestamp(value)
        self._token_expiry_dt = dt
        self.token_expiry = dt.isoformat() if dt else None

    @staticmethod
    def _parse_timestamp(value: Any) -> Optional[datetime]:
        if not value:
            return None
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, (int, float)):
            dt = datetime.fromtimestamp(value, tz=timezone.utc)
        elif isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            try:
                dt = datetime.fromisoformat(text)
            except ValueError:
                try:
                    dt = datetime.strptime(text, "%Y-%m-%dT%H:%M:%S")
                except ValueError:
                    return None
        else:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def _record_persist_credentials(self) -> None:
        persist: Dict[str, Any] = {}
        if self.access_token:
            persist["access_token"] = self.access_token
        if self.token_time:
            persist["token_time"] = self.token_time
        if self.refresh_token:
            persist["refresh_token"] = self.refresh_token
        if self.token_expiry:
            persist["token_expiry"] = self.token_expiry
        self.persist_credentials = persist

    def last_auth_error(self) -> Optional[str]:
        return self._last_auth_error_message

    def _should_refresh_token(self) -> bool:
        if not self._has_login_material():
            return False
        if not self.access_token:
            return True
        if not self._token_expiry_dt:
            return False
        now = datetime.now(timezone.utc)
        return now >= self._token_expiry_dt - self.TOKEN_REFRESH_SKEW

    def _perform_login(self, consent_app_id: str, otp_code: str) -> str:
        base_payload = {
            "user_id": self.login_id,
            "login_id": self.login_id,
            "imei_no": self._device_identifier(),
            "web_version": self._web_version(),
            "role": "Admin",
            "app_version": self._DHAN_APP_VERSION,
            "app_id": self._DHAN_APP_ID,
            "source": self._DHAN_SOURCE,
            "device_id": self._device_identifier(),
            "device_type": "WEB",
            "device_name": self._device_identifier(),
            "device_model": "Python Client",
            "device_os": "linux",
            "device_os_version": self._web_version(),
            "browser": "python-requests",
            "browser_version": requests.__version__,
            "login_flow": "TOTP",
            "is_trusted_device": "Y",
        }
        login_headers = {
            "Content-Type": "application/json",
            **self._auth_headers(),
        }

        try:
            self._login_api_post(
                "/jwt/token",
                base_payload,
                headers=login_headers,
                expect_json=False,
            )
        except requests.exceptions.RequestException as exc:
            raise DhanAuthError(f"JWT token request failed: {exc}") from exc

        login_payload = dict(base_payload)
        login_payload.update(
            {
                "pass": self.login_password or "",
                "password": self.login_password or "",
                "totp": otp_code,
                "otp": otp_code,
                "consent_app_id": consent_app_id,
                "totp_verified": True,
                "login_type": "PASSWORD",  # Explicit login type required by v2 API    
            }
        )

        try:
            login_response = self._login_api_post(
                "/loginV2/login",
                login_payload,
                headers=login_headers,
            )
        except requests.exceptions.RequestException as exc:
            raise DhanAuthError(f"Login request failed: {exc}") from exc

        status = str(login_response.get("status") or "").lower()
        if status != "success":
            message = login_response.get("message") or "Login API returned failure"
            raise DhanAuthError(str(message))
        data_list = login_response.get("data")
        if isinstance(data_list, dict):
            candidates = [data_list]
        elif isinstance(data_list, list):
            candidates = data_list
        else:
            raise DhanAuthError("Login API response missing data")
        if not candidates:
            raise DhanAuthError("Login API response missing data")
        token_details = candidates[0]
        token_id = token_details.get("token_id") or token_details.get("tokenId")
        if not token_id:
            raise DhanAuthError("Login API response missing token_id")
        return str(token_id)

    def _device_identifier(self) -> str:
        base = self.client_id or self.login_id or "unknown"
        return f"python-{base}"

    def _web_version(self) -> str:
        return "Python Requests"

    @classmethod
    def _dhan_cipher(cls) -> Cipher:
        aes_key = pbkdf2_hmac(
            "sha1",
            cls._DHAN_PBKDF2_PASS,
            cls._DHAN_PBKDF2_SALT,
            cls._DHAN_PBKDF2_ITERATIONS,
            dklen=16,
        )
        return Cipher(
            algorithms.AES(aes_key),
            modes.CBC(cls._DHAN_AES_IV),
            backend=default_backend(),
        )

    @classmethod
    def _encrypt_payload(cls, payload: Dict[str, Any]) -> str:
        text = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        padder = padding.PKCS7(128).padder()
        padded = padder.update(text.encode("utf-8")) + padder.finalize()
        encryptor = cls._dhan_cipher().encryptor()
        ciphertext = encryptor.update(padded) + encryptor.finalize()
        return base64.b64encode(ciphertext).decode("ascii")

    @classmethod
    def _decrypt_payload(cls, payload: str) -> str:
        ciphertext = base64.b64decode(payload)
        decryptor = cls._dhan_cipher().decryptor()
        padded = decryptor.update(ciphertext) + decryptor.finalize()
        unpadder = padding.PKCS7(128).unpadder()
        data = unpadder.update(padded) + unpadder.finalize()
        return data.decode("utf-8")

    def _login_api_post(
        self,
        path: str,
        payload: Dict[str, Any],
        *,
        headers: Optional[Dict[str, str]] = None,
        expect_json: bool = True,
    ) -> Dict[str, Any]:
        encrypted = self._encrypt_payload(payload)
        body = {
            "data": encrypted,
            "enc": self._DHAN_ENC_VERSION,
            "app_id": self._DHAN_APP_ID,
            "app_version": self._DHAN_APP_VERSION,
        }
        url = f"{self.LOGIN_API_BASE}{path}"
        resp = self._request(
            "post",
            url,
            json=body,
            headers=headers,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        if not expect_json:
            return {}
        try:
            raw = resp.text
        except AttributeError:
            raw = resp.content.decode("utf-8")
        try:
            wrapped = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise DhanAuthError(f"Invalid login API response: {exc}") from exc
        if isinstance(wrapped, dict):
            encrypted_payload = wrapped.get("data")
            if isinstance(encrypted_payload, str):
                try:
                    decrypted_text = self._decrypt_payload(encrypted_payload)
                except Exception:
                    # If the payload is not encrypted anymore fall back to the raw shape
                    return wrapped
                try:
                    return json.loads(decrypted_text)
                except json.JSONDecodeError as exc:
                    raise DhanAuthError(
                        f"Unable to decode login API payload: {exc}"
                    ) from exc
            return wrapped
        return wrapped

    def generate_session_token(self, *, force_refresh: bool = False) -> str:
        if not self._has_login_material():
            raise DhanAuthError("Missing Dhan login credentials for automated authentication")

        if not force_refresh and self.access_token and not self._should_refresh_token():
            return self.access_token

        try:
            otp_code = pyotp.TOTP(self.totp_secret).now()
        except Exception as exc:
            message = f"Failed to compute Dhan TOTP code: {exc}"
            self._last_auth_error_message = message
            raise DhanAuthError(message) from exc

        try:
            consent_resp = self._request(
                "post",
                f"{self.auth_base}/app/generate-consent",
                params={"client_id": self.client_id},
                headers=self._auth_headers(),
                timeout=self.timeout,
            )
            consent_resp.raise_for_status()
            consent_data = consent_resp.json()
        except requests.exceptions.RequestException as exc:
            message = f"Failed to generate Dhan consent: {exc}"
            self._last_auth_error_message = message
            raise DhanAuthError(message) from exc
        except json.JSONDecodeError as exc:
            message = f"Invalid consent response: {exc}"
            self._last_auth_error_message = message
            raise DhanAuthError(message) from exc

        consent_app_id = consent_data.get("consentAppId") or consent_data.get("consent_app_id")
        if not consent_app_id:
            message = "Consent response missing consentAppId"
            self._last_auth_error_message = message
            raise DhanAuthError(message)

        token_id = self._perform_login(str(consent_app_id), otp_code)

        try:
            consume_resp = self._request(
                "get",
                f"{self.auth_base}/app/consumeApp-consent",
                params={"tokenId": token_id},
                headers=self._auth_headers(),
                timeout=self.timeout,
            )
            consume_resp.raise_for_status()
            consume_data = consume_resp.json()
        except requests.exceptions.RequestException as exc:
            message = f"Failed to consume Dhan consent: {exc}"
            self._last_auth_error_message = message
            raise DhanAuthError(message) from exc
        except json.JSONDecodeError as exc:
            message = f"Invalid consume-consent response: {exc}"
            self._last_auth_error_message = message
            raise DhanAuthError(message) from exc

        new_token = consume_data.get("accessToken") or consume_data.get("access_token")
        if not new_token:
            message = "Consume consent response missing accessToken"
            self._last_auth_error_message = message
            raise DhanAuthError(message)

        self.refresh_token = consume_data.get("refreshToken") or consume_data.get("refresh_token")
        expiry_raw = consume_data.get("expiryTime") or consume_data.get("expiry_time")
        if expiry_raw:
            self._set_token_expiry(expiry_raw)
        else:
            self._token_expiry_dt = None
            self.token_expiry = None

        self.token_time = datetime.now(timezone.utc).isoformat()
        self._update_headers(str(new_token))
        self._record_persist_credentials()
        self._last_auth_error_message = None
        return self.access_token

    def _is_invalid_auth(self, data, status):
        """Return ``True`` if ``data``/``status`` indicate invalid credentials."""
        if status == 401:
            return True
        if isinstance(data, dict):
            code = str(data.get("errorCode") or "").upper()
            err_type = str(data.get("errorType") or "").lower()
            msg = str(
                data.get("errorMessage") or data.get("message") or ""
            ).lower()
            if err_type == "invalid_authentication" or code in {"DH-901"}:
                return True
            if "invalid" in msg and "token" in msg:
                return True
        return False

    def _normalize_segment(self, seg):
        """Return API-compatible exchange segment string."""
        if not seg or str(seg).upper() in ("ALL", "", "NONE"):
            return self.NSE
        seg = str(seg).upper()
        if seg == "NSE":
            return "NSE_EQ"
        if seg == "BSE":
            return "BSE_EQ"
        return seg

    def _normalize_product_type(self, product_type):
        if not product_type:
            return product_type
        pt = str(product_type).upper()
        if pt == "MIS":
            return "INTRADAY"
        return pt

    def _normalize_order_type(self, order_type):
        if not order_type:
            return order_type
        ot = str(order_type).upper()
        mapping = {
            "MKT": self.MARKET,
            "MARKET": self.MARKET,
            "L": self.LIMIT,
            "LIMIT": self.LIMIT,
        }
        return mapping.get(ot, ot)

    @staticmethod
    def _expiry_from_symbol(symbol):
        """Parse expiry date from derivative symbols like BANKNIFTY24AUGFUT.
        Returns a ``date`` object for the contract's expiry or ``None`` if the
        symbol cannot be parsed.
        """
        if not symbol:
            return None
        sym = symbol.upper()
        m = re.search(r"(\d{2})([A-Z]{3})", sym)
        if not m:
            return None
        year = 2000 + int(m.group(1))
        try:
            month = datetime.strptime(m.group(2), "%b").month
        except ValueError:
            return None
        last_day = calendar.monthrange(year, month)[1]
        expiry = date(year, month, last_day)
        while expiry.weekday() != 3:
            expiry -= timedelta(days=1)
        return expiry

    def place_order(
        self,
        tradingsymbol=None,
        security_id=None,
        exchange_segment=None,
        transaction_type=None,
        quantity=None,
        instrument_type=None,
        order_type="MARKET",
        product_type="INTRADAY",
        price=0,
        **extra
    ):
        """Place a new order on Dhan."""

        tradingsymbol = tradingsymbol or extra.pop("symbol", None)
        transaction_type = transaction_type or extra.pop("action", None)
        quantity = quantity or extra.pop("qty", None)
        security_id = security_id or extra.pop("security_id", None)
        instrument_type = instrument_type or extra.pop("instrument_type", None)
        exchange = exchange_segment or extra.pop("exchange", None)
        if exchange:
            exchange = str(exchange).upper()
            if exchange in {"NFO", "BFO"}:
                exchange_segment = "NSE_FNO" if exchange == "NFO" else "BSE_FNO"
                exchange_base = exchange
            else:
                exchange_segment = self._normalize_segment(exchange)
                exchange_base = exchange.split("_")[0]
        else:
            exchange_segment = None
            exchange_base = None

        if not instrument_type and tradingsymbol:
            sym = tradingsymbol.upper()
            has_digits = any(ch.isdigit() for ch in sym)
            if sym.endswith("FUT") and has_digits:
                instrument_type = "FUT"
            elif sym.endswith("CE") and has_digits:
                instrument_type = "CE"
            elif sym.endswith("PE") and has_digits:
                instrument_type = "PE"

        if instrument_type in {"FUT", "CE", "PE"} and (
            exchange_base in {None, "NSE", "BSE"}
        ):
            if exchange_base == "BSE":
                exchange_base = "BFO"
                exchange_segment = "BSE_FNO"
            else:
                exchange_base = "NFO"
                exchange_segment = "NSE_FNO"

        # Reject derivative contracts that have already expired before looking up
        # any security identifiers. The service should return a failure response
        # instead of raising an exception so callers can handle the error
        # gracefully and avoid network requests or symbol map lookups.
        if tradingsymbol and instrument_type in {"FUT", "CE", "PE"}:
            expiry = self._expiry_from_symbol(tradingsymbol)
            if expiry and expiry < datetime.utcnow().date():
                msg = f"Contract {tradingsymbol} expired on {expiry.isoformat()}"
                logger.warning(msg)
                return {"status": "failure", "error": msg}

        # Look up instrument details only when necessary.
        mapping = {}
        explicit_exchange = exchange_base is not None
        if security_id is None or exchange_segment is None:
            mapping = get_symbol_for_broker_lazy(
                tradingsymbol or "", self.BROKER, exchange_base
            )
        if security_id is None:
            try:
                security_id = mapping["security_id"]
            except KeyError:
                refresh_symbol_slice(tradingsymbol or "", exchange_base)
                mapping = get_symbol_for_broker_lazy(
                    tradingsymbol or "", self.BROKER, exchange_base
                )
                try:
                    security_id = mapping["security_id"]
                except KeyError:
                    security_id = None
            if (
                not security_id
                and tradingsymbol
                and self.symbol_map
                and not explicit_exchange
            ):
                security_id = self.symbol_map.get(tradingsymbol.upper())
        if not security_id:
            if explicit_exchange and not mapping:
                raise ValueError(
                    (
                        "DhanBroker: symbol {} is not available on {}. "
                        "Please choose a different exchange or instrument."
                    ).format(tradingsymbol, exchange_base)
                )
            raise ValueError(
                (
                    "DhanBroker: 'security_id' for symbol {} not found. "
                    "The symbol may be expired or not yet in Dhan's scrip master."
                ).format(tradingsymbol)
            )

        if exchange_segment is None:
            exchange_segment = mapping.get("exchange_segment", self.NSE)

        if not product_type:
            product_type = self.INTRA
        product_type = self._normalize_product_type(product_type)
        order_type = self._normalize_order_type(order_type)

        payload = {
            "dhanClientId": self.client_id,
            "securityId": security_id,
            "exchangeSegment": exchange_segment,
            "transactionType": transaction_type,
            "productType": product_type,
            "orderType": order_type,
            "quantity": int(quantity),
            "validity": "DAY",
            "price": float(price) if price else 0,
            "triggerPrice": "",
            "afterMarketOrder": False,
        }

        if order_type == self.LIMIT and not price:
            raise ValueError("Limit orders require a 'price'.")
        if order_type == self.MARKET:
            payload.pop("price", None)
            payload.pop("triggerPrice", None)
        try:
            r = self._request(
                "post",
                "{}/orders".format(self.api_base),
                json=payload,
                headers=self.headers,
                timeout=self.timeout,
            )
            resp = r.json()
        except requests.exceptions.Timeout:
            return {
                "status": "failure",
                "error": "Request to Dhan API timed out while placing order.",
                "source": "broker",
            }
        except requests.exceptions.RequestException as e:
            return {
                "status": "failure",
                "error": "Failed to place order with Dhan API: {}".format(str(e)),
                "source": "broker",
            }
        except json.JSONDecodeError:
            return {
                "status": "failure",
                "error": "Invalid JSON response from Dhan API: {}".format(r.text),
                "source": "broker",
            }
        except Exception as e:
            return {
                "status": "failure",
                "error": "An unexpected error occurred while placing order: {}".format(str(e)),
                "source": "broker",
            }

        if "orderId" in resp:
            result = {"status": "success"}
            if isinstance(resp, dict):
                result.update(resp)
            return result
        result = {"status": "failure", "source": "broker"}
        if isinstance(resp, dict):
            result.update(resp)
        return result

    def get_order_list(self, use_pagination=True, batch_size=100, max_batches=50):
        """
        Fetch order list. For large accounts, use pagination to avoid timeouts.
        If the Dhan API doesn't support offset/limit, set use_pagination=False.
        """
        if not use_pagination:
            try:
                r = self._request(
                    "get",
                    "{}/orders".format(self.api_base),
                    headers=self.headers,
                    timeout=self.timeout,
                )
                status = getattr(r, "status_code", 200)
                if status >= 400:
                    try:
                        data = r.json()
                    except Exception:
                        data = {}
                    logger.error(
                        "Error response from Dhan API while fetching order list: %r",
                        data or r.text,
                    )
                    return {
                        "status": "failure",
                        "error": data.get("errorMessage")
                        or data.get("message")
                        or "Failed to fetch order list from Dhan API.",
                        "source": "broker",
                    }
                data = r.json()
                return {"status": "success", "data": data}
            except requests.exceptions.Timeout:
                return {
                    "status": "failure",
                    "error": "Request to Dhan API timed out while fetching order list.",
                    "source": "broker",
                }
            except requests.exceptions.RequestException as e:
                return {
                    "status": "failure",
                    "error": "Failed to fetch order list from Dhan API: {}".format(str(e)),
                    "source": "broker",
                }
            except json.JSONDecodeError:
                return {
                    "status": "failure",
                    "error": "Invalid JSON response from Dhan API: {}".format(r.text),
                    "source": "broker",
                }
            except Exception as e:
                return {
                    "status": "failure",
                    "error": "An unexpected error occurred while fetching order list: {}".format(str(e)),
                    "source": "broker",
                }

        all_orders = []
        seen_ids = set()
        offset = 0
        for i in range(max_batches):
            try:
                url = "{}/orders?offset={}&limit={}".format(self.api_base, offset, batch_size)
                r = self._request("get", url, headers=self.headers, timeout=self.timeout)
                status = getattr(r, "status_code", 200)
                if status >= 400:
                    try:
                        batch = r.json()
                    except Exception:
                        batch = {}
                    if self._is_invalid_auth(batch, status):
                        logger.error(
                            "Error response from Dhan API at offset %s: %r", offset, batch or r.text
                        )
                        return {
                            "status": "failure",
                            "error": batch.get("errorMessage")
                            or batch.get("message")
                            or "Failed to fetch order list from Dhan API.",
                            "source": "broker",
                        }
                    log_fn = logger.warning if offset == 0 and use_pagination else logger.error
                    log_fn(
                        "Error response from Dhan API at offset %s: %r", offset, batch or r.text
                    )
                    if offset == 0 and use_pagination:
                        return self.get_order_list(
                            use_pagination=False
                        )
                    return {
                        "status": "partial_failure" if all_orders else "failure",
                        "error": batch.get("errorMessage")
                        or batch.get("message")
                        or "Failed to fetch order list from Dhan API.",
                        "data": all_orders,
                        "source": "broker",
                    }
                batch = r.json()
                if isinstance(batch, dict) and any(
                    k in batch for k in ("errorCode", "errorMessage", "errorType")
                ):
                    if self._is_invalid_auth(batch, status):
                        logger.error(
                            "Error response from Dhan API at offset %s: %r", offset, batch
                        )
                        return {
                            "status": "failure",
                            "error": batch.get("errorMessage")
                            or batch.get("message")
                            or "Failed to fetch order list from Dhan API.",
                            "source": "broker",
                        }
                    log_fn = logger.warning if offset == 0 and use_pagination else logger.error
                    log_fn(
                        "Error response from Dhan API at offset %s: %r", offset, batch
                    )
                    if offset == 0 and use_pagination:
                        return self.get_order_list(
                            use_pagination=False
                        )
                    return {
                        "status": "partial_failure" if all_orders else "failure",
                        "error": batch.get("errorMessage")
                        or batch.get("message")
                        or "Failed to fetch order list from Dhan API.",
                        "data": all_orders,
                        "source": "broker",
                    }
                batch_orders = batch.get("data", batch) if isinstance(batch, dict) else batch

                if not isinstance(batch_orders, list) or any(
                    not isinstance(o, dict) for o in batch_orders
                ):
                    logger.error(
                        "Invalid order batch structure from Dhan API at offset %s: %r",
                        offset,
                        batch,
                    )
                    return {
                        "status": "partial_failure" if all_orders else "failure",
                        "error": "Invalid order batch structure from Dhan API.",
                        "data": all_orders,
                        "source": "broker",
                    }

                if not batch_orders:
                    break

                first_oid = (batch_orders[0].get("orderId") or batch_orders[0].get("order_id")) if batch_orders else None
                if offset > 0 and first_oid in seen_ids:
                    break

                for o in batch_orders:
                    oid = o.get("orderId") or o.get("order_id")
                    if oid in seen_ids:
                        continue
                    seen_ids.add(oid)
                    all_orders.append(o)

                if len(batch_orders) < batch_size:
                    break
                offset += batch_size
            except requests.exceptions.Timeout:
                return {
                    "status": "partial_failure" if all_orders else "failure",
                    "error": "Request to Dhan API timed out during pagination.",
                    "data": all_orders,
                    "source": "broker",
                }
            except requests.exceptions.RequestException as e:
                return {
                    "status": "partial_failure" if all_orders else "failure",
                    "error": "Failed to fetch order list from Dhan API during pagination: {}".format(str(e)),
                    "data": all_orders,
                    "source": "broker",
                }
            except json.JSONDecodeError:
                return {
                    "status": "partial_failure" if all_orders else "failure",
                    "error": "Invalid JSON response from Dhan API during pagination: {}".format(r.text),
                    "data": all_orders,
                    "source": "broker",
                }
            except Exception as e:
                return {
                    "status": "partial_failure" if all_orders else "failure",
                    "error": "An unexpected error occurred during pagination: {}".format(str(e)),
                    "data": all_orders,
                    "source": "broker",
                }
        return {"status": "success", "data": all_orders}

    def get_trade_book(self):
        """Fetch the trade book for the day and normalize it."""
        try:
            r = self._request(
                "get",
                "{}/trades".format(self.api_base),
                headers=self.headers,
                timeout=self.timeout,
            )
            status = getattr(r, "status_code", 200)
            if status >= 400:
                try:
                    data = r.json()
                except Exception:
                    data = {}
                logger.error(
                    "Error response from Dhan API while fetching trade book: %r",
                    data or r.text,
                )
                return {
                    "status": "failure",
                    "error": data.get("errorMessage")
                    or data.get("message")
                    or "Failed to fetch trade book from Dhan API.",
                    "source": "broker",
                }
            
            trades = r.json()
            if not isinstance(trades, list):
                if isinstance(trades, dict) and "errorCode" in trades:
                    return {
                        "status": "failure",
                        "error": trades.get("errorMessage", "Unknown error"),
                    }
                logger.warning(
                    "Expected a list of trades, but got %s. Response: %r",
                    type(trades),
                    trades,
                )
                return {"status": "success", "trades": []}

            # Normalize fields to what master_trade_monitor expects
            normalized_trades = []
            for trade in trades:
                normalized_trades.append({
                    "order_id": trade.get("orderId"),
                    "symbol": trade.get("tradingSymbol"),
                    "action": trade.get("transactionType"),
                    "quantity": trade.get("tradedQuantity"),
                    "exchange": trade.get("exchangeSegment"),
                    "order_type": trade.get("orderType"),
                    "price": trade.get("tradedPrice"),
                    "product_type": trade.get("productType"),
                    "order_time": trade.get("exchangeTime"),
                    "trade_id": trade.get("exchangeTradeId"),
                })
            
            return {"status": "success", "trades": normalized_trades}

        except requests.exceptions.Timeout:
            return {
                "status": "failure",
                "error": "Request to Dhan API timed out while fetching trade book.",
                "source": "broker",
            }
        except requests.exceptions.RequestException as e:
            return {
                "status": "failure",
                "error": "Failed to fetch trade book from Dhan API: {}".format(str(e)),
                "source": "broker",
            }
        except json.JSONDecodeError:
            return {
                "status": "failure",
                "error": "Invalid JSON response from Dhan API while fetching trade book: {}".format(r.text),
                "source": "broker",
            }
        except Exception as e:
            return {
                "status": "failure",
                "error": "An unexpected error occurred while fetching trade book: {}".format(str(e)),
                "source": "broker",
            }

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
            symbol = order.get("tradingSymbol") or order.get("tradingsymbol") or order.get("symbol")
            if symbol is not None:
                order["symbol"] = str(symbol)

            action = order.get("transactionType") or order.get("action")
            if action is not None:
                order["action"] = str(action).upper()

            qty = order.get("quantity") or order.get("orderQty") or order.get("qty")
            try:
                order["qty"] = int(qty)
            except (TypeError, ValueError):
                pass

            exchange = order.get("exchangeSegment") or order.get("exchange")
            if exchange is not None:
                order["exchange"] = str(exchange).upper()

            order_type = order.get("orderType") or order.get("order_type")
            if order_type is not None:
                order["order_type"] = str(order_type).upper()

            status = order.get("status") or order.get("orderStatus")
            if status is not None:
                order["status"] = str(status).upper()

            order_id = order.get("orderId") or order.get("order_id")
            if order_id is not None:
                order["orderId"] = str(order_id)
                order["order_id"] = str(order_id)

            normalized.append(order)

        return normalized

    def cancel_order(self, order_id):
        """
        Cancel an order by order_id.
        """
        try:
            r = self._request(
                "delete",
                "{}/orders/{}".format(self.api_base, order_id),
                headers=self.headers,
                timeout=self.timeout,
            )
            return {"status": "success", "data": r.json()}
        except requests.exceptions.Timeout:
            return {"status": "failure", "error": "Request to Dhan API timed out while canceling order."}
        except requests.exceptions.RequestException as e:
            return {"status": "failure", "error": "Failed to cancel order with Dhan API: {}".format(str(e))}
        except json.JSONDecodeError:
            return {"status": "failure", "error": "Invalid JSON response from Dhan API: {}".format(r.text)}
        except Exception as e:
            return {"status": "failure", "error": "An unexpected error occurred while canceling order: {}".format(str(e))}

    def get_positions(self):
        """Fetch current positions and enrich them with LTP and P/L."""
        try:
            r = self._request(
                "get",
                f"{self.api_base}/positions",
                headers=self.headers,
                timeout=self.timeout,
            )
            data = r.json()
            if isinstance(data, dict):
                positions_payload = (
                    data.get("data")
                    or data.get("positions")
                    or data.get("net")
                    or data.get("netPositions")
                    or data.get("net_positions")
                    or data

                )
            else:
                
                positions_payload = data or []

            if isinstance(positions_payload, dict):
                combined_positions = []
                for key in (
                    "overallPositions",
                    "overall_positions",
                    "todayPositions",
                    "today_positions",
                    "netPositions",
                    "net_positions",
                    "positions",
                    "data",
                    "net",
                ):
                    value = positions_payload.get(key)
                    if isinstance(value, list):
                        combined_positions.extend(value)
                positions = combined_positions
            elif isinstance(positions_payload, list):
                positions = positions_payload
            else:
                positions = []

            securities = {}
            for p in positions:
                seg = self._normalize_segment(p.get("exchangeSegment") or p.get("exchange"))
                sid = p.get("securityId") or p.get("security_id")
                if seg and sid:
                    try:
                        securities.setdefault(seg, []).append(int(sid))
                    except Exception:
                        pass

            quotes = {}
            if securities:
                try:
                    qr = self._request(
                        "post",
                        f"{self.api_base}/marketfeed/ltp",
                        json=securities,
                        headers=self.headers,
                        timeout=self.timeout,
                    )
                    qdata = qr.json()
                    if isinstance(qdata, dict):
                        quotes = qdata.get("data", {})
                except Exception:
                    quotes = {}

            for p in positions:
                seg = self._normalize_segment(p.get("exchangeSegment") or p.get("exchange"))
                if seg:
                    p["exchangeSegment"] = seg
                sid = str(p.get("securityId") or p.get("security_id"))
                quote = quotes.get(seg, {}).get(sid)
                ltp = None
                if isinstance(quote, dict):
                    ltp = quote.get("last_price") or quote.get("ltp")
                if ltp is None and p.get("lastTradedPrice") not in (None, ""):
                    ltp = p.get("lastTradedPrice")
                if ltp is not None:
                    try:
                        ltp_val = float(ltp)
                        p["last_price"] = ltp_val
                        p.setdefault("ltp", ltp_val)
                        buy_qty = float(p.get("buyQty") or p.get("buyqty") or 0)
                        sell_qty = float(p.get("sellQty") or p.get("sellqty") or 0)
                        net_qty = float(p.get("netQty") or p.get("netqty") or (buy_qty - sell_qty))
                        avg_buy = float(p.get("buyAvg") or p.get("buyavg") or 0)
                        avg_sell = float(p.get("sellAvg") or p.get("sellavg") or 0)
                        avg = avg_buy if net_qty > 0 else avg_sell
                        pnl = round((ltp_val - avg) * net_qty, 2)
                        p.setdefault("unrealizedProfit", pnl)
                        p.setdefault("profitAndLoss", pnl)
                    except Exception:
                        pass

            return {"status": "success", "data": positions}
        except requests.exceptions.Timeout:
            return {"status": "failure", "error": "Request to Dhan API timed out while fetching positions."}
        except requests.exceptions.RequestException as e:
            return {"status": "failure", "error": f"Failed to fetch positions from Dhan API: {str(e)}"}
        except json.JSONDecodeError:
            return {"status": "failure", "error": f"Invalid JSON response from Dhan API: {r.text}"}
        except Exception as e:
            return {"status": "failure", "error": f"An unexpected error occurred while fetching positions: {str(e)}"}

    def get_holdings(self):
        """Fetch portfolio holdings and enrich with LTP and P/L."""
        try:
            r = self._request(
                "get",
                f"{self.api_base}/holdings",
                headers=self.headers,
                timeout=self.timeout,
            )
            holdings = r.json()
            if isinstance(holdings, dict) and "data" in holdings:
                holdings = holdings.get("data")
            if not isinstance(holdings, list):
                holdings = []

            securities = {}
            for h in holdings:
                seg = self._normalize_segment(h.get("exchangeSegment") or h.get("exchange"))
                sid = h.get("securityId") or h.get("security_id")
                if seg and sid:
                    try:
                        securities.setdefault(seg, []).append(int(sid))
                    except Exception:
                        pass

            quotes = {}
            if securities:
                try:
                    qr = self._request(
                        "post",
                        f"{self.api_base}/marketfeed/ltp",
                        json=securities,
                        headers=self.headers,
                        timeout=self.timeout,
                    )
                    qdata = qr.json()
                    if isinstance(qdata, dict):
                        quotes = qdata.get("data", {})
                except Exception:
                    quotes = {}

            for h in holdings:
                seg = self._normalize_segment(h.get("exchangeSegment") or h.get("exchange"))
                sid = str(h.get("securityId") or h.get("security_id"))
                quote = quotes.get(seg, {}).get(sid)
                ltp = None
                if isinstance(quote, dict):
                    ltp = quote.get("last_price") or quote.get("ltp")

                if ltp is None and h.get("lastTradedPrice") not in (None, ""):
                    ltp = h.get("lastTradedPrice")

                if ltp is not None:
                    try:
                        ltp_val = float(ltp)
                        h["last_price"] = ltp_val
                        h.setdefault("ltp", ltp_val)
                        qty = float(h.get("availableQty") or h.get("totalQty") or 0)
                        avg = float(h.get("avgCostPrice") or 0)
                        pnl = round((ltp_val - avg) * qty, 2)
                        h["pnl"] = pnl
                        h.setdefault("unrealizedProfit", pnl)
                    except Exception:
                        pass

            return {"status": "success", "data": holdings}
        except requests.exceptions.Timeout:
            return {"status": "failure", "error": "Request to Dhan API timed out while fetching holdings."}
        except requests.exceptions.RequestException as e:
            return {"status": "failure", "error": "Failed to fetch holdings from Dhan API: {}".format(str(e))}
        except json.JSONDecodeError:
            return {"status": "failure", "error": "Invalid JSON response from Dhan API: {}".format(r.text)}
        except Exception as e:
            return {"status": "failure", "error": "An unexpected error occurred while fetching holdings: {}".format(str(e))}

    def get_profile(self):
        """
        Return profile or fund data to confirm account id.
        """
        try:
            r = self._request(
                "get",
                "{}/fundlimit".format(self.api_base),
                headers=self.headers,
                timeout=self.timeout,
            )
            return {"status": "success", "data": r.json()}
        except requests.exceptions.Timeout:
            return {"status": "failure", "error": "Request to Dhan API timed out while fetching profile."}
        except requests.exceptions.RequestException as e:
            return {"status": "failure", "error": "Failed to fetch profile from Dhan API: {}".format(str(e))}
        except json.JSONDecodeError:
            return {"status": "failure", "error": "Invalid JSON response from Dhan API: {}".format(r.text)}
        except Exception as e:
            return {"status": "failure", "error": "An unexpected error occurred while fetching profile: {}".format(str(e))}

    def check_token_valid(self):
        """
        Validate access token and ensure it belongs to this client_id.
        """
        if self._should_refresh_token():
            try:
                self.generate_session_token(force_refresh=True)
            except DhanAuthError as exc:
                logger.warning("Failed to refresh Dhan token: %s", exc)
                return False

        try:
            r = self._request(
                "get",
                "{}/fundlimit".format(self.api_base),
                headers=self.headers,
                timeout=5,
            )
            if r.status_code == 401 and self._has_login_material():
                try:
                    self.generate_session_token(force_refresh=True)
                except DhanAuthError as exc:
                    logger.warning("Dhan token refresh after 401 failed: %s", exc)
                    return False
                r = self._request(
                    "get",
                    "{}/fundlimit".format(self.api_base),
                    headers=self.headers,
                    timeout=5,
                )
            r.raise_for_status()
            data = r.json()
            cid = str(data.get("clientId") or data.get("dhanClientId") or "").strip()
            if cid and cid != str(self.client_id):
                return False
            return True
        except requests.exceptions.Timeout:
            return False
        except requests.exceptions.RequestException as exc:
            logger.debug("Dhan token validation request failed: %s", exc)
            return False
        except json.JSONDecodeError:
            return False
        except Exception as exc:
            logger.debug("Unexpected Dhan token validation error: %s", exc)
            return False

    def get_opening_balance(self):
        """
        Fetch available cash balance from fundlimit API.
        """
        try:
            r = self._request(
                "get",
                "{}/fundlimit".format(self.api_base),
                headers=self.headers,
                timeout=self.timeout,
            )
            data = r.json()
            for key in [
                "openingBalance",
                "netCashAvailable",
                "availableBalance",
                "availabelBalance",
                "withdrawableBalance",
                "availableAmount",
                "netCash",
            ]:
                if key in data:
                    return float(data[key])
            return float(data.get("cash", 0))
        except requests.exceptions.Timeout:
            return None
        except requests.exceptions.RequestException:
            return None
        except json.JSONDecodeError:
            return None
        except Exception:
            return None
