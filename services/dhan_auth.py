"""Helpers for interacting with the Dhan consent based login flow.

This module mirrors the reference script provided by Dhan for
manual authentication. It exposes high level helpers that the web
application can use to guide the login flow as well as to renew
access tokens without forcing a full browser login each day.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional
import logging

import requests

LOGGER = logging.getLogger(__name__)

AUTH_BASE_URL = "https://auth.dhan.co"
API_BASE_URL = "https://api.dhan.co/v2"
LOGIN_CONSENT_URL = f"{AUTH_BASE_URL}/app/generate-consent"
CONSUME_CONSENT_URL = f"{AUTH_BASE_URL}/app/consumeApp-consent"
TOKEN_RENEW_URL = f"{API_BASE_URL}/RenewToken"
PROFILE_URL = f"{API_BASE_URL}/profile"
BROWSER_LOGIN_URL = "https://auth.dhan.co/login/consentApp-login"


class DhanAuthError(RuntimeError):
    """Raised when any step of the Dhan authentication flow fails."""


@dataclass
class ConsentResult:
    consent_app_id: str
    status: Optional[str] = None

    @property
    def login_url(self) -> str:
        """Return the browser login URL for the consent session."""
        return f"{BROWSER_LOGIN_URL}?consentAppId={self.consent_app_id}"


def _auth_headers(api_key: str, api_secret: str) -> Dict[str, str]:
    return {
        "app_id": api_key,
        "app_secret": api_secret,
        "Content-Type": "application/json",
    }


def generate_consent(
    *,
    client_id: str,
    api_key: str,
    api_secret: str,
    auth_base: str = AUTH_BASE_URL,
    timeout: float = 10.0,
) -> ConsentResult:
    """Call Dhan's ``generate-consent`` endpoint and return the consent id."""
    headers = _auth_headers(api_key, api_secret)
    params = {"client_id": client_id}

    try:
        response = requests.post(
            f"{auth_base.rstrip('/')}/app/generate-consent",
            headers=headers,
            params=params,
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.exceptions.HTTPError as exc:
        message = f"Failed to generate Dhan consent: {exc}"
        LOGGER.warning(message)
        raise DhanAuthError(message) from exc
    except requests.exceptions.RequestException as exc:
        message = f"Request error while generating Dhan consent: {exc}"
        LOGGER.warning(message)
        raise DhanAuthError(message) from exc
    except ValueError as exc:
        message = "Invalid JSON payload returned from Dhan consent API"
        LOGGER.warning(message)
        raise DhanAuthError(message) from exc

    consent_app_id = payload.get("consentAppId") or payload.get("consent_app_id")
    if not consent_app_id:
        raise DhanAuthError("Consent API response missing consentAppId")

    status = payload.get("consentAppStatus") or payload.get("status")
    return ConsentResult(str(consent_app_id), status=status)


def consume_consent(
    *,
    token_id: str,
    api_key: str,
    api_secret: str,
    auth_base: str = AUTH_BASE_URL,
    timeout: float = 10.0,
) -> Dict[str, Any]:
    """Exchange a browser supplied ``token_id`` for an access token."""
    if not token_id:
        raise DhanAuthError("token_id is required to consume consent")

    headers = _auth_headers(api_key, api_secret)
    params = {"tokenId": token_id}

    try:
        response = requests.get(
            f"{auth_base.rstrip('/')}/app/consumeApp-consent",
            headers=headers,
            params=params,
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.exceptions.HTTPError as exc:
        message = f"Failed to consume Dhan consent: {exc}"
        LOGGER.warning(message)
        raise DhanAuthError(message) from exc
    except requests.exceptions.RequestException as exc:
        message = f"Request error while consuming Dhan consent: {exc}"
        LOGGER.warning(message)
        raise DhanAuthError(message) from exc
    except ValueError as exc:
        message = "Invalid JSON payload returned from consent consumption"
        LOGGER.warning(message)
        raise DhanAuthError(message) from exc

    if not payload.get("accessToken") and not payload.get("access_token"):
        raise DhanAuthError("Consume consent response missing accessToken")

    return payload


def renew_token(
    *,
    access_token: str,
    client_id: str,
    api_base: str = API_BASE_URL,
    timeout: float = 10.0,
) -> Dict[str, Any]:
    """Programmatically renew an existing access token."""
    token = str(access_token).strip()
    if not token:
        raise DhanAuthError("Existing access token required for renewal")

    client_identifier = str(client_id).strip()
    if not client_identifier:
        raise DhanAuthError("Client id required for token renewal")

    headers = {
        "access-token": token,
        "dhanClientId": client_identifier,
        "Content-Type": "application/json",
    }
    
    # FIX: Removed the request_payload and the 'json' argument from requests.post
    # The RenewToken API relies solely on headers (access-token and dhanClientId)
    try:
        response = requests.post(
            f"{api_base.rstrip('/')}/RenewToken",
            headers=headers,
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.exceptions.HTTPError as exc:
        message = f"Failed to renew Dhan token: {exc}"
        LOGGER.warning(message)
        raise DhanAuthError(message) from exc
    except requests.exceptions.RequestException as exc:
        message = f"Request error while renewing Dhan token: {exc}"
        LOGGER.warning(message)
        raise DhanAuthError(message) from exc
    except ValueError as exc:
        message = "Invalid JSON payload returned from token renewal"
        LOGGER.warning(message)
        raise DhanAuthError(message) from exc

    if not payload.get("accessToken") and not payload.get("access_token"):
        raise DhanAuthError("Renew token response missing accessToken")

    return payload


def verify_token(
    *,
    access_token: str,
    api_base: str = API_BASE_URL,
    timeout: float = 10.0,
) -> Dict[str, Any]:
    """Verify a token by calling the Dhan profile endpoint."""
    if not access_token:
        raise DhanAuthError("Access token required to verify Dhan profile")

    headers = {"access-token": access_token}

    try:
        response = requests.get(
            f"{api_base.rstrip('/')}/profile",
            headers=headers,
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.exceptions.HTTPError as exc:
        message = f"Failed to verify Dhan token: {exc}"
        LOGGER.warning(message)
        raise DhanAuthError(message) from exc
    except requests.exceptions.RequestException as exc:
        message = f"Request error while verifying Dhan token: {exc}"
        LOGGER.warning(message)
        raise DhanAuthError(message) from exc
    except ValueError as exc:
        message = "Invalid JSON payload returned from Dhan profile API"
        LOGGER.warning(message)
        raise DhanAuthError(message) from exc

    return payload


def parse_expiry(value: Any) -> Optional[str]:
    """Normalise expiry timestamps returned by Dhan to ISO strings."""
    if not value:
        return None
    if isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value)
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    return dt.isoformat()
