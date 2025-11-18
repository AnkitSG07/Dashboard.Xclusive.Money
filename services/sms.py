"""MSG91 SMS helper for OTP verification."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import requests

DEFAULT_BASE_URL = "https://api.msg91.com/api/v5/otp"


class MSG91Error(RuntimeError):
    """Raised when MSG91 returns an error or configuration is invalid."""


@dataclass
class VerificationResponse:
    """Normalized response for OTP verification checks."""

    status: str
    raw: Any



def _get_config() -> tuple[str, str, str | None, str]:
    """Return the MSG91 configuration required for OTP flows.

    Raises
    ------
    MSG91Error
        If any required environment variable is missing.
    """

    auth_key = os.getenv("MSG91_AUTH_KEY")
    template_id = os.getenv("MSG91_TEMPLATE_ID")
    sender_id = os.getenv("MSG91_SENDER_ID")
    base_url = os.getenv("MSG91_BASE_URL", DEFAULT_BASE_URL).rstrip("/")

    missing: list[str] = []
    if not auth_key:
        missing.append("MSG91_AUTH_KEY")
    if not template_id:
        missing.append("MSG91_TEMPLATE_ID")

    if missing:
        raise MSG91Error(f"Missing MSG91 configuration: {', '.join(sorted(missing))}")

    return auth_key, template_id, sender_id, base_url



def _post(url: str, payload: dict[str, Any], auth_key: str) -> dict[str, Any]:
    """Send a POST request to MSG91 with standard headers and error handling."""

    try:
        response = requests.post(
            url,
            json=payload,
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "authkey": auth_key,
            },
            timeout=10,
        )
    except requests.RequestException as exc:  # pragma: no cover - network dependent
        raise MSG91Error(f"Failed to contact MSG91: {exc}") from exc

    if response.status_code >= 400:
        try:
            detail = response.json()
        except ValueError:
            detail = response.text
        raise MSG91Error(f"MSG91 error ({response.status_code}): {detail}")

    try:
        return response.json()
    except ValueError as exc:
        raise MSG91Error("MSG91 returned a non-JSON response.") from exc



def send_otp(phone: str) -> VerificationResponse:
    """Send an OTP via MSG91 to ``phone`` in E.164 format."""

    auth_key, template_id, sender_id, base_url = _get_config()

    payload: dict[str, Any] = {
        "mobile": phone,
        "template_id": template_id,
    }
    if sender_id:
        payload["sender"] = sender_id

    data = _post(base_url, payload, auth_key)
    status = "pending"
    if isinstance(data, dict) and data.get("type") == "success":
        status = "pending"

    return VerificationResponse(status=status, raw=data)



def check_otp(phone: str, code: str) -> VerificationResponse:
    """Validate a submitted OTP against MSG91."""

    auth_key, _, _, base_url = _get_config()

    payload = {
        "mobile": phone,
        "otp": code,
    }

    data = _post(f"{base_url}/verify", payload, auth_key)
    status = "approved" if isinstance(data, dict) and data.get("type") == "success" else "failed"

    return VerificationResponse(status=status, raw=data)
