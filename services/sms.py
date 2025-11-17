"""Twilio SMS helper for OTP verification."""
import os
from typing import Any

from twilio.rest import Client


def _get_config() -> tuple[Client, str]:
    """Return an initialized Twilio client and Verify SID.

    Raises
    ------
    RuntimeError
        If any of the required environment variables are missing.
    """

    account_sid = os.getenv("TWILIO_ACCOUNT_SID")
    auth_token = os.getenv("TWILIO_AUTH_TOKEN")
    verify_sid = os.getenv("TWILIO_VERIFY_SID")

    missing: list[str] = []
    if not account_sid:
        missing.append("TWILIO_ACCOUNT_SID")
    if not auth_token:
        missing.append("TWILIO_AUTH_TOKEN")
    if not verify_sid:
        missing.append("TWILIO_VERIFY_SID")

    if missing:
        raise RuntimeError(
            f"Missing Twilio configuration: {', '.join(sorted(missing))}"
        )

    client = Client(account_sid, auth_token)
    return client, verify_sid  # type: ignore[return-value]


def send_otp(phone: str) -> Any:
    """Send an OTP via SMS to ``phone``.

    Parameters
    ----------
    phone:
        The destination phone number in E.164 format.

    Returns
    -------
    Any
        The Twilio verification creation response.
    """

    client, verify_sid = _get_config()
    return client.verify.v2.services(verify_sid).verifications.create(
        to=phone, channel="sms"
    )


def check_otp(phone: str, code: str) -> Any:
    """Validate a submitted OTP against Twilio Verify.

    Parameters
    ----------
    phone:
        The destination phone number used when sending the OTP.
    code:
        The code provided by the user.

    Returns
    -------
    Any
        The Twilio verification check response.
    """

    client, verify_sid = _get_config()
    return client.verify.v2.services(verify_sid).verification_checks.create(
        to=phone, code=code
    )
