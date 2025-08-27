#!/usr/bin/env python3
"""Backfill Redis user settings for existing users.

This migration script populates ``user_settings:{user_id}`` in Redis for
all users that currently have broker credentials stored in the database.
The resulting settings follow the structure used by ``alert_guard``::

    {
        "brokers": [
            {"name": "broker", "client_id": "id", "access_token": "token", ...}
        ]
    }

Run this once in production to serialize existing credentials into the
settings store so that downstream services can access them efficiently.
"""

from __future__ import annotations

from typing import Any, Dict, List

from models import Account, User
from services import alert_guard
from services.db import get_session


def _broker_config(account: Account) -> Dict[str, Any]:
    """Return a broker configuration dict for *account*.

    Accounts without an ``access_token`` are skipped.
    """

    creds = account.credentials or {}
    access_token = creds.get("access_token")
    if not access_token:
        return {}

    cfg: Dict[str, Any] = {
        "name": account.broker,
        "client_id": account.client_id,
        "access_token": access_token,
    }

    extras = creds.get("extras")
    if extras is None:
        extras = {k: v for k, v in creds.items() if k not in {"access_token", "client_id"}}
    if extras:
        cfg["extras"] = extras
    return cfg


def main() -> None:
    """Populate Redis user settings for all users with broker accounts."""

    session = get_session()
    try:
        users: List[User] = session.query(User).all()
        for user in users:
            brokers = []
            for account in user.accounts:
                cfg = _broker_config(account)
                if cfg:
                    brokers.append(cfg)
            if brokers:
                settings = {"brokers": brokers}
                alert_guard.update_user_settings(user.id, settings)
    finally:
        session.close()


if __name__ == "__main__":  # pragma: no cover - simple script
    main()
