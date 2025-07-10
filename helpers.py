from flask import session
from sqlalchemy import or_
from models import User, OrderMapping, Account, d


def current_user():
    """Return the logged in ``User`` instance or ``None``."""
    email = session.get("user")
    if not email:
        return None
    return User.query.filter_by(email=email).first()


def user_account_ids(user):
    return [acc.client_id for acc in user.accounts]


def get_primary_account(user):
    """Return the first account for a user, if any."""
    if user.accounts:
        return user.accounts[0]
    return None


def order_mappings_for_user(user):
    ids = user_account_ids(user)
    return OrderMapping.query.filter(
        or_(OrderMapping.master_client_id.in_(ids),
            OrderMapping.child_client_id.in_(ids))
    )


def active_children_for_master(master):
    """Return active child accounts belonging to the same user as ``master``."""
    return Account.query.filter(
        Account.user_id == master.user_id,
        Account.role == 'child',
        Account.linked_master_id == master.client_id,
        db.func.lower(Account.copy_status) == 'on'
    ).all()

