from flask import Blueprint, jsonify, session, request
from helpers import (
    current_user,
    get_primary_account,
    order_mappings_for_user,
    normalize_position,
)
from models import Account
from dhanhq import dhanhq
from functools import wraps

api_bp = Blueprint('api', __name__, url_prefix='/api')


def login_required_api(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get('user'):
            return jsonify({'error': 'Unauthorized'}), 401
        return fn(*args, **kwargs)
    return wrapper


@api_bp.route('/portfolio', defaults={'client_id': None})
@api_bp.route('/portfolio/<client_id>')
@login_required_api
def portfolio(client_id=None):
    """Return positions for the requested account."""
    user = current_user()
    if client_id:
        account = Account.query.filter_by(user_id=user.id, client_id=client_id).first()
    else:
        account = get_primary_account(user)
    if not account or not account.credentials:
        return jsonify({'error': 'Account not configured'}), 400

    from app import broker_api, _account_to_dict
    api = broker_api(_account_to_dict(account))
    try:
        resp = api.get_positions()
        if isinstance(resp, dict):
            data = (
                resp.get('data')
                or resp.get('positions')
                or resp.get('net')
                or resp.get('netPositions')
                or resp.get('net_positions')
                or []
            )
        else:
            data = resp or []

        if isinstance(data, dict):
            data = (
                data.get('data')
                or data.get('positions')
                or data.get('net')
                or data.get('netPositions')
                or data.get('net_positions')
                or []
            )

        if not isinstance(data, list):
            data = []

        standardized = [normalize_position(p, account.broker) for p in data]
        standardized = [p for p in standardized if p]
        return jsonify(standardized)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@api_bp.route('/orders', defaults={'client_id': None})
@api_bp.route('/orders/<client_id>')
@login_required_api
def orders(client_id=None):
    """Return order list for the requested account."""
    user = current_user()
    if client_id:
        account = Account.query.filter_by(user_id=user.id, client_id=client_id).first()
    else:
        account = get_primary_account(user)
    if not account or not account.credentials:
        return jsonify({'error': 'Account not configured'}), 400

    from app import broker_api, _account_to_dict
    api = broker_api(_account_to_dict(account))
    try:
        resp = api.get_order_list()
        orders = []
        if isinstance(resp, dict):
            orders = resp.get('data') or resp.get('orders') or []
        elif isinstance(resp, list):
            orders = resp
        return jsonify(orders)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@api_bp.route('/holdings', defaults={'client_id': None})
@api_bp.route('/holdings/<client_id>')
@login_required_api
def holdings(client_id=None):
    """Return holdings for the requested account if supported."""
    user = current_user()
    if client_id:
        account = Account.query.filter_by(user_id=user.id, client_id=client_id).first()
    else:
        account = get_primary_account(user)
    if not account or not account.credentials:
        return jsonify({'error': 'Account not configured'}), 400

    from app import broker_api, _account_to_dict
    api = broker_api(_account_to_dict(account))

    if not hasattr(api, 'get_holdings'):
        return jsonify({'error': 'Holdings not supported for this broker'}), 400

    try:
        resp = api.get_holdings()
        if isinstance(resp, dict):
            data = resp.get('data') or resp.get('holdings') or []
        else:
            data = resp or []
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@api_bp.route('/account')
@login_required_api
def account_stats():
    user = current_user()
    account = get_primary_account(user)
    if not account or not account.credentials:
        return jsonify({'error': 'Account not configured'}), 400

    dhan = dhanhq(account.client_id, account.credentials.get('access_token'))
    try:
        stats_resp = dhan.get_fund_limits()
        if not isinstance(stats_resp, dict) or 'data' not in stats_resp:
            return jsonify({'error': 'Unexpected response format', 'details': stats_resp}), 500
        stats = stats_resp['data']
        mapped_stats = {
            'total_funds': stats.get('availabelBalance', 0),
            'available_margin': stats.get('withdrawableBalance', 0),
            'used_margin': stats.get('utilizedAmount', 0)
        }
        return jsonify(mapped_stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@api_bp.route('/order-mappings')
@login_required_api
def order_mappings():
    user = current_user()
    mappings = [
        {
            'master_order_id': m.master_order_id,
            'child_order_id': m.child_order_id,
            'master_client_id': m.master_client_id,
            'master_broker': m.master_broker,
            'child_client_id': m.child_client_id,
            'child_broker': m.child_broker,
            'symbol': m.symbol,
            'status': m.status,
            'timestamp': m.timestamp,
            'child_timestamp': m.child_timestamp,
            'remarks': m.remarks,
            'multiplier': m.multiplier,
        }
        for m in order_mappings_for_user(user).all()
    ]
    return jsonify(mappings)


@api_bp.route('/child-orders')
@login_required_api
def child_orders():
    user = current_user()
    master_order_id = request.args.get('master_order_id')
    mappings = order_mappings_for_user(user)
    if master_order_id:
        mappings = mappings.filter_by(master_order_id=master_order_id)
    data = [
        {
            'master_order_id': m.master_order_id,
            'child_order_id': m.child_order_id,
            'master_client_id': m.master_client_id,
            'master_broker': m.master_broker,
            'child_client_id': m.child_client_id,
            'child_broker': m.child_broker,
            'symbol': m.symbol,
            'status': m.status,
            'timestamp': m.timestamp,
            'child_timestamp': m.child_timestamp,
            'remarks': m.remarks,
            'multiplier': m.multiplier,
        }
        for m in mappings.all()
    ]
    return jsonify(data)
