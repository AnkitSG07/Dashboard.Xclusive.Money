from flask import Blueprint, jsonify, session, request
from helpers import current_user, get_primary_account, order_mappings_for_user
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


@api_bp.route('/portfolio')
@login_required_api
def portfolio():
    user = current_user()
    account = get_primary_account(user)
    if not account or not account.credentials:
        return jsonify({'error': 'Account not configured'}), 400

    client_id = account.client_id
    access_token = account.credentials.get('access_token')
    dhan = dhanhq(client_id, access_token)
    try:
        resp = dhan.get_positions()
        data = resp.get('data') or resp.get('positions') or []
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@api_bp.route('/orders')
@login_required_api
def orders():
    user = current_user()
    account = get_primary_account(user)
    if not account or not account.credentials:
        return jsonify({'error': 'Account not configured'}), 400

    dhan = dhanhq(account.client_id, account.credentials.get('access_token'))
    try:
        resp = dhan.get_order_list()
        if not isinstance(resp, dict) or 'data' not in resp:
            return jsonify({'error': 'Unexpected response format', 'details': resp}), 500
        orders = resp['data']
        return jsonify(orders)
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
