import os
import sys
import pytest
import tempfile

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

os.environ.setdefault('SECRET_KEY', 'test')
os.environ.setdefault('DATABASE_URL', 'sqlite:///:memory:')
os.environ.setdefault('ADMIN_EMAIL', 'a@a.com')
os.environ.setdefault('ADMIN_PASSWORD', 'pass')

import app as app_module

@pytest.fixture
def client():
    db_fd, db_path = tempfile.mkstemp()
    os.environ['DATABASE_URL'] = 'sqlite:///' + db_path
    from importlib import reload
    reload(app_module)
    local_app = app_module.app
    local_db = app_module.db
    User = app_module.User
    local_app.config['TESTING'] = True
    with local_app.app_context():
        local_db.create_all()
        if not local_db.session.query(User).filter_by(email='test@example.com').first():
            user = User(email='test@example.com')
            user.set_password('secret')
            local_db.session.add(user)
            local_db.session.commit()
        with local_app.test_client() as client:
            yield client
        local_db.session.remove()
        local_db.drop_all()
    os.close(db_fd)
    os.unlink(db_path)


def login(client):
    return client.post('/login', data={'email': 'test@example.com', 'password': 'secret'})


def test_account_endpoint_requires_auth(client):
    resp = client.get('/api/account')
    assert resp.status_code == 401
    login(client)
    resp = client.get('/api/account')
    assert resp.status_code in (200, 400)


def test_orders_endpoint_requires_auth(client):
    resp = client.get('/api/orders')
    assert resp.status_code == 401
    login(client)
    resp = client.get('/api/orders')
    assert resp.status_code in (200, 400, 500)


def test_portfolio_endpoint_requires_auth(client):
    resp = client.get('/api/portfolio')
    assert resp.status_code == 401
    login(client)
    resp = client.get('/api/portfolio')
    assert resp.status_code in (200, 400, 500)


def test_holdings_endpoint_requires_auth(client):
    resp = client.get('/api/holdings')
    assert resp.status_code == 401
    login(client)
    resp = client.get('/api/holdings')
    assert resp.status_code in (200, 400, 500)

def test_portfolio_parses_net_positions(client, monkeypatch):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    class DummyBroker:
        def __init__(self, *a, **k):
            pass
        def get_positions(self):
            return {"netPositions": [{"symbol": "ABC"}]}

    monkeypatch.setattr(app_module, 'broker_api', lambda acc: DummyBroker())

    with app.app_context():
        user = User.query.filter_by(email='test@example.com').first()
        acc = Account(user_id=user.id, broker='fyers', client_id='F1', credentials={'access_token': 'x'})
        db.session.add(acc)
        db.session.commit()

    resp = client.get('/api/portfolio/F1')
    assert resp.status_code == 200
    assert resp.get_json() == [{"symbol": "ABC"}]


def test_active_children_scoped_to_user(client):
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account
    with app.app_context():
        user1 = User.query.filter_by(email='test@example.com').first()
        user2 = User(email='other@example.com')
        user2.set_password('x')
        db.session.add(user2)
        db.session.commit()
        master = Account(user_id=user1.id, role='master', client_id='M1')
        c1 = Account(user_id=user1.id, role='child', client_id='C1', linked_master_id='M1', copy_status='On')
        c2 = Account(user_id=user2.id, role='child', client_id='C2', linked_master_id='M1', copy_status='On')
        db.session.add_all([master, c1, c2])
        db.session.commit()

        from helpers import active_children_for_master

        children = active_children_for_master(master)
        ids = {c.client_id for c in children}
        assert ids == {'C1'}

def test_active_children_case_insensitive_status(client):
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account
    with app.app_context():
        user = User.query.filter_by(email='test@example.com').first()
        master = Account(user_id=user.id, role='master', client_id='M2')
        c1 = Account(user_id=user.id, role='child', client_id='C3', linked_master_id='M2', copy_status='ON')
        db.session.add_all([master, c1])
        db.session.commit()

        from helpers import active_children_for_master

        children = active_children_for_master(master)
        ids = {c.client_id for c in children}
        assert ids == {'C3'}


def test_order_mappings_endpoint_requires_auth(client):
    resp = client.get('/api/order-mappings')
    assert resp.status_code == 401
    login(client)
    resp = client.get('/api/order-mappings')
    assert resp.status_code == 200


def test_child_orders_endpoint_requires_auth(client):
    resp = client.get('/api/child-orders')
    assert resp.status_code == 401
    login(client)
    resp = client.get('/api/child-orders')
    assert resp.status_code == 200

def test_save_account_persists_username(client):
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account
    with app.app_context():
        user = User.query.filter_by(email='test@example.com').first()
        data = {
            'broker': 'fyers',
            'client_id': 'F1',
            'username': 'demo',
            'credentials': {'access_token': 'x'}
        }
        app_module.save_account_to_user(user.email, data)
        acc = Account.query.filter_by(client_id='F1', user_id=user.id).first()
        assert acc.username == 'demo'

def test_poll_and_copy_trades_cross_broker(client, monkeypatch):
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    import brokers

    class DummyMasterBroker(brokers.base.BrokerBase):
        def place_order(self, *a, **k):
            pass
        def get_order_list(self):
            return [{
                'orderId': '1',
                'status': 'COMPLETE',
                'filledQuantity': 1,
                'price': 100,
                'tradingSymbol': 'TESTSYM',
                'transactionType': 'BUY'
            }]
        def get_positions(self):
            return []
        def cancel_order(self, order_id):
            pass

    placed = []
    class DummyChildBroker(brokers.base.BrokerBase):
        def place_order(self, **kwargs):
            placed.append(kwargs)
            return {'status': 'success', 'order_id': 'child1'}
        def get_order_list(self):
            return []
        def get_positions(self):
            return []
        def cancel_order(self, order_id):
            pass

    def fake_get_broker_class(name):
        if name == 'master_broker':
            return DummyMasterBroker
        return DummyChildBroker

    monkeypatch.setattr(brokers.factory, 'get_broker_class', fake_get_broker_class)
    monkeypatch.setattr(app_module, 'get_broker_class', fake_get_broker_class)
    assert app_module.poll_and_copy_trades.__globals__['get_broker_class'] is fake_get_broker_class
    monkeypatch.setattr(app_module, 'save_log', lambda *a, **k: None)
    monkeypatch.setattr(app_module, 'save_order_mapping', lambda *a, **k: None)
    monkeypatch.setattr(app_module, 'record_trade', lambda *a, **k: None)

    with app.app_context():
        user = User.query.filter_by(email='test@example.com').first()
        master = Account(user_id=user.id, role='master', broker='master_broker', client_id='M', credentials={'access_token': 'x'})
        child = Account(user_id=user.id, role='child', broker='child_broker', client_id='C', linked_master_id='M', copy_status='On', credentials={'access_token': 'y'}, last_copied_trade_id='0')
        db.session.add_all([master, child])
        db.session.commit()

        monkeypatch.setitem(brokers.symbol_map.SYMBOL_MAP, 'TESTSYM', {
            'master_broker': {'trading_symbol': 'TESTSYM'},
            'child_broker': {'tradingsymbol': 'TESTSYM'}
        })

        app_module.poll_and_copy_trades()

        assert placed
        db.session.refresh(child)
        assert child.last_copied_trade_id == '1'

def test_poll_and_copy_trades_token_lookup(client, monkeypatch):
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    import brokers

    placed = []

    class DummyZerodha(brokers.base.BrokerBase):
        def __init__(self, *a, **k):
            pass

        def place_order(self, **kwargs):
            placed.append(kwargs)
            return {'status': 'success', 'order_id': 'child1'}

        def get_order_list(self):
            return [{
                'orderId': '2',
                'status': 'COMPLETE',
                'filledQuantity': 1,
                'price': 50,
                'transactionType': 'BUY',
                'instrument_token': '926241'
            }]

        def get_positions(self):
            return []

        def cancel_order(self, order_id):
            pass

    def fake_get_broker_class(name):
        return DummyZerodha

    monkeypatch.setattr(brokers.factory, 'get_broker_class', fake_get_broker_class)
    monkeypatch.setattr(app_module, 'get_broker_class', fake_get_broker_class)
    monkeypatch.setattr(app_module, 'save_log', lambda *a, **k: None)
    monkeypatch.setattr(app_module, 'save_order_mapping', lambda *a, **k: None)
    monkeypatch.setattr(app_module, 'record_trade', lambda *a, **k: None)

    with app.app_context():
        user = User.query.filter_by(email='test@example.com').first()
        master = Account(user_id=user.id, role='master', broker='zerodha', client_id='ZM', credentials={'access_token': 'x'})
        child = Account(user_id=user.id, role='child', broker='zerodha', client_id='ZC', linked_master_id='ZM', copy_status='On', credentials={'access_token': 'y'}, last_copied_trade_id='0')
        db.session.add_all([master, child])
        db.session.commit()

        monkeypatch.setitem(brokers.symbol_map.SYMBOL_MAP, 'IDEA', {
            'zerodha': {'trading_symbol': 'IDEA', 'token': '926241'}
        })

        app_module.poll_and_copy_trades()

        assert placed
        db.session.refresh(child)
        assert child.last_copied_trade_id == '2'
        

def test_opening_balance_cache(monkeypatch):
    app = app_module.app

    class DummyBroker:
        def get_opening_balance(self):
            calls.append(1)
            return 42

    calls = []
    monkeypatch.setattr(app_module, 'broker_api', lambda acc: DummyBroker())
    app_module.OPENING_BALANCE_CACHE.clear()

    acc = {'client_id': 'A1', 'broker': 'dummy', 'credentials': {}}

    bal1 = app_module.get_opening_balance_for_account(acc)
    bal2 = app_module.get_opening_balance_for_account(acc)

    assert bal1 == 42
    assert bal2 == 42
    assert len(calls) == 1

def test_add_account_stores_all_credentials(client, monkeypatch):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    class DummyBroker:
        def __init__(self, *a, **k):
            self.access_token = 'dummy_token'
        def check_token_valid(self):
            return True

    monkeypatch.setattr(app_module, 'get_broker_class', lambda name: DummyBroker)

    data = {
        'broker': 'finvasia',
        'client_id': 'FIN123',
        'username': 'finuser',
        'password': 'p',
        'totp_secret': 't',
        'vendor_code': 'v',
        'api_key': 'a',
        'imei': 'i'
    }

    resp = client.post('/api/add-account', json=data)
    assert resp.status_code == 200

    with app.app_context():
        user = User.query.filter_by(email='test@example.com').first()
        acc = Account.query.filter_by(user_id=user.id, client_id='FIN123').first()
        assert acc is not None
        expected = {
            'password': 'p',
            'totp_secret': 't',
            'vendor_code': 'v',
            'api_key': 'a',
            'imei': 'i'
        }
        assert acc.credentials == expected


def test_reconnect_uses_stored_credentials(client, monkeypatch):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    captured = {}
    class DummyBroker:
        def __init__(self, *a, **k):
            captured['args'] = a
            captured['kwargs'] = k
            self.access_token = 'newtoken'
        def check_token_valid(self):
            return True

    monkeypatch.setattr(app_module, 'get_broker_class', lambda name: DummyBroker)

    data = {
        'broker': 'finvasia',
        'client_id': 'FIN124',
        'username': 'finuser',
        'password': 'p',
        'totp_secret': 't',
        'vendor_code': 'v',
        'api_key': 'a',
        'imei': 'i'
    }

    resp = client.post('/api/add-account', json=data)
    assert resp.status_code == 200

    with app.app_context():
        user = User.query.filter_by(email='test@example.com').first()
        acc = Account.query.filter_by(user_id=user.id, client_id='FIN124').first()
        acc.status = 'Failed'
        db.session.commit()

    resp = client.post('/api/reconnect-account', json={'client_id': 'FIN124'})
    assert resp.status_code == 200

    with app.app_context():
        user = User.query.filter_by(email='test@example.com').first()
        acc = Account.query.filter_by(user_id=user.id, client_id='FIN124').first()
        assert acc.status == 'Connected'
        assert acc.credentials['access_token'] == 'newtoken'
        assert captured['kwargs']['password'] == 'p'
        assert captured['kwargs']['totp_secret'] == 't'
        assert captured['kwargs']['vendor_code'] == 'v'
        assert captured['kwargs']['api_key'] == 'a'
        assert captured['kwargs']['imei'] == 'i'
        count = Account.query.filter_by(user_id=user.id, client_id='FIN124').count()
        assert count == 1
