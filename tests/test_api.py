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
