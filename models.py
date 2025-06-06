from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash


db = SQLAlchemy()

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
    name = db.Column(db.String(120))
    phone = db.Column(db.String(20))
    plan = db.Column(db.String(20), default='Free')
    last_login = db.Column(db.String(32))
    subscription_start = db.Column(db.String(32))
    subscription_end = db.Column(db.String(32))
    payment_status = db.Column(db.String(32))
    is_admin = db.Column(db.Boolean, default=False)

    def set_password(self, password: str):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

class Account(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    broker = db.Column(db.String(50))
    client_id = db.Column(db.String(50))
    token_expiry = db.Column(db.String(32))
    status = db.Column(db.String(20))
    user = db.relationship('User', backref='accounts')

class Trade(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    symbol = db.Column(db.String(50))
    action = db.Column(db.String(10))
    qty = db.Column(db.Integer)
    price = db.Column(db.Float)
    status = db.Column(db.String(20))
    timestamp = db.Column(db.String(32))
    user = db.relationship('User', backref='trades')

class WebhookLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    status = db.Column(db.Integer)
    time = db.Column(db.String(32))
    reason = db.Column(db.String(255))

class SystemLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    type = db.Column(db.String(50))
    time = db.Column(db.String(32))
    details = db.Column(db.String(255))

class Setting(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(100), unique=True)
    value = db.Column(db.String(255))
