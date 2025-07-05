from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash

db = SQLAlchemy()

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(256), nullable=False)
    name = db.Column(db.String(120))
    phone = db.Column(db.String(20))
    webhook_token = db.Column(db.String(64), unique=True)
    profile_image = db.Column(db.String(120))
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
    username = db.Column(db.String(120))  # Add this field
    token_expiry = db.Column(db.String(32))
    status = db.Column(db.String(20), default='Connected')
    role = db.Column(db.String(20))  # 'master', 'child', or None
    linked_master_id = db.Column(db.String(50))  # For child accounts
    copy_status = db.Column(db.String(10), default="Off")  # 'On' or 'Off'
    multiplier = db.Column(db.Float, default=1.0)
    credentials = db.Column(db.JSON)
    last_copied_trade_id = db.Column(db.String(50))  # Individual marker per account
    auto_login = db.Column(db.Boolean, default=True)  # Add this field
    last_login_time = db.Column(db.String(32))  # Add this field
    device_number = db.Column(db.String(64))  # For AliceBlue
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

# Group system for bulk operations
group_members = db.Table(
    "group_members",
    db.Column("group_id", db.Integer, db.ForeignKey("group.id")),
    db.Column("account_id", db.Integer, db.ForeignKey("account.id")),
)

class Group(db.Model):
    """Collection of accounts owned by a user."""
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"))
    name = db.Column(db.String(120))
    user = db.relationship("User", backref="groups")
    accounts = db.relationship(
        "Account", secondary=group_members, backref="groups", lazy="dynamic"
    )

class OrderMapping(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    master_order_id = db.Column(db.String(50))
    child_order_id = db.Column(db.String(50))
    master_client_id = db.Column(db.String(50))
    master_broker = db.Column(db.String(50))
    child_client_id = db.Column(db.String(50))
    child_broker = db.Column(db.String(50))
    symbol = db.Column(db.String(50))
    status = db.Column(db.String(20))
    timestamp = db.Column(db.String(32))
    child_timestamp = db.Column(db.String(32))
    remarks = db.Column(db.String(255))
    multiplier = db.Column(db.Float, default=1.0)
    action = db.Column(db.String(10))  # Add this field
    quantity = db.Column(db.Integer)   # Add this field

class TradeLog(db.Model):
    """Log entry for trade actions and responses."""
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.String(32))
    user_id = db.Column(db.String(50))
    symbol = db.Column(db.String(50))
    action = db.Column(db.String(10))
    quantity = db.Column(db.Integer)
    status = db.Column(db.String(20))
    response = db.Column(db.Text)
