from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime
from sqlalchemy.orm import backref

db = SQLAlchemy()

class User(db.Model):
    __tablename__ = "user"
    
    # CORRECTED: Changed from Integer to String(36) for UUID

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(128), nullable=True)
    name = db.Column(db.String(120))
    phone = db.Column(db.String(20))
    webhook_token = db.Column(db.String(64), unique=True, index=True)
    # Store the profile image as a data URL so it persists in the database
    # rather than relying on the local filesystem which may be wiped on
    # redeploy. Existing deployments with filenames will continue to work.
    profile_image = db.Column(db.Text)
    plan = db.Column(db.String(20), default="Free")
    
    last_login = db.Column(db.DateTime)
    subscription_start = db.Column(db.DateTime)
    subscription_end = db.Column(db.DateTime)
    payment_status = db.Column(db.String(32))
    is_admin = db.Column(db.Boolean, default=False)
    
    # created_at = db.Column(db.DateTime, default=datetime.utcnow)
    # updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def set_password(self, password: str):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        if not self.password_hash:
            return False
        return check_password_hash(self.password_hash, password)

    def __repr__(self):
        return f"<User {self.email}>"

class Account(db.Model):
    __tablename__ = "account"
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    broker = db.Column(db.String(50), index=True)
    client_id = db.Column(db.String(50), nullable=False, index=True)
    
    username = db.Column(db.String(100))
    auto_login = db.Column(db.Boolean, default=True)
    last_login_time = db.Column(db.DateTime)
    device_number = db.Column(db.String(50))
    
    token_expiry = db.Column(db.DateTime)
    status = db.Column(db.String(20), default="Pending")
    role = db.Column(db.String(20), index=True)
    linked_master_id = db.Column(db.String(50), index=True)
    copy_status = db.Column(db.String(10), default="Off", index=True)
    multiplier = db.Column(db.Float, default=1.0)
    credentials = db.Column(db.JSON)
    last_copied_trade_id = db.Column(db.String(50))
    
    # created_at = db.Column(db.DateTime, default=datetime.utcnow)
    # updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    user = db.relationship("User", backref=db.backref("accounts", lazy=True, cascade="all, delete-orphan"))
    
    __table_args__ = (
        db.Index("idx_user_client", "user_id", "client_id"),
        db.Index("idx_role_status", "role", "copy_status"),
        db.Index("idx_master_children", "linked_master_id", "role"),
        db.UniqueConstraint("user_id", "client_id", name="uq_user_client"),
    )

    def __repr__(self):
        return f"<Account {self.client_id} - {self.broker}>"

class Trade(db.Model):
    __tablename__ = "trade"
    
    # CORRECTED: Changed from Integer to String(36) for UUID

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    symbol = db.Column(db.String(50), index=True)
    action = db.Column(db.String(10))
    qty = db.Column(db.Integer)
    price = db.Column(db.Float)
    status = db.Column(db.String(20), index=True)
    
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    
    broker = db.Column(db.String(50))
    order_id = db.Column(db.String(50))
    client_id = db.Column(db.String(50))
    
    user = db.relationship("User", backref=db.backref("trades", lazy=True))
    
    __table_args__ = (
        db.Index("idx_user_timestamp", "user_id", "timestamp"),
        db.Index("idx_trade_symbol_action", "symbol", "action"),
    )

    def __repr__(self):
        return f"<Trade {self.symbol} {self.action} {self.qty}>"

class WebhookLog(db.Model):
    __tablename__ = "webhook_log"
    
    # CORRECTED: Changed from Integer to String(36) for UUID
    id = db.Column(db.Integer, primary_key=True)
    status = db.Column(db.Integer)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    reason = db.Column(db.Text)
    
    user_id = db.Column(db.Integer, index=True)
    endpoint = db.Column(db.String(100))
    request_data = db.Column(db.JSON)
    response_data = db.Column(db.JSON)

    def __repr__(self):
        return f"<WebhookLog {self.status} at {self.timestamp}>"

class SystemLog(db.Model):
    __tablename__ = "system_log"
    
    # CORRECTED: Changed from Integer to String(36) for UUID
    id = db.Column(db.Integer, primary_key=True)
    level = db.Column(db.String(20), default="INFO")
    message = db.Column(db.Text)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    details = db.Column(db.JSON)
    
    user_id = db.Column(db.Integer, index=True)
    module = db.Column(db.String(50))
    
    __table_args__ = (
        db.Index("idx_level_timestamp", "level", "timestamp"),
    )

    def __repr__(self):
        return f"<SystemLog {self.level}: {self.message[:50]}>"

class Setting(db.Model):
    __tablename__ = "setting"
    
    # CORRECTED: Changed from Integer to String(36) for UUID
    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(100), unique=True, nullable=False, index=True)
    value = db.Column(db.Text)
    description = db.Column(db.String(255))
    
    # created_at = db.Column(db.DateTime, default=datetime.utcnow)
    # updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<Setting {self.key}: {self.value}>"

# Association table for many-to-many relationship
group_members = db.Table(
    "group_members",
    # CORRECTED: Changed foreign key types to String(36)
    db.Column("group_id", db.Integer, db.ForeignKey("group.id"), primary_key=True),
    db.Column("account_id", db.Integer, db.ForeignKey("account.id"), primary_key=True),
)

class Group(db.Model):
    __tablename__ = "group"
    
    # CORRECTED: Changed from Integer to String(36) for UUID
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    name = db.Column(db.String(120), nullable=False)
    description = db.Column(db.String(255))
    
    # created_at = db.Column(db.DateTime, default=datetime.utcnow)
    # updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    user = db.relationship("User", backref=db.backref("groups", lazy=True))
    accounts = db.relationship(
        "Account", 
        secondary=group_members, 
        backref=db.backref("groups", lazy=True),
        lazy="dynamic"
    )
    
    __table_args__ = (
        db.UniqueConstraint("user_id", "name", name="uq_user_group_name"),
    )

    def __repr__(self):
        return f"<Group {self.name}>"

class OrderMapping(db.Model):
    __tablename__ = "order_mapping"
    
    # CORRECTED: Changed from Integer to String(36) for UUID
    id = db.Column(db.Integer, primary_key=True)
    master_order_id = db.Column(db.String(50), nullable=False, index=True)
    child_order_id = db.Column(db.String(50), index=True)
    master_client_id = db.Column(db.String(50), nullable=False, index=True)
    master_broker = db.Column(db.String(50))
    child_client_id = db.Column(db.String(50), nullable=False, index=True)
    child_broker = db.Column(db.String(50))
    symbol = db.Column(db.String(50), index=True)
    status = db.Column(db.String(20), default="ACTIVE", index=True)
    
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    child_timestamp = db.Column(db.DateTime)
    
    remarks = db.Column(db.Text)
    multiplier = db.Column(db.Float, default=1.0)
    
    action = db.Column(db.String(10))
    quantity = db.Column(db.Integer)
    price = db.Column(db.Float)
    
    __table_args__ = (
        db.Index("idx_master_order_status", "master_order_id", "status"),
        db.Index("idx_child_client_status", "child_client_id", "status"),
        db.Index("idx_master_client_timestamp", "master_client_id", "timestamp"),
    )

    def __repr__(self):
        return f"<OrderMapping {self.master_order_id} -> {self.child_order_id}>"

class TradeLog(db.Model):
    __tablename__ = "trade_log"
    
    # CORRECTED: Changed from Integer to String(36) for UUID
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    user_id = db.Column(db.Integer, index=True)
    symbol = db.Column(db.String(50), index=True)
    action = db.Column(db.String(10))
    quantity = db.Column(db.Integer)
    status = db.Column(db.String(20), index=True)
    response = db.Column(db.Text)
    
    broker = db.Column(db.String(50))
    client_id = db.Column(db.String(50), index=True)
    order_id = db.Column(db.String(50))
    price = db.Column(db.Float)
    error_code = db.Column(db.String(50))
    
    __table_args__ = (
        db.Index("idx_client_timestamp", "client_id", "timestamp"),
        db.Index("idx_status_timestamp", "status", "timestamp"),
        db.Index("idx_tradelog_symbol_action", "symbol", "action"),
    )

    def __repr__(self):
        return f"<TradeLog {self.symbol} {self.action} {self.status}>"


class Strategy(db.Model):
    __tablename__ = "strategy"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(
        db.Integer, db.ForeignKey("user.id"), nullable=False, index=True
    )
    account_id = db.Column(db.Integer, db.ForeignKey("account.id"), index=True)
    name = db.Column(db.String(120), nullable=False)
    description = db.Column(db.Text)
    asset_class = db.Column(db.String(50), nullable=False)
    style = db.Column(db.String(50), nullable=False)
    allow_auto_submit = db.Column(db.Boolean, default=True)
    allow_live_trading = db.Column(db.Boolean, default=True)
    allow_any_ticker = db.Column(db.Boolean, default=True)
    allowed_tickers = db.Column(db.Text)
    notification_emails = db.Column(db.Text)
    notify_failures_only = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=False, index=True)
    last_run_at = db.Column(db.DateTime)
    signal_source = db.Column(db.String(100))
    risk_max_positions = db.Column(db.Integer)
    risk_max_allocation = db.Column(db.Float)
    schedule = db.Column(db.String(120))
    webhook_secret = db.Column(db.String(120))
    track_performance = db.Column(db.Boolean, default=False)
    log_retention_days = db.Column(db.Integer, default=30)
    is_public = db.Column(db.Boolean, default=False, index=True)
    icon = db.Column(db.Text)
    brokers = db.Column(db.Text)
    master_accounts = db.Column(db.Text)
    
    user = db.relationship("User", backref=db.backref("strategies", lazy=True, cascade="all, delete-orphan"))
    account = db.relationship(
        "Account",
        backref=backref("strategies", lazy=True),
    )

    def __repr__(self):
        return f"<Strategy {self.name}>"


class StrategyLog(db.Model):
    __tablename__ = "strategy_log"

    id = db.Column(db.Integer, primary_key=True)
    strategy_id = db.Column(db.Integer, db.ForeignKey("strategy.id"), nullable=False, index=True)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    level = db.Column(db.String(20), default="INFO")
    message = db.Column(db.Text)
    performance = db.Column(db.JSON)

    strategy = db.relationship("Strategy", backref=db.backref("logs", lazy=True, cascade="all, delete-orphan"))

    def __repr__(self):
        return f"<StrategyLog {self.level}: {self.message[:30]}>"



class StrategySubscription(db.Model):
    __tablename__ = "strategy_subscription"

    id = db.Column(db.Integer, primary_key=True)
    strategy_id = db.Column(
        db.Integer, db.ForeignKey("strategy.id"), nullable=False, index=True
    )
    subscriber_id = db.Column(
        db.Integer, db.ForeignKey("user.id"), nullable=False, index=True
    )
    account_id = db.Column(db.Integer, db.ForeignKey("account.id"), index=True)
    auto_submit = db.Column(db.Boolean, default=True)
    order_type = db.Column(db.String(20), default="MARKET")
    qty_mode = db.Column(db.String(20), default="signal")
    fixed_qty = db.Column(db.Integer)
    approved = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    strategy = db.relationship("Strategy", backref=db.backref("subscriptions", lazy=True, cascade="all, delete-orphan"))
    subscriber = db.relationship("User", backref=db.backref("strategy_subscriptions", lazy=True, cascade="all, delete-orphan"))
    account = db.relationship("Account")
    
    __table_args__ = (
        db.UniqueConstraint("strategy_id", "subscriber_id", name="uq_strategy_subscriber"),
    )

    def __repr__(self):
        return f"<StrategySubscription {self.strategy_id} {self.subscriber_id}>"
