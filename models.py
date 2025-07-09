from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime

db = SQLAlchemy()

class User(db.Model):
    __tablename__ = 'user'
    
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(128), nullable=True)
    name = db.Column(db.String(120))
    phone = db.Column(db.String(20))
    webhook_token = db.Column(db.String(64), unique=True, index=True)
    profile_image = db.Column(db.String(120))
    plan = db.Column(db.String(20), default='Free')
    
    # Fixed: Use DateTime instead of String
    last_login = db.Column(db.DateTime)
    subscription_start = db.Column(db.DateTime)
    subscription_end = db.Column(db.DateTime)
    payment_status = db.Column(db.String(32))
    is_admin = db.Column(db.Boolean, default=False)
    
    # COMMENTED OUT: Timestamps causing database errors
    # created_at = db.Column(db.DateTime, default=datetime.utcnow)
    # updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def set_password(self, password: str):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        if not self.password_hash:
            return False
        return check_password_hash(self.password_hash, password)

    def __repr__(self):
        return f'<User {self.email}>'

class Account(db.Model):
    __tablename__ = 'account'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    broker = db.Column(db.String(50), index=True)
    client_id = db.Column(db.String(50), nullable=False, index=True)
    
    # FIXED: Added missing fields from app.py
    username = db.Column(db.String(100))  # Critical missing field
    auto_login = db.Column(db.Boolean, default=True)  # Missing field
    last_login_time = db.Column(db.DateTime)  # Missing field
    device_number = db.Column(db.String(50))  # Missing field
    
    token_expiry = db.Column(db.DateTime)  # Fixed: DateTime instead of String
    status = db.Column(db.String(20), default='Pending')
    role = db.Column(db.String(20), index=True)  # Added index
    linked_master_id = db.Column(db.String(50), index=True)  # Added index
    copy_status = db.Column(db.String(10), default="Off", index=True)  # Added index
    multiplier = db.Column(db.Float, default=1.0)
    credentials = db.Column(db.JSON)
    last_copied_trade_id = db.Column(db.String(50))
    
    # COMMENTED OUT: Timestamps causing database errors
    # created_at = db.Column(db.DateTime, default=datetime.utcnow)
    # updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    user = db.relationship('User', backref=db.backref('accounts', lazy=True, cascade='all, delete-orphan'))
    
    # Added: Composite indexes for performance
    __table_args__ = (
        db.Index('idx_user_client', 'user_id', 'client_id'),
        db.Index('idx_role_status', 'role', 'copy_status'),
        db.Index('idx_master_children', 'linked_master_id', 'role'),
        db.UniqueConstraint('user_id', 'client_id', name='uq_user_client'),
    )

    def __repr__(self):
        return f'<Account {self.client_id} - {self.broker}>'

class Trade(db.Model):
    __tablename__ = 'trade'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    symbol = db.Column(db.String(50), index=True)
    action = db.Column(db.String(10))  # BUY/SELL
    qty = db.Column(db.Integer)
    price = db.Column(db.Float)
    status = db.Column(db.String(20), index=True)
    
    # Fixed: Use DateTime instead of String
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    
    # Added: Additional fields for better tracking
    broker = db.Column(db.String(50))
    order_id = db.Column(db.String(50))
    client_id = db.Column(db.String(50))
    
    # Relationships
    user = db.relationship('User', backref=db.backref('trades', lazy=True))
    
    # Added: Indexes for performance
    __table_args__ = (
        db.Index('idx_user_timestamp', 'user_id', 'timestamp'),
        db.Index('idx_trade_symbol_action', 'symbol', 'action'),
    )

    def __repr__(self):
        return f'<Trade {self.symbol} {self.action} {self.qty}>'

class WebhookLog(db.Model):
    __tablename__ = 'webhook_log'
    
    id = db.Column(db.Integer, primary_key=True)
    status = db.Column(db.Integer)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, index=True)  # Fixed
    reason = db.Column(db.Text)  # Changed to Text for longer messages
    
    # Added: Additional tracking fields
    user_id = db.Column(db.Integer, index=True)
    endpoint = db.Column(db.String(100))
    request_data = db.Column(db.JSON)
    response_data = db.Column(db.JSON)

    def __repr__(self):
        return f'<WebhookLog {self.status} at {self.timestamp}>'

class SystemLog(db.Model):
    __tablename__ = 'system_log'
    
    id = db.Column(db.Integer, primary_key=True)
    level = db.Column(db.String(20), default='INFO')  # Added: Log level
    message = db.Column(db.Text)  # Changed from 'type' to 'message'
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, index=True)  # Fixed
    details = db.Column(db.JSON)  # Changed to JSON for structured data
    
    # Added: Additional tracking
    user_id = db.Column(db.Integer, index=True)
    module = db.Column(db.String(50))
    
    __table_args__ = (
        db.Index('idx_level_timestamp', 'level', 'timestamp'),
    )

    def __repr__(self):
        return f'<SystemLog {self.level}: {self.message[:50]}>'

class Setting(db.Model):
    __tablename__ = 'setting'
    
    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(100), unique=True, nullable=False, index=True)
    value = db.Column(db.Text)  # Changed to Text for longer values
    description = db.Column(db.String(255))  # Added: Setting description
    
    # COMMENTED OUT: Timestamps causing database errors
    # created_at = db.Column(db.DateTime, default=datetime.utcnow)
    # updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f'<Setting {self.key}: {self.value}>'

# Association table for many-to-many relationship
group_members = db.Table(
    "group_members",
    db.Column("group_id", db.Integer, db.ForeignKey("group.id"), primary_key=True),
    db.Column("account_id", db.Integer, db.ForeignKey("account.id"), primary_key=True),
)

class Group(db.Model):
    __tablename__ = 'group'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    name = db.Column(db.String(120), nullable=False)
    description = db.Column(db.String(255))  # Added: Group description
    
    # COMMENTED OUT: Timestamps causing database errors
    # created_at = db.Column(db.DateTime, default=datetime.utcnow)
    # updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    user = db.relationship("User", backref=db.backref("groups", lazy=True))
    accounts = db.relationship(
        "Account", 
        secondary=group_members, 
        backref=db.backref("groups", lazy=True),
        lazy="dynamic"
    )
    
    # Added: Constraint for unique group names per user
    __table_args__ = (
        db.UniqueConstraint('user_id', 'name', name='uq_user_group_name'),
    )

    def __repr__(self):
        return f'<Group {self.name}>'

class OrderMapping(db.Model):
    __tablename__ = 'order_mapping'
    
    id = db.Column(db.Integer, primary_key=True)
    master_order_id = db.Column(db.String(50), nullable=False, index=True)
    child_order_id = db.Column(db.String(50), index=True)
    master_client_id = db.Column(db.String(50), nullable=False, index=True)
    master_broker = db.Column(db.String(50))
    child_client_id = db.Column(db.String(50), nullable=False, index=True)
    child_broker = db.Column(db.String(50))
    symbol = db.Column(db.String(50), index=True)
    status = db.Column(db.String(20), default='ACTIVE', index=True)
    
    # Fixed: Use DateTime objects
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    child_timestamp = db.Column(db.DateTime)
    
    remarks = db.Column(db.Text)  # Changed to Text for longer remarks
    multiplier = db.Column(db.Float, default=1.0)
    
    # Added: Additional tracking fields
    action = db.Column(db.String(10))  # BUY/SELL
    quantity = db.Column(db.Integer)
    price = db.Column(db.Float)
    
    # Added: Composite indexes for performance
    __table_args__ = (
        db.Index('idx_master_order_status', 'master_order_id', 'status'),
        db.Index('idx_child_client_status', 'child_client_id', 'status'),
        db.Index('idx_master_client_timestamp', 'master_client_id', 'timestamp'),
    )

    def __repr__(self):
        return f'<OrderMapping {self.master_order_id} -> {self.child_order_id}>'

class TradeLog(db.Model):
    __tablename__ = 'trade_log'
    
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, index=True)  # Fixed
    user_id = db.Column(db.Integer, index=True)
    symbol = db.Column(db.String(50), index=True)
    action = db.Column(db.String(10))  # BUY/SELL/SQUARE_OFF
    quantity = db.Column(db.Integer)
    status = db.Column(db.String(20), index=True)  # SUCCESS/FAILED/ERROR
    response = db.Column(db.Text)
    
    # Added: Additional tracking fields
    broker = db.Column(db.String(50))
    client_id = db.Column(db.String(50), index=True)
    order_id = db.Column(db.String(50))
    price = db.Column(db.Float)
    error_code = db.Column(db.String(50))
    
    # Added: Indexes for performance
    __table_args__ = (
        db.Index('idx_client_timestamp', 'client_id', 'timestamp'),
        db.Index('idx_status_timestamp', 'status', 'timestamp'),
        db.Index('idx_tradelog_symbol_action', 'symbol', 'action'),
    )

    def __repr__(self):
        return f'<TradeLog {self.symbol} {self.action} {self.status}>'
