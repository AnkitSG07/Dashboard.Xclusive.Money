import json
import os
from app import app, db
from models import User, Account, Group, group_members


def migrate_users():
    if not os.path.exists('users.json'):
        return
    with open('users.json') as f:
        data = json.load(f)
    for email, info in data.items():
        if not User.query.filter_by(email=email).first():
            user = User(email=email)
            if 'password' in info:
                user.set_password(info['password'])
            db.session.add(user)
    db.session.commit()


def migrate_accounts():
    if not os.path.exists('accounts.json'):
        return
    with open('accounts.json') as f:
        data = json.load(f)
    for entry in data.get('accounts', []):
        owner = entry.get('owner')
        user = User.query.filter_by(email=owner).first()
        if not user:
            user = User(email=owner)
            db.session.add(user)
            db.session.commit()
        if not Account.query.filter_by(client_id=entry.get('client_id')).first():
            acc = Account(
                user_id=user.id,
                broker=entry.get('broker'),
                client_id=entry.get('client_id'),
                token_expiry=entry.get('token_expiry'),
                status=entry.get('status'),
                role=entry.get('role'),
                linked_master_id=entry.get('linked_master_id'),
                copy_status=entry.get('copy_status'),
                multiplier=entry.get('multiplier', 1.0),
                credentials=entry.get('credentials'),
                last_copied_trade_id=entry.get('last_copied_trade_id'),
            )
            db.session.add(acc)
    db.session.commit()


def migrate_groups():
    if not os.path.exists('groups.json'):
        return
    with open('groups.json') as f:
        data = json.load(f)
    for g in data.get('groups', []):
        owner = g.get('owner')
        user = User.query.filter_by(email=owner).first()
        if not user:
            continue
        group = Group.query.filter_by(name=g.get('name'), user_id=user.id).first()
        if not group:
            group = Group(name=g.get('name'), user_id=user.id)
            db.session.add(group)
            db.session.commit()
        for cid in g.get('members', []):
            acc = Account.query.filter_by(client_id=cid, user_id=user.id).first()
            if acc and acc not in group.accounts:
                group.accounts.append(acc)
    db.session.commit()


if __name__ == '__main__':
    with app.app_context():
        migrate_users()
        migrate_accounts()
        migrate_groups()
        print('Migration completed')
