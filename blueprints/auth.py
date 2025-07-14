from flask import Blueprint, render_template, request, session, redirect, url_for
from models import db, User

auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        identifier = request.form.get('identifier')
        password = request.form.get('password')
        user = None
        if identifier:
            if '@' in identifier:
                user = User.query.filter_by(email=identifier).first()
            else:
                user = User.query.filter_by(phone=identifier).first()
        if user and user.check_password(password):
            session['user'] = user.email or user.phone
            username = user.name
            if not username:
                if user.email:
                    username = user.email.split('@')[0]
                else:
                    username = user.phone
            session['username'] = username
            return redirect(url_for('summary'))
        return render_template('log-in.html', error='Invalid credentials')
    return render_template('log-in.html')

@auth_bp.route('/signup', methods=['GET', 'POST'])
def signup():
    if request.method == 'POST':
        email = request.form.get('email')
        phone = request.form.get('phone')
        name = request.form.get('username')
        password = request.form.get('password')

        if not (email or phone):
            return render_template('sign-up.html', error='Email or mobile required')

        existing = None
        if email:
            existing = User.query.filter_by(email=email).first()
        if existing is None and phone:
            existing = User.query.filter_by(phone=phone).first()
        if existing:
            return render_template('sign-up.html', error='User already exists')

        user = User(email=email, phone=phone, name=name)
        user.set_password(password)
        db.session.add(user)
        db.session.commit()
        session['user'] = user.email or user.phone
        username = user.name
        if not username:
            if user.email:
                username = user.email.split('@')[0]
            else:
                username = user.phone
        session['username'] = username
        return redirect(url_for('summary'))
    return render_template('sign-up.html')

@auth_bp.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('home'))
