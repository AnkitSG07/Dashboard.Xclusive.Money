from flask import Blueprint, render_template, request, session, redirect, url_for
from models import db, User

auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        user = User.query.filter_by(email=email).first()
        if user and user.check_password(password):
            session['user'] = user.email
            return redirect(url_for('summary'))
        return render_template('log-in.html', error='Invalid credentials')
    return render_template('log-in.html')

@auth_bp.route('/signup', methods=['GET', 'POST'])
def signup():
    if request.method == 'POST':
        email = request.form.get('email') or request.form.get('username')
        password = request.form.get('password')
        if User.query.filter_by(email=email).first():
            return render_template('sign-up.html', error='User already exists')
        user = User(email=email)
        user.set_password(password)
        db.session.add(user)
        db.session.commit()
        session['user'] = user.email
        return redirect(url_for('summary'))
    return render_template('sign-up.html')

@auth_bp.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('home'))
