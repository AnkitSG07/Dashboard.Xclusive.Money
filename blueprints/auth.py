from flask import Blueprint, render_template, request, session, redirect, url_for, flash, current_app
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from flask_mail import Message
from models import db, User
from mail import mail

auth_bp = Blueprint('auth', __name__)

def _generate_token(email: str) -> str:
    """Return a signed token for the given email address."""
    s = URLSafeTimedSerializer(current_app.secret_key)
    return s.dumps(email, salt="password-reset")


def _verify_token(token: str, max_age: int = 3600) -> str | None:
    """Return the email contained in ``token`` if valid, else ``None``."""
    s = URLSafeTimedSerializer(current_app.secret_key)
    try:
        return s.loads(token, salt="password-reset", max_age=max_age)
    except (BadSignature, SignatureExpired):  # pragma: no cover - handled gracefully
        return None

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

@auth_bp.route('/request-password-reset', methods=['GET', 'POST'])
def request_password_reset():
    """Collect email and send a password reset link."""
    if request.method == 'POST':
        email = request.form.get('email')
        user = User.query.filter_by(email=email).first()
        if user:
            token = _generate_token(user.email)
            reset_url = url_for('auth.reset_password', token=token, _external=True)
            msg = Message(
                subject="Password Reset Request",
                recipients=[user.email],
                body=f"Click the link to reset your password: {reset_url}",
            )
            mail.send(msg)
        flash('If that email address exists, a reset link has been sent.')
        return redirect(url_for('auth.login'))
    return render_template('forgot-password.html')


@auth_bp.route('/reset-password/<token>', methods=['GET', 'POST'])
def reset_password(token: str):
    """Verify token and update the user's password."""
    email = _verify_token(token)
    if not email:
        flash('The password reset link is invalid or has expired.')
        return redirect(url_for('auth.request_password_reset'))

    if request.method == 'POST':
        password = request.form.get('password')
        user = User.query.filter_by(email=email).first()
        if user:
            user.set_password(password)
            db.session.commit()
            flash('Your password has been reset. Please log in.')
            return redirect(url_for('auth.login'))
        flash('User not found.')
        return redirect(url_for('auth.request_password_reset'))
    return render_template('reset-password.html', token=token)
