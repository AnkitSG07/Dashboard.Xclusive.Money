import random
import re
from datetime import datetime, timedelta

from flask import (
    Blueprint,
    render_template,
    request,
    session,
    redirect,
    url_for,
    flash,
    current_app,
)
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from models import db, User
from services import sms
from services.sms import MSG91Error

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

def _set_timed_session(key: str, data: dict, ttl_minutes: int = 10) -> None:
    payload = dict(data)
    payload["created_at"] = datetime.utcnow().isoformat()
    payload["ttl_minutes"] = ttl_minutes
    session[key] = payload


def _get_timed_session(key: str) -> dict | None:
    data = session.get(key)
    if not data:
        return None

    created_raw = data.get("created_at")
    ttl_minutes = data.get("ttl_minutes", 10)
    try:
        created_at = datetime.fromisoformat(created_raw)
    except (TypeError, ValueError):
        created_at = None

    if created_at and datetime.utcnow() - created_at > timedelta(minutes=ttl_minutes):
        session.pop(key, None)
        return None
    return data

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    next_url = request.args.get('next')
    if request.method == 'POST':
        email = (request.form.get('email') or '').strip()
        phone_raw = (request.form.get('phone') or '').strip()
        country_code = (request.form.get('country_code') or '').strip()
        password = request.form.get('password')
        remember_raw = request.form.get('remember_me')
        remember = False
        if remember_raw:
            remember = remember_raw.lower() not in {'0', 'false', 'off', 'no'}
        next_url = request.form.get('next') or next_url
        user = None
        if email:
            user = User.query.filter_by(email=email).first()

        if not user and phone_raw:
            normalized_phone = re.sub(r'\D', '', phone_raw)
            phone_candidates = []

            if normalized_phone:
                phone_candidates.append(normalized_phone)
                if country_code:
                    normalized_code = re.sub(r'\D', '', country_code)
                    if normalized_code:
                        phone_candidates.append(f"+{normalized_code}{normalized_phone}")
                        phone_candidates.append(f"{normalized_code}{normalized_phone}")

            for candidate in phone_candidates:
                user = User.query.filter_by(phone=candidate).first()
                if user:
                    break

            if not user and normalized_phone:
                existing_with_phone = User.query.filter(User.phone.isnot(None)).all()
                for candidate_user in existing_with_phone:
                    stored_normalized = re.sub(r'\D', '', candidate_user.phone or '')
                    if stored_normalized == normalized_phone:
                        user = candidate_user
                        break

        if user and user.check_password(password):
            if remember:
                session.permanent = True
                lifetime = current_app.config.get('REMEMBER_ME_DURATION', timedelta(days=30))
                current_app.permanent_session_lifetime = lifetime
            else:
                session.permanent = False
            session['user'] = user.email
            return redirect(next_url or url_for('summary'))
        flash('Invalid credentials', 'error')
        return render_template('log-in.html', next=next_url)
    return render_template('log-in.html', next=next_url)


@auth_bp.route('/login/otp', methods=['GET', 'POST'])
def login_otp():
    if request.method == 'GET' and request.args.get('reset') == '1':
        session.pop('login_otp', None)

    step = 'phone'
    phone_display = ''
    phone_value = ''
    country_code_value = '+91'

    stored_session = _get_login_session()
    if stored_session:
        step = 'otp'
        phone_display = stored_session.get('phone_display', '')

    if request.method == 'POST':
        stage = request.form.get('stage', 'phone')

        if stage == 'send_otp':
            country_code_value = request.form.get('country_code', '').strip() or '+91'
            phone_value = request.form.get('phone', '').strip()
            normalized_phone = re.sub(r"\D", "", phone_value)

            if not normalized_phone or len(normalized_phone) < 6:
                flash('Please enter a valid phone number.', 'error')
            else:
                user = _find_user_by_phone(phone_value, country_code_value)
                if not user:
                    flash('No account is associated with that phone number.', 'error')
                else:
                    display_phone = f"{country_code_value}{normalized_phone}" if country_code_value else normalized_phone
                    try:
                        sms.send_otp(display_phone)
                    except MSG91Error as exc:
                        flash(f'Could not send OTP: {exc}', 'error')
                    else:
                        _set_login_session(user, display_phone, normalized_phone)
                        flash('An OTP has been sent to your phone number.', 'success')
                        step = 'otp'
                        phone_display = display_phone

        elif stage == 'verify_otp':
            stored = _get_login_session()
            otp = ''.join(
                request.form.get(f'otp{i}', '').strip() for i in range(1, 7)
            )

            if not stored:
                flash('Your login session has expired. Please request a new OTP.', 'error')
                return redirect(url_for('auth.login_otp'))

            if len(otp) != 6 or not otp.isdigit():
                flash('Please enter the 6-digit OTP.', 'error')
            else:
                user = User.query.get(stored.get('user_id'))
                if not user:
                    session.pop('login_otp', None)
                    flash('We could not find your account. Please try again.', 'error')
                    return redirect(url_for('auth.login_otp'))

                try:
                    verification = sms.check_otp(stored['phone_display'], otp)
                except MSG91Error as exc:
                    flash(f'Could not verify OTP: {exc}', 'error')
                else:
                    if verification.status != 'approved':
                        flash('The OTP you entered is incorrect or expired. Please try again.', 'error')
                    else:
                        session.permanent = False
                        session['user'] = user.email
                        session.pop('login_otp', None)
                        return redirect(url_for('summary'))

            step = 'otp'
            phone_display = stored.get('phone_display', '') if stored else ''

        elif stage == 'resend_otp':
            stored = _get_login_session()
            if not stored:
                flash('Please enter your phone number to request a new OTP.', 'error')
            else:
                user = User.query.get(stored.get('user_id'))
                if not user:
                    session.pop('login_otp', None)
                    flash('Please enter your phone number to request a new OTP.', 'error')
                else:
                    try:
                        sms.send_otp(stored['phone_display'])
                    except MSG91Error as exc:
                        flash(f'Could not send OTP: {exc}', 'error')
                    else:
                        _set_login_session(
                            user,
                            stored['phone_display'],
                            stored['normalized_phone'],
                        )
                        flash('A new OTP has been sent to your phone number.', 'success')
                        step = 'otp'
                        phone_display = stored.get('phone_display', '')

    return render_template(
        'login-with-otp.html',
        step=step,
        phone_display=phone_display,
        phone_value=phone_value,
        country_code_value=country_code_value,
    )

@auth_bp.route('/signup', methods=['GET', 'POST'])
def signup():
    if request.method == 'GET' and request.args.get('reset') == '1':
        session.pop('signup_verification', None)

    pending_signup = _get_signup_session()
    if request.method == 'GET' and pending_signup:
        return render_template(
            'otp-verification.html',
            phone=pending_signup.get('phone_display', ''),
            email=pending_signup.get('email', ''),
        )

    if request.method == 'POST':
        stage = request.form.get('stage', 'start')

        if stage == 'start':
            email = (request.form.get('email') or request.form.get('username') or '').strip()
            phone_input = (request.form.get('phone') or '').strip()
            country_code = (request.form.get('country_code') or '+91').strip()
            normalized_phone = re.sub(r"\D", "", phone_input)
            phone_display = f"{country_code}{normalized_phone}" if normalized_phone else ''

            if not email or not normalized_phone:
                return render_template('sign-up.html', error='Email and phone are required')

            if User.query.filter_by(email=email).first():
                return render_template('sign-up.html', error='User already exists')

            existing_by_phone = _find_user_by_phone(phone_display)
            if existing_by_phone:
                return render_template(
                    'sign-up.html',
                    error='Phone number already registered. Please log in or reset your password.',
                )

            try:
                sms.send_otp(phone_display)
            except MSG91Error as exc:
                return render_template('sign-up.html', error=f'Could not send OTP: {exc}')

            _set_signup_session(email, phone_display, normalized_phone)
            flash('An OTP has been sent to your phone number.', 'success')
            return render_template('otp-verification.html', phone=phone_display, email=email)

        elif stage == 'verify':
            pending_signup = _get_signup_session()
            otp = ''.join(
                request.form.get(f'otp{i}', '').strip() for i in range(1, 7)
            )
            password = request.form.get('password') or ''
            confirm_password = request.form.get('confirm_password') or ''

            if not pending_signup:
                flash('Your signup verification has expired. Please restart the process.', 'error')
                return redirect(url_for('auth.signup'))

            if len(otp) != 6 or not otp.isdigit():
                flash('Please enter the 6-digit OTP.', 'error')
                return render_template(
                    'otp-verification.html',
                    phone=pending_signup.get('phone_display', ''),
                    email=pending_signup.get('email', ''),
                )

            if not password or len(password) < 6:
                flash('Password must be at least 6 characters long.', 'error')
                return render_template(
                    'otp-verification.html',
                    phone=pending_signup.get('phone_display', ''),
                    email=pending_signup.get('email', ''),
                )

            if password != confirm_password:
                flash('Passwords do not match.', 'error')
                return render_template(
                    'otp-verification.html',
                    phone=pending_signup.get('phone_display', ''),
                    email=pending_signup.get('email', ''),
                )

            if User.query.filter_by(email=pending_signup.get('email')).first():
                session.pop('signup_verification', None)
                flash('User already exists. Please log in.', 'error')
                return redirect(url_for('auth.login'))

            existing_by_phone = _find_user_by_phone(pending_signup.get('phone_display', ''))
            if existing_by_phone:
                session.pop('signup_verification', None)
                flash('Phone number already registered. Please log in or reset your password.', 'error')
                return redirect(url_for('auth.login'))

            try:
                verification = sms.check_otp(pending_signup['phone_display'], otp)
            except MSG91Error as exc:
                flash(f'Could not verify OTP: {exc}', 'error')
                return render_template(
                    'otp-verification.html',
                    phone=pending_signup.get('phone_display', ''),
                    email=pending_signup.get('email', ''),
                )

            if verification.status != 'approved':
                flash('The OTP you entered is incorrect or has expired. Please try again.', 'error')
                return render_template(
                    'otp-verification.html',
                    phone=pending_signup.get('phone_display', ''),
                    email=pending_signup.get('email', ''),
                )

            user = User(email=pending_signup.get('email', ''), phone=pending_signup.get('phone_display', ''))
            user.set_password(password)
            db.session.add(user)
            db.session.commit()
            session.pop('signup_verification', None)
            return redirect(url_for('auth.signup_success'))

    return render_template('sign-up.html')


@auth_bp.route('/otp-verification')
def otp_verification():
    phone = request.args.get('phone', '')
    email = request.args.get('email', '')
    return render_template('otp-verification.html', phone=phone, email=email)


@auth_bp.route('/signup/success')
def signup_success():
    return render_template('account-success.html')

@auth_bp.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('home'))

def _find_user_by_phone(phone_raw: str, country_code: str | None = None) -> User | None:
    """Return the first user that matches ``phone_raw`` in any stored format."""

    normalized_phone = re.sub(r"\D", "", phone_raw or "")
    normalized_code = re.sub(r"\D", "", country_code or "")

    candidates: list[str] = []
    if normalized_phone:
        candidates.append(normalized_phone)
        if normalized_code:
            candidates.append(f"+{normalized_code}{normalized_phone}")
            candidates.append(f"{normalized_code}{normalized_phone}")

    for candidate in candidates:
        user = User.query.filter_by(phone=candidate).first()
        if user:
            return user

    if normalized_phone:
        users_with_phone = User.query.filter(User.phone.isnot(None)).all()
        for user in users_with_phone:
            stored_normalized = re.sub(r"\D", "", user.phone or "")
            if stored_normalized == normalized_phone:
                return user
    return None


def _set_reset_session(user: User, display_phone: str, normalized_phone: str, otp: str) -> None:
    session['password_reset'] = {
        'user_id': user.id,
        'otp': otp,
        'phone_display': display_phone,
        'normalized_phone': normalized_phone,
        'created_at': datetime.utcnow().isoformat(),
    }


def _set_reset_session(user: User, display_phone: str, normalized_phone: str) -> None:
    _set_timed_session(
        'password_reset',
        {
            'user_id': user.id,
            'phone_display': display_phone,
            'normalized_phone': normalized_phone,
        },
    )


def _get_reset_session() -> dict | None:
    return _get_timed_session('password_reset')


def _set_login_session(user: User, display_phone: str, normalized_phone: str) -> None:
    _set_timed_session(
        'login_otp',
        {
            'user_id': user.id,
            'phone_display': display_phone,
            'normalized_phone': normalized_phone,
        },
    )


def _get_login_session() -> dict | None:
    return _get_timed_session('login_otp')


def _set_signup_session(email: str, display_phone: str, normalized_phone: str) -> None:
    _set_timed_session(
        'signup_verification',
        {
            'email': email,
            'phone_display': display_phone,
            'normalized_phone': normalized_phone,
        },
        ttl_minutes=15,
    )


def _get_signup_session() -> dict | None:
    return _get_timed_session('signup_verification')


@auth_bp.route('/request-password-reset', methods=['GET', 'POST'])
def request_password_reset():
    """Collect a phone number and send a one-time password."""

    if request.method == 'GET' and request.args.get('reset') == '1':
        session.pop('password_reset', None)

    step = 'phone'
    phone_display = ''
    phone_value = ''
    country_code_value = '+91'

    if request.method == 'POST':
        stage = request.form.get('stage', 'phone')
        if stage == 'send_otp':
            country_code_value = request.form.get('country_code', '').strip()
            phone_value = request.form.get('phone', '').strip()
            normalized_phone = re.sub(r"\D", "", phone_value)

            if not normalized_phone or len(normalized_phone) < 6:
                flash('Please enter a valid phone number.', 'error')
            else:
                user = _find_user_by_phone(phone_value, country_code_value)
                if not user:
                    flash('No account is associated with that phone number.', 'error')
                else:
                    display_phone = f"{country_code_value}{normalized_phone}" if country_code_value else normalized_phone
                    try:
                        sms.send_otp(display_phone)
                    except MSG91Error as exc:
                        flash(f'Could not send OTP: {exc}', 'error')
                    else:
                        _set_reset_session(user, display_phone, normalized_phone)
                        flash('An OTP has been sent to your phone number.', 'success')
                        step = 'otp'
                        phone_display = display_phone

        elif stage == 'resend_otp':
            stored = _get_reset_session()
            if not stored:
                flash('Please enter your phone number to request a new OTP.', 'error')
            else:
                user = User.query.get(stored['user_id'])
                if not user:
                    session.pop('password_reset', None)
                    flash('Please restart the password reset process.', 'error')
                else:
                    try:
                        sms.send_otp(stored['phone_display'])
                    except MSG91Error as exc:
                        flash(f'Could not send OTP: {exc}', 'error')
                    else:
                        _set_reset_session(user, stored['phone_display'], stored['normalized_phone'])
                        flash('A new OTP has been sent to your phone number.', 'success')
                        step = 'otp'
                        phone_display = stored['phone_display']

    stored_context = _get_reset_session()
    if stored_context and step == 'phone':
        # The user already requested an OTP earlier in the session.
        step = 'otp'
        phone_display = stored_context.get('phone_display', '')

    return render_template(
        'forgot-password.html',
        step=step,
        phone_display=phone_display,
        phone_value=phone_value,
        country_code_value=country_code_value,
    )


@auth_bp.route('/request-password-reset/verify', methods=['POST'])
def verify_password_reset():
    """Verify the OTP and update the user's password."""

    stored = _get_reset_session()
    if not stored:
        flash('Your password reset session has expired. Please request a new OTP.', 'error')
        return redirect(url_for('auth.request_password_reset'))

    otp = ''.join(
        request.form.get(f'otp{i}', '').strip() for i in range(1, 7)
    )
    password = request.form.get('password')
    confirm_password = request.form.get('confirm_password')

    if len(otp) != 6 or not otp.isdigit():
        flash('Please enter the 6-digit OTP.', 'error')
        return render_template(
            'forgot-password.html',
            step='otp',
            phone_display=stored.get('phone_display', ''),
        )

    try:
        verification = sms.check_otp(stored['phone_display'], otp)
    except MSG91Error as exc:
        flash(f'Could not verify OTP: {exc}', 'error')
        return render_template(
            'forgot-password.html',
            step='otp',
            phone_display=stored.get('phone_display', ''),
        )

    if verification.status != 'approved':
        flash('The OTP you entered is incorrect or has expired. Please try again.', 'error')
        return render_template(
            'forgot-password.html',
            step='otp',
            phone_display=stored.get('phone_display', ''),
        )

    if not password or len(password) < 6:
        flash('Password must be at least 6 characters long.', 'error')
        return render_template(
            'forgot-password.html',
            step='otp',
            phone_display=stored.get('phone_display', ''),
        )

    if password != (confirm_password or ''):
        flash('Passwords do not match.', 'error')
        return render_template(
            'forgot-password.html',
            step='otp',
            phone_display=stored.get('phone_display', ''),
        )

    user = User.query.get(stored.get('user_id'))
    if not user:
        session.pop('password_reset', None)
        flash('We could not find your account. Please restart the process.', 'error')
        return redirect(url_for('auth.request_password_reset'))

    user.set_password(password)
    db.session.commit()
    session.pop('password_reset', None)
    flash('Your password has been updated. Please log in.', 'success')
    return redirect(url_for('auth.login'))


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
