# Dhan Trading Copy App

This Flask application connects to multiple broker APIs and allows a master trading account to automatically copy trades to linked child accounts. Currently adapters are provided for Dhan and AliceBlue along with several mock brokers.

## Setup

1. Install Python 3.11 or later.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Start the server:
   ```bash
   python app.py
   ```

The background scheduler will poll masters every 10 seconds and replicate new trades to their children.

## Configuration

Accounts are stored in `accounts.json`. Each account entry contains broker credentials and role information. Example structure:

```json
{
  "accounts": [
    {
      "broker": "dhan",
      "client_id": "ABCD1234",
      "credentials": {
        "access_token": "YOUR_TOKEN",
        "password": "optional"  
      },
      "role": "master",
      "linked_master_id": null,
      "multiplier": 1,
      "copy_status": "Off"
    }
  ]
}
```

Masters can have multiple children with the child's `linked_master_id` field pointing to the master `client_id`.

For AliceBlue accounts, `credentials` must include `password` and `totp_secret` for TOTP login. The application uses `pyotp` for generating TOTP codes.

## Dependencies

The main requirements are listed in `requirements.txt` and include:
- `flask` and `Flask-Cors` for the web server
- `dhanhq` for Dhan API access
- `pyotp` for AliceBlue two-factor authentication
- `apscheduler` for background jobs

## Usage

Once the server is running, visit `/` in your browser to access the basic dashboard. REST endpoints are available for managing accounts and copying trades. See `app.py` for available routes.

---

This project is a simple reference implementation and is not production ready. Use at your own risk.
