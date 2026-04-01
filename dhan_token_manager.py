"""
=============================================================================
Dhan API v2 — Auto Token Manager
=============================================================================
Automatically generates and renews your Dhan Access Token daily.

TWO METHODS SUPPORTED:
  Method 1 (RECOMMENDED — Fully Automatic):
      Uses TOTP (Time-based OTP) + PIN to generate token via API.
      Requires: dhanClientId, PIN (4-digit), TOTP_SECRET
      Endpoint: POST https://auth.dhan.co/app/generateAccessToken

  Method 2 (Fallback — if token is still active):
      Renews an existing valid token for another 24 hours.
      Endpoint: GET https://api.dhan.co/v2/RenewToken

HOW TO SETUP TOTP:
  1. Go to web.dhan.co → Profile → API Access
  2. Enable TOTP — scan the QR code with Google Authenticator
     OR copy the text secret shown (looks like: LETTKFDCQGROSTHG...)
  3. Paste that secret as DHAN_TOTP_SECRET in your .env file

.env file required:
  DHAN_CLIENT_ID    = your 10-digit client ID
  DHAN_PIN          = your 4-digit trading PIN
  DHAN_TOTP_SECRET  = your TOTP secret key (from web.dhan.co)

pip install: requests python-dotenv pyotp schedule

Usage:
  python dhan_token_manager.py          # generates token once and exits
  python dhan_token_manager.py --daemon  # runs continuously, auto-renews daily at 8 AM

=============================================================================
"""

import os
import sys
import json
import time
import logging
import argparse
import platform
import schedule
from datetime import datetime, timezone
from pathlib import Path

import requests
import pyotp                  # pip install pyotp
from dotenv import load_dotenv, set_key   # pip install python-dotenv

# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("DhanTokenManager")

# ─────────────────────────────────────────────────────────────
#  PyInstaller-safe base path
# ─────────────────────────────────────────────────────────────
if getattr(sys, 'frozen', False):
    _BASE = Path(sys.executable).parent
else:
    _BASE = Path(__file__).resolve().parent

ENV_FILE = _BASE / ".env"

# ─────────────────────────────────────────────────────────────
#  Shared token file — written by dhan-token-generator EXE
#  All strategies read from here instead of generating their own
# ─────────────────────────────────────────────────────────────
if platform.system() == "Windows":
    SHARED_TOKEN_FILE = Path("C:/balfund_shared/dhan_token.json")
else:
    SHARED_TOKEN_FILE = Path.home() / "balfund_shared" / "dhan_token.json"


def read_shared_token() -> dict:
    """
    Read token from the shared JSON file written by dhan-token-generator.
    Returns dict with 'client_id' and 'access_token', or empty dict if not found.
    """
    if not SHARED_TOKEN_FILE.exists():
        return {}
    try:
        data = json.loads(SHARED_TOKEN_FILE.read_text(encoding="utf-8"))
        client_id    = str(data.get("client_id", "")).strip()
        access_token = str(data.get("access_token", "")).strip()
        if client_id and access_token:
            return {"client_id": client_id, "access_token": access_token}
    except Exception as e:
        log.warning("Could not read shared token file: %s", e)
    return {}


def load_config() -> dict:
    """Re-reads .env with override=True. Shared token from dhan-token-generator takes priority."""
    load_dotenv(ENV_FILE, override=True)
    config = {
        "client_id":    os.getenv("DHAN_CLIENT_ID", "").strip(),
        "pin":          os.getenv("DHAN_PIN", "").strip(),
        "totp_secret":  os.getenv("DHAN_TOTP_SECRET", "").strip(),
        "access_token": os.getenv("DHAN_ACCESS_TOKEN", "").strip(),
    }
    if not config["client_id"]:
        raise ValueError("DHAN_CLIENT_ID is missing in .env file.")
    # Shared file from dhan-token-generator takes priority over .env token
    shared = read_shared_token()
    if shared.get("access_token"):
        config["access_token"] = shared["access_token"]
    return config


def save_token_to_env(access_token: str, expiry: str = ""):
    """Persist the new token back into .env so other scripts pick it up."""
    set_key(str(ENV_FILE), "DHAN_ACCESS_TOKEN", access_token)
    if expiry:
        set_key(str(ENV_FILE), "DHAN_TOKEN_EXPIRY", expiry)
    log.info(f"Token saved to {ENV_FILE}")


# ─────────────────────────────────────────────────────────────
#  METHOD 1 — TOTP-based Token Generation (fully automatic)
# ─────────────────────────────────────────────────────────────

def generate_totp(totp_secret: str) -> str:
    """Generate current 6-digit TOTP code from secret."""
    totp = pyotp.TOTP(totp_secret)
    code = totp.now()
    log.info(f"Generated TOTP: {code} (valid for ~{30 - (int(time.time()) % 30)}s)")
    return code


def generate_token_via_totp(client_id: str, pin: str, totp_secret: str) -> dict:
    """
    Method 1: Fully automated token generation using TOTP.

    POST https://auth.dhan.co/app/generateAccessToken
         ?dhanClientId={client_id}&pin={pin}&totp={totp_code}

    Response:
    {
        "dhanClientId":      "1000000001",
        "dhanClientName":    "JOHN DOE",
        "dhanClientUcc":     "ABCD12345E",
        "givenPowerOfAttorney": false,
        "accessToken":       "eyJ...",
        "expiryTime":        "2026-01-01T00:00:00.000"
    }
    """
    totp_code = generate_totp(totp_secret)
    url = (
        f"https://auth.dhan.co/app/generateAccessToken"
        f"?dhanClientId={client_id}&pin={pin}&totp={totp_code}"
    )
    log.info(f"Requesting new token via TOTP for client {client_id}...")

    try:
        resp = requests.post(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        if "accessToken" in data:
            log.info(f"✅ Token generated successfully!")
            log.info(f"   Client  : {data.get('dhanClientName', 'N/A')}")
            log.info(f"   Expires : {data.get('expiryTime', 'N/A')}")
            return {
                "success":      True,
                "access_token": data["accessToken"],
                "expiry":       data.get("expiryTime", ""),
                "client_name":  data.get("dhanClientName", ""),
                "method":       "TOTP",
            }
        else:
            log.error(f"❌ Token generation failed: {data}")
            return {"success": False, "error": str(data)}

    except requests.exceptions.HTTPError as e:
        log.error(f"❌ HTTP error: {e.response.status_code} — {e.response.text}")
        return {"success": False, "error": str(e)}
    except Exception as e:
        log.error(f"❌ Request failed: {e}")
        return {"success": False, "error": str(e)}


# ─────────────────────────────────────────────────────────────
#  METHOD 2 — Renew Existing Token (extends by 24h)
# ─────────────────────────────────────────────────────────────

def renew_token(client_id: str, access_token: str) -> dict:
    """
    Method 2: Renew an active token for another 24 hours.
    Only works if current token is NOT expired.

    GET https://api.dhan.co/v2/RenewToken
    Headers: access-token, dhanClientId
    """
    url = "https://api.dhan.co/v2/RenewToken"
    headers = {
        "access-token":  access_token,
        "dhanClientId":  client_id,
        "Content-Type":  "application/json",
    }
    log.info("Attempting to renew existing token...")

    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        if "accessToken" in data:
            log.info(f"✅ Token renewed successfully! Expires: {data.get('expiryTime', 'N/A')}")
            return {
                "success":      True,
                "access_token": data["accessToken"],
                "expiry":       data.get("expiryTime", ""),
                "method":       "RENEW",
            }
        else:
            log.warning(f"⚠️ Renew returned unexpected response: {data}")
            return {"success": False, "error": str(data)}

    except requests.exceptions.HTTPError as e:
        log.warning(f"⚠️ Token renew failed (token may be expired): {e.response.status_code}")
        return {"success": False, "error": str(e)}
    except Exception as e:
        log.error(f"❌ Renew request failed: {e}")
        return {"success": False, "error": str(e)}


# ─────────────────────────────────────────────────────────────
#  VERIFY TOKEN — quick check if token is still valid
# ─────────────────────────────────────────────────────────────

def verify_token(client_id: str, access_token: str) -> bool:
    """Ping the user profile endpoint to check if token is still valid."""
    if not access_token:
        return False
    url = "https://api.dhan.co/v2/profile"
    headers = {
        "access-token": access_token,
        "client-id":    client_id,
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            log.info("✅ Current token is valid.")
            return True
        else:
            log.warning(f"⚠️ Token validation failed: {resp.status_code}")
            return False
    except Exception as e:
        log.warning(f"⚠️ Token check error: {e}")
        return False


# ─────────────────────────────────────────────────────────────
#  MASTER FUNCTION — smart token refresh
# ─────────────────────────────────────────────────────────────

def get_fresh_token(config: dict, force_new: bool = False) -> str:
    """
    Smart token manager:
    1. If token exists and is valid, try to renew it (Method 2)
    2. If renew fails or force_new=True, generate fresh via TOTP (Method 1)
    3. Save new token to .env

    Returns the fresh access_token string.
    """
    client_id    = config["client_id"]
    pin          = config["pin"]
    totp_secret  = config["totp_secret"]
    access_token = config["access_token"]

    result = None

    # Step 1: Try renewing if we have an existing token
    if access_token and not force_new:
        if verify_token(client_id, access_token):
            result = renew_token(client_id, access_token)
            if result["success"]:
                save_token_to_env(result["access_token"], result.get("expiry", ""))
                return result["access_token"]

    # Step 2: Generate fresh token via TOTP
    if totp_secret and pin:
        result = generate_token_via_totp(client_id, pin, totp_secret)
        if result["success"]:
            save_token_to_env(result["access_token"], result.get("expiry", ""))
            return result["access_token"]
    else:
        log.error("❌ Cannot generate token: DHAN_PIN or DHAN_TOTP_SECRET missing in .env")

    if result and not result["success"]:
        raise RuntimeError(f"Token generation failed: {result.get('error', 'Unknown error')}")

    raise RuntimeError("Token generation failed. Check credentials in .env")


# ─────────────────────────────────────────────────────────────
#  SCHEDULER — auto-refresh every day at 8:00 AM
# ─────────────────────────────────────────────────────────────

def scheduled_refresh():
    """Called by scheduler every morning to refresh the token."""
    log.info("=" * 60)
    log.info("⏰ Scheduled token refresh starting...")
    try:
        config = load_config()
        token  = get_fresh_token(config, force_new=True)
        log.info(f"✅ Scheduled refresh complete. New token: {token[:20]}...")
    except Exception as e:
        log.error(f"❌ Scheduled refresh failed: {e}")
    log.info("=" * 60)


def run_daemon(refresh_time: str = "08:00"):
    """
    Run as background daemon.
    Generates token immediately on start, then auto-refreshes daily.

    refresh_time: "HH:MM" format, default "08:00" (8 AM daily)
    """
    log.info(f"🚀 DhanTokenManager daemon started.")
    log.info(f"   Token will auto-refresh daily at {refresh_time}")

    # Generate token immediately on start
    scheduled_refresh()

    # Schedule daily refresh
    schedule.every().day.at(refresh_time).do(scheduled_refresh)

    log.info(f"⏳ Next refresh scheduled at {refresh_time}. Running...")
    while True:
        schedule.run_pending()
        time.sleep(30)  # check every 30 seconds


# ─────────────────────────────────────────────────────────────
#  SETUP HELPER — creates .env template if it doesn't exist
# ─────────────────────────────────────────────────────────────

def create_env_template():
    if ENV_FILE.exists():
        log.info(f".env already exists at {ENV_FILE.absolute()}")
        return

    template = """\
# ──────────────────────────────────────────────
# Dhan API Credentials
# ──────────────────────────────────────────────

# Your Dhan Client ID (10-digit number from web.dhan.co → Profile)
DHAN_CLIENT_ID=

# Your 4-digit Dhan trading PIN
DHAN_PIN=

# TOTP Secret Key (from web.dhan.co → Profile → API Access → Enable TOTP)
# It looks like: LETTKFDCQGROSTHGRNXQUGLVSJBUKXTT
DHAN_TOTP_SECRET=

# These are auto-filled by the token manager — leave blank
DHAN_ACCESS_TOKEN=
DHAN_TOKEN_EXPIRY=
"""
    with open(ENV_FILE, "w") as f:
        f.write(template)
    log.info(f"✅ Created .env template at {ENV_FILE.absolute()}")
    log.info("   Please fill in DHAN_CLIENT_ID, DHAN_PIN, and DHAN_TOTP_SECRET")


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dhan API v2 Auto Token Manager")
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Run as daemon — auto-refreshes token daily at 8 AM"
    )
    parser.add_argument(
        "--refresh-time",
        default="08:00",
        help="Time to refresh token daily in HH:MM format (default: 08:00)"
    )
    parser.add_argument(
        "--setup",
        action="store_true",
        help="Create a .env template file"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force generate new token even if current one is valid"
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Just verify if the current token in .env is valid"
    )
    args = parser.parse_args()

    if args.setup:
        create_env_template()

    elif args.verify:
        cfg = load_config()
        is_valid = verify_token(cfg["client_id"], cfg["access_token"])
        print(f"Token valid: {is_valid}")

    elif args.daemon:
        run_daemon(refresh_time=args.refresh_time)

    else:
        # One-shot: generate token and print it
        try:
            cfg   = load_config()
            token = get_fresh_token(cfg, force_new=args.force)
            print(f"\n{'='*60}")
            print(f"✅ ACCESS TOKEN:")
            print(f"   {token}")
            print(f"{'='*60}\n")
        except Exception as e:
            log.error(f"Failed: {e}")
            exit(1)
