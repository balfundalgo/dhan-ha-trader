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

pip install: requests python-dotenv pyotp schedule
=============================================================================
"""

import os
import sys
import time
import logging
import argparse
import schedule
from pathlib import Path

import requests
import pyotp
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("DhanTokenManager")

# ── PyInstaller-safe .env path ────────────────────────────────────────────────
if getattr(sys, 'frozen', False):
    _BASE = Path(sys.executable).parent
else:
    _BASE = Path(__file__).resolve().parent

ENV_FILE = _BASE / ".env"


# ─────────────────────────────────────────────────────────────────────────────
#  ENV helpers
# ─────────────────────────────────────────────────────────────────────────────

def _save_env_key(key: str, value: str):
    """
    Write/update a single key=value in .env AND immediately update os.environ.
    This prevents stale cache issues when load_dotenv was already called earlier.
    """
    # Write to file
    lines = []
    found = False
    if ENV_FILE.exists():
        with open(ENV_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith(f"{key}=") or stripped.startswith(f"{key} ="):
                lines[i] = f"{key}={value}\n"
                found = True
                break
    if not found:
        lines.append(f"{key}={value}\n")
    with open(ENV_FILE, "w", encoding="utf-8") as f:
        f.writelines(lines)

    # Also update os.environ directly so any subsequent os.getenv() calls
    # in the same process see the new value immediately
    os.environ[key] = value


def load_config() -> dict:
    """
    Always re-reads .env with override=True so fresh values from the file
    overwrite any stale values already in os.environ from a previous load.
    """
    load_dotenv(ENV_FILE, override=True)
    config = {
        "client_id":    os.getenv("DHAN_CLIENT_ID", "").strip(),
        "pin":          os.getenv("DHAN_PIN", "").strip(),
        "totp_secret":  os.getenv("DHAN_TOTP_SECRET", "").strip(),
        "access_token": os.getenv("DHAN_ACCESS_TOKEN", "").strip(),
    }
    if not config["client_id"]:
        raise ValueError("DHAN_CLIENT_ID is missing in .env file.")
    return config


def save_token_to_env(access_token: str, expiry: str = ""):
    _save_env_key("DHAN_ACCESS_TOKEN", access_token)
    if expiry:
        _save_env_key("DHAN_TOKEN_EXPIRY", expiry)
    log.info(f"Token saved to {ENV_FILE}")


# ─────────────────────────────────────────────────────────────────────────────
#  METHOD 1 — TOTP-based Token Generation
# ─────────────────────────────────────────────────────────────────────────────

def generate_totp(totp_secret: str) -> str:
    totp = pyotp.TOTP(totp_secret)
    code = totp.now()
    log.info(f"Generated TOTP: {code} (valid for ~{30 - (int(time.time()) % 30)}s)")
    return code


def generate_token_via_totp(client_id: str, pin: str, totp_secret: str) -> dict:
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
            log.info(f"✅ Token generated! Client: {data.get('dhanClientName', 'N/A')}  Expires: {data.get('expiryTime', 'N/A')}")
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


# ─────────────────────────────────────────────────────────────────────────────
#  METHOD 2 — Renew Existing Token
# ─────────────────────────────────────────────────────────────────────────────

def renew_token(client_id: str, access_token: str) -> dict:
    url = "https://api.dhan.co/v2/RenewToken"
    headers = {
        "access-token": access_token,
        "dhanClientId": client_id,
        "Content-Type": "application/json",
    }
    log.info("Attempting to renew existing token...")
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if "accessToken" in data:
            log.info(f"✅ Token renewed! Expires: {data.get('expiryTime', 'N/A')}")
            return {
                "success":      True,
                "access_token": data["accessToken"],
                "expiry":       data.get("expiryTime", ""),
                "method":       "RENEW",
            }
        else:
            log.warning(f"⚠️ Renew unexpected response: {data}")
            return {"success": False, "error": str(data)}
    except requests.exceptions.HTTPError as e:
        log.warning(f"⚠️ Token renew failed: {e.response.status_code}")
        return {"success": False, "error": str(e)}
    except Exception as e:
        log.error(f"❌ Renew request failed: {e}")
        return {"success": False, "error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
#  VERIFY TOKEN
# ─────────────────────────────────────────────────────────────────────────────

def verify_token(client_id: str, access_token: str) -> bool:
    if not access_token:
        return False
    url = "https://api.dhan.co/v2/profile"
    headers = {"access-token": access_token, "client-id": client_id}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            log.info("✅ Token is valid.")
            return True
        else:
            log.warning(f"⚠️ Token validation failed: {resp.status_code}")
            return False
    except Exception as e:
        log.warning(f"⚠️ Token check error: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
#  MASTER FUNCTION
# ─────────────────────────────────────────────────────────────────────────────

def get_fresh_token(config: dict, force_new: bool = False) -> str:
    client_id    = config["client_id"]
    pin          = config["pin"]
    totp_secret  = config["totp_secret"]
    access_token = config["access_token"]

    result = None

    # Try renewing existing token first (unless force_new)
    if access_token and not force_new:
        if verify_token(client_id, access_token):
            result = renew_token(client_id, access_token)
            if result["success"]:
                save_token_to_env(result["access_token"], result.get("expiry", ""))
                return result["access_token"]

    # Generate fresh token via TOTP
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


# ─────────────────────────────────────────────────────────────────────────────
#  DAEMON SCHEDULER
# ─────────────────────────────────────────────────────────────────────────────

def scheduled_refresh():
    log.info("=" * 60)
    log.info("⏰ Scheduled token refresh starting...")
    try:
        config = load_config()
        token  = get_fresh_token(config, force_new=True)
        log.info(f"✅ Refresh complete. Token: {token[:20]}...")
    except Exception as e:
        log.error(f"❌ Scheduled refresh failed: {e}")
    log.info("=" * 60)


def run_daemon(refresh_time: str = "08:00"):
    log.info(f"🚀 DhanTokenManager daemon started. Auto-refresh at {refresh_time}")
    scheduled_refresh()
    schedule.every().day.at(refresh_time).do(scheduled_refresh)
    while True:
        schedule.run_pending()
        time.sleep(30)


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dhan API v2 Auto Token Manager")
    parser.add_argument("--daemon",       action="store_true")
    parser.add_argument("--refresh-time", default="08:00")
    parser.add_argument("--force",        action="store_true")
    parser.add_argument("--verify",       action="store_true")
    args = parser.parse_args()

    if args.verify:
        cfg = load_config()
        print(f"Token valid: {verify_token(cfg['client_id'], cfg['access_token'])}")
    elif args.daemon:
        run_daemon(refresh_time=args.refresh_time)
    else:
        try:
            cfg   = load_config()
            token = get_fresh_token(cfg, force_new=args.force)
            print(f"\n{'='*60}\n✅ ACCESS TOKEN:\n   {token}\n{'='*60}\n")
        except Exception as e:
            log.error(f"Failed: {e}")
            exit(1)
