"""
=============================================================================
Dhan API v2 — Auto Token Manager
=============================================================================
Automatically generates and renews your Dhan Access Token daily.
Uses TOTP + PIN for fully automatic token generation.
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
from pathlib import Path

import requests
import pyotp
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("DhanTokenManager")

# ── PyInstaller-safe .env path ────────────────────────────────────────────────
if getattr(sys, 'frozen', False):
    _BASE = Path(sys.executable).parent
else:
    _BASE = Path(__file__).resolve().parent

ENV_FILE = _BASE / ".env"

# ── Shared token file (written by dhan-token-generator EXE) ──────────────────
# Windows: C:\balfund_shared\dhan_token.json  (auto-created)
# Mac/Linux (dev): ~/balfund_shared/dhan_token.json  (auto-created)
if platform.system() == "Windows":
    SHARED_TOKEN_FILE = Path("C:/balfund_shared/dhan_token.json")
else:
    SHARED_TOKEN_FILE = Path.home() / "balfund_shared" / "dhan_token.json"

# Auto-create the shared folder on any PC — no manual setup needed
SHARED_TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)


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
            log.info("Shared token loaded from %s", SHARED_TOKEN_FILE)
            return {"client_id": client_id, "access_token": access_token}
    except Exception as e:
        log.warning("Could not read shared token file: %s", e)
    return {}


def _save_env_key(key: str, value: str):
    """Write/update key in .env AND update os.environ immediately."""
    lines = []
    found = False
    if ENV_FILE.exists():
        with open(ENV_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        for i, line in enumerate(lines):
            if line.strip().startswith(f"{key}=") or line.strip().startswith(f"{key} ="):
                lines[i] = f"{key}={value}\n"
                found = True
                break
    if not found:
        lines.append(f"{key}={value}\n")
    with open(ENV_FILE, "w", encoding="utf-8") as f:
        f.writelines(lines)
    os.environ[key] = value  # immediately visible in same process


def load_config() -> dict:
    """
    Always re-reads .env with override=True — prevents stale os.environ cache.
    If shared token file (from dhan-token-generator) exists, its token takes priority.
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

    # Shared file from dhan-token-generator takes priority over .env token
    shared = read_shared_token()
    if shared.get("access_token"):
        config["access_token"] = shared["access_token"]
        log.info("Using token from dhan-token-generator shared file.")

    return config


def save_token_to_env(access_token: str, expiry: str = ""):
    _save_env_key("DHAN_ACCESS_TOKEN", access_token)
    if expiry:
        _save_env_key("DHAN_TOKEN_EXPIRY", expiry)
    log.info("Token saved to %s", ENV_FILE)


def generate_totp(totp_secret: str) -> str:
    totp = pyotp.TOTP(totp_secret)
    code = totp.now()
    log.info("Generated TOTP: %s (valid ~%ds)", code, 30 - (int(time.time()) % 30))
    return code


def generate_token_via_totp(client_id: str, pin: str, totp_secret: str) -> dict:
    totp_code = generate_totp(totp_secret)
    url = (f"https://auth.dhan.co/app/generateAccessToken"
           f"?dhanClientId={client_id}&pin={pin}&totp={totp_code}")
    log.info("Requesting new token via TOTP for client %s...", client_id)
    try:
        resp = requests.post(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if "accessToken" in data:
            log.info("✅ Token generated! Expires: %s", data.get('expiryTime', 'N/A'))
            return {"success": True, "access_token": data["accessToken"],
                    "expiry": data.get("expiryTime", ""), "method": "TOTP"}
        log.error("❌ Token generation failed: %s", data)
        return {"success": False, "error": str(data)}
    except requests.exceptions.HTTPError as e:
        log.error("❌ HTTP error: %s — %s", e.response.status_code, e.response.text)
        return {"success": False, "error": str(e)}
    except Exception as e:
        log.error("❌ Request failed: %s", e)
        return {"success": False, "error": str(e)}


def renew_token(client_id: str, access_token: str) -> dict:
    headers = {"access-token": access_token, "dhanClientId": client_id,
               "Content-Type": "application/json"}
    try:
        resp = requests.get("https://api.dhan.co/v2/RenewToken", headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if "accessToken" in data:
            log.info("✅ Token renewed! Expires: %s", data.get('expiryTime', 'N/A'))
            return {"success": True, "access_token": data["accessToken"],
                    "expiry": data.get("expiryTime", ""), "method": "RENEW"}
        return {"success": False, "error": str(data)}
    except Exception as e:
        log.warning("⚠️ Token renew failed: %s", e)
        return {"success": False, "error": str(e)}


def verify_token(client_id: str, access_token: str) -> bool:
    if not access_token:
        return False
    try:
        resp = requests.get("https://api.dhan.co/v2/profile",
                            headers={"access-token": access_token, "client-id": client_id},
                            timeout=10)
        if resp.status_code == 200:
            log.info("✅ Token is valid.")
            return True
        log.warning("⚠️ Token validation failed: %s", resp.status_code)
        return False
    except Exception as e:
        log.warning("⚠️ Token check error: %s", e)
        return False


def get_fresh_token(config: dict, force_new: bool = False) -> str:
    client_id = config["client_id"]
    result = None

    if config["access_token"] and not force_new:
        if verify_token(client_id, config["access_token"]):
            result = renew_token(client_id, config["access_token"])
            if result["success"]:
                save_token_to_env(result["access_token"], result.get("expiry", ""))
                return result["access_token"]

    if config["totp_secret"] and config["pin"]:
        result = generate_token_via_totp(client_id, config["pin"], config["totp_secret"])
        if result["success"]:
            save_token_to_env(result["access_token"], result.get("expiry", ""))
            return result["access_token"]
    else:
        log.error("❌ Cannot generate token: DHAN_PIN or DHAN_TOTP_SECRET missing")

    raise RuntimeError(f"Token generation failed: {result.get('error', 'Unknown') if result else 'No result'}")


def scheduled_refresh():
    log.info("=" * 60)
    log.info("⏰ Scheduled token refresh...")
    try:
        config = load_config()
        token = get_fresh_token(config, force_new=True)
        log.info("✅ Refresh complete. Token: %s...", token[:20])
    except Exception as e:
        log.error("❌ Refresh failed: %s", e)
    log.info("=" * 60)


def run_daemon(refresh_time: str = "08:00"):
    log.info("🚀 Token daemon started. Auto-refresh at %s", refresh_time)
    scheduled_refresh()
    schedule.every().day.at(refresh_time).do(scheduled_refresh)
    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
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
            cfg = load_config()
            token = get_fresh_token(cfg, force_new=args.force)
            print(f"\n{'='*60}\n✅ ACCESS TOKEN:\n   {token}\n{'='*60}\n")
        except Exception as e:
            log.error("Failed: %s", e)
            exit(1)
