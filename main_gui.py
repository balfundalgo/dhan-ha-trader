"""
=============================================================================
Dhan WS HA Paper/Live Trader  |  GUI Edition  v7
=============================================================================
Tabs:
  🔑 Token Manager  — credentials, generate/verify token
  📈 Live Strategy  — trading mode, order type, per-symbol lot+buffer,
                      variation + KC/RSI params, Start/Stop/SquareOff
=============================================================================
"""

import os
import sys
import threading
from datetime import datetime
from pathlib import Path

import customtkinter as ctk
from tkinter import messagebox

# ── PyInstaller-safe base directory ──────────────────────────────────────────
if getattr(sys, 'frozen', False):
    BASE_DIR = Path(sys.executable).parent
else:
    BASE_DIR = Path(__file__).resolve().parent

ENV_FILE = BASE_DIR / ".env"


# ── .env helpers ──────────────────────────────────────────────────────────────
def _load_env() -> dict:
    data = {}
    if ENV_FILE.exists():
        with open(ENV_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                data[k.strip()] = v.strip().strip('"').strip("'")
    return data


def _save_env_key(key: str, value: str):
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
    os.environ[key] = value


# ── Palette ───────────────────────────────────────────────────────────────────
DARK_BG    = "#0d1117"
PANEL_BG   = "#161b22"
CARD_BG    = "#21262d"
ACCENT     = "#238636"
ACCENT_H   = "#2ea043"
RED_COL    = "#da3633"
RED_H      = "#b91c1c"
ORANGE_COL = "#d29922"
CYAN_COL   = "#58a6ff"
WHITE_COL  = "#e6edf3"
GREY_COL   = "#8b949e"
BORDER     = "#30363d"
LIVE_COL   = "#f85149"   # bright red for live mode warning

F_TITLE  = ("Segoe UI", 20, "bold")
F_HEAD   = ("Segoe UI", 15, "bold")
F_LABEL  = ("Segoe UI", 13)
F_BTN    = ("Segoe UI", 13, "bold")
F_MONO   = ("Consolas", 12)
F_MONO_S = ("Consolas", 11)
F_SMALL  = ("Segoe UI", 11)

ALL_SYMBOLS = ["CRUDEOILM", "GOLDPETAL", "SILVERMIC"]
TF_OPTIONS  = ["1m", "45m", "65m", "130m"]

VARIATION_OPTIONS = ["ha_static", "two_consecutive", "keltner", "rsi_keltner"]
VARIATION_LABELS  = {
    "ha_static":       "HA Static  (basic HA breakout)",
    "two_consecutive": "Two Consecutive  (HH / LL confirm)",
    "keltner":         "Keltner Channel  (HA + KC breakout)",
    "rsi_keltner":     "RSI + Keltner  (HA + KC + RSI filter)",
}
KC_VARIATIONS  = {"keltner", "rsi_keltner"}
RSI_VARIATIONS = {"rsi_keltner"}

ORDER_TYPES = ["MARKET", "SL-M", "LIMIT"]

SYM_BUF_DEFAULTS = {"CRUDEOILM": "3.0", "GOLDPETAL": "10.0", "SILVERMIC": "10.0"}

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("dark-blue")


# ══════════════════════════════════════════════════════════════════════════════
#  TOKEN MANAGER TAB
# ══════════════════════════════════════════════════════════════════════════════

class TokenTab(ctk.CTkFrame):
    def __init__(self, master, on_token_saved):
        super().__init__(master, fg_color=DARK_BG)
        self.on_token_saved = on_token_saved
        self._build()
        self._load_saved()

    def _build(self):
        ctk.CTkLabel(self, text="🔑  Dhan API — Token Manager",
                     font=F_TITLE, text_color=WHITE_COL).pack(pady=(30, 4))
        ctk.CTkLabel(self,
            text="Credentials saved locally in .env next to this app — never uploaded anywhere.",
            font=F_SMALL, text_color=GREY_COL).pack(pady=(0, 22))

        form = ctk.CTkFrame(self, fg_color=PANEL_BG, corner_radius=14)
        form.pack(padx=80, fill="x")

        def _row(label, show=""):
            row = ctk.CTkFrame(form, fg_color="transparent")
            row.pack(fill="x", padx=28, pady=10)
            ctk.CTkLabel(row, text=label, width=180, anchor="w",
                         font=F_LABEL, text_color=WHITE_COL).pack(side="left")
            e = ctk.CTkEntry(row, show=show, width=440, height=38,
                             fg_color=CARD_BG, border_color=BORDER,
                             text_color=WHITE_COL, font=F_MONO_S)
            e.pack(side="left", padx=(10, 0))
            return e

        ctk.CTkFrame(form, fg_color="transparent", height=10).pack()
        self.e_client = _row("Client ID")
        self.e_pin    = _row("PIN  (6-digit)", show="●")
        self.e_totp   = _row("TOTP Secret",    show="●")
        self.e_token  = _row("Access Token")
        ctk.CTkFrame(form, fg_color="transparent", height=10).pack()

        btn_row = ctk.CTkFrame(self, fg_color="transparent")
        btn_row.pack(pady=20)

        ctk.CTkButton(btn_row, text="💾  Save Credentials", width=200, height=42,
                      fg_color=CARD_BG, hover_color=BORDER, text_color=WHITE_COL,
                      font=F_BTN, command=self._save_creds).pack(side="left", padx=10)

        self.gen_btn = ctk.CTkButton(btn_row, text="⚡  Generate Token", width=200, height=42,
                      fg_color=ACCENT, hover_color=ACCENT_H, text_color=WHITE_COL,
                      font=F_BTN, command=self._generate_token)
        self.gen_btn.pack(side="left", padx=10)

        ctk.CTkButton(btn_row, text="✅  Verify Token", width=200, height=42,
                      fg_color=CARD_BG, hover_color=BORDER, text_color=WHITE_COL,
                      font=F_BTN, command=self._verify_token).pack(side="left", padx=10)

        ctk.CTkLabel(self, text="Log", anchor="w",
                     font=("Segoe UI", 12, "bold"), text_color=GREY_COL
                     ).pack(padx=80, anchor="w", pady=(14, 2))
        self.log_box = ctk.CTkTextbox(self, height=180, font=F_MONO_S,
                                      fg_color=PANEL_BG, text_color=WHITE_COL,
                                      border_color=BORDER, border_width=1)
        self.log_box.pack(padx=80, fill="x")
        self.log_box.configure(state="disabled")

        # ── Shared token section ──────────────────────────────────────────────
        shared_frame = ctk.CTkFrame(self, fg_color=PANEL_BG, corner_radius=12)
        shared_frame.pack(padx=80, fill="x", pady=(14, 0))

        ctk.CTkLabel(shared_frame,
                     text="🔗  dhan-token-generator  (shared token source)",
                     font=("Segoe UI", 12, "bold"), text_color=CYAN_COL
                     ).pack(side="left", padx=16, pady=10)

        self.shared_status_lbl = ctk.CTkLabel(
            shared_frame, text="Not detected", width=200,
            font=F_SMALL, text_color=GREY_COL)
        self.shared_status_lbl.pack(side="left", padx=10)

        ctk.CTkButton(
            shared_frame, text="🔄  Load from Token Generator", width=240, height=34,
            fg_color=CARD_BG, hover_color=BORDER, text_color=WHITE_COL, font=F_BTN,
            command=self._load_from_shared
        ).pack(side="right", padx=16, pady=8)

        # Check shared file on startup
        self.after(500, self._check_shared_token_status)

    def _load_saved(self):
        env = _load_env()
        self.e_client.insert(0, env.get("DHAN_CLIENT_ID", ""))
        self.e_pin.insert(0, env.get("DHAN_PIN", ""))
        self.e_totp.insert(0, env.get("DHAN_TOTP_SECRET", ""))
        self.e_token.insert(0, env.get("DHAN_ACCESS_TOKEN", ""))

    def _log(self, msg):
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_box.configure(state="normal")
        self.log_box.insert("end", f"[{ts}]  {msg}\n")
        self.log_box.see("end")
        self.log_box.configure(state="disabled")

    def _save_creds(self):
        _save_env_key("DHAN_CLIENT_ID",   self.e_client.get().strip())
        _save_env_key("DHAN_PIN",         self.e_pin.get().strip())
        _save_env_key("DHAN_TOTP_SECRET", self.e_totp.get().strip())
        token = self.e_token.get().strip()
        if token:
            _save_env_key("DHAN_ACCESS_TOKEN", token)
        self._log("✅  Credentials saved to .env")

    def _generate_token(self):
        self._save_creds()
        self.gen_btn.configure(state="disabled", text="⏳  Generating…")
        self._log("⏳  Generating token via TOTP …")

        def _run():
            try:
                from dhan_token_manager import load_config, get_fresh_token
                cfg   = load_config()
                token = get_fresh_token(cfg, force_new=True)
                _save_env_key("DHAN_ACCESS_TOKEN", token)
                cl = cfg["client_id"]; tp = token[:28]
                def _done():
                    self.e_token.delete(0, "end")
                    self.e_token.insert(0, token)
                    self._log(f"✅  Token generated: {tp}…")
                    self.on_token_saved(cl, token)
                    self.gen_btn.configure(state="normal", text="⚡  Generate Token")
                self.after(0, _done)
            except Exception as e:
                err = str(e)
                def _fail():
                    self._log(f"❌  Error: {err}")
                    self.gen_btn.configure(state="normal", text="⚡  Generate Token")
                self.after(0, _fail)

        threading.Thread(target=_run, daemon=True).start()

    def _verify_token(self):
        self._log("🔍  Verifying token …")
        def _run():
            try:
                from dhan_token_manager import load_config, verify_token
                cfg = load_config()
                valid = verify_token(cfg["client_id"], cfg["access_token"])
                if valid:
                    self.after(0, lambda: self._log("✅  Token is VALID — ready to trade."))
                    self.after(0, lambda: self.on_token_saved(cfg["client_id"], cfg["access_token"]))
                else:
                    self.after(0, lambda: self._log("❌  Token INVALID or expired. Click Generate Token."))
            except Exception as e:
                err = str(e)
                self.after(0, lambda: self._log(f"❌  Error: {err}"))
        threading.Thread(target=_run, daemon=True).start()

    def _check_shared_token_status(self):
        """Check if dhan-token-generator shared file exists and show status."""
        try:
            from dhan_token_manager import SHARED_TOKEN_FILE, read_shared_token
            shared = read_shared_token()
            if shared.get("access_token"):
                tok_preview = shared["access_token"][:20]
                self.shared_status_lbl.configure(
                    text=f"✅  Token found: {tok_preview}…",
                    text_color="#3fb950")
            elif SHARED_TOKEN_FILE.exists():
                self.shared_status_lbl.configure(
                    text="⚠️  File found but empty/invalid",
                    text_color=ORANGE_COL)
            else:
                self.shared_status_lbl.configure(
                    text=f"Not found at {SHARED_TOKEN_FILE}",
                    text_color=GREY_COL)
        except Exception as e:
            self.shared_status_lbl.configure(text=f"Error: {e}", text_color=RED_COL)

    def _load_from_shared(self):
        """Load token from dhan-token-generator shared file and apply it."""
        try:
            from dhan_token_manager import read_shared_token, SHARED_TOKEN_FILE
            shared = read_shared_token()
            if not shared.get("access_token"):
                self._log(f"❌  Shared token file not found or empty at {SHARED_TOKEN_FILE}")
                self._log("    Make sure dhan-token-generator EXE is running and has generated a token.")
                return
            token     = shared["access_token"]
            client_id = shared.get("client_id", self.e_client.get().strip())

            # Update the Access Token field
            self.e_token.delete(0, "end")
            self.e_token.insert(0, token)
            # Also save to .env
            _save_env_key("DHAN_ACCESS_TOKEN", token)
            if client_id:
                _save_env_key("DHAN_CLIENT_ID", client_id)

            self._log(f"✅  Token loaded from dhan-token-generator: {token[:28]}…")
            self.on_token_saved(client_id, token)
            self._check_shared_token_status()
        except Exception as e:
            self._log(f"❌  Error loading shared token: {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  STRATEGY TAB
# ══════════════════════════════════════════════════════════════════════════════

class StrategyTab(ctk.CTkFrame):
    def __init__(self, master):
        super().__init__(master, fg_color=DARK_BG)
        self._client_id    = ""
        self._access_token = ""
        self._app          = None
        self._running      = False
        self._build()

    def set_credentials(self, client_id: str, token: str):
        self._client_id    = client_id
        self._access_token = token

    def _build(self):
        # ── Top bar ──────────────────────────────────────────────────────────
        top = ctk.CTkFrame(self, fg_color=PANEL_BG, corner_radius=0)
        top.pack(fill="x")
        ctk.CTkLabel(top, text="📈  Live Strategy Dashboard",
                     font=F_HEAD, text_color=WHITE_COL).pack(side="left", padx=20, pady=14)
        self.status_lbl = ctk.CTkLabel(
            top, text="⏹  Stopped", width=160, height=34,
            fg_color=CARD_BG, corner_radius=8, font=F_BTN, text_color=GREY_COL)
        self.status_lbl.pack(side="right", padx=20)

        # ── Row 1: TF + Variation ─────────────────────────────────────────────
        row1 = ctk.CTkFrame(self, fg_color=CARD_BG, corner_radius=10)
        row1.pack(fill="x", padx=14, pady=(10, 4))

        ctk.CTkLabel(row1, text="Timeframe:", font=F_LABEL,
                     text_color=WHITE_COL).pack(side="left", padx=(16, 6), pady=10)
        self.tf_dd = ctk.CTkOptionMenu(
            row1, values=TF_OPTIONS, width=110, height=36,
            fg_color=PANEL_BG, button_color=BORDER, button_hover_color=ACCENT,
            text_color=WHITE_COL, font=F_LABEL, dropdown_font=F_LABEL)
        self.tf_dd.set("65m")
        self.tf_dd.pack(side="left", padx=6)

        ctk.CTkLabel(row1, text="Variation:", font=F_LABEL,
                     text_color=WHITE_COL).pack(side="left", padx=(18, 6))
        self.var_dd = ctk.CTkOptionMenu(
            row1, values=[VARIATION_LABELS[v] for v in VARIATION_OPTIONS],
            width=310, height=36,
            fg_color=PANEL_BG, button_color=BORDER, button_hover_color=ACCENT,
            text_color=WHITE_COL, font=F_LABEL, dropdown_font=F_LABEL,
            command=self._on_variation_change)
        self.var_dd.set(VARIATION_LABELS["ha_static"])
        self.var_dd.pack(side="left", padx=6)

        # ── Row 2: Symbol checkboxes + per-symbol lot + buffer ────────────────
        row2 = ctk.CTkFrame(self, fg_color=CARD_BG, corner_radius=10)
        row2.pack(fill="x", padx=14, pady=(0, 4))

        ctk.CTkLabel(row2, text="Symbols:", font=F_LABEL,
                     text_color=WHITE_COL).pack(side="left", padx=(16, 10), pady=10)

        self._sym_vars    = {}
        self._lot_entries = {}
        self._buf_entries = {}

        self._all_var = ctk.BooleanVar(value=True)
        ctk.CTkCheckBox(row2, text="All", variable=self._all_var,
                        font=F_LABEL, text_color=WHITE_COL,
                        fg_color=ACCENT, hover_color=ACCENT_H,
                        command=self._on_all_toggled).pack(side="left", padx=(0, 14))

        for sym in ALL_SYMBOLS:
            grp = ctk.CTkFrame(row2, fg_color=PANEL_BG, corner_radius=8)
            grp.pack(side="left", padx=5, pady=8)

            var = ctk.BooleanVar(value=True)
            ctk.CTkCheckBox(grp, text=sym, variable=var,
                            font=F_LABEL, text_color=WHITE_COL,
                            fg_color=ACCENT, hover_color=ACCENT_H,
                            command=self._on_sym_toggled).pack(side="left", padx=(10, 4), pady=6)

            ctk.CTkLabel(grp, text="lot:", font=("Segoe UI", 11),
                         text_color=GREY_COL).pack(side="left")
            lot_e = ctk.CTkEntry(grp, width=52, height=28, placeholder_text="auto",
                                 fg_color=CARD_BG, border_color=BORDER,
                                 text_color=WHITE_COL, font=F_MONO_S)
            lot_e.pack(side="left", padx=(2, 6), pady=6)

            ctk.CTkLabel(grp, text="buf:", font=("Segoe UI", 11),
                         text_color=GREY_COL).pack(side="left")
            buf_e = ctk.CTkEntry(grp, width=52, height=28,
                                 fg_color=CARD_BG, border_color=BORDER,
                                 text_color=WHITE_COL, font=F_MONO_S)
            buf_e.insert(0, SYM_BUF_DEFAULTS.get(sym, "3.0"))
            buf_e.pack(side="left", padx=(2, 10), pady=6)

            self._sym_vars[sym]    = var
            self._lot_entries[sym] = lot_e
            self._buf_entries[sym] = buf_e

        # ── Row 3: Trading Mode + Order Type + Offsets ────────────────────────
        row3 = ctk.CTkFrame(self, fg_color=CARD_BG, corner_radius=10)
        row3.pack(fill="x", padx=14, pady=(0, 4))

        ctk.CTkLabel(row3, text="Trading Mode:", font=F_LABEL,
                     text_color=WHITE_COL).pack(side="left", padx=(16, 8), pady=10)

        self._mode_var = ctk.StringVar(value="PAPER")
        ctk.CTkRadioButton(row3, text="📄 Paper", variable=self._mode_var, value="PAPER",
                           font=F_LABEL, text_color=WHITE_COL,
                           fg_color=ACCENT, hover_color=ACCENT_H,
                           command=self._on_mode_change).pack(side="left", padx=6)
        ctk.CTkRadioButton(row3, text="🔴 Live", variable=self._mode_var, value="LIVE",
                           font=F_LABEL, text_color=LIVE_COL,
                           fg_color=LIVE_COL, hover_color="#c0392b",
                           command=self._on_mode_change).pack(side="left", padx=6)

        self.live_warn_lbl = ctk.CTkLabel(
            row3, text="",
            font=("Segoe UI", 11, "bold"), text_color=LIVE_COL)
        self.live_warn_lbl.pack(side="left", padx=(6, 20))

        ctk.CTkLabel(row3, text="Order Type:", font=F_LABEL,
                     text_color=WHITE_COL).pack(side="left", padx=(4, 6))
        self.order_dd = ctk.CTkOptionMenu(
            row3, values=ORDER_TYPES, width=130, height=36,
            fg_color=PANEL_BG, button_color=BORDER, button_hover_color=ACCENT,
            text_color=WHITE_COL, font=F_LABEL, dropdown_font=F_LABEL,
            command=self._on_order_type_change)
        self.order_dd.set("MARKET")
        self.order_dd.pack(side="left", padx=6)

        # Offset inputs — shown/hidden based on order type
        self.trig_frame = ctk.CTkFrame(row3, fg_color="transparent")
        ctk.CTkLabel(self.trig_frame, text="Trigger Offset:", font=F_LABEL,
                     text_color=WHITE_COL).pack(side="left", padx=(10, 6))
        self.e_trig_offset = ctk.CTkEntry(self.trig_frame, width=80, height=36,
                                           fg_color=PANEL_BG, border_color=BORDER,
                                           text_color=WHITE_COL, font=F_MONO_S)
        self.e_trig_offset.insert(0, "2.0")
        self.e_trig_offset.pack(side="left")
        ctk.CTkLabel(self.trig_frame, text="pts", font=F_SMALL,
                     text_color=GREY_COL).pack(side="left", padx=(4, 0))

        self.lmt_frame = ctk.CTkFrame(row3, fg_color="transparent")
        ctk.CTkLabel(self.lmt_frame, text="Limit Offset:", font=F_LABEL,
                     text_color=WHITE_COL).pack(side="left", padx=(10, 6))
        self.e_lmt_offset = ctk.CTkEntry(self.lmt_frame, width=80, height=36,
                                          fg_color=PANEL_BG, border_color=BORDER,
                                          text_color=WHITE_COL, font=F_MONO_S)
        self.e_lmt_offset.insert(0, "5.0")
        self.e_lmt_offset.pack(side="left")
        ctk.CTkLabel(self.lmt_frame, text="pts", font=F_SMALL,
                     text_color=GREY_COL).pack(side="left", padx=(4, 0))

        # Initially hide both offset frames (MARKET selected)
        # They'll show when order type changes

        # ── Row 4: KC / RSI params (conditional) ─────────────────────────────
        self.params_row = ctk.CTkFrame(self, fg_color=CARD_BG, corner_radius=10)
        # Not packed initially

        def _param(parent, label, default, width=80):
            ctk.CTkLabel(parent, text=label, font=F_LABEL,
                         text_color=GREY_COL).pack(side="left", padx=(14, 4))
            e = ctk.CTkEntry(parent, width=width, height=32,
                             fg_color=PANEL_BG, border_color=BORDER,
                             text_color=WHITE_COL, font=F_MONO_S)
            e.insert(0, str(default))
            e.pack(side="left", padx=(0, 6), pady=8)
            return e

        self.kc_frame = ctk.CTkFrame(self.params_row, fg_color="transparent")
        ctk.CTkLabel(self.kc_frame, text="Keltner →", font=("Segoe UI", 11, "bold"),
                     text_color=CYAN_COL).pack(side="left", padx=(14, 8))
        self.e_kc_len  = _param(self.kc_frame, "KC Length",  21)
        self.e_kc_atr  = _param(self.kc_frame, "ATR Length", 21)
        self.e_kc_mult = _param(self.kc_frame, "Multiplier", 0.5)

        self.rsi_frame = ctk.CTkFrame(self.params_row, fg_color="transparent")
        ctk.CTkLabel(self.rsi_frame, text="RSI →", font=("Segoe UI", 11, "bold"),
                     text_color=ORANGE_COL).pack(side="left", padx=(14, 8))
        self.e_rsi_len  = _param(self.rsi_frame, "RSI Length", 14)
        self.e_rsi_buy  = _param(self.rsi_frame, "Buy Level",  52.0)
        self.e_rsi_sell = _param(self.rsi_frame, "Sell Level", 32.0)

        # ── Row 5: Buttons ────────────────────────────────────────────────────
        btn_row = ctk.CTkFrame(self, fg_color=CARD_BG, corner_radius=10)
        btn_row.pack(fill="x", padx=14, pady=(0, 8))

        self.start_btn = ctk.CTkButton(
            btn_row, text="▶  Start", width=130, height=38,
            fg_color=ACCENT, hover_color=ACCENT_H,
            text_color=WHITE_COL, font=F_BTN, command=self._start)
        self.start_btn.pack(side="left", padx=14, pady=10)

        self.stop_btn = ctk.CTkButton(
            btn_row, text="■  Stop", width=130, height=38,
            fg_color=RED_COL, hover_color=RED_H,
            text_color=WHITE_COL, font=F_BTN,
            state="disabled", command=self._stop)
        self.stop_btn.pack(side="left", padx=4)

        self.squareoff_btn = ctk.CTkButton(
            btn_row, text="⬛  Square Off All", width=190, height=38,
            fg_color=ORANGE_COL, hover_color="#b45309",
            text_color=WHITE_COL, font=F_BTN,
            state="disabled", command=self._square_off)
        self.squareoff_btn.pack(side="left", padx=12)

        self.info_lbl = ctk.CTkLabel(btn_row, text="", font=F_SMALL, text_color=GREY_COL)
        self.info_lbl.pack(side="left", padx=12)

        # ── Dashboard ─────────────────────────────────────────────────────────
        self.dash = ctk.CTkTextbox(self, font=F_MONO,
                                   fg_color=PANEL_BG, text_color=WHITE_COL,
                                   border_color=BORDER, border_width=1, wrap="none")
        self.dash.pack(fill="both", expand=True, padx=14, pady=(0, 6))
        self.dash.configure(state="disabled")

        # ── Event log ─────────────────────────────────────────────────────────
        ctk.CTkLabel(self, text="Event Log", anchor="w",
                     font=("Segoe UI", 12, "bold"), text_color=GREY_COL
                     ).pack(padx=14, anchor="w")
        self.event_log = ctk.CTkTextbox(self, height=110, font=F_MONO_S,
                                         fg_color=PANEL_BG, text_color=WHITE_COL,
                                         border_color=BORDER, border_width=1)
        self.event_log.pack(fill="x", padx=14, pady=(2, 12))
        self.event_log.configure(state="disabled")

    # ── Checkbox callbacks ────────────────────────────────────────────────────
    def _on_all_toggled(self):
        val = self._all_var.get()
        for var in self._sym_vars.values():
            var.set(val)

    def _on_sym_toggled(self):
        self._all_var.set(all(v.get() for v in self._sym_vars.values()))

    # ── Mode / order type callbacks ───────────────────────────────────────────
    def _on_mode_change(self):
        if self._mode_var.get() == "LIVE":
            self.live_warn_lbl.configure(
                text="⚠️  REAL ORDERS WILL BE PLACED ON YOUR ACCOUNT")
        else:
            self.live_warn_lbl.configure(text="")

    def _on_order_type_change(self, ot: str):
        """Show/hide trigger and limit offset inputs based on order type."""
        self.trig_frame.pack_forget()
        self.lmt_frame.pack_forget()
        if ot == "SL-M":
            self.trig_frame.pack(side="left")
        elif ot == "LIMIT":
            self.lmt_frame.pack(side="left")
        # MARKET: nothing shown

    # ── Variation callbacks ───────────────────────────────────────────────────
    def _on_variation_change(self, label: str):
        var = self._label_to_variation(label)
        show_kc  = var in KC_VARIATIONS
        show_rsi = var in RSI_VARIATIONS

        if show_kc or show_rsi:
            self.params_row.pack(fill="x", padx=14, pady=(0, 4))
            if show_kc:
                self.kc_frame.pack(side="left")
            else:
                self.kc_frame.pack_forget()
            if show_rsi:
                self.rsi_frame.pack(side="left", padx=(10, 0))
            else:
                self.rsi_frame.pack_forget()
        else:
            self.params_row.pack_forget()

    def _label_to_variation(self, label: str) -> str:
        for v, l in VARIATION_LABELS.items():
            if l == label:
                return v
        return "ha_static"

    # ── Helpers ───────────────────────────────────────────────────────────────
    def _elog(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        self.event_log.configure(state="normal")
        self.event_log.insert("end", f"[{ts}]  {msg}\n")
        self.event_log.see("end")
        self.event_log.configure(state="disabled")

    def _set_dash(self, text: str):
        self.dash.configure(state="normal")
        self.dash.delete("1.0", "end")
        self.dash.insert("end", text)
        self.dash.configure(state="disabled")

    def _get_tf(self) -> int:
        return int(self.tf_dd.get().replace("m", ""))

    def _get_variation(self) -> str:
        return self._label_to_variation(self.var_dd.get())

    def _get_symbols_filter(self):
        selected = [s for s, v in self._sym_vars.items() if v.get()]
        return None if len(selected) == len(ALL_SYMBOLS) else selected

    def _get_lot_overrides(self) -> dict:
        result = {}
        for sym, entry in self._lot_entries.items():
            val = entry.get().strip()
            try:
                result[sym] = int(val)
            except (ValueError, TypeError):
                pass
        return result

    def _get_buf_overrides(self) -> dict:
        result = {}
        for sym, entry in self._buf_entries.items():
            if not self._sym_vars[sym].get():
                continue
            val = entry.get().strip()
            try:
                result[sym] = float(val)
            except (ValueError, TypeError):
                pass
        return result

    def _get_order_type(self) -> str:
        return self.order_dd.get()

    def _get_trigger_offset(self) -> float:
        try:
            return float(self.e_trig_offset.get())
        except (ValueError, TypeError):
            return 0.0

    def _get_limit_offset(self) -> float:
        try:
            return float(self.e_lmt_offset.get())
        except (ValueError, TypeError):
            return 0.0

    def _get_kc_params(self) -> dict:
        try:
            return {"kc_length": int(self.e_kc_len.get()),
                    "kc_atr_length": int(self.e_kc_atr.get()),
                    "kc_multiplier": float(self.e_kc_mult.get())}
        except Exception:
            return {"kc_length": 21, "kc_atr_length": 21, "kc_multiplier": 0.5}

    def _get_rsi_params(self) -> dict:
        try:
            return {"rsi_length": int(self.e_rsi_len.get()),
                    "rsi_buy_level": float(self.e_rsi_buy.get()),
                    "rsi_sell_level": float(self.e_rsi_sell.get())}
        except Exception:
            return {"rsi_length": 14, "rsi_buy_level": 52.0, "rsi_sell_level": 32.0}

    # ── Start ─────────────────────────────────────────────────────────────────
    def _start(self):
        if self._running:
            return

        if not self._client_id or not self._access_token:
            env = _load_env()
            self._client_id    = env.get("DHAN_CLIENT_ID", "")
            self._access_token = env.get("DHAN_ACCESS_TOKEN", "")

        # Also try shared token from dhan-token-generator as final fallback
        if not self._access_token:
            try:
                from dhan_token_manager import read_shared_token
                shared = read_shared_token()
                if shared.get("access_token"):
                    self._access_token = shared["access_token"]
                    if shared.get("client_id"):
                        self._client_id = shared["client_id"]
            except Exception:
                pass

        if not self._client_id or not self._access_token:
            messagebox.showerror("No Credentials",
                "Please go to Token Manager, enter credentials and generate a token first.")
            return

        selected = [s for s, v in self._sym_vars.items() if v.get()]
        if not selected:
            messagebox.showerror("No Symbols", "Please select at least one symbol.")
            return

        is_live = self._mode_var.get() == "LIVE"
        if is_live:
            if not messagebox.askyesno(
                "⚠️  LIVE TRADING CONFIRMATION",
                "You are about to start LIVE trading.\n\n"
                "REAL orders will be placed on your Dhan account.\n\n"
                "Are you absolutely sure you want to proceed?"
            ):
                return

        tf_val       = self._get_tf()
        sym_filter   = self._get_symbols_filter()
        variation    = self._get_variation()
        lot_ov       = self._get_lot_overrides()
        buf_ov       = self._get_buf_overrides()
        order_type   = self._get_order_type()
        trig_off     = self._get_trigger_offset()
        lmt_off      = self._get_limit_offset()
        kc           = self._get_kc_params()
        rsi          = self._get_rsi_params()
        sym_str      = ", ".join(selected) if sym_filter else "All"
        mode_str     = "🔴 LIVE" if is_live else "📄 Paper"

        self.start_btn.configure(state="disabled")
        self.stop_btn.configure(state="normal")
        self.squareoff_btn.configure(state="normal")
        self.status_lbl.configure(
            text="⏳  Starting…",
            text_color=LIVE_COL if is_live else ORANGE_COL)
        self._elog(f"Starting — {mode_str}  TF={tf_val}m  {variation}  {order_type}  Symbols={sym_str}")
        self.info_lbl.configure(
            text=f"{mode_str}  |  TF={tf_val}m  |  {variation}  |  {order_type}  |  {sym_str}")

        def _run():
            try:
                from main import TradingApp
                self._app = TradingApp(
                    strategy_tf=tf_val,
                    symbols_filter=sym_filter,
                    variation=variation,
                    lot_size_overrides=lot_ov,
                    buffer_overrides=buf_ov,
                    live_mode=is_live,
                    order_type=order_type,
                    trigger_offset=trig_off,
                    limit_offset=lmt_off,
                    client_id=self._client_id,
                    access_token=self._access_token,
                    **kc, **rsi,
                )
                self._app.start(with_terminal_ui=False)
                self._running = True
                run_col = LIVE_COL if is_live else "#3fb950"
                run_txt = "🔴  LIVE" if is_live else "🟢  Running"
                self.after(0, lambda: self.status_lbl.configure(text=run_txt, text_color=run_col))
                self.after(0, lambda: self._elog("✅  Strategy started successfully."))
                self.after(0, self._poll_dashboard)
            except Exception as e:
                err = str(e)
                self._running = False
                self.after(0, lambda: self._elog(f"❌  Start error: {err}"))
                self.after(0, lambda: self.status_lbl.configure(text="❌  Error", text_color=RED_COL))
                self.after(0, lambda: self.start_btn.configure(state="normal"))
                self.after(0, lambda: self.stop_btn.configure(state="disabled"))
                self.after(0, lambda: self.squareoff_btn.configure(state="disabled"))

        threading.Thread(target=_run, daemon=True).start()

    def _stop(self):
        if not self._running or self._app is None:
            return
        self._running = False
        self._elog("Stopping strategy …")
        self.status_lbl.configure(text="⏹  Stopping…", text_color=ORANGE_COL)

        def _run():
            try:
                self._app.stop()
            except Exception:
                pass
            self._app = None
            self.after(0, lambda: self.status_lbl.configure(text="⏹  Stopped", text_color=GREY_COL))
            self.after(0, lambda: self.start_btn.configure(state="normal"))
            self.after(0, lambda: self.stop_btn.configure(state="disabled"))
            self.after(0, lambda: self.squareoff_btn.configure(state="disabled"))
            self.after(0, lambda: self.info_lbl.configure(text=""))
            self.after(0, lambda: self._elog("✅  Strategy stopped."))

        threading.Thread(target=_run, daemon=True).start()

    def _square_off(self):
        if not self._running or self._app is None:
            return
        is_live = self._app.live_mode if self._app else False
        msg = ("Close ALL open LIVE positions at market price?\n\nThis will place real MARKET orders.\n\nAre you sure?"
               if is_live else
               "Close ALL open paper positions at current LTP?\n\nAre you sure?")
        if not messagebox.askyesno("Square Off All", msg):
            return
        self._elog("⬛  Squaring off all positions …")

        def _run():
            try:
                self._app.square_off_all()
                self.after(0, lambda: self._elog("✅  All positions squared off."))
            except Exception as e:
                err = str(e)
                self.after(0, lambda: self._elog(f"❌  Square off error: {err}"))

        threading.Thread(target=_run, daemon=True).start()

    # ── Dashboard ─────────────────────────────────────────────────────────────
    def _poll_dashboard(self):
        if not self._running or self._app is None:
            return
        try:
            snap = self._app.get_snapshot()
            self._render_dashboard(snap)
        except Exception as e:
            self._elog(f"Dashboard error: {e}")
        self.after(1000, self._poll_dashboard)

    def _render_dashboard(self, snap: dict):
        lines = []
        now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        pkt  = snap["packets"]
        mode = "🔴 LIVE" if snap.get("live_mode") else "📄 Paper"
        ot   = snap.get("order_type", "MARKET")
        toff = snap.get("trigger_offset", 0.0)
        loff = snap.get("limit_offset", 0.0)

        # Build offset string for header
        if ot == "SL-M":
            ot_str = f"SL-M (trig+{toff}pts)"
        elif ot == "LIMIT":
            ot_str = f"LIMIT (lmt+{loff}pts)"
        else:
            ot_str = "MARKET"

        lines.append(
            f"  {mode}  │  TF {snap['strategy_tf']}m  │  {snap['variation']}  │  "
            f"Order: {ot_str}  │  {now}  │  WS: {snap['ws_uptime']}"
        )
        lines.append("─" * 172)
        lines.append(
            f"  {'Symbol':<14}  {'Contract':<22}  {'LTP':>10}  {'Pos':>6}  "
            f"{'Entry':>10}  {'Pending':>7}  {'Trigger':>10}  "
            f"{'Buf':>6}  {'Lot':>5}  {'uPnL':>12}  {'rPnL':>12}  {'SL':>10}  {'Event':<28}"
        )
        lines.append("─" * 172)

        for s in snap["symbols"]:
            prec     = s["prec"]
            ltp      = f"{s['ltp']:.{prec}f}"    if s["ltp"]     is not None else "-"
            entry    = f"{s['entry']:.{prec}f}"   if s["entry"]   is not None else "-"
            trig     = f"{s['trigger']:.{prec}f}" if s["trigger"] is not None else "-"
            sl       = f"{s['sl_price']:.{prec}f}" if s.get("sl_price") else "-"
            upnl     = f"{s['unrealized']:>+.2f}"
            rpnl     = f"{s['realized']:>+.2f}"
            ha_str   = f"{s['ha_color']} x{s['ha_streak']}" if s["ha_color"] != "-" else "-"
            contract = str(s.get("contract_display") or "-")[:22]
            buf_val  = f"{s['buffer']:.1f}"
            lines.append(
                f"  {s['name'][:14]:<14}  {contract:<22}  {ltp:>10}  {s['position']:>6}  "
                f"{entry:>10}  {s['pending']:>7}  {trig:>10}  "
                f"{buf_val:>6}  {s['lot']:>5}  {upnl:>12}  {rpnl:>12}  "
                f"{sl:>10}  {str(s['event'])[:28]:<28}"
            )

        lines.append("─" * 172)
        lines.append(
            f"  {'TOTAL':<38}  {'':>10}  {'':>6}  {'':>10}  {'':>7}  {'':>10}  "
            f"{'':>6}  {'':>5}  {snap['total_unrealized']:>+12.2f}  {snap['total_realized']:>+12.2f}"
        )
        lines.append("")
        lines.append(
            f"  Packets: ticker={pkt.get(2,0)}  prev_close={pkt.get(6,0)}  "
            f"other={pkt.get('other',0)}  disconnect={pkt.get(50,0)}"
        )
        if snap["ws_error"]:
            lines.append(f"  ⚠️  WS Error: {snap['ws_error']}")

        lines.append("")
        lines.append("  Last 5 HA Candles")
        lines.append(
            f"  {'Symbol':<14}  {'Time':>5}  {'Open':>10}  "
            f"{'High':>10}  {'Low':>10}  {'Close':>10}  {'Color':>6}  {'Streak':>6}"
        )
        lines.append("  " + "─" * 80)

        from market_data import epoch_to_local_str
        for s in snap["symbols"]:
            hist = list(s["ha_history"])
            if not hist:
                continue
            first = True
            for row in reversed(hist):
                prec  = s["prec"]
                label = s["name"][:14] if first else " " * 14
                first = False
                t_str = epoch_to_local_str(int(row["bucket"]), False)
                lines.append(
                    f"  {label:<14}  {t_str:>5}  "
                    f"{float(row['open']):>10.{prec}f}  {float(row['high']):>10.{prec}f}  "
                    f"{float(row['low']):>10.{prec}f}  {float(row['close']):>10.{prec}f}  "
                    f"{row['color']:>6}  {int(row['streak']):>6}"
                )

        self._set_dash("\n".join(lines))


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN WINDOW
# ══════════════════════════════════════════════════════════════════════════════

class MainApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Dhan HA Paper/Live Trader  |  Balfund Trading Pvt. Ltd.")
        self.geometry("1450x960")
        self.minsize(1150, 760)
        self.configure(fg_color=DARK_BG)
        self._build()

    def _build(self):
        hdr = ctk.CTkFrame(self, fg_color=PANEL_BG, corner_radius=0, height=52)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        ctk.CTkLabel(hdr, text="  BALFUND TRADING PVT. LTD.  |  Dhan HA Trader",
                     font=("Segoe UI", 14, "bold"), text_color=CYAN_COL).pack(side="left", padx=18)
        ctk.CTkLabel(hdr, text="Paper & Live Trading — Use Live Mode with caution",
                     font=F_SMALL, text_color=GREY_COL).pack(side="right", padx=18)

        tabs = ctk.CTkTabview(
            self, fg_color=DARK_BG,
            segmented_button_fg_color=PANEL_BG,
            segmented_button_selected_color=ACCENT,
            segmented_button_unselected_color=PANEL_BG,
            segmented_button_selected_hover_color=ACCENT_H,
            text_color=WHITE_COL)
        tabs.pack(fill="both", expand=True)
        tabs.add("🔑  Token Manager")
        tabs.add("📈  Live Strategy")

        self.strategy_tab = StrategyTab(tabs.tab("📈  Live Strategy"))
        self.strategy_tab.pack(fill="both", expand=True)

        self.token_tab = TokenTab(tabs.tab("🔑  Token Manager"),
                                  on_token_saved=self._on_token_saved)
        self.token_tab.pack(fill="both", expand=True)

    def _on_token_saved(self, client_id: str, token: str):
        self.strategy_tab.set_credentials(client_id, token)

    def on_closing(self):
        if self.strategy_tab._running and self.strategy_tab._app:
            self.strategy_tab._app.stop()
        self.destroy()


if __name__ == "__main__":
    app = MainApp()
    app.protocol("WM_DELETE_WINDOW", app.on_closing)
    app.mainloop()
