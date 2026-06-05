"""
=============================================================================
Dhan HA Trader  |  Standalone CLI  (v9)
=============================================================================
Run directly in VS Code / terminal:

    python main.py                           ← uses CONFIG block below
    python main.py --tf 45 --variation two_consecutive
    python main.py --tf 65 --variation ha_static --symbols crude
    python main.py --tf 45 --variation two_consecutive --live

Credentials: fill in .env file next to this script.
=============================================================================
"""

# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  CONFIG — edit these and click ▶ Play in VS Code                        ║
# ╚══════════════════════════════════════════════════════════════════════════╝

TF_MINUTES   = 1            # 1 / 3 / 5 / 7 / 9 / 45 / 65 / 130
VARIATION    = "two_consecutive"  # ha_static | two_consecutive | keltner | rsi_keltner
SYMBOLS      = "all"        # MCX: all | crude | gold | silver | CRUDEOILM,GOLDPETAL
INDEX_SYMBOLS = ""          # NSE/BSE synthetic: NIFTY | BANKNIFTY | SENSEX | NIFTY,BANKNIFTY,SENSEX | blank=none
LIVE_MODE    = False        # False = paper trading | True = real orders (use with caution!)
ORDER_TYPE   = "MARKET"     # MARKET | SL-M | LIMIT
TRIGGER_OFFSET = 0.0        # pts added to signal price for SL-M trigger
LIMIT_OFFSET   = 0.0        # pts added to signal price for LIMIT order
GLOBAL_SL_PCT  = 0.0        # % of account balance (0 = disabled)
MCX_END_TIME   = "23:30"    # IST HH:MM — 23:30 during DST, 23:55 normal

# Per-symbol buffer overrides (None = use default from BUFFER_MAP)
BUF_CRUDEOILM = None        # e.g. 3.0
BUF_GOLDPETAL = None        # e.g. 10.0
BUF_SILVERMIC = None        # e.g. 10.0

# KC / RSI params (only used for keltner / rsi_keltner variations)
KC_LENGTH     = 21
KC_ATR_LENGTH = 21
KC_MULT       = 0.5
RSI_LENGTH    = 14
RSI_BUY       = 52.0
RSI_SELL      = 32.0

# ══════════════════════════════════════════════════════════════════════════

import os
import sys
import time
import signal
import argparse
import threading
import json
import csv
import logging
import logging.handlers
from pathlib import Path
from typing import Dict, Optional
from dotenv import load_dotenv

from market_data import (build_instrument_list, build_index_futures_list,
                         MarketDataEngine, fetch_intraday_1m_history,
                         resolve_option_contracts, get_monthly_expiry,
                         round_to_atm_strike)
from strategy_ha_static import HAStaticTriggerStrategy, SUPPORTED_VARIATIONS
from paper_engine import PaperTradeEngine
from synthetic_engine import SyntheticPaperEngine
from dashboard import print_dashboard

# ── Base directory ────────────────────────────────────────────────────────────
if getattr(sys, 'frozen', False):
    BASE_DIR = Path(sys.executable).resolve().parent
else:
    BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_FILE, override=True)

DHAN_CLIENT_ID    = os.getenv("DHAN_CLIENT_ID", "").strip()
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "").strip()

SYMBOL_PRESETS = {
    "all":    None,
    "crude":  ["CRUDEOILM"],
    "gold":   ["GOLDPETAL"],
    "silver": ["SILVERMIC"],
}

# Instruments that use synthetic futures execution (options CE+PE)
SYNTHETIC_INDEX_SYMBOLS = {"NIFTY", "BANKNIFTY", "SENSEX"}

ORDER_TYPE_MAP = {
    "MARKET": "MARKET",
    "SL-M":   "STOP_LOSS_MARKET",
    "LIMIT":  "LIMIT",
}

MCX_SESSION_END_DEFAULT = (23, 30)
DHAN_FUNDS_URL = "https://api.dhan.co/v2/fundlimit"


def fetch_account_balance(client_id: str, access_token: str) -> float:
    try:
        import requests as _req
        r = _req.get(DHAN_FUNDS_URL,
                     headers={"access-token": access_token, "client-id": client_id},
                     timeout=10)
        if r.status_code == 200:
            d = r.json()
            return float(d.get("availabelBalance") or d.get("sodLimit") or
                         d.get("netAvailableMargin") or 0)
    except Exception:
        pass
    return 0.0


# ═════════════════════════════════════════════════════════════════════════════
class TradingApp:
# ═════════════════════════════════════════════════════════════════════════════

    def __init__(
        self,
        strategy_tf: int,
        symbols_filter=None,
        index_symbols_filter=None,      # NIFTY/BANKNIFTY/SENSEX
        squareoff_symbols=None,
        variation: str = "ha_static",
        rsi_length: int = 14,
        rsi_buy_level: float = 52.0,
        rsi_sell_level: float = 32.0,
        kc_length: int = 21,
        kc_atr_length: int = 21,
        kc_multiplier: float = 0.5,
        buffer_overrides: dict = None,
        lot_size_overrides: dict = None,
        live_mode: bool = False,
        order_type: str = "MARKET",
        trigger_offset: float = 0.0,
        limit_offset: float = 0.0,
        global_sl_pct: float = 0.0,
        mcx_session_end: tuple = MCX_SESSION_END_DEFAULT,
        client_id: str = "",
        access_token: str = "",
    ):
        global DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN

        load_dotenv(dotenv_path=ENV_FILE, override=True)
        DHAN_CLIENT_ID    = os.getenv("DHAN_CLIENT_ID", "").strip()
        DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
        if client_id:    DHAN_CLIENT_ID    = str(client_id).strip()
        if access_token: DHAN_ACCESS_TOKEN = str(access_token).strip()

        # Fallback: shared token from dhan-token-generator
        if not DHAN_ACCESS_TOKEN:
            try:
                from dhan_token_manager import read_shared_token
                s = read_shared_token()
                if s.get("access_token"): DHAN_ACCESS_TOKEN = s["access_token"]
                if not DHAN_CLIENT_ID and s.get("client_id"): DHAN_CLIENT_ID = s["client_id"]
            except Exception:
                pass

        if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
            raise RuntimeError(
                "Missing DHAN credentials.\n"
                "Please fill DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN in your .env file.")

        self.strategy_tf     = int(strategy_tf)
        self.variation       = str(variation).strip().lower()
        self.rsi_length      = int(rsi_length)
        self.rsi_buy_level   = float(rsi_buy_level)
        self.rsi_sell_level  = float(rsi_sell_level)
        self.kc_length       = int(kc_length)
        self.kc_atr_length   = int(kc_atr_length)
        self.kc_multiplier   = float(kc_multiplier)
        self.buffer_overrides   = {k.upper(): float(v) for k, v in (buffer_overrides or {}).items() if v is not None}
        self.lot_size_overrides = {k.upper(): int(v)   for k, v in (lot_size_overrides or {}).items() if v is not None}
        self.squareoff_symbols  = {s.strip().upper() for s in (squareoff_symbols or []) if s.strip()}
        self.live_mode       = bool(live_mode)
        self.order_type      = str(order_type).upper()
        self.trigger_offset  = float(trigger_offset)
        self.limit_offset    = float(limit_offset)
        self.global_sl_pct   = float(global_sl_pct)
        self.global_sl_rupees = 0.0
        self.global_sl_hit   = False
        self.mcx_session_end = tuple(mcx_session_end)

        # Date-stamped daily log and trade files — one per symbol
        # Created after instruments are resolved, stored in self._sym_log_paths
        today      = __import__("datetime").date.today().strftime("%Y-%m-%d")
        self._today = today
        self.logs_dir   = BASE_DIR / "logs"
        self.trades_dir = BASE_DIR / "trades"
        self.logs_dir.mkdir(exist_ok=True)
        self.trades_dir.mkdir(exist_ok=True)
        # Main app log (startup, errors, general events)
        self.log_path   = self.logs_dir / f"app_{today}.log"
        self.state_path = BASE_DIR / "paper_state.json"

        self._setup_logging()
        self.logger.info(
            "Mode=%s | TF=%dm | Variation=%s | OrderType=%s | GSL=%.1f%% | MCXEnd=%02d:%02d",
            "LIVE" if self.live_mode else "PAPER", self.strategy_tf, self.variation,
            self.order_type, self.global_sl_pct,
            self.mcx_session_end[0], self.mcx_session_end[1])

        # Live order engine
        self.live_engine = None
        if self.live_mode:
            from live_order_engine import LiveOrderEngine
            self.live_engine = LiveOrderEngine(
                client_id=DHAN_CLIENT_ID, access_token=DHAN_ACCESS_TOKEN,
                logger=self.logger)

        # Global SL
        if self.global_sl_pct > 0:
            bal = fetch_account_balance(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
            if bal > 0:
                self.global_sl_rupees = bal * self.global_sl_pct / 100.0
                self.logger.info("Global SL: %.1f%% of ₹%.2f = ₹%.2f",
                                 self.global_sl_pct, bal, self.global_sl_rupees)

        # Instruments
        # MCX instruments (unchanged)
        self.instruments = build_instrument_list(
            symbol_filter=symbols_filter, logger=self.logger)

        # Index futures instruments for synthetic trading (additive — MCX untouched)
        index_insts = build_index_futures_list(
            symbols_filter=index_symbols_filter, logger=self.logger)
        self.instruments = self.instruments + index_insts

        if not self.instruments:
            raise RuntimeError("No instruments resolved. Check symbol names / instrument master.")

        for inst in self.instruments:
            self.logger.info(
                "Instrument: %s [%s] secId=%s lot=%s contract=%s synthetic=%s",
                inst["name"], inst["exchange"], inst["security_id"],
                inst.get("lot_size", 1), inst.get("contract_display", ""),
                inst.get("is_synthetic", False))

        self.sec_to_inst  = {str(x["security_id"]): x for x in self.instruments}
        self.strategies   = {}
        self.paper        = {}   # PaperTradeEngine — MCX only
        self.synthetic    = {}   # SyntheticPaperEngine — index only
        self._ensure_trade_log_header()

        # Preload instrument master once for option resolution at trade time
        from market_data import load_instrument_master
        self._inst_df = load_instrument_master()

        # Track live option WS subscriptions: option_sec_id → {futures_sec, leg}
        self._option_subs: Dict[str, dict] = {}

        for inst in self.instruments:
            sec      = str(inst["security_id"])
            sym_name = inst["name"].upper()
            sym_buf  = self.buffer_overrides.get(sym_name)
            sym_lot  = self.lot_size_overrides.get(sym_name, inst.get("lot_size", 1))

            self.strategies[sec] = HAStaticTriggerStrategy(
                inst["name"], self.strategy_tf,
                variation=self.variation,
                rsi_length=self.rsi_length, rsi_buy_level=self.rsi_buy_level,
                rsi_sell_level=self.rsi_sell_level, kc_length=self.kc_length,
                kc_atr_length=self.kc_atr_length, kc_multiplier=self.kc_multiplier,
                buffer_override=sym_buf, exchange=inst.get("exchange", ""))

            if inst.get("is_synthetic"):
                # Index instrument — synthetic futures via options
                self.synthetic[sec] = SyntheticPaperEngine(
                    symbol_name=sym_name, lot_size=sym_lot,
                    display_prec=inst.get("display_prec", 2),
                    event_callback=self._on_trade_event)
            else:
                # MCX instrument — unchanged paper engine
                self.paper[sec] = PaperTradeEngine(
                    sym_lot, inst.get("display_prec", 2),
                    event_callback=self._on_trade_event,
                    symbol_name=inst["name"])

        self.market = MarketDataEngine(
            client_id=DHAN_CLIENT_ID, access_token=DHAN_ACCESS_TOKEN,
            instruments=self.instruments,
            on_new_1m_candle=self._on_new_1m_candle,
            on_ltp=self._on_ltp,
            strategy_tf_sec=self.strategy_tf * 60,
            logger=self.logger)

        self.stop_event  = threading.Event()
        self.ui_thread   = None
        self._gsl_thread = None
        self._mcx_session_closed_today: set = set()

        self._run_startup_backfill()
        self.state_restored = self._load_state()
        if not self.state_restored:
            self._reset_actionable_state_after_backfill()
        self.logger.info("Startup complete. REST poller armed, trading live.")
        self._apply_startup_squareoff()
        self._save_state()

    # ── Logging ───────────────────────────────────────────────────────────────
    def _setup_logging(self):
        self.logger = logging.getLogger(f"dhan_ha_{id(self)}")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()

        # Daily rotating file handler — app-level log
        fh = logging.handlers.TimedRotatingFileHandler(
            self.log_path, when="midnight", interval=1,
            backupCount=30, encoding="utf-8", utc=False)
        fh.suffix = "%Y-%m-%d"
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        self.logger.addHandler(fh)

        # Console output for VS Code terminal
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        ch.setLevel(logging.INFO)
        self.logger.addHandler(ch)

        self.logger.propagate = False
        self.logger.info("==== App start ==== log=%s", self.log_path)

    def _get_symbol_logger(self, sym_name: str) -> logging.Logger:
        """Return (creating if needed) a per-symbol daily rotating logger."""
        log_name = f"dhan_ha_{sym_name}_{id(self)}"
        sym_logger = logging.getLogger(log_name)
        if sym_logger.handlers:
            return sym_logger   # already set up
        sym_logger.setLevel(logging.INFO)
        sym_log_path = self.logs_dir / f"{sym_name}_{self._today}.log"
        fh = logging.handlers.TimedRotatingFileHandler(
            sym_log_path, when="midnight", interval=1,
            backupCount=30, encoding="utf-8", utc=False)
        fh.suffix = "%Y-%m-%d"
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        sym_logger.addHandler(fh)
        sym_logger.propagate = False
        return sym_logger

    # ── Trade log — per symbol ────────────────────────────────────────────────
    def _get_todays_trade_log(self, sym_name: str) -> Path:
        """Return today's trade CSV path for a specific symbol, creating header if new."""
        today = __import__("datetime").date.today().strftime("%Y-%m-%d")
        self.trades_dir.mkdir(exist_ok=True)
        path = self.trades_dir / f"{sym_name}_{today}.csv"
        if not path.exists():
            with path.open("w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([
                    "ts", "symbol", "event_type", "mode", "order_type",
                    "position_side", "entry_price", "entry_time",
                    "exit_price", "exit_time",
                    "closed_entry_price", "closed_pnl",
                    "realized_pnl", "trade_count", "lot_size"])
        return path

    def _ensure_trade_log_header(self):
        # Headers created lazily per symbol on first trade — nothing to do here
        pass

    def _append_trade_log(self, payload):
        import datetime as _dt
        def _fmt(epoch):
            if epoch is None: return ""
            try: return _dt.datetime.fromtimestamp(int(epoch)).strftime("%Y-%m-%d %H:%M:%S")
            except: return str(epoch)

        sym_name = str(payload.get("symbol", "UNKNOWN")).upper()
        is_open  = payload.get("event_type", "").startswith("OPEN")
        path     = self._get_todays_trade_log(sym_name)

        # Also write to per-symbol logger
        sym_logger = self._get_symbol_logger(sym_name)
        sym_logger.info("TRADE %s | price=%s | pnl=%s",
                        payload.get("event_type"),
                        payload.get("entry_price") or payload.get("exit_price"),
                        payload.get("closed_pnl") or "-")

        with path.open("a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                _fmt(payload.get("ts")),
                payload.get("symbol"),
                payload.get("event_type"),
                "LIVE" if self.live_mode else "PAPER",
                self.order_type,
                payload.get("position_side"),
                payload.get("entry_price"),
                _fmt(payload.get("entry_ts")),
                payload.get("exit_price") if not is_open else "",
                _fmt(payload.get("ts")) if not is_open else "",
                payload.get("closed_entry_price"),
                payload.get("closed_pnl"),
                payload.get("realized_pnl"),
                payload.get("trade_count"),
                payload.get("lot_size"),
            ])

    def _on_trade_event(self, event_type, payload):
        payload["event_type"] = event_type
        self._append_trade_log(payload)
        self.logger.info("TRADE %s | %s", event_type, payload)
        self._save_state()

    # ── State ─────────────────────────────────────────────────────────────────
    def _state_blob(self):
        symbols = {}
        for sec in self.sec_to_inst:
            blob = {"instrument": self.sec_to_inst[sec],
                    "strategy":   self.strategies[sec].persist_state()}
            if self._is_synthetic(sec):
                blob["synthetic"] = self.synthetic[sec].persist_state()
            else:
                blob["paper"] = self.paper[sec].persist_state()
            symbols[sec] = blob
        return {
            "strategy_tf": self.strategy_tf, "variation": self.variation,
            "live_mode": self.live_mode, "order_type": self.order_type,
            "buffer_overrides": self.buffer_overrides,
            "lot_size_overrides": self.lot_size_overrides,
            "symbols": symbols,
        }

    def _save_state(self):
        try:
            self.state_path.write_text(json.dumps(self._state_blob(), indent=2))
        except Exception as e:
            self.logger.warning("State save failed: %s", e)

    def _load_state(self) -> bool:
        if not self.state_path.exists():
            return False
        try:
            data = json.loads(self.state_path.read_text())
        except Exception:
            return False
        if int(data.get("strategy_tf", 0)) != self.strategy_tf: return False
        if str(data.get("variation", "")).lower() != self.variation: return False
        restored = 0
        for sec, blob in data.get("symbols", {}).items():
            # Only restore instruments that are part of THIS run's selection.
            # Prevents a stale state file from bringing back unchecked symbols.
            if sec not in self.sec_to_inst:
                continue
            if sec in self.strategies:
                self.strategies[sec].restore_state(blob.get("strategy", {}))
                if self._is_synthetic(sec) and blob.get("synthetic"):
                    self.synthetic[sec].restore_state(blob["synthetic"])
                elif not self._is_synthetic(sec) and blob.get("paper"):
                    self.paper[sec].restore_state(blob["paper"])
                restored += 1
        self.logger.info("State restored for %d symbols.", restored)
        return restored > 0

    # ── Backfill ──────────────────────────────────────────────────────────────
    def _run_startup_backfill(self):
        self.logger.info("Startup backfill (REST, closed candles only)...")
        total = 0
        rows_by_sec: Dict[str, list] = {}
        for inst in self.instruments:
            sec  = str(inst["security_id"])
            rows = fetch_intraday_1m_history(
                DHAN_ACCESS_TOKEN, inst, days=5,
                logger=self.logger, client_id=DHAN_CLIENT_ID)
            for row in rows:
                self.market.engines[sec].seed_from_1m_candle(row)
                self.strategies[sec].on_new_1m_candle(row)
                self.market.engines[sec].last_ltp       = float(row["close"])
                self.market.engines[sec].last_ltt_epoch = int(row["bucket"]) + 59
            rows_by_sec[sec] = rows
            total += len(rows)
        self.market.seed_last_buckets(rows_by_sec)
        self.logger.info("Backfill complete: %d closed 1m candles.", total)

    def _reset_actionable_state_after_backfill(self):
        for sec in self.sec_to_inst:
            s = self.strategies[sec]
            s.entry_wait_bucket = None; s.entry_wait_side = None
            s.sl_side = None; s.sl_price = None; s.sl_from_bucket = None
            s.pending_side = None; s.pending_trigger = None; s.pending_from_bucket = None
            s.last_event = "Startup: waiting for today's first candle to close..."
            self.logger.info("Startup: %s — waiting for first live candle",
                             self.sec_to_inst[sec]["name"])

            if sec in self.synthetic:
                # Index instrument — clear synthetic position
                syn = self.synthetic[sec]
                syn.position_side = None
                syn.ce_leg.close(); syn.pe_leg.close()
            else:
                # MCX instrument — unchanged
                p = self.paper[sec]
                p.position_side = None; p.entry_price = None; p.entry_ts = None

    # ── Square off ────────────────────────────────────────────────────────────
    def _apply_startup_squareoff(self):
        if not self.squareoff_symbols: return
        for sec, inst in self.sec_to_inst.items():
            if "ALL" not in self.squareoff_symbols and inst["name"].upper() not in self.squareoff_symbols: continue
            if self._is_synthetic(sec): continue   # synthetic handled separately
            snap = self.market.engines[sec].snapshot()
            ltp  = snap["ltp"]; ts = snap["ltt_epoch"] or int(time.time())
            if ltp is None or self.paper[sec].position_side is None: continue
            self.paper[sec].square_off(float(ltp), int(ts))
            self.strategies[sec].clear_trade_tracking(None)
            self.strategies[sec].pending_side = None

    def square_off_all(self):
        for sec, inst in self.sec_to_inst.items():
            if sec in self.synthetic:
                syn = self.synthetic[sec]
                if not syn.is_open(): continue
                syn.close_position(
                    ce_exit_price=syn.ce_leg.current_ltp or 0,
                    pe_exit_price=syn.pe_leg.current_ltp or 0,
                    ts=int(time.time()))
                self.strategies[sec].clear_trade_tracking(None)
                self.strategies[sec].pending_side = None
            else:
                if self.paper[sec].position_side is None: continue
                snap = self.market.engines[sec].snapshot(); ltp = snap["ltp"]
                if ltp is None: continue
                if self.live_mode and self.live_engine:
                    side = "SELL" if self.paper[sec].position_side == "LONG" else "BUY"
                    def _on_sq(fp, oid, _sec=sec):
                        self.paper[_sec].square_off(fp, int(time.time()))
                        self.strategies[_sec].clear_trade_tracking(None)
                        self.strategies[_sec].pending_side = None
                        self._save_state()
                    self.live_engine.execute_with_fallback(
                        side, inst["security_id"], inst["exchange"],
                        self.paper[sec].lot_size, "MARKET", on_fill=_on_sq)
                else:
                    self.paper[sec].square_off(float(ltp), int(snap["ltt_epoch"] or time.time()))
                    self.strategies[sec].clear_trade_tracking(None)
                    self.strategies[sec].pending_side = None
        self._save_state()

    # ── Global SL ─────────────────────────────────────────────────────────────
    def _global_sl_monitor(self):
        while not self.stop_event.is_set():
            time.sleep(5)
            if self.global_sl_hit or self.global_sl_rupees <= 0: continue
            try:
                total_pnl = 0.0
                for sec in self.sec_to_inst:
                    base  = self.market.engines[sec].snapshot()
                    if self._is_synthetic(sec):
                        syn = self.synthetic[sec]
                        total_pnl += float(syn.realized_pnl) + float(syn.unrealized_pnl)
                    else:
                        paper = self.paper[sec].snapshot(base["ltp"])
                        total_pnl += float(paper["realized_pnl"]) + float(paper["unrealized_pnl"])
                if total_pnl <= -abs(self.global_sl_rupees):
                    self.global_sl_hit = True
                    self.logger.warning("GLOBAL SL HIT: P&L ₹%.2f. Squaring off.", total_pnl)
                    self.square_off_all()
                    for sec in self.sec_to_inst:
                        self.strategies[sec].pending_side    = None
                        self.strategies[sec].pending_trigger = None
                        self.strategies[sec].last_event = "⛔ Global SL hit"
            except Exception as e:
                self.logger.warning("Global SL monitor error: %s", e)

    # ── MCX session end ───────────────────────────────────────────────────────
    def _check_mcx_session_end(self):
        import datetime as _dt
        now_ist   = _dt.datetime.now()
        today_str = now_ist.strftime("%Y-%m-%d")
        end_h, end_m = self.mcx_session_end
        if now_ist.hour < end_h or (now_ist.hour == end_h and now_ist.minute < end_m): return
        for sec, inst in self.sec_to_inst.items():
            if inst.get("exchange", "") != "MCX_COMM": continue
            key = f"{sec}_{today_str}"
            if key in self._mcx_session_closed_today: continue
            engine = self.market.engines[sec]
            with engine.lock:
                current = engine.current
                if current is None: self._mcx_session_closed_today.add(key); continue
                fake_row = {k: current[k] for k in ("bucket","open","high","low","close")}
                fake_row["tick_count"] = int(current.get("tick_count", 1))
            self._mcx_session_closed_today.add(key)
            self.logger.info("MCX session end %02d:%02d — force-finalizing %s", end_h, end_m, inst["name"])
            self.strategies[sec].on_new_1m_candle(fake_row)
            self.strategies[sec].on_signal_aligned_position(self.paper[sec].position_side)
            self._save_state()

    # ── Signal execution ──────────────────────────────────────────────────────
    def _compute_order_prices(self, side, signal_price):
        is_buy = side.upper() == "BUY"
        if self.order_type == "MARKET":  return 0.0, 0.0
        elif self.order_type == "SL-M":
            trig = signal_price + self.trigger_offset if is_buy else signal_price - self.trigger_offset
            return 0.0, trig
        elif self.order_type == "LIMIT":
            lmt = signal_price + self.limit_offset if is_buy else signal_price - self.limit_offset
            return lmt, 0.0
        return 0.0, 0.0

    # ── Helpers ───────────────────────────────────────────────────────────────
    def _is_synthetic(self, sec: str) -> bool:
        return sec in self.synthetic

    def _get_position_side(self, sec: str):
        if self._is_synthetic(sec):
            return self.synthetic[sec].position_side
        return self.paper[sec].position_side

    # ── Signal execution dispatcher ───────────────────────────────────────────
    def _execute_signal(self, sec, sig, ts_epoch):
        if self.global_sl_hit: return
        if self._is_synthetic(sec):
            self._execute_synthetic_signal(sec, sig, ts_epoch)
        else:
            self._execute_mcx_signal(sec, sig, ts_epoch)

    def _execute_mcx_signal(self, sec, sig, ts_epoch):
        """MCX single-leg execution — completely unchanged."""
        side         = sig["side"]
        signal_price = float(sig["price"])
        inst         = self.sec_to_inst[sec]
        quantity     = self.paper[sec].lot_size
        dhan_type    = ORDER_TYPE_MAP.get(self.order_type, "MARKET")
        order_price, trigger_price = self._compute_order_prices(side, signal_price)

        if self.live_mode and self.live_engine:
            def _on_fill(fp, oid, _sec=sec, _sig=sig):
                filled = dict(_sig); filled["price"] = fp
                self.paper[_sec].execute_signal(filled, int(time.time()))
                self.strategies[_sec].on_trade_executed(_sig["side"])
                self._save_state()
            def _on_err(err, _sec=sec):
                self.logger.error("Live MCX order error %s: %s", inst["name"], err)
                self.strategies[_sec].on_trade_executed(side)
            self.live_engine.execute_with_fallback(
                transaction_type=side, security_id=inst["security_id"],
                exchange_segment=inst["exchange"], quantity=quantity,
                order_type=dhan_type, price=order_price, trigger_price=trigger_price,
                fallback_timeout=10, on_fill=_on_fill, on_fallback=_on_fill, on_error=_on_err)
            self.strategies[sec].on_trade_executed(side)
        else:
            self.paper[sec].execute_signal(sig, ts_epoch)
            self.strategies[sec].on_trade_executed(side)
            self._save_state()

    def _resolve_atm_options(self, sec: str) -> Optional[tuple]:
        """
        Resolve ATM CE+PE contracts using current futures LTP.
        Returns (contracts_dict, atm_strike, expiry_date) or None on failure.
        """
        inst       = self.sec_to_inst[sec]
        index_name = inst["name"].upper()
        ltp        = self.market.engines[sec].snapshot()["ltp"]
        if ltp is None:
            self.logger.warning("Cannot resolve ATM for %s: no futures LTP", index_name)
            return None

        atm_strike = round_to_atm_strike(ltp, index_name)
        expiry     = get_monthly_expiry(index_name)
        lot_ov     = self.lot_size_overrides.get(index_name)

        self.logger.info("%s futures LTP=%.2f → ATM strike=%d expiry=%s",
                         index_name, ltp, atm_strike, expiry)

        contracts = resolve_option_contracts(
            index_name=index_name, strike=atm_strike,
            expiry_date=expiry, df=self._inst_df,
            lot_size_override=lot_ov, logger=self.logger)

        if contracts is None:
            self.logger.error("Could not resolve options for %s strike=%d expiry=%s",
                              index_name, atm_strike, expiry)
            return None

        return contracts, atm_strike, expiry

    def _execute_synthetic_signal(self, sec, sig, ts_epoch):
        """
        Synthetic futures execution for index instruments.
        BUY  → BUY ATM CE  + SELL ATM PE
        SELL → SELL ATM CE + BUY ATM PE
        Reversal: exit old legs first (sequentially), then enter new legs.
        """
        syn_engine = self.synthetic[sec]
        new_side   = "LONG" if sig["side"] == "BUY" else "SHORT"
        has_pos    = syn_engine.is_open()

        def _run():
            try:
                # ── Step 1 & 2: Exit existing legs sequentially ───────────────
                if has_pos:
                    old_ce_sec   = syn_engine.ce_leg.security_id
                    old_pe_sec   = syn_engine.pe_leg.security_id
                    old_ce_exit  = "SELL" if syn_engine.ce_leg.side == "BUY" else "BUY"
                    old_pe_exit  = "SELL" if syn_engine.pe_leg.side == "BUY" else "BUY"
                    exchange     = self.sec_to_inst[sec]["exchange"]
                    lot          = syn_engine.ce_leg.lot_size
                    ce_fill      = [syn_engine.ce_leg.current_ltp or 0]
                    pe_fill      = [syn_engine.pe_leg.current_ltp or 0]

                    if self.live_mode and self.live_engine:
                        # Exit CE leg
                        def _ce_exit_filled(fp, oid): ce_fill[0] = fp
                        self.live_engine.execute_with_fallback(
                            old_ce_exit, old_ce_sec, exchange, lot,
                            "MARKET", on_fill=_ce_exit_filled)
                        deadline = time.time() + 15
                        while ce_fill[0] == (syn_engine.ce_leg.current_ltp or 0) and time.time() < deadline:
                            time.sleep(0.2)

                        # Exit PE leg
                        def _pe_exit_filled(fp, oid): pe_fill[0] = fp
                        self.live_engine.execute_with_fallback(
                            old_pe_exit, old_pe_sec, exchange, lot,
                            "MARKET", on_fill=_pe_exit_filled)
                        deadline = time.time() + 15
                        while pe_fill[0] == (syn_engine.pe_leg.current_ltp or 0) and time.time() < deadline:
                            time.sleep(0.2)

                    syn_engine.close_position(
                        ce_exit_price=ce_fill[0],
                        pe_exit_price=pe_fill[0],
                        ts=int(time.time()))
                    self.logger.info("Synthetic closed: %s", self.sec_to_inst[sec]["name"])

                # ── Step 3 & 4: Resolve fresh ATM and enter new legs ──────────
                result = self._resolve_atm_options(sec)
                if result is None:
                    self.strategies[sec].on_trade_executed(sig["side"])
                    return

                contracts, atm_strike, expiry = result
                ce_info  = contracts["CE"]
                pe_info  = contracts["PE"]
                exchange = ce_info["exchange"]
                lot      = ce_info["lot_size"]

                ce_entry_side = "BUY"  if new_side == "LONG" else "SELL"
                pe_entry_side = "SELL" if new_side == "LONG" else "BUY"

                # Subscribe option LTPs via WebSocket for P&L tracking
                self._subscribe_option(ce_info["security_id"], sec, "CE")
                self._subscribe_option(pe_info["security_id"], sec, "PE")

                ce_price = [0.0]
                pe_price = [0.0]

                if self.live_mode and self.live_engine:
                    # Enter CE leg
                    def _ce_filled(fp, oid): ce_price[0] = fp
                    self.live_engine.execute_with_fallback(
                        ce_entry_side, ce_info["security_id"], exchange,
                        lot, "MARKET", on_fill=_ce_filled)
                    deadline = time.time() + 15
                    while ce_price[0] == 0 and time.time() < deadline:
                        time.sleep(0.2)

                    # Enter PE leg
                    def _pe_filled(fp, oid): pe_price[0] = fp
                    self.live_engine.execute_with_fallback(
                        pe_entry_side, pe_info["security_id"], exchange,
                        lot, "MARKET", on_fill=_pe_filled)
                    deadline = time.time() + 15
                    while pe_price[0] == 0 and time.time() < deadline:
                        time.sleep(0.2)
                else:
                    # Paper mode — use a nominal price (actual LTP not available yet)
                    ce_price[0] = 100.0
                    pe_price[0] = 100.0

                syn_engine.open_position(
                    synthetic_side=new_side,
                    strike=atm_strike,
                    expiry_str=ce_info["expiry_str"],
                    ce_security_id=ce_info["security_id"],
                    pe_security_id=pe_info["security_id"],
                    ce_price=ce_price[0],
                    pe_price=pe_price[0],
                    ts=int(time.time()),
                    lot_size=lot)

                self.strategies[sec].on_trade_executed(sig["side"])
                self._save_state()
                self.logger.info(
                    "Synthetic %s opened: %s strike=%d CE=%s PE=%s",
                    new_side, self.sec_to_inst[sec]["name"],
                    atm_strike, ce_info["security_id"], pe_info["security_id"])

            except Exception as e:
                self.logger.error("_execute_synthetic_signal error %s: %s",
                                  self.sec_to_inst[sec]["name"], e)
                self.strategies[sec].on_trade_executed(sig["side"])

        import threading as _th
        _th.Thread(target=_run, daemon=True).start()

    def _subscribe_option(self, option_sec_id: str, futures_sec: str, leg: str):
        """Subscribe to option LTP via WebSocket for P&L tracking."""
        self._option_subs[option_sec_id] = {"futures_sec": futures_sec, "leg": leg}
        try:
            inst     = self.sec_to_inst.get(futures_sec, {})
            exchange = inst.get("exchange", "NSE_FNO")
            sub_msg  = {
                "RequestCode":     15,
                "InstrumentCount": 1,
                "InstrumentList":  [{"ExchangeSegment": exchange,
                                     "SecurityId": str(option_sec_id)}],
            }
            if self.market.ws:
                import json as _json
                self.market.ws.send(_json.dumps(sub_msg))
                self.logger.info("Subscribed option LTP: %s leg=%s", option_sec_id, leg)
        except Exception as e:
            self.logger.warning("Option WS subscription failed: %s", e)

    def _handle_strategy_exit_if_any(self, sec, fallback_ts):
        exit_sig = self.strategies[sec].check_stoploss_exit()
        if not exit_sig: return
        pos_side  = self._get_position_side(sec)
        side_match = ((exit_sig["exit_side"]=="LONG"  and pos_side=="LONG") or
                      (exit_sig["exit_side"]=="SHORT" and pos_side=="SHORT"))
        if not side_match: return

        if self._is_synthetic(sec):
            syn = self.synthetic[sec]
            if syn.is_open():
                syn.close_position(
                    ce_exit_price=syn.ce_leg.current_ltp or 0,
                    pe_exit_price=syn.pe_leg.current_ltp or 0,
                    ts=int(fallback_ts))
            self.strategies[sec].clear_trade_tracking(None)
        else:
            paper      = self.paper[sec]
            inst       = self.sec_to_inst[sec]
            close_side = "SELL" if exit_sig["exit_side"] == "LONG" else "BUY"
            if self.live_mode and self.live_engine:
                def _on_sl(fp, oid, _sec=sec):
                    self.paper[_sec].square_off(fp, int(time.time()))
                    self.strategies[_sec].clear_trade_tracking(None); self._save_state()
                self.live_engine.execute_with_fallback(
                    close_side, inst["security_id"], inst["exchange"],
                    paper.lot_size, "MARKET", on_fill=_on_sl)
            else:
                paper.square_off(float(exit_sig["exit_price"]), int(fallback_ts))
            self.strategies[sec].clear_trade_tracking(None)

    # ── Callbacks ─────────────────────────────────────────────────────────────
    def _on_new_1m_candle(self, sec, row_1m):
        self.strategies[sec].on_new_1m_candle(row_1m)
        self._handle_strategy_exit_if_any(sec, int(row_1m["bucket"]) + 59)
        self.strategies[sec].on_signal_aligned_position(self._get_position_side(sec))
        if self.sec_to_inst[sec].get("exchange") == "MCX_COMM":
            self._check_mcx_session_end()
        sig = self.strategies[sec].check_intrabar_range_hit(
            float(row_1m["high"]), float(row_1m["low"]))
        if sig:
            self._execute_signal(sec, sig, int(row_1m["bucket"]) + 59)
        self._save_state()

    def _on_ltp(self, sec, ltp, ts_epoch):
        # Route option LTPs to synthetic engine for P&L tracking
        if sec in self._option_subs:
            sub_info    = self._option_subs[sec]
            futures_sec = sub_info["futures_sec"]
            leg         = sub_info["leg"]
            if futures_sec in self.synthetic:
                if leg == "CE": self.synthetic[futures_sec].on_ce_ltp(ltp)
                else:           self.synthetic[futures_sec].on_pe_ltp(ltp)
            return

        # Normal futures/MCX trigger check
        self.strategies[sec].on_signal_aligned_position(self._get_position_side(sec))
        signal_hit = self.strategies[sec].check_trigger_hit(ltp)
        if signal_hit:
            self._execute_signal(sec, signal_hit, ts_epoch)

    # ── Lifecycle ─────────────────────────────────────────────────────────────
    def _run_ui(self):
        while not self.stop_event.is_set():
            try:
                print_dashboard(self)
            except Exception as e:
                self.logger.warning("Dashboard error: %s", e)
            time.sleep(1)

    def start(self, with_terminal_ui: bool = True):
        self.market.start()
        if self.global_sl_rupees > 0:
            self._gsl_thread = threading.Thread(
                target=self._global_sl_monitor, daemon=True)
            self._gsl_thread.start()
        if with_terminal_ui:
            mode = "LIVE" if self.live_mode else "PAPER"
            print(f"\n{'='*60}")
            print(f"  Dhan HA Trader  |  {mode}")
            print(f"  TF={self.strategy_tf}m  |  Variation={self.variation}  |  OrderType={self.order_type}")
            print(f"  MCX Session End: {self.mcx_session_end[0]:02d}:{self.mcx_session_end[1]:02d}  |  Global SL: {self.global_sl_pct}%")
            for x in self.instruments:
                buf = self.strategies[str(x["security_id"])].buffer
                lot = self.paper[str(x["security_id"])].lot_size
                print(f"  {x['name']:<12} lot={lot}  buf={buf}  contract={x.get('contract_display','')}")
            print(f"{'='*60}\n")
            time.sleep(1)
            self.ui_thread = threading.Thread(target=self._run_ui, daemon=True)
            self.ui_thread.start()

    def stop(self):
        self.stop_event.set()
        self._save_state()
        self.market.stop()
        self.logger.info("==== App stop ====")

    # ── Snapshot (for dashboard) ───────────────────────────────────────────────
    def get_snapshot(self) -> dict:
        result = {
            "strategy_tf": self.strategy_tf, "variation": self.variation,
            "live_mode": self.live_mode, "order_type": self.order_type,
            "trigger_offset": self.trigger_offset, "limit_offset": self.limit_offset,
            "global_sl_pct": self.global_sl_pct, "global_sl_rupees": self.global_sl_rupees,
            "global_sl_hit": self.global_sl_hit, "mcx_session_end": self.mcx_session_end,
            "symbols": [], "total_unrealized": 0.0, "total_realized": 0.0,
            "ws_uptime": "-", "packets": {}, "ws_error": None,
        }
        ms = self.market.market_snapshot()
        ct = ms["last_ws_connect_time"]
        if ct: result["ws_uptime"] = f"{int(time.time()-ct)}s"
        result["packets"]  = ms["packet_counts"]
        result["ws_error"] = ms["last_ws_error"]

        for sec, inst in self.sec_to_inst.items():
            base  = self.market.engines[sec].snapshot()
            strat = self.strategies[sec].snapshot()
            prec  = int(inst["display_prec"])
            ha_last = strat["ha_last"]

            if self._is_synthetic(sec):
                syn = self.synthetic[sec]
                ce  = syn.ce_leg; pe = syn.pe_leg
                upnl = syn.unrealized_pnl
                rpnl = syn.realized_pnl
                result["total_unrealized"] += float(upnl)
                result["total_realized"]   += float(rpnl)
                pos_str = "-"
                if syn.position_side:
                    strike = ce.strike or pe.strike or "-"
                    pos_str = f"{syn.position_side} {strike}"
                entry_str = None
                if ce.entry_price and pe.entry_price:
                    entry_str = f"C{ce.entry_price:.0f}/P{pe.entry_price:.0f}"
                result["symbols"].append({
                    "name":             inst["name"],
                    "contract_display": inst.get("contract_display", "-"),
                    "buffer":           self.strategies[sec].buffer,
                    "lot":              int(syn.lot_size),
                    "prec":             prec,
                    "ltp":              base["ltp"],
                    "position":         pos_str,
                    "entry":            entry_str,
                    "pending":          strat["pending_side"] or "-",
                    "trigger":          strat["pending_trigger"],
                    "unrealized":       float(upnl),
                    "realized":         float(rpnl),
                    "ha_color":         ha_last["color"] if ha_last else "-",
                    "ha_streak":        int(ha_last["streak"]) if ha_last else 0,
                    "event":            syn.last_event if syn.last_event != "-" else strat["last_event"],
                    "ha_history":       strat["ha_history"][-5:],
                    "sl_price":         strat.get("sl_price"),
                })
            else:
                paper = self.paper[sec].snapshot(base["ltp"])
                result["total_unrealized"] += float(paper["unrealized_pnl"])
                result["total_realized"]   += float(paper["realized_pnl"])
                result["symbols"].append({
                    "name":             inst["name"],
                    "contract_display": inst.get("contract_display", "-"),
                    "buffer":           self.strategies[sec].buffer,
                    "lot":              int(paper["lot_size"]),
                    "prec":             prec,
                    "ltp":              base["ltp"],
                    "position":         paper["position_side"] or "-",
                    "entry":            paper["entry_price"],
                    "pending":          strat["pending_side"] or "-",
                    "trigger":          strat["pending_trigger"],
                    "unrealized":       float(paper["unrealized_pnl"]),
                    "realized":         float(paper["realized_pnl"]),
                    "ha_color":         ha_last["color"] if ha_last else "-",
                    "ha_streak":        int(ha_last["streak"]) if ha_last else 0,
                    "event":            paper.get("last_event","-") if paper.get("last_event","-") != "-" else strat["last_event"],
                    "ha_history":       strat["ha_history"][-5:],
                    "sl_price":         strat.get("sl_price"),
                })
        return result


# ═════════════════════════════════════════════════════════════════════════════
#  CLI ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(
        description="Dhan HA Trader — Paper/Live Strategy",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --tf 45 --variation two_consecutive
  python main.py --tf 65 --variation ha_static --symbols crude
  python main.py --tf 9  --variation two_consecutive --symbols crude,silver
  python main.py --tf 45 --variation two_consecutive --live --order-type MARKET
  python main.py --tf 45 --variation two_consecutive --global-sl-pct 2.0
        """
    )
    parser.add_argument("--tf", type=int, default=65,
                        choices=[1, 3, 5, 7, 9, 45, 65, 130],
                        help="Timeframe in minutes (default: 65)")
    parser.add_argument("--symbols", type=str, default="all",
                        help="all | crude | gold | silver | CRUDEOILM,GOLDPETAL (default: all)")
    parser.add_argument("--variation", type=str, default="ha_static",
                        choices=sorted(SUPPORTED_VARIATIONS),
                        help="Strategy variation (default: ha_static)")
    parser.add_argument("--live", action="store_true",
                        help="Enable live trading (default: paper mode)")
    parser.add_argument("--order-type", type=str, default="MARKET",
                        choices=["MARKET", "SL-M", "LIMIT"],
                        help="Order type (default: MARKET)")
    parser.add_argument("--trigger-offset", type=float, default=0.0,
                        help="Trigger offset in pts for SL-M orders")
    parser.add_argument("--limit-offset", type=float, default=0.0,
                        help="Limit offset in pts for LIMIT orders")
    parser.add_argument("--global-sl-pct", type=float, default=0.0,
                        help="Global SL as %% of account balance (e.g. 2.0)")
    parser.add_argument("--mcx-end", type=str, default="23:30",
                        help="MCX session end time IST HH:MM (default: 23:30 DST / 23:55 normal)")
    parser.add_argument("--buf-crude", type=float, default=None, help="Buffer override for CRUDEOILM")
    parser.add_argument("--buf-gold",  type=float, default=None, help="Buffer override for GOLDPETAL")
    parser.add_argument("--buf-silver",type=float, default=None, help="Buffer override for SILVERMIC")
    parser.add_argument("--kc-length",    type=int,   default=21)
    parser.add_argument("--kc-atr-length",type=int,   default=21)
    parser.add_argument("--kc-mult",      type=float, default=0.5)
    parser.add_argument("--rsi-length",   type=int,   default=14)
    parser.add_argument("--rsi-buy",      type=float, default=52.0)
    parser.add_argument("--rsi-sell",     type=float, default=32.0)
    return parser.parse_args()


def _ensure_token() -> bool:
    """
    If DHAN_ACCESS_TOKEN is missing or empty, auto-generate it using
    TOTP credentials from .env. Saves the new token back to .env.
    Returns True if credentials are ready, False if cannot proceed.
    """
    global DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN
    if DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN:
        return True

    print("\n⏳  Access token missing — attempting auto-generation via TOTP...")
    try:
        from dhan_token_manager import load_config, get_fresh_token, save_token_to_env
        cfg = load_config()
        if not cfg.get("totp_secret") or not cfg.get("pin"):
            print("❌  DHAN_PIN and DHAN_TOTP_SECRET must be set in .env for auto-generation.")
            return False
        token = get_fresh_token(cfg, force_new=True)
        save_token_to_env(token)
        DHAN_CLIENT_ID    = cfg["client_id"]
        DHAN_ACCESS_TOKEN = token
        os.environ["DHAN_ACCESS_TOKEN"] = token
        print(f"✅  Token generated: {token[:28]}...")
        return True
    except Exception as e:
        print(f"❌  Auto token generation failed: {e}")
        print("    Please set DHAN_ACCESS_TOKEN manually in .env")
        return False


def main():
    if not _ensure_token():
        raise SystemExit(1)

    # If no CLI args provided (e.g. VS Code Play button), use CONFIG block above.
    # If CLI args are passed, they override the CONFIG block.
    use_cli = len(sys.argv) > 1

    if use_cli:
        args = parse_args()
        tf_val         = args.tf
        variation_val  = args.variation
        symbols_val    = args.symbols
        index_symbols_val = getattr(args, "index_symbols", "")
        live_val       = args.live
        order_type_val = args.order_type
        trig_off_val   = args.trigger_offset
        lmt_off_val    = args.limit_offset
        gsl_val        = args.global_sl_pct
        mcx_end_str    = args.mcx_end
        buf_crude      = args.buf_crude
        buf_gold       = args.buf_gold
        buf_silver     = args.buf_silver
        kc_len         = args.kc_length
        kc_atr         = args.kc_atr_length
        kc_mult        = args.kc_mult
        rsi_len        = args.rsi_length
        rsi_buy        = args.rsi_buy
        rsi_sell       = args.rsi_sell
    else:
        # ── Using CONFIG block (VS Code Play button) ──────────────────────
        tf_val         = TF_MINUTES
        variation_val  = VARIATION
        symbols_val    = SYMBOLS
        index_symbols_val = INDEX_SYMBOLS
        live_val       = LIVE_MODE
        order_type_val = ORDER_TYPE
        trig_off_val   = TRIGGER_OFFSET
        lmt_off_val    = LIMIT_OFFSET
        gsl_val        = GLOBAL_SL_PCT
        mcx_end_str    = MCX_END_TIME
        buf_crude      = BUF_CRUDEOILM
        buf_gold       = BUF_GOLDPETAL
        buf_silver     = BUF_SILVERMIC
        kc_len         = KC_LENGTH
        kc_atr         = KC_ATR_LENGTH
        kc_mult        = KC_MULT
        rsi_len        = RSI_LENGTH
        rsi_buy        = RSI_BUY
        rsi_sell       = RSI_SELL

    # Parse MCX symbols
    sym_key = symbols_val.strip().lower()
    if sym_key in SYMBOL_PRESETS:
        sym_filter = SYMBOL_PRESETS[sym_key]
    else:
        sym_filter = [x.strip().upper() for x in symbols_val.split(",") if x.strip()]

    # Parse index symbols (NIFTY/BANKNIFTY/SENSEX)
    if index_symbols_val and index_symbols_val.strip():
        index_filter = [x.strip().upper() for x in index_symbols_val.split(",") if x.strip()]
    else:
        index_filter = None

    # Parse MCX end time
    try:
        h, m = mcx_end_str.split(":")
        mcx_end = (int(h), int(m))
    except Exception:
        mcx_end = MCX_SESSION_END_DEFAULT

    # Buffer overrides
    buf_overrides = {}
    if buf_crude  is not None: buf_overrides["CRUDEOILM"] = buf_crude
    if buf_gold   is not None: buf_overrides["GOLDPETAL"]  = buf_gold
    if buf_silver is not None: buf_overrides["SILVERMIC"]  = buf_silver

    if live_val:
        confirm = input("\n⚠️  LIVE TRADING MODE — real orders will be placed!\nType 'yes' to confirm: ").strip().lower()
        if confirm != "yes":
            print("Cancelled."); raise SystemExit(0)

    app = TradingApp(
        strategy_tf=tf_val,
        symbols_filter=sym_filter,
        index_symbols_filter=index_filter,
        variation=variation_val,
        live_mode=live_val,
        order_type=order_type_val,
        trigger_offset=trig_off_val,
        limit_offset=lmt_off_val,
        global_sl_pct=gsl_val,
        mcx_session_end=mcx_end,
        buffer_overrides=buf_overrides if buf_overrides else None,
        kc_length=kc_len,
        kc_atr_length=kc_atr,
        kc_multiplier=kc_mult,
        rsi_length=rsi_len,
        rsi_buy_level=rsi_buy,
        rsi_sell_level=rsi_sell,
    )

    def _sig_handler(sig, frame):
        print("\n⏹  Stopping...")
        app.stop()
        raise SystemExit(0)

    signal.signal(signal.SIGINT,  _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    app.start(with_terminal_ui=True)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        app.stop()


if __name__ == "__main__":
    main()
