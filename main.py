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
from typing import Dict
from dotenv import load_dotenv

from market_data import build_instrument_list, MarketDataEngine, fetch_intraday_1m_history
from strategy_ha_static import HAStaticTriggerStrategy, SUPPORTED_VARIATIONS
from paper_engine import PaperTradeEngine
from dashboard import print_dashboard

if getattr(sys, 'frozen', False):
    BASE_DIR = Path(sys.executable).parent
else:
    BASE_DIR = Path(__file__).resolve().parent

ENV_FILE = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_FILE, override=True)

DHAN_CLIENT_ID    = os.getenv("DHAN_CLIENT_ID", "").strip()
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "").strip()

SYMBOL_PRESETS = {"all": None, "crude": ["CRUDEOILM"],
                  "gold": ["GOLDPETAL"], "silver": ["SILVERMIC"]}
ORDER_TYPE_MAP = {"MARKET": "MARKET", "SL-M": "STOP_LOSS_MARKET", "LIMIT": "LIMIT"}
MCX_SESSION_END_DEFAULT = (23, 30)
DHAN_FUNDS_URL = "https://api.dhan.co/v2/fundlimit"


def fetch_account_balance(client_id, access_token):
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


class TradingApp:
    def __init__(self, strategy_tf, symbols_filter=None, squareoff_symbols=None,
                 variation="ha_static", rsi_length=14, rsi_buy_level=52.0,
                 rsi_sell_level=32.0, kc_length=21, kc_atr_length=21,
                 kc_multiplier=0.5, buffer_overrides=None, lot_size_overrides=None,
                 live_mode=False, order_type="MARKET", trigger_offset=0.0,
                 limit_offset=0.0, global_sl_pct=0.0,
                 mcx_session_end=MCX_SESSION_END_DEFAULT,
                 client_id="", access_token=""):
        global DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN
        load_dotenv(dotenv_path=ENV_FILE, override=True)
        DHAN_CLIENT_ID    = os.getenv("DHAN_CLIENT_ID", "").strip()
        DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
        if client_id:    DHAN_CLIENT_ID    = str(client_id).strip()
        if access_token: DHAN_ACCESS_TOKEN = str(access_token).strip()
        if not DHAN_ACCESS_TOKEN:
            try:
                from dhan_token_manager import read_shared_token
                s = read_shared_token()
                if s.get("access_token"): DHAN_ACCESS_TOKEN = s["access_token"]
                if not DHAN_CLIENT_ID and s.get("client_id"): DHAN_CLIENT_ID = s["client_id"]
            except Exception:
                pass
        if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
            raise RuntimeError("Missing DHAN credentials — please generate a token first.")

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

        self.project_dir = BASE_DIR
        self.state_path  = BASE_DIR / "paper_state.json"

        # Date-stamped daily log and trade files
        today      = __import__("datetime").date.today().strftime("%Y-%m-%d")
        logs_dir   = BASE_DIR / "logs"
        trades_dir = BASE_DIR / "trades"
        logs_dir.mkdir(exist_ok=True)
        trades_dir.mkdir(exist_ok=True)
        self.log_path       = logs_dir   / f"{today}.log"
        self.trade_log_path = trades_dir / f"{today}.csv"

        self._setup_logging()
        self.logger.info("Mode=%s | TF=%dm | Variation=%s | OrderType=%s | GSL=%.1f%%",
            "LIVE" if self.live_mode else "PAPER", self.strategy_tf,
            self.variation, self.order_type, self.global_sl_pct)

        self.live_engine = None
        if self.live_mode:
            from live_order_engine import LiveOrderEngine
            self.live_engine = LiveOrderEngine(
                client_id=DHAN_CLIENT_ID, access_token=DHAN_ACCESS_TOKEN,
                logger=self.logger)

        if self.global_sl_pct > 0:
            bal = fetch_account_balance(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
            if bal > 0:
                self.global_sl_rupees = bal * self.global_sl_pct / 100.0
                self.logger.info("Global SL: %.1f%% of ₹%.2f = ₹%.2f",
                                 self.global_sl_pct, bal, self.global_sl_rupees)

        self.instruments = build_instrument_list(
            symbol_filter=symbols_filter, logger=self.logger)
        if not self.instruments:
            raise RuntimeError("No instruments resolved.")
        for inst in self.instruments:
            self.logger.info("Instrument: %s [%s] secId=%s lot=%s contract=%s",
                inst["name"], inst["exchange"], inst["security_id"],
                inst.get("lot_size", 1), inst.get("contract_display", ""))

        self.sec_to_inst = {str(x["security_id"]): x for x in self.instruments}
        self.strategies  = {}
        self.paper       = {}
        self._ensure_trade_log_header()

        for inst in self.instruments:
            sec      = str(inst["security_id"])
            sym_name = inst["name"].upper()
            sym_buf  = self.buffer_overrides.get(sym_name)
            sym_lot  = self.lot_size_overrides.get(sym_name, inst.get("lot_size", 1))
            self.strategies[sec] = HAStaticTriggerStrategy(
                inst["name"], self.strategy_tf,
                variation=self.variation, rsi_length=self.rsi_length,
                rsi_buy_level=self.rsi_buy_level, rsi_sell_level=self.rsi_sell_level,
                kc_length=self.kc_length, kc_atr_length=self.kc_atr_length,
                kc_multiplier=self.kc_multiplier, buffer_override=sym_buf,
                exchange=inst.get("exchange", ""))
            self.paper[sec] = PaperTradeEngine(
                sym_lot, inst.get("display_prec", 2),
                event_callback=self._on_trade_event, symbol_name=inst["name"])

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
        self.logger.info("Startup complete. REST poller armed.")
        self._apply_startup_squareoff()
        self._save_state()

    # ── Logging ──────────────────────────────────────────────────────────────
    def _setup_logging(self):
        self.logger = logging.getLogger(f"dhan_ha_{id(self)}")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()
        fh = logging.handlers.TimedRotatingFileHandler(
            self.log_path, when="midnight", interval=1,
            backupCount=30, encoding="utf-8", utc=False)
        fh.suffix = "%Y-%m-%d"
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        self.logger.addHandler(fh)
        self.logger.propagate = False
        self.logger.info("==== App start ==== log=%s", self.log_path)

    # ── Trade log ─────────────────────────────────────────────────────────────
    def _get_todays_trade_log(self) -> Path:
        today = __import__("datetime").date.today().strftime("%Y-%m-%d")
        trades_dir = BASE_DIR / "trades"
        trades_dir.mkdir(exist_ok=True)
        path = trades_dir / f"{today}.csv"
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
        self._get_todays_trade_log()

    def _append_trade_log(self, payload):
        import datetime as _dt
        def _fmt(epoch):
            if epoch is None: return ""
            try: return _dt.datetime.fromtimestamp(int(epoch)).strftime("%Y-%m-%d %H:%M:%S")
            except: return str(epoch)

        is_open = payload.get("event_type", "").startswith("OPEN")
        path = self._get_todays_trade_log()
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
        return {
            "strategy_tf": self.strategy_tf, "variation": self.variation,
            "live_mode": self.live_mode, "order_type": self.order_type,
            "buffer_overrides": self.buffer_overrides,
            "lot_size_overrides": self.lot_size_overrides,
            "symbols": {sec: {
                "instrument": self.sec_to_inst[sec],
                "strategy":   self.strategies[sec].persist_state(),
                "paper":      self.paper[sec].persist_state(),
            } for sec in self.sec_to_inst},
        }

    def _save_state(self):
        try:
            self.state_path.write_text(json.dumps(self._state_blob(), indent=2))
        except Exception as e:
            self.logger.warning("State save failed: %s", e)

    def _load_state(self):
        if not self.state_path.exists(): return False
        try:
            data = json.loads(self.state_path.read_text())
        except Exception: return False
        if int(data.get("strategy_tf", 0)) != self.strategy_tf: return False
        if str(data.get("variation", "")).lower() != self.variation: return False
        restored = 0
        for sec, blob in data.get("symbols", {}).items():
            if sec in self.strategies:
                self.strategies[sec].restore_state(blob.get("strategy", {}))
                self.paper[sec].restore_state(blob.get("paper", {}))
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
            p = self.paper[sec]
            p.position_side = None; p.entry_price = None; p.entry_ts = None
            s._rebuild_and_update_pending()
            if s.pending_side:
                self.logger.info("Startup: %s pending %s @ %.4f",
                    self.sec_to_inst[sec]["name"], s.pending_side, s.pending_trigger or 0)
            else:
                s.last_event = "Startup: watching..."

    # ── Square off ────────────────────────────────────────────────────────────
    def _apply_startup_squareoff(self):
        if not self.squareoff_symbols: return
        for sec, inst in self.sec_to_inst.items():
            if "ALL" not in self.squareoff_symbols and inst["name"].upper() not in self.squareoff_symbols: continue
            snap = self.market.engines[sec].snapshot()
            ltp  = snap["ltp"]
            ts   = snap["ltt_epoch"] or int(time.time())
            if ltp is None or self.paper[sec].position_side is None: continue
            self.paper[sec].square_off(float(ltp), int(ts))
            self.strategies[sec].clear_trade_tracking(None)
            self.strategies[sec].pending_side = None

    def square_off_all(self):
        for sec, inst in self.sec_to_inst.items():
            if self.paper[sec].position_side is None: continue
            snap = self.market.engines[sec].snapshot()
            ltp  = snap["ltp"]
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
                    paper = self.paper[sec].snapshot(base["ltp"])
                    total_pnl += float(paper["realized_pnl"]) + float(paper["unrealized_pnl"])
                if total_pnl <= -abs(self.global_sl_rupees):
                    self.global_sl_hit = True
                    self.logger.warning("GLOBAL SL HIT: P&L ₹%.2f. Squaring off all.", total_pnl)
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
                if current is None:
                    self._mcx_session_closed_today.add(key); continue
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
        if self.order_type == "MARKET":   return 0.0, 0.0
        elif self.order_type == "SL-M":
            trig = signal_price + self.trigger_offset if is_buy else signal_price - self.trigger_offset
            return 0.0, trig
        elif self.order_type == "LIMIT":
            lmt = signal_price + self.limit_offset if is_buy else signal_price - self.limit_offset
            return lmt, 0.0
        return 0.0, 0.0

    def _execute_signal(self, sec, sig, ts_epoch):
        if self.global_sl_hit: return
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
                self.logger.error("Live order error %s: %s", inst["name"], err)
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

    def _handle_strategy_exit_if_any(self, sec, fallback_ts):
        exit_sig = self.strategies[sec].check_stoploss_exit()
        if not exit_sig: return
        paper = self.paper[sec]
        inst  = self.sec_to_inst[sec]
        side_match = ((exit_sig["exit_side"]=="LONG" and paper.position_side=="LONG") or
                      (exit_sig["exit_side"]=="SHORT" and paper.position_side=="SHORT"))
        if not side_match: return
        close_side = "SELL" if exit_sig["exit_side"] == "LONG" else "BUY"
        if self.live_mode and self.live_engine:
            def _on_sl(fp, oid, _sec=sec):
                self.paper[_sec].square_off(fp, int(time.time()))
                self.strategies[_sec].clear_trade_tracking(None)
                self._save_state()
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
        self.strategies[sec].on_signal_aligned_position(self.paper[sec].position_side)
        if self.sec_to_inst[sec].get("exchange") == "MCX_COMM":
            self._check_mcx_session_end()
        sig = self.strategies[sec].check_intrabar_range_hit(
            float(row_1m["high"]), float(row_1m["low"]))
        if sig:
            self._execute_signal(sec, sig, int(row_1m["bucket"]) + 59)
        self._save_state()

    def _on_ltp(self, sec, ltp, ts_epoch):
        self.strategies[sec].on_signal_aligned_position(self.paper[sec].position_side)
        signal_hit = self.strategies[sec].check_trigger_hit(ltp)
        if signal_hit:
            self._execute_signal(sec, signal_hit, ts_epoch)

    # ── Lifecycle ─────────────────────────────────────────────────────────────
    def _run_ui(self):
        while not self.stop_event.is_set():
            try: print_dashboard(self)
            except Exception as e: self.logger.warning("UI error: %s", e)
            time.sleep(1)

    def start(self, with_terminal_ui=True):
        self.market.start()
        if self.global_sl_rupees > 0:
            self._gsl_thread = threading.Thread(target=self._global_sl_monitor, daemon=True)
            self._gsl_thread.start()
        if with_terminal_ui:
            print(f"TF={self.strategy_tf}m | {self.variation} | {'LIVE' if self.live_mode else 'PAPER'}")
            time.sleep(1)
            self.ui_thread = threading.Thread(target=self._run_ui, daemon=True)
            self.ui_thread.start()

    def stop(self):
        self.stop_event.set()
        self._save_state()
        self.market.stop()
        self.logger.info("==== App stop ====")

    # ── Snapshot ──────────────────────────────────────────────────────────────
    def get_snapshot(self):
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
            paper = self.paper[sec].snapshot(base["ltp"])
            prec  = int(inst["display_prec"])
            result["total_unrealized"] += float(paper["unrealized_pnl"])
            result["total_realized"]   += float(paper["realized_pnl"])
            ha_last = strat["ha_last"]
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


def main():
    if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
        raise SystemExit("Missing DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN in .env")
    parser = argparse.ArgumentParser()
    parser.add_argument("--tf",       type=int, default=65, choices=[1,3,5,7,9,45,65,130])
    parser.add_argument("--symbols",  type=str, default="all")
    parser.add_argument("--variation",type=str, default="ha_static", choices=sorted(SUPPORTED_VARIATIONS))
    parser.add_argument("--live",     action="store_true")
    parser.add_argument("--order-type",type=str,default="MARKET",choices=["MARKET","SL-M","LIMIT"])
    parser.add_argument("--global-sl-pct",type=float,default=0.0)
    args = parser.parse_args()
    sym_key = args.symbols.strip().lower()
    sym_filter = SYMBOL_PRESETS.get(sym_key) if sym_key in SYMBOL_PRESETS else \
                 [x.strip().upper() for x in args.symbols.split(",") if x.strip()]
    app = TradingApp(strategy_tf=args.tf, symbols_filter=sym_filter,
                     variation=args.variation, live_mode=args.live,
                     order_type=args.order_type, global_sl_pct=args.global_sl_pct)
    def _sig(s, f): app.stop(); raise SystemExit(0)
    signal.signal(signal.SIGINT, _sig); signal.signal(signal.SIGTERM, _sig)
    app.start(with_terminal_ui=True)
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        app.stop()

if __name__ == "__main__":
    main()
