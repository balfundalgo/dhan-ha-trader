import os
import sys
import time
import signal
import argparse
import threading
import json
import csv
import logging
from pathlib import Path
from dotenv import load_dotenv

from market_data import build_instrument_list, MarketDataEngine, fetch_intraday_1m_history
from strategy_ha_static import HAStaticTriggerStrategy, SUPPORTED_VARIATIONS
from paper_engine import PaperTradeEngine
from dashboard import print_dashboard

# ── PyInstaller-safe base directory ──────────────────────────────────────────
if getattr(sys, 'frozen', False):
    BASE_DIR = Path(sys.executable).parent
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


class TradingApp:
    def __init__(
        self,
        strategy_tf: int,
        symbols_filter=None,
        squareoff_symbols=None,
        variation: str = "ha_static",
        rsi_length: int = 14,
        rsi_buy_level: float = 52.0,
        rsi_sell_level: float = 32.0,
        kc_length: int = 21,
        kc_atr_length: int = 21,
        kc_multiplier: float = 0.5,
        buffer_overrides: dict = None,
        client_id: str = "",
        access_token: str = "",
    ):
        global DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN

        if client_id:
            DHAN_CLIENT_ID = client_id.strip()
        if access_token:
            DHAN_ACCESS_TOKEN = access_token.strip()

        if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
            raise RuntimeError("Missing DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN — please generate a token first.")

        self.strategy_tf     = int(strategy_tf)
        self.variation       = str(variation).strip().lower()
        self.rsi_length      = int(rsi_length)
        self.rsi_buy_level   = float(rsi_buy_level)
        self.rsi_sell_level  = float(rsi_sell_level)
        self.kc_length       = int(kc_length)
        self.kc_atr_length   = int(kc_atr_length)
        self.kc_multiplier   = float(kc_multiplier)
        # Per-symbol buffer overrides: {"CRUDEOILM": 5.0, "GOLDPETAL": 12.0, ...}
        self.buffer_overrides = {k.upper(): float(v) for k, v in (buffer_overrides or {}).items() if v is not None}
        self.squareoff_symbols = {s.strip().upper() for s in (squareoff_symbols or []) if s.strip()}

        self.project_dir    = BASE_DIR
        self.state_path     = self.project_dir / "paper_state.json"
        self.log_path       = self.project_dir / "paper_runtime.log"
        self.trade_log_path = self.project_dir / "trade_log.csv"
        self._setup_logging()

        self.instruments = build_instrument_list(symbol_filter=symbols_filter, logger=self.logger)
        if not self.instruments:
            raise RuntimeError("No instruments resolved. Check symbols / instrument master.")
        self.logger.info("Resolved %d instruments.", len(self.instruments))
        for inst in self.instruments:
            self.logger.info(
                "Instrument: %s [%s] secId=%s lot=%s contract=%s",
                inst["name"], inst["exchange"], inst["security_id"],
                inst.get("lot_size", 1), inst.get("contract_display", "")
            )

        self.sec_to_inst = {str(x["security_id"]): x for x in self.instruments}
        self.strategies  = {}
        self.paper       = {}
        self._ensure_trade_log_header()

        for inst in self.instruments:
            sec = str(inst["security_id"])
            sym_buf = self.buffer_overrides.get(inst["name"].upper())
            self.strategies[sec] = HAStaticTriggerStrategy(
                inst["name"], self.strategy_tf,
                variation=self.variation,
                rsi_length=self.rsi_length,
                rsi_buy_level=self.rsi_buy_level,
                rsi_sell_level=self.rsi_sell_level,
                kc_length=self.kc_length,
                kc_atr_length=self.kc_atr_length,
                kc_multiplier=self.kc_multiplier,
                buffer_override=sym_buf,  # None = use BUFFER_MAP default
            )
            self.paper[sec] = PaperTradeEngine(
                inst["lot_size"], inst.get("display_prec", 2),
                event_callback=self._on_trade_event,
                symbol_name=inst["name"],
            )

        self.market = MarketDataEngine(
            client_id=DHAN_CLIENT_ID,
            access_token=DHAN_ACCESS_TOKEN,
            instruments=self.instruments,
            on_new_1m_candle=self._on_new_1m_candle,
            on_ltp=self._on_ltp,
        )

        self.stop_event  = threading.Event()
        self.ui_thread   = None
        self.symbol_live_enabled = {sec: False for sec in self.sec_to_inst}
        self.state_restored = False

        self._run_startup_backfill()
        self.state_restored = self._load_state()
        if not self.state_restored:
            self._reset_actionable_state_after_backfill()
            self.logger.info("Fresh start. Trading begins after first live 1m close per symbol.")
        else:
            for sec in self.symbol_live_enabled:
                self.symbol_live_enabled[sec] = True
            self.logger.info("State restored. Trading live immediately.")

        self._apply_startup_squareoff()
        self._save_state()

    def _setup_logging(self):
        self.logger = logging.getLogger(f"dhan_paper_{id(self)}")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()
        fh = logging.FileHandler(self.log_path, encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        self.logger.addHandler(fh)
        self.logger.propagate = False
        self.logger.info("==== App start ====")

    def _ensure_trade_log_header(self):
        if not self.trade_log_path.exists():
            with self.trade_log_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["ts", "symbol", "event_type", "position_side",
                                  "entry_price", "entry_ts", "exit_price",
                                  "closed_entry_price", "closed_pnl", "realized_pnl",
                                  "trade_count", "lot_size"])

    def _append_trade_log(self, payload):
        with self.trade_log_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                payload.get("ts"), payload.get("symbol"), payload.get("event_type"),
                payload.get("position_side"), payload.get("entry_price"), payload.get("entry_ts"),
                payload.get("exit_price"), payload.get("closed_entry_price"),
                payload.get("closed_pnl"), payload.get("realized_pnl"),
                payload.get("trade_count"), payload.get("lot_size"),
            ])

    def _on_trade_event(self, event_type, payload):
        payload["event_type"] = event_type
        self._append_trade_log(payload)
        self.logger.info("TRADE %s | %s", event_type, payload)
        self._save_state()

    def _state_blob(self):
        data = {
            "strategy_tf":     self.strategy_tf,
            "variation":       self.variation,
            "rsi_length":      self.rsi_length,
            "rsi_buy_level":   self.rsi_buy_level,
            "rsi_sell_level":  self.rsi_sell_level,
            "kc_length":       self.kc_length,
            "kc_atr_length":   self.kc_atr_length,
            "kc_multiplier":   self.kc_multiplier,
            "buffer_overrides": self.buffer_overrides,
            "symbols": {},
        }
        for sec in self.sec_to_inst:
            data["symbols"][sec] = {
                "instrument": self.sec_to_inst[sec],
                "strategy":   self.strategies[sec].persist_state(),
                "paper":      self.paper[sec].persist_state(),
            }
        return data

    def _save_state(self):
        try:
            self.state_path.write_text(json.dumps(self._state_blob(), indent=2))
        except Exception as e:
            self.logger.warning("State save failed: %s", e)

    def _load_state(self) -> bool:
        if not self.state_path.exists():
            self.logger.info("No previous state file found.")
            return False
        try:
            data = json.loads(self.state_path.read_text())
        except Exception as e:
            self.logger.warning("State load failed: %s", e)
            return False
        if int(data.get("strategy_tf", self.strategy_tf)) != self.strategy_tf:
            self.logger.info("State TF mismatch. Ignoring.")
            return False
        if str(data.get("variation", self.variation)).lower() != self.variation:
            self.logger.info("State variation mismatch. Ignoring.")
            return False
        symbols = data.get("symbols", {})
        restored = 0
        for sec, blob in symbols.items():
            if sec in self.strategies:
                self.strategies[sec].restore_state(blob.get("strategy", {}))
                self.paper[sec].restore_state(blob.get("paper", {}))
                restored += 1
        self.logger.info("Restored state for %d symbols.", restored)
        return restored > 0

    def _run_startup_backfill(self):
        self.logger.info("Starting startup history seed (no historical trade execution)...")
        total_rows = 0
        for inst in self.instruments:
            sec = str(inst["security_id"])
            rows = fetch_intraday_1m_history(DHAN_ACCESS_TOKEN, inst, days=5, logger=self.logger)
            for row in rows:
                self.market.engines[sec].seed_from_1m_candle(row)
                self.strategies[sec].on_new_1m_candle(row)
                self.market.engines[sec].last_ltp = float(row["close"])
                self.market.engines[sec].last_ltt_epoch = int(row["bucket"]) + 59
            total_rows += len(rows)
        self.logger.info("Startup history seed complete. Total 1m candles: %d", total_rows)

    def _reset_actionable_state_after_backfill(self):
        for sec in self.sec_to_inst:
            s = self.strategies[sec]
            s.pending_side = None; s.pending_trigger = None; s.pending_from_bucket = None
            s.entry_wait_bucket = None; s.entry_wait_side = None
            s.sl_side = None; s.sl_price = None; s.sl_from_bucket = None
            s.last_event = "Startup armed: waiting for first live 1m close"
            p = self.paper[sec]
            p.position_side = None; p.entry_price = None; p.entry_ts = None

    def _apply_startup_squareoff(self):
        if not self.squareoff_symbols:
            return
        for sec, inst in self.sec_to_inst.items():
            if "ALL" not in self.squareoff_symbols and inst["name"].upper() not in self.squareoff_symbols:
                continue
            snap = self.market.engines[sec].snapshot()
            ltp  = snap["ltp"]
            ts   = snap["ltt_epoch"] or int(time.time())
            if ltp is None or self.paper[sec].position_side is None:
                continue
            self.paper[sec].square_off(float(ltp), int(ts))
            self.strategies[sec].clear_trade_tracking(None)
            self.strategies[sec].pending_side = None
            self.strategies[sec].pending_trigger = None
            self.strategies[sec].pending_from_bucket = None
            self.logger.info("Square-off applied for %s at %s", inst["name"], ltp)

    def square_off_all(self):
        for sec, inst in self.sec_to_inst.items():
            if self.paper[sec].position_side is None:
                continue
            snap = self.market.engines[sec].snapshot()
            ltp  = snap["ltp"]
            ts   = snap["ltt_epoch"] or int(time.time())
            if ltp is None:
                continue
            self.paper[sec].square_off(float(ltp), int(ts))
            self.strategies[sec].clear_trade_tracking(None)
            self.strategies[sec].pending_side = None
            self.strategies[sec].pending_trigger = None
            self.strategies[sec].pending_from_bucket = None
            self.logger.info("GUI square-off for %s at %s", inst["name"], ltp)
        self._save_state()

    def _handle_strategy_exit_if_any(self, sec: str, fallback_ts: int):
        exit_sig = self.strategies[sec].check_stoploss_exit()
        if not exit_sig:
            return
        paper = self.paper[sec]
        if exit_sig["exit_side"] == "LONG" and paper.position_side == "LONG":
            paper.square_off(float(exit_sig["exit_price"]), int(fallback_ts))
            self.strategies[sec].clear_trade_tracking(None)
        elif exit_sig["exit_side"] == "SHORT" and paper.position_side == "SHORT":
            paper.square_off(float(exit_sig["exit_price"]), int(fallback_ts))
            self.strategies[sec].clear_trade_tracking(None)

    def _execute_signal(self, sec: str, sig: dict, ts_epoch: int):
        self.paper[sec].execute_signal(sig, ts_epoch)
        self.strategies[sec].on_trade_executed(sig["side"])
        self._save_state()

    def _on_new_1m_candle(self, sec: str, row_1m):
        self.strategies[sec].on_new_1m_candle(row_1m)
        self._handle_strategy_exit_if_any(sec, int(row_1m["bucket"]) + 59)
        self.strategies[sec].on_signal_aligned_position(self.paper[sec].position_side)
        if not self.symbol_live_enabled[sec]:
            self.symbol_live_enabled[sec] = True
            self.strategies[sec].last_event = "First live 1m close; live trading enabled"
            self._save_state()
            return
        sig = self.strategies[sec].check_intrabar_range_hit(
            float(row_1m["high"]), float(row_1m["low"]))
        if sig:
            self._execute_signal(sec, sig, int(row_1m["bucket"]) + 59)
        self._save_state()

    def _on_ltp(self, sec: str, ltp: float, ts_epoch: int):
        if not self.symbol_live_enabled[sec]:
            return
        self.strategies[sec].on_signal_aligned_position(self.paper[sec].position_side)
        signal_hit = self.strategies[sec].check_trigger_hit(ltp)
        if signal_hit:
            self._execute_signal(sec, signal_hit, ts_epoch)

    def _run_ui(self):
        while not self.stop_event.is_set():
            try:
                print_dashboard(self)
            except Exception as e:
                print(f"\nUI error: {e}")
                self.logger.warning("UI error: %s", e)
            time.sleep(1)

    def start(self, with_terminal_ui: bool = True):
        self.market.start()
        if with_terminal_ui:
            print(f"TF={self.strategy_tf}m | Variation={self.variation} | Buffers={self.buffer_overrides or 'default'}")
            for x in self.instruments:
                print(f"  - {x['name']} [{x.get('contract_display','')}] lot={x.get('lot_size',1)}")
            time.sleep(1)
            self.ui_thread = threading.Thread(target=self._run_ui, daemon=True)
            self.ui_thread.start()

    def stop(self):
        self.stop_event.set()
        self._save_state()
        self.market.stop()
        self.logger.info("==== App stop ====")

    def get_snapshot(self) -> dict:
        result = {
            "strategy_tf":      self.strategy_tf,
            "variation":        self.variation,
            "buffer_overrides":  self.buffer_overrides,
            "symbols":          [],
            "total_unrealized": 0.0,
            "total_realized":   0.0,
            "ws_uptime":        "-",
            "packets":          {},
            "ws_error":         None,
        }
        market_stats = self.market.market_snapshot()
        conn_time = market_stats["last_ws_connect_time"]
        if conn_time:
            result["ws_uptime"] = f"{int(time.time() - conn_time)}s"
        result["packets"]  = market_stats["packet_counts"]
        result["ws_error"] = market_stats["last_ws_error"]

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
                "prec":             prec,
                "ltp":              base["ltp"],
                "position":         paper["position_side"] or "-",
                "entry":            paper["entry_price"],
                "pending":          strat["pending_side"] or "-",
                "trigger":          strat["pending_trigger"],
                "unrealized":       float(paper["unrealized_pnl"]),
                "realized":         float(paper["realized_pnl"]),
                "lot":              int(paper["lot_size"]),
                "ha_color":         ha_last["color"] if ha_last else "-",
                "ha_streak":        int(ha_last["streak"]) if ha_last else 0,
                "event":            paper["last_event"] if paper["last_event"] != "-" else strat["last_event"],
                "ha_history":       strat["ha_history"][-5:],
                "sl_price":         strat.get("sl_price"),
                "sl_side":          strat.get("sl_side"),
            })
        return result


# ── CLI entry point ───────────────────────────────────────────────────────────
def parse_args():
    parser = argparse.ArgumentParser(description="Dhan WS HA Paper Trader")
    parser.add_argument("--tf",             type=int,   default=65, choices=[1, 45, 65, 130])
    parser.add_argument("--symbols",        type=str,   default="all")
    parser.add_argument("--squareoff",      type=str,   default="")
    parser.add_argument("--variation",      type=str,   default="ha_static", choices=sorted(SUPPORTED_VARIATIONS))
    parser.add_argument("--buffer",         type=float, default=None, help="Override default buffer for all symbols")
    parser.add_argument("--rsi-length",     type=int,   default=14)
    parser.add_argument("--rsi-buy",        type=float, default=52.0)
    parser.add_argument("--rsi-sell",       type=float, default=32.0)
    parser.add_argument("--kc-length",      type=int,   default=21)
    parser.add_argument("--kc-atr-length",  type=int,   default=21)
    parser.add_argument("--kc-mult",        type=float, default=0.5)
    return parser.parse_args()


def main():
    if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
        raise SystemExit("Missing DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN in .env")

    args = parse_args()
    symbol_key = args.symbols.strip().lower()
    symbols_filter = SYMBOL_PRESETS.get(symbol_key) if symbol_key in SYMBOL_PRESETS else \
                     [x.strip().upper() for x in args.symbols.split(",") if x.strip()]
    squareoff_symbols = [x.strip() for x in args.squareoff.split(",") if x.strip()] if args.squareoff.strip() else []

    app = TradingApp(
        strategy_tf=args.tf,
        symbols_filter=symbols_filter,
        squareoff_symbols=squareoff_symbols,
        variation=args.variation,
        buffer_override=args.buffer,
        rsi_length=args.rsi_length,
        rsi_buy_level=args.rsi_buy,
        rsi_sell_level=args.rsi_sell,
        kc_length=args.kc_length,
        kc_atr_length=args.kc_atr_length,
        kc_multiplier=args.kc_mult,
    )

    def _sig_handler(sig, frame):
        app.stop()
        raise SystemExit(0)

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)
    app.start(with_terminal_ui=True)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        app.stop()


if __name__ == "__main__":
    main()
