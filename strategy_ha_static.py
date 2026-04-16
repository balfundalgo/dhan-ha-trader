from collections import deque
from typing import Dict, Any, Optional, List, Tuple

from market_data import strategy_bucket_start, compute_ha_series

BUFFER_MAP = {
    "NIFTY 50": 3.0,
    "BANKNIFTY": 3.0,
    "SENSEX": 3.0,
    "CRUDEOILM": 3.0,
    "GOLDPETAL": 10.0,
    "SILVERMIC": 10.0,
}

SUPPORTED_VARIATIONS = {
    "ha_static",          # V1 variation 1
    "two_consecutive",    # V1 variation 2
    "keltner",            # V2 variation 1
    "rsi_keltner",        # V2 variation 2
}

MIN_HH_LL_DIFF = 1.0
MIN_BAND_DISTANCE = 1.0


def _ema(values: List[float], length: int) -> List[Optional[float]]:
    length = max(1, int(length))
    out: List[Optional[float]] = []
    alpha = 2.0 / (length + 1.0)
    ema_val: Optional[float] = None
    for v in values:
        v = float(v)
        if ema_val is None:
            ema_val = v
        else:
            ema_val = alpha * v + (1.0 - alpha) * ema_val
        out.append(ema_val)
    return out


def _atr(rows: List[Dict[str, Any]], length: int) -> List[Optional[float]]:
    tr_values: List[float] = []
    prev_close: Optional[float] = None
    for row in rows:
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])
        if prev_close is None:
            tr = h - l
        else:
            tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        tr_values.append(tr)
        prev_close = c
    return _ema(tr_values, length)


def _rsi(closes: List[float], length: int) -> List[Optional[float]]:
    length = max(1, int(length))
    out: List[Optional[float]] = []
    avg_gain: Optional[float] = None
    avg_loss: Optional[float] = None
    prev_close: Optional[float] = None
    for close in closes:
        close = float(close)
        if prev_close is None:
            out.append(None)
            prev_close = close
            continue
        change = close - prev_close
        gain = max(change, 0.0)
        loss = max(-change, 0.0)
        if avg_gain is None or avg_loss is None:
            avg_gain = gain
            avg_loss = loss
        else:
            avg_gain = ((avg_gain * (length - 1)) + gain) / length
            avg_loss = ((avg_loss * (length - 1)) + loss) / length
        if avg_loss == 0:
            rsi_val = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi_val = 100.0 - (100.0 / (1.0 + rs))
        out.append(rsi_val)
        prev_close = close
    return out


class HAStaticTriggerStrategy:
    def __init__(
        self,
        name: str,
        strategy_tf: int,
        variation: str = "ha_static",
        rsi_length: int = 14,
        rsi_buy_level: float = 52.0,
        rsi_sell_level: float = 32.0,
        kc_length: int = 21,
        kc_atr_length: int = 21,
        kc_multiplier: float = 0.5,
        buffer_override: float = None,
        exchange: str = "",
    ):
        self.name = name
        self.strategy_tf = int(strategy_tf)
        self.variation = str(variation).strip().lower()
        if self.variation not in SUPPORTED_VARIATIONS:
            self.variation = "ha_static"

        self.rsi_length = int(rsi_length)
        self.rsi_buy_level = float(rsi_buy_level)
        self.rsi_sell_level = float(rsi_sell_level)
        self.kc_length = int(kc_length)
        self.kc_atr_length = int(kc_atr_length)
        self.kc_multiplier = float(kc_multiplier)
        self.buffer = float(buffer_override) if buffer_override is not None else float(BUFFER_MAP.get(self.name, 1.0))

        # Session start in UTC seconds from day midnight for bucket alignment.
        # MCX_COMM: 09:00 IST = 03:30 UTC = 12600s
        # NSE/BSE:  09:15 IST = 03:45 UTC = 13500s
        exch = str(exchange).strip().upper()
        if exch == "MCX_COMM":
            self._session_start_utc_secs = 9 * 3600 - 19800        # 09:00 IST
        else:
            self._session_start_utc_secs = 9 * 3600 + 15 * 60 - 19800  # 09:15 IST

        self.agg_current: Optional[Dict[str, Any]] = None
        self.agg_completed = deque(maxlen=500)
        self.ha_completed = deque(maxlen=500)
        self.pending_side: Optional[str] = None
        self.pending_trigger: Optional[float] = None
        self.pending_from_bucket: Optional[int] = None
        self.last_event: str = "-"
        self.session_anchor_epoch: Optional[int] = None
        self.last_1m_bucket: Optional[int] = None
        self.session_gap_reset_sec: int = 90 * 60

        self.entry_wait_bucket: Optional[int] = None
        self.entry_wait_side: Optional[str] = None
        self.sl_side: Optional[str] = None
        self.sl_price: Optional[float] = None
        self.sl_from_bucket: Optional[int] = None

    def on_new_1m_candle(self, row_1m: Dict[str, Any]) -> bool:
        ts = int(row_1m["bucket"])

        # Session-anchor aligned bucket — matches exchange boundaries exactly.
        # MCX 45m: 09:00, 09:45, 10:30...  NSE 45m: 09:15, 10:00, 10:45...
        if self.strategy_tf <= 1:
            sb = ts - (ts % 60)
        else:
            tf_sec    = self.strategy_tf * 60
            day_start = (ts // 86400) * 86400
            anchor    = day_start + self._session_start_utc_secs
            elapsed   = ts - anchor
            if elapsed < 0:
                sb = (ts // tf_sec) * tf_sec   # pre-session fallback
            else:
                sb = anchor + (elapsed // tf_sec) * tf_sec

        is_new_session = False
        if self.last_1m_bucket is None:
            is_new_session = True
        else:
            gap = ts - int(self.last_1m_bucket)
            if gap > self.session_gap_reset_sec:
                is_new_session = True

        self.last_1m_bucket = ts

        if self.agg_current is None:
            self.agg_current = {
                "bucket": sb,
                "open": float(row_1m["open"]),
                "high": float(row_1m["high"]),
                "low": float(row_1m["low"]),
                "close": float(row_1m["close"]),
            }
            return False

        cur_bucket = int(self.agg_current["bucket"])

        if is_new_session and ts != cur_bucket:
            # Push yesterday's incomplete forming candle into agg_completed
            # so it becomes the "carry" candle for cross-day HH/LL check.
            finalized = dict(self.agg_current)
            self.agg_completed.append(finalized)

            if self.variation == "two_consecutive":
                # Do NOT arm a trigger yet. We need today's first candle to
                # close before checking HH/LL. Clear any stale pending and wait.
                self._cancel_pending("New session: waiting for today's first candle (two_consecutive)")
            else:
                # For other variations: run normal rebuild (ha_static etc.)
                self._rebuild_and_update_pending()

            self.agg_current = {
                "bucket": sb,
                "open": float(row_1m["open"]),
                "high": float(row_1m["high"]),
                "low": float(row_1m["low"]),
                "close": float(row_1m["close"]),
            }
            return True

        if sb == cur_bucket:
            self.agg_current["high"] = max(float(self.agg_current["high"]), float(row_1m["high"]))
            self.agg_current["low"] = min(float(self.agg_current["low"]), float(row_1m["low"]))
            self.agg_current["close"] = float(row_1m["close"])
            return False

        if sb > cur_bucket:
            finalized = dict(self.agg_current)
            self.agg_completed.append(finalized)
            self._rebuild_and_update_pending()
            self.agg_current = {
                "bucket": sb,
                "open": float(row_1m["open"]),
                "high": float(row_1m["high"]),
                "low": float(row_1m["low"]),
                "close": float(row_1m["close"]),
            }
            return True

        return False

    def _rebuild_and_update_pending(self):
        rows = list(self.agg_completed)
        if not rows:
            return
        ha_all = compute_ha_series(rows)
        self.ha_completed = deque(ha_all, maxlen=500)
        self._maybe_arm_stoploss_on_closed_bucket(rows[-1])
        self._update_pending_from_latest(rows, ha_all)
        self._maybe_trigger_stoploss_on_latest_close(rows[-1])

    def _latest_colors(self, ha_all: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
        if not ha_all:
            return None, None
        curr = ha_all[-1]["color"]
        prev = ha_all[-2]["color"] if len(ha_all) >= 2 else None
        return curr, prev

    def _cancel_pending(self, reason: str):
        self.pending_side = None
        self.pending_trigger = None
        self.pending_from_bucket = None
        self.last_event = reason

    def _set_pending(self, side: str, trigger: float, bucket: int, reason: str):
        self.pending_side = side
        self.pending_trigger = float(trigger)
        self.pending_from_bucket = int(bucket)
        self.last_event = reason

    def _update_pending_from_latest(self, rows: List[Dict[str, Any]], ha_all: List[Dict[str, Any]]):
        if not ha_all:
            return
        last_ha = ha_all[-1]
        last_color = last_ha["color"]

        if last_color == "DOJI":
            self._cancel_pending("Pending cancelled (HA DOJI)")
            return

        opposite_side = "SELL" if last_color == "BULL" else "BUY"
        if self.pending_side == opposite_side:
            self._cancel_pending(f"Pending cancelled (opposite {last_color})")

        # Keep same-direction pending alive exactly as requested.
        if self.pending_side is not None:
            return

        if self.variation == "ha_static":
            if last_color == "BULL":
                self._set_pending("BUY", float(last_ha["high"]) + self.buffer, int(last_ha["bucket"]), "New pending BUY")
            elif last_color == "BEAR":
                self._set_pending("SELL", float(last_ha["low"]) - self.buffer, int(last_ha["bucket"]), "New pending SELL")
            return

        if self.variation == "two_consecutive":
            if len(ha_all) < 2:
                return
            prev_ha = ha_all[-2]
            if prev_ha["color"] == "BULL" and last_color == "BULL":
                diff = float(last_ha["high"]) - float(prev_ha["high"])
                if diff >= MIN_HH_LL_DIFF:
                    self._set_pending("BUY", float(last_ha["high"]) + self.buffer, int(last_ha["bucket"]), "New pending BUY (2x HH)")
            elif prev_ha["color"] == "BEAR" and last_color == "BEAR":
                diff = float(prev_ha["low"]) - float(last_ha["low"])
                if diff >= MIN_HH_LL_DIFF:
                    self._set_pending("SELL", float(last_ha["low"]) - self.buffer, int(last_ha["bucket"]), "New pending SELL (2x LL)")
            return

        closes = [float(r["close"]) for r in rows]
        mid = _ema(closes, self.kc_length)
        atr_vals = _atr(rows, self.kc_atr_length)
        mid_last = mid[-1]
        atr_last = atr_vals[-1]
        if mid_last is None or atr_last is None:
            return
        upper = float(mid_last) + self.kc_multiplier * float(atr_last)
        lower = float(mid_last) - self.kc_multiplier * float(atr_last)
        close_last = float(rows[-1]["close"])

        keltner_buy_ok = (last_color == "BULL" and close_last > upper and (close_last - upper) >= MIN_BAND_DISTANCE)
        keltner_sell_ok = (last_color == "BEAR" and close_last < lower and (lower - close_last) >= MIN_BAND_DISTANCE)

        if self.variation == "keltner":
            if keltner_buy_ok:
                self._set_pending("BUY", float(last_ha["high"]) + self.buffer, int(last_ha["bucket"]), "New pending BUY (KC)")
            elif keltner_sell_ok:
                self._set_pending("SELL", float(last_ha["low"]) - self.buffer, int(last_ha["bucket"]), "New pending SELL (KC)")
            return

        if self.variation == "rsi_keltner":
            rsi_vals = _rsi(closes, self.rsi_length)
            rsi_last = rsi_vals[-1]
            if rsi_last is None:
                return
            if keltner_buy_ok and float(rsi_last) > self.rsi_buy_level:
                self._set_pending("BUY", float(last_ha["high"]) + self.buffer, int(last_ha["bucket"]), "New pending BUY (RSI+KC)")
            elif keltner_sell_ok and float(rsi_last) < self.rsi_sell_level:
                self._set_pending("SELL", float(last_ha["low"]) - self.buffer, int(last_ha["bucket"]), "New pending SELL (RSI+KC)")

    def on_signal_aligned_position(self, aligned_side: Optional[str]):
        if aligned_side == "LONG" and self.pending_side == "BUY":
            self._cancel_pending("Pending cancelled (already long)")
        elif aligned_side == "SHORT" and self.pending_side == "SELL":
            self._cancel_pending("Pending cancelled (already short)")

    def on_trade_executed(self, signal_side: str):
        signal_side = str(signal_side).upper()
        if self.agg_current is None:
            self.entry_wait_bucket = None
            self.entry_wait_side = None
            return
        self.entry_wait_bucket = int(self.agg_current["bucket"])
        self.entry_wait_side = "LONG" if signal_side == "BUY" else "SHORT"
        if self.entry_wait_side == "LONG":
            self.sl_side = None if self.variation not in {"keltner", "rsi_keltner"} else self.sl_side
        if self.variation not in {"keltner", "rsi_keltner"}:
            self.sl_side = None
            self.sl_price = None
            self.sl_from_bucket = None
        self.last_event = f"{signal_side} executed"

    def _maybe_arm_stoploss_on_closed_bucket(self, closed_row: Dict[str, Any]):
        if self.variation not in {"keltner", "rsi_keltner"}:
            return
        if self.entry_wait_bucket is None or self.entry_wait_side is None:
            return
        closed_bucket = int(closed_row["bucket"])
        if closed_bucket != int(self.entry_wait_bucket):
            return
        if self.entry_wait_side == "LONG":
            self.sl_side = "LONG"
            self.sl_price = float(closed_row["low"])
        else:
            self.sl_side = "SHORT"
            self.sl_price = float(closed_row["high"])
        self.sl_from_bucket = closed_bucket
        self.entry_wait_bucket = None
        self.entry_wait_side = None
        self.last_event = f"SL armed @ {self.sl_price:.2f}"

    def check_stoploss_exit(self) -> Optional[Dict[str, Any]]:
        if getattr(self, "_queued_sl_exit", None):
            exit_sig = self._queued_sl_exit
            self._queued_sl_exit = None
            return exit_sig
        return None

    def _maybe_trigger_stoploss_on_latest_close(self, closed_row: Dict[str, Any]):
        self._queued_sl_exit = None
        if self.variation not in {"keltner", "rsi_keltner"}:
            return
        if self.sl_side is None or self.sl_price is None or self.sl_from_bucket is None:
            return
        closed_bucket = int(closed_row["bucket"])
        if closed_bucket <= int(self.sl_from_bucket):
            return
        close_price = float(closed_row["close"])
        if self.sl_side == "LONG" and close_price < float(self.sl_price):
            self._queued_sl_exit = {"exit_side": "LONG", "exit_price": close_price, "reason": "CLOSE_BELOW_SL"}
            self.last_event = f"LONG SL hit @ close {close_price:.2f}"
            self.sl_side = None
            self.sl_price = None
            self.sl_from_bucket = None
        elif self.sl_side == "SHORT" and close_price > float(self.sl_price):
            self._queued_sl_exit = {"exit_side": "SHORT", "exit_price": close_price, "reason": "CLOSE_ABOVE_SL"}
            self.last_event = f"SHORT SL hit @ close {close_price:.2f}"
            self.sl_side = None
            self.sl_price = None
            self.sl_from_bucket = None

    def clear_trade_tracking(self, position_side: Optional[str]):
        if position_side is None:
            self.entry_wait_bucket = None
            self.entry_wait_side = None
            self.sl_side = None
            self.sl_price = None
            self.sl_from_bucket = None

    def check_trigger_hit(self, ltp: float) -> Optional[Dict[str, Any]]:
        if self.pending_side is None or self.pending_trigger is None:
            return None
        if self.pending_side == "BUY" and ltp >= self.pending_trigger:
            trigger = self.pending_trigger
            self.pending_side = None
            self.pending_trigger = None
            self.pending_from_bucket = None
            self.last_event = "BUY trigger hit"
            return {"side": "BUY", "price": trigger}
        if self.pending_side == "SELL" and ltp <= self.pending_trigger:
            trigger = self.pending_trigger
            self.pending_side = None
            self.pending_trigger = None
            self.pending_from_bucket = None
            self.last_event = "SELL trigger hit"
            return {"side": "SELL", "price": trigger}
        return None

    def check_intrabar_range_hit(self, candle_high: float, candle_low: float) -> Optional[Dict[str, Any]]:
        if self.pending_side is None or self.pending_trigger is None:
            return None
        if self.pending_side == "BUY" and float(candle_high) >= float(self.pending_trigger):
            trigger = self.pending_trigger
            self.pending_side = None
            self.pending_trigger = None
            self.pending_from_bucket = None
            self.last_event = "BUY trigger hit (1m candle)"
            return {"side": "BUY", "price": trigger}
        if self.pending_side == "SELL" and float(candle_low) <= float(self.pending_trigger):
            trigger = self.pending_trigger
            self.pending_side = None
            self.pending_trigger = None
            self.pending_from_bucket = None
            self.last_event = "SELL trigger hit (1m candle)"
            return {"side": "SELL", "price": trigger}
        return None

    def restore_state(self, data: Dict[str, Any]):
        self.pending_side = data.get("pending_side")
        self.pending_trigger = data.get("pending_trigger")
        self.pending_from_bucket = data.get("pending_from_bucket")
        self.last_event = data.get("last_event", self.last_event)
        self.session_anchor_epoch = data.get("session_anchor_epoch")
        self.last_1m_bucket = data.get("last_1m_bucket")
        self.entry_wait_bucket = data.get("entry_wait_bucket")
        self.entry_wait_side = data.get("entry_wait_side")
        self.sl_side = data.get("sl_side")
        self.sl_price = data.get("sl_price")
        self.sl_from_bucket = data.get("sl_from_bucket")

    def persist_state(self) -> Dict[str, Any]:
        return {
            "pending_side": self.pending_side,
            "pending_trigger": self.pending_trigger,
            "pending_from_bucket": self.pending_from_bucket,
            "last_event": self.last_event,
            "session_anchor_epoch": self.session_anchor_epoch,
            "last_1m_bucket": self.last_1m_bucket,
            "entry_wait_bucket": self.entry_wait_bucket,
            "entry_wait_side": self.entry_wait_side,
            "sl_side": self.sl_side,
            "sl_price": self.sl_price,
            "sl_from_bucket": self.sl_from_bucket,
        }

    def snapshot(self) -> Dict[str, Any]:
        return {
            "variation": self.variation,
            "pending_side": self.pending_side,
            "pending_trigger": self.pending_trigger,
            "pending_from_bucket": self.pending_from_bucket,
            "last_event": self.last_event,
            "agg_current": dict(self.agg_current) if self.agg_current else None,
            "agg_history": list(self.agg_completed),
            "ha_history": list(self.ha_completed),
            "ha_last": self.ha_completed[-1] if self.ha_completed else None,
            "sl_side": self.sl_side,
            "sl_price": self.sl_price,
            "sl_from_bucket": self.sl_from_bucket,
        }
