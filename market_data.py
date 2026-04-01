import time
import json
import struct
import threading
from datetime import datetime, timezone, timedelta
from collections import deque
from typing import Dict, Optional, Any, List

import requests
import pandas as pd
import websocket

INSTRUMENT_CSV_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
HIST_INTRADAY_URL = "https://api.dhan.co/v2/charts/intraday"

REQ_SUB_TICKER = 15
RESP_TICKER = 2
RESP_PREV_CLOSE = 6
RESP_DISCONNECT = 50

EXCH_SEG_MAP_NUM_TO_NAME = {
    0: "IDX_I",
    1: "NSE_EQ",
    2: "NSE_FNO",
    3: "NSE_CURRENCY",
    4: "BSE_EQ",
    5: "MCX_COMM",
    7: "BSE_CURRENCY",
    8: "BSE_FNO",
}

# Commodities only (indices removed intentionally)
COMMODITY_ROOTS = [
    {"root": "CRUDEOILM", "label": "CRUDEOILM", "display_prec": 2},
    {"root": "GOLDPETAL", "label": "GOLDPETAL", "display_prec": 2},
    {"root": "SILVERMIC", "label": "SILVERMIC", "display_prec": 2},
]

COMMODITY_FALLBACKS = {
    "CRUDEOILM": {"security_id": "0", "lot_size": 1},
    "GOLDPETAL": {"security_id": "0", "lot_size": 1},
    "SILVERMIC": {"security_id": "0", "lot_size": 1},
}


def normalize_dhan_epoch(ts: int) -> int:
    ts = int(ts)
    now_ts = int(time.time())
    diff = ts - now_ts
    if int(4.5 * 3600) <= diff <= int(6.5 * 3600):
        ts -= 19800
    return ts


def local_dt_from_epoch(ts: int) -> datetime:
    ts = normalize_dhan_epoch(int(ts))
    return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone()


def epoch_to_local_str(ts: Optional[int], with_seconds=True) -> str:
    if not ts:
        return "-"
    dt = local_dt_from_epoch(int(ts))
    return dt.strftime("%H:%M:%S" if with_seconds else "%H:%M")


def now_local_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def minute_bucket_epoch(epoch_sec: int) -> int:
    epoch_sec = normalize_dhan_epoch(int(epoch_sec))
    return epoch_sec - (epoch_sec % 60)


def strategy_bucket_start(
    ts_epoch: int,
    tf_min: int,
    anchor_minute_of_day: int = 9 * 60 + 15,
    session_anchor_epoch: Optional[int] = None,
) -> int:
    tf_min = int(tf_min)
    ts_epoch = normalize_dhan_epoch(int(ts_epoch))

    if tf_min <= 1:
        return minute_bucket_epoch(ts_epoch)

    # Preferred mode: dynamic broker-session anchor
    if session_anchor_epoch is not None:
        session_anchor_epoch = minute_bucket_epoch(int(session_anchor_epoch))
        if ts_epoch >= session_anchor_epoch:
            elapsed = ts_epoch - session_anchor_epoch
            bucket_index = elapsed // (tf_min * 60)
            return session_anchor_epoch + bucket_index * tf_min * 60

    # Fallback mode: fixed anchor (legacy)
    dt = local_dt_from_epoch(ts_epoch)
    mins = dt.hour * 60 + dt.minute
    day_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    if mins >= anchor_minute_of_day:
        offset = mins - anchor_minute_of_day
        bucket_index = offset // tf_min
        start_mins = anchor_minute_of_day + bucket_index * tf_min
    else:
        prev_day_start = day_start.timestamp() - 86400
        start_epoch = int(prev_day_start) + anchor_minute_of_day * 60
        elapsed = ts_epoch - start_epoch
        bucket_index = elapsed // (tf_min * 60)
        return start_epoch + bucket_index * tf_min * 60
    return int(day_start.timestamp()) + start_mins * 60


def compute_ha_series(ohlc_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    prev_ha_open = None
    prev_ha_close = None
    streak = 0
    prev_color = None

    for row in ohlc_rows:
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])
        bucket = int(row["bucket"])

        ha_close = (o + h + l + c) / 4.0
        ha_open = (o + c) / 2.0 if prev_ha_open is None else (prev_ha_open + prev_ha_close) / 2.0
        ha_high = max(h, ha_open, ha_close)
        ha_low = min(l, ha_open, ha_close)

        if ha_close > ha_open:
            color = "BULL"
        elif ha_close < ha_open:
            color = "BEAR"
        else:
            color = "DOJI"

        if color == prev_color:
            streak += 1
        else:
            streak = 1
            prev_color = color

        out.append({
            "bucket": bucket,
            "open": ha_open,
            "high": ha_high,
            "low": ha_low,
            "close": ha_close,
            "color": color,
            "streak": streak,
        })

        prev_ha_open = ha_open
        prev_ha_close = ha_close

    return out


class CandleEngine:
    def __init__(self, sec_id: str, name: str, seg: str, display_prec: int = 2):
        self.sec_id = str(sec_id)
        self.name = name
        self.seg = seg
        self.display_prec = int(display_prec)

        self.lock = threading.Lock()
        self.prev_close: Optional[float] = None
        self.last_ltp: Optional[float] = None
        self.last_ltt_epoch: Optional[int] = None
        self.last_tick_seen_epoch: Optional[int] = None
        self.current: Optional[Dict[str, Any]] = None
        self.completed = deque(maxlen=5000)

    def update_prev_close(self, prev_close: float):
        with self.lock:
            self.prev_close = float(prev_close)

    def seed_from_1m_candle(self, row: Dict[str, Any]):
        with self.lock:
            bucket = int(row["bucket"])
            if self.current is None:
                self.current = {
                    "bucket": bucket,
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "tick_count": int(row.get("tick_count", row.get("volume", 1) or 1)),
                }
                self.last_ltp = float(row["close"])
                self.last_ltt_epoch = bucket + 59
                return

            cur_bucket = int(self.current["bucket"])
            if bucket > cur_bucket:
                finalized = {
                    "bucket": int(self.current["bucket"]),
                    "open": float(self.current["open"]),
                    "high": float(self.current["high"]),
                    "low": float(self.current["low"]),
                    "close": float(self.current["close"]),
                    "tick_count": int(self.current["tick_count"]),
                }
                self.completed.append(finalized)
                self.current = {
                    "bucket": bucket,
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "tick_count": int(row.get("tick_count", row.get("volume", 1) or 1)),
                }
            elif bucket == cur_bucket:
                self.current = {
                    "bucket": bucket,
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "tick_count": int(row.get("tick_count", row.get("volume", 1) or 1)),
                }

            self.last_ltp = float(row["close"])
            self.last_ltt_epoch = bucket + 59

    def on_tick(self, ltp: float, ltt_epoch: int) -> None:
        """Update LTP only. Candle OHLC is handled by RestCandlePoller via REST API."""
        ltp = float(ltp)
        ltt_epoch = normalize_dhan_epoch(int(ltt_epoch))
        with self.lock:
            self.last_ltp = ltp
            self.last_ltt_epoch = ltt_epoch
            self.last_tick_seen_epoch = int(time.time())

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            chg = None
            chg_pct = None
            if self.last_ltp is not None and self.prev_close not in (None, 0):
                chg = self.last_ltp - self.prev_close
                chg_pct = (chg / self.prev_close) * 100.0
            return {
                "ltp": self.last_ltp,
                "ltt_epoch": self.last_ltt_epoch,
                "prev_close": self.prev_close,
                "chg": chg,
                "chg_pct": chg_pct,
                "current": dict(self.current) if self.current else None,
                "history": list(self.completed),
                "display_prec": self.display_prec,
            }


def load_instrument_master() -> Optional[pd.DataFrame]:
    try:
        r = requests.get(INSTRUMENT_CSV_URL, timeout=20)
        r.raise_for_status()
        from io import StringIO
        df = pd.read_csv(StringIO(r.text), low_memory=False)
        for c in ["EXCH_ID", "SEGMENT", "INSTRUMENT", "SYMBOL_NAME", "DISPLAY_NAME"]:
            if c in df.columns:
                df[c] = df[c].astype(str).str.strip()
        if "SM_EXPIRY_DATE" in df.columns:
            df["SM_EXPIRY_DATE"] = pd.to_datetime(df["SM_EXPIRY_DATE"], errors="coerce")
        if "LOT_SIZE" in df.columns:
            df["LOT_SIZE"] = pd.to_numeric(df["LOT_SIZE"], errors="coerce")
        return df
    except Exception:
        return None


def resolve_front_month_commodities(df: Optional[pd.DataFrame], logger=None) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if df is None:
        for item in COMMODITY_ROOTS:
            fb = COMMODITY_FALLBACKS[item["root"]]
            sec_id = str(fb["security_id"])
            if sec_id == "0":
                continue
            if logger:
                logger.warning("Fallback used for %s lot/security id", item["root"])
            rows.append({
                "name": item["label"],
                "exchange": "MCX_COMM",
                "security_id": sec_id,
                "display_prec": item["display_prec"],
                "lot_size": int(fb["lot_size"]),
                "instrument_type": "FUTCOM",
            })
        return rows

    today = pd.Timestamp.now().normalize()
    d = df[
        (df["EXCH_ID"].astype(str).str.upper() == "MCX")
        & (df["INSTRUMENT"].astype(str).str.upper() == "FUTCOM")
    ].copy()

    for item in COMMODITY_ROOTS:
        root = item["root"]
        sub = d[
            (d["SYMBOL_NAME"].astype(str).str.upper() == root)
            & d["SM_EXPIRY_DATE"].notna()
            & (d["SM_EXPIRY_DATE"] >= today)
        ].sort_values("SM_EXPIRY_DATE")

        if sub.empty:
            fb = COMMODITY_FALLBACKS[root]
            sec_id = str(fb["security_id"])
            if sec_id == "0":
                if logger:
                    logger.warning("Could not resolve %s front-month contract", root)
                continue
            if logger:
                logger.warning("Fallback used for %s front-month contract", root)
            rows.append({
                "name": item["label"],
                "exchange": "MCX_COMM",
                "security_id": sec_id,
                "display_prec": item["display_prec"],
                "lot_size": int(fb["lot_size"]),
                "instrument_type": "FUTCOM",
            })
            continue

        row = sub.iloc[0]
        lot = row.get("LOT_SIZE", 1)
        lot = int(lot) if pd.notna(lot) and int(lot) > 0 else 1
        rows.append({
            "name": item["label"],
            "exchange": "MCX_COMM",
            "security_id": str(int(float(row["SECURITY_ID"]))),
            "display_prec": item["display_prec"],
            "lot_size": lot,
            "instrument_type": "FUTCOM",
            "contract_display": str(row.get("DISPLAY_NAME", "")),
        })
        if logger:
            logger.info(
                "Resolved %s -> secId=%s lot=%s contract=%s",
                item["label"],
                rows[-1]["security_id"],
                lot,
                rows[-1].get("contract_display", ""),
            )

    return rows


def build_instrument_list(symbol_filter: Optional[List[str]] = None, logger=None) -> List[Dict[str, Any]]:
    df = load_instrument_master()
    instruments = resolve_front_month_commodities(df, logger=logger)
    if symbol_filter:
        wanted = {s.strip().upper() for s in symbol_filter if s.strip()}
        instruments = [x for x in instruments if x["name"].upper() in wanted]
    return instruments


def _instrument_for_segment(exchange_segment: str) -> str:
    """Map exchange segment to Dhan instrument type string for history API."""
    seg = str(exchange_segment).strip().upper()
    if seg == "IDX_I":
        return "INDEX"
    if seg in ("NSE_EQ", "BSE_EQ"):
        return "EQUITY"
    if seg in ("NSE_FNO", "BSE_FNO"):
        return "OPTIDX"
    if seg in ("NSE_CURRENCY", "BSE_CURRENCY"):
        return "CURRENCY"
    if seg == "MCX_COMM":
        return "FUTCOM"
    return "EQUITY"


def fetch_intraday_1m_history(access_token: str, inst: Dict[str, Any], days: int = 5,
                               logger=None, client_id: str = "") -> List[Dict[str, Any]]:
    days = max(1, min(int(days), 30))
    end_dt   = datetime.now().replace(second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=days)

    exchange_segment = str(inst["exchange"])
    instrument_type  = _instrument_for_segment(exchange_segment)

    payload = {
        "securityId":      str(inst["security_id"]),
        "exchangeSegment": exchange_segment,
        "instrument":      instrument_type,
        "interval":        "1",
        "oi":              False,
        "fromDate":        start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "toDate":          end_dt.strftime("%Y-%m-%d %H:%M:%S"),
    }
    headers = {
        "Accept":       "application/json",
        "Content-Type": "application/json",
        "access-token": access_token,
    }
    if client_id:
        headers["client-id"] = str(client_id)

    try:
        r = requests.post(HIST_INTRADAY_URL, headers=headers, json=payload, timeout=30)
        if not r.ok:
            if logger:
                logger.warning("Backfill failed for %s: HTTP %s %s",
                               inst["name"], r.status_code, r.text[:200])
            return []
        data = r.json()
        ts  = data.get("timestamp", []) or []
        op  = data.get("open",      []) or []
        hi  = data.get("high",      []) or []
        lo  = data.get("low",       []) or []
        cl  = data.get("close",     []) or []
        vol = data.get("volume",    []) or []
        n   = min(len(ts), len(op), len(hi), len(lo), len(cl))
        rows = []
        for i in range(n):
            # Use raw REST timestamp directly — do NOT call normalize_dhan_epoch()
            # or minute_bucket_epoch() here. REST timestamps are already correct
            # UTC epochs. normalize_dhan_epoch() is only for WebSocket ticker packets
            # which sometimes send IST epoch instead of UTC.
            bucket = int(ts[i])
            rows.append({
                "bucket":     bucket,
                "open":       float(op[i]),
                "high":       float(hi[i]),
                "low":        float(lo[i]),
                "close":      float(cl[i]),
                "tick_count": int(vol[i]) if i < len(vol) and vol[i] is not None else 1,
            })
        rows.sort(key=lambda x: x["bucket"])

        # Drop the current running (incomplete) 1m candle — only keep closed bars.
        # current_minute_bucket is in UTC just like REST timestamps.
        if rows:
            current_minute_bucket = (int(time.time()) // 60) * 60
            if int(rows[-1]["bucket"]) >= current_minute_bucket:
                rows = rows[:-1]

        if logger:
            logger.info("Backfill loaded for %s: %d x 1m closed candles "
                        "(seg=%s instr=%s)",
                        inst["name"], len(rows), exchange_segment, instrument_type)
        return rows
    except Exception as e:
        if logger:
            logger.warning("Backfill exception for %s: %s", inst["name"], e)
        return []


def parse_header_8(msg: bytes) -> Optional[Dict[str, Any]]:
    if len(msg) < 8:
        return None
    return {
        "resp_code": msg[0],
        "msg_len": struct.unpack_from("<H", msg, 1)[0],
        "exch_seg_num": msg[3],
        "exch_seg_name": EXCH_SEG_MAP_NUM_TO_NAME.get(msg[3], str(msg[3])),
        "security_id": str(struct.unpack_from("<I", msg, 4)[0]),
        "payload": msg[8:],
    }


def parse_ticker(payload: bytes) -> Optional[Dict[str, Any]]:
    if len(payload) < 8:
        return None
    return {"ltp": float(struct.unpack_from("<f", payload, 0)[0]), "ltt_epoch": int(struct.unpack_from("<I", payload, 4)[0])}


def parse_prev_close(payload: bytes) -> Optional[Dict[str, Any]]:
    if len(payload) < 8:
        return None
    return {"prev_close": float(struct.unpack_from("<f", payload, 0)[0]), "prev_oi": int(struct.unpack_from("<I", payload, 4)[0])}


class RestCandlePoller:
    """
    Polls Dhan REST API once per minute, timed to fire ~3 seconds after
    each minute boundary (HH:MM:03). Guarantees the just-closed candle is
    available on every poll. Fires on_new_1m_candle_cb for each new bucket.
    WebSocket handles LTP only.
    """
    POLL_DELAY_AFTER_MINUTE = 3
    LOOKBACK_DAYS_LIVE      = 1

    def __init__(self, client_id, access_token, instruments,
                 candle_engines, on_new_1m_candle_cb, logger=None):
        self.client_id    = client_id
        self.access_token = access_token
        self.instruments  = instruments
        self.engines      = candle_engines
        self.on_new_1m_candle_cb = on_new_1m_candle_cb
        self.logger       = logger
        self.stop_event   = threading.Event()
        self._thread      = None
        self._last_seen_bucket: Dict[str, int] = {}

    def _seconds_until_next_poll(self) -> float:
        elapsed   = time.time() % 60
        remaining = 60 - elapsed
        return remaining + self.POLL_DELAY_AFTER_MINUTE

    def _poll_once(self):
        for inst in self.instruments:
            if self.stop_event.is_set():
                return
            sec = str(inst["security_id"])
            try:
                rows = fetch_intraday_1m_history(
                    access_token=self.access_token,
                    inst=inst,
                    days=self.LOOKBACK_DAYS_LIVE,
                    logger=self.logger,
                    client_id=self.client_id,
                )
                if not rows:
                    continue
                prev_bucket  = self._last_seen_bucket.get(sec, 0)
                new_candles  = [r for r in rows if int(r["bucket"]) > prev_bucket]
                if new_candles:
                    engine = self.engines.get(sec)
                    if engine:
                        for r in new_candles:
                            engine.seed_from_1m_candle(r)
                    self._last_seen_bucket[sec] = int(rows[-1]["bucket"])
                    for candle in new_candles:
                        if self.logger:
                            self.logger.info(
                                "REST 1m: %s bucket=%s O=%.4f H=%.4f L=%.4f C=%.4f",
                                inst["name"], candle["bucket"],
                                candle["open"], candle["high"],
                                candle["low"], candle["close"])
                        self.on_new_1m_candle_cb(sec, candle)
            except Exception as e:
                if self.logger:
                    self.logger.warning("RestCandlePoller error %s: %s", inst["name"], e)

    def _run(self):
        while not self.stop_event.is_set():
            sleep_secs = self._seconds_until_next_poll()
            deadline   = time.time() + sleep_secs
            while time.time() < deadline:
                if self.stop_event.is_set():
                    return
                time.sleep(0.5)
            if self.stop_event.is_set():
                return
            try:
                self._poll_once()
            except Exception as e:
                if self.logger:
                    self.logger.warning("RestCandlePoller loop error: %s", e)

    def seed_last_buckets(self, rows_by_sec: Dict[str, List[Dict[str, Any]]]):
        for sec, rows in rows_by_sec.items():
            if rows:
                self._last_seen_bucket[sec] = int(rows[-1]["bucket"])

    def start(self):
        self.stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self.stop_event.set()


class MarketDataEngine:
    """
    Hybrid engine:
      WebSocket → LTP only (real-time trigger hits)
      REST poll → accurate 1m OHLC every minute (candle strategy)
    """
    def __init__(self, client_id: str, access_token: str,
                 instruments: List[Dict[str, Any]],
                 on_new_1m_candle, on_ltp, logger=None):
        self.client_id    = client_id
        self.access_token = access_token
        self.ws_url = (
            f"wss://api-feed.dhan.co?version=2"
            f"&token={self.access_token}&clientId={self.client_id}&authType=2"
        )
        self.instruments         = instruments
        self.on_new_1m_candle_cb = on_new_1m_candle
        self.on_ltp_cb           = on_ltp
        self.logger              = logger

        self.stop_event = threading.Event()
        self.ws         = None
        self.thread     = None

        self.engines: Dict[str, CandleEngine] = {}
        for inst in instruments:
            sec = str(inst["security_id"])
            self.engines[sec] = CandleEngine(
                sec, inst["name"], inst["exchange"], inst.get("display_prec", 2))

        self.rest_poller = RestCandlePoller(
            client_id=client_id,
            access_token=access_token,
            instruments=instruments,
            candle_engines=self.engines,
            on_new_1m_candle_cb=on_new_1m_candle,
            logger=logger,
        )

        self.last_ws_connect_time = None
        self.last_ws_error        = None
        self.packet_counts = {
            RESP_TICKER: 0, RESP_PREV_CLOSE: 0, RESP_DISCONNECT: 0, "other": 0}
        self._last_ticker_key: Dict[str, tuple] = {}
        self.lock = threading.Lock()

    def seed_last_buckets(self, rows_by_sec: Dict[str, List[Dict[str, Any]]]):
        self.rest_poller.seed_last_buckets(rows_by_sec)

    def on_open(self, ws):
        with self.lock:
            self.last_ws_connect_time = time.time()
            self.last_ws_error = None
        ws.send(json.dumps({
            "RequestCode":     REQ_SUB_TICKER,
            "InstrumentCount": len(self.instruments),
            "InstrumentList":  [
                {"ExchangeSegment": i["exchange"], "SecurityId": str(i["security_id"])}
                for i in self.instruments
            ],
        }))

    def on_message(self, ws, message):
        if isinstance(message, str):
            return
        hdr = parse_header_8(bytes(message))
        if not hdr:
            return
        sec    = hdr["security_id"]
        engine = self.engines.get(sec)
        if engine is None:
            return
        code = int(hdr["resp_code"])

        if code == RESP_TICKER:
            t = parse_ticker(hdr["payload"])
            if not t:
                return
            ltp       = float(t["ltp"])
            ltt_epoch = int(t["ltt_epoch"])
            dedup     = (round(ltp, 8), ltt_epoch)
            if self._last_ticker_key.get(sec) == dedup:
                return
            self._last_ticker_key[sec] = dedup
            engine.on_tick(ltp, ltt_epoch)   # LTP only — no candle building
            self.on_ltp_cb(sec, ltp, ltt_epoch)
            with self.lock:
                self.packet_counts[RESP_TICKER] += 1
            return

        if code == RESP_PREV_CLOSE:
            p = parse_prev_close(hdr["payload"])
            if p:
                engine.update_prev_close(p["prev_close"])
                with self.lock:
                    self.packet_counts[RESP_PREV_CLOSE] += 1
            return

        if code == RESP_DISCONNECT:
            with self.lock:
                self.packet_counts[RESP_DISCONNECT] += 1
            return

        with self.lock:
            self.packet_counts["other"] += 1

    def on_error(self, ws, error):
        with self.lock:
            self.last_ws_error = str(error)

    def on_close(self, ws, status_code, msg):
        pass

    def _run(self):
        websocket.enableTrace(False)
        while not self.stop_event.is_set():
            try:
                self.ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_open=self.on_open, on_message=self.on_message,
                    on_error=self.on_error, on_close=self.on_close,
                )
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                with self.lock:
                    self.last_ws_error = f"WS exception: {e}"
            finally:
                if not self.stop_event.is_set():
                    time.sleep(2)

    def start(self):
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        self.rest_poller.start()

    def stop(self):
        self.stop_event.set()
        self.rest_poller.stop()
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass

    def market_snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "last_ws_connect_time": self.last_ws_connect_time,
                "last_ws_error":        self.last_ws_error,
                "packet_counts":        dict(self.packet_counts),
            }
