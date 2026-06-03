"""
Synthetic Futures Engine
BUY  signal → BUY  ATM CE  +  SELL ATM PE  (synthetic long)
SELL signal → SELL ATM CE  +  BUY  ATM PE  (synthetic short)
Reversal: exit old legs sequentially, then enter new legs at fresh ATM.
"""
from typing import Optional, Callable, Dict, Any


class SyntheticLeg:
    def __init__(self, option_type: str):
        self.option_type = option_type   # "CE" or "PE"
        self.side        = None          # "BUY" or "SELL"
        self.entry_price = None
        self.entry_ts    = None
        self.current_ltp = None
        self.security_id = None
        self.strike      = None
        self.expiry_str  = None
        self.lot_size    = 1

    @property
    def unrealized_pnl(self) -> float:
        if self.entry_price is None or self.current_ltp is None:
            return 0.0
        if self.side == "BUY":
            return (float(self.current_ltp) - float(self.entry_price)) * self.lot_size
        elif self.side == "SELL":
            return (float(self.entry_price) - float(self.current_ltp)) * self.lot_size
        return 0.0

    def open(self, side, price, ts, security_id, strike, expiry_str, lot_size):
        self.side        = side
        self.entry_price = float(price)
        self.entry_ts    = int(ts)
        self.security_id = str(security_id)
        self.strike      = int(strike)
        self.expiry_str  = expiry_str
        self.lot_size    = int(lot_size)
        self.current_ltp = float(price)

    def close(self):
        pnl = self.unrealized_pnl
        self.side = None; self.entry_price = None; self.entry_ts = None
        self.security_id = None; self.strike = None; self.expiry_str = None
        self.current_ltp = None
        return pnl

    def is_open(self) -> bool:
        return self.side is not None

    def snapshot(self) -> dict:
        return {"option_type": self.option_type, "side": self.side,
                "entry_price": self.entry_price, "entry_ts": self.entry_ts,
                "current_ltp": self.current_ltp, "security_id": self.security_id,
                "strike": self.strike, "expiry_str": self.expiry_str,
                "lot_size": self.lot_size, "unrealized_pnl": self.unrealized_pnl}

    def persist(self) -> dict:
        return self.snapshot()

    def restore(self, data: dict):
        self.side = data.get("side"); self.entry_price = data.get("entry_price")
        self.entry_ts = data.get("entry_ts"); self.current_ltp = data.get("current_ltp")
        self.security_id = data.get("security_id"); self.strike = data.get("strike")
        self.expiry_str = data.get("expiry_str"); self.lot_size = data.get("lot_size", 1)


class SyntheticPaperEngine:
    def __init__(self, symbol_name: str, lot_size: int = 1,
                 display_prec: int = 2, event_callback: Optional[Callable] = None):
        self.symbol_name    = symbol_name
        self.lot_size       = int(lot_size)
        self.display_prec   = int(display_prec)
        self.event_callback = event_callback
        self.ce_leg         = SyntheticLeg("CE")
        self.pe_leg         = SyntheticLeg("PE")
        self.position_side  = None
        self.realized_pnl   = 0.0
        self.trade_count    = 0
        self.last_event     = "-"

    @property
    def unrealized_pnl(self) -> float:
        return self.ce_leg.unrealized_pnl + self.pe_leg.unrealized_pnl

    def open_position(self, synthetic_side, strike, expiry_str,
                      ce_security_id, pe_security_id,
                      ce_price, pe_price, ts, lot_size=None):
        lot = lot_size or self.lot_size
        synthetic_side = synthetic_side.upper()
        ce_side = "BUY"  if synthetic_side == "LONG" else "SELL"
        pe_side = "SELL" if synthetic_side == "LONG" else "BUY"
        self.ce_leg.open(ce_side, ce_price, ts, ce_security_id, strike, expiry_str, lot)
        self.pe_leg.open(pe_side, pe_price, ts, pe_security_id, strike, expiry_str, lot)
        self.position_side = synthetic_side
        self.trade_count  += 1
        self.last_event    = (f"Open {synthetic_side} strike={strike} "
                              f"CE={ce_price:.2f} PE={pe_price:.2f}")
        self._emit("OPEN_SYNTHETIC", {
            "synthetic_side": synthetic_side, "strike": strike,
            "expiry_str": expiry_str,
            "ce_security_id": ce_security_id, "pe_security_id": pe_security_id,
            "ce_entry_price": ce_price, "pe_entry_price": pe_price,
            "ce_side": ce_side, "pe_side": pe_side,
            "lot_size": lot, "ts": ts,
            "entry_ts": ts, "entry_price": f"CE={ce_price} PE={pe_price}",
        })

    def close_position(self, ce_exit_price, pe_exit_price, ts):
        if not self.is_open():
            return 0.0
        self.ce_leg.current_ltp = ce_exit_price
        self.pe_leg.current_ltp = pe_exit_price
        closed_pnl       = self.unrealized_pnl
        self.realized_pnl += closed_pnl
        old_side         = self.position_side
        old_strike       = self.ce_leg.strike
        old_expiry       = self.ce_leg.expiry_str
        ce_entry         = self.ce_leg.entry_price
        pe_entry         = self.pe_leg.entry_price
        ce_sec           = self.ce_leg.security_id
        pe_sec           = self.pe_leg.security_id
        ce_entry_ts      = self.ce_leg.entry_ts
        lot              = self.ce_leg.lot_size
        self.ce_leg.close(); self.pe_leg.close()
        self.position_side = None
        self.last_event    = (f"Close {old_side} strike={old_strike} "
                              f"CE={ce_exit_price:.2f} PE={pe_exit_price:.2f} "
                              f"pnl={closed_pnl:+.2f}")
        self._emit("CLOSE_SYNTHETIC", {
            "synthetic_side": old_side, "strike": old_strike,
            "expiry_str": old_expiry,
            "ce_security_id": ce_sec, "pe_security_id": pe_sec,
            "ce_exit_price": ce_exit_price, "pe_exit_price": pe_exit_price,
            "ce_entry_price": ce_entry, "pe_entry_price": pe_entry,
            "closed_pnl": closed_pnl, "realized_pnl": self.realized_pnl,
            "lot_size": lot, "ts": ts,
            "entry_ts": ce_entry_ts, "exit_price": f"CE={ce_exit_price} PE={pe_exit_price}",
        })
        return closed_pnl

    def is_open(self) -> bool:
        return self.position_side is not None

    def on_ce_ltp(self, ltp: float):
        self.ce_leg.current_ltp = float(ltp)

    def on_pe_ltp(self, ltp: float):
        self.pe_leg.current_ltp = float(ltp)

    def _emit(self, event_type: str, extra: dict):
        if self.event_callback:
            payload = {"symbol": self.symbol_name, "realized_pnl": self.realized_pnl,
                       "trade_count": self.trade_count, "lot_size": self.lot_size}
            payload.update(extra)
            try:
                self.event_callback(event_type, payload)
            except Exception:
                pass

    def snapshot(self, futures_ltp=None) -> dict:
        return {
            "symbol": self.symbol_name, "position_side": self.position_side,
            "unrealized_pnl": self.unrealized_pnl, "realized_pnl": self.realized_pnl,
            "trade_count": self.trade_count, "lot_size": self.lot_size,
            "last_event": self.last_event, "futures_ltp": futures_ltp,
            "ce_leg": self.ce_leg.snapshot(), "pe_leg": self.pe_leg.snapshot(),
        }

    def persist_state(self) -> dict:
        return {"position_side": self.position_side, "realized_pnl": self.realized_pnl,
                "trade_count": self.trade_count, "lot_size": self.lot_size,
                "last_event": self.last_event,
                "ce_leg": self.ce_leg.persist(), "pe_leg": self.pe_leg.persist()}

    def restore_state(self, data: dict):
        self.position_side = data.get("position_side")
        self.realized_pnl  = float(data.get("realized_pnl", 0.0))
        self.trade_count   = int(data.get("trade_count", 0))
        self.lot_size      = int(data.get("lot_size", self.lot_size))
        self.last_event    = data.get("last_event", "-")
        if data.get("ce_leg"): self.ce_leg.restore(data["ce_leg"])
        if data.get("pe_leg"): self.pe_leg.restore(data["pe_leg"])
