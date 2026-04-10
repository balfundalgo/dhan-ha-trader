
from typing import Optional, Dict, Any, Callable

class PaperTradeEngine:
    def __init__(self, lot_size: int, display_prec: int = 2, event_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None, symbol_name: str = ""):
        self.lot_size = int(lot_size)
        self.display_prec = int(display_prec)
        self.symbol_name = symbol_name
        self.event_callback = event_callback

        self.position_side: Optional[str] = None
        self.entry_price: Optional[float] = None
        self.entry_ts: Optional[int] = None

        self.realized_pnl: float = 0.0
        self.trade_count: int = 0
        self.last_event: str = "-"
        self.last_closed_pnl: float = 0.0

    def _emit(self, event_type: str, extra: Dict[str, Any]):
        if self.event_callback:
            payload = {
                "symbol": self.symbol_name,
                "position_side": self.position_side,
                "entry_price": self.entry_price,
                "entry_ts": self.entry_ts,
                "realized_pnl": self.realized_pnl,
                "trade_count": self.trade_count,
                "lot_size": self.lot_size,
            }
            payload.update(extra)
            self.event_callback(event_type, payload)

    def execute_signal(self, signal: Dict[str, Any], ts_epoch: int):
        side = signal["side"]
        price = float(signal["price"])
        if side == "BUY":
            self._execute_buy(price, ts_epoch)
        elif side == "SELL":
            self._execute_sell(price, ts_epoch)

    def square_off(self, price: float, ts_epoch: int):
        if self.position_side is None or self.entry_price is None:
            return
        price = float(price)
        old_entry = float(self.entry_price)

        if self.position_side == "LONG":
            pnl = price - old_entry
            event_type = "SQUAREOFF_LONG"
        else:
            pnl = old_entry - price
            event_type = "SQUAREOFF_SHORT"

        pnl *= self.lot_size
        self.realized_pnl += pnl
        self.last_closed_pnl = pnl
        self.trade_count += 1
        self.last_event = f"Square off {self.position_side} @ {price:.{self.display_prec}f} | close pnl {pnl:.2f}"

        self._emit(event_type, {
            "exit_price": price,
            "closed_entry_price": old_entry,
            "closed_pnl": pnl,
            "ts": int(ts_epoch),
        })

        self.position_side = None
        self.entry_price = None
        self.entry_ts = None

    def _execute_buy(self, price: float, ts_epoch: int):
        if self.position_side == "LONG":
            return
        if self.position_side == "SHORT" and self.entry_price is not None:
            exit_price = price
            old_entry = float(self.entry_price)
            pnl = (old_entry - exit_price) * self.lot_size
            self.realized_pnl += pnl
            self.last_closed_pnl = pnl
            self.trade_count += 1
            self.last_event = f"Reverse SHORT->LONG @ {price:.{self.display_prec}f} | close pnl {pnl:.2f}"
            self._emit("CLOSE_SHORT", {"exit_price": exit_price, "closed_entry_price": old_entry, "closed_pnl": pnl, "ts": int(ts_epoch)})
        else:
            self.last_event = f"Open LONG @ {price:.{self.display_prec}f}"
        self.position_side = "LONG"
        self.entry_price = price
        self.entry_ts = int(ts_epoch)
        self._emit("OPEN_LONG", {"entry_price": price, "ts": int(ts_epoch)})

    def _execute_sell(self, price: float, ts_epoch: int):
        if self.position_side == "SHORT":
            return
        if self.position_side == "LONG" and self.entry_price is not None:
            exit_price = price
            old_entry = float(self.entry_price)
            pnl = (exit_price - old_entry) * self.lot_size
            self.realized_pnl += pnl
            self.last_closed_pnl = pnl
            self.trade_count += 1
            self.last_event = f"Reverse LONG->SHORT @ {price:.{self.display_prec}f} | close pnl {pnl:.2f}"
            self._emit("CLOSE_LONG", {"exit_price": exit_price, "closed_entry_price": old_entry, "closed_pnl": pnl, "ts": int(ts_epoch)})
        else:
            self.last_event = f"Open SHORT @ {price:.{self.display_prec}f}"
        self.position_side = "SHORT"
        self.entry_price = price
        self.entry_ts = int(ts_epoch)
        self._emit("OPEN_SHORT", {"entry_price": price, "ts": int(ts_epoch)})

    def unrealized_pnl(self, current_ltp: Optional[float]) -> float:
        if self.position_side == "LONG" and self.entry_price is not None and current_ltp is not None:
            return (float(current_ltp) - float(self.entry_price)) * self.lot_size
        if self.position_side == "SHORT" and self.entry_price is not None and current_ltp is not None:
            return (float(self.entry_price) - float(current_ltp)) * self.lot_size
        return 0.0

    def restore_state(self, data: Dict[str, Any]):
        self.position_side = data.get("position_side")
        self.entry_price = data.get("entry_price")
        self.entry_ts = data.get("entry_ts")
        self.realized_pnl = float(data.get("realized_pnl", 0.0))
        self.trade_count = int(data.get("trade_count", 0))
        self.last_event = data.get("last_event", self.last_event)
        self.last_closed_pnl = float(data.get("last_closed_pnl", 0.0))

    def persist_state(self) -> Dict[str, Any]:
        return {
            "position_side": self.position_side,
            "entry_price": self.entry_price,
            "entry_ts": self.entry_ts,
            "realized_pnl": self.realized_pnl,
            "trade_count": self.trade_count,
            "last_event": self.last_event,
            "last_closed_pnl": self.last_closed_pnl,
        }

    def snapshot(self, current_ltp: Optional[float]) -> Dict[str, Any]:
        return {
            "position_side": self.position_side,
            "entry_price": self.entry_price,
            "entry_ts": self.entry_ts,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl(current_ltp),
            "trade_count": self.trade_count,
            "last_event": self.last_event,
            "last_closed_pnl": self.last_closed_pnl,
            "lot_size": self.lot_size,
        }
