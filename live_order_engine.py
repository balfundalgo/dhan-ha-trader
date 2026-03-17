"""
=============================================================================
Dhan Live Order Engine
=============================================================================
Handles real order placement via Dhan REST API v2.
- Supports MARKET, STOP_LOSS_MARKET, LIMIT order types
- 10-second fill timeout: if order not filled, cancels and places MARKET order
- Runs fill-monitoring in background thread (non-blocking)
=============================================================================
"""

import time
import threading
import logging
import requests
from typing import Optional, Callable, Dict, Any

DHAN_BASE_URL = "https://api.dhan.co/v2"


class LiveOrderEngine:
    def __init__(self, client_id: str, access_token: str, logger=None):
        self.client_id    = str(client_id).strip()
        self.access_token = str(access_token).strip()
        self.logger       = logger or logging.getLogger("LiveOrderEngine")
        self._headers     = {
            "Content-Type": "application/json",
            "access-token": self.access_token,
        }

    def _corr_id(self) -> str:
        """Generate unique correlation ID under 20 chars."""
        return f"b{int(time.time() * 1000) % (10 ** 13)}"[:20]

    # ── Core REST calls ───────────────────────────────────────────────────────

    def place_order(
        self,
        transaction_type: str,
        security_id: str,
        exchange_segment: str,
        quantity: int,
        order_type: str,
        price: float = 0.0,
        trigger_price: float = 0.0,
        product_type: str = "INTRADAY",
        correlation_id: str = None,
    ) -> Dict[str, Any]:
        """
        Place a single order. Returns Dhan response dict with orderId.
        order_type: MARKET | STOP_LOSS_MARKET | LIMIT
        """
        body = {
            "dhanClientId":       self.client_id,
            "correlationId":      correlation_id or self._corr_id(),
            "transactionType":    transaction_type.upper(),
            "exchangeSegment":    exchange_segment,
            "productType":        product_type,
            "orderType":          order_type.upper(),
            "validity":           "DAY",
            "securityId":         str(security_id),
            "quantity":           int(quantity),
            "disclosedQuantity":  0,
            "price":              float(price),
            "triggerPrice":       float(trigger_price),
            "afterMarketOrder":   False,
            "amoTime":            "",
        }
        self.logger.info(
            "Placing %s %s | qty=%d | type=%s | price=%.4f | trigger=%.4f",
            transaction_type, security_id, quantity, order_type, price, trigger_price
        )
        r = requests.post(f"{DHAN_BASE_URL}/orders", json=body,
                          headers=self._headers, timeout=10)
        r.raise_for_status()
        data = r.json()
        self.logger.info("Order placed → orderId=%s status=%s",
                         data.get("orderId"), data.get("orderStatus"))
        return data

    def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """Poll order status. Returns full order dict."""
        try:
            r = requests.get(f"{DHAN_BASE_URL}/orders/{order_id}",
                             headers=self._headers, timeout=10)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            self.logger.warning("Status poll failed for %s: %s", order_id, e)
            return {}

    def cancel_order(self, order_id: str) -> bool:
        """Cancel a pending order. Returns True if successful."""
        try:
            r = requests.delete(f"{DHAN_BASE_URL}/orders/{order_id}",
                                headers=self._headers, timeout=10)
            ok = r.status_code in (200, 202)
            self.logger.info("Cancel order %s → HTTP %s", order_id, r.status_code)
            return ok
        except Exception as e:
            self.logger.warning("Cancel failed for %s: %s", order_id, e)
            return False

    # ── Main execute method ───────────────────────────────────────────────────

    def execute_with_fallback(
        self,
        transaction_type: str,
        security_id: str,
        exchange_segment: str,
        quantity: int,
        order_type: str,
        price: float = 0.0,
        trigger_price: float = 0.0,
        fallback_timeout: int = 10,
        on_fill: Optional[Callable[[float, str], None]] = None,
        on_fallback: Optional[Callable[[float, str], None]] = None,
        on_error: Optional[Callable[[str], None]] = None,
    ):
        """
        Place order in a background thread.
        If not filled within fallback_timeout seconds:
          → Cancel original order
          → Place a MARKET order immediately

        Callbacks:
          on_fill(fill_price, order_id)       — original order filled
          on_fallback(fill_price, order_id)   — market fallback order filled
          on_error(error_message)             — something failed
        """
        def _run():
            corr = self._corr_id()
            try:
                # ── Step 1: Place the original order ─────────────────────────
                resp = self.place_order(
                    transaction_type, security_id, exchange_segment, quantity,
                    order_type, price=price, trigger_price=trigger_price,
                    correlation_id=corr,
                )
                order_id = resp.get("orderId", "")
                if not order_id:
                    raise RuntimeError(f"No orderId in response: {resp}")

                # ── Step 2: Poll for fill ─────────────────────────────────────
                # Market orders fill near-instantly; poll every 1s
                # Limit/SL-M: poll every 2s for up to fallback_timeout seconds
                is_market = order_type.upper() == "MARKET"
                poll_interval = 1 if is_market else 2
                deadline      = time.time() + (3 if is_market else fallback_timeout)
                filled        = False
                fill_price    = price if price > 0 else trigger_price

                while time.time() < deadline:
                    time.sleep(poll_interval)
                    status     = self.get_order_status(order_id)
                    os_val     = status.get("orderStatus", "")
                    filled_qty = int(status.get("filledQty") or 0)
                    avg_price  = float(status.get("averageTradedPrice") or 0)

                    if os_val == "TRADED" or filled_qty >= quantity:
                        fill_price = avg_price if avg_price > 0 else fill_price
                        filled = True
                        self.logger.info("Order %s FILLED @ %.4f", order_id, fill_price)
                        break

                    if os_val in ("REJECTED", "CANCELLED", "EXPIRED"):
                        self.logger.warning(
                            "Order %s %s — triggering market fallback", order_id, os_val)
                        break

                if filled:
                    if on_fill:
                        on_fill(fill_price, order_id)
                    return

                # ── Step 3: Not filled — cancel and market fallback ───────────
                if not is_market:
                    self.logger.info(
                        "Order %s NOT filled in %ds — cancelling → MARKET fallback",
                        order_id, fallback_timeout
                    )
                    self.cancel_order(order_id)
                    time.sleep(0.3)

                # Place MARKET fallback
                mkt_corr = (corr + "m")[:20]
                mkt_resp = self.place_order(
                    transaction_type, security_id, exchange_segment, quantity,
                    "MARKET", price=0.0, trigger_price=0.0,
                    correlation_id=mkt_corr,
                )
                mkt_id = mkt_resp.get("orderId", "")

                # Poll market order (fills fast)
                time.sleep(1)
                mkt_status = self.get_order_status(mkt_id)
                mkt_price  = float(mkt_status.get("averageTradedPrice") or fill_price)
                self.logger.info(
                    "MARKET fallback order %s filled @ %.4f", mkt_id, mkt_price)

                if on_fallback:
                    on_fallback(mkt_price, mkt_id)
                elif on_fill:
                    on_fill(mkt_price, mkt_id)

            except Exception as e:
                self.logger.error("execute_with_fallback error: %s", e)
                if on_error:
                    on_error(str(e))

        threading.Thread(target=_run, daemon=True).start()
