import time, threading, logging, requests
from typing import Optional, Callable, Dict, Any
DHAN_BASE_URL = "https://api.dhan.co/v2"
class LiveOrderEngine:
    def __init__(self, client_id, access_token, logger=None):
        self.client_id = str(client_id).strip()
        self.access_token = str(access_token).strip()
        self.logger = logger or logging.getLogger("LiveOrderEngine")
        self._headers = {"Content-Type":"application/json","access-token":self.access_token,"client-id":self.client_id}
    def _corr_id(self): return f"b{int(time.time()*1000)%(10**13)}"[:20]
    def place_order(self,transaction_type,security_id,exchange_segment,quantity,order_type,price=0.0,trigger_price=0.0,product_type="INTRADAY",correlation_id=None):
        body={"dhanClientId":self.client_id,"correlationId":correlation_id or self._corr_id(),"transactionType":transaction_type.upper(),"exchangeSegment":exchange_segment,"productType":product_type,"orderType":order_type.upper(),"validity":"DAY","securityId":str(security_id),"quantity":int(quantity),"disclosedQuantity":0,"price":float(price),"triggerPrice":float(trigger_price),"afterMarketOrder":False,"amoTime":""}
        r=requests.post(f"{DHAN_BASE_URL}/orders",json=body,headers=self._headers,timeout=10)
        r.raise_for_status(); data=r.json()
        self.logger.info("Order placed → orderId=%s status=%s",data.get("orderId"),data.get("orderStatus"))
        return data
    def get_order_status(self,order_id):
        try:
            r=requests.get(f"{DHAN_BASE_URL}/orders/{order_id}",headers=self._headers,timeout=10)
            r.raise_for_status(); return r.json()
        except Exception as e:
            self.logger.warning("Status poll failed %s: %s",order_id,e); return {}
    def cancel_order(self,order_id):
        try:
            r=requests.delete(f"{DHAN_BASE_URL}/orders/{order_id}",headers=self._headers,timeout=10)
            return r.status_code in (200,202)
        except Exception as e:
            self.logger.warning("Cancel failed %s: %s",order_id,e); return False
    def execute_with_fallback(self,transaction_type,security_id,exchange_segment,quantity,order_type,price=0.0,trigger_price=0.0,fallback_timeout=10,on_fill=None,on_fallback=None,on_error=None):
        def _run():
            try:
                resp=self.place_order(transaction_type,security_id,exchange_segment,quantity,order_type,price=price,trigger_price=trigger_price)
                order_id=resp.get("orderId","")
                if not order_id: raise RuntimeError(f"No orderId: {resp}")
                is_market=order_type.upper()=="MARKET"
                deadline=time.time()+(3 if is_market else fallback_timeout)
                fill_price=price if price>0 else trigger_price; filled=False
                while time.time()<deadline:
                    time.sleep(1 if is_market else 2)
                    s=self.get_order_status(order_id)
                    if s.get("orderStatus")=="TRADED" or int(s.get("filledQty") or 0)>=quantity:
                        fill_price=float(s.get("averageTradedPrice") or fill_price); filled=True; break
                    if s.get("orderStatus") in ("REJECTED","CANCELLED","EXPIRED"): break
                if filled:
                    if on_fill: on_fill(fill_price,order_id)
                    return
                if not is_market: self.cancel_order(order_id); time.sleep(0.3)
                mr=self.place_order(transaction_type,security_id,exchange_segment,quantity,"MARKET")
                mid=mr.get("orderId",""); time.sleep(1)
                ms=self.get_order_status(mid); mp=float(ms.get("averageTradedPrice") or fill_price)
                if on_fallback: on_fallback(mp,mid)
                elif on_fill: on_fill(mp,mid)
            except Exception as e:
                self.logger.error("execute_with_fallback error: %s",e)
                if on_error: on_error(str(e))
        threading.Thread(target=_run,daemon=True).start()
