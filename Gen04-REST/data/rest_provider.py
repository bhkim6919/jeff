"""
rest_provider.py — Kiwoom REST API Provider
============================================
BrokerProvider 구현체. COM(QAxWidget) 대신 HTTP REST + WebSocket 사용.

Phase 0: HTTP 조회/주문.
Phase 1: WebSocket 실시간 (0B 가격, 00 주문체결, 04 잔고).
"""
from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional

import requests
from dotenv import load_dotenv

from data.provider_base import BrokerProvider
from data.rest_logger import setup_rest_logging
from data.rest_token_manager import TokenManager
from data.rest_websocket import KiwoomWebSocket
from web.api_state import tracker as api_tracker

# 로깅 자동 초기화 (import 시 1회)
setup_rest_logging()

logger = logging.getLogger("gen4.rest")

# Rate limit: minimum interval between REST calls
_MIN_REQUEST_INTERVAL = 0.2  # 200ms


def _decode_name(raw: str) -> str:
    """Decode Kiwoom EUC-KR encoded stock name."""
    if raw and any(ord(c) > 0x7F for c in raw):
        try:
            return raw.encode("latin-1").decode("euc-kr", errors="replace")
        except Exception:
            pass
    return raw


class KiwoomRestProvider(BrokerProvider):
    """Kiwoom REST API provider for Q-TRON Gen4."""

    def __init__(
        self,
        *,
        server_type: str = "REAL",
        sector_map_path: Optional[str] = None,
        env_path: Optional[str] = None,
    ) -> None:
        import os

        env_file = env_path or str(Path(__file__).resolve().parent.parent / ".env")
        load_dotenv(env_file)

        self._server_type_val = server_type
        self._alive = True

        # Credentials
        app_key = os.getenv("KIWOOM_APP_KEY", "")
        app_secret = os.getenv("KIWOOM_APP_SECRET", "")
        self._account_no = os.getenv("KIWOOM_ACCOUNT", "").replace("-", "")
        self._base_url = os.getenv("KIWOOM_API_URL", "https://api.kiwoom.com")

        if not app_key or not app_secret:
            raise RuntimeError("KIWOOM_APP_KEY / KIWOOM_APP_SECRET not set in .env")

        self._token_mgr = TokenManager(app_key, app_secret, self._base_url)

        # Tracker: server info + token
        api_tracker.set_server_info(server_type, self._base_url)

        # Rate limiter
        self._last_request_time = 0.0

        # Callbacks
        self._ghost_fill_callback: Optional[Callable] = None
        self._real_data_callback: Optional[Callable] = None
        self._micro_callback: Optional[Callable] = None
        self._ghost_orders_list: List[Dict] = []

        # WebSocket client (lazy init on first register_real)
        self._ws: Optional[KiwoomWebSocket] = None
        self._ws_started = False

        # Pending order tracking (for WebSocket fill matching)
        self._pending_lock = threading.Lock()
        self._pending_order_no: str = ""
        self._pending_exec_price: int = 0
        self._pending_exec_qty: int = 0
        self._pending_filled = threading.Event()

        # Sector map
        self._sector_map: Dict[str, str] = {}
        if sector_map_path:
            self._load_sector_map(sector_map_path)

        logger.info(
            f"[REST_PROVIDER] init: server={server_type}, "
            f"account={self._account_no[:4]}****, base={self._base_url}"
        )

    def _load_sector_map(self, path: str) -> None:
        import json

        try:
            with open(path, "r", encoding="utf-8") as f:
                self._sector_map = json.load(f)
        except Exception as e:
            logger.warning(f"[REST_PROVIDER] sector_map load failed: {e}")

    # ── Central HTTP Request ──────────────────────────────────

    def _request(
        self,
        api_id: str,
        path: str,
        body: dict,
        retry_on_401: bool = True,
        related_code: str = "",
    ) -> dict:
        """Central REST API caller with rate limit, token refresh, and tracker."""
        if not self._alive:
            return {"return_code": -1, "return_msg": "Provider shut down"}

        # Rate limit
        elapsed = time.time() - self._last_request_time
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)

        headers = self._token_mgr.auth_headers()
        headers["api-id"] = api_id

        url = f"{self._base_url}{path}"
        self._last_request_time = time.time()

        # Tracker: start
        req_id = api_tracker.record_request_start(path, api_id, related_code=related_code)
        t0 = time.time()

        try:
            resp = requests.post(url, json=body, headers=headers, timeout=15)
            latency = (time.time() - t0) * 1000

            if resp.status_code >= 500:
                logger.error(f"[REST] {api_id} HTTP {resp.status_code}")
                api_tracker.record_request_end(
                    req_id, status="error", http_status=resp.status_code, latency_ms=latency,
                    error=f"HTTP {resp.status_code}")
                return {"return_code": -1, "return_msg": f"HTTP {resp.status_code}"}

            data = resp.json()

            # Token expired → refresh and retry once
            if data.get("return_code") == 1 and retry_on_401:
                msg = data.get("return_msg", "")
                if "토큰" in msg or "token" in msg.lower() or "401" in msg:
                    logger.warning(f"[REST] Token expired, refreshing...")
                    self._token_mgr.invalidate()
                    api_tracker.record_request_end(
                        req_id, status="retry", latency_ms=latency, error="token_expired", retry_count=1)
                    return self._request(api_id, path, body, retry_on_401=False, related_code=related_code)

            rc = data.get("return_code", -1)
            if rc not in (0, None):
                logger.warning(
                    f"[REST] {api_id} rc={rc} "
                    f"msg={data.get('return_msg', '')[:80]}"
                )
                api_tracker.record_request_end(
                    req_id, status="error", http_status=resp.status_code, latency_ms=latency,
                    error=data.get("return_msg", "")[:200])
            else:
                api_tracker.record_request_end(
                    req_id, status="ok", http_status=resp.status_code, latency_ms=latency)

            return data

        except requests.Timeout:
            latency = (time.time() - t0) * 1000
            logger.error(f"[REST_TIMEOUT] {api_id} {path}")
            api_tracker.record_request_end(
                req_id, status="timeout", latency_ms=latency, error="HTTP timeout")
            return {"return_code": -1, "return_msg": "HTTP timeout"}
        except Exception as e:
            latency = (time.time() - t0) * 1000
            logger.error(f"[REST_ERROR] {api_id}: {e}")
            api_tracker.record_request_end(
                req_id, status="error", latency_ms=latency, error=str(e)[:200])
            return {"return_code": -1, "return_msg": str(e)}

    # ── Lifecycle ─────────────────────────────────────────────

    def shutdown(self) -> None:
        self._alive = False
        self._real_data_callback = None
        self._micro_callback = None
        if self._ws:
            self._ws.stop()
            self._ws = None
            self._ws_started = False
        logger.info("[REST_PROVIDER] shutdown")

    @property
    def alive(self) -> bool:
        return self._alive

    def is_connected(self) -> bool:
        try:
            tok = self._token_mgr.token
            return bool(tok)
        except Exception:
            return False

    def ensure_connected(self) -> bool:
        if not self._alive:
            return False
        try:
            self._token_mgr.invalidate()
            tok = self._token_mgr.token
            return bool(tok)
        except Exception:
            return False

    # ── Identity ──────────────────────────────────────────────

    @property
    def server_type(self) -> str:
        return self._server_type_val

    # ── Stock Information ─────────────────────────────────────

    def get_stock_info(self, code: str) -> dict:
        data = self._request("ka10001", "/api/dostk/stkinfo", {"stk_cd": code})
        if data.get("return_code") != 0:
            return {"name": "", "sector": "", "market": "", "market_cap": 0, "listed_shares": 0}

        name = _decode_name(data.get("stk_nm", ""))

        return {
            "name": name,
            "sector": self._sector_map.get(code, ""),
            "market": data.get("mrkt_tp", ""),
            "market_cap": int(data.get("mac", "0") or "0"),
            "listed_shares": int(data.get("flo_stk", "0") or "0"),
        }

    def get_current_price(self, code: str) -> float:
        data = self._request("ka10004", "/api/dostk/mrkcond", {"stk_cd": code})
        if data.get("return_code") != 0:
            return 0.0
        # Use best bid/ask midpoint or first available price
        buy1 = data.get("buy_fpr_bid", "0")
        sell1 = data.get("sel_fpr_bid", "0")
        buy_p = abs(int(buy1 or "0"))
        sell_p = abs(int(sell1 or "0"))
        if buy_p and sell_p:
            return float((buy_p + sell_p) // 2)
        return float(buy_p or sell_p)

    # ── Account Queries ───────────────────────────────────────

    def query_account_holdings(self) -> List[Dict]:
        data = self._request(
            "kt00018",
            "/api/dostk/acnt",
            {"qry_tp": "2", "dmst_stex_tp": "KRX"},  # 2=개별
        )
        if data.get("return_code") != 0:
            return []

        holdings = []
        for item in data.get("acnt_evlt_remn_indv_tot", []):
            code = item.get("stk_cd", "").replace("A", "")
            name = _decode_name(item.get("stk_nm", ""))

            holdings.append({
                "code": code,
                "name": name,
                "qty": int(item.get("rmnd_qty", "0")),
                "quantity": int(item.get("rmnd_qty", "0")),
                "avg_price": int(item.get("pur_pric", "0")),
                "cur_price": int(item.get("cur_prc", "0")),
                "pnl": int(item.get("evltv_prft", "0")),
            })
        return holdings

    def query_account_summary(self) -> Dict:
        data = self._request(
            "kt00018",
            "/api/dostk/acnt",
            {"qry_tp": "1", "dmst_stex_tp": "KRX"},  # 1=합산
        )
        if data.get("return_code") != 0:
            return {"error": data.get("return_msg", "query failed"), "holdings_reliable": False}

        tot_eval = int(data.get("tot_evlt_amt", "0"))
        prsm_asset = int(data.get("prsm_dpst_aset_amt", "0"))
        available_cash = prsm_asset - tot_eval

        holdings = []
        for item in data.get("acnt_evlt_remn_indv_tot", []):
            code = item.get("stk_cd", "").replace("A", "")
            name = _decode_name(item.get("stk_nm", ""))
            holdings.append({
                "code": code,
                "name": name,
                "qty": int(item.get("rmnd_qty", "0")),
                "avg_price": int(item.get("pur_pric", "0")),
                "cur_price": int(item.get("cur_prc", "0")),
                "eval_amt": int(item.get("evlt_amt", "0")),
                "pnl": int(item.get("evltv_prft", "0")),
                "pnl_rate": item.get("prft_rt", "0"),
            })

        api_tracker.update_freshness("account_summary")
        api_tracker.update_freshness("holdings")
        return {
            "추정예탁자산": prsm_asset,
            "총매입금액": int(data.get("tot_pur_amt", "0")),
            "총평가금액": tot_eval,
            "총평가손익금액": int(data.get("tot_evlt_pl", "0")),
            "holdings": holdings,
            "available_cash": available_cash,
            "error": None,
            "holdings_reliable": True,
        }

    def query_sellable_qty(self, code: str) -> Dict:
        holdings = self.query_account_holdings()
        for h in holdings:
            if h["code"] == code:
                return {
                    "code": code,
                    "hold_qty": h["qty"],
                    "sellable_qty": h["qty"],  # REST에서는 trde_able_qty 사용 가능
                    "source": "rest_kt00018",
                    "error": None,
                }
        return {
            "code": code,
            "hold_qty": 0,
            "sellable_qty": 0,
            "source": "rest_kt00018",
            "error": "not found in holdings",
        }

    # ── Order Execution ───────────────────────────────────────

    def send_order(
        self,
        code: str,
        side: str,
        quantity: int,
        price: int = 0,
        hoga_type: str = "03",
    ) -> Dict:
        api_id = "kt10000" if side.upper() == "BUY" else "kt10001"

        # Map hoga_type: "03"=시장가 → trde_tp "3"
        trde_tp_map = {"01": "0", "03": "3", "00": "0"}
        trde_tp = trde_tp_map.get(hoga_type, "3")

        body: dict = {
            "dmst_stex_tp": "KRX",
            "stk_cd": code,
            "ord_qty": str(quantity),
            "trde_tp": trde_tp,
        }
        if price > 0 and trde_tp == "0":
            body["ord_uv"] = str(price)

        # Reset pending order state for WebSocket fill matching
        with self._pending_lock:
            self._pending_order_no = ""
            self._pending_exec_price = 0
            self._pending_exec_qty = 0
            self._pending_filled.clear()

        data = self._request(api_id, "/api/dostk/ordr", body)

        if data.get("return_code") == 0:
            order_no = data.get("ord_no", "")
            with self._pending_lock:
                self._pending_order_no = order_no
            logger.info(
                f"[REST_ORDER] {side} {code} qty={quantity} → order_no={order_no}"
            )

            # Wait for WebSocket fill (if WS connected, max 30s)
            exec_price = 0
            exec_qty = 0
            if self._ws_started and self._ws and self._ws.connected:
                filled = self._pending_filled.wait(timeout=30)
                with self._pending_lock:
                    exec_price = self._pending_exec_price
                    exec_qty = self._pending_exec_qty
                if filled and exec_qty > 0:
                    logger.info(
                        f"[REST_ORDER_FILLED] {order_no}: {exec_qty}@{exec_price}"
                    )
                else:
                    logger.warning(
                        f"[REST_ORDER_TIMEOUT] {order_no}: no fill in 30s"
                    )

            with self._pending_lock:
                self._pending_order_no = ""
            return {
                "order_no": order_no,
                "exec_price": exec_price,
                "exec_qty": exec_qty,
                "error": None,
                "status": "FILLED" if exec_qty > 0 else "SUBMITTED",
            }
        else:
            error_msg = data.get("return_msg", "order failed")
            logger.error(f"[REST_ORDER_FAIL] {side} {code}: {error_msg}")
            return {
                "order_no": "",
                "exec_price": 0,
                "exec_qty": 0,
                "error": error_msg,
            }

    def query_open_orders(self) -> Optional[List[Dict]]:
        data = self._request(
            "ka10075",
            "/api/dostk/acnt",
            {
                "qry_tp": "0",
                "all_stk_tp": "0",
                "sell_tp": "0",
                "sort_tp": "1",
                "trde_tp": "0",
                "stex_tp": "KRX",
                "dmst_stex_tp": "KRX",
            },
        )
        if data.get("return_code") != 0:
            return None

        orders = []
        for item in data.get("oso", []):
            orders.append({
                "order_no": item.get("ord_no", ""),
                "code": item.get("stk_cd", "").replace("A", ""),
                "side": "SELL" if item.get("sell_tp", "") == "1" else "BUY",
                "qty": int(item.get("ord_qty", "0")),
                "filled_qty": int(item.get("cntr_qty", "0")),
                "remaining": int(item.get("noncntr_qty", "0")),
                "order_time": item.get("ord_tm", ""),
                "status_raw": item.get("ord_stt", ""),
            })
        api_tracker.update_freshness("open_orders")
        return orders

    def cancel_order(
        self, code: str, order_no: str, qty: int, side: str = "BUY"
    ) -> Dict:
        body = {
            "dmst_stex_tp": "KRX",
            "stk_cd": code,
            "orig_ord_no": order_no,
            "cncl_qty": str(qty) if qty > 0 else "0",
        }
        data = self._request("kt10003", "/api/dostk/ordr", body)
        if data.get("return_code") == 0:
            logger.info(f"[REST_CANCEL] {code} order_no={order_no} → OK")
            return {"ok": True, "error": None}
        else:
            return {"ok": False, "error": data.get("return_msg", "cancel failed")}

    def cancel_all_open_orders(self) -> Optional[int]:
        orders = self.query_open_orders()
        if orders is None:
            return None
        count = 0
        for o in orders:
            result = self.cancel_order(
                o["code"], o["order_no"], o["remaining"], o["side"]
            )
            if result["ok"]:
                count += 1
        return count

    # ── Ghost Order Management ────────────────────────────────

    def set_ghost_fill_callback(self, callback: Optional[Callable]) -> None:
        self._ghost_fill_callback = callback

    def get_ghost_orders(self) -> List[Dict]:
        return list(self._ghost_orders_list)

    def clear_ghost_orders(self) -> None:
        self._ghost_orders_list.clear()

    # ── WebSocket Helpers ────────────────────────────────────

    def _ensure_ws(self) -> KiwoomWebSocket:
        """Lazy-init and start WebSocket client."""
        if not self._ws:
            self._ws = KiwoomWebSocket(
                token=self._token_mgr.token,
                server_type=self._server_type_val,
                token_refresher=lambda: self._token_mgr.token,
            )
            self._ws.set_on_price_tick(self._on_ws_price)
            self._ws.set_on_order_exec(self._on_ws_order)
            self._ws.set_on_balance_update(self._on_ws_balance)
        if not self._ws_started:
            self._ws.start()
            self._ws_started = True
            # Wait briefly for connection
            for _ in range(20):
                if self._ws.connected:
                    break
                import time as _t
                _t.sleep(0.25)
            if self._ws.connected:
                logger.info("[WS] Connected and ready")
            else:
                logger.warning("[WS] Connection pending (may connect later)")
        return self._ws

    def _on_ws_price(self, code: str, values: dict) -> None:
        """Handle Type 0B (주식체결) WebSocket message."""
        try:
            price_raw = values.get("10", "0")
            price = abs(int(price_raw.replace("+", "").replace("-", "") or "0"))
            volume_raw = values.get("13", "0")
            volume = abs(int(volume_raw.replace("+", "").replace("-", "") or "0"))

            if self._real_data_callback and price > 0:
                self._real_data_callback(code, float(price), volume)

            if self._micro_callback and price > 0:
                fid_data = {
                    "timestamp": values.get("20", ""),
                    "price": price,
                    "best_ask": abs(int(values.get("27", "0").replace("+", "").replace("-", "") or "0")),
                    "best_bid": abs(int(values.get("28", "0").replace("+", "").replace("-", "") or "0")),
                    "ask_qty_1": abs(int(values.get("1030", "0") or "0")),
                    "bid_qty_1": abs(int(values.get("1031", "0") or "0")),
                    "total_ask": 0,  # not in 0B, available in 0D
                    "total_bid": 0,
                    "net_bid": 0,
                    "volume": volume,
                }
                self._micro_callback(code, fid_data)

        except Exception as e:
            logger.error(f"[WS_PRICE_ERR] {code}: {e}")

    def _on_ws_order(self, values: dict) -> None:
        """Handle Type 00 (주문체결) WebSocket message."""
        try:
            order_no = values.get("9203", "")
            code = values.get("9001", "").replace("A", "")
            exec_qty = abs(int(values.get("911", "0") or "0"))
            exec_price = abs(int(values.get("910", "0") or "0"))
            order_status = values.get("913", "")
            side_raw = values.get("907", "")
            side = "BUY" if side_raw == "1" else "SELL"

            logger.info(
                f"[WS_ORDER] {side} {code} order={order_no} "
                f"status={order_status} exec={exec_qty}@{exec_price}"
            )

            # Update pending order if we have one (thread-safe)
            with self._pending_lock:
                if order_no and self._pending_order_no == order_no:
                    if exec_qty > 0 and exec_price > 0:
                        self._pending_exec_price = exec_price
                        self._pending_exec_qty += exec_qty
                        if order_status in ("체결", "확인"):
                            self._pending_filled.set()

            # Ghost fill callback
            if self._ghost_fill_callback and order_no:
                self._ghost_fill_callback({
                    "order_no": order_no,
                    "code": code,
                    "side": side,
                    "exec_qty": exec_qty,
                    "exec_price": exec_price,
                    "status": order_status,
                })

        except Exception as e:
            logger.error(f"[WS_ORDER_ERR] {e}")

    def _on_ws_balance(self, values: dict) -> None:
        """Handle Type 04 (잔고) WebSocket message."""
        code = values.get("9001", "").replace("A", "")
        qty = abs(int(values.get("930", "0") or "0"))
        logger.info(f"[WS_BALANCE] {code} qty={qty}")

    # ── Real-time Data ────────────────────────────────────────

    def register_real(self, codes: List[str], fids: str = "10;27") -> None:
        ws = self._ensure_ws()
        ws.subscribe(codes, "0B")
        # Also subscribe to order execution for this session
        ws.subscribe([""], "00")
        logger.info(f"[REST] register_real: {len(codes)} codes via WebSocket")

    def unregister_real(self) -> None:
        if self._ws:
            self._ws.unsubscribe_all()
        logger.info("[REST] unregister_real: WebSocket unsubscribed")

    def register_real_append(
        self, codes: List[str], fids: str = "10;27", screen: Optional[str] = None
    ) -> int:
        ws = self._ensure_ws()
        ws.subscribe(codes, "0B")
        return len(codes)

    def unregister_real_screen(self, screen: str) -> None:
        # WebSocket has no screen concept — log only
        logger.info(f"[REST] unregister_real_screen: {screen} (no-op in REST)")

    def set_real_data_callback(self, callback: Optional[Callable]) -> None:
        self._real_data_callback = callback

    def set_micro_callback(self, callback: Optional[Callable]) -> None:
        self._micro_callback = callback

    # ── Index Data ────────────────────────────────────────────

    def get_kospi_close(self, trade_date: str = "") -> float:
        data = self._request(
            "ka20001",
            "/api/dostk/sect",
            {"mrkt_tp": "0", "inds_cd": "001"},
        )
        if data.get("return_code") != 0:
            return 0.0
        raw = data.get("cur_prc", "0")
        val = float(raw.replace("+", "").replace("-", ""))
        return val

    def get_kosdaq_close(self, trade_date: str = "") -> float:
        data = self._request(
            "ka20001",
            "/api/dostk/sect",
            {"mrkt_tp": "1", "inds_cd": "101"},
        )
        if data.get("return_code") != 0:
            return 0.0
        raw = data.get("cur_prc", "0")
        val = float(raw.replace("+", "").replace("-", ""))
        return val

    def get_index_minute_bars(
        self,
        index_code: str = "001",
        trade_date: str = "",
        tick_range: int = 1,
    ) -> List[dict]:
        # Phase 0: stub — REST 업종분봉(ka20005) 구현은 Phase 1
        logger.info("[REST_STUB] get_index_minute_bars (Phase 1)")
        return []
