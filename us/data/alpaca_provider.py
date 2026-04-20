# -*- coding: utf-8 -*-
"""
alpaca_provider.py — Alpaca REST API Broker Provider
=====================================================
Q-TRON US 1.0 — Non-blocking order + Fill Monitor pattern.

Design:
- send_order() returns immediately (SUBMITTED)
- Fill monitor thread polls active orders
- Ghost reconciler handles timeout + late fills
- Event queue decouples fill callbacks from monitor thread
- avg_price = Alpaca API value only (no internal calculation)
"""
from __future__ import annotations

import logging
import os
import queue
import threading
import time
from enum import Enum
from pathlib import Path
from typing import Callable, Dict, List, Optional

import requests

logger = logging.getLogger("qtron.us.alpaca")


# ── Order Status Enum ────────────────────────────────────────

class OrderStatus(str, Enum):
    SUBMITTED = "SUBMITTED"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    TIMEOUT_UNCERTAIN = "TIMEOUT_UNCERTAIN"
    PENDING_EXTERNAL = "PENDING_EXTERNAL"


# ── Alpaca Provider ──────────────────────────────────────────

class AlpacaProvider:
    """
    Alpaca REST API broker provider.

    Non-blocking design:
    - send_order() → SUBMITTED immediately
    - start_fill_monitor() → background thread polls fills
    - process_events() → main loop consumes fill events from queue
    """

    def __init__(self, config=None):
        from dotenv import load_dotenv
        env_path = Path(__file__).resolve().parent.parent / ".env"
        load_dotenv(env_path)

        self._config = config
        self._api_key = os.getenv("ALPACA_API_KEY", "")
        self._secret = os.getenv("ALPACA_SECRET_KEY", "")
        self._base_url = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
        self._data_url = getattr(config, "ALPACA_DATA_URL", "https://data.alpaca.markets")

        self._session = requests.Session()
        self._session.headers.update({
            "APCA-API-KEY-ID": self._api_key,
            "APCA-API-SECRET-KEY": self._secret,
            "Content-Type": "application/json",
        })

        # Order tracking
        self._active_orders: Dict[str, dict] = {}
        self._processed_fill_keys: set = set()
        self._ghost_orders: Dict[str, dict] = {}
        self._fill_callbacks: List[Callable] = []
        self._event_queue: queue.Queue = queue.Queue()

        # Per-order lock (poll ↔ reconcile race prevention)
        self._order_locks: Dict[str, threading.Lock] = {}

        self._alive = True
        self._monitor_started = False

        # D-TRACE/D-HIST: Request tracker (deque 200 max)
        from collections import deque as _dq
        self._traces: _dq = _dq(maxlen=200)
        self._traces_lock = threading.Lock()

    # ── Request Tracker (for D-TRACE / D-HIST) ───────────────
    def _record_trace(self, method: str, path: str, status: int,
                      latency_ms: float, error: str = "") -> None:
        """각 HTTP 호출 결과를 ring buffer 에 기록."""
        with self._traces_lock:
            self._traces.append({
                "ts": time.time(),
                "method": method,
                "path": path,
                "status": status,
                "latency_ms": round(latency_ms, 1),
                "error": (error or "")[:200],
            })

    def get_traces(self, limit: int = 50) -> list:
        with self._traces_lock:
            items = list(self._traces)[-limit:]
        return list(reversed(items))

    def get_latency_histogram(self, bucket_ms: int = 100) -> dict:
        """최근 traces 의 latency 분포 (bucket 단위)."""
        with self._traces_lock:
            items = list(self._traces)
        if not items:
            return {"buckets": [], "count": 0, "p50": None, "p95": None, "avg": None}
        latencies = sorted([t["latency_ms"] for t in items if t.get("latency_ms")])
        if not latencies:
            return {"buckets": [], "count": 0, "p50": None, "p95": None, "avg": None}
        n = len(latencies)
        p50 = latencies[int(n * 0.50)]
        p95 = latencies[min(int(n * 0.95), n - 1)]
        avg = round(sum(latencies) / n, 1)
        # Bucket: 0-100, 100-200, ..., up to max
        max_ms = max(latencies)
        n_buckets = int(max_ms / bucket_ms) + 1
        n_buckets = min(n_buckets, 20)   # cap
        counts = [0] * n_buckets
        for ms in latencies:
            idx = min(int(ms / bucket_ms), n_buckets - 1)
            counts[idx] += 1
        buckets = [
            {"range": f"{i*bucket_ms}-{(i+1)*bucket_ms}ms", "count": c}
            for i, c in enumerate(counts)
        ]
        return {
            "buckets": buckets, "count": n, "p50": p50, "p95": p95, "avg": avg,
            "bucket_ms": bucket_ms,
        }

    # ── HTTP Helpers ─────────────────────────────────────────

    def _handle_http_error(self, method: str, path: str, status: int, body: str) -> None:
        """HTTP 오류 공통 처리. 403/429는 CRITICAL 로그로 즉시 가시화."""
        if status == 403:
            logger.critical(
                f"[ALPACA_403] {method} {path} → 403 Forbidden — "
                f"API key 만료 또는 권한 없음. trading BLOCKED. body={body[:200]}"
            )
        elif status == 429:
            logger.warning(
                f"[ALPACA_429] {method} {path} → 429 Rate Limited. body={body[:100]}"
            )
        else:
            logger.warning(f"[ALPACA] {method} {path} → {status}: {body[:100]}")

        # B2: Auth error circuit breaker
        # 401/403 연속 N회 → halt + CRITICAL DataEvent + Telegram
        if status in (401, 403):
            self._cb_note_auth_error(status, method, path, body)
        elif 200 <= status < 300:
            # success path — breaker reset 은 _get/_post 에서 처리

            pass

    # ── Circuit Breaker (B2) ─────────────────────────────────
    def _cb_init(self) -> None:
        """__init__ 에서 호출. state 초기화."""
        import threading as _th
        if not hasattr(self, "_cb_lock"):
            self._cb_lock = _th.Lock()
            self._cb_consecutive_auth_err = 0
            self._cb_halt_until: float = 0.0
            self._cb_threshold = 5
            self._cb_halt_sec = 300   # 5분

    def _cb_is_halted(self) -> bool:
        self._cb_init()
        with self._cb_lock:
            return self._cb_halt_until > 0 and time.time() < self._cb_halt_until

    def _cb_note_auth_error(self, status: int, method: str, path: str, body: str) -> None:
        self._cb_init()
        with self._cb_lock:
            self._cb_consecutive_auth_err += 1
            reached = (self._cb_consecutive_auth_err >= self._cb_threshold
                       and self._cb_halt_until == 0.0)
            if reached:
                self._cb_halt_until = time.time() + self._cb_halt_sec
        if reached:
            try:
                from shared.data_events import emit_event, Level
                emit_event(
                    source=f"ALPACA.auth",
                    level=Level.CRITICAL,
                    code="consecutive_auth_error",
                    message=(
                        f"Alpaca {status} 연속 {self._cb_threshold}회 — "
                        f"{self._cb_halt_sec}s halt"
                    ),
                    details={
                        "status": status,
                        "method": method,
                        "path": path,
                        "body_tail": body[:200],
                    },
                    telegram=True,
                )
            except Exception:
                pass

    def _cb_note_success(self) -> None:
        self._cb_init()
        was_halted = False
        with self._cb_lock:
            if self._cb_consecutive_auth_err > 0 or self._cb_halt_until > 0:
                was_halted = True
            self._cb_consecutive_auth_err = 0
            self._cb_halt_until = 0.0
        if was_halted:
            try:
                from shared.data_events import emit_event, Level
                emit_event(
                    source="ALPACA.auth",
                    level=Level.INFO,
                    code="consecutive_auth_error",
                    message="Alpaca 인증 복구됨",
                    telegram=False,
                )
            except Exception:
                pass

    def _get(self, path: str, *, _retry: bool = True) -> Optional[dict]:
        if self._cb_is_halted():
            return None
        t0 = time.time()
        try:
            url = f"{self._base_url}{path}"
            resp = self._session.get(url, timeout=15)
            lat = (time.time() - t0) * 1000
            self._record_trace("GET", path, resp.status_code, lat)
            if resp.status_code == 200:
                self._cb_note_success()
                return resp.json()
            self._handle_http_error("GET", path, resp.status_code, resp.text)
            # 429: 1회 backoff retry
            if resp.status_code == 429 and _retry:
                time.sleep(5)
                return self._get(path, _retry=False)
            return None
        except Exception as e:
            lat = (time.time() - t0) * 1000
            self._record_trace("GET", path, -1, lat, str(e))
            logger.error(f"[ALPACA] GET {path} error: {e}")
            return None

    def _post(self, path: str, body: dict, *, _retry: bool = True) -> Optional[dict]:
        if self._cb_is_halted():
            return None
        t0 = time.time()
        try:
            url = f"{self._base_url}{path}"
            resp = self._session.post(url, json=body, timeout=15)
            lat = (time.time() - t0) * 1000
            self._record_trace("POST", path, resp.status_code, lat)
            if resp.status_code in (200, 201):
                self._cb_note_success()
                return resp.json()
            self._handle_http_error("POST", path, resp.status_code, resp.text)
            # 429: 1회 backoff retry
            if resp.status_code == 429 and _retry:
                time.sleep(5)
                return self._post(path, body, _retry=False)
            return None
        except Exception as e:
            lat = (time.time() - t0) * 1000
            self._record_trace("POST", path, -1, lat, str(e))
            logger.error(f"[ALPACA] POST {path} error: {e}")
            return None

    def _delete(self, path: str) -> Optional[dict]:
        if self._cb_is_halted():
            return None
        t0 = time.time()
        try:
            url = f"{self._base_url}{path}"
            resp = self._session.delete(url, timeout=15)
            lat = (time.time() - t0) * 1000
            self._record_trace("DELETE", path, resp.status_code, lat)
            if resp.status_code in (200, 204):
                self._cb_note_success()
                try:
                    return resp.json()
                except Exception:
                    return {"ok": True}
            self._handle_http_error("DELETE", path, resp.status_code, resp.text)
            return None
        except Exception as e:
            lat = (time.time() - t0) * 1000
            self._record_trace("DELETE", path, -1, lat, str(e))
            logger.error(f"[ALPACA] DELETE {path} error: {e}")
            return None

    # ── Lifecycle ────────────────────────────────────────────

    @property
    def server_type(self) -> str:
        return "PAPER" if "paper" in self._base_url else "LIVE"

    @property
    def alive(self) -> bool:
        return self._alive

    def is_connected(self) -> bool:
        r = self._get("/v2/clock")
        return r is not None

    def shutdown(self) -> None:
        self._alive = False
        logger.info("[ALPACA] Shutdown")

    # ── Account ──────────────────────────────────────────────

    def query_account_summary(self) -> Dict:
        acct = self._get("/v2/account")
        if not acct:
            return {"error": "account query failed"}
        return {
            "equity": float(acct.get("equity", 0)),
            "cash": float(acct.get("cash", 0)),
            "buying_power": float(acct.get("buying_power", 0)),
            "available_cash": float(acct.get("cash", 0)),
            "portfolio_value": float(acct.get("portfolio_value", 0)),
            "last_equity": float(acct.get("last_equity", 0)),
        }

    def query_account_holdings(self) -> List[Dict]:
        positions = self._get("/v2/positions")
        if not positions:
            return []
        return [{
            "code": p["symbol"],
            "name": p["symbol"],
            "qty": int(float(p.get("qty", 0))),
            "quantity": int(float(p.get("qty", 0))),
            "avg_price": float(p.get("avg_entry_price", 0)),
            "cur_price": float(p.get("current_price", 0)),
            "pnl": float(p.get("unrealized_pl", 0)),
            "pnl_pct": float(p.get("unrealized_plpc", 0)) * 100,
            "market_value": float(p.get("market_value", 0)),
        } for p in positions]

    def query_sellable_qty(self, symbol: str) -> Dict:
        pos = self._get(f"/v2/positions/{symbol}")
        if not pos:
            return {"code": symbol, "hold_qty": 0, "sellable_qty": 0}
        qty = int(float(pos.get("qty", 0)))
        return {"code": symbol, "hold_qty": qty, "sellable_qty": qty}

    # ── Stock Info ───────────────────────────────────────────

    def get_stock_info(self, symbol: str) -> dict:
        asset = self._get(f"/v2/assets/{symbol}")
        if not asset:
            return {"name": symbol}
        return {
            "name": asset.get("name", symbol),
            "exchange": asset.get("exchange", ""),
            "tradable": asset.get("tradable", False),
            "shortable": asset.get("shortable", False),
            "symbol": symbol,
        }

    def get_current_price(self, symbol: str) -> float:
        """Latest quote from Alpaca Data API."""
        try:
            url = f"{self._data_url}/v2/stocks/{symbol}/quotes/latest"
            resp = self._session.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                quote = data.get("quote", {})
                # midpoint of bid/ask
                bid = float(quote.get("bp", 0))
                ask = float(quote.get("ap", 0))
                if bid > 0 and ask > 0:
                    return (bid + ask) / 2
                return ask or bid
        except Exception as e:
            logger.warning(f"[ALPACA] Price {symbol}: {e}")
        return 0.0

    def get_daily_change_pct(self, symbol: str) -> float:
        """Today's % change vs previous close (using snapshot API)."""
        try:
            url = f"{self._data_url}/v2/stocks/{symbol}/snapshot"
            resp = self._session.get(url, timeout=10)
            if resp.status_code == 200:
                d = resp.json()
                prev = d.get("prevDailyBar") or {}
                today = d.get("dailyBar") or {}
                latest = d.get("latestTrade") or {}
                prev_close = float(prev.get("c", 0) or 0)
                # 우선 latestTrade, 없으면 dailyBar 종가
                cur = float(latest.get("p", 0) or today.get("c", 0) or 0)
                if prev_close > 0 and cur > 0:
                    return round((cur / prev_close - 1) * 100, 2)
        except Exception as e:
            logger.warning(f"[ALPACA] Snapshot {symbol}: {e}")
        return 0.0

    # ── Non-blocking Order Submission ────────────────────────

    def send_order(self, symbol: str, side: str, quantity: int,
                   price: float = 0, order_type: str = "market") -> Dict:
        """Submit order → return immediately with SUBMITTED status."""
        body = {
            "symbol": symbol,
            "qty": str(quantity),
            "side": side.lower(),
            "type": order_type,
            "time_in_force": "day",
        }
        if order_type == "limit" and price > 0:
            body["limit_price"] = str(price)

        resp = self._post("/v2/orders", body)
        if not resp:
            return {"error": "order submission failed", "status": "REJECTED"}

        order_id = resp["id"]
        self._active_orders[order_id] = {
            "order_id": order_id,
            "symbol": symbol,
            "side": side.upper(),
            "requested_qty": quantity,
            "filled_qty": 0,
            "remaining_qty": quantity,
            "avg_price": 0.0,
            "status": OrderStatus.SUBMITTED,
            "submitted_at": time.time(),
        }

        logger.info(f"[ORDER] {side.upper()} {symbol} x{quantity} → {order_id[:8]}")
        return {
            "order_no": order_id,
            "exec_qty": 0,
            "exec_price": 0,
            "status": "SUBMITTED",
        }

    # ── Order Queries ────────────────────────────────────────

    def query_open_orders(self) -> Optional[List[Dict]]:
        orders = self._get("/v2/orders?status=open")
        if orders is None:
            return None
        return [{
            "order_no": o["id"],
            "code": o["symbol"],
            "side": o["side"].upper(),
            "qty": int(float(o.get("qty", 0))),
            "filled_qty": int(float(o.get("filled_qty", 0))),
            "remaining": int(float(o.get("qty", 0))) - int(float(o.get("filled_qty", 0))),
            "status": o["status"],
        } for o in orders]

    def cancel_order(self, symbol: str, order_id: str,
                     qty: int = 0, side: str = "BUY") -> Dict:
        result = self._delete(f"/v2/orders/{order_id}")
        if result:
            return {"ok": True}
        return {"ok": False, "error": "cancel failed"}

    def cancel_all_open_orders(self) -> Optional[int]:
        result = self._delete("/v2/orders")
        if result is None:
            return None
        if isinstance(result, list):
            return len(result)
        return 0

    # ── Fill Monitor (background thread) ─────────────────────

    def start_fill_monitor(self):
        """Start background fill monitoring thread."""
        if self._monitor_started:
            return
        self._monitor_started = True

        def _monitor():
            while self._alive:
                try:
                    self._poll_active_orders()
                    self._reconcile_ghost_orders()
                except Exception as e:
                    logger.error(f"[FILL_MONITOR] error: {e}")
                time.sleep(2)

        t = threading.Thread(target=_monitor, daemon=True, name="fill-monitor")
        t.start()
        logger.info("[FILL_MONITOR] Started")

    def _get_order_lock(self, order_id: str) -> threading.Lock:
        if order_id not in self._order_locks:
            self._order_locks[order_id] = threading.Lock()
        return self._order_locks[order_id]

    def _poll_active_orders(self):
        """Poll all active orders, update state."""
        for order_id in list(self._active_orders.keys()):
            # Skip if ghost reconcile owns this order
            if order_id in self._ghost_orders:
                continue

            lock = self._get_order_lock(order_id)
            if not lock.acquire(blocking=False):
                continue

            try:
                tracking = self._active_orders[order_id]

                # Skip terminal states
                if tracking["status"] in (OrderStatus.FILLED,
                                           OrderStatus.CANCELLED,
                                           OrderStatus.REJECTED):
                    continue

                order = self._get(f"/v2/orders/{order_id}")
                if not order:
                    continue

                self._process_order_update(order_id, tracking, order)
            finally:
                lock.release()

    def _process_order_update(self, order_id: str, tracking: dict, order: dict):
        """Shared logic for processing an order update (poll + reconcile)."""
        api_status = order["status"]
        api_filled_qty = int(float(order.get("filled_qty") or 0))
        api_avg_price = float(order.get("filled_avg_price") or 0)

        # Fill dedup key (exclude avg_price — it fluctuates)
        fill_key = f"{order_id}_{api_filled_qty}"
        if fill_key in self._processed_fill_keys:
            return

        # Partial fill accumulation
        prev_filled = tracking["filled_qty"]
        new_fill = api_filled_qty - prev_filled

        if new_fill > 0:
            # Qty clamp: never exceed requested
            clamped = min(new_fill, tracking["remaining_qty"])

            # avg_price = Alpaca API value only (no internal calculation)
            if api_filled_qty > tracking["filled_qty"]:
                tracking["avg_price"] = api_avg_price

            total_qty = prev_filled + clamped
            tracking["filled_qty"] = total_qty
            tracking["remaining_qty"] = max(
                tracking["requested_qty"] - total_qty, 0
            )

            self._processed_fill_keys.add(fill_key)

            # Enqueue event (main loop processes, not monitor thread)
            self._event_queue.put({
                "order_id": order_id,
                "symbol": tracking["symbol"],
                "side": tracking["side"],
                "new_fill_qty": clamped,
                "total_filled_qty": total_qty,
                "avg_price": tracking["avg_price"],
                "remaining_qty": tracking["remaining_qty"],
            })

            logger.info(
                f"[FILL] {tracking['symbol']} {tracking['side']} "
                f"+{clamped} (total {total_qty}/{tracking['requested_qty']}) "
                f"@{tracking['avg_price']:.2f}"
            )

        # Status transition
        if api_status == "filled":
            tracking["status"] = OrderStatus.FILLED
            tracking["completed_at"] = time.time()
        elif api_status == "partially_filled":
            tracking["status"] = OrderStatus.PARTIAL
        elif api_status in ("cancelled", "expired"):
            tracking["status"] = OrderStatus.CANCELLED
            tracking["completed_at"] = time.time()
        elif api_status == "rejected":
            tracking["status"] = OrderStatus.REJECTED
            tracking["completed_at"] = time.time()

        # Timeout detection
        age = time.time() - tracking["submitted_at"]
        fill_timeout = getattr(self._config, "FILL_TIMEOUT_SEC", 30.0)
        if age > fill_timeout and tracking["status"] == OrderStatus.SUBMITTED:
            tracking["status"] = OrderStatus.TIMEOUT_UNCERTAIN
            self._ghost_orders[order_id] = {
                "order_id": order_id,
                "symbol": tracking["symbol"],
                "side": tracking["side"],
                "requested_qty": tracking["requested_qty"],
                "filled_qty": tracking["filled_qty"],
                "created_at": time.time(),
            }
            logger.warning(
                f"[GHOST] {tracking['symbol']} {order_id[:8]} "
                f"→ TIMEOUT_UNCERTAIN after {age:.0f}s"
            )

    def _reconcile_ghost_orders(self):
        """Reconcile timeout orders — check if they got filled later."""
        resolved = []

        for order_id, ghost in list(self._ghost_orders.items()):
            lock = self._get_order_lock(order_id)
            if not lock.acquire(blocking=False):
                continue

            try:
                order = self._get(f"/v2/orders/{order_id}")
                if not order:
                    continue

                tracking = self._active_orders.get(order_id)
                if not tracking:
                    resolved.append(order_id)
                    continue

                if order["status"] == "filled":
                    self._process_order_update(order_id, tracking, order)
                    tracking["status"] = OrderStatus.FILLED
                    tracking["completed_at"] = time.time()

                    # Mark event as ghost-resolved
                    self._event_queue.put({
                        "order_id": order_id,
                        "symbol": tracking["symbol"],
                        "side": tracking["side"],
                        "new_fill_qty": 0,
                        "total_filled_qty": tracking["filled_qty"],
                        "avg_price": tracking["avg_price"],
                        "remaining_qty": tracking["remaining_qty"],
                        "was_ghost": True,
                    })
                    resolved.append(order_id)
                    logger.info(f"[GHOST_RESOLVED] {tracking['symbol']} {order_id[:8]}")

                elif order["status"] in ("cancelled", "expired"):
                    tracking["status"] = OrderStatus.CANCELLED
                    tracking["completed_at"] = time.time()
                    resolved.append(order_id)

            finally:
                lock.release()

        # Ghost TTL: force drop after MAX_GHOST_AGE_SEC
        max_age = getattr(self._config, "MAX_GHOST_AGE_SEC", 300.0)
        now = time.time()
        for oid, g in list(self._ghost_orders.items()):
            if now - g["created_at"] > max_age:
                logger.warning(f"[GHOST_TTL] {oid[:8]} expired after {max_age}s")
                resolved.append(oid)

        for oid in resolved:
            self._ghost_orders.pop(oid, None)

        # Cleanup terminal orders (60s after completion)
        # P2-GHOST-3 fix (2026-04-17): _processed_fill_keys도 함께 정리.
        # key 형식: "{order_id}_{filled_qty}" — order_id prefix 매칭으로 제거.
        #
        # INVARIANT-7 (terminal cleanup):
        #   terminal 상태(FILLED/CANCELLED/REJECTED)가 된 지 60s 후 _active_orders에서 제거.
        #   제거와 동시에 해당 order_id prefix의 fill key도 _processed_fill_keys에서 삭제.
        #
        #   안전 근거:
        #   - 제거 후 해당 order는 _poll_active_orders 순회 대상에서 제외됨.
        #   - fill monitor 스레드가 이 order에 대한 이벤트를 더 이상 생성하지 않음.
        #   - fill key 삭제 후 뒤늦은 poll이 와도 중복 반영 경로가 없음.
        #   - _monitor()는 단일 스레드 → _poll과 cleanup이 동시 실행되지 않음.
        #
        #   STEP 3+ 작업 시 이 TTL(60s)을 줄이거나 cleanup 순서를 바꾸지 말 것.
        #   cleanup 이전에 order 재사용(same oid) 금지 (Alpaca는 항상 새 UUID 발급).
        stale = [
            oid for oid, t in self._active_orders.items()
            if t["status"] in (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED)
            and now - t.get("completed_at", now) > 60
        ]
        for oid in stale:
            t = self._active_orders[oid]
            age = now - t.get("completed_at", now)
            stale_keys = {k for k in self._processed_fill_keys if k.startswith(f"{oid}_")}
            del self._active_orders[oid]
            self._order_locks.pop(oid, None)
            self._processed_fill_keys -= stale_keys
            if stale_keys:
                logger.debug(
                    f"[FILL_KEY_CLEANUP] order_id={oid[:8]} "
                    f"removed={len(stale_keys)} age_sec={age:.0f}"
                )

    # ── Event Queue Consumer ─────────────────────────────────

    def process_events(self):
        """Main loop calls this. Processes fill events from queue."""
        while not self._event_queue.empty():
            try:
                event = self._event_queue.get_nowait()
                for cb in self._fill_callbacks:
                    try:
                        cb(event)
                    except Exception as e:
                        logger.error(f"[FILL_CB] error: {e}")
            except queue.Empty:
                break

    def set_fill_callback(self, callback: Callable) -> None:
        """Register callback for fill events."""
        self._fill_callbacks.append(callback)

    # ── Market Hours ─────────────────────────────────────────

    def get_clock(self) -> Optional[Dict]:
        """Market clock: is_open, next_open, next_close."""
        return self._get("/v2/clock")

    def get_calendar(self, start: str, end: str) -> List[str]:
        """Trading calendar: list of trading day dates (YYYY-MM-DD)."""
        try:
            url = f"{self._base_url}/v2/calendar"
            resp = self._session.get(url, params={"start": start, "end": end}, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                return [d.get("date", "") for d in data if d.get("date")]
        except Exception as e:
            logger.warning(f"[ALPACA] Calendar error: {e}")
        return []

    def is_market_open(self) -> bool:
        clock = self.get_clock()
        if clock:
            return clock.get("is_open", False)
        return False
