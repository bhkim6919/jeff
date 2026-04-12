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

    # ── HTTP Helpers ─────────────────────────────────────────

    def _get(self, path: str) -> Optional[dict]:
        try:
            url = f"{self._base_url}{path}"
            resp = self._session.get(url, timeout=15)
            if resp.status_code == 200:
                return resp.json()
            logger.warning(f"[ALPACA] GET {path} → {resp.status_code}: {resp.text[:100]}")
            return None
        except Exception as e:
            logger.error(f"[ALPACA] GET {path} error: {e}")
            return None

    def _post(self, path: str, body: dict) -> Optional[dict]:
        try:
            url = f"{self._base_url}{path}"
            resp = self._session.post(url, json=body, timeout=15)
            if resp.status_code in (200, 201):
                return resp.json()
            logger.warning(f"[ALPACA] POST {path} → {resp.status_code}: {resp.text[:200]}")
            return None
        except Exception as e:
            logger.error(f"[ALPACA] POST {path} error: {e}")
            return None

    def _delete(self, path: str) -> Optional[dict]:
        try:
            url = f"{self._base_url}{path}"
            resp = self._session.delete(url, timeout=15)
            if resp.status_code in (200, 204):
                try:
                    return resp.json()
                except Exception:
                    return {"ok": True}
            logger.warning(f"[ALPACA] DELETE {path} → {resp.status_code}")
            return None
        except Exception as e:
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
        stale = [
            oid for oid, t in self._active_orders.items()
            if t["status"] in (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED)
            and now - t.get("completed_at", now) > 60
        ]
        for oid in stale:
            del self._active_orders[oid]
            self._order_locks.pop(oid, None)

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
