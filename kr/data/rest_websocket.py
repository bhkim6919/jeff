"""
rest_websocket.py — Kiwoom REST API WebSocket Client
=====================================================
실시간 데이터 수신: 주식체결(0B), 주문체결(00), 잔고(04).
별도 스레드에서 asyncio 이벤트루프 실행.

Phase 1: 연결 + 구독 + 메시지 라우팅.
"""
from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from typing import Callable, Dict, List, Optional, Set

import websockets
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger("gen4.rest.ws")

WS_URL_REAL = "wss://api.kiwoom.com:10000/api/dostk/websocket"
WS_URL_MOCK = "wss://mockapi.kiwoom.com:10000/api/dostk/websocket"

# Reconnect strategy (Jeff 2026-04-29 — incident: 15:20:23 KST WS hit
# MAX_RECONNECT=5 mid-session and flipped to REST fallback even though
# the underlying connection was recoverable; Surge tick_count froze and
# Lab realtime ticks dropped to 0 for the rest of the day).
#
# Two-tier policy:
#   * MAX_RECONNECT_MARKET_HOURS — generous during market hours so a
#     transient API hiccup doesn't lose the rest of the trading day.
#   * MAX_RECONNECT_OFF_HOURS — tighter when there's nothing to lose
#     by giving up; an idle WS doesn't need to retry forever.
#
# Korean market open window (Kiwoom REST WebSocket lifetime that
# matters): 09:00~15:30 KST. Pre-market 08:30~09:00 also bursty. We
# treat 08:00~16:00 KST as "market hours" for the retry budget.
MAX_RECONNECT_MARKET_HOURS = 60   # ~5 minutes at RECONNECT_DELAY=5
MAX_RECONNECT_OFF_HOURS = 5
MAX_RECONNECT = MAX_RECONNECT_MARKET_HOURS  # legacy alias for any
                                            # external readers; the
                                            # actual budget is computed
                                            # at retry time below.
RECONNECT_DELAY = 5  # seconds
PING_INTERVAL = 30  # seconds


class KiwoomWebSocket:
    """Kiwoom REST API WebSocket client for real-time data."""

    def __init__(
        self,
        token: str,
        server_type: str = "REAL",
        token_refresher: Optional[Callable[[], str]] = None,
    ) -> None:
        self._token = token
        self._token_refresher = token_refresher  # callable that returns fresh token
        self._ws_url = WS_URL_REAL if server_type == "REAL" else WS_URL_MOCK
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._connected = False
        self._authenticated = False
        self._shutting_down = False

        # Subscriptions tracking
        self._subscribed_codes: Dict[str, Set[str]] = {}  # type -> set of codes
        # Owner-aware subscription refcount: (ws_type, code) -> set(owner_key)
        self._sub_owners: Dict[tuple, Set[str]] = {}

        # Callbacks — list-based event bus for multiple consumers
        self._price_tick_listeners: List[Callable] = []   # [(code, values_dict)]
        self._order_exec_listeners: List[Callable] = []   # [(values_dict)]
        self._balance_listeners: List[Callable] = []      # [(values_dict)]
        # Legacy single callback compat
        self._on_price_tick: Optional[Callable] = None
        self._on_order_exec: Optional[Callable] = None
        self._on_balance_update: Optional[Callable] = None

        # Asyncio event loop in separate thread
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._reconnect_count = 0
        self._permanently_down = False

    # ── Public API ────────────────────────────────────────────

    def start(self) -> None:
        """Start WebSocket connection in background thread."""
        if self._thread and self._thread.is_alive():
            logger.warning("[WS] Already running")
            return

        self._shutting_down = False
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info(f"[WS] Started background thread → {self._ws_url}")

    def stop(self) -> None:
        """Stop WebSocket and background thread."""
        self._shutting_down = True
        if self._ws and self._loop and self._loop.is_running():
            # Close WebSocket gracefully before stopping loop
            async def _close():
                try:
                    if self._ws:
                        await self._ws.close()
                except Exception:
                    pass
            try:
                future = asyncio.run_coroutine_threadsafe(_close(), self._loop)
                future.result(timeout=3)
            except Exception:
                pass
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread:
            self._thread.join(timeout=5)
        self._connected = False
        self._ws = None
        logger.info("[WS] Stopped")

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def authenticated(self) -> bool:
        return self._authenticated

    @property
    def permanently_down(self) -> bool:
        """True if MAX_RECONNECT exceeded — REST fallback mode."""
        return self._permanently_down

    def subscribe(self, codes: List[str], ws_type: str = "0B",
                  owner_key: str = "") -> None:
        """Subscribe to real-time data for given codes and type.

        owner_key: if provided, enables refcount-based ownership.
        Actual WS REMOVE only fires when last owner unsubscribes a code.
        """
        # Track ownership
        if owner_key:
            for code in codes:
                self._sub_owners.setdefault((ws_type, code), set()).add(owner_key)

        # Always track for resubscribe after auth
        self._subscribed_codes.setdefault(ws_type, set()).update(codes)

        if not self._loop or not self._connected or not self._authenticated:
            logger.info(f"[WS] Queued subscription: {ws_type} {len(codes)} codes (auth={self._authenticated})")
            return

        asyncio.run_coroutine_threadsafe(
            self._send_subscribe(codes, ws_type, "REG"), self._loop
        )

    def unsubscribe(self, codes: List[str], ws_type: str = "0B",
                    owner_key: str = "") -> None:
        """Unsubscribe from real-time data.

        owner_key: if provided, only removes this owner's claim.
        Actual WS REMOVE only fires for codes with no remaining owners.
        """
        if owner_key:
            actually_remove = []
            for code in codes:
                key = (ws_type, code)
                owners = self._sub_owners.get(key)
                if owners:
                    owners.discard(owner_key)
                    if not owners:
                        actually_remove.append(code)
                        self._sub_owners.pop(key, None)
                else:
                    actually_remove.append(code)
        else:
            actually_remove = list(codes)

        if not actually_remove:
            return

        tracked = self._subscribed_codes.get(ws_type, set())
        tracked -= set(actually_remove)

        if self._loop and self._connected:
            asyncio.run_coroutine_threadsafe(
                self._send_subscribe(actually_remove, ws_type, "REMOVE"), self._loop
            )

    def unsubscribe_all(self) -> None:
        """Unsubscribe all types and codes. WS will stop reconnecting."""
        for ws_type, codes in self._subscribed_codes.items():
            if codes and self._loop and self._connected:
                asyncio.run_coroutine_threadsafe(
                    self._send_subscribe(list(codes), ws_type, "REMOVE"), self._loop
                )
        self._subscribed_codes.clear()
        self._authenticated = False

    # Callback setters (legacy — single callback compat)
    def set_on_price_tick(self, cb: Optional[Callable]) -> None:
        self._on_price_tick = cb

    def set_on_order_exec(self, cb: Optional[Callable]) -> None:
        self._on_order_exec = cb

    def set_on_balance_update(self, cb: Optional[Callable]) -> None:
        self._on_balance_update = cb

    # Event bus — multiple listeners
    def add_price_listener(self, cb: Callable, key: str = "") -> None:
        """Add a price tick listener. key for later removal."""
        self._price_tick_listeners.append((key, cb))

    def remove_price_listener(self, key: str) -> None:
        """Remove listener by key."""
        self._price_tick_listeners = [(k, cb) for k, cb in self._price_tick_listeners if k != key]

    def add_order_listener(self, cb: Callable, key: str = "") -> None:
        self._order_exec_listeners.append((key, cb))

    def remove_order_listener(self, key: str) -> None:
        self._order_exec_listeners = [(k, cb) for k, cb in self._order_exec_listeners if k != key]

    # ── Asyncio Event Loop ────────────────────────────────────

    def _run_loop(self) -> None:
        """Run asyncio event loop in background thread."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._connect_loop())
        except Exception as e:
            if not self._shutting_down:
                logger.error(f"[WS] Event loop error: {e}")
        finally:
            self._loop.close()
            self._loop = None

    async def _connect_loop(self) -> None:
        """Connect and reconnect loop."""
        while not self._shutting_down:
            try:
                await self._connect_and_listen()
            except Exception as e:
                if self._shutting_down:
                    break
                logger.warning(f"[WS] Exception disconnect: {type(e).__name__}: {e}")

            # Common reconnect logic for both normal close and exception
            self._connected = False
            self._authenticated = False

            if self._shutting_down:
                break

            # Don't reconnect if no subscriptions (idle WS)
            total_subs = sum(len(v) for v in self._subscribed_codes.values())
            if total_subs == 0:
                logger.info("[WS] No subscriptions, stopping reconnect loop")
                break

            self._reconnect_count += 1
            # Market-hours-aware budget (Jeff 2026-04-29). KST 08:00~16:00
            # gets a generous retry budget (MAX_RECONNECT_MARKET_HOURS,
            # ~5 min of attempts) so a transient API hiccup doesn't lose
            # the trading day. Off hours fall back to the original tight
            # budget — there's no liquidity to chase after 16:00 KST.
            try:
                from datetime import datetime as _dt
                from zoneinfo import ZoneInfo
                _kst_hour = _dt.now(ZoneInfo("Asia/Seoul")).hour
                _within_market = 8 <= _kst_hour < 16
            except Exception:
                _within_market = False  # safe default — tighter budget
            budget = (
                MAX_RECONNECT_MARKET_HOURS if _within_market
                else MAX_RECONNECT_OFF_HOURS
            )
            if self._reconnect_count > budget:
                logger.error(
                    f"[WS_FALLBACK_REST] Max reconnect ({budget}) exceeded "
                    f"(market_hours={_within_market}). "
                    f"WS permanently down - switching to REST fallback mode."
                )
                self._permanently_down = True
                break
            logger.warning(
                f"[WS] Reconnecting {self._reconnect_count}/{budget} "
                f"in {RECONNECT_DELAY}s (market_hours={_within_market})..."
            )
            await asyncio.sleep(RECONNECT_DELAY)

    async def _connect_and_listen(self) -> None:
        """Single connection session."""
        # Refresh token on reconnect
        if self._token_refresher and self._reconnect_count > 0:
            try:
                self._token = self._token_refresher()
                logger.info("[WS] Token refreshed for reconnect")
            except Exception as e:
                logger.warning(f"[WS] Token refresh failed: {e}")

        extra_headers = {
            "authorization": f"Bearer {self._token}",
        }
        async with websockets.connect(
            self._ws_url,
            additional_headers=extra_headers,
            ping_interval=None,   # disable library ping — Kiwoom uses app-level PING
            ping_timeout=None,
            close_timeout=5,
        ) as ws:
            self._ws = ws
            self._connected = True
            logger.info(f"[WS_CONNECTED] {self._ws_url} (reconnect={self._reconnect_count})")

            # Kiwoom REST WS requires explicit LOGIN message
            login_msg = json.dumps({"trnm": "LOGIN", "token": self._token})
            await ws.send(login_msg)
            logger.info("[WS_LOGIN] Sent LOGIN packet")

            # Wait for LOGIN response
            try:
                login_resp = await asyncio.wait_for(ws.recv(), timeout=10.0)
                login_data = json.loads(login_resp)
                rc = login_data.get("return_code", -1)
                logger.info(f"[WS_LOGIN] Response: rc={rc} msg={login_data.get('return_msg', '')[:80]}")
                if rc != 0:
                    logger.error(f"[WS_LOGIN] Failed: rc={rc}")
                    return
            except asyncio.TimeoutError:
                logger.warning("[WS_LOGIN] No response within 10s, proceeding anyway")
            except Exception as e:
                logger.warning(f"[WS_LOGIN] Error: {e}")

            self._authenticated = True
            logger.info("[WS_AUTH] Authenticated")

            # Re-subscribe existing subscriptions
            await self._resubscribe_all()

            # Listen for messages
            msg_count = 0
            async for raw_msg in ws:
                if self._shutting_down:
                    break
                msg_count += 1
                try:
                    msg = json.loads(raw_msg)
                    self._dispatch(msg)
                except json.JSONDecodeError:
                    logger.warning(f"[WS] Non-JSON message: {str(raw_msg)[:100]}")
                except Exception as e:
                    logger.error(f"[WS] Message handling error: {e}")

            # async for exited = server closed connection
            if msg_count > 0:
                self._reconnect_count = 0  # got data → healthy, reset counter
            close_code = getattr(ws, 'close_code', None)
            close_reason = getattr(ws, 'close_reason', '') or ''
            logger.warning(
                f"[WS_CLOSED] Server closed after {msg_count} msgs, "
                f"code={close_code} reason={close_reason[:80]}"
            )

    # ── Auth Wait ─────────────────────────────────────────────

    async def _wait_for_auth(self, ws, timeout: float = 10.0) -> bool:
        """Wait for Kiwoom WS auth confirmation before subscribing.
        Returns True if auth confirmed, False on timeout/reject."""
        self._authenticated = False
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            msg = json.loads(raw)
            rc = msg.get("return_code", -1)
            trnm = msg.get("trnm", "")
            if rc == 0:
                logger.info(f"[WS_AUTH] OK (trnm={trnm})")
                self._authenticated = True
                return True
            else:
                logger.warning(f"[WS_AUTH] rc={rc} msg={msg.get('return_msg', '')[:80]}")
                if rc == 100013:
                    return False
                self._authenticated = True
                return True
        except asyncio.TimeoutError:
            logger.warning("[WS_AUTH] Timeout waiting for auth response")
            self._authenticated = True  # 타임아웃이어도 구독 시도
            return True
        except Exception as e:
            logger.warning(f"[WS_AUTH] Error: {e}")
            return False

    # ── Subscription ──────────────────────────────────────────

    async def _send_subscribe(
        self, codes: List[str], ws_type: str, action: str = "REG"
    ) -> None:
        """Send subscription/unsubscription message."""
        if not self._ws:
            return

        msg = {
            "trnm": action,
            "grp_no": "1",
            "refresh": "1",
            "data": [{"item": codes, "type": [ws_type]}],
        }
        try:
            await self._ws.send(json.dumps(msg))
            logger.info(
                f"[WS_{action}] type={ws_type} codes={len(codes)} "
                f"(first={codes[0] if codes else 'empty'})"
            )
        except ConnectionClosed:
            logger.warning(f"[WS] Connection closed during {action}")

    async def _resubscribe_all(self) -> None:
        """Re-subscribe all tracked subscriptions after reconnect."""
        for ws_type, codes in self._subscribed_codes.items():
            if codes:
                await self._send_subscribe(list(codes), ws_type, "REG")
                logger.info(f"[WS_RESUB] type={ws_type} codes={len(codes)}")

    # ── Message Dispatch ──────────────────────────────────────

    def _dispatch(self, msg: dict) -> None:
        """Route incoming WebSocket message to appropriate callback.

        SAFETY: Callbacks must NEVER place synchronous orders.
        WebSocket thread != main thread. Order placement from here
        causes race conditions with RECON and portfolio state.
        """
        trnm = msg.get("trnm", "")

        # PING echo — Kiwoom requires app-level PING response
        if trnm == "PING":
            if self._ws and self._loop:
                asyncio.run_coroutine_threadsafe(
                    self._ws.send(json.dumps(msg)), self._loop
                )
            return

        # REG/REMOVE responses — log only (100013 = stale auth reject, safe to ignore)
        if trnm in ("REG", "REMOVE"):
            rc = msg.get("return_code", -1)
            if rc == 100013:
                logger.debug(f"[WS_{trnm}] rc=100013 (stale auth reject, ignored)")
            else:
                logger.info(f"[WS_{trnm}] rc={rc} msg={msg.get('return_msg', '')[:60]}")
            return

        # Log unrecognized message types (first 3 only)
        if trnm != "REAL":
            if not hasattr(self, '_unk_count'):
                self._unk_count = 0
            self._unk_count += 1
            if self._unk_count <= 5:
                logger.info(f"[WS_MSG] trnm={trnm} keys={list(msg.keys())[:5]} rc={msg.get('return_code', 'N/A')}")
            return

        for data_item in msg.get("data", []):
            ws_type = data_item.get("type", "")
            item = data_item.get("item", "")
            values = data_item.get("values", {})

            if ws_type == "0B":
                code = item.replace("A", "")
                n_listeners = len(self._price_tick_listeners)
                if n_listeners > 0 and not hasattr(self, '_0b_log_count'):
                    self._0b_log_count = 0
                if n_listeners > 0:
                    self._0b_log_count = getattr(self, '_0b_log_count', 0) + 1
                    if self._0b_log_count <= 3:
                        logger.info(f"[WS_0B] code={code} listeners={n_listeners} legacy={self._on_price_tick is not None}")
                # Legacy single callback
                if self._on_price_tick:
                    try:
                        self._on_price_tick(code, values)
                    except Exception as _e:
                        logger.debug(f"[WS] price_tick callback error: {_e}")
                # Event bus listeners (P1-1: snapshot before iteration)
                for _key, _cb in list(self._price_tick_listeners):
                    try:
                        _cb(code, values)
                    except Exception as _e:
                        logger.debug(f"[WS] price listener '{_key}' error: {_e}")

            elif ws_type == "00":
                if self._on_order_exec:
                    try:
                        self._on_order_exec(values)
                    except Exception:
                        pass
                for _key, _cb in list(self._order_exec_listeners):
                    try:
                        _cb(values)
                    except Exception:
                        pass

            elif ws_type == "04":
                if self._on_balance_update:
                    try:
                        self._on_balance_update(values)
                    except Exception:
                        pass
                for _key, _cb in self._balance_listeners:
                    try:
                        _cb(values)
                    except Exception:
                        pass
