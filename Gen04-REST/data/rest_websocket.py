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

MAX_RECONNECT = 5
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
        self._shutting_down = False

        # Subscriptions tracking
        self._subscribed_codes: Dict[str, Set[str]] = {}  # type -> set of codes

        # Callbacks
        self._on_price_tick: Optional[Callable] = None  # (code, values_dict)
        self._on_order_exec: Optional[Callable] = None  # (values_dict)
        self._on_balance_update: Optional[Callable] = None  # (values_dict)

        # Asyncio event loop in separate thread
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._reconnect_count = 0

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

    def subscribe(self, codes: List[str], ws_type: str = "0B") -> None:
        """Subscribe to real-time data for given codes and type."""
        if not self._loop or not self._connected:
            logger.warning(f"[WS] Not connected, queuing subscription: {ws_type} {len(codes)} codes")
            # Store for re-subscription on connect
            self._subscribed_codes.setdefault(ws_type, set()).update(codes)
            return

        self._subscribed_codes.setdefault(ws_type, set()).update(codes)
        asyncio.run_coroutine_threadsafe(
            self._send_subscribe(codes, ws_type, "REG"), self._loop
        )

    def unsubscribe(self, codes: List[str], ws_type: str = "0B") -> None:
        """Unsubscribe from real-time data."""
        tracked = self._subscribed_codes.get(ws_type, set())
        tracked -= set(codes)

        if self._loop and self._connected:
            asyncio.run_coroutine_threadsafe(
                self._send_subscribe(codes, ws_type, "REMOVE"), self._loop
            )

    def unsubscribe_all(self) -> None:
        """Unsubscribe all types and codes."""
        for ws_type, codes in self._subscribed_codes.items():
            if codes and self._loop and self._connected:
                asyncio.run_coroutine_threadsafe(
                    self._send_subscribe(list(codes), ws_type, "REMOVE"), self._loop
                )
        self._subscribed_codes.clear()

    # Callback setters
    def set_on_price_tick(self, cb: Optional[Callable]) -> None:
        self._on_price_tick = cb

    def set_on_order_exec(self, cb: Optional[Callable]) -> None:
        self._on_order_exec = cb

    def set_on_balance_update(self, cb: Optional[Callable]) -> None:
        self._on_balance_update = cb

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
                self._connected = False
                self._reconnect_count += 1
                if self._reconnect_count > MAX_RECONNECT:
                    logger.error(f"[WS] Max reconnect ({MAX_RECONNECT}) exceeded. Giving up.")
                    break
                logger.warning(
                    f"[WS] Disconnected ({e}), reconnect {self._reconnect_count}/{MAX_RECONNECT} "
                    f"in {RECONNECT_DELAY}s..."
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
            ping_interval=PING_INTERVAL,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            self._ws = ws
            self._connected = True
            self._reconnect_count = 0
            logger.info(f"[WS_CONNECTED] {self._ws_url}")

            # Re-subscribe existing subscriptions
            await self._resubscribe_all()

            # Listen for messages
            async for raw_msg in ws:
                if self._shutting_down:
                    break
                try:
                    msg = json.loads(raw_msg)
                    self._dispatch(msg)
                except json.JSONDecodeError:
                    logger.warning(f"[WS] Non-JSON message: {str(raw_msg)[:100]}")
                except Exception as e:
                    logger.error(f"[WS] Message handling error: {e}")

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

        # REG/REMOVE responses — log only
        if trnm in ("REG", "REMOVE"):
            rc = msg.get("return_code", -1)
            logger.info(f"[WS_{trnm}] rc={rc} msg={msg.get('return_msg', '')[:60]}")
            return

        # REAL data messages
        if trnm != "REAL":
            return

        for data_item in msg.get("data", []):
            ws_type = data_item.get("type", "")
            item = data_item.get("item", "")
            values = data_item.get("values", {})

            if ws_type == "0B" and self._on_price_tick:
                code = item.replace("A", "")
                self._on_price_tick(code, values)

            elif ws_type == "00" and self._on_order_exec:
                self._on_order_exec(values)

            elif ws_type == "04" and self._on_balance_update:
                self._on_balance_update(values)
