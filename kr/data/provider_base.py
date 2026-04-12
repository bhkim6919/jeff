"""
provider_base.py — BrokerProvider Abstract Base Class
=====================================================
Gen4 브로커 프로바이더의 공통 인터페이스 정의.
COM(Kiwoom OpenAPI+)과 REST(Kiwoom REST API) 모두 이 ABC를 구현.

Phase 0: ABC 정의만. kr-legacy/ 코드 변경 없음.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Optional


class BrokerProvider(ABC):
    """Abstract broker provider interface for Q-TRON Gen4."""

    # ── Lifecycle ─────────────────────────────────────────────

    @abstractmethod
    def shutdown(self) -> None:
        """Graceful shutdown. Block callbacks, release resources."""

    @property
    @abstractmethod
    def alive(self) -> bool:
        """True if provider is operational."""

    @abstractmethod
    def is_connected(self) -> bool:
        """Check broker connection status."""

    @abstractmethod
    def ensure_connected(self) -> bool:
        """Reconnect if disconnected. Returns True if connected."""

    # ── Identity ──────────────────────────────────────────────

    @property
    @abstractmethod
    def server_type(self) -> str:
        """'REAL' or 'MOCK' — set at construction, immutable."""

    # ── Stock Information ─────────────────────────────────────

    @abstractmethod
    def get_stock_info(self, code: str) -> dict:
        """Return {name, sector, market, market_cap, listed_shares}."""

    @abstractmethod
    def get_current_price(self, code: str) -> float:
        """Current price for a single stock. 0.0 on failure."""

    # ── Account Queries ───────────────────────────────────────

    @abstractmethod
    def query_account_holdings(self) -> List[Dict]:
        """Return [{code, name, qty, quantity, avg_price, cur_price, pnl}, ...]."""

    @abstractmethod
    def query_account_summary(self) -> Dict:
        """Return {추정예탁자산, 총매입금액, 총평가금액, 총평가손익금액,
        holdings: [...], available_cash, error, holdings_reliable}."""

    @abstractmethod
    def query_sellable_qty(self, code: str) -> Dict:
        """Return {code, hold_qty, sellable_qty, source, error}."""

    # ── Order Execution ───────────────────────────────────────

    @abstractmethod
    def send_order(
        self,
        code: str,
        side: str,
        quantity: int,
        price: int = 0,
        hoga_type: str = "03",
    ) -> Dict:
        """Send order. Returns {order_no, exec_price, exec_qty, error}
        or timeout/partial status."""

    @abstractmethod
    def query_open_orders(self) -> Optional[List[Dict]]:
        """Return [{order_no, code, side, qty, filled_qty, remaining, ...}]
        or None on failure."""

    @abstractmethod
    def cancel_order(
        self, code: str, order_no: str, qty: int, side: str = "BUY"
    ) -> Dict:
        """Cancel specific order. Returns {ok: bool, error: str}."""

    @abstractmethod
    def cancel_all_open_orders(self) -> Optional[int]:
        """Cancel all open orders. Returns count cancelled or None."""

    # ── Ghost Order Management ────────────────────────────────

    @abstractmethod
    def set_ghost_fill_callback(self, callback: Optional[Callable]) -> None:
        """Register callback for delayed ghost fills."""

    @abstractmethod
    def get_ghost_orders(self) -> List[Dict]:
        """Return unresolved ghost/timeout orders."""

    @abstractmethod
    def clear_ghost_orders(self) -> None:
        """Clear ghost order list."""

    @property
    def ghost_orders_raw(self) -> List[Dict]:
        """Direct access to ghost orders (replaces _ghost_orders access)."""
        return self.get_ghost_orders()

    # ── Real-time Data ────────────────────────────────────────

    @abstractmethod
    def register_real(self, codes: List[str], fids: str = "10;27") -> None:
        """Register codes for real-time tick updates."""

    @abstractmethod
    def unregister_real(self) -> None:
        """Unregister all real-time feeds."""

    @abstractmethod
    def register_real_append(
        self, codes: List[str], fids: str = "10;27", screen: Optional[str] = None
    ) -> int:
        """Add codes to real-time feed (append mode). Returns count."""

    @abstractmethod
    def unregister_real_screen(self, screen: str) -> None:
        """Unregister feeds on specific screen."""

    @abstractmethod
    def set_real_data_callback(self, callback: Optional[Callable]) -> None:
        """Register callback(code, price, volume) for real-time ticks."""

    @abstractmethod
    def set_micro_callback(self, callback: Optional[Callable]) -> None:
        """Register callback(code, fid_data_dict) for microstructure data."""

    # ── Index Data ────────────────────────────────────────────

    @abstractmethod
    def get_kospi_close(self, trade_date: str = "") -> float:
        """KOSPI closing value. 0.0 on failure."""

    @abstractmethod
    def get_kosdaq_close(self, trade_date: str = "") -> float:
        """KOSDAQ closing value. 0.0 on failure."""

    @abstractmethod
    def get_index_minute_bars(
        self,
        index_code: str = "001",
        trade_date: str = "",
        tick_range: int = 1,
    ) -> List[dict]:
        """Return [{time, datetime, open, high, low, close, volume}, ...]."""
