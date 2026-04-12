# -*- coding: utf-8 -*-
"""
risk_guard.py -- Daily Risk Guard (lock-free)
===============================================
일일 진입 제한, 동시 보유 제한, 연속 손실 중지.
내부 lock 없음 — engine._lock이 보호.
"""
from __future__ import annotations

from typing import Dict, Tuple

from web.surge.config import SurgeConfig


class RiskGuard:
    """
    Stateful risk guard — tracks daily counters.
    All methods must be called under engine._lock.
    """

    def __init__(self):
        self._daily_entry_count: int = 0
        self._concurrent_positions: int = 0
        self._per_stock_loss_count: Dict[str, int] = {}
        self._consecutive_losses: int = 0
        self._halted: bool = False

    def can_enter(self, code: str, config: SurgeConfig) -> Tuple[bool, str]:
        """Check all risk limits. Returns (allowed, block_reason)."""
        if self._halted:
            return False, "HALT_CONSECUTIVE_LOSS"

        if self._daily_entry_count >= config.max_daily_entries:
            return False, f"DAILY_LIMIT({self._daily_entry_count}/{config.max_daily_entries})"

        if self._concurrent_positions >= config.max_concurrent:
            return False, f"CONCURRENT_LIMIT({self._concurrent_positions}/{config.max_concurrent})"

        stock_losses = self._per_stock_loss_count.get(code, 0)
        if stock_losses >= config.max_loss_per_stock:
            return False, f"STOCK_LOSS_LIMIT({code}={stock_losses}/{config.max_loss_per_stock})"

        return True, "OK"

    def on_entry(self, code: str) -> None:
        self._daily_entry_count += 1
        self._concurrent_positions += 1

    def on_exit(self, code: str, is_win: bool, config: SurgeConfig) -> None:
        self._concurrent_positions = max(0, self._concurrent_positions - 1)
        if is_win:
            self._consecutive_losses = 0
        else:
            self._consecutive_losses += 1
            self._per_stock_loss_count[code] = self._per_stock_loss_count.get(code, 0) + 1
            if self._consecutive_losses >= config.consecutive_loss_halt:
                self._halted = True

    def reset_daily(self) -> None:
        self._daily_entry_count = 0
        self._per_stock_loss_count.clear()
        self._consecutive_losses = 0
        self._halted = False
        # concurrent_positions는 리셋하지 않음 (실제 포지션 기반)

    def get_state(self) -> dict:
        return {
            "daily_entries": self._daily_entry_count,
            "concurrent": self._concurrent_positions,
            "consecutive_losses": self._consecutive_losses,
            "halted": self._halted,
            "per_stock_losses": dict(self._per_stock_loss_count),
        }
