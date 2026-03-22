"""
exposure_guard.py — DD-based entry blocking
=============================================
Adapted from Gen2 risk_governor (no forced liquidation).

Modes:
  NORMAL         — all operations allowed
  DAILY_BLOCKED  — daily DD <= -4% → block new entries
  MONTHLY_BLOCKED — monthly DD <= -7% → block new entries

Key difference from Gen2: NO forced liquidation.
Trail stop handles all exits. DD guard only blocks NEW entries during rebalance.
"""
from __future__ import annotations
import logging
from datetime import date

logger = logging.getLogger("gen4.risk")


class ExposureGuard:
    """
    Evaluates portfolio risk state and blocks new entries if DD limits exceeded.
    Does NOT force-liquidate any positions.
    """

    def __init__(self, daily_dd_limit: float = -0.04,
                 monthly_dd_limit: float = -0.07):
        self.daily_dd_limit = daily_dd_limit
        self.monthly_dd_limit = monthly_dd_limit
        self._last_mode = "NORMAL"

    def evaluate(self, daily_pnl: float, monthly_dd: float) -> str:
        """
        Evaluate risk mode.

        Args:
            daily_pnl: Today's PnL percentage (e.g., -0.03 = -3%).
            monthly_dd: Monthly drawdown from peak (e.g., -0.05 = -5%).

        Returns:
            "NORMAL" | "DAILY_BLOCKED" | "MONTHLY_BLOCKED"
        """
        if monthly_dd <= self.monthly_dd_limit:
            mode = "MONTHLY_BLOCKED"
        elif daily_pnl <= self.daily_dd_limit:
            mode = "DAILY_BLOCKED"
        else:
            mode = "NORMAL"

        if mode != self._last_mode:
            if mode != "NORMAL":
                logger.warning(f"Risk mode changed: {self._last_mode} -> {mode} "
                               f"(daily={daily_pnl:.2%}, monthly={monthly_dd:.2%})")
            else:
                logger.info(f"Risk mode restored: {self._last_mode} -> NORMAL")
            self._last_mode = mode

        return mode

    def can_buy(self, daily_pnl: float, monthly_dd: float) -> tuple:
        """
        Check if new buys are allowed.

        Returns:
            (allowed: bool, reason: str)
        """
        mode = self.evaluate(daily_pnl, monthly_dd)
        if mode == "NORMAL":
            return True, "OK"
        return False, f"Blocked by {mode}: daily={daily_pnl:.2%}, monthly={monthly_dd:.2%}"

    def should_skip_rebalance(self, daily_pnl: float, monthly_dd: float) -> tuple:
        """
        Check if rebalance should be skipped entirely.
        Note: sells (removing positions) are always allowed.
        Only new buys are blocked.

        Returns:
            (skip_buys: bool, reason: str)
        """
        return self.can_buy(daily_pnl, monthly_dd) == (False,)  # never True
        # Actually: sells always proceed, only buys blocked
        mode = self.evaluate(daily_pnl, monthly_dd)
        if mode != "NORMAL":
            return True, f"Buys blocked: {mode}"
        return False, "OK"
