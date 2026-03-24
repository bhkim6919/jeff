"""
exposure_guard.py — DD-based entry blocking + graduated response
=================================================================
Adapted from Gen2 risk_governor.

Modes (legacy):
  NORMAL         — all operations allowed
  DAILY_BLOCKED  — daily DD <= -4% → block new entries
  MONTHLY_BLOCKED — monthly DD <= -7% → block new entries

DD Graduated Levels (STEP 5):
  DD_CAUTION     — -5%  → buy 70%
  DD_WARNING     — -10% → buy 50%
  DD_CRITICAL    — -15% → buy 0%
  DD_SEVERE      — -20% → buy 0% + trim 20%
  DD_SAFE_MODE   — -25% → buy 0% + trim 50%

SAFE MODE release: DD >= -20% (configurable)
"""
from __future__ import annotations
import logging
from datetime import date
from typing import Dict

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

        # STEP 5: trim + safe mode state tracking
        self._last_trim_date: str = ""       # "YYYY-MM-DD" of last trim
        self._last_trim_level: str = ""      # level at last trim
        self._safe_mode_active: bool = False
        self._safe_mode_entered_date: str = ""
        self._safe_mode_reason: str = ""     # reason for force_safe_mode

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
        # Force safe mode blocks all buys (RECON excess, etc.)
        if self._safe_mode_active:
            return False, f"Blocked by SAFE_MODE: {self._safe_mode_reason}"
        mode = self.evaluate(daily_pnl, monthly_dd)
        if mode == "NORMAL":
            return True, "OK"
        return False, f"Blocked by {mode}: daily={daily_pnl:.2%}, monthly={monthly_dd:.2%}"

    def should_skip_rebalance(self, daily_pnl: float, monthly_dd: float) -> tuple:
        """
        Check if rebalance buys should be skipped (legacy, bool return).
        Note: sells (removing positions) are ALWAYS allowed.
        Only new buys are blocked when DD limits exceeded.

        Returns:
            (skip_buys: bool, reason: str)
        """
        allowed, reason = self.can_buy(daily_pnl, monthly_dd)
        if not allowed:
            return True, f"Buys blocked: {reason}"
        return False, "OK"

    # ── DD Graduated Response (STEP 5) ────────────────────────────

    def get_risk_action(self, daily_pnl: float, monthly_dd: float,
                        dd_levels: tuple = None,
                        safe_mode_release: float = -0.20) -> Dict:
        """
        Graduated DD response. Returns action dict.

        Args:
            daily_pnl: today's PnL (e.g., -0.03)
            monthly_dd: monthly drawdown from peak (e.g., -0.12)
            dd_levels: tuple of (threshold, buy_scale, trim_ratio, label)
                       ordered most severe first
            safe_mode_release: DD above which SAFE MODE is released

        Returns:
            {
                "buy_scale": 0.0~1.0,
                "trim_ratio": 0.0~1.0,
                "level": str,
                "safe_mode": bool,
                "reason": str,
            }
        """
        # Default levels if not provided
        if dd_levels is None:
            dd_levels = (
                (-0.25, 0.00, 0.50, "DD_SAFE_MODE"),
                (-0.20, 0.00, 0.20, "DD_SEVERE"),
                (-0.15, 0.00, 0.00, "DD_CRITICAL"),
                (-0.10, 0.50, 0.00, "DD_WARNING"),
                (-0.05, 0.70, 0.00, "DD_CAUTION"),
            )

        # Evaluate monthly DD against graduated levels
        level = "NORMAL"
        buy_scale = 1.0
        trim_ratio = 0.0
        safe_mode = False

        for threshold, scale, trim, label in dd_levels:
            if monthly_dd <= threshold:
                level = label
                buy_scale = scale
                trim_ratio = trim
                safe_mode = ("SAFE_MODE" in label)
                break  # most severe first, stop at first match

        # SAFE MODE hysteresis: separate entry/exit thresholds
        today_str = str(date.today())

        if self._safe_mode_active:
            # Already in safe mode — apply hysteresis for release
            if monthly_dd > safe_mode_release:
                # DD recovered past release threshold
                if self._safe_mode_entered_date == today_str:
                    # Same-day release blocked (anti-flapping)
                    safe_mode = True
                    logger.info("[DD_SAFE_MODE_HELD] same-day release blocked "
                                "(entered today, DD=%.2f%%)", monthly_dd * 100)
                else:
                    self._safe_mode_active = False
                    safe_mode = False
                    logger.info("[DD_SAFE_MODE_RELEASE] DD=%.2f%% > release=%.2f%%",
                                monthly_dd * 100, safe_mode_release * 100)
            else:
                # Still below release threshold — stay in safe mode
                safe_mode = True
        elif safe_mode:
            # New safe mode entry (from DD_LEVELS evaluation)
            self._safe_mode_active = True
            self._safe_mode_entered_date = today_str
            logger.critical("[DD_SAFE_MODE_ENTERED] DD=%.2f%%", monthly_dd * 100)

        # Trim dedup: max 1 trim per day, skip if same level already trimmed
        if trim_ratio > 0:
            if self._last_trim_date == today_str:
                logger.info("[DD_TRIM_SKIPPED] already trimmed today "
                            "(level=%s, prev=%s)", level, self._last_trim_level)
                trim_ratio = 0.0  # suppress duplicate trim

        # Legacy daily guard (override if worse)
        if daily_pnl <= self.daily_dd_limit and buy_scale > 0:
            buy_scale = 0.0
            if level == "NORMAL":
                level = "DAILY_BLOCKED"

        # Note: when DD_LEVELS is active, graduated levels supersede
        # the legacy MONTHLY_DD_LIMIT. Legacy daily guard still applies above.

        reason = (f"{level} (daily={daily_pnl:.2%}, monthly_dd={monthly_dd:.2%}, "
                  f"buy_scale={buy_scale:.0%}, trim={trim_ratio:.0%})")

        # Log transitions
        if level != self._last_mode:
            if level != "NORMAL":
                logger.warning(f"[DD_GUARD] {reason}")
            else:
                logger.info(f"[DD_GUARD] restored to NORMAL")
            self._last_mode = level

        return {
            "buy_scale": buy_scale,
            "trim_ratio": trim_ratio,
            "level": level,
            "safe_mode": safe_mode,
            "reason": reason,
        }

    def mark_trim_executed(self, level: str) -> None:
        """Call after _execute_dd_trim() to prevent same-day repeat."""
        self._last_trim_date = str(date.today())
        self._last_trim_level = level
        logger.info(f"[DD_TRIM_MARKED] level={level} date={self._last_trim_date}")

    def force_safe_mode(self, reason: str = "") -> None:
        """Force SAFE MODE from external trigger (e.g., RECON excess).

        Idempotent — calling multiple times with different reasons appends.
        Behavior:
          a. New buys blocked (can_buy → False, get_risk_action buy_scale=0)
          b. Rebalance buys skipped
          c. Trail stop / protective sells ALLOWED
          d. Reason logged as CRITICAL
        """
        if self._safe_mode_active and self._safe_mode_reason:
            # Already active — append new reason
            if reason and reason not in self._safe_mode_reason:
                self._safe_mode_reason += f"; {reason}"
                logger.critical(f"[SAFE_MODE_FORCE] additional reason: {reason}")
            return
        self._safe_mode_active = True
        self._safe_mode_entered_date = str(date.today())
        self._safe_mode_reason = reason
        self._last_mode = "RECON_SAFE_MODE"
        logger.critical(f"[SAFE_MODE_FORCE] {reason}")

    @property
    def safe_mode_reason(self) -> str:
        return getattr(self, "_safe_mode_reason", "")
