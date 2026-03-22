"""
trail_stop.py — Trailing stop calculation
==========================================
Simple percentage trailing from entry.
  high_wm = max(high_wm, today_high)
  if today_low <= high_wm * (1 - TRAIL_PCT): EXIT at trail price

Matches backtest_gen4_core.py lines 249-266 exactly.
"""
from __future__ import annotations
from typing import Tuple


def check_trail_stop(high_watermark: float,
                     today_close: float,
                     trail_pct: float = 0.12) -> Tuple[bool, float, float]:
    """
    Check if trailing stop is triggered (close-based).

    Uses close price for both HWM update and trigger check.
    This matches the validated backtest (validate_gen4.py).

    Args:
        high_watermark: Current high watermark for the position.
        today_close: Today's close price.
        trail_pct: Trailing stop percentage (0.12 = -12%).

    Returns:
        (triggered, new_high_watermark, exit_price)
        - triggered: True if stop was hit
        - new_high_watermark: Updated HWM (always >= input)
        - exit_price: Close price if triggered, 0.0 otherwise
    """
    # Update high watermark with close price
    new_hwm = max(high_watermark, today_close)

    # Check drawdown from peak
    if new_hwm > 0:
        dd = (today_close - new_hwm) / new_hwm
        if dd <= -trail_pct:
            return True, new_hwm, today_close

    return False, new_hwm, 0.0


def update_high_watermarks(positions: dict, highs: dict) -> None:
    """
    Batch update high watermarks for all positions (in-place).

    Args:
        positions: {ticker: position_dict} where each has 'high_watermark' key.
        highs: {ticker: today_high_price}
    """
    for ticker, pos in positions.items():
        h = highs.get(ticker, 0.0)
        if h > pos.get("high_watermark", 0.0):
            pos["high_watermark"] = h


def calc_trail_stop_price(high_watermark: float, trail_pct: float = 0.12) -> float:
    """Calculate current trail stop price level."""
    return high_watermark * (1.0 - trail_pct)
