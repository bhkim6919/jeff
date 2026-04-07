"""
trail_stop.py — Trailing stop calculation
==========================================
Simple percentage trailing from entry.
  high_wm = max(high_wm, today_close)
  if today_close <= high_wm * (1 - TRAIL_PCT): EXIT at close price

Matches backtest_gen4_core.py lines 249-266 exactly.
Close-based: HWM updated with close, trigger checked on close.
"""
from __future__ import annotations

import logging
import math
from typing import Tuple

logger = logging.getLogger("gen4.trail_stop")

# Price sanity bounds (Korean market)
# ── Sanity-check bounds (Korean equity market) ──────────────
# Purpose: prevent trail stop from triggering on data errors.
# These are NOT strategy parameters. They exist solely to reject
# prices that are structurally invalid (data feed corruption,
# API parse errors, zero-fill from missing data).
#
# MIN: KOSPI/KOSDAQ minimum tick is 1원; 100원 catches most
#      zero-fill / parse-error cases without blocking penny stocks.
# MAX: Samsung Bio (207940) ~1M, Berkshire-class prices don't exist
#      in KRX; 100M gives ample headroom for splits/rights.
#
# If a legitimate price falls outside these bounds, the position
# is HELD (not sold), and a warning is logged. This is the safe
# default — never sell on bad data.
_MIN_VALID_PRICE = 100            # 100원 미만 → data error
_MAX_VALID_PRICE = 100_000_000    # 1억 원 초과 → data error


def _is_valid_price(price) -> bool:
    """Check if price is a finite positive number within sane bounds."""
    if price is None:
        return False
    try:
        p = float(price)
    except (TypeError, ValueError):
        return False
    if math.isnan(p) or math.isinf(p):
        return False
    if p <= 0:
        return False
    return True


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

    INVALID_PRICE: If today_close is invalid (<=0, None, NaN, inf,
    out of bounds), returns (False, unchanged_hwm, 0.0) — HOLD,
    never triggers a sell on bad data.
    """
    # Guard: invalid close price → HOLD, preserve HWM
    if not _is_valid_price(today_close):
        logger.warning(
            "[TRAIL_INVALID_PRICE] today_close=%s hwm=%s "
            "— decision=HOLD (price rejected)",
            today_close, high_watermark)
        return False, high_watermark, 0.0

    # Guard: extreme price (sanity bounds)
    if today_close < _MIN_VALID_PRICE or today_close > _MAX_VALID_PRICE:
        logger.warning(
            "[TRAIL_EXTREME_PRICE] today_close=%s hwm=%s "
            "bounds=[%s, %s] — decision=HOLD (out of bounds)",
            today_close, high_watermark,
            _MIN_VALID_PRICE, _MAX_VALID_PRICE)
        return False, high_watermark, 0.0

    # Update high watermark with close price
    new_hwm = max(high_watermark, today_close)

    # Check drawdown from peak
    if new_hwm > 0:
        dd = (today_close - new_hwm) / new_hwm
        if dd <= -trail_pct:
            return True, new_hwm, today_close

    return False, new_hwm, 0.0



def calc_trail_stop_price(high_watermark: float, trail_pct: float = 0.12) -> float:
    """Calculate current trail stop price level."""
    return high_watermark * (1.0 - trail_pct)
