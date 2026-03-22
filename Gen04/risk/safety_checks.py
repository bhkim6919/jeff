"""
safety_checks.py — Pre-order safety validations
=================================================
Trading halt, admin issue, order sanity checks.
"""
from __future__ import annotations
import logging
from typing import Dict, Optional, Tuple

logger = logging.getLogger("gen4.safety")


def is_trading_halt(code: str, price: float) -> bool:
    """
    Check if stock appears to be halted.
    A price of 0 or missing data suggests halt/suspension.
    """
    if price <= 0:
        logger.warning(f"{code}: possible trading halt (price={price})")
        return True
    return False


def validate_order(side: str, code: str, qty: int, price: float,
                   cash: float = 0, held_qty: int = 0,
                   buy_cost: float = 0.00115) -> Tuple[bool, str]:
    """
    Pre-order sanity check.

    Args:
        side: "BUY" or "SELL"
        code: Ticker code
        qty: Order quantity
        price: Order price
        cash: Available cash (for BUY)
        held_qty: Current held quantity (for SELL)
        buy_cost: Transaction cost multiplier

    Returns:
        (valid: bool, reason: str)
    """
    if not code or len(code) != 6:
        return False, f"Invalid code: {code}"

    if qty <= 0:
        return False, f"Invalid qty: {qty}"

    if price <= 0:
        return False, f"Invalid price: {price}"

    if side == "BUY":
        total_cost = qty * price * (1 + buy_cost)
        if total_cost > cash:
            return False, f"Insufficient cash: need {total_cost:,.0f}, have {cash:,.0f}"

    elif side == "SELL":
        if qty > held_qty:
            return False, f"Sell qty {qty} > held {held_qty}"

    else:
        return False, f"Invalid side: {side}"

    return True, "OK"


def check_universe_exit(held_codes: list, current_universe: list) -> list:
    """
    Check if any held stocks have left the tradeable universe.

    Returns:
        List of codes that exited the universe.
    """
    universe_set = set(current_universe)
    exited = [code for code in held_codes if code not in universe_set]
    if exited:
        logger.warning(f"Universe exit detected: {exited}")
    return exited


def check_sellable_qty(code: str, position_qty: int,
                       broker_info: Optional[dict] = None) -> Tuple[int, str]:
    """
    Determine sellable quantity.

    Args:
        code: Ticker code
        position_qty: Position quantity in our state
        broker_info: Optional broker query result {hold_qty, sellable_qty}

    Returns:
        (sellable_qty, note)
    """
    if broker_info is None:
        # No broker info, assume all sellable (offline/mock mode)
        return position_qty, "assumed_sellable"

    if broker_info.get("error"):
        return 0, f"broker_error: {broker_info['error']}"

    broker_hold = broker_info.get("hold_qty", 0)
    if broker_hold < position_qty:
        logger.warning(f"{code}: broker hold {broker_hold} < position {position_qty}")
        return min(broker_hold, position_qty), "qty_mismatch"

    return position_qty, "confirmed"
