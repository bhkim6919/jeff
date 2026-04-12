"""
risk_gates.py — 6-gate risk control system
============================================
Gate 1: HARD_STOP  (monthly DD < -7%)  → liquidate all + block
Gate 2: DAILY_KILL (daily DD < -4%)    → block new entries
Gate 3: SOFT_STOP  (daily DD < -2%)    → close worst + block
Gate 4: MAX_POSITIONS                  → BULL: 20 / BEAR: 8
Gate 5: MAX_PER_STOCK                  → 10% of equity
Gate 6: SECTOR_EXPOSURE               → 30% of equity
Gate 7: TOTAL_EXPOSURE                 → 95% of equity
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass
class RiskEvent:
    date: str
    gate: str       # HARD_STOP, DAILY_KILL, SOFT_STOP
    detail: str
    equity: float
    daily_dd: float = 0.0
    monthly_dd: float = 0.0


class RiskGates:
    """Portfolio-level and order-level risk gates."""

    def __init__(
        self,
        daily_loss_limit: float = -0.02,    # SOFT_STOP
        daily_kill_limit: float = -0.04,    # DAILY_KILL
        monthly_dd_limit: float = -0.07,    # HARD_STOP
        max_pos_bull: int = 20,
        max_pos_bear: int = 8,
        max_per_stock: float = 0.10,
        max_sector_exp: float = 0.30,
        max_exposure: float = 0.95,
        max_early: int = 3,
        sector_cap: int = 4,
        sector_cap_etc: int = 8,
        sector_cap_early: int = 1,
    ):
        self.daily_loss_limit = daily_loss_limit
        self.daily_kill_limit = daily_kill_limit
        self.monthly_dd_limit = monthly_dd_limit
        self.max_pos_bull = max_pos_bull
        self.max_pos_bear = max_pos_bear
        self.max_per_stock = max_per_stock
        self.max_sector_exp = max_sector_exp
        self.max_exposure = max_exposure
        self.max_early = max_early
        self.sector_cap = sector_cap
        self.sector_cap_etc = sector_cap_etc
        self.sector_cap_early = sector_cap_early

        self.events: List[RiskEvent] = []
        self._hard_stopped: bool = False
        self._hard_stop_month: str = ""  # YYYY-MM of last HARD_STOP

    def check_portfolio_gates(
        self,
        equity: float,
        prev_equity: float,
        month_start_equity: float,
        date_str: str,
    ) -> Tuple[bool, bool, bool]:
        """Check portfolio-level gates.

        Returns:
            (hard_stop, daily_kill, soft_stop)
        """
        # HARD_STOP stays active for rest of month
        current_month = date_str[:7]
        if self._hard_stopped:
            if current_month == self._hard_stop_month:
                return True, True, False  # still in HARD_STOP month
            else:
                self._hard_stopped = False  # new month → release

        daily_dd = equity / prev_equity - 1 if prev_equity > 0 else 0
        monthly_dd = equity / month_start_equity - 1 if month_start_equity > 0 else 0

        hard_stop = monthly_dd < self.monthly_dd_limit
        daily_kill = daily_dd < self.daily_kill_limit
        soft_stop = daily_dd < self.daily_loss_limit

        if hard_stop:
            self._hard_stopped = True
            self._hard_stop_month = date_str[:7]  # block rest of month
            self.events.append(RiskEvent(
                date=date_str, gate="HARD_STOP",
                detail=f"monthly_dd={monthly_dd:.2%}",
                equity=equity, daily_dd=daily_dd, monthly_dd=monthly_dd))
            # Note: A policy = block today only, resume next day
            # Set _hard_stopped = False at start of next day
        if daily_kill:
            self.events.append(RiskEvent(
                date=date_str, gate="DAILY_KILL",
                detail=f"daily_dd={daily_dd:.2%}",
                equity=equity, daily_dd=daily_dd, monthly_dd=monthly_dd))
        if soft_stop and not daily_kill and not hard_stop:
            self.events.append(RiskEvent(
                date=date_str, gate="SOFT_STOP",
                detail=f"daily_dd={daily_dd:.2%}",
                equity=equity, daily_dd=daily_dd, monthly_dd=monthly_dd))

        return hard_stop, daily_kill, soft_stop

    def reset_daily(self):
        """Reset daily flags. HARD_STOP persists until end of month."""
        pass  # HARD_STOP handled by month check in check_portfolio_gates

    def can_enter(
        self,
        order_amount: float,
        equity: float,
        n_positions: int,
        is_bull: bool,
        sector: str,
        sector_exposure: float,
        total_exposure: float,
        stage: str = "B",
        n_early: int = 0,
        sector_early_count: int = 0,
    ) -> Tuple[bool, str]:
        """Check order-level gates for a new entry.

        Returns:
            (allowed, reject_reason)
        """
        max_pos = self.max_pos_bull if is_bull else self.max_pos_bear

        # Gate 4: max positions
        if n_positions >= max_pos:
            return False, f"MAX_POS ({n_positions}/{max_pos})"

        # Gate 5: per-stock limit
        if equity > 0 and order_amount / equity > self.max_per_stock:
            return False, f"MAX_PER_STOCK ({order_amount/equity:.1%}>{self.max_per_stock:.0%})"

        # Gate 6: sector exposure
        new_sector_exp = (sector_exposure + order_amount) / equity if equity > 0 else 0
        if new_sector_exp > self.max_sector_exp:
            return False, f"SECTOR_EXP ({new_sector_exp:.1%}>{self.max_sector_exp:.0%})"

        # Gate 7: total exposure
        new_total_exp = (total_exposure + order_amount) / equity if equity > 0 else 0
        if new_total_exp > self.max_exposure:
            return False, f"TOTAL_EXP ({new_total_exp:.1%}>{self.max_exposure:.0%})"

        # Stage A specific
        if stage == "A":
            if n_early >= self.max_early:
                return False, f"MAX_EARLY ({n_early}/{self.max_early})"
            cap = self.sector_cap_etc if sector == "기타" else self.sector_cap_early
            if sector_early_count >= cap:
                return False, f"SECTOR_EARLY ({sector})"

        # Stage B: sector cap
        if stage == "B":
            cap = self.sector_cap_etc if sector == "기타" else self.sector_cap
            # Note: sector_early_count here means total sector position count
            # This is simplified; caller should pass total sector count
            pass

        return True, "OK"
