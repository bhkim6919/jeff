"""
portfolio_manager.py — Portfolio tracking and risk assessment
==============================================================
Simplified from Gen3 (removed regime/stages/v7.6+ complexity).

Tracks:
  - Positions (code, qty, avg_price, entry_date, high_watermark)
  - Cash, equity, daily/monthly PnL
  - Risk mode (NORMAL / DAILY_BLOCKED / MONTHLY_BLOCKED)
"""
from __future__ import annotations
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("gen4.portfolio")


@dataclass
class Position:
    """Gen4 position: minimal fields (no TP/SL/stage)."""
    code: str
    quantity: int
    avg_price: float
    entry_date: str
    high_watermark: float = 0.0
    trail_stop_price: float = 0.0
    sector: str = ""
    current_price: float = 0.0
    last_price_ts: Optional[datetime] = None  # stale price detection
    invested_total: float = 0.0   # cumulative buy cost (qty * price * (1+fee))
    trail_skip_days: int = 0      # consecutive EOD trail skip count
    # ── Entry-time metadata (observation only, not used in trading logic) ──
    entry_rank: int = 0            # momentum rank at entry (1~20, 0=unrecorded)
    score_mom: float = 0.0         # momentum score at entry

    @property
    def market_value(self) -> float:
        return self.quantity * self.current_price if self.current_price > 0 else 0.0

    @property
    def unrealized_pnl_pct(self) -> float:
        if self.avg_price <= 0:
            return 0.0
        return (self.current_price - self.avg_price) / self.avg_price

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "quantity": self.quantity,
            "avg_price": self.avg_price,
            "entry_date": self.entry_date,
            "high_watermark": self.high_watermark,
            "trail_stop_price": self.trail_stop_price,
            "sector": self.sector,
            "current_price": self.current_price,
            "invested_total": self.invested_total,
            "trail_skip_days": self.trail_skip_days,
            "entry_rank": self.entry_rank,
            "score_mom": self.score_mom,
        }

    @classmethod
    def from_dict(cls, d: dict, buy_cost: float = 0.00115) -> "Position":
        qty = d.get("quantity", d.get("qty", 0))
        avg = d.get("avg_price", d.get("entry_price", 0))
        # Fallback: if invested_total not stored, estimate from qty * avg * (1 + buy_cost).
        # buy_cost is injected from config — do NOT hard-code 1.00115 here.
        invested = d.get("invested_total", qty * avg * (1 + buy_cost))
        return cls(
            code=d["code"],
            quantity=qty,
            avg_price=avg,
            entry_date=str(d.get("entry_date", "")),
            high_watermark=d.get("high_watermark", d.get("high_wm", 0)),
            trail_stop_price=d.get("trail_stop_price", 0),
            sector=d.get("sector", ""),
            current_price=d.get("current_price", 0.0),
            invested_total=invested,
            trail_skip_days=d.get("trail_skip_days", 0),
            entry_rank=d.get("entry_rank", 0),
            score_mom=d.get("score_mom", 0.0),
        )


class PortfolioManager:
    """
    Gen4 portfolio manager.
    Tracks positions, cash, equity, and risk limits.
    """

    def __init__(self, initial_cash: float,
                 daily_dd_limit: float = -0.04,
                 monthly_dd_limit: float = -0.07,
                 max_positions: int = 20):
        self.cash: float = initial_cash
        self.positions: Dict[str, Position] = {}

        # Equity tracking
        self.prev_close_equity: float = initial_cash
        self.peak_equity: float = initial_cash
        self._peak_month: int = date.today().month

        # Risk limits
        self.daily_dd_limit = daily_dd_limit
        self.monthly_dd_limit = monthly_dd_limit
        self.max_positions = max_positions

    # ── Equity ───────────────────────────────────────────────────────

    def get_current_equity(self) -> float:
        """Total equity = cash + sum of market values."""
        return self.cash + sum(p.market_value for p in self.positions.values())

    def get_daily_pnl_pct(self) -> float:
        """Daily PnL vs previous close equity."""
        if self.prev_close_equity <= 0:
            return 0.0
        return (self.get_current_equity() - self.prev_close_equity) / self.prev_close_equity

    def get_monthly_dd_pct(self) -> float:
        """Monthly drawdown from peak."""
        today = date.today()
        if today.month != self._peak_month:
            self._peak_month = today.month
            self.peak_equity = self.get_current_equity()

        equity = self.get_current_equity()
        self.peak_equity = max(self.peak_equity, equity)
        if self.peak_equity <= 0:
            return 0.0
        return (equity - self.peak_equity) / self.peak_equity

    # ── Risk Mode ────────────────────────────────────────────────────

    def risk_mode(self) -> str:
        """
        Returns: "NORMAL" | "DAILY_BLOCKED" | "MONTHLY_BLOCKED"
        No forced liquidation. Only blocks new entries.
        """
        if self.get_monthly_dd_pct() <= self.monthly_dd_limit:
            return "MONTHLY_BLOCKED"
        if self.get_daily_pnl_pct() <= self.daily_dd_limit:
            return "DAILY_BLOCKED"
        return "NORMAL"

    def can_rebalance(self) -> Tuple[bool, str]:
        """Check if new entries are allowed."""
        mode = self.risk_mode()
        if mode != "NORMAL":
            return False, f"Blocked: {mode} (daily={self.get_daily_pnl_pct():.2%}, monthly={self.get_monthly_dd_pct():.2%})"
        return True, "OK"

    # ── Position Management ──────────────────────────────────────────

    def add_position(self, code: str, qty: int, price: float,
                     entry_date: str = "", sector: str = "",
                     buy_cost: float = 0.00115) -> bool:
        """Add a new position (BUY). Deducts price * qty * (1 + buy_cost) from cash."""
        if code in self.positions:
            logger.warning(f"Position {code} already exists")
            return False

        total_cost = qty * price * (1 + buy_cost)
        if total_cost > self.cash:
            logger.warning(f"Insufficient cash for {code}: need {total_cost:,.0f}, have {self.cash:,.0f}")
            return False

        self.cash -= total_cost
        self.positions[code] = Position(
            code=code,
            quantity=qty,
            avg_price=price,
            entry_date=entry_date or str(date.today()),
            high_watermark=price,
            sector=sector,
            current_price=price,
            invested_total=total_cost,
        )
        logger.info(f"BUY {code}: qty={qty}, price={price:,.0f}, cost={total_cost:,.0f} (fee incl)")
        return True

    def remove_position(self, code: str, price: float,
                        sell_cost: float = 0.00295,
                        qty: int = 0) -> Optional[dict]:
        """
        Remove (partial or full) a position (SELL). Returns trade info.

        Args:
            code: stock code
            price: fill price
            sell_cost: transaction cost rate
            qty: shares to sell. 0 = full position (legacy default).
        """
        if code not in self.positions:
            logger.warning(f"Position {code} not found")
            return None

        pos = self.positions[code]

        # Determine sell quantity
        if qty <= 0:
            sell_qty = pos.quantity
        else:
            sell_qty = min(qty, pos.quantity)
            if qty > pos.quantity:
                logger.warning(f"[SELL] {code}: requested qty={qty} > held={pos.quantity}, "
                               f"clamped to {pos.quantity}")

        proceeds = sell_qty * price * (1 - sell_cost)

        # Invested basis: proportional allocation for partial sells
        sell_ratio = sell_qty / pos.quantity  # ratio before quantity reduction
        invested_for_sell = pos.invested_total * sell_ratio
        pnl_pct = (proceeds - invested_for_sell) / invested_for_sell if invested_for_sell > 0 else 0

        self.cash += proceeds

        trade = {
            "code": code,
            "entry_date": pos.entry_date,
            "exit_date": str(date.today()),
            "entry_price": pos.avg_price,
            "exit_price": price,
            "quantity": sell_qty,
            "pnl_pct": pnl_pct,
            "pnl_amount": proceeds - invested_for_sell,
            "invested": invested_for_sell,
        }

        if sell_qty >= pos.quantity:
            del self.positions[code]
            logger.info(f"[FULL SELL APPLIED] {code}: qty={sell_qty}, "
                        f"price={price:,.0f}, pnl={pnl_pct:+.2%}")
        else:
            pos.quantity -= sell_qty
            pos.invested_total -= invested_for_sell
            logger.info(f"[PARTIAL SELL APPLIED] {code}: sold={sell_qty}, "
                        f"remaining={pos.quantity}, price={price:,.0f}, "
                        f"pnl={pnl_pct:+.2%}")

        return trade

    def update_prices(self, prices: Dict[str, float]) -> None:
        """Update current prices for all positions."""
        now = datetime.now()
        for code, pos in self.positions.items():
            if code in prices and prices[code] > 0:
                pos.current_price = prices[code]
                pos.last_price_ts = now

    def check_stale_prices(self, threshold_sec: float = 600.0) -> List[str]:
        """Return codes with stale prices (not updated for threshold_sec).
        For observability only — does not block trading.
        """
        now = datetime.now()
        stale = []
        for code, pos in self.positions.items():
            if pos.last_price_ts is None:
                stale.append(code)
            elif (now - pos.last_price_ts).total_seconds() > threshold_sec:
                stale.append(code)
        if stale:
            logger.warning(f"[STALE_PRICE_WARNING] {len(stale)} positions "
                           f"not updated for {threshold_sec:.0f}s: "
                           f"{stale[:5]}{'...' if len(stale) > 5 else ''}")
        return stale

    # ── EOD ──────────────────────────────────────────────────────────

    def end_of_day(self) -> None:
        """End-of-day update: save equity baseline."""
        self.prev_close_equity = self.get_current_equity()
        logger.info(f"EOD equity: {self.prev_close_equity:,.0f}, "
                     f"positions: {len(self.positions)}, "
                     f"cash: {self.cash:,.0f}")

    # ── Serialization ────────────────────────────────────────────────

    def to_dict(self) -> dict:
        """Serialize for state_manager."""
        return {
            "cash": self.cash,
            "prev_close_equity": self.prev_close_equity,
            "peak_equity": self.peak_equity,
            "peak_month": self._peak_month,
            "positions": {code: pos.to_dict() for code, pos in self.positions.items()},
        }

    def restore_from_dict(self, data: dict, buy_cost: float = 0.00115) -> None:
        """Restore from saved state.

        Args:
            data: Portfolio state dict from StateManager.
            buy_cost: Buy transaction cost rate from config.
                      Used to reconstruct invested_total for legacy positions
                      that predate explicit invested_total storage.
        """
        self.cash = data.get("cash", self.cash)
        self.prev_close_equity = data.get("prev_close_equity", self.cash)
        self.peak_equity = data.get("peak_equity", self.cash)
        self._peak_month = data.get("peak_month", date.today().month)

        self.positions = {}
        for code, pos_data in data.get("positions", {}).items():
            self.positions[code] = Position.from_dict(pos_data, buy_cost=buy_cost)

        logger.info(f"Restored: {len(self.positions)} positions, cash={self.cash:,.0f}")

    def summary(self) -> dict:
        """Return portfolio summary."""
        equity = self.get_current_equity()
        return {
            "equity": equity,
            "cash": self.cash,
            "n_positions": len(self.positions),
            "daily_pnl": self.get_daily_pnl_pct(),
            "monthly_dd": self.get_monthly_dd_pct(),
            "risk_mode": self.risk_mode(),
            "positions": {code: {
                "qty": pos.quantity,
                "avg_price": pos.avg_price,
                "current": pos.current_price,
                "pnl": pos.unrealized_pnl_pct,
                "hwm": pos.high_watermark,
            } for code, pos in self.positions.items()},
        }
