"""Portfolio state — cash + positions + G10 sanity invariants (Jeff D4).

Sanity rules enforced (Jeff G10):
    - cash_krw >= 0  (never negative — buying more than cash → ValueError)
    - 0 <= exposure_pct <= 100
    - len(positions) <= max_positions  (configurable; default 100)

The cost_model is the SOLE entry point for fees; ``buy()`` / ``sell()``
delegate to ``calculate_cost`` so trade KRW accounting is identical across
validate / backtest / simulation. This is Jeff D4 G1's enforcement point —
no other module subtracts fees inline.

Mutability:
    Portfolio is mutated in-place by ``buy`` / ``sell``. The engine
    snapshots equity after each step into an evidence record; the Portfolio
    itself is the *current* state. (Tried frozen-immutable variant and the
    rebalancer ergonomics suffered without measurable benefit.)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from crypto.backtest.cost_model import (
    CostBreakdown,
    CostConfig,
    CostMode,
    calculate_cost,
)


@dataclass
class Position:
    pair: str
    qty: float
    cost_basis_krw: float  # cumulative KRW paid for current qty (post-fee, pre-slippage tracked separately)

    @property
    def avg_price_krw(self) -> float:
        return self.cost_basis_krw / self.qty if self.qty else 0.0


class PortfolioInvariantError(AssertionError):
    """Raised when a portfolio mutation would violate a G10 invariant."""


@dataclass
class Portfolio:
    """Backtest portfolio state.

    Each ``buy`` / ``sell`` mutation passes through ``_validate`` so any
    invariant breach surfaces at the moment of mutation, not in
    end-of-run analytics.
    """
    cash_krw: float
    positions: dict[str, Position] = field(default_factory=dict)
    timestamp: Optional[datetime] = None
    max_positions: int = 100

    # Cost configuration is held on the portfolio so every fill uses the
    # same parameters. Re-bind via ``with_config`` when running stress mode.
    cost_config: CostConfig = field(default_factory=CostConfig)
    cost_mode: CostMode = CostMode.NORMAL

    # Cumulative cost telemetry — surfaces in metrics / G9 diff report.
    total_fees_krw: float = 0.0
    total_slippage_krw: float = 0.0

    # ---- Equity / exposure -------------------------------------------------

    def equity_krw(self, prices: dict[str, float]) -> float:
        """Current marked-to-market equity. ``prices`` must contain every
        held pair; missing key → KeyError (caller must filter NaN closes
        before calling)."""
        market = sum(p.qty * prices[p.pair] for p in self.positions.values())
        return self.cash_krw + market

    def exposure_pct(self, prices: dict[str, float]) -> float:
        eq = self.equity_krw(prices)
        if eq <= 0:
            return 0.0
        market = eq - self.cash_krw
        return max(0.0, min(100.0, market / eq * 100.0))

    # ---- Trade primitives -------------------------------------------------

    def buy(
        self,
        *,
        pair: str,
        price_krw: float,
        qty: float,
        volume_24h_krw: Optional[float] = None,
    ) -> CostBreakdown:
        """Buy ``qty`` of ``pair`` at ``price_krw``. Cash deduction =
        notional + fees + slippage (all from cost_model). Raises
        ``PortfolioInvariantError`` if the trade would breach an invariant.
        """
        if qty <= 0:
            raise ValueError(f"buy qty must be > 0, got {qty}")
        if price_krw <= 0:
            raise ValueError(f"price must be > 0, got {price_krw}")
        notional = price_krw * qty
        cost = calculate_cost(
            side="buy",
            trade_value_krw=notional,
            mode=self.cost_mode,
            config=self.cost_config,
            volume_24h_krw=volume_24h_krw,
        )
        cash_after = self.cash_krw - notional - cost.total_krw
        if cash_after < 0:
            raise PortfolioInvariantError(
                f"buy {pair}: cash {self.cash_krw:.0f} insufficient for "
                f"notional {notional:.0f} + cost {cost.total_krw:.0f}"
            )
        if pair not in self.positions and len(self.positions) >= self.max_positions:
            raise PortfolioInvariantError(
                f"buy {pair}: max_positions ({self.max_positions}) reached"
            )

        # Apply
        self.cash_krw = cash_after
        if pair in self.positions:
            pos = self.positions[pair]
            pos.qty += qty
            pos.cost_basis_krw += notional + cost.fee_krw  # slippage is realized loss, tracked separately
        else:
            self.positions[pair] = Position(
                pair=pair,
                qty=qty,
                cost_basis_krw=notional + cost.fee_krw,
            )
        self.total_fees_krw += cost.fee_krw
        self.total_slippage_krw += cost.slippage_krw
        self._validate(after=f"buy {pair}")
        return cost

    def sell(
        self,
        *,
        pair: str,
        price_krw: float,
        qty: float,
        volume_24h_krw: Optional[float] = None,
    ) -> CostBreakdown:
        """Sell ``qty`` of ``pair`` at ``price_krw``. Cash credit =
        notional - fees - slippage."""
        if qty <= 0:
            raise ValueError(f"sell qty must be > 0, got {qty}")
        if price_krw <= 0:
            raise ValueError(f"price must be > 0, got {price_krw}")
        if pair not in self.positions:
            raise PortfolioInvariantError(f"sell {pair}: no position held")
        pos = self.positions[pair]
        if qty > pos.qty + 1e-12:
            raise PortfolioInvariantError(
                f"sell {pair}: qty {qty} exceeds held {pos.qty}"
            )

        notional = price_krw * qty
        cost = calculate_cost(
            side="sell",
            trade_value_krw=notional,
            mode=self.cost_mode,
            config=self.cost_config,
            volume_24h_krw=volume_24h_krw,
        )
        proceeds = notional - cost.total_krw

        # Reduce cost basis proportionally to the qty sold.
        fraction_sold = qty / pos.qty if pos.qty else 1.0
        pos.cost_basis_krw -= pos.cost_basis_krw * fraction_sold
        pos.qty -= qty
        if pos.qty <= 1e-12:
            del self.positions[pair]

        self.cash_krw += proceeds
        self.total_fees_krw += cost.fee_krw
        self.total_slippage_krw += cost.slippage_krw
        self._validate(after=f"sell {pair}")
        return cost

    # ---- Invariants -------------------------------------------------------

    def _validate(self, *, after: str) -> None:
        if self.cash_krw < -1e-6:
            raise PortfolioInvariantError(
                f"after {after}: cash_krw negative ({self.cash_krw})"
            )
        if len(self.positions) > self.max_positions:
            raise PortfolioInvariantError(
                f"after {after}: positions {len(self.positions)} > "
                f"max {self.max_positions}"
            )
        for pos in self.positions.values():
            if pos.qty < 0:
                raise PortfolioInvariantError(
                    f"after {after}: position {pos.pair} qty negative ({pos.qty})"
                )

    def sanity(self, prices: dict[str, float]) -> dict:
        """Snapshot dict for evidence JSON (Jeff G10 reportable form)."""
        eq = self.equity_krw(prices)
        return {
            "cash_krw": self.cash_krw,
            "equity_krw": eq,
            "exposure_pct": self.exposure_pct(prices),
            "position_count": len(self.positions),
            "max_positions": self.max_positions,
            "total_fees_krw": self.total_fees_krw,
            "total_slippage_krw": self.total_slippage_krw,
            "cost_mode": self.cost_mode.value,
        }
