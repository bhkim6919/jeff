"""Single source of truth for backtest costs (Jeff D4 G1, Q2=C).

Why this module exists at all:
    KR Gen4 had two backtest implementations using different fee schedules
    (validate_gen4: 0.115%/0.295% → +472.5%, backtest_gen4_core: 0.65%/0.83%
    → +28.9%). The 16x outcome gap was the proximate cause of the Live
    rollback decision. Crypto Lab forbids that failure mode by design:
    every cost calculation must enter through ``calculate_cost``, and
    fee/slippage parameters live in ``CostConfig`` — never hardcoded.

Two modes (Jeff DESIGN §6, both required per Q2=C):
    NORMAL — Upbit KRW spot maker/taker = 0.05%, slippage = 0
    STRESS — 0.25% fee, slippage = trade_value / volume_24h × 0.5

The runner is expected to compute the same trade twice (NORMAL + STRESS) and
emit a diff report (G9). ``cost_diff`` is the convenience for that.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class CostMode(Enum):
    NORMAL = "normal"
    STRESS = "stress"


@dataclass(frozen=True)
class CostConfig:
    """Cost parameters. Default = Upbit KRW spot (2026-04 reference).

    Externalize via constructor — never hardcode in callers. KR Gen4
    incident tag for the future-Claude reading this: cost_comparison.md.
    """
    # NORMAL mode (Upbit KRW)
    maker_fee_pct: float = 0.0005   # 0.05%
    taker_fee_pct: float = 0.0005   # 0.05%
    # STRESS mode
    stress_fee_pct: float = 0.0025  # 0.25%
    stress_slippage_factor: float = 0.5  # slippage_pct = trade_value / volume_24h × this

    def fee_pct(self, mode: CostMode, *, is_taker: bool = True) -> float:
        if mode is CostMode.NORMAL:
            return self.taker_fee_pct if is_taker else self.maker_fee_pct
        if mode is CostMode.STRESS:
            return self.stress_fee_pct
        raise ValueError(f"unknown CostMode: {mode!r}")


@dataclass(frozen=True)
class CostBreakdown:
    """Per-trade cost detail. Always returned by ``calculate_cost`` so callers
    can attribute KRW deductions to fee vs slippage in evidence."""
    side: str                  # "buy" or "sell"
    mode: CostMode
    trade_value_krw: float
    fee_krw: float
    slippage_krw: float

    @property
    def total_krw(self) -> float:
        return self.fee_krw + self.slippage_krw

    @property
    def total_pct(self) -> float:
        return self.total_krw / self.trade_value_krw if self.trade_value_krw else 0.0


def calculate_cost(
    *,
    side: str,
    trade_value_krw: float,
    mode: CostMode,
    config: Optional[CostConfig] = None,
    volume_24h_krw: Optional[float] = None,
    is_taker: bool = True,
) -> CostBreakdown:
    """Compute fee + slippage for a single trade.

    ``side``:
        ``"buy"`` or ``"sell"``. Upbit KRW is symmetric (maker = taker, buy =
        sell), but this signature keeps the door open for asymmetric fees
        without reaching into the module again.

    ``mode``:
        ``CostMode.NORMAL`` → fee_pct only, slippage 0.
        ``CostMode.STRESS`` → fee_pct + slippage = trade/volume_24h × factor.
        STRESS mode requires ``volume_24h_krw``; missing → ValueError.

    Returns ``CostBreakdown``. The caller subtracts ``total_krw`` from the
    notional in the SAME accounting step (no spreading across modules) — see
    portfolio.Portfolio.buy/sell for the canonical use site.
    """
    if side not in {"buy", "sell"}:
        raise ValueError(f"side must be 'buy' or 'sell', got {side!r}")
    if trade_value_krw < 0:
        raise ValueError(f"trade_value_krw must be >= 0, got {trade_value_krw}")
    cfg = config or CostConfig()

    fee_pct = cfg.fee_pct(mode, is_taker=is_taker)
    fee_krw = trade_value_krw * fee_pct

    if mode is CostMode.STRESS:
        if volume_24h_krw is None:
            raise ValueError(
                "STRESS mode requires volume_24h_krw; pass per-pair 24h KRW "
                "value or fall back to NORMAL"
            )
        if volume_24h_krw <= 0:
            # Degenerate liquidity — treat as full-impact slippage. Caller
            # may still elect to skip the trade upstream; we don't decide
            # that here.
            slippage_pct = 1.0
        else:
            slippage_pct = (trade_value_krw / volume_24h_krw) * cfg.stress_slippage_factor
        slippage_krw = trade_value_krw * slippage_pct
    else:
        slippage_krw = 0.0

    return CostBreakdown(
        side=side,
        mode=mode,
        trade_value_krw=trade_value_krw,
        fee_krw=fee_krw,
        slippage_krw=slippage_krw,
    )


def cost_diff(
    *,
    side: str,
    trade_value_krw: float,
    config: Optional[CostConfig] = None,
    volume_24h_krw: Optional[float] = None,
) -> dict:
    """Convenience for G9: produce normal + stress side-by-side.

    Returns a flat dict suitable for evidence JSON::

        {
            "normal":      {fee_krw, slippage_krw, total_krw, total_pct},
            "stress":      {fee_krw, slippage_krw, total_krw, total_pct},
            "diff_total":  stress_total - normal_total,
            "diff_pct":    stress_pct - normal_pct,
        }

    The runner aggregates these per-trade diffs into a per-strategy summary
    so high-cost-sensitivity strategies surface early (Jeff D4 G9 rationale).
    """
    n = calculate_cost(
        side=side, trade_value_krw=trade_value_krw,
        mode=CostMode.NORMAL, config=config,
    )
    s = calculate_cost(
        side=side, trade_value_krw=trade_value_krw,
        mode=CostMode.STRESS, config=config, volume_24h_krw=volume_24h_krw,
    )
    return {
        "normal": {
            "fee_krw": n.fee_krw,
            "slippage_krw": n.slippage_krw,
            "total_krw": n.total_krw,
            "total_pct": n.total_pct,
        },
        "stress": {
            "fee_krw": s.fee_krw,
            "slippage_krw": s.slippage_krw,
            "total_krw": s.total_krw,
            "total_pct": s.total_pct,
        },
        "diff_total_krw": s.total_krw - n.total_krw,
        "diff_pct": s.total_pct - n.total_pct,
    }
