"""6 backtest metrics + canonical hash helper (Jeff D4 G4 + G6).

Crypto-specific conventions:
    - Sharpe annualization uses ``√365`` (24/7 market, calendar days).
    - CAGR uses calendar days, not "trade days" (Jeff Q5=A: there are no
      거래일 in crypto).
    - Risk-free rate omitted (default 0). Caller can provide ``rf_pct``
      to ``sharpe`` if needed.

Canonical hash (G6):
    SHA256 over the rounded metrics dict + trade_count + final_equity.
    The equity curve is intentionally excluded from the hash — it lives
    in a CSV sidecar (Jeff E7=A) and minor floating-point drift from
    rounding accumulation would otherwise break idempotency without
    indicating a real correctness issue.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import date
from typing import Iterable, Optional

PERIODS_PER_YEAR_CRYPTO = 365  # 24/7 market


def cagr(equity_curve: list[tuple[date, float]]) -> float:
    """Compound annual growth rate over the equity curve.

    Returns 0.0 for degenerate curves (length < 2 or non-positive endpoints).
    """
    if len(equity_curve) < 2:
        return 0.0
    start_dt, start_eq = equity_curve[0]
    end_dt, end_eq = equity_curve[-1]
    if start_eq <= 0 or end_eq <= 0:
        return 0.0
    days = (end_dt - start_dt).days
    if days <= 0:
        return 0.0
    return (end_eq / start_eq) ** (PERIODS_PER_YEAR_CRYPTO / days) - 1.0


def max_drawdown(equity_curve: list[tuple[date, float]]) -> float:
    """Peak-to-trough max drawdown. Returns a negative number (e.g. -0.25
    = -25%). 0.0 for monotonic-non-decreasing curves."""
    peak = -math.inf
    mdd = 0.0
    for _, eq in equity_curve:
        if eq > peak:
            peak = eq
        if peak > 0:
            dd = (eq - peak) / peak
            if dd < mdd:
                mdd = dd
    return mdd


def daily_returns(equity_curve: list[tuple[date, float]]) -> list[float]:
    out: list[float] = []
    prev = None
    for _, eq in equity_curve:
        if prev is not None and prev > 0:
            out.append(eq / prev - 1.0)
        prev = eq
    return out


def sharpe(
    equity_curve: list[tuple[date, float]],
    *,
    rf_pct_annual: float = 0.0,
) -> float:
    """Annualized Sharpe ratio. ``√365`` for crypto.

    Returns 0.0 if there are <2 returns or the std is zero.
    """
    rets = daily_returns(equity_curve)
    if len(rets) < 2:
        return 0.0
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
    std = var ** 0.5
    if std == 0:
        return 0.0
    daily_rf = rf_pct_annual / PERIODS_PER_YEAR_CRYPTO
    return (mean - daily_rf) / std * math.sqrt(PERIODS_PER_YEAR_CRYPTO)


def calmar(cagr_value: float, mdd_value: float) -> float:
    """CAGR / |MDD|. Returns 0.0 if MDD is zero."""
    if mdd_value == 0:
        return 0.0
    return cagr_value / abs(mdd_value)


def trade_count(trade_log: Iterable) -> int:
    return sum(1 for _ in trade_log)


def avg_exposure_pct(
    equity_curve: list[tuple[date, float]],
    market_curve: list[tuple[date, float]],
) -> float:
    """Average market_value / equity over the curve, in 0..100. The two
    curves MUST be aligned by index (engine builds them together).
    """
    if not equity_curve or not market_curve or len(equity_curve) != len(market_curve):
        return 0.0
    total = 0.0
    n = 0
    for (_, eq), (_, mv) in zip(equity_curve, market_curve):
        if eq <= 0:
            continue
        total += max(0.0, min(100.0, mv / eq * 100.0))
        n += 1
    return total / n if n else 0.0


def compute_all(
    equity_curve: list[tuple[date, float]],
    market_curve: list[tuple[date, float]],
    trade_log: list,
) -> dict[str, float]:
    """Return all six metrics in a fixed key order (Jeff G4)."""
    c = cagr(equity_curve)
    m = max_drawdown(equity_curve)
    s = sharpe(equity_curve)
    return {
        "cagr": c,
        "mdd": m,
        "sharpe": s,
        "calmar": calmar(c, m),
        "trades": trade_count(trade_log),
        "exposure_pct": avg_exposure_pct(equity_curve, market_curve),
    }


def canonical_hash(
    metrics: dict[str, float],
    *,
    trade_count_value: int,
    final_equity_krw: float,
    decimals: int = 8,
) -> str:
    """Stable SHA256 over the metrics + trade_count + final_equity.

    Exclusions (Jeff E7=A):
        - equity_curve (CSV sidecar; floating drift not material here)
        - trade_log details (count is enough for idempotency proof)
        - timestamps (would break determinism)

    Same input → same hash. Run twice → byte-identical hashes (G6).
    """
    payload = {
        "metrics": {k: round(float(v), decimals) for k, v in sorted(metrics.items())},
        "trade_count": int(trade_count_value),
        "final_equity_krw": round(float(final_equity_krw), decimals),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode("utf-8")
    ).hexdigest()
