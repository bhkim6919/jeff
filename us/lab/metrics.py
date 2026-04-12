# -*- coding: utf-8 -*-
"""
metrics.py — Strategy Performance Metrics (12 required)
========================================================
CAGR, MDD, Sharpe, Calmar, turnover, avg_hold_days, trade_count,
win_rate, exposure, avg_positions, exit_reason_distribution, missing_data_ratio.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Dict, List


def calc_metrics(equity_curve: pd.Series, trades: List[dict],
                 total_days: int, total_tickers: int = 0,
                 excluded_tickers: int = 0,
                 positions_history: List[int] = None) -> dict:
    """
    Calculate all 12 required metrics.

    Args:
        equity_curve: Series indexed by date, values = portfolio equity
        trades: List of trade dicts {symbol, side, qty, price, date, pnl, hold_days, exit_reason}
        total_days: Total trading days in simulation
        total_tickers: Total tickers in universe
        excluded_tickers: Tickers excluded by missing data filter
        positions_history: Daily position count list
    """
    result = {}

    # ── CAGR ────────────────────────────────────────────
    if len(equity_curve) >= 2 and equity_curve.iloc[0] > 0:
        total_return = equity_curve.iloc[-1] / equity_curve.iloc[0]
        years = total_days / 252
        if years > 0 and total_return > 0:
            result["cagr"] = round((total_return ** (1 / years) - 1) * 100, 2)
        else:
            result["cagr"] = 0.0
    else:
        result["cagr"] = 0.0

    # ── MDD ─────────────────────────────────────────────
    if len(equity_curve) >= 2:
        peak = equity_curve.expanding().max()
        drawdown = (equity_curve - peak) / peak
        result["mdd"] = round(drawdown.min() * 100, 2)
    else:
        result["mdd"] = 0.0

    # ── Sharpe ──────────────────────────────────────────
    if len(equity_curve) >= 10:
        daily_returns = equity_curve.pct_change().dropna()
        mean_r = daily_returns.mean()
        std_r = daily_returns.std()
        result["sharpe"] = round(mean_r / std_r * np.sqrt(252), 2) if std_r > 0 else 0.0
    else:
        result["sharpe"] = 0.0

    # ── Calmar ──────────────────────────────────────────
    mdd_abs = abs(result["mdd"])
    cagr_abs = abs(result["cagr"])
    result["calmar"] = round(cagr_abs / mdd_abs, 2) if mdd_abs > 1 else 0.0

    # ── Turnover (annual) ───────────────────────────────
    if trades and len(equity_curve) >= 2:
        total_traded = sum(abs(t.get("qty", 0) * t.get("price", 0)) for t in trades)
        avg_equity = equity_curve.mean()
        if avg_equity > 0 and total_days > 0:
            result["turnover"] = round(total_traded / avg_equity / total_days * 252 * 100, 1)
        else:
            result["turnover"] = 0.0
    else:
        result["turnover"] = 0.0

    # ── Avg Hold Days ───────────────────────────────────
    hold_days = [t.get("hold_days", 0) for t in trades if t.get("hold_days", 0) > 0]
    result["avg_hold_days"] = round(np.mean(hold_days), 1) if hold_days else 0.0

    # ── Trade Count ─────────────────────────────────────
    result["trade_count"] = len(trades)

    # ── Win Rate ────────────────────────────────────────
    closed = [t for t in trades if t.get("pnl") is not None]
    if closed:
        winners = sum(1 for t in closed if t["pnl"] > 0)
        result["win_rate"] = round(winners / len(closed) * 100, 1)
    else:
        result["win_rate"] = 0.0

    # ── Exposure ────────────────────────────────────────
    if len(equity_curve) >= 2 and equity_curve.mean() > 0:
        # Approximate: 1 - avg_cash_ratio
        # If positions_history available, use it
        if positions_history and len(positions_history) > 0:
            avg_invested_ratio = np.mean([min(p, 20) / 20 for p in positions_history])
            result["exposure"] = round(avg_invested_ratio * 100, 1)
        else:
            result["exposure"] = 0.0
    else:
        result["exposure"] = 0.0

    # ── Avg Positions ───────────────────────────────────
    if positions_history:
        result["avg_positions"] = round(np.mean(positions_history), 1)
    else:
        result["avg_positions"] = 0.0

    # ── Exit Reason Distribution (SELL trades only) ────
    exit_reasons = {}
    for t in trades:
        if t.get("side") == "SELL":
            reason = t.get("exit_reason", "unknown")
            exit_reasons[reason] = exit_reasons.get(reason, 0) + 1
    result["exit_reason_distribution"] = exit_reasons

    # ── Missing Data Ratio ──────────────────────────────
    if total_tickers > 0:
        result["missing_data_ratio"] = round(excluded_tickers / total_tickers * 100, 1)
    else:
        result["missing_data_ratio"] = 0.0

    return result
