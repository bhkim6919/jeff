# -*- coding: utf-8 -*-
"""
run_averaging_down.py
======================
Averaging-down strategy backtest.

Rules:
  1. Buy with X% of total capital (single position)
  2. NO stop loss
  3. If position drops -10% from entry → add 50% of original qty (averaging down)
  4. Only 1 additional buy allowed
  5. Sell all when avg cost profit reaches +8% or +10%
  6. Re-entry toggle
  7. Transaction costs + slippage applied

Comparison:
  A: 40% capital / 8% TP
  B: 40% capital / 10% TP
  × re-entry on/off
"""

from __future__ import annotations

import sys
import math
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from collections import Counter

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from backtest.historical_provider import HistoricalProvider
from backtest.strategies import _rs_returns, _wilder_atr


# ── Cost model ──────────────────────────────────────────────────────────

FEE = 0.00015
SLIPPAGE = 0.001
TAX = 0.0018
ENTRY_COST = FEE + SLIPPAGE
EXIT_COST = FEE + SLIPPAGE + TAX


# ── Data structures ─────────────────────────────────────────────────────

@dataclass
class AvgDownPosition:
    code: str
    entry_date: pd.Timestamp
    entry_price: float        # first buy price
    quantity: int              # current total qty
    initial_qty: int           # first buy qty
    avg_cost: float            # weighted average cost
    total_invested: float      # total cash spent (incl. costs)
    avg_down_count: int = 0    # how many times averaged down
    avg_down_dates: list = field(default_factory=list)
    avg_down_prices: list = field(default_factory=list)
    peak_price: float = 0.0   # highest price seen
    trough_price: float = 0.0  # lowest price seen


@dataclass
class ClosedTrade:
    code: str
    entry_date: pd.Timestamp
    exit_date: pd.Timestamp
    entry_price: float
    exit_price: float
    avg_cost: float
    quantity: int
    pnl_pct: float             # based on avg_cost
    pnl_won: float
    hold_days: int
    averaged_down: bool
    avg_down_triggered_pct: float  # drawdown when avg down happened
    exit_type: str             # "TP" or "EOD_FORCE"


# ── Top 50 Market Cap Universe (by avg trading value) ───────────────────

def build_top50_universe(provider: HistoricalProvider) -> List[str]:
    """Trading value 기준 Top 50 대형주 선별."""
    import os
    results = []
    ohlcv_dir = provider.ohlcv_dir
    for ticker in provider.tickers:
        df = provider._cache.get(ticker)
        if df is None or len(df) < 100:
            continue
        amt = (df["close"].astype(float) * df["volume"].astype(float)).tail(60).mean()
        results.append({"ticker": ticker, "avg_amt": amt})

    rdf = pd.DataFrame(results).sort_values("avg_amt", ascending=False)
    top50 = rdf.head(50)["ticker"].tolist()
    print(f"  [Universe] Top 50 large-cap tickers selected (avg trading value)")
    return top50


# ── Signal generator (RS Composite within Top 50 large-caps) ───────────

def generate_entry_signals(provider: HistoricalProvider, eval_date: pd.Timestamp,
                            universe: Dict[str, pd.DataFrame],
                            large_cap_tickers: List[str],
                            top_n: int = 5) -> List[dict]:
    """Top RS Composite among large-cap stocks with breakout."""
    features = []
    for ticker in large_cap_tickers:
        df = universe.get(ticker)
        if df is None:
            continue
        df_cut = df[df["date"] <= eval_date]
        if len(df_cut) < 130:
            continue
        close = df_cut["close"].astype(float)
        high = df_cut["high"].astype(float)
        last = float(close.iloc[-1])
        if last <= 0:
            continue

        rs20 = _rs_returns(close, 20)
        rs60 = _rs_returns(close, 60)
        rs120 = _rs_returns(close, 120)

        high_20 = float(high.tail(21).iloc[:-1].max()) if len(high) >= 21 else float("nan")
        breakout = int(last >= high_20) if not np.isnan(high_20) else 0

        if not breakout:
            continue

        features.append({
            "ticker": ticker, "last_close": last,
            "rs20": rs20, "rs60": rs60, "rs120": rs120,
        })

    if not features:
        return []

    fdf = pd.DataFrame(features)
    for col, out in [("rs20", "rs20_r"), ("rs60", "rs60_r"), ("rs120", "rs120_r")]:
        v = fdf[col].notna()
        fdf.loc[v, out] = fdf.loc[v, col].rank(pct=True)
        fdf.loc[~v, out] = float("nan")

    fdf["rs_composite"] = (
        fdf["rs20_r"].fillna(0) * 0.30 +
        fdf["rs60_r"].fillna(0) * 0.50 +
        fdf["rs120_r"].fillna(0) * 0.20
    )

    # Lower threshold for top 50 (smaller pool)
    cands = fdf[fdf["rs_composite"] >= 0.50].sort_values("rs_composite", ascending=False)
    result = []
    for _, r in cands.head(top_n).iterrows():
        result.append({
            "ticker": r["ticker"],
            "price": r["last_close"],
            "rs": r["rs_composite"],
        })
    return result


# ── Backtest Engine ─────────────────────────────────────────────────────

class AveragingDownEngine:
    def __init__(
        self,
        provider: HistoricalProvider,
        *,
        capital_pct: float = 0.40,
        tp_pct: float = 0.08,
        avg_down_trigger: float = -0.10,
        avg_down_qty_mult: float = 0.50,
        max_avg_downs: int = 1,
        allow_reentry: bool = False,
        signal_interval: int = 5,
        initial_cash: float = 100_000_000,
        large_cap_tickers: Optional[List[str]] = None,
        label: str = "AvgDown",
    ):
        self.provider = provider
        self.capital_pct = capital_pct
        self.tp_pct = tp_pct
        self.avg_down_trigger = avg_down_trigger
        self.avg_down_qty_mult = avg_down_qty_mult
        self.max_avg_downs = max_avg_downs
        self.allow_reentry = allow_reentry
        self.signal_interval = signal_interval
        self.initial_cash = initial_cash
        self.large_cap_tickers = large_cap_tickers or []
        self.label = label

        self.cash: float = initial_cash
        self.positions: Dict[str, AvgDownPosition] = {}  # code → position
        self.trades: List[ClosedTrade] = []
        self.equity_curve: List[dict] = []
        self.traded_codes: set = set()  # for re-entry tracking

    def run(self, start: str, end: str) -> Dict[str, Any]:
        dates = self.provider.get_trade_dates(start, end)
        if not dates:
            return {}

        # warmup
        warmup = 200
        all_idx_dates = self.provider.index_df["date"].tolist()
        start_idx = 0
        for i, d in enumerate(all_idx_dates):
            if d >= dates[0]:
                start_idx = i
                break
        if start_idx < warmup:
            actual_start = all_idx_dates[warmup] if warmup < len(all_idx_dates) else dates[0]
            dates = [d for d in dates if d >= actual_start]

        total = len(dates)
        print(f"  [{self.label}] {dates[0].strftime('%Y-%m-%d')} ~ "
              f"{dates[-1].strftime('%Y-%m-%d')} ({total} days)")

        for i, bar_date in enumerate(dates):
            # 1. Update existing positions (check avg-down + TP)
            self._update_positions(bar_date)

            # 2. Try entry if capital available
            if i % self.signal_interval == 0:
                universe = self.provider.get_universe_at(bar_date)
                signals = generate_entry_signals(
                    self.provider, bar_date, universe,
                    self.large_cap_tickers)
                self._try_enter(signals, bar_date)

            # 3. Record equity
            eq = self._equity(bar_date)
            self.equity_curve.append({
                "date": bar_date, "equity": eq, "cash": self.cash,
                "has_position": len(self.positions),
            })

            if (i + 1) % 100 == 0:
                pnl = (eq / self.initial_cash - 1) * 100
                print(f"\r    [{self.label}] {i+1}/{total} "
                      f"{bar_date.strftime('%Y-%m-%d')} "
                      f"eq={eq:,.0f} ({pnl:+.1f}%) pos={len(self.positions)}     ", end="")

        # Force close remaining
        if self.positions and dates:
            self._force_close_all(dates[-1])

        print()
        return self._build_result(dates)

    def _try_enter(self, signals: List[dict], bar_date: pd.Timestamp) -> None:
        # Capital check: need at least capital_pct of equity available
        equity = self._equity(bar_date)
        min_amount = equity * self.capital_pct * 0.5  # at least half the target

        for sig in signals:
            code = sig["ticker"]
            price = sig["price"]

            if code in self.positions:
                continue  # already holding

            if not self.allow_reentry and code in self.traded_codes:
                continue

            amount = equity * self.capital_pct
            amount = min(amount, self.cash * 0.90)
            if amount < min_amount:
                break  # not enough cash for any more entries

            qty = int(amount // price)
            if qty <= 0:
                continue

            cost = price * qty * (1 + ENTRY_COST)
            if cost > self.cash:
                continue

            self.cash -= cost
            self.positions[code] = AvgDownPosition(
                code=code,
                entry_date=bar_date,
                entry_price=price,
                quantity=qty,
                initial_qty=qty,
                avg_cost=price * (1 + ENTRY_COST),
                total_invested=cost,
                peak_price=price,
                trough_price=price,
            )
            self.traded_codes.add(code)

    def _update_positions(self, bar_date: pd.Timestamp) -> None:
        to_close = []
        for code, pos in self.positions.items():
            bar = self.provider.get_bar(code, bar_date)
            if bar is None:
                continue

            price = bar["close"]
            pos.peak_price = max(pos.peak_price, price)
            pos.trough_price = min(pos.trough_price, price)

            # Check averaging down trigger: -10% from last buy price
            if pos.avg_down_count < self.max_avg_downs:
                ref_price = pos.avg_down_prices[-1] if pos.avg_down_prices else pos.entry_price
                drawdown = (price / ref_price) - 1
                if drawdown <= self.avg_down_trigger:
                    self._average_down(pos, price, bar_date)

            # Check TP: avg cost basis
            profit_pct = (price / pos.avg_cost) - 1
            if profit_pct >= self.tp_pct:
                to_close.append((code, price, bar_date, "TP"))

        for code, price, dt, reason in to_close:
            self._close_position(code, price, dt, reason)

    def _average_down(self, pos: AvgDownPosition, price: float,
                      bar_date: pd.Timestamp) -> None:
        add_qty = int(pos.initial_qty * self.avg_down_qty_mult)
        if add_qty <= 0:
            return

        cost = price * add_qty * (1 + ENTRY_COST)
        if cost > self.cash * 0.95:
            add_qty = int((self.cash * 0.95) // (price * (1 + ENTRY_COST)))
            if add_qty <= 0:
                return
            cost = price * add_qty * (1 + ENTRY_COST)

        self.cash -= cost
        new_total_qty = pos.quantity + add_qty
        pos.avg_cost = (pos.avg_cost * pos.quantity + price * (1 + ENTRY_COST) * add_qty) / new_total_qty
        pos.quantity = new_total_qty
        pos.total_invested += cost
        pos.avg_down_count += 1
        pos.avg_down_dates.append(bar_date)
        pos.avg_down_prices.append(price)

    def _close_position(self, code: str, exit_price: float,
                        exit_date: pd.Timestamp, exit_type: str) -> None:
        pos = self.positions.pop(code, None)
        if pos is None:
            return

        proceeds = exit_price * pos.quantity
        cost = proceeds * EXIT_COST
        net = proceeds - cost
        self.cash += net

        pnl_pct = (exit_price * (1 - EXIT_COST)) / pos.avg_cost - 1
        pnl_won = net - pos.total_invested

        hold_days = len(self.provider.get_trade_dates(
            pos.entry_date.strftime("%Y-%m-%d"),
            exit_date.strftime("%Y-%m-%d")))

        avg_down_dd = 0.0
        if pos.avg_down_count > 0:
            worst_price = min(pos.avg_down_prices)
            avg_down_dd = (worst_price / pos.entry_price) - 1

        self.trades.append(ClosedTrade(
            code=pos.code,
            entry_date=pos.entry_date,
            exit_date=exit_date,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            avg_cost=pos.avg_cost,
            quantity=pos.quantity,
            pnl_pct=pnl_pct,
            pnl_won=pnl_won,
            hold_days=hold_days,
            averaged_down=(pos.avg_down_count > 0),
            avg_down_triggered_pct=avg_down_dd,
            exit_type=exit_type,
        ))

    def _force_close_all(self, last_date: pd.Timestamp) -> None:
        codes = list(self.positions.keys())
        for code in codes:
            bar = self.provider.get_bar(code, last_date)
            price = bar["close"] if bar else self.positions[code].entry_price
            self._close_position(code, price, last_date, "EOD_FORCE")

    def _equity(self, bar_date: pd.Timestamp) -> float:
        pos_val = 0.0
        for code, pos in self.positions.items():
            bar = self.provider.get_bar(code, bar_date)
            if bar:
                pos_val += bar["close"] * pos.quantity
            else:
                pos_val += pos.entry_price * pos.quantity
        return self.cash + pos_val

    def _build_result(self, dates) -> Dict[str, Any]:
        if not self.equity_curve:
            return {}

        eq = [e["equity"] for e in self.equity_curve]
        total_return = eq[-1] / self.initial_cash - 1

        n_days = len(eq)
        years = n_days / 252.0
        cagr = (eq[-1] / self.initial_cash) ** (1.0 / years) - 1 if years > 0 else 0

        # MDD
        peak = eq[0]
        mdd = 0.0
        for e in eq:
            peak = max(peak, e)
            dd = (e - peak) / peak
            mdd = min(mdd, dd)

        # Sharpe (daily returns annualized)
        daily_ret = np.diff(eq) / np.array(eq[:-1])
        sharpe = 0.0
        if len(daily_ret) > 1 and np.std(daily_ret) > 0:
            sharpe = (np.mean(daily_ret) / np.std(daily_ret)) * np.sqrt(252)

        calmar = cagr / abs(mdd) if mdd != 0 else 0

        # Trade stats
        n_trades = len(self.trades)
        wins = [t for t in self.trades if t.pnl_pct > 0]
        losses = [t for t in self.trades if t.pnl_pct <= 0]
        win_rate = len(wins) / n_trades if n_trades else 0

        hold_days_list = [t.hold_days for t in self.trades]
        avg_hold = np.mean(hold_days_list) if hold_days_list else 0
        max_hold = max(hold_days_list) if hold_days_list else 0

        # Averaging down stats
        avg_down_trades = [t for t in self.trades if t.averaged_down]
        avg_down_rate = len(avg_down_trades) / n_trades if n_trades else 0
        avg_down_profit = sum(1 for t in avg_down_trades if t.pnl_pct > 0)
        avg_down_profit_rate = avg_down_profit / len(avg_down_trades) if avg_down_trades else 0

        # Unclosed positions (EOD_FORCE)
        unclosed = sum(1 for t in self.trades if t.exit_type == "EOD_FORCE")

        # Yearly returns
        eq_df = pd.DataFrame(self.equity_curve)
        eq_df["year"] = eq_df["date"].dt.year
        yearly = {}
        prev_eq = self.initial_cash
        for y in sorted(eq_df["year"].unique()):
            y_data = eq_df[eq_df["year"] == y]
            end_eq = y_data["equity"].iloc[-1]
            yearly[int(y)] = end_eq / prev_eq - 1
            prev_eq = end_eq

        # Capital lockup analysis
        # Days with position / total days
        pos_days = sum(1 for e in self.equity_curve if e["has_position"])
        capital_utilization = pos_days / len(self.equity_curve) if self.equity_curve else 0

        # Max consecutive days in a single losing position
        max_locked_days = 0
        for t in self.trades:
            if t.pnl_pct <= 0:
                max_locked_days = max(max_locked_days, t.hold_days)

        # Longest single position hold
        longest_trade = max(self.trades, key=lambda t: t.hold_days) if self.trades else None

        return {
            "label": self.label,
            "total_return": total_return,
            "cagr": cagr,
            "mdd": mdd,
            "calmar": calmar,
            "sharpe": sharpe,
            "n_trades": n_trades,
            "win_rate": win_rate,
            "avg_hold": avg_hold,
            "max_hold": max_hold,
            "avg_down_rate": avg_down_rate,
            "avg_down_profit_rate": avg_down_profit_rate,
            "avg_down_count": len(avg_down_trades),
            "unclosed": unclosed,
            "yearly": yearly,
            "final_equity": eq[-1],
            "capital_utilization": capital_utilization,
            "max_locked_days": max_locked_days,
            "longest_trade": longest_trade,
            "trades": self.trades,
            "equity_curve": self.equity_curve,
        }


# ── OOS Split ───────────────────────────────────────────────────────────

def run_with_oos_split(provider, config, large_cap, is_start, is_end, oos_start, oos_end):
    """Run in-sample and out-of-sample separately."""
    results = {}
    params = {**config["params"], "large_cap_tickers": large_cap}

    engine_is = AveragingDownEngine(provider, label=f"{config['label']}_IS", **params)
    results["IS"] = engine_is.run(is_start, is_end)

    engine_oos = AveragingDownEngine(provider, label=f"{config['label']}_OOS", **params)
    results["OOS"] = engine_oos.run(oos_start, oos_end)

    engine_full = AveragingDownEngine(provider, label=config["label"], **params)
    results["FULL"] = engine_full.run(is_start, oos_end)

    return results


# ── Print Report ────────────────────────────────────────────────────────

def print_full_report(all_results: Dict[str, Dict]):
    """Print comprehensive comparison report."""

    labels = list(all_results.keys())
    full_results = {k: v["FULL"] for k, v in all_results.items() if v.get("FULL")}

    print("\n" + "=" * 130)
    print("  Averaging-Down Strategy Backtest: Comparison Report")
    print("=" * 130)

    # ── Main metrics table ───────────────────────────────────────────
    hdr = f"{'Metric':<35}"
    for lb in labels:
        hdr += f" {lb:>20}"
    print(hdr)
    print("-" * 130)

    rows = [
        ("Total Return",        lambda r: f"{r['total_return']*100:+.2f}%"),
        ("CAGR",                lambda r: f"{r['cagr']*100:+.2f}%"),
        ("MDD",                 lambda r: f"{r['mdd']*100:.2f}%"),
        ("Calmar",              lambda r: f"{r['calmar']:.2f}"),
        ("Sharpe",              lambda r: f"{r['sharpe']:.2f}"),
        ("---", None),
        ("Win Rate",            lambda r: f"{r['win_rate']*100:.1f}%"),
        ("Trades",              lambda r: f"{r['n_trades']}"),
        ("Avg Hold (days)",     lambda r: f"{r['avg_hold']:.0f}"),
        ("Max Hold (days)",     lambda r: f"{r['max_hold']}"),
        ("---", None),
        ("Avg-Down Rate",       lambda r: f"{r['avg_down_rate']*100:.1f}% ({r['avg_down_count']})"),
        ("Avg-Down -> Profit",  lambda r: f"{r['avg_down_profit_rate']*100:.1f}%"),
        ("Unclosed Positions",  lambda r: f"{r['unclosed']}"),
        ("---", None),
        ("Capital Utilization", lambda r: f"{r['capital_utilization']*100:.1f}%"),
        ("Max Locked Days",     lambda r: f"{r['max_locked_days']}"),
        ("Final Equity",        lambda r: f"{r['final_equity']:,.0f}"),
    ]

    for label, fmt in rows:
        if fmt is None:
            print("-" * 130)
            continue
        line = f"{label:<35}"
        for lb in labels:
            r = full_results.get(lb, {})
            try:
                line += f" {fmt(r):>20}"
            except Exception:
                line += f" {'N/A':>20}"
        print(line)

    # ── Yearly returns ───────────────────────────────────────────────
    print("\n" + "-" * 130)
    print("  Yearly Returns")
    print("-" * 130)

    all_years = set()
    for lb in labels:
        r = full_results.get(lb, {})
        all_years.update(r.get("yearly", {}).keys())

    hdr = f"{'Year':<35}"
    for lb in labels:
        hdr += f" {lb:>20}"
    print(hdr)

    for y in sorted(all_years):
        line = f"  {y:<33}"
        for lb in labels:
            r = full_results.get(lb, {})
            yr = r.get("yearly", {}).get(y, None)
            if yr is not None:
                line += f" {yr*100:>+19.2f}%"
            else:
                line += f" {'N/A':>20}"
        print(line)

    # ── OOS Analysis ─────────────────────────────────────────────────
    print("\n" + "=" * 130)
    print("  In-Sample vs Out-of-Sample")
    print("=" * 130)

    hdr = f"{'Metric':<35}"
    for lb in labels:
        hdr += f" {lb:>20}"
    print(hdr)
    print("-" * 130)

    for period_key, period_label in [("IS", "In-Sample"), ("OOS", "Out-of-Sample")]:
        print(f"\n  [{period_label}]")
        oos_rows = [
            ("  Return",  lambda r: f"{r['total_return']*100:+.2f}%"),
            ("  MDD",     lambda r: f"{r['mdd']*100:.2f}%"),
            ("  Sharpe",  lambda r: f"{r['sharpe']:.2f}"),
            ("  Trades",  lambda r: f"{r['n_trades']}"),
            ("  WinRate", lambda r: f"{r['win_rate']*100:.1f}%"),
        ]
        for label, fmt in oos_rows:
            line = f"{label:<35}"
            for lb in labels:
                r = all_results[lb].get(period_key, {})
                try:
                    line += f" {fmt(r):>20}"
                except Exception:
                    line += f" {'N/A':>20}"
            print(line)

    # ── Risk Analysis ────────────────────────────────────────────────
    print("\n" + "=" * 130)
    print("  Risk Analysis: Capital Lockup & Averaging-Down Effectiveness")
    print("=" * 130)

    for lb in labels:
        r = full_results.get(lb, {})
        trades = r.get("trades", [])
        if not trades:
            continue

        print(f"\n  [{lb}]")

        # Averaging down detail
        ad_trades = [t for t in trades if t.averaged_down]
        non_ad = [t for t in trades if not t.averaged_down]

        if ad_trades:
            ad_wins = sum(1 for t in ad_trades if t.pnl_pct > 0)
            ad_avg_pnl = np.mean([t.pnl_pct for t in ad_trades])
            ad_avg_hold = np.mean([t.hold_days for t in ad_trades])
            ad_max_hold = max(t.hold_days for t in ad_trades)
            print(f"    Avg-Down trades:    {len(ad_trades)} ({ad_wins} wins, "
                  f"avg PnL {ad_avg_pnl*100:+.2f}%, "
                  f"avg hold {ad_avg_hold:.0f}d, max hold {ad_max_hold}d)")

        if non_ad:
            nad_wins = sum(1 for t in non_ad if t.pnl_pct > 0)
            nad_avg_pnl = np.mean([t.pnl_pct for t in non_ad])
            nad_avg_hold = np.mean([t.hold_days for t in non_ad])
            print(f"    Non-AvgDown trades: {len(non_ad)} ({nad_wins} wins, "
                  f"avg PnL {nad_avg_pnl*100:+.2f}%, "
                  f"avg hold {nad_avg_hold:.0f}d)")

        # Worst locked positions
        forced = [t for t in trades if t.exit_type == "EOD_FORCE"]
        if forced:
            print(f"    Unclosed (force-closed at EOD): {len(forced)}")
            for t in sorted(forced, key=lambda x: x.pnl_pct)[:3]:
                print(f"      {t.code}: entry {t.entry_date.strftime('%Y-%m-%d')} "
                      f"PnL {t.pnl_pct*100:+.2f}% hold {t.hold_days}d "
                      f"{'(avg-down)' if t.averaged_down else ''}")

        # Top 5 longest holds
        longest = sorted(trades, key=lambda t: t.hold_days, reverse=True)[:5]
        print(f"    Top 5 longest holds:")
        for t in longest:
            print(f"      {t.code}: {t.hold_days}d "
                  f"PnL {t.pnl_pct*100:+.2f}% "
                  f"exit={t.exit_type} "
                  f"{'avg-down' if t.averaged_down else 'normal'}")

        # Capital idle analysis
        util = r.get("capital_utilization", 0)
        idle = 1 - util
        print(f"    Capital: {util*100:.1f}% utilized, {idle*100:.1f}% idle")

    # ── Verdict ──────────────────────────────────────────────────────
    print("\n" + "=" * 130)
    print("  Verdict")
    print("=" * 130)

    for lb in labels:
        r = full_results.get(lb, {})
        oos_r = all_results[lb].get("OOS", {})
        is_r = all_results[lb].get("IS", {})

        warnings = []
        if r.get("mdd", 0) < -0.20:
            warnings.append(f"DANGER: MDD {r['mdd']*100:.1f}% exceeds -20%")
        if r.get("max_locked_days", 0) > 200:
            warnings.append(f"WARN: max locked {r['max_locked_days']}d (>200d)")
        if r.get("unclosed", 0) > 0:
            warnings.append(f"WARN: {r['unclosed']} unclosed at backtest end")
        if r.get("capital_utilization", 0) < 0.30:
            warnings.append(f"WARN: capital idle {(1-r['capital_utilization'])*100:.0f}%")

        # OOS degradation
        if is_r and oos_r:
            is_ret = is_r.get("total_return", 0)
            oos_ret = oos_r.get("total_return", 0)
            if is_ret > 0 and oos_ret < 0:
                warnings.append("WARN: OOS negative (in-sample only strategy)")
            elif is_ret > 0 and oos_ret > 0:
                ratio = oos_ret / is_ret if is_ret != 0 else 0
                if ratio < 0.3:
                    warnings.append(f"WARN: OOS return only {ratio*100:.0f}% of IS")

        avg_down_rate = r.get("avg_down_rate", 0)
        avg_down_profit_rate = r.get("avg_down_profit_rate", 0)
        if avg_down_rate > 0.40:
            warnings.append(f"WARN: avg-down triggers too often ({avg_down_rate*100:.0f}%)")
        if avg_down_rate > 0 and avg_down_profit_rate < 0.50:
            warnings.append(f"ALERT: avg-down profit conversion only {avg_down_profit_rate*100:.0f}%")

        status = " | ".join(warnings) if warnings else "OK"
        print(f"  [{lb}] {status}")

    print("=" * 130)


# ── Main ────────────────────────────────────────────────────────────────

def main():
    data_dir = ROOT / "data"
    provider = HistoricalProvider(
        ohlcv_dir=str(data_dir / "ohlcv_kospi_daily"),
        index_file=str(data_dir / "kospi_index_daily_5y.csv"),
        universe_file=str(data_dir / "universe_kospi.csv"),
        sector_map_path=str(data_dir / "sector_map.json"),
    )
    provider.load_all(min_rows=130)

    # Build Top 50 large-cap universe
    large_cap = build_top50_universe(provider)

    # OOS split: IS = 2023-06 ~ 2025-06, OOS = 2025-06 ~ 2026-03
    IS_START, IS_END = "2023-06-01", "2025-06-30"
    OOS_START, OOS_END = "2025-07-01", "2026-03-14"

    configs = [
        # 1-round avg-down
        {
            "label": "TP8%_1x_noRE",
            "params": {
                "capital_pct": 0.40, "tp_pct": 0.08,
                "max_avg_downs": 1, "allow_reentry": False,
                "initial_cash": 100_000_000,
            },
        },
        {
            "label": "TP8%_1x_RE",
            "params": {
                "capital_pct": 0.40, "tp_pct": 0.08,
                "max_avg_downs": 1, "allow_reentry": True,
                "initial_cash": 100_000_000,
            },
        },
        {
            "label": "TP10%_1x_noRE",
            "params": {
                "capital_pct": 0.40, "tp_pct": 0.10,
                "max_avg_downs": 1, "allow_reentry": False,
                "initial_cash": 100_000_000,
            },
        },
        {
            "label": "TP10%_1x_RE",
            "params": {
                "capital_pct": 0.40, "tp_pct": 0.10,
                "max_avg_downs": 1, "allow_reentry": True,
                "initial_cash": 100_000_000,
            },
        },
        # 3-round avg-down
        {
            "label": "TP8%_3x_RE",
            "params": {
                "capital_pct": 0.40, "tp_pct": 0.08,
                "max_avg_downs": 3, "allow_reentry": True,
                "initial_cash": 100_000_000,
            },
        },
        {
            "label": "TP10%_3x_RE",
            "params": {
                "capital_pct": 0.40, "tp_pct": 0.10,
                "max_avg_downs": 3, "allow_reentry": True,
                "initial_cash": 100_000_000,
            },
        },
    ]

    all_results = {}

    for cfg in configs:
        print(f"\n{'='*60}")
        print(f"  {cfg['label']}")
        print(f"  capital={cfg['params']['capital_pct']*100:.0f}%, "
              f"tp={cfg['params']['tp_pct']*100:.0f}%, "
              f"reentry={'YES' if cfg['params']['allow_reentry'] else 'NO'}")
        print(f"{'='*60}")

        results = run_with_oos_split(
            provider, cfg, large_cap,
            IS_START, IS_END, OOS_START, OOS_END,
        )
        all_results[cfg["label"]] = results

    print_full_report(all_results)


if __name__ == "__main__":
    main()
