"""
theme_proxy_backtest.py — Theme Proxy standalone backtest
==========================================================
Signal on day T close -> entry on day T+1 open (via pending_entries).
3 exit variants: V1 (SL/TP/5d), V2 (SL/trail/7d), V3 (SL/TP/10d).

Strategy: simplified theme proxy (volume surge + momentum + high proximity)
This is the "b version" — pure OHLCV-based, no sector_map/cluster.

Usage:
  python -m backtest.theme_proxy_backtest
  python -m backtest.theme_proxy_backtest --variant V2 --start 2021-01-01
"""
from __future__ import annotations
import argparse
import json
import logging
import sys
import warnings
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from strategy.theme_proxy import select_theme_candidates

logger = logging.getLogger("theme_backtest")

# ── Config ──────────────────────────────────────────────────────────────────

BUY_COST = 0.00115
SELL_COST = 0.00295
INITIAL_CASH = 100_000_000
MAX_STOCKS = 5
SINGLE_CAP = 0.30  # max 30% per stock

VARIANTS = {
    "V1": {"sl": -0.08, "tp": 0.20, "trail": None, "max_hold": 5},
    "V2": {"sl": -0.08, "tp": None, "trail": 0.10, "max_hold": 7},
    "V3": {"sl": -0.10, "tp": 0.15, "trail": None, "max_hold": 10},
}


def is_valid_common_stock(code: str) -> bool:
    """6-digit numeric code ending in 0 (common stock only)."""
    return len(code) == 6 and code.isdigit() and code[-1] == '0'


@dataclass
class Position:
    code: str
    entry_price: float
    quantity: int
    entry_idx: int
    signal_idx: int = 0       # day signal was generated (T)
    high_watermark: float = 0.0

    @property
    def cost_basis(self):
        return self.entry_price * self.quantity


# ── Data Loading ────────────────────────────────────────────────────────────

def load_ohlcv_matrix(ohlcv_dir: Path, min_history: int = 60):
    """Load all per-stock CSVs into aligned matrices.
    Only close is forward-filled (for MTM).
    open/high/volume are left as NaN on non-trading days."""
    dates_set = set()
    raw = {}

    for f in sorted(ohlcv_dir.glob("*.csv")):
        code = f.stem
        if not is_valid_common_stock(code):
            continue
        try:
            df = pd.read_csv(f, parse_dates=["date"])
            if len(df) < min_history:
                continue
            for c in ("open", "high", "low", "close", "volume"):
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
            df = df.sort_values("date").reset_index(drop=True)
            raw[code] = df
            dates_set.update(df["date"].tolist())
        except Exception:
            continue

    dates = sorted(dates_set)
    date_to_idx = {d: i for i, d in enumerate(dates)}
    n_dates = len(dates)

    close_m, open_m, high_m, volume_m = {}, {}, {}, {}
    codes = []

    for code, df in raw.items():
        c = np.full(n_dates, np.nan)
        o = np.full(n_dates, np.nan)
        h = np.full(n_dates, np.nan)
        v = np.full(n_dates, np.nan)
        for _, row in df.iterrows():
            idx = date_to_idx.get(row["date"])
            if idx is not None:
                c[idx] = row["close"]
                o[idx] = row["open"]
                h[idx] = row["high"]
                v[idx] = row["volume"]
        # Forward fill close ONLY (for MTM on non-trading days)
        for i in range(1, n_dates):
            if np.isnan(c[i]):
                c[i] = c[i - 1]
        # Do NOT ffill open, high, volume — NaN = not tradable
        close_m[code] = c
        open_m[code] = o
        high_m[code] = h
        volume_m[code] = v
        codes.append(code)

    return dates, codes, close_m, open_m, high_m, volume_m


def load_kospi(index_file: Path, dates: list) -> np.ndarray:
    """Load KOSPI close aligned to dates."""
    kospi = np.full(len(dates), np.nan)
    try:
        df = pd.read_csv(index_file, encoding="utf-8-sig")
        col_date = "index" if "index" in df.columns else df.columns[0]
        col_close = "Close" if "Close" in df.columns else "close"
        df[col_date] = pd.to_datetime(df[col_date])
        date_map = {d: float(r[col_close]) for _, r in df.iterrows()
                    for d in [r[col_date]]}
        for i, d in enumerate(dates):
            if d in date_map:
                kospi[i] = date_map[d]
        # Forward fill
        for i in range(1, len(kospi)):
            if np.isnan(kospi[i]):
                kospi[i] = kospi[i - 1]
    except Exception as e:
        logger.warning("KOSPI load failed: %s", e)
    return kospi


# ── Backtest Engine ─────────────────────────────────────────────────────────

def run_theme_backtest(
    dates, codes, close_m, open_m, high_m, volume_m, kospi,
    variant: str = "V1",
    start_date=None, end_date=None,
) -> dict:
    """Run Theme Proxy backtest with correct T+1 entry timing."""
    cfg = VARIANTS[variant]
    sl = cfg["sl"]
    tp = cfg["tp"]
    trail = cfg["trail"]
    max_hold = cfg["max_hold"]

    cash = float(INITIAL_CASH)
    positions: Dict[str, Position] = {}
    pending_entries: List[dict] = []   # signals waiting for T+1 fill
    trades = []
    equity_curve = []
    _sample_count = 0  # for verification logging

    start_idx = 0
    end_idx = len(dates) - 1
    if start_date:
        for i, d in enumerate(dates):
            if d >= start_date:
                start_idx = i
                break
    if end_date:
        for i, d in enumerate(dates):
            if d >= end_date:
                end_idx = i
                break

    for t in range(start_idx, end_idx + 1):
        # ── 0. Fill pending entries from yesterday's signals ───
        for pe in list(pending_entries):
            if t != pe["entry_day"]:
                continue
            code = pe["code"]
            if code in positions:
                pending_entries.remove(pe)
                continue  # already held (e.g. exit+re-signal same day)
            o = open_m[code]
            if t >= len(o) or np.isnan(o[t]) or o[t] <= 0:
                pending_entries.remove(pe)
                continue  # no valid open price (halted etc.)

            entry_price = o[t]
            qty = int(pe["alloc"] / (entry_price * (1 + BUY_COST)))
            if qty <= 0:
                pending_entries.remove(pe)
                continue
            cost = qty * entry_price * (1 + BUY_COST)
            if cost > cash:
                pending_entries.remove(pe)
                continue

            cash -= cost
            positions[code] = Position(
                code=code, entry_price=entry_price,
                quantity=qty, entry_idx=t,
                signal_idx=pe["signal_day"],
                high_watermark=entry_price)
            pending_entries.remove(pe)

            # Verification: log first 10 fills
            if _sample_count < 10:
                _sample_count += 1
                logger.info(
                    "[TIMING] #%d signal=%s entry=%s open=%.0f code=%s",
                    _sample_count,
                    dates[pe["signal_day"]].strftime("%Y-%m-%d"),
                    dates[t].strftime("%Y-%m-%d"),
                    entry_price, code)

        # Expire stale pendings (entry day already passed)
        pending_entries = [pe for pe in pending_entries if pe["entry_day"] >= t]

        # ── 1. Check exits on today's prices ────────────────────
        for code in list(positions.keys()):
            pos = positions[code]
            c = close_m[code]
            if t >= len(c) or np.isnan(c[t]) or c[t] <= 0:
                continue

            price = c[t]
            hold_days = t - pos.entry_idx
            pnl_pct = price / pos.entry_price - 1

            # Update HWM
            pos.high_watermark = max(pos.high_watermark, price)

            exit_reason = None

            # SL
            if pnl_pct <= sl:
                exit_reason = "SL"
            # TP
            elif tp and pnl_pct >= tp:
                exit_reason = "TP"
            # Trailing stop
            elif trail and pos.high_watermark > 0:
                dd = (price - pos.high_watermark) / pos.high_watermark
                if dd <= -trail:
                    exit_reason = "TRAIL"
            # Max hold
            if hold_days >= max_hold and exit_reason is None:
                exit_reason = "MAX_HOLD"

            if exit_reason:
                assert is_valid_common_stock(code), \
                    f"Non-common stock in trade exit: {code}"
                proceeds = pos.quantity * price * (1 - SELL_COST)
                cash += proceeds
                trades.append({
                    "code": code,
                    "signal_date": dates[pos.signal_idx],
                    "entry_date": dates[pos.entry_idx],
                    "exit_date": dates[t],
                    "entry_price": pos.entry_price,
                    "exit_price": price,
                    "quantity": pos.quantity,
                    "pnl_pct": pnl_pct,
                    "pnl_amount": pos.quantity * (price - pos.entry_price)
                                  - pos.cost_basis * (BUY_COST + SELL_COST),
                    "hold_days": hold_days,
                    "exit_reason": exit_reason,
                })
                del positions[code]

        # ── 2. Signal generation (today's close) → pending for T+1 ─
        if t < end_idx:
            n_slots = MAX_STOCKS - len(positions) - len(pending_entries)
            if n_slots > 0:
                # Cash reserved for pending entries (not yet deducted)
                reserved = sum(pe["alloc"] for pe in pending_entries)
                available_cash = cash - reserved

                candidates = select_theme_candidates(
                    t, codes, close_m, volume_m, high_m,
                    min_price=3000, min_amt_ratio=3.0,
                    min_ret_5d_pctile=0.90, max_stocks=n_slots)

                # Filter already held or already pending
                pending_codes = {pe["code"] for pe in pending_entries}
                candidates = [c for c in candidates
                              if c["code"] not in positions
                              and c["code"] not in pending_codes]

                for cand in candidates:
                    code = cand["code"]
                    assert is_valid_common_stock(code), \
                        f"Non-common stock in candidates: {code}"

                    alloc = min(available_cash * SINGLE_CAP,
                                available_cash / max(n_slots, 1))
                    if alloc <= 0:
                        break

                    # Queue for T+1 fill — do NOT deduct cash yet
                    pending_entries.append({
                        "code": code,
                        "entry_day": t + 1,
                        "signal_day": t,
                        "alloc": alloc,
                    })
                    available_cash -= alloc
                    n_slots -= 1
                    if n_slots <= 0:
                        break

        # ── 3. Mark-to-market equity (filled positions ONLY) ──
        mtm = cash
        for code, pos in positions.items():
            c = close_m[code]
            p = c[t] if t < len(c) and not np.isnan(c[t]) else pos.entry_price
            mtm += pos.quantity * p
        equity_curve.append({"date": dates[t], "equity": mtm,
                             "n_positions": len(positions)})

    # Force close remaining
    for code in list(positions.keys()):
        pos = positions[code]
        c = close_m[code]
        price = c[end_idx] if end_idx < len(c) and not np.isnan(c[end_idx]) \
                else pos.entry_price
        cash += pos.quantity * price * (1 - SELL_COST)
        trades.append({
            "code": code,
            "signal_date": dates[pos.signal_idx],
            "entry_date": dates[pos.entry_idx],
            "exit_date": dates[end_idx],
            "entry_price": pos.entry_price,
            "exit_price": price, "quantity": pos.quantity,
            "pnl_pct": price / pos.entry_price - 1,
            "pnl_amount": pos.quantity * (price - pos.entry_price),
            "hold_days": end_idx - pos.entry_idx, "exit_reason": "EOD",
        })

    return {
        "variant": variant,
        "config": cfg,
        "equity": pd.DataFrame(equity_curve),
        "trades": pd.DataFrame(trades) if trades else pd.DataFrame(),
        "initial_cash": INITIAL_CASH,
        "final_equity": equity_curve[-1]["equity"] if equity_curve else INITIAL_CASH,
    }


# ── Metrics ─────────────────────────────────────────────────────────────────

def compute_metrics(result: dict, kospi: np.ndarray = None,
                    dates: list = None) -> dict:
    eq = result["equity"]
    trades = result["trades"]
    ic = result["initial_cash"]
    fe = result["final_equity"]

    if eq.empty:
        return {"total_return": 0, "cagr": 0, "mdd": 0, "sharpe": 0}

    eqs = eq["equity"].values
    n_days = len(eqs)
    years = max(n_days / 252, 0.1)

    total_ret = fe / ic - 1
    cagr = (fe / ic) ** (1 / years) - 1

    # MDD
    peak = np.maximum.accumulate(eqs)
    dd = (eqs - peak) / peak
    mdd = float(dd.min())

    # Daily returns
    rets = np.diff(eqs) / eqs[:-1]
    rets = rets[~np.isnan(rets)]
    sharpe = float(np.mean(rets) / np.std(rets) * np.sqrt(252)) if len(rets) > 10 and np.std(rets) > 0 else 0
    calmar = cagr / abs(mdd) if mdd != 0 else 0

    # Trade stats
    n_trades = len(trades)
    if n_trades > 0:
        pnls = trades["pnl_pct"].values
        winners = pnls[pnls > 0]
        losers = pnls[pnls <= 0]
        win_rate = len(winners) / n_trades
        avg_win = float(np.mean(winners)) if len(winners) > 0 else 0
        avg_loss = float(np.mean(losers)) if len(losers) > 0 else 0
        payoff = abs(avg_win / avg_loss) if avg_loss != 0 else 0
        pf = abs(np.sum(winners) / np.sum(losers)) if np.sum(losers) != 0 else 0
        avg_hold = float(trades["hold_days"].mean())
        max_loss = float(pnls.min())
        max_win = float(pnls.max())

        # Exit reason breakdown
        exit_counts = trades["exit_reason"].value_counts().to_dict()

        # Gross return (no costs)
        gross_pnl = sum(trades["quantity"] * (trades["exit_price"] - trades["entry_price"]))
        gross_ret = gross_pnl / ic
    else:
        win_rate = avg_win = avg_loss = payoff = pf = avg_hold = 0
        max_loss = max_win = gross_ret = 0
        exit_counts = {}

    # Exposure
    pos_days = eq[eq["n_positions"] > 0].shape[0]
    exposure = pos_days / max(n_days, 1)

    # Cash days
    cash_days = n_days - pos_days

    # KOSPI comparison
    kospi_ret = None
    if kospi is not None and dates is not None and len(kospi) > 0:
        eq_dates = eq["date"].tolist()
        if eq_dates:
            k_start = k_end = None
            for i, d in enumerate(dates):
                if d >= eq_dates[0] and k_start is None:
                    k_start = kospi[i]
                if d <= eq_dates[-1]:
                    k_end = kospi[i]
            if k_start and k_end and k_start > 0:
                kospi_ret = k_end / k_start - 1

    return {
        "total_return": round(total_ret, 4),
        "gross_return": round(gross_ret, 4),
        "cagr": round(cagr, 4),
        "mdd": round(mdd, 4),
        "sharpe": round(sharpe, 2),
        "calmar": round(calmar, 2),
        "win_rate": round(win_rate, 4),
        "avg_win": round(avg_win, 4),
        "avg_loss": round(avg_loss, 4),
        "payoff": round(payoff, 2),
        "profit_factor": round(pf, 2),
        "n_trades": n_trades,
        "avg_hold": round(avg_hold, 1),
        "max_loss_trade": round(max_loss, 4),
        "max_win_trade": round(max_win, 4),
        "exposure": round(exposure, 4),
        "cash_days": cash_days,
        "exit_counts": exit_counts,
        "kospi_return": round(kospi_ret, 4) if kospi_ret is not None else None,
        "excess_return": round(total_ret - kospi_ret, 4) if kospi_ret is not None else None,
    }


def print_metrics(m: dict, label: str = ""):
    print(f"\n{'=' * 50}")
    print(f"  {label}")
    print(f"{'=' * 50}")
    print(f"  Total Return:   {m['total_return']*100:+.2f}%")
    print(f"  Gross Return:   {m['gross_return']*100:+.2f}%")
    print(f"  CAGR:           {m['cagr']*100:+.2f}%")
    print(f"  MDD:            {m['mdd']*100:.2f}%")
    print(f"  Sharpe:         {m['sharpe']:.2f}")
    print(f"  Calmar:         {m['calmar']:.2f}")
    print(f"  Win Rate:       {m['win_rate']*100:.1f}%")
    print(f"  Payoff:         {m['payoff']:.2f}")
    print(f"  Profit Factor:  {m['profit_factor']:.2f}")
    print(f"  Trades:         {m['n_trades']}")
    print(f"  Avg Hold:       {m['avg_hold']:.1f} days")
    print(f"  Max Loss Trade: {m['max_loss_trade']*100:.2f}%")
    print(f"  Max Win Trade:  {m['max_win_trade']*100:.2f}%")
    print(f"  Exposure:       {m['exposure']*100:.1f}%")
    print(f"  Cash Days:      {m['cash_days']}")
    if m.get("kospi_return") is not None:
        print(f"  KOSPI Return:   {m['kospi_return']*100:+.2f}%")
        print(f"  Excess Return:  {m['excess_return']*100:+.2f}%")
    if m.get("exit_counts"):
        print(f"  Exit Reasons:   {m['exit_counts']}")


def save_results(result: dict, metrics: dict, output_dir: Path,
                  label: str = "", period: str = "", verdict: str = ""):
    """Save equity CSV, trades CSV, and summary JSON."""
    import json
    output_dir.mkdir(parents=True, exist_ok=True)

    variant = result.get("variant", label).replace(" ", "_").replace("+", "_")
    prefix = variant.lower()

    # Equity CSV
    eq = result["equity"]
    if not eq.empty:
        eq_path = output_dir / f"{prefix}_equity.csv"
        eq.to_csv(eq_path, index=False)
        print(f"  Saved: {eq_path}")

    # Trades CSV
    trades = result["trades"]
    if not trades.empty:
        tr_path = output_dir / f"{prefix}_trades.csv"
        trades.to_csv(tr_path, index=False)
        print(f"  Saved: {tr_path}")

    # Summary JSON (metrics + strategy params)
    summary = {
        "strategy": label or variant,
        "variant": result.get("variant", ""),
        "params": {k: v for k, v in result.get("config", {}).items()},
        "period": period,
        "cost": {"buy": BUY_COST, "sell": SELL_COST},
        "initial_cash": result.get("initial_cash", INITIAL_CASH),
        "final_equity": result.get("final_equity", 0),
        "metrics": {k: v for k, v in metrics.items() if k != "exit_counts"},
        "exit_counts": metrics.get("exit_counts", {}),
        "verdict": verdict,
        "generated": datetime.now().isoformat(),
    }
    json_path = output_dir / f"{prefix}_summary.json"
    json_path.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8")
    print(f"  Saved: {json_path}")

    return {"equity": eq_path if not eq.empty else None,
            "trades": tr_path if not trades.empty else None,
            "summary": json_path}


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Theme Proxy Backtest")
    parser.add_argument("--variant", default="V1", choices=["V1", "V2", "V3", "ALL"])
    parser.add_argument("--start", default="2021-01-01")
    parser.add_argument("--end", default="2026-03-20")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    base = Path(__file__).resolve().parent.parent
    ohlcv_dir = base.parent / "backtest" / "data_full" / "ohlcv"
    index_file = base.parent / "backtest" / "data_full" / "index" / "KOSPI.csv"

    print("Loading OHLCV data...")
    dates, codes, close_m, open_m, high_m, volume_m = load_ohlcv_matrix(ohlcv_dir)
    kospi = load_kospi(index_file, dates)
    print(f"Loaded {len(codes)} stocks, {len(dates)} dates")

    start_dt = pd.Timestamp(args.start)
    end_dt = pd.Timestamp(args.end)

    variants = ["V1", "V2", "V3"] if args.variant == "ALL" else [args.variant]

    for v in variants:
        print(f"\nRunning Theme Proxy {v}...")
        result = run_theme_backtest(
            dates, codes, close_m, open_m, high_m, volume_m, kospi,
            variant=v, start_date=start_dt, end_date=end_dt)

        metrics = compute_metrics(result, kospi, dates)
        print_metrics(metrics, f"Theme Proxy {v} ({args.start} ~ {args.end})")

        # Save results
        out_dir = base.parent / "backtest" / "results" / "theme_proxy"
        save_results(result, metrics, out_dir,
                     label=f"Theme Proxy {v}",
                     period=f"{args.start} ~ {args.end}")

        # IS/OOS split
        eq = result["equity"]
        if not eq.empty:
            n = len(eq)
            split = int(n * 0.7)
            is_dates = eq["date"].iloc[:split].tolist()
            oos_dates = eq["date"].iloc[split:].tolist()
            if is_dates and oos_dates:
                print(f"\n  IS period:  {is_dates[0].strftime('%Y-%m-%d')} ~ "
                      f"{is_dates[-1].strftime('%Y-%m-%d')} ({len(is_dates)} days)")
                print(f"  OOS period: {oos_dates[0].strftime('%Y-%m-%d')} ~ "
                      f"{oos_dates[-1].strftime('%Y-%m-%d')} ({len(oos_dates)} days)")

                # OOS metrics
                oos_result = run_theme_backtest(
                    dates, codes, close_m, open_m, high_m, volume_m, kospi,
                    variant=v, start_date=oos_dates[0], end_date=oos_dates[-1])
                oos_metrics = compute_metrics(oos_result, kospi, dates)
                print_metrics(oos_metrics, f"Theme Proxy {v} OOS")

        # Sample trade verification
        tr = result["trades"]
        if not tr.empty and len(tr) >= 10:
            print(f"\n  === Sample Trades (first 10) ===")
            sample = tr.head(10)
            for _, row in sample.iterrows():
                print(f"    {row['code']} signal={row.get('signal_date','?')} "
                      f"entry={row['entry_date']} exit={row['exit_date']} "
                      f"open={row['entry_price']:.0f} pnl={row['pnl_pct']*100:+.1f}% "
                      f"reason={row['exit_reason']}")


if __name__ == "__main__":
    main()
