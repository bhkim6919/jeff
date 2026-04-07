"""
theme_proxy_compare.py — Core vs Theme vs Core+Theme comparison
================================================================
Compares Gen4 Core (external equity CSV) with Theme Proxy on identical data.

Usage:
  python -m backtest.theme_proxy_compare
  python -m backtest.theme_proxy_compare --core-equity ../backtest/results/gen4_core_equity.csv
"""
from __future__ import annotations
import argparse
import logging
import sys
import warnings
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backtest.theme_proxy_backtest import (
    load_ohlcv_matrix, load_kospi,
    run_theme_backtest, compute_metrics, print_metrics, save_results,
    INITIAL_CASH, BUY_COST, SELL_COST,
)
from strategy.scoring import calc_volatility, calc_momentum

logger = logging.getLogger("theme_compare")


# ── Load external Core equity ──────────────────────────────────────────────

def load_external_core(equity_path: Path, start_date=None, end_date=None) -> dict:
    """Load validated Core v7 equity CSV as a result dict."""
    df = pd.read_csv(equity_path, parse_dates=["date"])
    if "equity" not in df.columns:
        raise ValueError(f"Core equity CSV must have 'equity' column: {equity_path}")

    if "n_positions" not in df.columns:
        df["n_positions"] = 20  # assume full allocation

    if start_date:
        df = df[df["date"] >= start_date]
    if end_date:
        df = df[df["date"] <= end_date]
    df = df.reset_index(drop=True)

    if df.empty:
        raise ValueError(f"No data in date range: {equity_path}")

    ic = float(df["equity"].iloc[0])
    fe = float(df["equity"].iloc[-1])

    return {
        "variant": "Gen4_Core",
        "config": {"source": "external", "file": str(equity_path)},
        "equity": df[["date", "equity", "n_positions"]],
        "trades": pd.DataFrame(),  # trades not available from equity CSV
        "initial_cash": ic,
        "final_equity": fe,
    }


# ── Gen4 Core Backtest (experimental, NOT default) ─────────────────────────

def _run_gen4_core_experimental(dates, codes, close_m, open_m, high_m, volume_m, kospi,
                                 start_date=None, end_date=None) -> dict:
    """Simplified Gen4 Core: LowVol+Mom12-1, monthly rebal, trail -12%.
    WARNING: This is an experimental reimplementation. Use external Core
    equity CSV for validated comparisons."""
    cash = float(INITIAL_CASH)
    positions = {}
    trades = []
    equity_curve = []
    rebal_counter = 0

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
        # Trail stop check
        for code in list(positions.keys()):
            pos = positions[code]
            c = close_m[code]
            if t >= len(c) or np.isnan(c[t]) or c[t] <= 0:
                continue
            price = c[t]
            pos["hwm"] = max(pos["hwm"], price)
            dd = (price - pos["hwm"]) / pos["hwm"]
            if dd <= -0.12:
                proceeds = pos["qty"] * price * (1 - SELL_COST)
                cash += proceeds
                trades.append({
                    "code": code, "entry_date": dates[pos["entry_idx"]],
                    "exit_date": dates[t], "entry_price": pos["entry_price"],
                    "exit_price": price, "quantity": pos["qty"],
                    "pnl_pct": price / pos["entry_price"] - 1,
                    "pnl_amount": pos["qty"] * (price - pos["entry_price"]),
                    "hold_days": t - pos["entry_idx"], "exit_reason": "TRAIL",
                })
                del positions[code]

        # Monthly rebalance
        rebal_counter += 1
        if rebal_counter >= 21 and t < end_idx:
            rebal_counter = 0
            scored = []
            for code in codes:
                c = close_m[code]
                if t >= len(c) or np.isnan(c[t]):
                    continue
                cs = c[:t + 1]
                cs = cs[~np.isnan(cs)]
                if len(cs) < 252 or cs[-1] < 2000:
                    continue
                v = volume_m[code][:t + 1]
                amt = cs[-20:] * v[t - 19:t + 1] if t >= 19 else np.array([0])
                if np.mean(amt) < 2e9:
                    continue
                vol = calc_volatility(pd.Series(cs), 252)
                mom = calc_momentum(pd.Series(cs), 252, 22)
                if np.isnan(vol) or np.isnan(mom) or mom <= 0:
                    continue
                scored.append({"code": code, "vol": vol, "mom": mom,
                               "close": float(cs[-1])})

            if scored:
                df = pd.DataFrame(scored)
                vol_thresh = df["vol"].quantile(0.30)
                low_vol = df[df["vol"] <= vol_thresh]
                candidates = low_vol[low_vol["mom"] > 0].nlargest(20, "mom")
                target = set(candidates["code"].tolist())

                for code in list(positions.keys()):
                    if code not in target:
                        pos = positions[code]
                        price = close_m[code][t]
                        proceeds = pos["qty"] * price * (1 - SELL_COST)
                        cash += proceeds
                        trades.append({
                            "code": code, "entry_date": dates[pos["entry_idx"]],
                            "exit_date": dates[t], "entry_price": pos["entry_price"],
                            "exit_price": price, "quantity": pos["qty"],
                            "pnl_pct": price / pos["entry_price"] - 1,
                            "pnl_amount": pos["qty"] * (price - pos["entry_price"]),
                            "hold_days": t - pos["entry_idx"],
                            "exit_reason": "REBAL",
                        })
                        del positions[code]

                to_buy = target - set(positions.keys())
                if to_buy and t + 1 <= end_idx:
                    equity = cash + sum(
                        pos["qty"] * close_m[cd][t]
                        for cd, pos in positions.items()
                        if t < len(close_m[cd]))
                    alloc = equity / 20
                    for code in list(to_buy):
                        o = open_m[code]
                        if t + 1 >= len(o) or np.isnan(o[t + 1]) or o[t + 1] <= 0:
                            continue
                        ep = o[t + 1]
                        qty = int(alloc / (ep * (1 + BUY_COST)))
                        if qty <= 0 or qty * ep * (1 + BUY_COST) > cash:
                            continue
                        cash -= qty * ep * (1 + BUY_COST)
                        positions[code] = {
                            "entry_price": ep, "qty": qty,
                            "entry_idx": t + 1, "hwm": ep,
                        }

        mtm = cash
        for code, pos in positions.items():
            c = close_m[code]
            p = c[t] if t < len(c) and not np.isnan(c[t]) else pos["entry_price"]
            mtm += pos["qty"] * p
        equity_curve.append({"date": dates[t], "equity": mtm,
                             "n_positions": len(positions)})

    for code in list(positions.keys()):
        pos = positions[code]
        price = close_m[code][end_idx]
        cash += pos["qty"] * price * (1 - SELL_COST)
        trades.append({
            "code": code, "entry_date": dates[pos["entry_idx"]],
            "exit_date": dates[end_idx], "entry_price": pos["entry_price"],
            "exit_price": price, "quantity": pos["qty"],
            "pnl_pct": price / pos["entry_price"] - 1,
            "pnl_amount": pos["qty"] * (price - pos["entry_price"]),
            "hold_days": end_idx - pos["entry_idx"], "exit_reason": "EOD",
        })

    return {
        "variant": "Gen4_Core",
        "config": {"trail": -0.12, "rebal": 21, "n_stocks": 20,
                    "source": "internal_experimental"},
        "equity": pd.DataFrame(equity_curve),
        "trades": pd.DataFrame(trades) if trades else pd.DataFrame(),
        "initial_cash": INITIAL_CASH,
        "final_equity": equity_curve[-1]["equity"] if equity_curve else INITIAL_CASH,
    }


# ── Combined Core + Theme ──────────────────────────────────────────────────

def run_core_plus_theme(core_equity_df, theme_result,
                        theme_weight: float = 0.10) -> dict:
    """Combine Core equity curve + Theme equity curve with given weights."""
    core_eq = core_equity_df.set_index("date")["equity"]
    theme_eq = theme_result["equity"].set_index("date")["equity"]
    theme_ic = theme_result["initial_cash"]

    core_start = float(core_eq.iloc[0])
    core_weight = 1 - theme_weight

    combined = []
    for d in core_eq.index:
        # Normalize both to return-based combination
        ce_ret = core_eq.get(d, core_start) / core_start
        te_ret = theme_eq.get(d, theme_ic) / theme_ic if d in theme_eq.index else 1.0
        combined_ret = ce_ret * core_weight + te_ret * theme_weight
        combined.append({
            "date": d,
            "equity": combined_ret * INITIAL_CASH,
            "n_positions": 0,
        })

    return {
        "variant": f"Core{int(core_weight*100)}_Theme{int(theme_weight*100)}",
        "config": {"core_weight": core_weight, "theme_weight": theme_weight},
        "equity": pd.DataFrame(combined),
        "trades": pd.DataFrame(),
        "initial_cash": INITIAL_CASH,
        "final_equity": combined[-1]["equity"] if combined else INITIAL_CASH,
    }


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Core vs Theme Comparison")
    parser.add_argument("--start", default="2021-01-01")
    parser.add_argument("--end", default="2026-03-20")
    parser.add_argument("--theme-weight", type=float, default=0.10)
    parser.add_argument("--core-equity", default=None,
                        help="Path to validated Core v7 equity CSV (recommended)")
    parser.add_argument("--use-internal-core", action="store_true",
                        help="Use experimental internal Core (NOT recommended)")
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

    sd = pd.Timestamp(args.start)
    ed = pd.Timestamp(args.end)

    out_dir = base.parent / "backtest" / "results" / "comparison"
    period = f"{args.start} ~ {args.end}"

    # ── A. Load Core baseline ──
    core_equity_path = Path(args.core_equity) if args.core_equity else None
    if core_equity_path is None:
        # Auto-detect default location
        default_path = base.parent / "backtest" / "results" / "gen4_core" / "equity.csv"
        if default_path.exists():
            core_equity_path = default_path

    use_external = (core_equity_path and core_equity_path.exists()
                    and not args.use_internal_core)

    if use_external:
        print(f"\n[A] Loading Core baseline: EXTERNAL {core_equity_path}")
        core = load_external_core(core_equity_path, start_date=sd, end_date=ed)
    else:
        if not args.use_internal_core:
            print("\n[A] WARNING: No external Core equity found.")
            print("    Using experimental internal Core (results may differ from validated).")
            print("    Run Gen04/backtester.py first and provide --core-equity path.")
        else:
            print("\n[A] Using experimental internal Core (--use-internal-core)")
        core = _run_gen4_core_experimental(
            dates, codes, close_m, open_m, high_m, volume_m, kospi,
            start_date=sd, end_date=ed)

    core_m = compute_metrics(core, kospi, dates)
    print_metrics(core_m, f"Gen4 Core (source={core['config'].get('source', 'unknown')})")
    save_results(core, core_m, out_dir, label="Gen4 Core", period=period)

    # ── B. Theme Proxy V1 ──
    print("\n[B] Running Theme Proxy V1...")
    theme = run_theme_backtest(dates, codes, close_m, open_m, high_m, volume_m, kospi,
                                variant="V1", start_date=sd, end_date=ed)
    theme_m = compute_metrics(theme, kospi, dates)
    print_metrics(theme_m, "Theme Proxy V1 (SL-8%/TP+20%/5d)")
    save_results(theme, theme_m, out_dir, label="Theme Proxy V1", period=period,
                 verdict="REJECT" if theme_m["total_return"] < 0 else "REVIEW")

    # ── C. Core + Theme ──
    core_eq_df = core["equity"]
    for tw in [0.05, 0.10, 0.15]:
        print(f"\n[C] Core+Theme ({int(tw*100)}%)...")
        combo = run_core_plus_theme(core_eq_df, theme, theme_weight=tw)
        combo_m = compute_metrics(combo, kospi, dates)
        print_metrics(combo_m, f"Core{int((1-tw)*100)}+Theme{int(tw*100)}")
        save_results(combo, combo_m, out_dir,
                     label=f"Core{int((1-tw)*100)}+Theme{int(tw*100)}", period=period)

    # ── Summary Table ──
    print("\n" + "=" * 80)
    print("  COMPARISON SUMMARY")
    print("=" * 80)
    print(f"  Baseline: {'EXTERNAL ' + str(core_equity_path) if use_external else 'INTERNAL experimental'}")
    print()
    print(f"{'Strategy':<30} {'Return':>8} {'CAGR':>7} {'MDD':>8} {'Sharpe':>7} {'PF':>5} {'WR':>6} {'Trades':>7}")
    print("-" * 80)
    for label, m in [
        ("Gen4 Core", core_m),
        ("Theme Proxy V1", theme_m),
    ]:
        pf = m.get('profit_factor', 0)
        wr = m.get('win_rate', 0)
        nt = m.get('n_trades', 0)
        print(f"{label:<30} {m['total_return']*100:>+7.1f}% {m['cagr']*100:>+6.1f}% "
              f"{m['mdd']*100:>7.1f}% {m['sharpe']:>7.2f} {pf:>5.2f} "
              f"{wr*100:>5.1f}% {nt:>7}")

    # ── OOS Split ──
    print("\n" + "=" * 80)
    print("  OOS ANALYSIS (last 30%)")
    print("=" * 80)

    core_eq = core["equity"]
    if not core_eq.empty:
        split = int(len(core_eq) * 0.7)
        oos_start = core_eq["date"].iloc[split]
        print(f"  OOS start: {oos_start.strftime('%Y-%m-%d')}")

        # Core OOS: slice equity directly
        core_oos = {
            "variant": "Gen4_Core_OOS",
            "config": core["config"],
            "equity": core_eq.iloc[split:].reset_index(drop=True),
            "trades": pd.DataFrame(),
            "initial_cash": float(core_eq["equity"].iloc[split]),
            "final_equity": float(core_eq["equity"].iloc[-1]),
        }

        theme_oos = run_theme_backtest(dates, codes, close_m, open_m, high_m, volume_m, kospi,
                                        variant="V1", start_date=oos_start, end_date=ed)

        core_oos_m = compute_metrics(core_oos, kospi, dates)
        theme_oos_m = compute_metrics(theme_oos, kospi, dates)

        print(f"\n{'Strategy':<30} {'Return':>8} {'MDD':>8} {'Sharpe':>7}")
        print("-" * 55)
        print(f"{'Core OOS':<30} {core_oos_m['total_return']*100:>+7.1f}% "
              f"{core_oos_m['mdd']*100:>7.1f}% {core_oos_m['sharpe']:>7.2f}")
        print(f"{'Theme OOS':<30} {theme_oos_m['total_return']*100:>+7.1f}% "
              f"{theme_oos_m['mdd']*100:>7.1f}% {theme_oos_m['sharpe']:>7.2f}")

    # ── Final Verdict ──
    print("\n" + "=" * 80)
    print("  VERDICT")
    print("=" * 80)

    theme_viable = (theme_m["total_return"] > 0 and
                    theme_m["sharpe"] > 0.3 and
                    theme_m.get("profit_factor", 0) > 1.0)
    oos_viable = (theme_oos_m["total_return"] > 0 and
                  theme_oos_m["sharpe"] > 0) if not core_eq.empty else False

    if theme_viable and oos_viable:
        print("  Theme Proxy: ADOPT (conditional)")
        print("  - Positive return, acceptable Sharpe, OOS holds")
    elif theme_viable and not oos_viable:
        print("  Theme Proxy: HOLD (OOS degraded)")
        print("  - IS looks good but OOS is weak — monitor with small allocation")
    else:
        print("  Theme Proxy: REJECT")
        print("  - Does not meet minimum viability criteria")

    core_calmar = core_m.get("calmar", 0)
    if core_calmar > 0:
        print(f"\n  Core Calmar: {core_calmar:.2f}")
        print(f"  Recommendation: {'Core alone is strong enough' if core_calmar > 1.5 else 'Small satellite may help diversify'}")


if __name__ == "__main__":
    main()
