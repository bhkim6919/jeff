# -*- coding: utf-8 -*-
"""
run_regime_comparison.py
=========================
5-Case regime detector comparison backtest.

Case 0: Original (asymmetric — BEAR = below OR low breadth)
Case A: Symmetric (BEAR = below AND low breadth)
Case B: Relaxed thresholds (original structure, breadth_bull=0.45, breadth_bear=0.30)
Case C1: MA200-first (MA200 primary, breadth modifies strength)
Case C2: MA200-only (no breadth, pure trend)
"""

from __future__ import annotations

import sys
import math
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from backtest.historical_provider import HistoricalProvider, BacktestRegimeDetector
from backtest.strategy_base import StrategySelector
from backtest.strategies import TrendStrategy, MeanReversionStrategy, DefenseStrategy
from backtest.backtest_loop import BacktestEngine


# ── Configuration ────────────────────────────────────────────────────────

CASES = [
    {
        "label": "Case0_Original",
        "mode": "original",
        "breadth_bull": 0.55,
        "breadth_bear": 0.35,
    },
    {
        "label": "CaseA_Symmetric",
        "mode": "symmetric",
        "breadth_bull": 0.55,
        "breadth_bear": 0.35,
    },
    {
        "label": "CaseB_Relaxed",
        "mode": "relaxed",
        "breadth_bull": 0.45,
        "breadth_bear": 0.30,
    },
    {
        "label": "CaseC1_MA_First",
        "mode": "ma_first",
        "breadth_bull": 0.45,
        "breadth_bear": 0.35,
    },
    {
        "label": "CaseC2_MA_Only",
        "mode": "ma_only",
        "breadth_bull": 0.55,  # unused
        "breadth_bear": 0.35,  # unused
    },
]

START = "2023-06-01"
END = "2026-03-14"
INITIAL_CASH = 100_000_000
MAX_POSITIONS = 20
WEIGHT = 0.07
SIGNAL_INTERVAL = 5


def make_selector():
    """MultiStrategy selector (same for all cases)."""
    return StrategySelector({
        "BULL": TrendStrategy(top_n=30, atr_mult=2.5, rs_entry_min=0.80, max_hold=60),
        "SIDEWAYS": MeanReversionStrategy(rsi_thresh=30.0, tp_pct=0.04, sl_pct=0.02,
                                           top_n=15, max_hold=20),
        "BEAR": DefenseStrategy(max_pos=5, weight_mult=0.30, rs_min=0.90,
                                atr_max_pct=0.40, atr_mult=1.0, top_n=10, max_hold=30),
    })


def compute_metrics(result: dict, regime_history: list, index_df, dates) -> dict:
    """Compute all required comparison metrics."""
    if not result or not result.get("equity_curve"):
        return {}

    eq = [e["equity"] for e in result["equity_curve"]]
    total_return = eq[-1] / INITIAL_CASH - 1

    # CAGR
    n_days = len(eq)
    years = n_days / 252.0
    cagr = (eq[-1] / INITIAL_CASH) ** (1.0 / years) - 1 if years > 0 else 0

    # MDD
    peak = eq[0]
    mdd = 0.0
    for e in eq:
        peak = max(peak, e)
        dd = (e - peak) / peak
        mdd = min(mdd, dd)

    # Calmar
    calmar = cagr / abs(mdd) if mdd != 0 else 0

    # Trade stats
    trades = result.get("trades", [])
    n_trades = len(trades)
    wins = sum(1 for t in trades if t.pnl_pct > 0)
    win_rate = wins / n_trades if n_trades else 0

    # Regime distribution (from equity_curve which has daily regime)
    regime_days = Counter(e["regime"] for e in result["equity_curve"])
    total_days = sum(regime_days.values())

    bull_days = regime_days.get("BULL", 0)
    sw_days = regime_days.get("SIDEWAYS", 0)
    bear_days = regime_days.get("BEAR", 0)

    # MA200 above/below analysis
    import pandas as pd
    idx = index_df.copy()
    idx["ma200"] = idx["close"].rolling(200).mean()
    bt_idx = idx[(idx["date"] >= pd.Timestamp(START)) & (idx["date"] <= pd.Timestamp(END))].copy()
    bt_idx = bt_idx.dropna(subset=["ma200"])
    above_ma200_total = int((bt_idx["close"] > bt_idx["ma200"]).sum())
    below_ma200_total = len(bt_idx) - above_ma200_total

    # Match regime to MA200 status per day (approximate via equity_curve dates)
    eq_dates = {e["date"]: e["regime"] for e in result["equity_curve"]}

    above_and_bull = 0
    below_and_bear = 0
    above_total_matched = 0
    below_total_matched = 0

    for _, row in bt_idx.iterrows():
        d = row["date"]
        regime = eq_dates.get(d)
        if regime is None:
            continue
        if row["close"] > row["ma200"]:
            above_total_matched += 1
            if regime == "BULL":
                above_and_bull += 1
        else:
            below_total_matched += 1
            if regime == "BEAR":
                below_and_bear += 1

    bull_pct_when_above = above_and_bull / above_total_matched if above_total_matched else 0
    bear_pct_when_below = below_and_bear / below_total_matched if below_total_matched else 0

    # Regime transition count
    transitions = 0
    prev_regime = None
    for e in result["equity_curve"]:
        r = e["regime"]
        if prev_regime is not None and r != prev_regime:
            transitions += 1
        prev_regime = r

    return {
        "label": result["label"],
        "total_return": total_return,
        "cagr": cagr,
        "mdd": mdd,
        "calmar": calmar,
        "n_trades": n_trades,
        "win_rate": win_rate,
        "bull_days": bull_days,
        "sw_days": sw_days,
        "bear_days": bear_days,
        "bull_pct": bull_days / total_days if total_days else 0,
        "sw_pct": sw_days / total_days if total_days else 0,
        "bear_pct": bear_days / total_days if total_days else 0,
        "bull_pct_when_above_ma200": bull_pct_when_above,
        "bear_pct_when_below_ma200": bear_pct_when_below,
        "transitions": transitions,
        "final_equity": eq[-1],
    }


def print_comparison(all_metrics: list):
    """Print side-by-side comparison table."""
    print("\n" + "=" * 120)
    print("  Regime Detector Comparison: 5-Case Backtest")
    print("=" * 120)

    # Index return for reference
    print(f"  Index return (backtest period): +153.4%")
    print(f"  Period: {START} ~ {END}\n")

    labels = [m["label"] for m in all_metrics]

    # Header
    hdr = f"{'Metric':<30}"
    for lb in labels:
        hdr += f" {lb:>16}"
    print(hdr)
    print("-" * 120)

    rows = [
        ("BULL days",        lambda m: f"{m['bull_days']}"),
        ("SIDEWAYS days",    lambda m: f"{m['sw_days']}"),
        ("BEAR days",        lambda m: f"{m['bear_days']}"),
        ("BULL %",           lambda m: f"{m['bull_pct']*100:.1f}%"),
        ("SIDEWAYS %",       lambda m: f"{m['sw_pct']*100:.1f}%"),
        ("BEAR %",           lambda m: f"{m['bear_pct']*100:.1f}%"),
        ("---", None),
        ("BULL% when >MA200", lambda m: f"{m['bull_pct_when_above_ma200']*100:.1f}%"),
        ("BEAR% when <MA200", lambda m: f"{m['bear_pct_when_below_ma200']*100:.1f}%"),
        ("Regime transitions", lambda m: f"{m['transitions']}"),
        ("---", None),
        ("Total Return",     lambda m: f"{m['total_return']*100:+.2f}%"),
        ("CAGR",             lambda m: f"{m['cagr']*100:+.2f}%"),
        ("MDD",              lambda m: f"{m['mdd']*100:.2f}%"),
        ("Calmar",           lambda m: f"{m['calmar']:.2f}"),
        ("Trades",           lambda m: f"{m['n_trades']}"),
        ("Win Rate",         lambda m: f"{m['win_rate']*100:.1f}%"),
        ("Final Equity",     lambda m: f"{m['final_equity']:,.0f}"),
    ]

    for label, fmt in rows:
        if fmt is None:
            print("-" * 120)
            continue
        line = f"{label:<30}"
        for m in all_metrics:
            try:
                line += f" {fmt(m):>16}"
            except Exception:
                line += f" {'N/A':>16}"
        print(line)

    # Warnings
    print("\n" + "=" * 120)
    print("  Warnings & Analysis")
    print("=" * 120)

    base_transitions = all_metrics[0]["transitions"]
    for m in all_metrics:
        warnings = []
        if m["transitions"] > base_transitions * 2:
            warnings.append(f"WARN: transitions {m['transitions']} >> baseline {base_transitions} (2x+)")
        if m["bull_pct_when_above_ma200"] < 0.30:
            warnings.append(f"WARN: BULL only {m['bull_pct_when_above_ma200']*100:.0f}% when above MA200 (too low)")
        if m["bear_pct"] > 0.50 and m["bull_pct_when_above_ma200"] < 0.50:
            warnings.append(f"WARN: BEAR overweight ({m['bear_pct']*100:.0f}%) despite above-MA200 BULL being low")

        status = " | ".join(warnings) if warnings else "OK"
        print(f"  [{m['label']}] {status}")

    # Structure vs Tuning verdict
    print("\n" + "-" * 120)
    print("  Verdict: Structure change vs Threshold tuning")
    print("-" * 120)

    # Compare Case0 vs CaseA (structure only, same thresholds)
    c0 = all_metrics[0]
    ca = all_metrics[1]
    cb = all_metrics[2]

    struct_delta_return = ca["total_return"] - c0["total_return"]
    struct_delta_bull = ca["bull_pct_when_above_ma200"] - c0["bull_pct_when_above_ma200"]

    tune_delta_return = cb["total_return"] - c0["total_return"]
    tune_delta_bull = cb["bull_pct_when_above_ma200"] - c0["bull_pct_when_above_ma200"]

    print(f"  Structure fix (A vs 0): return {struct_delta_return*100:+.2f}%, "
          f"BULL-above-MA200 {struct_delta_bull*100:+.1f}pp")
    print(f"  Threshold tune (B vs 0): return {tune_delta_return*100:+.2f}%, "
          f"BULL-above-MA200 {tune_delta_bull*100:+.1f}pp")

    if abs(struct_delta_bull) > abs(tune_delta_bull):
        print("  >> Structure change (AND/OR asymmetry fix) has MORE impact than threshold tuning")
    else:
        print("  >> Threshold tuning has MORE impact than structure change")

    print("=" * 120)


def main():
    # Load data once
    data_dir = ROOT / "data"
    provider = HistoricalProvider(
        ohlcv_dir=str(data_dir / "ohlcv_kospi_daily"),
        index_file=str(data_dir / "kospi_index_daily_5y.csv"),
        universe_file=str(data_dir / "universe_kospi.csv"),
        sector_map_path=str(data_dir / "sector_map.json"),
    )
    provider.load_all(min_rows=130)

    all_metrics = []

    for case in CASES:
        print(f"\n{'='*60}")
        print(f"  Running: {case['label']}")
        print(f"  mode={case['mode']}, breadth_bull={case['breadth_bull']}, "
              f"breadth_bear={case['breadth_bear']}")
        print(f"{'='*60}")

        det = BacktestRegimeDetector(
            ma_period=200,
            breadth_bull=case["breadth_bull"],
            breadth_bear=case["breadth_bear"],
            mode=case["mode"],
        )

        engine = BacktestEngine(
            provider=provider,
            selector=make_selector(),
            regime_detector=det,
            initial_cash=INITIAL_CASH,
            max_positions=MAX_POSITIONS,
            weight_per_pos=WEIGHT,
            signal_interval=SIGNAL_INTERVAL,
            label=case["label"],
        )
        result = engine.run(START, END, progress=True)

        if result:
            metrics = compute_metrics(
                result, engine.regime_history, provider.index_df,
                provider.get_trade_dates(START, END),
            )
            all_metrics.append(metrics)

    if all_metrics:
        print_comparison(all_metrics)


if __name__ == "__main__":
    main()
