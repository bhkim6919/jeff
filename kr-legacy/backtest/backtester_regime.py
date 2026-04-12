"""
backtester_regime.py - Gen4 Regime Filter + Exposure Overlay Backtest (v2)
==========================================================================
Extends Gen4 core strategy (LowVol+Momentum 12-1) with:
  1. Market Regime Filter (BULL/SIDE/BEAR) - skip buys in BEAR
  2. Exposure Overlay - reduce CASH ALLOCATION per regime (not stock count)

Key design (v2 changes):
  - Exposure = total_eq * exposure_ratio / N_STOCKS per position
    (stock count stays at N_STOCKS, cash allocation scales down)
  - Regime decomposition uses CONTIGUOUS segments (not scattered days)
  - Symmetric BEAR rule variant added for comparison
  - Rebalance allocation log for debugging under/over-investment

Runs 6 configurations × IS/OOS split for comparison.
Does NOT modify existing backtester.py.

Usage:
    cd kr-legacy
    python -m backtest.backtester_regime
"""
from __future__ import annotations
import sys
import time
import argparse
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import Gen4Config
from strategy.scoring import calc_volatility, calc_momentum
from backtest.backtester import (
    load_ohlcv, build_matrices, get_universe, calc_metrics, print_results,
)

# ── Regime Constants ─────────────────────────────────────────────────────────
EXPOSURE_MAP = {"BULL": 1.0, "SIDE": 0.7, "BEAR": 0.4}
BREADTH_BULL = 0.60
BREADTH_BEAR = 0.40
MA_WINDOW = 200


# ── Regime Detection ─────────────────────────────────────────────────────────
def calc_regime(idx_close_val: float, kospi_ma200_val: float,
                breadth: float, i: int, symmetric: bool = False) -> str:
    """
    Market regime classification.

    Asymmetric (default):
      BULL: KOSPI > MA200 AND breadth > 60%
      BEAR: KOSPI < MA200 OR  breadth < 40%
      SIDE: else

    Symmetric (symmetric=True):
      BULL: KOSPI > MA200 AND breadth > 60%
      BEAR: KOSPI < MA200 AND breadth < 40%
      SIDE: else
    """
    if i < MA_WINDOW:
        return "SIDE"
    if np.isnan(kospi_ma200_val) or kospi_ma200_val <= 0:
        return "SIDE"

    kospi_above = idx_close_val > kospi_ma200_val
    breadth_healthy = breadth > BREADTH_BULL
    breadth_weak = breadth < BREADTH_BEAR

    if kospi_above and breadth_healthy:
        return "BULL"
    if symmetric:
        if (not kospi_above) and breadth_weak:
            return "BEAR"
    else:
        if (not kospi_above) or breadth_weak:
            return "BEAR"
    return "SIDE"


def calc_breadth(close_row: pd.Series, ma200_row: pd.Series) -> float:
    """% of valid stocks trading above their own 200-day MA."""
    valid = close_row[(close_row > 0) & close_row.notna()]
    ma_valid = ma200_row.reindex(valid.index)
    ma_valid = ma_valid[ma_valid.notna() & (ma_valid > 0)]
    if len(ma_valid) == 0:
        return 0.5
    common = valid.index.intersection(ma_valid.index)
    return float((valid[common] > ma_valid[common]).mean())


# ── Core Backtest with Regime ────────────────────────────────────────────────
def run_backtest_regime(close, opn, high, low, vol, idx_close, dates,
                        start_i: int, end_i: int, config: Gen4Config,
                        regime_filter: bool = False,
                        exposure_overlay: bool = False,
                        kospi_ma200: pd.Series = None,
                        stock_ma200: pd.DataFrame = None,
                        symmetric_bear: bool = False,
                        ) -> Tuple[pd.Series, list, list, list]:
    """
    Gen4 backtest with optional regime filter and exposure overlay.

    regime_filter=True:    BEAR → skip new buys (sells/trail still active)
    exposure_overlay=True: scale per-position cash allocation by regime
                           (stock count stays at N_STOCKS, investment amount scales)
    symmetric_bear=True:   use AND logic for BEAR (stricter)

    Returns: (equity_series, trades, regime_log, rebal_log)
    """
    cash = float(config.INITIAL_CASH)
    positions = {}
    pending_buys = []
    trades = []
    equity_hist = {}
    regime_log = []
    rebal_log = []  # allocation debugging

    last_rebal = -999

    for i in range(start_i, end_i + 1):
        dt = dates[i]

        # ── Regime calculation (daily) ───────────────────────────
        breadth = 0.5
        regime = "SIDE"
        if kospi_ma200 is not None and stock_ma200 is not None and i >= MA_WINDOW:
            breadth = calc_breadth(close.iloc[i], stock_ma200.iloc[i])
            regime = calc_regime(
                float(idx_close.iloc[i]),
                float(kospi_ma200.iloc[i]),
                breadth, i,
                symmetric=symmetric_bear)
        regime_log.append({
            "date": str(dt.date()),
            "kospi": round(float(idx_close.iloc[i]), 2),
            "kospi_ma200": round(float(kospi_ma200.iloc[i]), 2) if kospi_ma200 is not None and i < len(kospi_ma200) and not np.isnan(kospi_ma200.iloc[i]) else 0,
            "breadth": round(breadth, 4),
            "regime": regime,
        })

        # ── 0) Fill pending buys at today's open ────────────────
        for pb in list(pending_buys):
            if i != pb["target_idx"]:
                continue
            tk = pb["tk"]
            if tk in positions:
                pending_buys.remove(pb)
                continue

            entry_price = float(opn[tk].iloc[i]) if not pd.isna(opn[tk].iloc[i]) else 0
            if entry_price <= 0:
                pending_buys.remove(pb)
                continue

            per_pos = pb["per_pos"]
            buy_cost_total = entry_price * (1 + config.BUY_COST)
            qty = int(min(per_pos, cash * 0.95) / buy_cost_total)
            if qty <= 0 or qty * buy_cost_total > cash:
                pending_buys.remove(pb)
                continue

            actual_invested = qty * buy_cost_total
            cash -= actual_invested
            positions[tk] = dict(
                qty=qty,
                entry_price=entry_price,
                entry_idx=i,
                high_wm=entry_price,
                buy_cost_total=qty * entry_price * config.BUY_COST,
            )
            pending_buys.remove(pb)

        pending_buys = [pb for pb in pending_buys if pb["target_idx"] >= i]

        # ── 1) Trail Stop (close-based) ─────────────────────────
        for tk in list(positions.keys()):
            pos = positions[tk]
            p = float(close[tk].iloc[i])
            if p <= 0 or pd.isna(p):
                continue
            if p > pos["high_wm"]:
                pos["high_wm"] = p
            dd = (p - pos["high_wm"]) / pos["high_wm"] if pos["high_wm"] > 0 else 0
            if dd <= -config.TRAIL_PCT:
                net = pos["qty"] * p * (1 - config.SELL_COST)
                invested = pos["qty"] * pos["entry_price"] + pos["buy_cost_total"]
                pnl = (net - invested) / invested if invested > 0 else 0
                cash += net
                trades.append(dict(
                    ticker=tk,
                    entry_date=str(dates[pos["entry_idx"]].date()),
                    exit_date=str(dt.date()),
                    entry_price=pos["entry_price"],
                    exit_price=p,
                    pnl_pct=pnl,
                    pnl_amount=net - invested,
                    hold_days=i - pos["entry_idx"],
                    exit_reason="TRAIL",
                ))
                del positions[tk]

        # ── 2) Monthly Rebalance ────────────────────────────────
        if i - last_rebal >= config.REBAL_DAYS:
            last_rebal = i

            universe = get_universe(close, vol, i,
                                    config.UNIV_MIN_CLOSE, config.UNIV_MIN_AMOUNT)

            scored = []
            for tk in universe:
                series = close[tk].iloc[:i+1]
                if len(series) < max(config.VOL_LOOKBACK, config.MOM_LOOKBACK):
                    continue
                c_val = float(series.iloc[-1])
                if c_val <= 0 or pd.isna(c_val):
                    continue
                v = calc_volatility(series, config.VOL_LOOKBACK)
                if np.isnan(v):
                    continue
                m = calc_momentum(series, config.MOM_LOOKBACK, config.MOM_SKIP)
                if np.isnan(m):
                    continue
                scored.append({"tk": tk, "vol": v, "mom": m})

            if scored:
                sdf = pd.DataFrame(scored)
                vol_thresh = sdf["vol"].quantile(config.VOL_PERCENTILE)
                low_vol = sdf[sdf["vol"] <= vol_thresh]
                candidates = low_vol[low_vol["mom"] > 0]

                # Stock count always N_STOCKS (v2: exposure scales cash, not count)
                n_target = config.N_STOCKS
                top = candidates.sort_values("mom", ascending=False).head(n_target)
                target_codes = set(top["tk"].tolist())

                # ── Sell non-targets ────────────────────────────
                for tk in list(positions.keys()):
                    if tk not in target_codes:
                        pos = positions[tk]
                        p = float(close[tk].iloc[i])
                        if p <= 0 or pd.isna(p):
                            p = pos["entry_price"]
                        net = pos["qty"] * p * (1 - config.SELL_COST)
                        invested = pos["qty"] * pos["entry_price"] + pos["buy_cost_total"]
                        pnl = (net - invested) / invested if invested > 0 else 0
                        cash += net
                        trades.append(dict(
                            ticker=tk,
                            entry_date=str(dates[pos["entry_idx"]].date()),
                            exit_date=str(dt.date()),
                            entry_price=pos["entry_price"],
                            exit_price=p,
                            pnl_pct=pnl,
                            pnl_amount=net - invested,
                            hold_days=i - pos["entry_idx"],
                            exit_reason="REBALANCE",
                        ))
                        del positions[tk]

                # ── Regime filter: skip buys in BEAR ────────────
                skip_buys = regime_filter and regime == "BEAR"

                # ── Queue buys for T+1 open ─────────────────────
                if not skip_buys:
                    pv_held = sum(pos["qty"] * float(close[c].iloc[i])
                                  for c, pos in positions.items()
                                  if float(close[c].iloc[i]) > 0)
                    total_eq = cash + pv_held

                    # v2: Exposure overlay scales CASH ALLOCATION, not stock count
                    exposure_ratio = 1.0
                    if exposure_overlay:
                        exposure_ratio = EXPOSURE_MAP.get(regime, 1.0)

                    new_codes = [c for c in target_codes if c not in positions]
                    slots = n_target - len(positions) - len(pending_buys)

                    # per_pos = (total_eq * exposure_ratio) / n_target
                    per_pos = (total_eq * exposure_ratio) / n_target

                    if new_codes and slots > 0 and i + 1 <= end_i:
                        actual_buys = min(len(new_codes), slots)
                        for tk in new_codes[:slots]:
                            pending_buys.append({
                                "tk": tk,
                                "target_idx": i + 1,
                                "per_pos": per_pos,
                            })

                        # Rebalance allocation log (Test 5)
                        rebal_log.append({
                            "date": str(dt.date()),
                            "regime": regime,
                            "exposure_ratio": exposure_ratio,
                            "total_eq": round(total_eq),
                            "n_target": n_target,
                            "n_held": len(positions),
                            "n_new_buys": actual_buys,
                            "intended_per_pos": round(per_pos),
                            "cash_before": round(cash),
                            "cash_ratio": round(cash / total_eq, 4) if total_eq > 0 else 0,
                            "skip_buys": skip_buys,
                        })
                elif skip_buys:
                    pv_held = sum(pos["qty"] * float(close[c].iloc[i])
                                  for c, pos in positions.items()
                                  if float(close[c].iloc[i]) > 0)
                    total_eq = cash + pv_held
                    rebal_log.append({
                        "date": str(dt.date()),
                        "regime": regime,
                        "exposure_ratio": 0.0,
                        "total_eq": round(total_eq),
                        "n_target": n_target,
                        "n_held": len(positions),
                        "n_new_buys": 0,
                        "intended_per_pos": 0,
                        "cash_before": round(cash),
                        "cash_ratio": round(cash / total_eq, 4) if total_eq > 0 else 0,
                        "skip_buys": True,
                    })

        # ── Equity snapshot ──────────────────────────────────────
        pv = cash
        for tk, pos in positions.items():
            c = float(close[tk].iloc[i])
            if c > 0 and not pd.isna(c):
                pv += pos["qty"] * c
        equity_hist[dt] = pv

    # Close remaining
    for tk, pos in list(positions.items()):
        p = float(close[tk].iloc[end_i])
        if p > 0 and not pd.isna(p):
            net = pos["qty"] * p * (1 - config.SELL_COST)
            invested = pos["qty"] * pos["entry_price"] + pos["buy_cost_total"]
            pnl = (net - invested) / invested if invested > 0 else 0
            trades.append(dict(
                ticker=tk,
                entry_date=str(dates[pos["entry_idx"]].date()),
                exit_date=str(dates[end_i].date()),
                entry_price=pos["entry_price"],
                exit_price=p,
                pnl_pct=pnl,
                pnl_amount=net - invested,
                hold_days=end_i - pos["entry_idx"],
                exit_reason="EOD",
            ))

    return pd.Series(equity_hist).sort_index(), trades, regime_log, rebal_log


# ── Regime Decomposition: Contiguous Segments ────────────────────────────────
def decompose_by_regime_segments(eq: pd.Series, regime_log: list) -> dict:
    """
    Break equity into CONTIGUOUS regime segments.
    Returns per-regime: {days, pct_time, n_segments, avg_segment_return,
                         median_segment_return, weighted_ann_return, worst_mdd}
    """
    if not regime_log:
        return {}

    rl = pd.DataFrame(regime_log)
    rl["date"] = pd.to_datetime(rl["date"])

    # Identify contiguous segments
    rl["regime_shift"] = (rl["regime"] != rl["regime"].shift()).cumsum()
    segments = []
    for _, grp in rl.groupby("regime_shift"):
        segments.append({
            "regime": grp["regime"].iloc[0],
            "start": grp["date"].iloc[0],
            "end": grp["date"].iloc[-1],
            "days": len(grp),
        })

    result = {}
    total_days = len(rl)

    for regime_name in ["BULL", "SIDE", "BEAR"]:
        segs = [s for s in segments if s["regime"] == regime_name]
        days = sum(s["days"] for s in segs)
        if days < 2 or not segs:
            result[regime_name] = {
                "days": days, "pct_time": round(days / total_days * 100, 1),
                "n_segments": len(segs), "avg_return": 0, "median_return": 0,
                "weighted_ann_return": 0, "worst_mdd": 0,
            }
            continue

        seg_returns = []
        seg_mdds = []
        for s in segs:
            eq_seg = eq.loc[s["start"]:s["end"]].dropna()
            if len(eq_seg) < 2:
                continue
            ret = float(eq_seg.iloc[-1] / eq_seg.iloc[0] - 1)
            seg_returns.append({"ret": ret, "days": len(eq_seg)})
            pk = eq_seg.expanding().max()
            dd = (eq_seg - pk) / pk
            seg_mdds.append(float(dd.min()))

        if not seg_returns:
            result[regime_name] = {
                "days": days, "pct_time": round(days / total_days * 100, 1),
                "n_segments": len(segs), "avg_return": 0, "median_return": 0,
                "weighted_ann_return": 0, "worst_mdd": 0,
            }
            continue

        rets = [s["ret"] for s in seg_returns]
        # Day-weighted annualized return
        total_seg_days = sum(s["days"] for s in seg_returns)
        weighted_daily = sum(s["ret"] / max(s["days"], 1) * s["days"]
                             for s in seg_returns) / total_seg_days if total_seg_days > 0 else 0
        weighted_ann = weighted_daily * 252

        result[regime_name] = {
            "days": days,
            "pct_time": round(days / total_days * 100, 1),
            "n_segments": len(segs),
            "avg_return": round(np.mean(rets) * 100, 2),
            "median_return": round(np.median(rets) * 100, 2),
            "weighted_ann_return": round(weighted_ann * 100, 1),
            "worst_mdd": round(min(seg_mdds) * 100, 1) if seg_mdds else 0,
        }

    return result


# ── Main Comparison Runner ───────────────────────────────────────────────────
CONFIGS = [
    {"name": "Baseline",              "regime_filter": False, "exposure_overlay": False, "symmetric": False},
    {"name": "Regime Only",           "regime_filter": True,  "exposure_overlay": False, "symmetric": False},
    {"name": "Exposure Only",         "regime_filter": False, "exposure_overlay": True,  "symmetric": False},
    {"name": "Regime + Exposure",     "regime_filter": True,  "exposure_overlay": True,  "symmetric": False},
    {"name": "Regime Sym",            "regime_filter": True,  "exposure_overlay": False, "symmetric": True},
    {"name": "Regime Sym + Exposure", "regime_filter": True,  "exposure_overlay": True,  "symmetric": True},
]


def main():
    parser = argparse.ArgumentParser(description="Gen4 Regime+Exposure Backtester v2")
    parser.add_argument("--start-is", default="2019-01-02")
    parser.add_argument("--end-is", default="2023-12-29")
    parser.add_argument("--start-oos", default="2024-01-02")
    parser.add_argument("--end-oos", default="2026-03-20")
    args = parser.parse_args()

    config = Gen4Config()

    print("=" * 90)
    print("  Gen4 Regime + Exposure Overlay Backtest v2")
    print("  LowVol + Momentum 12-1 x Regime Filter x Exposure (cash allocation) Scaling")
    print("  v2: exposure = cash scaling (not stock count), symmetric BEAR variant")
    print("=" * 90)

    t0 = time.time()

    # ── Load data once ───────────────────────────────────────────
    print(f"\n[1/4] Loading OHLCV from {config.OHLCV_DIR}...")
    all_data = load_ohlcv(config.OHLCV_DIR, config.UNIV_MIN_HISTORY)

    idx_df = pd.read_csv(config.INDEX_FILE)
    date_col = "index" if "index" in idx_df.columns else "date"
    rename = {date_col: "date"}
    for s, d_ in [("Open", "open"), ("High", "high"), ("Low", "low"),
                   ("Close", "close"), ("Volume", "volume")]:
        if s in idx_df.columns:
            rename[s] = d_
    idx_df = idx_df.rename(columns=rename)
    idx_df["date"] = pd.to_datetime(idx_df["date"], errors="coerce")
    idx_df = idx_df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    for c in ["open", "high", "low", "close", "volume"]:
        if c in idx_df.columns:
            idx_df[c] = pd.to_numeric(idx_df[c], errors="coerce").fillna(0)
    dates = idx_df["date"]
    print(f"  {len(all_data)} stocks, {len(dates)} dates")

    close, opn, high, low, vol = build_matrices(all_data, dates)
    idx_close = idx_df.set_index("date")["close"].reindex(dates).ffill()

    # ── Precompute MA200 (once) ──────────────────────────────────
    print("[2/4] Precomputing MA200...")
    kospi_ma200 = idx_close.rolling(MA_WINDOW).mean()
    stock_ma200 = close.rolling(MA_WINDOW).mean()
    print(f"  KOSPI MA200 range: {kospi_ma200.dropna().iloc[0]:.0f} ~ {kospi_ma200.dropna().iloc[-1]:.0f}")

    # ── Period indices ───────────────────────────────────────────
    periods = {
        "IS": (pd.Timestamp(args.start_is), pd.Timestamp(args.end_is)),
        "OOS": (pd.Timestamp(args.start_oos), pd.Timestamp(args.end_oos)),
        "FULL": (pd.Timestamp(args.start_is), pd.Timestamp(args.end_oos)),
    }
    period_idx = {}
    for pname, (sd, ed) in periods.items():
        si = int((dates >= sd).values.argmax())
        ei = int(len(dates) - 1 - (dates <= ed).values[::-1].argmax())
        period_idx[pname] = (si, ei)
        print(f"  {pname}: {dates[si].date()} ~ {dates[ei].date()} ({ei-si+1} days)")

    # ── Run configs ──────────────────────────────────────────────
    print(f"\n[3/4] Running {len(CONFIGS)} configs x {len(periods)} periods...")
    results = []
    saved_regime_log = None
    saved_equities = {}
    saved_rebal_logs = {}

    for cfg in CONFIGS:
        for pname in ["IS", "OOS", "FULL"]:
            si, ei = period_idx[pname]
            eq, trades, rlog, rebal_log = run_backtest_regime(
                close, opn, high, low, vol, idx_close, dates,
                si, ei, config,
                regime_filter=cfg["regime_filter"],
                exposure_overlay=cfg["exposure_overlay"],
                kospi_ma200=kospi_ma200,
                stock_ma200=stock_ma200,
                symmetric_bear=cfg["symmetric"],
            )
            m = calc_metrics(eq, trades)

            regime_decomp = {}
            if pname == "FULL":
                regime_decomp = decompose_by_regime_segments(eq, rlog)
                saved_equities[cfg["name"]] = eq
                saved_rebal_logs[cfg["name"]] = rebal_log
                if saved_regime_log is None:
                    saved_regime_log = rlog

            kospi_ret = float(idx_close.iloc[ei] / idx_close.iloc[si] - 1)

            results.append({
                "config": cfg["name"],
                "period": pname,
                "cagr": m.get("cagr", 0),
                "mdd": m.get("mdd", 0),
                "sharpe": m.get("sharpe", 0),
                "calmar": m.get("calmar", 0),
                "sortino": m.get("sortino", 0),
                "win_rate": m.get("win_rate", 0),
                "n_trades": m.get("n_trades", 0),
                "avg_hold": m.get("avg_hold_days", 0),
                "profit_factor": m.get("profit_factor", 0),
                "total_return": m.get("total_return", 0),
                "kospi_return": kospi_ret,
                "exit_reasons": m.get("exit_reasons", {}),
                "regime_decomp": regime_decomp,
            })

            print(f"  {cfg['name']:24s} {pname:4s}: "
                  f"CAGR={m.get('cagr',0)*100:+6.1f}%  "
                  f"MDD={m.get('mdd',0)*100:6.1f}%  "
                  f"Sharpe={m.get('sharpe',0):5.2f}  "
                  f"Calmar={m.get('calmar',0):5.2f}  "
                  f"Trades={m.get('n_trades',0):4d}")

    # ── Results ──────────────────────────────────────────────────
    print(f"\n[4/4] Results")
    elapsed = time.time() - t0

    # Main comparison table (IS + OOS only)
    print(f"\n{'='*100}")
    print(f"  {'Config':24s} {'Period':5s} {'CAGR':>7s} {'MDD':>7s} {'Sharpe':>7s} "
          f"{'Calmar':>7s} {'WR%':>5s} {'Trades':>6s} {'AvgHold':>7s} {'PF':>5s}")
    print(f"{'─'*100}")
    for r in results:
        if r["period"] == "FULL":
            continue
        print(f"  {r['config']:24s} {r['period']:5s} "
              f"{r['cagr']*100:+6.1f}% {r['mdd']*100:6.1f}% "
              f"{r['sharpe']:6.2f}  {r['calmar']:6.2f}  "
              f"{r['win_rate']*100:4.0f}% {r['n_trades']:5d}  "
              f"{r['avg_hold']:6.1f}  {r['profit_factor']:4.1f}")
    print(f"{'='*100}")

    # KOSPI benchmark
    for pname in ["IS", "OOS"]:
        si, ei = period_idx[pname]
        kr = float(idx_close.iloc[ei] / idx_close.iloc[si] - 1)
        print(f"  KOSPI Buy&Hold {pname}: {kr*100:+.1f}%")

    # Regime distribution (asymmetric vs symmetric)
    if saved_regime_log:
        rl_df = pd.DataFrame(saved_regime_log)
        total = len(rl_df)
        regime_counts = rl_df["regime"].value_counts()
        print(f"\n  Regime Distribution - Asymmetric (default):")
        for rn in ["BULL", "SIDE", "BEAR"]:
            cnt = regime_counts.get(rn, 0)
            print(f"    {rn:5s}: {cnt:4d} days ({cnt/total*100:4.1f}%)")

    # Symmetric regime distribution
    sym_log = [r for r in results if r["config"] == "Regime Sym" and r["period"] == "FULL"]
    # Compute symmetric regime distribution from a separate run
    print(f"\n  Regime Distribution - Symmetric BEAR variant:")
    # Run a quick pass to count symmetric regimes
    sym_counts = {"BULL": 0, "SIDE": 0, "BEAR": 0}
    si_full, ei_full = period_idx["FULL"]
    for i in range(si_full, ei_full + 1):
        if i < MA_WINDOW:
            sym_counts["SIDE"] += 1
            continue
        b = calc_breadth(close.iloc[i], stock_ma200.iloc[i])
        r = calc_regime(float(idx_close.iloc[i]), float(kospi_ma200.iloc[i]),
                        b, i, symmetric=True)
        sym_counts[r] += 1
    total_sym = sum(sym_counts.values())
    for rn in ["BULL", "SIDE", "BEAR"]:
        print(f"    {rn:5s}: {sym_counts[rn]:4d} days ({sym_counts[rn]/total_sym*100:4.1f}%)")

    # Regime segment decomposition
    print(f"\n  Regime Segment Decomposition (contiguous segments, FULL):")
    for cfg_name in ["Regime Only", "Regime + Exposure"]:
        r_full = [r for r in results if r["config"] == cfg_name and r["period"] == "FULL"]
        if r_full and r_full[0].get("regime_decomp"):
            print(f"\n    [{cfg_name}]")
            print(f"    {'Regime':6s} {'Days':>5s} {'%Time':>6s} {'#Seg':>5s} "
                  f"{'AvgRet':>8s} {'MedRet':>8s} {'WgtAnn':>8s} {'WstMDD':>8s}")
            for rn in ["BULL", "SIDE", "BEAR"]:
                info = r_full[0]["regime_decomp"].get(rn, {})
                print(f"    {rn:6s} {info.get('days',0):5d} "
                      f"{info.get('pct_time',0):5.1f}% "
                      f"{info.get('n_segments',0):5d} "
                      f"{info.get('avg_return',0):+7.2f}% "
                      f"{info.get('median_return',0):+7.2f}% "
                      f"{info.get('weighted_ann_return',0):+7.1f}% "
                      f"{info.get('worst_mdd',0):7.1f}%")

    # Exit reasons
    print(f"\n  Exit Reasons (FULL period):")
    for r in results:
        if r["period"] != "FULL":
            continue
        reasons = r.get("exit_reasons", {})
        parts = []
        for reason in ["TRAIL", "REBALANCE", "EOD"]:
            info = reasons.get(reason, {"count": 0})
            parts.append(f"{reason}={info['count']}")
        print(f"    {r['config']:24s}  {', '.join(parts)}")

    # IS→OOS delta
    print(f"\n  IS -> OOS Performance Delta:")
    for cfg in CONFIGS:
        is_r = [r for r in results if r["config"] == cfg["name"] and r["period"] == "IS"]
        oos_r = [r for r in results if r["config"] == cfg["name"] and r["period"] == "OOS"]
        if is_r and oos_r:
            cagr_d = (oos_r[0]["cagr"] - is_r[0]["cagr"]) * 100
            mdd_d = (oos_r[0]["mdd"] - is_r[0]["mdd"]) * 100
            sharpe_d = oos_r[0]["sharpe"] - is_r[0]["sharpe"]
            print(f"    {cfg['name']:24s}  CAGR: {cagr_d:+5.1f}pp  "
                  f"MDD: {mdd_d:+5.1f}pp  Sharpe: {sharpe_d:+5.2f}")

    # Rebalance allocation log summary (Test 5)
    print(f"\n  Rebalance Allocation Summary:")
    for cfg_name in ["Baseline", "Exposure Only", "Regime + Exposure"]:
        rlog = saved_rebal_logs.get(cfg_name, [])
        if not rlog:
            continue
        rl_df = pd.DataFrame(rlog)
        avg_pp = rl_df["intended_per_pos"].mean()
        avg_cr = rl_df["cash_ratio"].mean()
        avg_buys = rl_df["n_new_buys"].mean()
        skipped = rl_df["skip_buys"].sum()
        print(f"    {cfg_name:24s}  "
              f"avg_per_pos={avg_pp:,.0f}  "
              f"avg_cash_ratio={avg_cr:.1%}  "
              f"avg_new_buys={avg_buys:.1f}  "
              f"skipped_rebals={skipped}")

    print(f"\n  Elapsed: {elapsed:.0f}s")

    # ── Save outputs ─────────────────────────────────────────────
    out_dir = config.BASE_DIR.parent / "backtest" / "results" / "gen4_regime"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Comparison CSV
    comp_df = pd.DataFrame([
        {k: v for k, v in r.items() if k not in ("exit_reasons", "regime_decomp")}
        for r in results
    ])
    comp_df.to_csv(out_dir / "comparison.csv", index=False)

    # Equity curves
    for name, eq in saved_equities.items():
        fname = name.lower().replace(" ", "_").replace("+", "_")
        eq_df = eq.reset_index()
        eq_df.columns = ["date", "equity"]
        eq_df.to_csv(out_dir / f"equity_{fname}.csv", index=False)

    # Regime log
    if saved_regime_log:
        pd.DataFrame(saved_regime_log).to_csv(out_dir / "regime_log.csv", index=False)

    # Rebalance logs
    for cfg_name, rlog in saved_rebal_logs.items():
        if rlog:
            fname = cfg_name.lower().replace(" ", "_").replace("+", "_")
            pd.DataFrame(rlog).to_csv(out_dir / f"rebal_log_{fname}.csv", index=False)

    print(f"\n  Results saved to {out_dir}/")
    print("=" * 90)


if __name__ == "__main__":
    main()
