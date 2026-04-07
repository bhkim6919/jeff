"""
backtester_regime_v3.py - Gen4 Regime Response Strategies (A/B/C/D)
===================================================================
Addresses the core weakness: 21-day rebalance gap leaves positions exposed
when regime shifts from BULL to BEAR mid-cycle.

Strategies:
  A: Emergency Rebalance - BEAR transition triggers immediate position trim
  B: Daily Exposure Adj  - daily regime check, next-day exposure adjustment
  C: Short Cycle         - rebalance period 21 -> 10 days
  D: Tight Trail Stop    - BEAR regime tightens trail from -12% to -8%

Combinations: A+D, A+B+D, and all vs Baseline + R+E baseline.

Signal/execution timing:
  - Regime computed on day i close
  - Sell/trim signals: detected on day i, executed at day i close (same-day)
  - Buy signals: queued on day i, filled at day i+1 open (T+1)
  - This matches backtester_regime.py conventions

Usage:
    cd Gen04
    python -m backtest.backtester_regime_v3
"""
from __future__ import annotations
import sys
import time
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Optional

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

# ── Strategy D: Tight trail in BEAR ──────────────────────────────────────────
BEAR_TRAIL_PCT = 0.08    # -8% in BEAR (vs default -12%)


def calc_regime(idx_close_val, kospi_ma200_val, breadth, i, symmetric=False):
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


def calc_breadth(close_row, ma200_row):
    valid = close_row[(close_row > 0) & close_row.notna()]
    ma_valid = ma200_row.reindex(valid.index)
    ma_valid = ma_valid[ma_valid.notna() & (ma_valid > 0)]
    if len(ma_valid) == 0:
        return 0.5
    common = valid.index.intersection(ma_valid.index)
    return float((valid[common] > ma_valid[common]).mean())


# ── Core Backtest ────────────────────────────────────────────────────────────
def run_backtest(close, opn, high, low, vol, idx_close, dates,
                 start_i, end_i, config,
                 regime_filter=False, exposure_overlay=False,
                 kospi_ma200=None, stock_ma200=None,
                 # Strategy flags
                 emergency_rebal=False,     # A
                 daily_exposure=False,       # B
                 short_cycle=False,          # C
                 tight_trail=False,          # D
                 ):
    """
    Returns: (equity_series, trades, regime_log, rebal_log, event_log)
    """
    cash = float(config.INITIAL_CASH)
    positions = {}
    pending_buys = []
    trades = []
    equity_hist = {}
    regime_log = []
    rebal_log = []
    event_log = []         # emergency/daily adjustment events

    rebal_days = 10 if short_cycle else config.REBAL_DAYS
    last_rebal = -999
    prev_regime = "SIDE"

    for i in range(start_i, end_i + 1):
        dt = dates[i]

        # ── Regime (daily) ────────────────────────────────────────
        breadth = 0.5
        regime = "SIDE"
        if kospi_ma200 is not None and stock_ma200 is not None and i >= MA_WINDOW:
            breadth = calc_breadth(close.iloc[i], stock_ma200.iloc[i])
            regime = calc_regime(
                float(idx_close.iloc[i]), float(kospi_ma200.iloc[i]),
                breadth, i)
        regime_log.append({
            "date": str(dt.date()), "regime": regime,
            "kospi": round(float(idx_close.iloc[i]), 2),
            "breadth": round(breadth, 4),
        })

        # ── 0) Fill pending buys at today's open ─────────────────
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
            cash -= qty * buy_cost_total
            positions[tk] = dict(
                qty=qty, entry_price=entry_price, entry_idx=i,
                high_wm=entry_price,
                buy_cost_total=qty * entry_price * config.BUY_COST,
            )
            pending_buys.remove(pb)
        pending_buys = [pb for pb in pending_buys if pb["target_idx"] >= i]

        # ── 1) Trail Stop ────────────────────────────────────────
        # Strategy D: tighter trail in BEAR
        trail_pct = BEAR_TRAIL_PCT if (tight_trail and regime == "BEAR") else config.TRAIL_PCT

        for tk in list(positions.keys()):
            pos = positions[tk]
            p = float(close[tk].iloc[i])
            if p <= 0 or pd.isna(p):
                continue
            if p > pos["high_wm"]:
                pos["high_wm"] = p
            dd = (p - pos["high_wm"]) / pos["high_wm"] if pos["high_wm"] > 0 else 0
            if dd <= -trail_pct:
                net = pos["qty"] * p * (1 - config.SELL_COST)
                invested = pos["qty"] * pos["entry_price"] + pos["buy_cost_total"]
                pnl = (net - invested) / invested if invested > 0 else 0
                cash += net
                trades.append(dict(
                    ticker=tk,
                    entry_date=str(dates[pos["entry_idx"]].date()),
                    exit_date=str(dt.date()),
                    entry_price=pos["entry_price"], exit_price=p,
                    pnl_pct=pnl, pnl_amount=net - invested,
                    hold_days=i - pos["entry_idx"],
                    exit_reason="TRAIL_TIGHT" if trail_pct < config.TRAIL_PCT else "TRAIL",
                ))
                del positions[tk]

        # ── 2A) Strategy A: Emergency Rebalance on BEAR transition ─
        if emergency_rebal and regime == "BEAR" and prev_regime != "BEAR" and positions:
            # Regime just turned BEAR -> trim to target exposure
            pv_held = sum(pos["qty"] * max(0, float(close[c].iloc[i]))
                          for c, pos in positions.items()
                          if not pd.isna(close[c].iloc[i]))
            total_eq = cash + pv_held
            target_exp = total_eq * EXPOSURE_MAP["BEAR"]  # 40%

            if pv_held > target_exp * 1.05:  # 5% tolerance
                # Trim worst-performing positions first
                pos_pnl = []
                for tk, pos in positions.items():
                    p = float(close[tk].iloc[i])
                    if p > 0:
                        ret = (p - pos["entry_price"]) / pos["entry_price"]
                        pos_pnl.append((tk, ret, p))
                pos_pnl.sort(key=lambda x: x[1])  # worst first

                trim_target = pv_held - target_exp
                trimmed = 0
                for tk, ret, p in pos_pnl:
                    if trimmed >= trim_target:
                        break
                    pos = positions[tk]
                    pos_value = pos["qty"] * p
                    # Sell entire position if it fits, else partial
                    if pos_value <= (trim_target - trimmed) * 1.5:
                        # Full sell
                        net = pos["qty"] * p * (1 - config.SELL_COST)
                        invested = pos["qty"] * pos["entry_price"] + pos["buy_cost_total"]
                        pnl = (net - invested) / invested if invested > 0 else 0
                        cash += net
                        trimmed += pos_value
                        trades.append(dict(
                            ticker=tk,
                            entry_date=str(dates[pos["entry_idx"]].date()),
                            exit_date=str(dt.date()),
                            entry_price=pos["entry_price"], exit_price=p,
                            pnl_pct=pnl, pnl_amount=net - invested,
                            hold_days=i - pos["entry_idx"],
                            exit_reason="EMERGENCY_A",
                        ))
                        del positions[tk]
                    else:
                        # Partial sell
                        sell_qty = max(1, int((trim_target - trimmed) / p))
                        sell_qty = min(sell_qty, pos["qty"] - 1)
                        if sell_qty > 0:
                            net = sell_qty * p * (1 - config.SELL_COST)
                            cost_per_share = pos["entry_price"] + pos["buy_cost_total"] / pos["qty"]
                            invested = sell_qty * cost_per_share
                            pnl = (net - invested) / invested if invested > 0 else 0
                            cash += net
                            trimmed += sell_qty * p
                            # Adjust position
                            ratio = sell_qty / pos["qty"]
                            pos["buy_cost_total"] *= (1 - ratio)
                            pos["qty"] -= sell_qty
                            trades.append(dict(
                                ticker=tk,
                                entry_date=str(dates[pos["entry_idx"]].date()),
                                exit_date=str(dt.date()),
                                entry_price=pos["entry_price"], exit_price=p,
                                pnl_pct=pnl, pnl_amount=net - invested,
                                hold_days=i - pos["entry_idx"],
                                exit_reason="EMERGENCY_A_PARTIAL",
                            ))

                event_log.append({
                    "date": str(dt.date()), "event": "EMERGENCY_REBAL",
                    "regime": regime, "pv_before": round(pv_held),
                    "target_exp": round(target_exp), "trimmed": round(trimmed),
                    "positions_after": len(positions),
                })

        # ── 2B) Strategy B: Daily Exposure Adjustment ────────────
        if daily_exposure and regime == "BEAR" and positions:
            # Check daily: if exposure exceeds BEAR target, trim
            pv_held = sum(pos["qty"] * max(0, float(close[c].iloc[i]))
                          for c, pos in positions.items()
                          if not pd.isna(close[c].iloc[i]))
            total_eq = cash + pv_held
            target_exp = total_eq * EXPOSURE_MAP.get(regime, 1.0)

            if pv_held > target_exp * 1.10:  # 10% tolerance (less aggressive than A)
                # Trim worst performer
                worst_tk = None
                worst_ret = 999
                for tk, pos in positions.items():
                    p = float(close[tk].iloc[i])
                    if p > 0:
                        ret = (p - pos["entry_price"]) / pos["entry_price"]
                        if ret < worst_ret:
                            worst_ret = ret
                            worst_tk = tk

                if worst_tk and worst_tk in positions:
                    pos = positions[worst_tk]
                    p = float(close[worst_tk].iloc[i])
                    if p > 0:
                        net = pos["qty"] * p * (1 - config.SELL_COST)
                        invested = pos["qty"] * pos["entry_price"] + pos["buy_cost_total"]
                        pnl = (net - invested) / invested if invested > 0 else 0
                        cash += net
                        trades.append(dict(
                            ticker=worst_tk,
                            entry_date=str(dates[pos["entry_idx"]].date()),
                            exit_date=str(dt.date()),
                            entry_price=pos["entry_price"], exit_price=p,
                            pnl_pct=pnl, pnl_amount=net - invested,
                            hold_days=i - pos["entry_idx"],
                            exit_reason="DAILY_TRIM_B",
                        ))
                        del positions[worst_tk]

                        event_log.append({
                            "date": str(dt.date()), "event": "DAILY_TRIM",
                            "regime": regime, "ticker": worst_tk,
                            "pnl_pct": round(pnl * 100, 2),
                        })

        # ── 3) Monthly Rebalance ─────────────────────────────────
        if i - last_rebal >= rebal_days:
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
                n_target = config.N_STOCKS
                top = candidates.sort_values("mom", ascending=False).head(n_target)
                target_codes = set(top["tk"].tolist())

                # Sell non-targets
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
                            entry_price=pos["entry_price"], exit_price=p,
                            pnl_pct=pnl, pnl_amount=net - invested,
                            hold_days=i - pos["entry_idx"],
                            exit_reason="REBALANCE",
                        ))
                        del positions[tk]

                skip_buys = regime_filter and regime == "BEAR"

                if not skip_buys:
                    pv_held = sum(pos["qty"] * float(close[c].iloc[i])
                                  for c, pos in positions.items()
                                  if float(close[c].iloc[i]) > 0)
                    total_eq = cash + pv_held
                    exposure_ratio = EXPOSURE_MAP.get(regime, 1.0) if exposure_overlay else 1.0
                    new_codes = [c for c in target_codes if c not in positions]
                    slots = n_target - len(positions) - len(pending_buys)
                    per_pos = (total_eq * exposure_ratio) / n_target

                    if new_codes and slots > 0 and i + 1 <= end_i:
                        actual_buys = min(len(new_codes), slots)
                        for tk in new_codes[:slots]:
                            pending_buys.append({
                                "tk": tk, "target_idx": i + 1, "per_pos": per_pos,
                            })
                        rebal_log.append({
                            "date": str(dt.date()), "regime": regime,
                            "exposure_ratio": exposure_ratio,
                            "total_eq": round(total_eq),
                            "n_held": len(positions), "n_new_buys": actual_buys,
                            "per_pos": round(per_pos), "skip_buys": False,
                        })
                else:
                    pv_held = sum(pos["qty"] * float(close[c].iloc[i])
                                  for c, pos in positions.items()
                                  if float(close[c].iloc[i]) > 0)
                    total_eq = cash + pv_held
                    rebal_log.append({
                        "date": str(dt.date()), "regime": regime,
                        "exposure_ratio": 0.0, "total_eq": round(total_eq),
                        "n_held": len(positions), "n_new_buys": 0,
                        "per_pos": 0, "skip_buys": True,
                    })

        prev_regime = regime

        # ── Equity snapshot ───────────────────────────────────────
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
                entry_price=pos["entry_price"], exit_price=p,
                pnl_pct=pnl, pnl_amount=net - invested,
                hold_days=end_i - pos["entry_idx"],
                exit_reason="EOD",
            ))

    return (pd.Series(equity_hist).sort_index(), trades, regime_log,
            rebal_log, event_log)


# ── Exposure Tracker ─────────────────────────────────────────────────────────
def calc_exposure_violations(eq, regime_log, close, positions_not_available=True):
    """Approximate exposure violations from equity curve + regime log."""
    # This is computed inside the main loop instead for accuracy
    pass


# ── Config Definitions ───────────────────────────────────────────────────────
CONFIGS = [
    {"name": "Baseline",
     "regime_filter": False, "exposure_overlay": False,
     "emergency_rebal": False, "daily_exposure": False,
     "short_cycle": False, "tight_trail": False},

    {"name": "R+E (Yesterday)",
     "regime_filter": True, "exposure_overlay": True,
     "emergency_rebal": False, "daily_exposure": False,
     "short_cycle": False, "tight_trail": False},

    {"name": "A: Emergency Rebal",
     "regime_filter": True, "exposure_overlay": True,
     "emergency_rebal": True, "daily_exposure": False,
     "short_cycle": False, "tight_trail": False},

    {"name": "B: Daily Exposure",
     "regime_filter": True, "exposure_overlay": True,
     "emergency_rebal": False, "daily_exposure": True,
     "short_cycle": False, "tight_trail": False},

    {"name": "C: Short Cycle 10d",
     "regime_filter": True, "exposure_overlay": True,
     "emergency_rebal": False, "daily_exposure": False,
     "short_cycle": True, "tight_trail": False},

    {"name": "D: Tight Trail",
     "regime_filter": True, "exposure_overlay": True,
     "emergency_rebal": False, "daily_exposure": False,
     "short_cycle": False, "tight_trail": True},

    {"name": "A+D",
     "regime_filter": True, "exposure_overlay": True,
     "emergency_rebal": True, "daily_exposure": False,
     "short_cycle": False, "tight_trail": True},

    {"name": "A+B+D",
     "regime_filter": True, "exposure_overlay": True,
     "emergency_rebal": True, "daily_exposure": True,
     "short_cycle": False, "tight_trail": True},
]


def main():
    parser = argparse.ArgumentParser(description="Gen4 Regime v3: A/B/C/D Strategies")
    parser.add_argument("--start-is", default="2019-01-02")
    parser.add_argument("--end-is", default="2023-12-29")
    parser.add_argument("--start-oos", default="2024-01-02")
    parser.add_argument("--end-oos", default="2026-03-20")
    args = parser.parse_args()

    config = Gen4Config()

    print("=" * 100)
    print("  Gen4 Regime Response Strategies v3")
    print("  A: Emergency Rebal | B: Daily Exposure | C: Short Cycle | D: Tight Trail")
    print("  + Combinations: A+D, A+B+D vs Baseline + R+E")
    print("=" * 100)

    t0 = time.time()

    # Load data
    print(f"\n[1/4] Loading OHLCV...")
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

    print("[2/4] Precomputing MA200...")
    kospi_ma200 = idx_close.rolling(MA_WINDOW).mean()
    stock_ma200 = close.rolling(MA_WINDOW).mean()

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

    # Run all configs
    print(f"\n[3/4] Running {len(CONFIGS)} configs x {len(periods)} periods...")
    results = []
    saved_equities = {}
    saved_event_logs = {}

    for cfg in CONFIGS:
        for pname in ["IS", "OOS", "FULL"]:
            si, ei = period_idx[pname]
            eq, trades, rlog, rebal_log, event_log = run_backtest(
                close, opn, high, low, vol, idx_close, dates, si, ei, config,
                regime_filter=cfg["regime_filter"],
                exposure_overlay=cfg["exposure_overlay"],
                kospi_ma200=kospi_ma200, stock_ma200=stock_ma200,
                emergency_rebal=cfg["emergency_rebal"],
                daily_exposure=cfg["daily_exposure"],
                short_cycle=cfg["short_cycle"],
                tight_trail=cfg["tight_trail"],
            )
            m = calc_metrics(eq, trades)
            kospi_ret = float(idx_close.iloc[ei] / idx_close.iloc[si] - 1)

            # Count exit reasons
            exit_counts = {}
            for t in trades:
                r = t.get("exit_reason", "UNKNOWN")
                exit_counts[r] = exit_counts.get(r, 0) + 1

            if pname == "FULL":
                saved_equities[cfg["name"]] = eq
                saved_event_logs[cfg["name"]] = event_log

            results.append({
                "config": cfg["name"], "period": pname,
                "cagr": m.get("cagr", 0), "mdd": m.get("mdd", 0),
                "sharpe": m.get("sharpe", 0), "calmar": m.get("calmar", 0),
                "sortino": m.get("sortino", 0), "win_rate": m.get("win_rate", 0),
                "n_trades": m.get("n_trades", 0), "avg_hold": m.get("avg_hold_days", 0),
                "profit_factor": m.get("profit_factor", 0),
                "total_return": m.get("total_return", 0),
                "kospi_return": kospi_ret,
                "exit_counts": exit_counts,
                "n_events": len(event_log),
            })

            evt_tag = f"  [events={len(event_log)}]" if event_log else ""
            print(f"  {cfg['name']:24s} {pname:4s}: "
                  f"CAGR={m.get('cagr',0)*100:+6.1f}%  "
                  f"MDD={m.get('mdd',0)*100:6.1f}%  "
                  f"Sharpe={m.get('sharpe',0):5.2f}  "
                  f"Calmar={m.get('calmar',0):5.2f}  "
                  f"Trades={m.get('n_trades',0):4d}{evt_tag}")

    # ── Results ──────────────────────────────────────────────────
    print(f"\n[4/4] Results")
    elapsed = time.time() - t0

    # === Main Comparison Table ===
    print(f"\n{'='*120}")
    print(f"  STRATEGY COMPARISON (IS / OOS)")
    print(f"{'='*120}")
    print(f"  {'Config':24s} {'Prd':4s} {'CAGR':>7s} {'MDD':>7s} {'Sharpe':>7s} "
          f"{'Calmar':>7s} {'Sortino':>8s} {'WR%':>5s} {'Trades':>6s} {'AvgHld':>6s} {'PF':>5s} {'Events':>6s}")
    print(f"  {'-'*116}")
    for r in results:
        if r["period"] == "FULL":
            continue
        print(f"  {r['config']:24s} {r['period']:4s} "
              f"{r['cagr']*100:+6.1f}% {r['mdd']*100:6.1f}% "
              f"{r['sharpe']:6.2f}  {r['calmar']:6.2f}  "
              f"{r['sortino']:7.2f}  "
              f"{r['win_rate']*100:4.0f}% {r['n_trades']:5d} "
              f"{r['avg_hold']:5.1f}  {r['profit_factor']:4.1f} "
              f"{r['n_events']:5d}")
    print(f"{'='*120}")

    # === FULL Period Comparison ===
    print(f"\n{'='*120}")
    print(f"  FULL PERIOD COMPARISON")
    print(f"{'='*120}")
    print(f"  {'Config':24s} {'CAGR':>7s} {'MDD':>7s} {'Sharpe':>7s} "
          f"{'Calmar':>7s} {'Sortino':>8s} {'WR%':>5s} {'Trades':>6s} {'PF':>5s}")
    print(f"  {'-'*80}")
    for r in results:
        if r["period"] != "FULL":
            continue
        print(f"  {r['config']:24s} "
              f"{r['cagr']*100:+6.1f}% {r['mdd']*100:6.1f}% "
              f"{r['sharpe']:6.2f}  {r['calmar']:6.2f}  "
              f"{r['sortino']:7.2f}  "
              f"{r['win_rate']*100:4.0f}% {r['n_trades']:5d} "
              f"{r['profit_factor']:4.1f}")
    print(f"{'='*120}")

    # === Delta vs R+E (Yesterday) ===
    print(f"\n{'='*120}")
    print(f"  DELTA vs R+E (Yesterday) -- OOS Period")
    print(f"{'='*120}")
    re_oos = [r for r in results if r["config"] == "R+E (Yesterday)" and r["period"] == "OOS"]
    if re_oos:
        ref = re_oos[0]
        print(f"  {'Config':24s} {'dCAGR':>8s} {'dMDD':>8s} {'dSharpe':>8s} {'dCalmar':>8s}")
        print(f"  {'-'*60}")
        for r in results:
            if r["period"] != "OOS" or r["config"] == "Baseline":
                continue
            dc = (r["cagr"] - ref["cagr"]) * 100
            dm = (r["mdd"] - ref["mdd"]) * 100
            ds = r["sharpe"] - ref["sharpe"]
            dcal = r["calmar"] - ref["calmar"]
            print(f"  {r['config']:24s} {dc:+7.2f}pp {dm:+7.2f}pp {ds:+7.2f}   {dcal:+7.2f}")
    print(f"{'='*120}")

    # === Exit Reason Breakdown (FULL) ===
    print(f"\n  Exit Reason Breakdown (FULL):")
    for r in results:
        if r["period"] != "FULL":
            continue
        ec = r.get("exit_counts", {})
        parts = [f"{k}={v}" for k, v in sorted(ec.items())]
        print(f"    {r['config']:24s}  {', '.join(parts)}")

    # === Event Summary ===
    print(f"\n  Emergency/Daily Events (FULL):")
    for cfg_name, evts in saved_event_logs.items():
        if evts:
            evt_df = pd.DataFrame(evts)
            evt_counts = evt_df["event"].value_counts().to_dict()
            parts = [f"{k}={v}" for k, v in evt_counts.items()]
            print(f"    {cfg_name:24s}  {', '.join(parts)}")

    # === IS -> OOS Stability ===
    print(f"\n  IS -> OOS Stability:")
    for cfg in CONFIGS:
        is_r = [r for r in results if r["config"] == cfg["name"] and r["period"] == "IS"]
        oos_r = [r for r in results if r["config"] == cfg["name"] and r["period"] == "OOS"]
        if is_r and oos_r:
            dc = (oos_r[0]["cagr"] - is_r[0]["cagr"]) * 100
            dm = (oos_r[0]["mdd"] - is_r[0]["mdd"]) * 100
            ds = oos_r[0]["sharpe"] - is_r[0]["sharpe"]
            print(f"    {cfg['name']:24s}  CAGR: {dc:+5.1f}pp  MDD: {dm:+5.1f}pp  Sharpe: {ds:+5.2f}")

    # KOSPI benchmark
    for pname in ["IS", "OOS"]:
        si, ei = period_idx[pname]
        kr = float(idx_close.iloc[ei] / idx_close.iloc[si] - 1)
        print(f"\n  KOSPI Buy&Hold {pname}: {kr*100:+.1f}%")

    print(f"\n  Elapsed: {elapsed:.0f}s")

    # ── Save ──────────────────────────────────────────────────────
    out_dir = config.BASE_DIR.parent / "backtest" / "results" / "gen4_regime_v3"
    out_dir.mkdir(parents=True, exist_ok=True)

    comp_df = pd.DataFrame([
        {k: v for k, v in r.items() if k not in ("exit_counts",)}
        for r in results
    ])
    comp_df.to_csv(out_dir / "comparison.csv", index=False)

    for name, eq in saved_equities.items():
        fname = name.lower().replace(" ", "_").replace("+", "_").replace(":", "").replace("(", "").replace(")", "")
        eq_df = eq.reset_index()
        eq_df.columns = ["date", "equity"]
        eq_df.to_csv(out_dir / f"equity_{fname}.csv", index=False)

    for cfg_name, evts in saved_event_logs.items():
        if evts:
            fname = cfg_name.lower().replace(" ", "_").replace("+", "_").replace(":", "").replace("(", "").replace(")", "")
            pd.DataFrame(evts).to_csv(out_dir / f"events_{fname}.csv", index=False)

    print(f"\n  Results saved to {out_dir}/")
    print("=" * 100)


if __name__ == "__main__":
    main()
