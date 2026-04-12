"""
backtester_bear_dipbuy.py - Gen4 BEAR Dip-Buy Backtest
======================================================
Extends backtester_regime.py with BEAR-regime limited averaging-down (DipBuy).

Core principle:
  DipBuy is a "return improvement experiment", NOT a "risk solution".
  Exposure control comes first; DipBuy operates ONLY within that boundary.

Key safeguards (user-specified):
  1. Exposure sync: pending_dipbuys included in exposure calculation
  2. original_per_pos: frozen at rebalance entry, never changed
  3. Signal timing: regime/signal at i-1, execution at T+1 open (no lookahead)
  4. Daily dipbuy cap: max 2 tickers per day
  5. Short signal hardened: three_day_up AND (price > MA5) OR bounce > 5%
  6. 1 dipbuy per position, 3-day cooldown

Usage:
    cd kr-legacy
    python -m backtest.backtester_bear_dipbuy
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

# ── DipBuy Constants ─────────────────────────────────────────────────────────
DIPBUY_RATIO = 0.03           # 3% of original_per_pos
DIPBUY_MAX_COUNT = 1          # max 1 dipbuy per position
DIPBUY_COOLDOWN = 3           # min 3 days between dipbuys
DIPBUY_MAX_PER_DAY = 2        # max 2 tickers per day
DIPBUY_MIN_LOSS = -0.05       # unrealized must be <= -5%
DIPBUY_MOM_DECAY = 0.9        # current_mom >= entry_mom * 0.9
DIPBUY_BOUNCE_MIN = 0.05      # bounce > 5%
MARKET_CRASH_THRESHOLD = -0.02  # KOSPI daily return <= -2%
BEAR_STREAK_MIN = 2           # BEAR must persist >= 2 days


# ── Regime Detection ─────────────────────────────────────────────────────────
def calc_regime(idx_close_val: float, kospi_ma200_val: float,
                breadth: float, i: int, symmetric: bool = False) -> str:
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
    valid = close_row[(close_row > 0) & close_row.notna()]
    ma_valid = ma200_row.reindex(valid.index)
    ma_valid = ma_valid[ma_valid.notna() & (ma_valid > 0)]
    if len(ma_valid) == 0:
        return 0.5
    common = valid.index.intersection(ma_valid.index)
    return float((valid[common] > ma_valid[common]).mean())


# ── Core Backtest with Regime + DipBuy ───────────────────────────────────────
def run_backtest_regime(close, opn, high, low, vol, idx_close, dates,
                        start_i: int, end_i: int, config: Gen4Config,
                        regime_filter: bool = False,
                        exposure_overlay: bool = False,
                        kospi_ma200: pd.Series = None,
                        stock_ma200: pd.DataFrame = None,
                        symmetric_bear: bool = False,
                        bear_dipbuy: bool = False,
                        ) -> Tuple[pd.Series, list, list, list, list, list]:
    """
    Returns: (equity_series, trades, regime_log, rebal_log, dipbuy_logs, exposure_logs)
    """
    cash = float(config.INITIAL_CASH)
    positions = {}
    pending_buys = []
    pending_dipbuys = []      # T+1 dipbuy orders
    trades = []
    equity_hist = {}
    regime_log = []
    rebal_log = []
    dipbuy_logs = []
    exposure_logs = []

    last_rebal = -999
    bear_streak = 0

    # Precompute MA5 for short signal validation
    stock_ma5 = close.rolling(5).mean()

    for i in range(start_i, end_i + 1):
        dt = dates[i]

        # ── Regime calculation (daily, using TODAY's data) ─────────
        breadth = 0.5
        regime = "SIDE"
        if kospi_ma200 is not None and stock_ma200 is not None and i >= MA_WINDOW:
            breadth = calc_breadth(close.iloc[i], stock_ma200.iloc[i])
            regime = calc_regime(
                float(idx_close.iloc[i]),
                float(kospi_ma200.iloc[i]),
                breadth, i,
                symmetric=symmetric_bear)

        # bear_streak tracking
        if regime == "BEAR":
            bear_streak += 1
        else:
            bear_streak = 0

        regime_log.append({
            "date": str(dt.date()),
            "kospi": round(float(idx_close.iloc[i]), 2),
            "kospi_ma200": round(float(kospi_ma200.iloc[i]), 2) if kospi_ma200 is not None and i < len(kospi_ma200) and not np.isnan(kospi_ma200.iloc[i]) else 0,
            "breadth": round(breadth, 4),
            "regime": regime,
        })

        # ── 0a) Fill pending buys at today's open ─────────────────
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

            # Store entry_mom for dipbuy RS comparison
            series_for_mom = close[tk].iloc[:i+1]
            entry_mom = calc_momentum(series_for_mom, config.MOM_LOOKBACK, config.MOM_SKIP)

            positions[tk] = dict(
                qty=qty,
                entry_price=entry_price,
                entry_idx=i,
                high_wm=entry_price,
                buy_cost_total=qty * entry_price * config.BUY_COST,
                # DipBuy fields
                entry_mom=entry_mom if not np.isnan(entry_mom) else 0.0,
                dipbuy_count=0,
                original_per_pos=per_pos,   # frozen at rebalance, never changed
                last_dipbuy_idx=-999,
            )
            pending_buys.remove(pb)

        pending_buys = [pb for pb in pending_buys if pb["target_idx"] >= i]

        # ── 0b) Fill pending dipbuys at today's open ──────────────
        for pd_buy in list(pending_dipbuys):
            if i != pd_buy["target_idx"]:
                continue
            tk = pd_buy["tk"]
            if tk not in positions:
                pending_dipbuys.remove(pd_buy)
                continue

            fill_price = float(opn[tk].iloc[i]) if not pd.isna(opn[tk].iloc[i]) else 0
            if fill_price <= 0:
                pending_dipbuys.remove(pd_buy)
                continue

            dipbuy_amount = pd_buy["amount"]
            buy_cost_unit = fill_price * (1 + config.BUY_COST)
            add_qty = int(dipbuy_amount / buy_cost_unit)
            if add_qty <= 0 or add_qty * buy_cost_unit > cash:
                pending_dipbuys.remove(pd_buy)
                continue

            # Check total position (market value) doesn't exceed original target
            pos = positions[tk]
            current_pos_mktval = pos["qty"] * fill_price
            if current_pos_mktval + add_qty * fill_price > pos["original_per_pos"]:
                pending_dipbuys.remove(pd_buy)
                continue

            actual_cost = add_qty * buy_cost_unit
            cash -= actual_cost

            # Update position with consistent fee handling
            pos["qty"] += add_qty
            pos["buy_cost_total"] += add_qty * fill_price * config.BUY_COST
            pos["dipbuy_count"] = pos.get("dipbuy_count", 0) + 1
            pos["last_dipbuy_idx"] = i
            # Update high watermark if fill price is higher
            if fill_price > pos["high_wm"]:
                pos["high_wm"] = fill_price

            # Log the execution
            dipbuy_logs.append({
                "date": str(dt.date()),
                "ticker": tk,
                "fill_price": round(fill_price, 0),
                "add_qty": add_qty,
                "amount": round(actual_cost, 0),
                "signal_mom": pd_buy.get("signal_mom", 0),
                "signal_unrealized": pd_buy.get("signal_unrealized", 0),
                "bear_streak": pd_buy.get("bear_streak", 0),
                "event": "FILL",
            })
            pending_dipbuys.remove(pd_buy)

        pending_dipbuys = [p for p in pending_dipbuys if p["target_idx"] >= i]

        # ── 1) Trail Stop (close-based) ───────────────────────────
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
                    dipbuy_count=pos.get("dipbuy_count", 0),
                ))
                del positions[tk]

        # ── 2) Monthly Rebalance ──────────────────────────────────
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
                            entry_price=pos["entry_price"],
                            exit_price=p,
                            pnl_pct=pnl,
                            pnl_amount=net - invested,
                            hold_days=i - pos["entry_idx"],
                            exit_reason="REBALANCE",
                            dipbuy_count=pos.get("dipbuy_count", 0),
                        ))
                        del positions[tk]

                # Regime filter: skip buys in BEAR
                skip_buys = regime_filter and regime == "BEAR"

                # Queue buys for T+1 open
                if not skip_buys:
                    pv_held = sum(pos["qty"] * float(close[c].iloc[i])
                                  for c, pos in positions.items()
                                  if float(close[c].iloc[i]) > 0)
                    total_eq = cash + pv_held

                    exposure_ratio = 1.0
                    if exposure_overlay:
                        exposure_ratio = EXPOSURE_MAP.get(regime, 1.0)

                    new_codes = [c for c in target_codes if c not in positions]
                    slots = n_target - len(positions) - len(pending_buys)

                    # per_pos: regime-independent entry size (수정1)
                    per_pos = (total_eq * exposure_ratio) / n_target

                    if new_codes and slots > 0 and i + 1 <= end_i:
                        actual_buys = min(len(new_codes), slots)
                        for tk in new_codes[:slots]:
                            pending_buys.append({
                                "tk": tk,
                                "target_idx": i + 1,
                                "per_pos": per_pos,
                            })

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

        # ── 3) DipBuy Signal Detection (NEW) ─────────────────────
        # Signal uses i-1 data (yesterday), execution at T+1 (tomorrow)
        # This prevents lookahead bias: we decide based on yesterday's close
        if bear_dipbuy and i >= start_i + 2 and i + 1 <= end_i:
            # Use PREVIOUS day's regime for signal (수정6: no lookahead)
            prev_regime = regime_log[-2]["regime"] if len(regime_log) >= 2 else "SIDE"
            prev_bear_streak = bear_streak - 1 if regime == "BEAR" else 0

            # Only proceed if previous day was BEAR and streak >= 2
            if prev_regime == "BEAR" and prev_bear_streak >= BEAR_STREAK_MIN:

                # Market crash filter: previous day KOSPI return
                if i >= 2:
                    idx_ret_prev = (float(idx_close.iloc[i-1]) / float(idx_close.iloc[i-2]) - 1)
                else:
                    idx_ret_prev = 0.0

                if idx_ret_prev > MARKET_CRASH_THRESHOLD:  # not a crash day

                    # Calculate portfolio exposure (include pending dipbuys!)
                    pv_held = sum(pos["qty"] * float(close[c].iloc[i])
                                  for c, pos in positions.items()
                                  if float(close[c].iloc[i]) > 0)
                    total_eq = cash + pv_held
                    exposure_ratio_now = EXPOSURE_MAP.get(regime, 1.0) if exposure_overlay else 1.0
                    target_exposure = total_eq * exposure_ratio_now

                    # Include pending dipbuys in invested calculation (수정1)
                    pending_dipbuy_amount = sum(p["amount"] for p in pending_dipbuys)
                    total_invested = pv_held + pending_dipbuy_amount

                    available_exposure = target_exposure - total_invested

                    if available_exposure > 0:
                        dipbuy_count_today = 0

                        for tk in list(positions.keys()):
                            if dipbuy_count_today >= DIPBUY_MAX_PER_DAY:
                                break

                            pos = positions[tk]

                            # 1-time limit per position
                            if pos.get("dipbuy_count", 0) >= DIPBUY_MAX_COUNT:
                                continue

                            # 3-day cooldown
                            if i - pos.get("last_dipbuy_idx", -999) < DIPBUY_COOLDOWN:
                                continue

                            # RS filter: absolute momentum must be positive
                            current_mom = calc_momentum(
                                close[tk].iloc[:i], config.MOM_LOOKBACK, config.MOM_SKIP)
                            if np.isnan(current_mom) or current_mom < 0:
                                continue

                            # RS decay: current_mom >= entry_mom * 0.9
                            entry_mom = pos.get("entry_mom", 0)
                            mom_ok = (current_mom >= entry_mom * DIPBUY_MOM_DECAY)

                            # Short-term rebound signal (hardened: 수정5)
                            # Use i-1 as signal day (yesterday's close)
                            recent_close = close[tk].iloc[max(0, i-5):i].values.astype(float)
                            recent_valid = recent_close[recent_close > 0]

                            three_day_up = False
                            if len(recent_valid) >= 3:
                                last3 = recent_valid[-3:]
                                three_day_up = all(last3[j] > last3[j-1] for j in range(1, len(last3)))

                            # Price > MA5 check
                            ma5_val = float(stock_ma5[tk].iloc[i-1]) if not pd.isna(stock_ma5[tk].iloc[i-1]) else 0
                            price_above_ma5 = float(close[tk].iloc[i-1]) > ma5_val if ma5_val > 0 else False

                            # Bounce > 5%
                            bounce = False
                            if len(recent_valid) >= 5:
                                five_low = recent_valid[-5:].min()
                                if five_low > 0:
                                    bounce = (float(close[tk].iloc[i-1]) / five_low - 1) > DIPBUY_BOUNCE_MIN

                            # Hardened: (three_day_up AND price_above_ma5) OR bounce>5%
                            short_signal = (three_day_up and price_above_ma5) or bounce

                            # BOTH conditions required (수정3: AND not OR)
                            if not (mom_ok and short_signal):
                                continue

                            # Unrealized loss check (avg_price based, 수정4)
                            current_price = float(close[tk].iloc[i-1])  # signal day
                            if current_price <= 0:
                                continue
                            avg_price = (pos["qty"] * pos["entry_price"] + pos["buy_cost_total"]) / pos["qty"] if pos["qty"] > 0 else pos["entry_price"]
                            unrealized = (current_price - avg_price) / avg_price
                            if unrealized > DIPBUY_MIN_LOSS:
                                continue

                            # DipBuy amount: min(3% of original, available exposure)
                            dipbuy_amount = min(
                                pos["original_per_pos"] * DIPBUY_RATIO,
                                available_exposure
                            )
                            if dipbuy_amount <= 0:
                                continue

                            # Max position size check
                            current_pos_value = pos["qty"] * current_price
                            if current_pos_value + dipbuy_amount > pos["original_per_pos"]:
                                dipbuy_amount = pos["original_per_pos"] - current_pos_value
                                if dipbuy_amount <= 0:
                                    continue

                            # Queue for T+1 execution
                            pending_dipbuys.append({
                                "tk": tk,
                                "target_idx": i + 1,
                                "amount": dipbuy_amount,
                                "signal_mom": round(current_mom, 4),
                                "signal_unrealized": round(unrealized, 4),
                                "bear_streak": prev_bear_streak,
                            })

                            # Reduce available exposure for next candidate
                            available_exposure -= dipbuy_amount
                            dipbuy_count_today += 1

                            # Signal log
                            dipbuy_logs.append({
                                "date": str(dt.date()),
                                "ticker": tk,
                                "fill_price": 0,
                                "add_qty": 0,
                                "amount": round(dipbuy_amount, 0),
                                "signal_mom": round(current_mom, 4),
                                "signal_unrealized": round(unrealized, 4),
                                "bear_streak": prev_bear_streak,
                                "event": "SIGNAL",
                            })

        # ── Exposure log (every day, for verification) ────────────
        if bear_dipbuy and positions:
            pv_held_log = sum(pos["qty"] * max(0, float(close[c].iloc[i]))
                              for c, pos in positions.items()
                              if not pd.isna(close[c].iloc[i]))
            total_eq_log = cash + pv_held_log
            exp_ratio_log = EXPOSURE_MAP.get(regime, 1.0) if exposure_overlay else 1.0
            target_exp_log = total_eq_log * exp_ratio_log
            pending_amt = sum(p["amount"] for p in pending_dipbuys)
            over_flag = (pv_held_log + pending_amt) > target_exp_log * 1.01  # 1% tolerance

            exposure_logs.append({
                "date": str(dt.date()),
                "total_invested": round(pv_held_log),
                "pending_dipbuy": round(pending_amt),
                "target_exposure": round(target_exp_log),
                "total_eq": round(total_eq_log),
                "over_exposure": over_flag,
            })

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
                entry_price=pos["entry_price"],
                exit_price=p,
                pnl_pct=pnl,
                pnl_amount=net - invested,
                hold_days=end_i - pos["entry_idx"],
                exit_reason="EOD",
                dipbuy_count=pos.get("dipbuy_count", 0),
            ))

    return (pd.Series(equity_hist).sort_index(), trades, regime_log,
            rebal_log, dipbuy_logs, exposure_logs)


# ── Regime Decomposition ─────────────────────────────────────────────────────
def decompose_by_regime_segments(eq: pd.Series, regime_log: list) -> dict:
    if not regime_log:
        return {}
    rl = pd.DataFrame(regime_log)
    rl["date"] = pd.to_datetime(rl["date"])
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


# ── DipBuy Analysis ──────────────────────────────────────────────────────────
def analyze_dipbuys(dipbuy_logs: list, trades: list) -> dict:
    """Analyze dipbuy effectiveness."""
    if not dipbuy_logs:
        return {"total_signals": 0, "total_fills": 0}

    dl = pd.DataFrame(dipbuy_logs)
    signals = dl[dl["event"] == "SIGNAL"]
    fills = dl[dl["event"] == "FILL"]

    # DipBuy vs non-DipBuy trade performance
    if trades:
        tdf = pd.DataFrame(trades)
        dipbuy_trades = tdf[tdf.get("dipbuy_count", 0) > 0] if "dipbuy_count" in tdf.columns else pd.DataFrame()
        nodipbuy_trades = tdf[tdf.get("dipbuy_count", 0) == 0] if "dipbuy_count" in tdf.columns else tdf
    else:
        dipbuy_trades = pd.DataFrame()
        nodipbuy_trades = pd.DataFrame()

    # Monthly frequency
    if len(fills) > 0:
        fills_dt = pd.to_datetime(fills["date"])
        months_span = max(1, (fills_dt.max() - fills_dt.min()).days / 30)
        monthly_freq = len(fills) / months_span
    else:
        monthly_freq = 0

    # Bear phase distribution (early/mid/late)
    bear_phase = {"early": 0, "mid": 0, "late": 0}
    for _, row in fills.iterrows():
        streak = row.get("bear_streak", 0)
        if streak <= 3:
            bear_phase["early"] += 1
        elif streak <= 7:
            bear_phase["mid"] += 1
        else:
            bear_phase["late"] += 1

    return {
        "total_signals": len(signals),
        "total_fills": len(fills),
        "fill_rate": round(len(fills) / max(len(signals), 1) * 100, 1),
        "monthly_freq": round(monthly_freq, 1),
        "avg_fill_amount": round(fills["amount"].mean(), 0) if len(fills) > 0 else 0,
        "dipbuy_trade_pnl": round(dipbuy_trades["pnl_pct"].mean() * 100, 2) if len(dipbuy_trades) > 0 else 0,
        "nodipbuy_trade_pnl": round(nodipbuy_trades["pnl_pct"].mean() * 100, 2) if len(nodipbuy_trades) > 0 else 0,
        "bear_phase": bear_phase,
        "unique_tickers": fills["ticker"].nunique() if len(fills) > 0 else 0,
    }


# ── Main Comparison Runner ───────────────────────────────────────────────────
CONFIGS = [
    {"name": "Baseline",          "regime_filter": False, "exposure_overlay": False, "symmetric": False, "bear_dipbuy": False},
    {"name": "Regime+Exposure",   "regime_filter": True,  "exposure_overlay": True,  "symmetric": False, "bear_dipbuy": False},
    {"name": "R+E+DipBuy",       "regime_filter": True,  "exposure_overlay": True,  "symmetric": False, "bear_dipbuy": True},
]


def main():
    parser = argparse.ArgumentParser(description="Gen4 BEAR Dip-Buy Backtester")
    parser.add_argument("--start-is", default="2019-01-02")
    parser.add_argument("--end-is", default="2023-12-29")
    parser.add_argument("--start-oos", default="2024-01-02")
    parser.add_argument("--end-oos", default="2026-03-20")
    args = parser.parse_args()

    config = Gen4Config()

    print("=" * 90)
    print("  Gen4 BEAR Dip-Buy Backtest")
    print("  LowVol+Mom 12-1 x Regime+Exposure x BEAR DipBuy (3%, 1x, AND conditions)")
    print("  Safeguards: pending exposure sync, T+1 signal timing, daily cap, RS+bounce AND")
    print("=" * 90)

    t0 = time.time()

    # Load data
    print(f"\n[1/5] Loading OHLCV from {config.OHLCV_DIR}...")
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

    # Precompute MA200
    print("[2/5] Precomputing MA200...")
    kospi_ma200 = idx_close.rolling(MA_WINDOW).mean()
    stock_ma200 = close.rolling(MA_WINDOW).mean()
    print(f"  KOSPI MA200 range: {kospi_ma200.dropna().iloc[0]:.0f} ~ {kospi_ma200.dropna().iloc[-1]:.0f}")

    # Period indices
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

    # Run configs
    print(f"\n[3/5] Running {len(CONFIGS)} configs x {len(periods)} periods...")
    results = []
    saved_equities = {}
    saved_rebal_logs = {}
    saved_dipbuy_logs = {}
    saved_exposure_logs = {}
    saved_regime_log = None

    for cfg in CONFIGS:
        for pname in ["IS", "OOS", "FULL"]:
            si, ei = period_idx[pname]
            eq, trades, rlog, rebal_log, dipbuy_log, exposure_log = run_backtest_regime(
                close, opn, high, low, vol, idx_close, dates,
                si, ei, config,
                regime_filter=cfg["regime_filter"],
                exposure_overlay=cfg["exposure_overlay"],
                kospi_ma200=kospi_ma200,
                stock_ma200=stock_ma200,
                symmetric_bear=cfg["symmetric"],
                bear_dipbuy=cfg["bear_dipbuy"],
            )
            m = calc_metrics(eq, trades)

            regime_decomp = {}
            dipbuy_analysis = {}
            if pname == "FULL":
                regime_decomp = decompose_by_regime_segments(eq, rlog)
                saved_equities[cfg["name"]] = eq
                saved_rebal_logs[cfg["name"]] = rebal_log
                if cfg["bear_dipbuy"]:
                    dipbuy_analysis = analyze_dipbuys(dipbuy_log, trades)
                    saved_dipbuy_logs[cfg["name"]] = dipbuy_log
                    saved_exposure_logs[cfg["name"]] = exposure_log
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
                "dipbuy_analysis": dipbuy_analysis,
            })

            dipbuy_tag = ""
            if cfg["bear_dipbuy"] and dipbuy_log:
                fills = sum(1 for d in dipbuy_log if d["event"] == "FILL")
                dipbuy_tag = f"  [DipBuy fills={fills}]"

            print(f"  {cfg['name']:24s} {pname:4s}: "
                  f"CAGR={m.get('cagr',0)*100:+6.1f}%  "
                  f"MDD={m.get('mdd',0)*100:6.1f}%  "
                  f"Sharpe={m.get('sharpe',0):5.2f}  "
                  f"Calmar={m.get('calmar',0):5.2f}  "
                  f"Trades={m.get('n_trades',0):4d}{dipbuy_tag}")

    # ── Results ──────────────────────────────────────────────────
    print(f"\n[4/5] Results")
    elapsed = time.time() - t0

    # === Comparison Table (IS/OOS) ===
    print(f"\n{'='*110}")
    print(f"  STRATEGY COMPARISON (IS / OOS)")
    print(f"{'='*110}")
    print(f"  {'Config':24s} {'Period':5s} {'CAGR':>7s} {'MDD':>7s} {'Sharpe':>7s} "
          f"{'Calmar':>7s} {'Sortino':>8s} {'WR%':>5s} {'Trades':>6s} {'AvgHold':>7s} {'PF':>5s}")
    print(f"{'─'*110}")
    for r in results:
        if r["period"] == "FULL":
            continue
        print(f"  {r['config']:24s} {r['period']:5s} "
              f"{r['cagr']*100:+6.1f}% {r['mdd']*100:6.1f}% "
              f"{r['sharpe']:6.2f}  {r['calmar']:6.2f}  "
              f"{r['sortino']:7.2f}  "
              f"{r['win_rate']*100:4.0f}% {r['n_trades']:5d}  "
              f"{r['avg_hold']:6.1f}  {r['profit_factor']:4.1f}")
    print(f"{'='*110}")

    # === Yesterday vs Today Direct Comparison ===
    print(f"\n{'='*110}")
    print(f"  YESTERDAY (Regime+Exposure) vs TODAY (R+E+DipBuy)")
    print(f"{'='*110}")
    for pname in ["IS", "OOS", "FULL"]:
        re = [r for r in results if r["config"] == "Regime+Exposure" and r["period"] == pname]
        db = [r for r in results if r["config"] == "R+E+DipBuy" and r["period"] == pname]
        if re and db:
            re, db = re[0], db[0]
            cagr_d = (db["cagr"] - re["cagr"]) * 100
            mdd_d = (db["mdd"] - re["mdd"]) * 100
            sharpe_d = db["sharpe"] - re["sharpe"]
            calmar_d = db["calmar"] - re["calmar"]
            print(f"  {pname:4s}  CAGR: {re['cagr']*100:+5.1f}% -> {db['cagr']*100:+5.1f}% ({cagr_d:+5.2f}pp)  "
                  f"MDD: {re['mdd']*100:5.1f}% -> {db['mdd']*100:5.1f}% ({mdd_d:+5.2f}pp)  "
                  f"Sharpe: {re['sharpe']:5.2f} -> {db['sharpe']:5.2f} ({sharpe_d:+5.2f})  "
                  f"Calmar: {re['calmar']:5.2f} -> {db['calmar']:5.2f} ({calmar_d:+5.2f})")
    print(f"{'='*110}")

    # === DipBuy Analysis ===
    for cfg_name in ["R+E+DipBuy"]:
        r_full = [r for r in results if r["config"] == cfg_name and r["period"] == "FULL"]
        if r_full and r_full[0].get("dipbuy_analysis"):
            da = r_full[0]["dipbuy_analysis"]
            print(f"\n{'='*90}")
            print(f"  DIPBUY ANALYSIS (FULL period)")
            print(f"{'='*90}")
            print(f"  Signals:         {da['total_signals']}")
            print(f"  Fills:           {da['total_fills']}")
            print(f"  Fill Rate:       {da['fill_rate']}%")
            print(f"  Monthly Freq:    {da['monthly_freq']}/month")
            print(f"  Avg Fill Amount: {da['avg_fill_amount']:,.0f} KRW")
            print(f"  Unique Tickers:  {da['unique_tickers']}")
            print(f"  DipBuy Trade PnL:    {da['dipbuy_trade_pnl']:+.2f}%")
            print(f"  Non-DipBuy Trade PnL:{da['nodipbuy_trade_pnl']:+.2f}%")
            print(f"  Bear Phase Dist: early={da['bear_phase']['early']} "
                  f"mid={da['bear_phase']['mid']} late={da['bear_phase']['late']}")

            # Verdict
            print(f"\n  --- VERDICT ---")
            mdd_ok = True
            for pname in ["IS", "OOS"]:
                re = [r for r in results if r["config"] == "Regime+Exposure" and r["period"] == pname]
                db = [r for r in results if r["config"] == "R+E+DipBuy" and r["period"] == pname]
                if re and db:
                    if db[0]["mdd"] < re[0]["mdd"] * 1.01:  # 1% tolerance
                        pass
                    else:
                        mdd_ok = False
            print(f"  MDD <= R+E:      {'[PASS] PASS' if mdd_ok else '[FAIL] FAIL'}")
            cagr_improved = False
            re_oos = [r for r in results if r["config"] == "Regime+Exposure" and r["period"] == "OOS"]
            db_oos = [r for r in results if r["config"] == "R+E+DipBuy" and r["period"] == "OOS"]
            if re_oos and db_oos:
                cagr_improved = db_oos[0]["cagr"] > re_oos[0]["cagr"]
            print(f"  CAGR > R+E (OOS): {'[PASS] PASS' if cagr_improved else '[FAIL] FAIL'}")
            freq_ok = 1 <= da["monthly_freq"] <= 3
            freq_str = "[PASS] PASS" if freq_ok else f"[WARN] {da['monthly_freq']:.1f}/month"
            print(f"  DipBuy 1~3/month: {freq_str}")

    # === Exposure Verification ===
    for cfg_name, exp_logs in saved_exposure_logs.items():
        if exp_logs:
            el_df = pd.DataFrame(exp_logs)
            violations = el_df[el_df["over_exposure"] == True]
            print(f"\n  Exposure Verification [{cfg_name}]:")
            print(f"    Total days checked: {len(el_df)}")
            print(f"    Over-exposure days: {len(violations)}")
            if len(violations) > 0:
                print(f"    [FAIL] EXPOSURE VIOLATION DETECTED")
                print(violations.head(5).to_string(index=False))
            else:
                print(f"    [PASS] No exposure violations")

    # KOSPI benchmark
    for pname in ["IS", "OOS"]:
        si, ei = period_idx[pname]
        kr = float(idx_close.iloc[ei] / idx_close.iloc[si] - 1)
        print(f"\n  KOSPI Buy&Hold {pname}: {kr*100:+.1f}%")

    # IS->OOS delta
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

    print(f"\n  Elapsed: {elapsed:.0f}s")

    # ── Save outputs ──────────────────────────────────────────────
    print(f"\n[5/5] Saving outputs...")
    out_dir = config.BASE_DIR.parent / "backtest" / "results" / "gen4_bear_dipbuy"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Comparison CSV
    comp_df = pd.DataFrame([
        {k: v for k, v in r.items() if k not in ("exit_reasons", "regime_decomp", "dipbuy_analysis")}
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

    # DipBuy logs
    for cfg_name, dlog in saved_dipbuy_logs.items():
        if dlog:
            fname = cfg_name.lower().replace(" ", "_").replace("+", "_")
            pd.DataFrame(dlog).to_csv(out_dir / f"dipbuy_log_{fname}.csv", index=False)

    # Exposure logs
    for cfg_name, elog in saved_exposure_logs.items():
        if elog:
            fname = cfg_name.lower().replace(" ", "_").replace("+", "_")
            pd.DataFrame(elog).to_csv(out_dir / f"exposure_log_{fname}.csv", index=False)

    print(f"  Results saved to {out_dir}/")
    print("=" * 90)


if __name__ == "__main__":
    main()
