"""
backtest_compare_full.py
=========================
Gen2 / Gen3 / Gen4 / v7.2b  4-strategy comparison.

Data: backtest/data_full/ (949 stocks, real KOSPI index, 2019~2026)

Usage:
  python backtest_compare_full.py
  python backtest_compare_full.py --start 20190102 --end 20260310
"""
from __future__ import annotations

import argparse, json, sys, warnings, time as _time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "backtest" / "data_full"
OHLCV_DIR = DATA_DIR / "ohlcv"
INDEX_DIR = DATA_DIR / "index"
REPORT_DIR = BASE_DIR / "backtest" / "results"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

# ── Common Cost Model ───────────────────────────────────────────────────────
FEE = 0.00015
SLIPPAGE = 0.001
TAX = 0.0018
ENTRY_COST = FEE + SLIPPAGE       # 0.00115
EXIT_COST = FEE + SLIPPAGE + TAX  # 0.00295

INITIAL_CASH = 100_000_000


# ── Data Layer (per-stock CSV + real KOSPI index) ──────────────────────────
def load_index(name: str) -> pd.DataFrame:
    path = INDEX_DIR / f"{name}.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    # Handle notebook format: index column = date, capitalized OHLCV
    date_col = "index" if "index" in df.columns else "date"
    rename = {date_col: "date"}
    for src, dst in [("Open","open"),("High","high"),("Low","low"),("Close","close"),("Volume","volume")]:
        if src in df.columns:
            rename[src] = dst
    df = df.rename(columns=rename)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for c in ["open", "high", "low", "close", "volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)


def load_stock_histories(start: str, end: str) -> Tuple[Dict[str, pd.DataFrame], List[str]]:
    """Load per-stock CSVs from OHLCV_DIR. Returns (stock_hist, bdays)."""
    s_dt = pd.Timestamp(datetime.strptime(start, "%Y%m%d"))
    e_dt = pd.Timestamp(datetime.strptime(end, "%Y%m%d"))

    files = sorted(OHLCV_DIR.glob("*.csv"))
    print(f"  Loading {len(files)} stock files...")
    result = {}
    all_dates = set()
    for i, f in enumerate(files):
        code = f.stem
        try:
            df = pd.read_csv(f, parse_dates=["date"])
            df = df.sort_values("date").reset_index(drop=True)
            for c in ["open", "high", "low", "close", "volume"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
            # Filter date range (keep some history before start for indicators)
            df = df[df["date"] <= e_dt].copy()
            if len(df) >= 60:
                result[code] = df
                valid = df[(df["date"] >= s_dt) & (df["date"] <= e_dt)]
                all_dates.update(valid["date"].dt.strftime("%Y%m%d").tolist())
        except Exception:
            pass
        if i % 200 == 0:
            print(f"    {i}/{len(files)} loaded...")

    bdays = sorted(all_dates)
    print(f"  Loaded {len(result)} stocks, {len(bdays)} trading days")
    return result, bdays


def build_day_price_map(stock_hist: Dict[str, pd.DataFrame],
                        date_str: str) -> Dict[str, dict]:
    """Build {code: {open, high, low, close}} for a specific date."""
    as_of = pd.Timestamp(datetime.strptime(date_str, "%Y%m%d"))
    result = {}
    for code, df in stock_hist.items():
        match = df[df["date"] == as_of]
        if not match.empty:
            row = match.iloc[0]
            result[code] = {
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
            }
    return result


# ── Common Helpers ──────────────────────────────────────────────────────────
def wilder_atr(high, low, close, period=20):
    if len(close) < period + 1:
        return 0.0
    tr = np.maximum(high[1:] - low[1:],
                    np.maximum(np.abs(high[1:] - close[:-1]),
                               np.abs(low[1:] - close[:-1])))
    if len(tr) < period:
        return 0.0
    atr = float(tr[:period].mean())
    k = 1.0 / period
    for v in tr[period:]:
        atr = atr * (1 - k) + v * k
    return atr


def get_sub(df: pd.DataFrame, as_of: pd.Timestamp, n: int = 252):
    mask = df["date"] <= as_of
    sub = df[mask]
    return sub.tail(n)


def calc_metrics(equity_list: List[float], trades: List[dict]) -> dict:
    if len(equity_list) < 2:
        return {}
    eq = pd.Series(equity_list, dtype=float)
    init, final = eq.iloc[0], eq.iloc[-1]
    total_ret = (final - init) / init
    n_years = len(eq) / 252
    cagr = (final / init) ** (1 / max(n_years, 0.01)) - 1 if n_years > 0 else 0

    peak = eq.expanding().max()
    dd = (eq - peak) / peak
    mdd = float(dd.min())

    daily_ret = eq.pct_change().dropna()
    sharpe = float(daily_ret.mean() / daily_ret.std() * np.sqrt(252)) \
        if daily_ret.std() > 0 else 0
    calmar = abs(cagr / mdd) if mdd != 0 else 0

    if trades:
        wins = [t for t in trades if t.get("pnl_pct", 0) > 0]
        losses = [t for t in trades if t.get("pnl_pct", 0) <= 0]
        win_rate = len(wins) / len(trades) if trades else 0
        tw = sum(t["pnl_pct"] for t in wins)
        tl = abs(sum(t["pnl_pct"] for t in losses))
        pf = tw / tl if tl > 0 else float("inf")
        avg_hold = np.mean([t.get("hold_days", 0) for t in trades])
        reasons = {}
        for t in trades:
            r = t.get("exit_reason", "?")
            reasons[r] = reasons.get(r, 0) + 1
    else:
        win_rate = pf = avg_hold = 0
        reasons = {}

    return {
        "final_equity": final, "total_return": total_ret, "cagr": cagr,
        "mdd": mdd, "sharpe": sharpe, "calmar": calmar,
        "n_trades": len(trades), "win_rate": win_rate,
        "profit_factor": pf, "avg_hold_days": avg_hold,
        "exit_reasons": reasons,
    }


# ============================================================================
#  STRATEGY 1: Gen2 Q-Score
# ============================================================================
def run_gen2(bdays, day_cache, stock_hist, kospi_df, kosdaq_df):
    print("\n[Gen2] Q-Score (Tech+Price, no Demand)")
    cash = float(INITIAL_CASH)
    positions = {}  # code -> {entry_price, entry_date, qty, sl, tp, q_score}
    equity_list = []
    trades = []

    BULL_T, BEAR_T = 2.5, 1.5
    MAX_POS = 20

    def _idx_score(idx_df, as_of):
        df = idx_df[idx_df["date"] <= as_of].tail(60)
        if len(df) < 60:
            return 0.0
        c = df["close"].values.astype(float)
        v = df["volume"].values.astype(float)
        h = df["high"].values.astype(float)
        l = df["low"].values.astype(float)
        s = 0.0
        if c[-5:].mean() > c[-20:].mean() > c[-60:].mean(): s += 1
        if v[-5:].mean() > v[-20:].mean(): s += 1
        if c[-1] > c[-20]: s += 1
        tr = np.maximum(h[1:]-l[1:], np.maximum(np.abs(h[1:]-c[:-1]), np.abs(l[1:]-c[:-1])))
        if len(tr) >= 14:
            atr_s = pd.Series(tr).rolling(14).mean().dropna()
            if len(atr_s) >= 2 and atr_s.iloc[-1] <= atr_s.mean(): s += 1
        return s

    def _market_state(as_of):
        scores = []
        if not kospi_df.empty: scores.append(_idx_score(kospi_df, as_of))
        if not kosdaq_df.empty: scores.append(_idx_score(kosdaq_df, as_of))
        if not scores: return "SIDEWAYS"
        avg = sum(scores)/len(scores)
        if avg >= BULL_T: return "BULL"
        if avg <= BEAR_T: return "BEAR"
        return "SIDEWAYS"

    def _tech_score(c):
        if len(c) < 30: return 0.0
        cs = pd.Series(c)
        s = 0.0
        e12 = cs.ewm(span=12, adjust=False).mean()
        e26 = cs.ewm(span=26, adjust=False).mean()
        macd = e12 - e26; sig = macd.ewm(span=9, adjust=False).mean()
        if float(macd.iloc[-1]) > float(sig.iloc[-1]): s += 0.35
        delta = cs.diff(); gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rsi = 100 - 100/(1+float(gain.iloc[-1])/float(loss.iloc[-1])) if float(loss.iloc[-1])>0 else 100
        if 40 <= rsi <= 65: s += 0.35
        ma20 = cs.rolling(20).mean()
        if float(cs.iloc[-1]) > float(ma20.iloc[-1]): s += 0.30
        bb_up = ma20 + 2*cs.rolling(20).std()
        if float(cs.iloc[-1]) > float(bb_up.iloc[-1]): s -= 0.20
        return max(0.0, min(1.0, s))

    def _price_score(c, h):
        if len(c) < 60: return 0.0
        s = 0.0; last = c[-1]
        if c[-5:].mean() > c[-20:].mean() > c[-60:].mean(): s += 0.25
        n = min(252, len(h)); hw = float(h[-n:].max())
        if hw > 0 and last >= hw*0.85: s += 0.25
        if len(h) >= 22 and last > float(h[-21:-1].max()): s += 0.25
        ma20 = c[-20:].mean()
        if last >= ma20*0.97: s += 0.25
        return min(1.0, s)

    for i, d in enumerate(bdays):
        as_of = pd.Timestamp(datetime.strptime(d, "%Y%m%d"))
        dpm = day_cache.get(d, {})
        if not dpm:
            equity_list.append(cash)
            continue
        pm = {c: v["close"] for c, v in dpm.items()}

        # Exits: SL, TP, MA20
        for code in list(positions):
            pos = positions[code]
            p = pm.get(code)
            if not p or p <= 0: continue
            dp = dpm.get(code)
            if not dp: continue
            today_low = dp["low"]
            today_high = dp["high"]
            reason = None; ep = p
            if pos["sl"] > 0 and today_low <= pos["sl"]: reason="SL"; ep=pos["sl"]
            if not reason and pos["tp"] > 0 and today_high >= pos["tp"]: reason="TP"; ep=pos["tp"]
            if not reason and code in stock_hist:
                sub = get_sub(stock_hist[code], as_of, 25)
                if len(sub) >= 20:
                    ma20 = sub["close"].iloc[-20:].mean()
                    if p < ma20: reason="MA20"; ep=p
            if reason:
                ep = max(min(ep, today_high), today_low)
                net = pos["qty"]*ep*(1-EXIT_COST)
                cost = pos["qty"]*pos["entry_price"]
                pnl = net - cost
                cash += net
                hd = len([x for x in bdays if pos["entry_date"]<=x<=d])
                trades.append({"pnl_pct": pnl/cost if cost>0 else 0, "hold_days": hd, "exit_reason": reason})
                del positions[code]

        ms = _market_state(as_of)
        # Entry
        if ms != "BEAR" and len(positions) < MAX_POS:
            scored = []
            for code, hist in stock_hist.items():
                if code in positions: continue
                sub = get_sub(hist, as_of, 120)
                if len(sub) < 60: continue
                c = sub["close"].values.astype(float)
                h = sub["high"].values.astype(float)
                last = c[-1]
                if last < 1000: continue
                # Liquidity
                v = sub["volume"].values.astype(float)
                avg_amt = (c[-20:]*v[-20:]).mean() if len(c)>=20 else 0
                if avg_amt < 2e9: continue
                ma20 = c[-20:].mean()
                if last <= ma20: continue
                if ms=="BULL" and len(c)>=20 and (last/c[-20]-1)<=0.05: continue
                if ms=="SIDEWAYS" and len(c)>=20 and (last/c[-20]-1)<=0: continue
                tech = _tech_score(c); price = _price_score(c, h)
                w = {"t":0.50,"p":0.15} if ms=="BULL" else {"t":0.30,"p":0.30}
                q = (tech*w["t"] + price*w["p"])*100
                if q > 0: scored.append((code, q, c, h, sub["low"].values.astype(float)))
            scored.sort(key=lambda x: x[1], reverse=True)
            slots = MAX_POS - len(positions)
            per_pos = cash / max(slots, 1) if slots > 0 else 0
            for code, q, c, h, l in scored[:slots]:
                ep = c[-1]*(1+ENTRY_COST)
                qty = int(per_pos/(ep*(1+FEE)))
                if qty <= 0 or qty*ep*(1+FEE) > cash: continue
                atr = wilder_atr(h, l, c, 14)
                sl_m = 4.0 if ms=="BULL" else 2.5
                sl = ep - atr*sl_m if atr > 0 else ep*0.85
                tp = ep + (ep-sl)*2
                if sl <= 0: continue
                cash -= qty*ep*(1+FEE)
                positions[code] = {"entry_price":ep,"entry_date":d,"qty":qty,"sl":sl,"tp":tp,"q_score":q}

        pv = sum(pos["qty"]*pm.get(c, pos["entry_price"]) for c, pos in positions.items())
        equity_list.append(cash + pv)
        if i % 200 == 0:
            print(f"  [{d}] eq={equity_list[-1]:,.0f} pos={len(positions)} ms={ms}")

    # Force close
    for code, pos in positions.items():
        px = pm.get(code, pos["entry_price"]) if 'pm' in dir() else pos["entry_price"]
        net = pos["qty"]*px*(1-EXIT_COST)
        cost = pos["qty"]*pos["entry_price"]
        trades.append({"pnl_pct": (net-cost)/cost if cost>0 else 0, "hold_days": 0, "exit_reason": "EOD"})
        cash += net

    return equity_list, trades


# ============================================================================
#  STRATEGY 2: Gen3 RS+돌파 (기존 backtest_gen3.py 로직)
# ============================================================================
def run_gen3(bdays, day_cache, stock_hist, kospi_df, kosdaq_df):
    print("\n[Gen3] RS Composite + Breakout + MAX_LOSS_CAP")
    cash = float(INITIAL_CASH)
    positions = {}  # code -> {entry_price, qty, sl, tp, stage, entry_date, hold_days, atr}
    equity_list = []
    trades = []

    def _regime(as_of):
        sub = kospi_df[kospi_df["date"]<=as_of].tail(201)
        if len(sub) < 200: return "BULL", 0.5
        c = sub["close"].values.astype(float)
        ma200 = c[-200:].mean()
        base = "BULL" if c[-1] > ma200 else "BEAR"
        # Breadth (simplified from stock_hist)
        above = total = 0
        for code, h in stock_hist.items():
            s = get_sub(h, as_of, 25)
            if len(s) < 20: continue
            total += 1
            if s["close"].iloc[-1] > s["close"].iloc[-20:].mean(): above += 1
        breadth = above/total if total > 0 else 0.5
        if base=="BULL" and breadth < 0.35: return "BEAR", breadth
        return base, breadth

    for i, d in enumerate(bdays):
        as_of = pd.Timestamp(datetime.strptime(d, "%Y%m%d"))
        dpm = day_cache.get(d, {})
        if not dpm:
            equity_list.append(cash)
            continue
        pm = {c: v["close"] for c, v in dpm.items()}
        low_map = {c: v["low"] for c, v in dpm.items()}
        high_map = {c: v["high"] for c, v in dpm.items()}
        open_map = {c: v["open"] for c, v in dpm.items()}

        # Exits
        for code in list(positions):
            pos = positions[code]
            pos["hold_days"] += 1
            p = pm.get(code); tl = low_map.get(code, 0); th = high_map.get(code, 0)
            to = open_map.get(code, 0)
            if not p or p <= 0: continue
            reason = None; ep = p
            # MAX_LOSS_CAP -8%
            if tl > 0 and (tl/pos["entry_price"]-1) <= -0.08:
                reason="MAX_LOSS_CAP"; ep=pos["entry_price"]*0.92
            # ATR SL
            if not reason and pos["sl"]>0 and tl<=pos["sl"]:
                reason="ATR_SL"; ep=pos["sl"]
            # TP
            if not reason and pos["tp"]>0 and th>=pos["tp"]:
                reason="TP"; ep=pos["tp"]
            # MAX_HOLD
            if not reason and pos["hold_days"]>=60:
                reason="MAX_HOLD"; ep=p
            if reason:
                ep = max(min(ep, th), tl) if th > 0 else ep
                net = pos["qty"]*ep*(1-EXIT_COST)
                cost = pos["qty"]*pos["entry_price"]
                trades.append({"pnl_pct":(net-cost)/cost if cost>0 else 0,
                               "hold_days":pos["hold_days"], "exit_reason":reason})
                cash += net
                del positions[code]

        regime, breadth = _regime(as_of)

        # Generate signals & entry (every 5 days simplified)
        if i % 5 == 0 and regime != "BEAR":
            features = []
            for code, hist in stock_hist.items():
                if code in positions: continue
                sub = get_sub(hist, as_of, 252)
                if len(sub) < 130: continue
                c = sub["close"].values.astype(float)
                h = sub["high"].values.astype(float)
                l = sub["low"].values.astype(float)
                v = sub["volume"].values.astype(float)
                last = c[-1]
                if last < 2000: continue
                avg_amt = (c[-20:]*v[-20:]).mean() if len(c)>=20 else 0
                if avg_amt < 2e9: continue
                # RS
                def _ret(n):
                    return (last/c[-(n+1)]-1) if len(c)>n and c[-(n+1)]>0 else np.nan
                rs20, rs60, rs120 = _ret(20), _ret(60), _ret(120)
                # Breakout
                breakout = 1 if len(h)>=22 and last>=float(h[-21:-1].max()) else 0
                atr = wilder_atr(h, l, c, 20)
                features.append({"code":code, "rs20":rs20, "rs60":rs60, "rs120":rs120,
                                 "breakout":breakout, "atr":atr, "last":last, "h":h, "l":l, "c":c})

            if features:
                fdf = pd.DataFrame(features)
                for col, out in [("rs20","r20"),("rs60","r60"),("rs120","r120")]:
                    v = fdf[col].notna()
                    fdf.loc[v, out] = fdf.loc[v, col].rank(pct=True)
                fdf["rs_comp"] = fdf.get("r20",0)*0.30 + fdf.get("r60",0)*0.50 + fdf.get("r120",0)*0.20
                fdf = fdf.dropna(subset=["rs_comp"])
                cands = fdf[(fdf["breakout"]==1) & (fdf["rs_comp"]>=0.80)]
                cands = cands.sort_values("rs_comp", ascending=False).head(50)

                max_pos = 20 if regime=="BULL" else 8
                weight = 0.07 if regime=="BULL" else 0.05
                sl_mult = 2.5 if regime=="BULL" else 1.0
                slots = max_pos - len(positions)
                equity_est = cash + sum(pos["qty"]*pm.get(c, pos["entry_price"]) for c, pos in positions.items())

                for _, sig in cands.iterrows():
                    if slots <= 0: break
                    code = sig["code"]
                    ep = sig["last"]*(1+ENTRY_COST)
                    atr = sig["atr"]
                    sl = ep - atr*sl_mult if atr>0 else ep*0.92
                    sl = max(sl, ep*0.92)  # MAX_LOSS_CAP clamp
                    tp = ep + (ep-sl)*2
                    alloc = equity_est*weight
                    qty = int(min(alloc, cash*0.95)/(ep*(1+FEE)))
                    if qty <= 0 or qty*ep*(1+FEE) > cash: continue
                    cash -= qty*ep*(1+FEE)
                    positions[code] = {"entry_price":ep, "entry_date":d, "qty":qty,
                                       "sl":sl, "tp":tp, "stage":"B", "hold_days":0, "atr":atr}
                    slots -= 1

        pv = sum(pos["qty"]*pm.get(c, pos["entry_price"]) for c, pos in positions.items())
        equity_list.append(cash + pv)
        if i % 200 == 0:
            print(f"  [{d}] eq={equity_list[-1]:,.0f} pos={len(positions)} regime={regime}")

    for code, pos in positions.items():
        px = pm.get(code, pos["entry_price"])
        net = pos["qty"]*px*(1-EXIT_COST); cost = pos["qty"]*pos["entry_price"]
        trades.append({"pnl_pct":(net-cost)/cost if cost>0 else 0, "hold_days":0, "exit_reason":"EOD"})
        cash += net
    return equity_list, trades


# ============================================================================
#  STRATEGY 3: Gen4 Core (LowVol + Mom12-1, Monthly Rebal)
# ============================================================================
def run_gen4(bdays, day_cache, stock_hist, kospi_df, kosdaq_df):
    print("\n[Gen4] LowVol+Mom12-1, Monthly Rebal, Trail -12%")
    cash = float(INITIAL_CASH)
    positions = {}  # code -> {entry_price, qty, entry_date, high_wm}
    equity_list = []
    trades = []
    MAX_POS = 20
    REBAL_DAYS = 21
    TRAIL_STOP = -0.12

    last_rebal = -999

    for i, d in enumerate(bdays):
        as_of = pd.Timestamp(datetime.strptime(d, "%Y%m%d"))
        dpm = day_cache.get(d, {})
        if not dpm:
            equity_list.append(cash)
            continue
        pm = {c: v["close"] for c, v in dpm.items()}

        # Update HWM and check trailing stop
        for code in list(positions):
            pos = positions[code]
            p = pm.get(code, pos["entry_price"])
            if p > pos["high_wm"]: pos["high_wm"] = p
            dd = (p - pos["high_wm"])/pos["high_wm"] if pos["high_wm"]>0 else 0
            if dd <= TRAIL_STOP:
                net = pos["qty"]*p*(1-EXIT_COST); cost = pos["qty"]*pos["entry_price"]
                hd = len([x for x in bdays if pos["entry_date"]<=x<=d])
                trades.append({"pnl_pct":(net-cost)/cost if cost>0 else 0, "hold_days":hd, "exit_reason":"TRAIL"})
                cash += net
                del positions[code]

        # Monthly rebalance
        if i - last_rebal >= REBAL_DAYS:
            last_rebal = i

            # Score all stocks: 12m vol low → 12-1m momentum high
            scored = []
            for code, hist in stock_hist.items():
                sub = get_sub(hist, as_of, 260)
                if len(sub) < 252: continue
                c = sub["close"].values.astype(float)
                if c[-1] <= 0 or c[0] <= 0: continue
                # 12m volatility (daily returns std)
                rets = np.diff(c[-252:])/c[-252:-1]
                vol12 = float(np.std(rets)) if len(rets) > 0 else 999
                # 12-1 month momentum (skip last 21 days)
                if len(c) >= 252:
                    mom = c[-22]/c[-252] - 1 if c[-252]>0 else 0
                else:
                    mom = 0
                scored.append({"code": code, "vol12": vol12, "mom": mom, "last": c[-1]})

            if scored:
                sdf = pd.DataFrame(scored)
                # Step 1: Low vol bottom 30%
                vol_thresh = sdf["vol12"].quantile(0.30)
                low_vol = sdf[sdf["vol12"] <= vol_thresh]
                # Step 2: Top momentum
                top = low_vol.sort_values("mom", ascending=False).head(MAX_POS)

                target_codes = set(top["code"].tolist())
                # Sell positions not in target
                for code in list(positions):
                    if code not in target_codes:
                        pos = positions[code]
                        p = pm.get(code, pos["entry_price"])
                        net = pos["qty"]*p*(1-EXIT_COST); cost = pos["qty"]*pos["entry_price"]
                        hd = len([x for x in bdays if pos["entry_date"]<=x<=d])
                        trades.append({"pnl_pct":(net-cost)/cost if cost>0 else 0, "hold_days":hd, "exit_reason":"REBAL"})
                        cash += net
                        del positions[code]

                # Buy new targets
                # Revalue cash
                pv = sum(pos["qty"]*pm.get(c, pos["entry_price"]) for c, pos in positions.items())
                total_eq = cash + pv
                slots = MAX_POS - len(positions)
                new_codes = [c for c in target_codes if c not in positions]
                if new_codes and slots > 0:
                    per_pos = total_eq / MAX_POS
                    for code in new_codes[:slots]:
                        p = pm.get(code, 0)
                        if p <= 0: continue
                        ep = p*(1+ENTRY_COST)
                        qty = int(min(per_pos, cash*0.95)/(ep*(1+FEE)))
                        if qty <= 0 or qty*ep*(1+FEE) > cash: continue
                        cash -= qty*ep*(1+FEE)
                        positions[code] = {"entry_price":ep, "entry_date":d, "qty":qty, "high_wm":ep}

        pv = sum(pos["qty"]*pm.get(c, pos["entry_price"]) for c, pos in positions.items())
        equity_list.append(cash + pv)
        if i % 200 == 0:
            print(f"  [{d}] eq={equity_list[-1]:,.0f} pos={len(positions)}")

    for code, pos in positions.items():
        px = pm.get(code, pos["entry_price"])
        net = pos["qty"]*px*(1-EXIT_COST); cost = pos["qty"]*pos["entry_price"]
        trades.append({"pnl_pct":(net-cost)/cost if cost>0 else 0, "hold_days":0, "exit_reason":"EOD"})
        cash += net
    return equity_list, trades


# ============================================================================
#  STRATEGY 4: v7.2b Reference (SL 물타기, NO TP, HARD/SOFT STOP)
# ============================================================================
def run_v72b(bdays, day_cache, stock_hist, kospi_df, kosdaq_df):
    print("\n[v7.2b] RS+SL Averaging, No TP, Risk Gates")
    cash = float(INITIAL_CASH)
    positions = {}  # code -> {entry_price, qty, sl, entry_date, hold_days, atr, rs, avg_cnt, stage}
    equity_list = []
    trades = []

    # Risk tracking
    prev_equity = float(INITIAL_CASH)
    month_peak = float(INITIAL_CASH)
    current_month = None

    # Regime flip gate
    regime_streak = 0
    pending_regime = "BULL"
    confirmed_regime = "BULL"

    AVG_DOWN_MAX = 2
    AVG_DOWN_RS_MIN = 0.85
    AVG_DOWN_WEIGHT = 0.50
    AVG_DOWN_ATR_MULT = 3.0

    for i, d in enumerate(bdays):
        as_of = pd.Timestamp(datetime.strptime(d, "%Y%m%d"))
        dpm = day_cache.get(d, {})
        if not dpm:
            equity_list.append(cash)
            continue
        pm = {c: v["close"] for c, v in dpm.items()}
        low_map = {c: v["low"] for c, v in dpm.items()}
        high_map = {c: v["high"] for c, v in dpm.items()}

        # Equity
        pv = sum(pos["qty"]*pm.get(c, pos["entry_price"]) for c, pos in positions.items())
        equity = cash + pv

        # Monthly peak reset
        dt = datetime.strptime(d, "%Y%m%d")
        if current_month != dt.month:
            current_month = dt.month
            month_peak = equity
        month_peak = max(month_peak, equity)

        # Risk gates
        daily_dd = (equity - prev_equity)/prev_equity if prev_equity > 0 else 0
        monthly_dd = (equity - month_peak)/month_peak if month_peak > 0 else 0

        hard_stop = monthly_dd < -0.07
        daily_kill = daily_dd < -0.04
        soft_stop = daily_dd < -0.02

        # HARD_STOP: force close all
        if hard_stop:
            for code in list(positions):
                pos = positions[code]
                p = pm.get(code, pos["entry_price"])
                net = pos["qty"]*p*(1-EXIT_COST); cost = pos["qty"]*pos["entry_price"]
                trades.append({"pnl_pct":(net-cost)/cost if cost>0 else 0,
                               "hold_days":pos["hold_days"], "exit_reason":"HARD_STOP"})
                cash += net
                del positions[code]
            pv = 0; equity = cash

        # SOFT_STOP: close worst 1
        elif soft_stop and positions:
            worst = min(positions.items(), key=lambda x: pm.get(x[0], x[1]["entry_price"])/x[1]["entry_price"]-1)
            code, pos = worst
            p = pm.get(code, pos["entry_price"])
            net = pos["qty"]*p*(1-EXIT_COST); cost = pos["qty"]*pos["entry_price"]
            trades.append({"pnl_pct":(net-cost)/cost if cost>0 else 0,
                           "hold_days":pos["hold_days"], "exit_reason":"SOFT_STOP"})
            cash += net
            del positions[code]

        # Regime detection with flip gate
        idx_sub = kospi_df[kospi_df["date"]<=as_of].tail(201)
        if len(idx_sub) >= 200:
            c_idx = idx_sub["close"].values.astype(float)
            ma200 = c_idx[-200:].mean()
            raw_regime = "BULL" if c_idx[-1] > ma200 else "BEAR"
            # Breadth
            above = total = 0
            for code_b, h in stock_hist.items():
                s = get_sub(h, as_of, 25)
                if len(s) < 20: continue
                total += 1
                if s["close"].iloc[-1] > s["close"].iloc[-20:].mean(): above += 1
            breadth = above/total if total > 0 else 0.5
            if raw_regime=="BULL" and breadth < 0.35: raw_regime = "BEAR"
            if raw_regime=="BEAR" and breadth >= 0.55: pass  # stay BEAR until MA200 flips

            # Flip gate: 2 days
            if raw_regime != confirmed_regime:
                if raw_regime == pending_regime:
                    regime_streak += 1
                else:
                    pending_regime = raw_regime
                    regime_streak = 1
                if regime_streak >= 2:
                    confirmed_regime = raw_regime
            else:
                pending_regime = confirmed_regime
                regime_streak = 0

        regime = confirmed_regime

        # RAL
        idx_ret = 0
        if len(idx_sub) >= 2:
            idx_ret = (float(idx_sub["close"].iloc[-1])/float(idx_sub["close"].iloc[-2]) - 1)
        ral_crash = idx_ret <= -0.02
        ral_surge = idx_ret >= 0.015

        # Regular exits (SL with avg-down option)
        for code in list(positions):
            pos = positions[code]
            pos["hold_days"] += 1
            p = pm.get(code); tl = low_map.get(code, 0); th = high_map.get(code, 0)
            if not p or p <= 0: continue

            reason = None; ep = p

            # SL hit
            if pos["sl"] > 0 and tl <= pos["sl"]:
                # Try averaging down
                can_avg = (regime == "BULL" and pos["avg_cnt"] < AVG_DOWN_MAX
                           and pos.get("rs", 0) >= AVG_DOWN_RS_MIN)
                if can_avg:
                    # Average down
                    equity_now = cash + sum(ps["qty"]*pm.get(cd, ps["entry_price"]) for cd, ps in positions.items())
                    weight = 0.07 if regime=="BULL" else 0.05
                    add_alloc = equity_now * weight * AVG_DOWN_WEIGHT
                    add_qty = int(min(add_alloc, cash*0.5) / (p*(1+ENTRY_COST)))
                    if add_qty > 0 and add_qty*p*(1+ENTRY_COST) <= cash:
                        old_cost = pos["qty"]*pos["entry_price"]
                        new_cost = add_qty*p*(1+ENTRY_COST)
                        cash -= new_cost
                        pos["entry_price"] = (old_cost + new_cost)/(pos["qty"]+add_qty)
                        pos["qty"] += add_qty
                        pos["sl"] = p - pos["atr"]*AVG_DOWN_ATR_MULT if pos["atr"]>0 else p*0.90
                        pos["avg_cnt"] += 1
                        continue  # Don't exit
                reason = "SL"; ep = pos["sl"]

            # RAL CRASH
            if not reason and ral_crash and pos.get("rs", 0) < 0.45:
                reason = "RAL_CRASH"; ep = p

            # RS_EXIT (monthly 1-7th)
            if not reason and dt.day <= 7 and pos.get("rs", 0) < 0.40:
                reason = "RS_EXIT"; ep = p

            # MAX_HOLD 60 calendar days
            if not reason and pos["hold_days"] >= 42:  # ~60 calendar ≈ 42 trading
                reason = "MAX_HOLD"; ep = p

            # RAL SURGE: relax SL
            if not reason and ral_surge and pos["atr"] > 0:
                new_sl = pos["sl"] - 0.50*pos["atr"]
                pos["sl"] = min(pos["sl"], new_sl)

            if reason:
                ep = max(min(ep, th), tl) if th > 0 else ep
                net = pos["qty"]*ep*(1-EXIT_COST); cost = pos["qty"]*pos["entry_price"]
                trades.append({"pnl_pct":(net-cost)/cost if cost>0 else 0,
                               "hold_days":pos["hold_days"], "exit_reason":reason})
                cash += net
                del positions[code]

        # Entry (every 5 days, no entry on kill/hard)
        if i % 5 == 0 and not hard_stop and not daily_kill:
            features = []
            for code, hist in stock_hist.items():
                if code in positions: continue
                sub = get_sub(hist, as_of, 252)
                if len(sub) < 125: continue
                c = sub["close"].values.astype(float)
                h = sub["high"].values.astype(float)
                l = sub["low"].values.astype(float)
                v = sub["volume"].values.astype(float)
                last = c[-1]
                if last < 2000: continue
                avg_amt = (c[-20:]*v[-20:]).mean() if len(c)>=20 else 0
                if avg_amt < 2e9: continue
                def _ret(n):
                    return (last/c[-(n+1)]-1) if len(c)>n and c[-(n+1)]>0 else np.nan
                rs20, rs60, rs120 = _ret(20), _ret(60), _ret(120)
                breakout = 1 if len(h)>=22 and last>=float(h[-21:-1].max()) else 0
                atr = wilder_atr(h, l, c, 20)
                features.append({"code":code,"rs20":rs20,"rs60":rs60,"rs120":rs120,
                                 "breakout":breakout,"atr":atr,"last":last})

            if features:
                fdf = pd.DataFrame(features)
                for col, out in [("rs20","r20"),("rs60","r60"),("rs120","r120")]:
                    v = fdf[col].notna()
                    fdf.loc[v, out] = fdf.loc[v, col].rank(pct=True)
                fdf["rs_comp"] = fdf.get("r20",0)*0.30 + fdf.get("r60",0)*0.50 + fdf.get("r120",0)*0.20
                fdf = fdf.dropna(subset=["rs_comp"])
                # ATR rank
                va = fdf["atr"] > 0
                fdf.loc[va, "atr_rank"] = fdf.loc[va, "atr"].rank(pct=True)
                fdf.loc[~va, "atr_rank"] = 1.0

                cands = fdf[(fdf["breakout"]==1) & (fdf["rs_comp"]>=0.80)]
                cands = cands.sort_values("rs_comp", ascending=False)

                max_pos = 20 if regime=="BULL" else 8
                weight = 0.07 if regime=="BULL" else 0.05
                sl_mult = 4.0 if regime=="BULL" else 1.0
                atr_cap = 0.70 if regime=="BULL" else 0.40

                equity_now = cash + sum(ps["qty"]*pm.get(cd, ps["entry_price"]) for cd, ps in positions.items())
                slots = max_pos - len(positions)

                for _, sig in cands.iterrows():
                    if slots <= 0: break
                    code = sig["code"]
                    if sig.get("atr_rank", 1) > atr_cap: continue
                    if regime=="BEAR" and sig["rs_comp"] < 0.90: continue
                    ep = sig["last"]*(1+ENTRY_COST)
                    atr = sig["atr"]
                    sl = ep - atr*sl_mult if atr > 0 else ep*0.85
                    if sl <= 0 or (ep-sl)/ep < 0.01: continue
                    alloc = equity_now*weight
                    qty = int(min(alloc, cash*0.95)/(ep*(1+FEE)))
                    if qty <= 0 or qty*ep*(1+FEE) > cash: continue
                    cash -= qty*ep*(1+FEE)
                    positions[code] = {"entry_price":ep,"entry_date":d,"qty":qty,"sl":sl,
                                       "hold_days":0,"atr":atr,"rs":sig["rs_comp"],
                                       "avg_cnt":0,"stage":"B"}
                    slots -= 1

        pv = sum(pos["qty"]*pm.get(c, pos["entry_price"]) for c, pos in positions.items())
        equity = cash + pv
        equity_list.append(equity)
        prev_equity = equity

        if i % 200 == 0:
            print(f"  [{d}] eq={equity:,.0f} pos={len(positions)} regime={regime}")

    for code, pos in positions.items():
        px = pm.get(code, pos["entry_price"])
        net = pos["qty"]*px*(1-EXIT_COST); cost = pos["qty"]*pos["entry_price"]
        trades.append({"pnl_pct":(net-cost)/cost if cost>0 else 0, "hold_days":0, "exit_reason":"EOD"})
        cash += net
    return equity_list, trades


# ============================================================================
#  Main
# ============================================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="20190102")
    parser.add_argument("--end", default="20260310")
    args = parser.parse_args()

    print("=" * 70)
    print("  Q-TRON Strategy Comparison (FULL DATA: 949 stocks, real KOSPI)")
    print("=" * 70)

    kospi_df = load_index("KOSPI")
    kosdaq_df = pd.DataFrame()  # KOSDAQ index not available in full data
    print(f"  Index: KOSPI {len(kospi_df)}d (real index)")

    print("\n  Loading stock histories...")
    stock_hist, bdays = load_stock_histories(args.start, args.end)
    print(f"  Period: {bdays[0]} ~ {bdays[-1]} ({len(bdays)} trading days)")
    print(f"  Initial: {INITIAL_CASH:,}")

    # Pre-build day price maps for fast lookup
    print("  Pre-building daily price maps...")
    day_cache = {}  # date_str -> {code: {open,high,low,close}}
    for i, d in enumerate(bdays):
        day_cache[d] = build_day_price_map(stock_hist, d)
        if i % 200 == 0:
            print(f"    {i}/{len(bdays)}...")
    print(f"  Built {len(day_cache)} price maps")

    results = {}
    t0 = _time.time()

    for name, fn in [("Gen2_QScore", run_gen2), ("Gen3_RS_Breakout", run_gen3),
                     ("Gen4_LowVol_Mom", run_gen4), ("v7.2b_AvgDown", run_v72b)]:
        ts = _time.time()
        eq, tr = fn(bdays, day_cache, stock_hist, kospi_df, kosdaq_df)
        elapsed = _time.time() - ts
        m = calc_metrics(eq, tr)
        results[name] = {"equity": eq, "trades": tr, "metrics": m}
        print(f"  [{name}] done in {elapsed:.0f}s - Return: {m.get('total_return',0):.1%}")

    total_time = _time.time() - t0
    print(f"\n  Total time: {total_time:.0f}s")

    # Print comparison
    print("\n" + "=" * 90)
    print(f"  {'Strategy':<25} {'Return':>10} {'CAGR':>8} {'MDD':>8} {'Sharpe':>8} {'PF':>8} {'WinR':>8} {'Trades':>8} {'AvgHold':>8}")
    print("-" * 90)
    for name, r in results.items():
        m = r["metrics"]
        print(f"  {name:<25} {m.get('total_return',0):>9.1%} {m.get('cagr',0):>7.1%} "
              f"{m.get('mdd',0):>7.1%} {m.get('sharpe',0):>7.2f} {m.get('profit_factor',0):>7.2f} "
              f"{m.get('win_rate',0):>7.1%} {m.get('n_trades',0):>7d} {m.get('avg_hold_days',0):>7.1f}")
    print("=" * 90)

    # Exit reasons
    for name, r in results.items():
        reasons = r["metrics"].get("exit_reasons", {})
        if reasons:
            print(f"\n  [{name}] Exit Reasons:")
            for rr, cnt in sorted(reasons.items(), key=lambda x: -x[1]):
                print(f"    {rr:>15s}: {cnt}")

    # Save equity curves
    eq_df = pd.DataFrame({"date": bdays})
    for name, r in results.items():
        eq_df[name] = r["equity"][:len(bdays)]
    eq_path = REPORT_DIR / "comparison_full_equity.csv"
    eq_df.to_csv(eq_path, index=False)
    print(f"\n  Equity curves saved: {eq_path}")

    # Save HTML
    html_path = REPORT_DIR / "comparison_full.html"
    rows_html = ""
    for name, r in results.items():
        m = r["metrics"]
        ret_cls = "pos" if m.get("total_return",0)>=0 else "neg"
        rows_html += f"""<tr>
        <td>{name}</td>
        <td class="{ret_cls}">{m.get('total_return',0):.1%}</td>
        <td class="{ret_cls}">{m.get('cagr',0):.1%}</td>
        <td class="neg">{m.get('mdd',0):.1%}</td>
        <td>{m.get('sharpe',0):.2f}</td>
        <td>{m.get('profit_factor',0):.2f}</td>
        <td>{m.get('win_rate',0):.1%}</td>
        <td>{m.get('n_trades',0)}</td>
        <td>{m.get('avg_hold_days',0):.1f}</td>
        </tr>"""

    exit_html = ""
    for name, r in results.items():
        reasons = r["metrics"].get("exit_reasons", {})
        exit_html += f"<h3>{name}</h3><table><tr><th>Reason</th><th>Count</th></tr>"
        for rr, cnt in sorted(reasons.items(), key=lambda x: -x[1]):
            exit_html += f"<tr><td>{rr}</td><td>{cnt}</td></tr>"
        exit_html += "</table>"

    html = f"""<!doctype html><html lang="ko"><head><meta charset="utf-8"/>
<title>Q-TRON 4-Strategy Comparison</title>
<style>
body{{font-family:'Malgun Gothic',monospace;background:#1a1a2e;color:#e0e0e0;padding:20px;max-width:1200px;margin:0 auto;}}
h1{{color:#00d4ff;}} h2{{color:#ffd700;}} h3{{color:#aaa;}}
table{{border-collapse:collapse;margin:10px 0;width:100%;}}
td,th{{border:1px solid #444;padding:8px 12px;text-align:right;}}
th{{background:#2a2a4a;}}
.pos{{color:#00ff88;font-weight:bold;}} .neg{{color:#ff4444;font-weight:bold;}}
</style></head><body>
<h1>Q-TRON 4-Strategy Comparison</h1>
<p>Period: {args.start} ~ {args.end} ({len(bdays)} trading days) | Initial: {INITIAL_CASH:,}</p>
<h2>Performance Summary</h2>
<table>
<tr><th>Strategy</th><th>Return</th><th>CAGR</th><th>MDD</th><th>Sharpe</th><th>PF</th><th>WinRate</th><th>Trades</th><th>AvgHold</th></tr>
{rows_html}
</table>
<h2>Exit Reasons</h2>
{exit_html}
<p style="color:#666;margin-top:30px;">
Gen2: Q-Score (Tech+Price, Demand=0) | Gen3: RS+Breakout+MAX_LOSS_CAP(-8%)+TP |
Gen4: LowVol+Mom12-1 Monthly | v7.2b: RS+SL Averaging+No TP+Risk Gates
</p>
</body></html>"""
    html_path.write_text(html, encoding="utf-8")
    print(f"  HTML report saved: {html_path}")


if __name__ == "__main__":
    main()
