"""
compare_universe_size.py - Gen4 Core x universe size comparison
================================================================
Top100 / Top200 / Top300 / Full KOSPI (945)
Universe = KOSPI only, ranked by 20-day avg trading amount
Reranked monthly (not fixed)
"""
from __future__ import annotations
import json, time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Set, Tuple
import numpy as np, pandas as pd

BASE = Path(__file__).resolve().parent.parent
DATA = BASE / "backtest" / "data_full"
OHLCV_DIR = DATA / "ohlcv"
SECTOR_FILE = DATA / "sector_map.json"
RESULT_DIR = BASE / "backtest" / "results" / "universe_size"

INITIAL_CASH = 100_000_000
BUY_COST = 0.00115
SELL_COST = 0.00295
N_STOCKS = 20
REBAL_DAYS = 21
TRAIL_PCT = 0.12
MIN_CLOSE = 2000
MIN_AVG_AMT = 2_000_000_000

@dataclass
class Pos:
    ticker: str
    shares: int
    entry_price: float
    entry_date: str
    hwm: float

def load_data():
    with open(SECTOR_FILE, encoding="utf-8") as f:
        sm = json.load(f)
    kospi = {t for t, v in sm.items() if v.get("market") == "KOSPI"}
    all_data = {}
    for f in sorted(OHLCV_DIR.glob("*.csv")):
        t = f.stem
        if t not in kospi: continue
        try:
            df = pd.read_csv(f)
            if len(df) < 125: continue
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            df = df.sort_values("date").reset_index(drop=True)
            for c in ["open","high","low","close","volume"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            df = df.dropna(subset=["close"])
            df["ret_daily"] = df["close"].pct_change()
            df["vol_252"] = df["ret_daily"].rolling(252).std()
            df["mom_12_1"] = df["close"].shift(21) / df["close"].shift(252)
            df["volume_ma20"] = df["volume"].rolling(20).mean()
            df["avg_amt"] = df["close"] * df["volume_ma20"]
            all_data[t] = df
        except: continue
    dates = sorted(set().union(*(df["date"].tolist() for df in all_data.values())))
    return all_data, dates

def get_topN(all_data, date_str, n):
    """Get top N tickers by avg daily amount on date_str."""
    amts = {}
    for t, df in all_data.items():
        idx = df.index[df["date"] == date_str]
        if len(idx) == 0: continue
        row = df.iloc[idx[-1]]
        c = float(row["close"])
        a = float(row.get("avg_amt", 0))
        if c >= MIN_CLOSE and a >= MIN_AVG_AMT:
            amts[t] = a
    if not amts: return set()
    return set(pd.Series(amts).nlargest(n).index)

def score_gen4(all_data, universe, prev_date):
    vol_data, mom_data = {}, {}
    for t in universe:
        if t not in all_data: continue
        df = all_data[t]
        idx = df.index[df["date"] == prev_date]
        if len(idx) == 0: continue
        row = df.iloc[idx[-1]]
        v = float(row.get("vol_252", np.nan))
        m = float(row.get("mom_12_1", np.nan))
        if not np.isnan(v) and v > 0: vol_data[t] = v
        if not np.isnan(m) and m > 0: mom_data[t] = m
    if not vol_data: return []
    vol_s = pd.Series(vol_data)
    threshold = vol_s.quantile(0.30)
    low_vol = set(vol_s[vol_s <= threshold].index)
    mom_s = pd.Series({t: v for t, v in mom_data.items() if t in low_vol})
    if len(mom_s) == 0: return []
    return mom_s.sort_values(ascending=False).head(N_STOCKS).index.tolist()

def run_bt(label, all_data, dates, univ_size):
    cash = float(INITIAL_CASH)
    positions = {}
    trades = []
    equity_log = []
    last_rebal = -999
    current_univ = set()
    current_month = ""

    for di, ds in enumerate(dates):
        if di < 253: continue

        # Monthly universe refresh
        ms = ds[:7]
        if ms != current_month:
            current_month = ms
            prev_d = dates[di-1] if di > 0 else ds
            if univ_size == 0:  # full
                current_univ = set()
                for t, df in all_data.items():
                    idx = df.index[df["date"] == prev_d]
                    if len(idx) == 0: continue
                    row = df.iloc[idx[-1]]
                    c = float(row["close"])
                    a = float(row.get("avg_amt", 0))
                    if c >= MIN_CLOSE and a >= MIN_AVG_AMT:
                        current_univ.add(t)
            else:
                current_univ = get_topN(all_data, prev_d, univ_size)

        # Trail stop
        for t in list(positions.keys()):
            pos = positions[t]
            df = all_data.get(t)
            if df is None: continue
            idx = df.index[df["date"] == ds]
            if len(idx) == 0: continue
            p = float(df.iloc[idx[-1]]["close"])
            if p > pos.hwm: pos.hwm = p
            if pos.hwm > 0 and (p - pos.hwm) / pos.hwm <= -TRAIL_PCT:
                cash += pos.shares * p * (1 - SELL_COST)
                cb = pos.shares * pos.entry_price * (1 + BUY_COST)
                trades.append({"pnl_pct": (pos.shares*p*(1-SELL_COST)-cb)/cb, "reason": "TRAIL"})
                del positions[t]

        # Rebalance
        if di - last_rebal >= REBAL_DAYS:
            prev_d = dates[di-1] if di > 0 else ds
            universe_now = {}
            for t in current_univ:
                if t not in all_data: continue
                df = all_data[t]
                idx = df.index[df["date"] == ds]
                if len(idx) > 0:
                    universe_now[t] = df.iloc[idx[-1]]
            target = score_gen4(all_data, set(universe_now.keys()), prev_d)
            if not target:
                eq = cash + sum(
                    float(all_data[t].iloc[all_data[t].index[all_data[t]["date"]==ds][-1]]["close"])*p.shares
                    if ds in all_data[t]["date"].values else p.entry_price*p.shares
                    for t,p in positions.items())
                equity_log.append({"date": ds, "equity": eq})
                continue
            target_set = set(target[:N_STOCKS])
            last_rebal = di

            for t in list(positions.keys()):
                if t not in target_set:
                    pos = positions[t]
                    df = all_data.get(t)
                    price = pos.entry_price
                    if df is not None:
                        idx = df.index[df["date"] == ds]
                        if len(idx) > 0: price = float(df.iloc[idx[-1]]["open"])
                    cash += pos.shares * price * (1 - SELL_COST)
                    cb = pos.shares * pos.entry_price * (1 + BUY_COST)
                    trades.append({"pnl_pct": (pos.shares*price*(1-SELL_COST)-cb)/cb, "reason": "REBAL"})
                    del positions[t]

            eq = cash
            for t, pos in positions.items():
                df = all_data.get(t)
                if df is not None:
                    idx = df.index[df["date"] == ds]
                    if len(idx) > 0:
                        eq += float(df.iloc[idx[-1]]["close"]) * pos.shares
                        continue
                eq += pos.entry_price * pos.shares

            n_buy = N_STOCKS - len(positions)
            if n_buy > 0 and eq > 0:
                per = eq * 0.95 / N_STOCKS
                for t in target:
                    if t in positions or n_buy <= 0: continue
                    df = all_data.get(t)
                    if df is None: continue
                    idx = df.index[df["date"] == ds]
                    if len(idx) == 0: continue
                    price = float(df.iloc[idx[-1]]["open"])
                    if price <= 0: continue
                    budget = min(per, cash) / (1 + BUY_COST)
                    shares = int(budget / price)
                    if shares <= 0: continue
                    cost = shares * price * (1 + BUY_COST)
                    if cost > cash: continue
                    cash -= cost
                    positions[t] = Pos(t, shares, price, ds, price)
                    n_buy -= 1

        eq = cash
        for t, pos in positions.items():
            df = all_data.get(t)
            if df is not None:
                idx = df.index[df["date"] == ds]
                if len(idx) > 0:
                    eq += float(df.iloc[idx[-1]]["close"]) * pos.shares
                    continue
            eq += pos.entry_price * pos.shares
        equity_log.append({"date": ds, "equity": eq})

    edf = pd.DataFrame(equity_log)
    if len(edf) == 0: return None
    final = edf["equity"].iloc[-1]
    ny = len(edf)/252
    cagr = (final/INITIAL_CASH)**(1/ny)-1 if ny > 0 else 0
    peak = edf["equity"].cummax()
    mdd = ((edf["equity"]-peak)/peak).min()
    dr = edf["equity"].pct_change().dropna()
    sharpe = dr.mean()/dr.std()*np.sqrt(252) if dr.std()>0 else 0
    tdf = pd.DataFrame(trades)
    wr = (tdf["pnl_pct"]>0).mean() if len(tdf)>0 else 0
    edf.to_csv(RESULT_DIR / f"equity_{label}.csv", index=False)
    return {"label": label, "cagr": cagr, "mdd": mdd, "sharpe": sharpe,
            "trades": len(trades), "wr": wr}

def main():
    all_data, dates = load_data()
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Loaded {len(all_data)} KOSPI tickers, {len(dates)} dates")

    configs = [
        ("Top100", 100),
        ("Top200", 200),
        ("Top300", 300),
        ("Full_KOSPI", 0),
    ]
    results = []
    for label, size in configs:
        print(f"\n  Running {label}...", end=" ", flush=True)
        t0 = time.time()
        r = run_bt(label, all_data, dates, size)
        print(f"{time.time()-t0:.0f}s")
        if r:
            print(f"    CAGR={r['cagr']:.2%}  MDD={r['mdd']:.2%}  Sharpe={r['sharpe']:.3f}  Trades={r['trades']}  WR={r['wr']:.1%}")
            results.append(r)

    print(f"\n{'='*60}")
    for r in results:
        print(f"  {r['label']:15s}  CAGR={r['cagr']:+.2%}  MDD={r['mdd']:.2%}  Sharpe={r['sharpe']:.3f}")
    with open(RESULT_DIR / "summary.json", "w") as f:
        json.dump([{k: f"{v:.4f}" if isinstance(v,float) else v for k,v in r.items()} for r in results], f, indent=2)

if __name__ == "__main__":
    main()
