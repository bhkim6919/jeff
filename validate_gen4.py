"""
validate_gen4.py
=================
Gen4 LowVol+Momentum 전략 종합 검증 (8개 테스트).

1. OOS (In-Sample vs Out-of-Sample)
2. Slippage stress test
3. Universe sensitivity (Top200/500/All)
4. Survivorship bias proxy
5. Regime-based decomposition
6. Drawdown structure
7. Trade structure
8. Factor robustness (window/cutoff variations)
"""
from __future__ import annotations
import sys, warnings, time as _time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

BASE_DIR = Path(__file__).resolve().parent
OHLCV_DIR = BASE_DIR / "backtest" / "data_full" / "ohlcv"
INDEX_PATH = BASE_DIR / "backtest" / "data_full" / "index" / "KOSPI.csv"
REPORT_DIR = BASE_DIR / "backtest" / "results"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

INITIAL_CASH = 100_000_000


# ── Data Loading ────────────────────────────────────────────────────────────
def load_index() -> pd.DataFrame:
    df = pd.read_csv(INDEX_PATH)
    date_col = "index" if "index" in df.columns else "date"
    rename = {date_col: "date"}
    for s, d in [("Open","open"),("High","high"),("Low","low"),("Close","close"),("Volume","volume")]:
        if s in df.columns: rename[s] = d
    df = df.rename(columns=rename)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for c in ["open","high","low","close","volume"]:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)


def load_stocks() -> Dict[str, pd.DataFrame]:
    files = sorted(OHLCV_DIR.glob("*.csv"))
    result = {}
    for f in files:
        code = f.stem
        try:
            df = pd.read_csv(f, parse_dates=["date"])
            df = df.sort_values("date").reset_index(drop=True)
            for c in ["open","high","low","close","volume"]:
                if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
            if len(df) >= 60:
                result[code] = df
        except: pass
    return result


# ── Gen4 Core Engine ────────────────────────────────────────────────────────
def run_gen4(stock_hist: Dict[str, pd.DataFrame], idx_df: pd.DataFrame,
             start: str, end: str,
             max_pos: int = 20, rebal_days: int = 21,
             trail_stop: float = -0.12,
             entry_cost: float = 0.00115, exit_cost: float = 0.00295,
             mom_long: int = 252, mom_skip: int = 22,
             vol_window: int = 252, vol_pctile: float = 0.30,
             universe_codes: List[str] = None,
             ) -> Tuple[List[float], List[dict], List[str]]:
    """Parameterized Gen4 backtest. Returns (equity_list, trades, bdays_used)."""

    s_dt = pd.Timestamp(datetime.strptime(start, "%Y%m%d"))
    e_dt = pd.Timestamp(datetime.strptime(end, "%Y%m%d"))

    # Determine trading days from index
    idx_dates = idx_df[(idx_df["date"] >= s_dt) & (idx_df["date"] <= e_dt)]["date"]
    bdays = sorted(idx_dates.dt.strftime("%Y%m%d").tolist())
    if not bdays:
        return [], [], []

    # Filter universe
    hist = stock_hist if universe_codes is None else {c: stock_hist[c] for c in universe_codes if c in stock_hist}

    cash = float(INITIAL_CASH)
    positions = {}  # code -> {entry_price, qty, entry_date, high_wm}
    equity_list = []
    trades = []
    last_rebal = -999

    for i, d in enumerate(bdays):
        as_of = pd.Timestamp(datetime.strptime(d, "%Y%m%d"))

        # Build price map for today
        pm = {}
        for code, df in hist.items():
            match = df[df["date"] == as_of]
            if not match.empty:
                pm[code] = float(match.iloc[0]["close"])

        # Trailing stop check
        for code in list(positions):
            pos = positions[code]
            p = pm.get(code, pos["entry_price"])
            if p > pos["high_wm"]: pos["high_wm"] = p
            dd = (p - pos["high_wm"]) / pos["high_wm"] if pos["high_wm"] > 0 else 0
            if dd <= trail_stop:
                net = pos["qty"] * p * (1 - exit_cost)
                cost = pos["qty"] * pos["entry_price"]
                hd = len([x for x in bdays if pos["entry_date"] <= x <= d])
                pnl_pct = (net - cost) / cost if cost > 0 else 0
                trades.append({"pnl_pct": pnl_pct, "hold_days": hd, "exit_reason": "TRAIL",
                               "entry_date": pos["entry_date"], "exit_date": d, "code": code})
                cash += net
                del positions[code]

        # Monthly rebalance
        if i - last_rebal >= rebal_days:
            last_rebal = i

            scored = []
            for code, df in hist.items():
                sub = df[df["date"] <= as_of].tail(max(vol_window, mom_long) + 10)
                if len(sub) < max(vol_window, mom_long): continue
                c = sub["close"].values.astype(float)
                if c[-1] <= 0 or len(c) < mom_long: continue
                # Volatility
                rets = np.diff(c[-vol_window:]) / c[-vol_window:-1]
                vol = float(np.std(rets)) if len(rets) > 10 else 999
                # Momentum (skip recent mom_skip days)
                if len(c) >= mom_long:
                    mom = c[-mom_skip] / c[-mom_long] - 1 if c[-mom_long] > 0 else 0
                else:
                    mom = 0
                scored.append({"code": code, "vol": vol, "mom": mom, "last": c[-1]})

            if scored:
                sdf = pd.DataFrame(scored)
                vol_thresh = sdf["vol"].quantile(vol_pctile)
                low_vol = sdf[sdf["vol"] <= vol_thresh]
                top = low_vol.sort_values("mom", ascending=False).head(max_pos)
                target_codes = set(top["code"].tolist())

                # Sell non-targets
                for code in list(positions):
                    if code not in target_codes:
                        pos = positions[code]
                        p = pm.get(code, pos["entry_price"])
                        net = pos["qty"] * p * (1 - exit_cost)
                        cost = pos["qty"] * pos["entry_price"]
                        hd = len([x for x in bdays if pos["entry_date"] <= x <= d])
                        pnl_pct = (net - cost) / cost if cost > 0 else 0
                        trades.append({"pnl_pct": pnl_pct, "hold_days": hd, "exit_reason": "REBAL",
                                       "entry_date": pos["entry_date"], "exit_date": d, "code": code})
                        cash += net
                        del positions[code]

                # Buy new targets
                pv = sum(pos["qty"] * pm.get(c, pos["entry_price"]) for c, pos in positions.items())
                total_eq = cash + pv
                slots = max_pos - len(positions)
                new_codes = [c for c in target_codes if c not in positions]
                if new_codes and slots > 0:
                    per_pos = total_eq / max_pos
                    for code in new_codes[:slots]:
                        p = pm.get(code, 0)
                        if p <= 0: continue
                        ep = p * (1 + entry_cost)
                        qty = int(min(per_pos, cash * 0.95) / (ep * (1 + 0.00015)))
                        if qty <= 0 or qty * ep * (1 + 0.00015) > cash: continue
                        cash -= qty * ep * (1 + 0.00015)
                        positions[code] = {"entry_price": ep, "entry_date": d, "qty": qty, "high_wm": ep}

        pv = sum(pos["qty"] * pm.get(c, pos["entry_price"]) for c, pos in positions.items())
        equity_list.append(cash + pv)

    # Force close
    for code, pos in list(positions.items()):
        p = pm.get(code, pos["entry_price"])
        net = pos["qty"] * p * (1 - exit_cost)
        cost = pos["qty"] * pos["entry_price"]
        pnl_pct = (net - cost) / cost if cost > 0 else 0
        trades.append({"pnl_pct": pnl_pct, "hold_days": 0, "exit_reason": "EOD",
                       "entry_date": pos["entry_date"], "exit_date": bdays[-1], "code": code})
        cash += net

    return equity_list, trades, bdays


def calc_metrics(eq: List[float], trades: List[dict]) -> dict:
    if len(eq) < 2: return {"cagr": 0, "mdd": 0, "sharpe": 0, "total_return": 0}
    s = pd.Series(eq, dtype=float)
    init, final = s.iloc[0], s.iloc[-1]
    tr = (final - init) / init
    ny = len(s) / 252
    cagr = (final / init) ** (1 / max(ny, 0.01)) - 1 if ny > 0 else 0
    pk = s.expanding().max(); dd = (s - pk) / pk; mdd = float(dd.min())
    dr = s.pct_change().dropna()
    sharpe = float(dr.mean() / dr.std() * np.sqrt(252)) if dr.std() > 0 else 0
    dn = dr[dr < 0]
    sortino = float(dr.mean() / dn.std() * np.sqrt(252)) if dn.std() > 0 else 0
    calmar = abs(cagr / mdd) if mdd != 0 else 0
    n = len(trades)
    wr = len([t for t in trades if t["pnl_pct"] > 0]) / n if n > 0 else 0
    tw = sum(t["pnl_pct"] for t in trades if t["pnl_pct"] > 0)
    tl = abs(sum(t["pnl_pct"] for t in trades if t["pnl_pct"] <= 0))
    pf = tw / tl if tl > 0 else float("inf")
    ah = np.mean([t["hold_days"] for t in trades]) if trades else 0
    return {"total_return": tr, "cagr": cagr, "mdd": mdd, "sharpe": sharpe,
            "sortino": sortino, "calmar": calmar, "n_trades": n,
            "win_rate": wr, "profit_factor": pf, "avg_hold": ah}


def fmt_row(label, m):
    return (f"  {label:<30} {m['total_return']:>8.1%} {m['cagr']:>8.1%} {m['mdd']:>8.1%} "
            f"{m['sharpe']:>7.2f} {m.get('sortino',0):>7.2f} {m.get('calmar',0):>7.2f} "
            f"{m.get('profit_factor',0):>6.2f} {m.get('win_rate',0):>6.1%} {m.get('n_trades',0):>5d}")


HDR = (f"  {'Test':<30} {'Return':>8} {'CAGR':>8} {'MDD':>8} "
       f"{'Sharpe':>7} {'Sortin':>7} {'Calmar':>7} {'PF':>6} {'WinR':>6} {'#Tr':>5}")
SEP = "-" * 120


# ── Main ────────────────────────────────────────────────────────────────────
def main():
    print("=" * 120)
    print("  Gen4 LowVol+Momentum Comprehensive Validation")
    print("=" * 120)

    t0 = _time.time()
    idx_df = load_index()
    print(f"  Index: {len(idx_df)} days")

    print("  Loading stocks...")
    all_stocks = load_stocks()
    all_codes = sorted(all_stocks.keys())
    print(f"  Loaded: {len(all_stocks)} stocks")

    # Sort by avg market cap proxy (avg close * avg volume) for universe tiers
    avg_size = {}
    for code, df in all_stocks.items():
        if len(df) >= 252:
            avg_size[code] = (df["close"].iloc[-252:] * df["volume"].iloc[-252:]).mean()
        else:
            avg_size[code] = (df["close"] * df["volume"]).mean()
    sorted_by_size = sorted(avg_size.keys(), key=lambda x: avg_size[x], reverse=True)
    top200 = sorted_by_size[:200]
    top500 = sorted_by_size[:500]

    # ================================================================
    # TEST 1: OOS (Out-of-Sample)
    # ================================================================
    print(f"\n{'='*120}")
    print("  TEST 1: Out-of-Sample Validation")
    print(f"{'='*120}")
    print(HDR); print(SEP)

    eq_full, tr_full, bd_full = run_gen4(all_stocks, idx_df, "20190102", "20260310")
    m_full = calc_metrics(eq_full, tr_full)
    print(fmt_row("Full Period (2019-2026)", m_full))

    eq_is, tr_is, _ = run_gen4(all_stocks, idx_df, "20190102", "20221231")
    m_is = calc_metrics(eq_is, tr_is)
    print(fmt_row("In-Sample (2019-2022)", m_is))

    eq_oos, tr_oos, _ = run_gen4(all_stocks, idx_df, "20230101", "20260310")
    m_oos = calc_metrics(eq_oos, tr_oos)
    print(fmt_row("Out-of-Sample (2023-2026)", m_oos))

    print(f"\n  OOS CAGR {m_oos['cagr']:.1%} vs IS CAGR {m_is['cagr']:.1%} "
          f"-> {'PASS' if m_oos['cagr'] >= 0.15 else 'FAIL'} (threshold: 15%)")

    # ================================================================
    # TEST 2: Slippage Stress Test
    # ================================================================
    print(f"\n{'='*120}")
    print("  TEST 2: Slippage Stress Test")
    print(f"{'='*120}")
    print(HDR); print(SEP)
    print(fmt_row("Baseline (slip=0.1%)", m_full))

    for mult, slip in [(2, 0.002), (3, 0.003), (5, 0.005)]:
        ec = 0.00015 + slip
        xc = 0.00015 + slip + 0.0018
        eq_s, tr_s, _ = run_gen4(all_stocks, idx_df, "20190102", "20260310",
                                 entry_cost=ec, exit_cost=xc)
        m_s = calc_metrics(eq_s, tr_s)
        print(fmt_row(f"Slippage x{mult} ({slip:.1%})", m_s))

    # ================================================================
    # TEST 3: Universe Sensitivity
    # ================================================================
    print(f"\n{'='*120}")
    print("  TEST 3: Universe Sensitivity")
    print(f"{'='*120}")
    print(HDR); print(SEP)

    for label, codes in [("Top 200", top200), ("Top 500", top500), ("All 949", None)]:
        eq_u, tr_u, _ = run_gen4(all_stocks, idx_df, "20190102", "20260310",
                                 universe_codes=codes)
        m_u = calc_metrics(eq_u, tr_u)
        print(fmt_row(f"{label} ({len(codes) if codes else len(all_stocks)} stocks)", m_u))

    # ================================================================
    # TEST 4: Survivorship Bias Proxy
    # ================================================================
    print(f"\n{'='*120}")
    print("  TEST 4: Survivorship Bias Proxy")
    print(f"{'='*120}")

    # Check how many stocks have data gaps (delisted/suspended proxy)
    full_span = len(idx_df[(idx_df["date"] >= pd.Timestamp("2019-01-02")) &
                           (idx_df["date"] <= pd.Timestamp("2026-03-10"))])
    coverage = {}
    for code, df in all_stocks.items():
        mask = (df["date"] >= pd.Timestamp("2019-01-02")) & (df["date"] <= pd.Timestamp("2026-03-10"))
        coverage[code] = mask.sum() / full_span if full_span > 0 else 0

    full_cov = sum(1 for v in coverage.values() if v >= 0.95)
    partial_cov = sum(1 for v in coverage.values() if 0.5 <= v < 0.95)
    low_cov = sum(1 for v in coverage.values() if v < 0.5)

    print(f"  Full coverage (>=95%):   {full_cov} stocks")
    print(f"  Partial (50-95%):        {partial_cov} stocks")
    print(f"  Low (<50%):              {low_cov} stocks")
    print(f"  -> Survivorship bias risk: {'HIGH' if low_cov < 20 else 'MODERATE'} "
          f"(only {low_cov} stocks with <50% coverage)")

    # Run with only stocks that have full coverage (most biased)
    full_codes = [c for c, v in coverage.items() if v >= 0.95]
    partial_codes = [c for c, v in coverage.items() if v >= 0.50]

    print(f"\n{HDR}")
    print(SEP)
    eq_fc, tr_fc, _ = run_gen4(all_stocks, idx_df, "20190102", "20260310",
                               universe_codes=full_codes)
    m_fc = calc_metrics(eq_fc, tr_fc)
    print(fmt_row(f"Full-coverage only ({len(full_codes)})", m_fc))

    eq_pc, tr_pc, _ = run_gen4(all_stocks, idx_df, "20190102", "20260310",
                               universe_codes=partial_codes)
    m_pc = calc_metrics(eq_pc, tr_pc)
    print(fmt_row(f">=50% coverage ({len(partial_codes)})", m_pc))
    print(fmt_row("All stocks (baseline)", m_full))

    # ================================================================
    # TEST 5: Regime Decomposition
    # ================================================================
    print(f"\n{'='*120}")
    print("  TEST 5: Regime-Based Performance Decomposition")
    print(f"{'='*120}")

    # Build regime series from index
    idx_close = idx_df.set_index("date")["close"]
    ma200 = idx_close.rolling(200).mean()
    regime_series = (idx_close > ma200).map({True: "BULL", False: "BEAR"})

    eq_s = pd.Series(eq_full, index=[pd.Timestamp(datetime.strptime(d, "%Y%m%d")) for d in bd_full])
    dr_full = eq_s.pct_change().dropna()

    bull_rets = dr_full[dr_full.index.map(lambda d: regime_series.get(d, "BULL") == "BULL")]
    bear_rets = dr_full[dr_full.index.map(lambda d: regime_series.get(d, "BULL") == "BEAR")]

    def _regime_stats(rets, label):
        if len(rets) < 2:
            print(f"  {label:<15} N/A (insufficient data)")
            return
        ann_ret = float(rets.mean() * 252)
        ann_vol = float(rets.std() * np.sqrt(252))
        sharpe = ann_ret / ann_vol if ann_vol > 0 else 0
        cum = (1 + rets).cumprod()
        pk = cum.expanding().max()
        dd = (cum - pk) / pk
        mdd = float(dd.min())
        wr = float((rets > 0).sum() / len(rets))
        print(f"  {label:<15} Days={len(rets):>5}  AnnRet={ann_ret:>7.1%}  "
              f"Vol={ann_vol:>6.1%}  Sharpe={sharpe:>5.2f}  MDD={mdd:>7.1%}  WinDays={wr:>5.0%}")

    _regime_stats(dr_full, "ALL")
    _regime_stats(bull_rets, "BULL (>MA200)")
    _regime_stats(bear_rets, "BEAR (<MA200)")

    # ================================================================
    # TEST 6: Drawdown Structure
    # ================================================================
    print(f"\n{'='*120}")
    print("  TEST 6: Drawdown Structure Analysis")
    print(f"{'='*120}")

    eq_s2 = pd.Series(eq_full, dtype=float)
    pk = eq_s2.expanding().max()
    dd = (eq_s2 - pk) / pk

    # Find DD episodes (contiguous periods below -1%)
    in_dd = dd < -0.01
    episodes = []
    start_i = None
    for j in range(len(in_dd)):
        if in_dd.iloc[j] and start_i is None:
            start_i = j
        elif not in_dd.iloc[j] and start_i is not None:
            depth = float(dd.iloc[start_i:j].min())
            episodes.append({"start": start_i, "end": j, "duration": j - start_i, "depth": depth})
            start_i = None
    if start_i is not None:
        depth = float(dd.iloc[start_i:].min())
        episodes.append({"start": start_i, "end": len(dd)-1,
                         "duration": len(dd)-start_i, "depth": depth, "ongoing": True})

    print(f"  Total DD episodes (>1%): {len(episodes)}")
    if episodes:
        depths = [e["depth"] for e in episodes]
        durations = [e["duration"] for e in episodes]
        print(f"  Average depth:           {np.mean(depths):.1%}")
        print(f"  Average duration:        {np.mean(durations):.0f} days")
        print(f"  Max duration:            {max(durations)} days")
        print(f"  Median duration:         {np.median(durations):.0f} days")

        # Top 5 worst DDs
        worst = sorted(episodes, key=lambda x: x["depth"])[:5]
        print(f"\n  Top 5 Worst Drawdowns:")
        for k, ep in enumerate(worst):
            dt_start = bd_full[ep["start"]] if ep["start"] < len(bd_full) else "?"
            ongoing = " (ONGOING)" if ep.get("ongoing") else ""
            rec = ep["duration"]
            print(f"    {k+1}. {ep['depth']:>7.1%}  duration={ep['duration']:>4d}d  start={dt_start}{ongoing}")

    # ================================================================
    # TEST 7: Trade Structure
    # ================================================================
    print(f"\n{'='*120}")
    print("  TEST 7: Trade Structure Analysis")
    print(f"{'='*120}")

    if tr_full:
        pnls = [t["pnl_pct"] for t in tr_full]
        holds = [t["hold_days"] for t in tr_full]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]

        avg_win = np.mean(wins) if wins else 0
        avg_loss = np.mean(losses) if losses else 0
        expectancy = np.mean(pnls)
        win_rate = len(wins) / len(pnls)

        print(f"  Total trades:       {len(pnls)}")
        print(f"  Win rate:           {win_rate:.1%}")
        print(f"  Avg win:            {avg_win:.1%}")
        print(f"  Avg loss:           {avg_loss:.1%}")
        print(f"  Win/Loss ratio:     {abs(avg_win/avg_loss):.2f}" if avg_loss != 0 else "  Win/Loss ratio:     INF")
        print(f"  Expectancy/trade:   {expectancy:.2%}")
        print(f"  Avg hold (days):    {np.mean(holds):.1f}")
        print(f"  Median hold:        {np.median(holds):.0f}")
        print(f"  Turnover/year:      {len(pnls)/7.18/20:.1f}x")

        # By exit reason
        reasons = {}
        for t in tr_full:
            r = t["exit_reason"]
            if r not in reasons: reasons[r] = {"count": 0, "pnl_sum": 0, "wins": 0}
            reasons[r]["count"] += 1
            reasons[r]["pnl_sum"] += t["pnl_pct"]
            if t["pnl_pct"] > 0: reasons[r]["wins"] += 1

        print(f"\n  Exit Reason Breakdown:")
        print(f"    {'Reason':<15} {'Count':>6} {'WinRate':>8} {'AvgPnL':>8}")
        for r, v in sorted(reasons.items(), key=lambda x: -x[1]["count"]):
            wr = v["wins"]/v["count"] if v["count"]>0 else 0
            ap = v["pnl_sum"]/v["count"] if v["count"]>0 else 0
            print(f"    {r:<15} {v['count']:>6} {wr:>7.1%} {ap:>7.1%}")

    # ================================================================
    # TEST 8: Factor Robustness
    # ================================================================
    print(f"\n{'='*120}")
    print("  TEST 8: Factor Robustness (Parameter Sensitivity)")
    print(f"{'='*120}")

    print(f"\n  8a. Momentum Window Variations:")
    print(HDR); print(SEP)
    for label, ml, ms in [("Mom 6-1", 126, 22), ("Mom 9-1", 189, 22),
                          ("Mom 12-1 (base)", 252, 22), ("Mom 12-3", 252, 63)]:
        eq_v, tr_v, _ = run_gen4(all_stocks, idx_df, "20190102", "20260310",
                                 mom_long=ml, mom_skip=ms)
        m_v = calc_metrics(eq_v, tr_v)
        print(fmt_row(label, m_v))

    print(f"\n  8b. Volatility Cutoff Variations:")
    print(HDR); print(SEP)
    for label, vp in [("LowVol 20%ile", 0.20), ("LowVol 30%ile (base)", 0.30),
                      ("LowVol 40%ile", 0.40), ("LowVol 50%ile", 0.50)]:
        eq_v, tr_v, _ = run_gen4(all_stocks, idx_df, "20190102", "20260310",
                                 vol_pctile=vp)
        m_v = calc_metrics(eq_v, tr_v)
        print(fmt_row(label, m_v))

    print(f"\n  8c. Trail Stop Variations:")
    print(HDR); print(SEP)
    for label, ts in [("Trail -8%", -0.08), ("Trail -12% (base)", -0.12),
                      ("Trail -15%", -0.15), ("Trail -20%", -0.20), ("No Trail", -1.0)]:
        eq_v, tr_v, _ = run_gen4(all_stocks, idx_df, "20190102", "20260310",
                                 trail_stop=ts)
        m_v = calc_metrics(eq_v, tr_v)
        print(fmt_row(label, m_v))

    print(f"\n  8d. Position Count Variations:")
    print(HDR); print(SEP)
    for label, mp in [("10 stocks", 10), ("15 stocks", 15),
                      ("20 stocks (base)", 20), ("30 stocks", 30)]:
        eq_v, tr_v, _ = run_gen4(all_stocks, idx_df, "20190102", "20260310",
                                 max_pos=mp)
        m_v = calc_metrics(eq_v, tr_v)
        print(fmt_row(label, m_v))

    # ================================================================
    # FINAL SUMMARY
    # ================================================================
    elapsed = _time.time() - t0
    print(f"\n{'='*120}")
    print(f"  VALIDATION COMPLETE ({elapsed:.0f}s)")
    print(f"{'='*120}")

    # Judgment
    oos_pass = m_oos["cagr"] >= 0.15
    slip2_eq, slip2_tr, _ = run_gen4(all_stocks, idx_df, "20190102", "20260310",
                                     entry_cost=0.00015+0.002, exit_cost=0.00015+0.002+0.0018)
    slip2_m = calc_metrics(slip2_eq, slip2_tr)
    slip_pass = slip2_m["sharpe"] >= 1.0
    bear_mdd_ok = True  # from regime test
    dd_recovered = any(not ep.get("ongoing", False) for ep in episodes if ep["depth"] < -0.15) if episodes else True

    print(f"\n  PASS/FAIL CRITERIA:")
    print(f"    1. OOS CAGR >= 15%:           {'PASS' if oos_pass else 'FAIL'} ({m_oos['cagr']:.1%})")
    print(f"    2. Slip x2 Sharpe >= 1.0:     {'PASS' if slip_pass else 'FAIL'} ({slip2_m['sharpe']:.2f})")
    print(f"    3. Universe stable:           (see Test 3)")
    print(f"    4. BEAR MDD <= -25%:          (see Test 5)")
    print(f"    5. DD recoverable:            {'PASS' if dd_recovered else 'FAIL'}")


if __name__ == "__main__":
    main()
