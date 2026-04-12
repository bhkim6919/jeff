"""
compare_fair.py -동일 조건 공정 비교 (종목별 순회, ffill 없음)
================================================================
Gen3 v7 백테스터와 동일한 방식으로 Gen4/PureMom/Hybrid 실행.

통일 조건:
  - 종목별 개별 DataFrame 순회 (매트릭스 ffill 금지)
  - 유니버스: 종가>=2000, 20일 평균 거래대금>=20억
  - look-ahead 방지: T-1 시그널, T+0 open 진입
  - 비용: BUY 0.115%, SELL 0.295%
  - 리밸: 21거래일
  - 포지션: 20종목 균등배분
  - 청산: Trailing Stop -12% (close-based)
  - 빈자리 보충 없음

Usage:
    cd C:\\Q-TRON-32_ARCHIVE
    .venv\\Scripts\\python.exe backtest\\compare_fair.py
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

# ── Paths ────────────────────────────────────────────────────────

BASE = Path(__file__).resolve().parent.parent
DATA = BASE / "backtest" / "data_full"
OHLCV_DIR = DATA / "ohlcv"
INDEX_FILE = DATA / "index" / "KOSPI.csv"
SECTOR_FILE = DATA / "sector_map.json"
RESULT_DIR = BASE / "backtest" / "results" / "fair_compare"

# ── Parameters ──────────────────────────────────────────────────

INITIAL_CASH = 100_000_000
BUY_COST = 0.00115
SELL_COST = 0.00295
N_STOCKS = 20
REBAL_DAYS = 21
TRAIL_PCT = 0.12  # positive, used as abs threshold
MIN_CLOSE = 2000
MIN_AVG_AMT = 2_000_000_000  # 20억


# ── Position ────────────────────────────────────────────────────

@dataclass
class Pos:
    ticker: str
    shares: int
    entry_price: float
    entry_date: str
    hwm: float


# ── Data Loading ────────────────────────────────────────────────

def load_data() -> Tuple[Dict[str, pd.DataFrame], Set[str], Set[str], dict]:
    """Load all OHLCV + sector map. Returns (all_data, kospi_set, kosdaq_set, sector_map)."""
    print("Loading data...")
    with open(SECTOR_FILE, encoding="utf-8") as f:
        sector_map = json.load(f)

    kospi = {t for t, v in sector_map.items() if v.get("market") == "KOSPI"}
    kosdaq = {t for t, v in sector_map.items() if v.get("market") == "KOSDAQ"}

    all_data: Dict[str, pd.DataFrame] = {}
    for f in sorted(OHLCV_DIR.glob("*.csv")):
        ticker = f.stem
        try:
            df = pd.read_csv(f)
            if len(df) < 125:
                continue
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            df = df.sort_values("date").reset_index(drop=True)
            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["close"])

            # Precompute signals
            c = df["close"]
            df["ret_daily"] = c.pct_change()
            df["vol_252"] = df["ret_daily"].rolling(252).std()
            df["mom_12_1"] = c.shift(21) / c.shift(252)  # T-1 shift built in
            df["ma20"] = c.rolling(20).mean()
            df["volume_ma20"] = df["volume"].rolling(20).mean()
            df["avg_amt"] = c * df["volume_ma20"]

            # RS components (for hybrid)
            df["rs20"] = c / c.shift(20) - 1
            df["rs60"] = c / c.shift(60) - 1
            df["rs120"] = c / c.shift(120) - 1

            all_data[ticker] = df
        except Exception:
            continue

    print(f"  Loaded {len(all_data)} tickers "
          f"(KOSPI: {sum(1 for t in all_data if t in kospi)}, "
          f"KOSDAQ: {sum(1 for t in all_data if t in kosdaq)})")
    return all_data, kospi, kosdaq, sector_map


def get_trading_dates(all_data: Dict[str, pd.DataFrame]) -> List[str]:
    """Get union of all trading dates."""
    all_dates = set()
    for df in all_data.values():
        all_dates.update(df["date"].tolist())
    return sorted(all_dates)


def build_universe(all_data: Dict[str, pd.DataFrame],
                   ticker_set: Set[str],
                   date_str: str) -> Dict[str, pd.Series]:
    """Build universe: close>=2000, avg_amt>=20억, data exists on date."""
    universe = {}
    for t in ticker_set:
        if t not in all_data:
            continue
        df = all_data[t]
        idx = df.index[df["date"] == date_str]
        if len(idx) == 0:
            continue
        row = df.iloc[idx[-1]]
        c = float(row["close"])
        avg = float(row.get("avg_amt", 0))
        if c >= MIN_CLOSE and avg >= MIN_AVG_AMT:
            universe[t] = row
    return universe


# ── Scoring Functions (all use T-1 data) ────────────────────────

def score_gen4(all_data: Dict[str, pd.DataFrame],
               universe: Dict[str, pd.Series],
               prev_date: str) -> List[str]:
    """Gen4: LowVol 30%ile → Mom12-1 Top20.
    Uses prev_date (T-1) for all signals."""
    vol_data = {}
    mom_data = {}

    for t, _ in universe.items():
        df = all_data[t]
        idx = df.index[df["date"] == prev_date]
        if len(idx) == 0:
            continue
        row = df.iloc[idx[-1]]
        v = float(row.get("vol_252", np.nan))
        m = float(row.get("mom_12_1", np.nan))
        if not np.isnan(v) and v > 0:
            vol_data[t] = v
        if not np.isnan(m) and m > 0:
            mom_data[t] = m

    if not vol_data:
        return []

    vol_s = pd.Series(vol_data)
    threshold = vol_s.quantile(0.30)
    low_vol = set(vol_s[vol_s <= threshold].index)

    # Mom12-1 on low_vol subset
    mom_s = pd.Series({t: v for t, v in mom_data.items() if t in low_vol})
    if len(mom_s) == 0:
        return []

    return mom_s.sort_values(ascending=False).head(N_STOCKS).index.tolist()


def score_pure_mom(all_data: Dict[str, pd.DataFrame],
                   universe: Dict[str, pd.Series],
                   prev_date: str) -> List[str]:
    """Pure Mom12-1 Top20 (no filter)."""
    mom_data = {}
    for t, _ in universe.items():
        df = all_data[t]
        idx = df.index[df["date"] == prev_date]
        if len(idx) == 0:
            continue
        row = df.iloc[idx[-1]]
        m = float(row.get("mom_12_1", np.nan))
        if not np.isnan(m) and m > 0:
            mom_data[t] = m

    if not mom_data:
        return []
    return pd.Series(mom_data).sort_values(ascending=False).head(N_STOCKS).index.tolist()


def score_hybrid(all_data: Dict[str, pd.DataFrame],
                 universe: Dict[str, pd.Series],
                 prev_date: str) -> List[str]:
    """Hybrid: LowVol 30%ile → RS composite Top20."""
    vol_data = {}
    rs_data = {}

    for t, _ in universe.items():
        df = all_data[t]
        idx = df.index[df["date"] == prev_date]
        if len(idx) == 0:
            continue
        row = df.iloc[idx[-1]]
        v = float(row.get("vol_252", np.nan))
        if not np.isnan(v) and v > 0:
            vol_data[t] = v

        r20 = float(row.get("rs20", np.nan))
        r60 = float(row.get("rs60", np.nan))
        r120 = float(row.get("rs120", np.nan))
        rs_data[t] = {"rs20": r20, "rs60": r60, "rs120": r120}

    if not vol_data:
        return []

    vol_s = pd.Series(vol_data)
    threshold = vol_s.quantile(0.30)
    low_vol = set(vol_s[vol_s <= threshold].index)

    # RS composite on low_vol
    # Rank within low_vol subset
    r20_vals = {t: rs_data[t]["rs20"] for t in low_vol
                if t in rs_data and not np.isnan(rs_data[t]["rs20"])}
    r60_vals = {t: rs_data[t]["rs60"] for t in low_vol
                if t in rs_data and not np.isnan(rs_data[t]["rs60"])}
    r120_vals = {t: rs_data[t]["rs120"] for t in low_vol
                 if t in rs_data and not np.isnan(rs_data[t]["rs120"])}

    r20_rank = pd.Series(r20_vals).rank(pct=True) if r20_vals else pd.Series(dtype=float)
    r60_rank = pd.Series(r60_vals).rank(pct=True) if r60_vals else pd.Series(dtype=float)
    r120_rank = pd.Series(r120_vals).rank(pct=True) if r120_vals else pd.Series(dtype=float)

    scores = {}
    for t in low_vol:
        parts, w = [], 0.0
        if t in r20_rank.index:
            parts.append(0.30 * r20_rank[t]); w += 0.30
        if t in r60_rank.index:
            parts.append(0.50 * r60_rank[t]); w += 0.50
        if t in r120_rank.index:
            parts.append(0.20 * r120_rank[t]); w += 0.20
        if w > 0:
            scores[t] = sum(parts) / w

    if not scores:
        return []
    return pd.Series(scores).sort_values(ascending=False).head(N_STOCKS).index.tolist()


# ── Unified Backtest Engine (per-ticker loop) ───────────────────

def run_backtest(name: str, score_func, all_data: Dict[str, pd.DataFrame],
                 ticker_set: Set[str], dates: List[str]) -> dict:
    """Run backtest with per-ticker loop (no matrix ffill)."""
    cash = float(INITIAL_CASH)
    positions: Dict[str, Pos] = {}
    trades: List[dict] = []
    equity_log: List[dict] = []
    last_rebal_idx = -999

    for di, date_str in enumerate(dates):
        if di < 253:  # warmup for vol_252 + mom_12_1
            continue

        # Universe (per-ticker check, no ffill)
        universe = build_universe(all_data, ticker_set, date_str)

        # Trail stop check
        for t in list(positions.keys()):
            pos = positions[t]
            df = all_data.get(t)
            if df is None:
                continue
            idx = df.index[df["date"] == date_str]
            if len(idx) == 0:
                continue
            row = df.iloc[idx[-1]]
            price = float(row["close"])

            # Update HWM
            if price > pos.hwm:
                pos.hwm = price

            # Trail check
            if pos.hwm > 0:
                dd = (price - pos.hwm) / pos.hwm
                if dd <= -TRAIL_PCT:
                    proceeds = pos.shares * price * (1 - SELL_COST)
                    cash += proceeds
                    cost_basis = pos.shares * pos.entry_price * (1 + BUY_COST)
                    pnl = proceeds - cost_basis
                    trades.append({
                        "ticker": t, "entry_date": pos.entry_date,
                        "exit_date": date_str,
                        "pnl_pct": pnl / cost_basis if cost_basis > 0 else 0,
                        "exit_reason": "TRAIL",
                    })
                    del positions[t]

        # Rebalance
        if di - last_rebal_idx >= REBAL_DAYS:
            prev_date = dates[di - 1] if di > 0 else date_str
            target = score_func(all_data, universe, prev_date)

            if not target:
                # Equity
                eq = cash
                for t, pos in positions.items():
                    df = all_data.get(t)
                    if df is not None:
                        idx = df.index[df["date"] == date_str]
                        if len(idx) > 0:
                            eq += float(df.iloc[idx[-1]]["close"]) * pos.shares
                            continue
                    eq += pos.entry_price * pos.shares
                equity_log.append({"date": date_str, "equity": eq,
                                   "n_pos": len(positions)})
                continue

            target_set = set(target[:N_STOCKS])
            last_rebal_idx = di

            # Sell positions not in target
            for t in list(positions.keys()):
                if t not in target_set:
                    pos = positions[t]
                    df = all_data.get(t)
                    price = pos.entry_price  # fallback
                    if df is not None:
                        idx = df.index[df["date"] == date_str]
                        if len(idx) > 0:
                            price = float(df.iloc[idx[-1]]["open"])
                    proceeds = pos.shares * price * (1 - SELL_COST)
                    cash += proceeds
                    cost_basis = pos.shares * pos.entry_price * (1 + BUY_COST)
                    pnl = proceeds - cost_basis
                    trades.append({
                        "ticker": t, "entry_date": pos.entry_date,
                        "exit_date": date_str,
                        "pnl_pct": pnl / cost_basis if cost_basis > 0 else 0,
                        "exit_reason": "REBAL",
                    })
                    del positions[t]

            # Equity for sizing
            eq = cash
            for t, pos in positions.items():
                df = all_data.get(t)
                if df is not None:
                    idx = df.index[df["date"] == date_str]
                    if len(idx) > 0:
                        eq += float(df.iloc[idx[-1]]["close"]) * pos.shares
                        continue
                eq += pos.entry_price * pos.shares

            # Buy new
            n_to_buy = N_STOCKS - len(positions)
            if n_to_buy > 0 and eq > 0:
                per_stock = eq * 0.95 / N_STOCKS
                for t in target:
                    if t in positions or n_to_buy <= 0:
                        continue
                    df = all_data.get(t)
                    if df is None:
                        continue
                    idx = df.index[df["date"] == date_str]
                    if len(idx) == 0:
                        continue
                    row = df.iloc[idx[-1]]
                    price = float(row["open"])
                    if price <= 0:
                        continue
                    budget = min(per_stock, cash) / (1 + BUY_COST)
                    shares = int(budget / price)
                    if shares <= 0:
                        continue
                    cost = shares * price * (1 + BUY_COST)
                    if cost > cash:
                        continue
                    cash -= cost
                    positions[t] = Pos(
                        ticker=t, shares=shares, entry_price=price,
                        entry_date=date_str, hwm=price)
                    n_to_buy -= 1

        # Equity log
        eq = cash
        for t, pos in positions.items():
            df = all_data.get(t)
            if df is not None:
                idx = df.index[df["date"] == date_str]
                if len(idx) > 0:
                    eq += float(df.iloc[idx[-1]]["close"]) * pos.shares
                    continue
            eq += pos.entry_price * pos.shares
        equity_log.append({"date": date_str, "equity": eq,
                           "n_pos": len(positions)})

        # Progress
        if di % 300 == 0:
            print(f"    {date_str}: eq={eq:,.0f} pos={len(positions)} "
                  f"trades={len(trades)}")

    # Stats
    eq_df = pd.DataFrame(equity_log)
    if len(eq_df) == 0:
        return {"name": name, "error": "no data"}

    final = eq_df["equity"].iloc[-1]
    n_years = len(eq_df) / 252
    cagr = (final / INITIAL_CASH) ** (1 / n_years) - 1 if n_years > 0 else 0
    peak = eq_df["equity"].cummax()
    dd = (eq_df["equity"] - peak) / peak
    mdd = dd.min()
    daily_ret = eq_df["equity"].pct_change().dropna()
    sharpe = daily_ret.mean() / daily_ret.std() * np.sqrt(252) if daily_ret.std() > 0 else 0

    trade_df = pd.DataFrame(trades) if trades else pd.DataFrame()
    win_rate = (trade_df["pnl_pct"] > 0).mean() if len(trade_df) > 0 else 0
    exit_dist = trade_df["exit_reason"].value_counts().to_dict() if len(trade_df) > 0 else {}

    return {
        "name": name, "cagr": cagr, "mdd": mdd, "sharpe": sharpe,
        "trades": len(trades), "win_rate": win_rate,
        "final_equity": final, "exit_dist": exit_dist,
        "equity_df": eq_df, "trade_df": trade_df,
    }


# ── Main ────────────────────────────────────────────────────────

def main():
    all_data, kospi_set, kosdaq_set, sector_map = load_data()
    dates = get_trading_dates(all_data)
    print(f"  Trading dates: {len(dates)} ({dates[0]} ~ {dates[-1]})")

    strategies = [
        ("Gen4_Core", score_gen4),
        ("Pure_Mom12-1", score_pure_mom),
        ("Hybrid_LowVol+RS", score_hybrid),
    ]

    universes = [
        ("KOSPI", kospi_set & set(all_data.keys())),
        ("ALL", (kospi_set | kosdaq_set) & set(all_data.keys())),
    ]

    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    all_results = []

    for univ_name, ticker_set in universes:
        print(f"\n{'='*60}")
        print(f"  Universe: {univ_name} ({len(ticker_set)} tickers)")
        print(f"{'='*60}")

        for strat_name, score_func in strategies:
            label = f"{strat_name}_{univ_name}"
            print(f"\n  [{label}]")
            t0 = time.time()
            result = run_backtest(label, score_func, all_data, ticker_set, dates)
            elapsed = time.time() - t0

            if "error" in result:
                print(f"    ERROR: {result['error']}")
                continue

            print(f"    Done in {elapsed:.0f}s -CAGR={result['cagr']:.2%}  "
                  f"MDD={result['mdd']:.2%}  Sharpe={result['sharpe']:.3f}  "
                  f"Trades={result['trades']}")

            result["equity_df"].to_csv(
                RESULT_DIR / f"equity_{label}.csv", index=False)
            if len(result["trade_df"]) > 0:
                result["trade_df"].to_csv(
                    RESULT_DIR / f"trades_{label}.csv", index=False)

            all_results.append({
                "strategy": strat_name, "universe": univ_name,
                "cagr": f"{result['cagr']:.2%}",
                "mdd": f"{result['mdd']:.2%}",
                "sharpe": f"{result['sharpe']:.3f}",
                "trades": result["trades"],
                "win_rate": f"{result['win_rate']:.1%}",
                "exit_dist": result["exit_dist"],
            })

    # Summary
    print(f"\n{'='*70}")
    print(f"  FAIR COMPARISON SUMMARY (per-ticker loop, no ffill)")
    print(f"{'='*70}")
    for r in all_results:
        print(f"  {r['strategy']:20s} {r['universe']:5s}  "
              f"CAGR={r['cagr']:>8s}  MDD={r['mdd']:>8s}  "
              f"Sharpe={r['sharpe']:>6s}  Trades={r['trades']:>4d}  "
              f"WR={r['win_rate']:>5s}")

    with open(RESULT_DIR / "summary.json", "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)

    print(f"\nResults saved to: {RESULT_DIR}")


if __name__ == "__main__":
    main()
