"""
compare_strategies.py — 동일 조건 전략 비교 백테스트
====================================================
4개 전략 × 2개 유니버스 = 8개 결과 비교

전략:
  1. Gen4 Core:  LowVol 30%ile → Mom12-1 Top20, Trail -12%, 21일 리밸
  2. Gen3 v7:    RS composite breakout, 6중 게이트, LTR v2
  3. Pure Mom:   Mom12-1 Top20 (필터 없음, 벤치마크)
  4. Hybrid:     LowVol 30%ile → RS composite Top20

통일 조건:
  - 데이터: backtest/data_full/ohlcv/ (2019-01-02 ~ 2026-03-20)
  - 비용: BUY 0.115%, SELL 0.295%
  - 초기자금: 1억원
  - 리밸: 21거래일마다
  - 포지션: 20종목 균등배분
  - 청산: Trailing Stop -12% (close-based)
  - look-ahead 방지: T-1 시그널, T+0 open 진입

Usage:
    cd C:\\Q-TRON-32_ARCHIVE
    .venv\\Scripts\\python.exe backtest\\compare_strategies.py
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

# ── Paths ────────────────────────────────────────────────────────

BASE = Path(__file__).resolve().parent.parent
DATA = BASE / "backtest" / "data_full"
OHLCV_DIR = DATA / "ohlcv"
INDEX_FILE = DATA / "index" / "KOSPI.csv"
SECTOR_FILE = DATA / "sector_map.json"
RESULT_DIR = BASE / "backtest" / "results" / "strategy_compare"

# ── Unified Parameters ──────────────────────────────────────────

INITIAL_CASH = 100_000_000
BUY_COST = 0.00115
SELL_COST = 0.00295
N_STOCKS = 20
REBAL_DAYS = 21
TRAIL_PCT = -0.12
VOL_WINDOW = 252
MOM_WINDOW = 252
MOM_SKIP = 21  # skip recent 1 month


# ── Data Loading ────────────────────────────────────────────────

def load_all_data():
    """Load OHLCV, KOSPI index, sector map."""
    print("Loading data...")
    with open(SECTOR_FILE, encoding="utf-8") as f:
        sector_map = json.load(f)

    kospi_tickers = {t for t, v in sector_map.items() if v.get("market") == "KOSPI"}
    kosdaq_tickers = {t for t, v in sector_map.items() if v.get("market") == "KOSDAQ"}

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
            all_data[ticker] = df
        except Exception:
            continue

    # KOSPI index
    kospi = pd.read_csv(INDEX_FILE, encoding="utf-8-sig")
    col_map = {}
    for c in kospi.columns:
        cl = c.strip().lower()
        if cl in ("date", "index"):
            col_map[c] = "date"
        elif cl == "close":
            col_map[c] = "close"
    kospi = kospi.rename(columns=col_map)
    if "date" not in kospi.columns:
        kospi = kospi.reset_index()
        kospi.columns = ["date"] + list(kospi.columns[1:])
    kospi["date"] = pd.to_datetime(kospi["date"]).dt.strftime("%Y-%m-%d")
    kospi = kospi.sort_values("date").reset_index(drop=True)

    print(f"  Total tickers: {len(all_data)} "
          f"(KOSPI: {sum(1 for t in all_data if t in kospi_tickers)}, "
          f"KOSDAQ: {sum(1 for t in all_data if t in kosdaq_tickers)})")

    return all_data, kospi_tickers, kosdaq_tickers, sector_map


# ── Build Daily Matrix ──────────────────────────────────────────

def build_matrix(all_data: Dict[str, pd.DataFrame],
                 tickers: set) -> Tuple[pd.DataFrame, List[str]]:
    """Build close price matrix for selected tickers."""
    frames = {}
    for t in tickers:
        if t in all_data:
            df = all_data[t][["date", "close"]].copy()
            df = df.set_index("date")["close"]
            frames[t] = df
    matrix = pd.DataFrame(frames)
    matrix.index = pd.to_datetime(matrix.index)
    matrix = matrix.sort_index()

    # Also need open prices for execution
    open_frames = {}
    for t in tickers:
        if t in all_data:
            df = all_data[t][["date", "open"]].copy()
            df = df.set_index("date")["open"]
            open_frames[t] = df
    open_matrix = pd.DataFrame(open_frames)
    open_matrix.index = pd.to_datetime(open_matrix.index)
    open_matrix = open_matrix.sort_index()

    return matrix, open_matrix


# ── Scoring Functions ───────────────────────────────────────────

def score_gen4(close_matrix: pd.DataFrame, date_idx: int) -> pd.Series:
    """Gen4: LowVol 30%ile → Mom12-1 Top20."""
    if date_idx < VOL_WINDOW:
        return pd.Series(dtype=float)

    closes = close_matrix.iloc[:date_idx]

    # Volatility (252-day std of daily returns)
    ret = closes.pct_change()
    vol = ret.iloc[-VOL_WINDOW:].std()
    vol = vol.dropna()
    if len(vol) == 0:
        return pd.Series(dtype=float)

    # LowVol filter: bottom 30%ile
    vol_threshold = vol.quantile(0.30)
    low_vol = vol[vol <= vol_threshold].index.tolist()
    if not low_vol:
        return pd.Series(dtype=float)

    # Mom12-1
    if date_idx < MOM_WINDOW + MOM_SKIP:
        return pd.Series(dtype=float)
    p_now = closes.iloc[-(MOM_SKIP + 1)]
    p_past = closes.iloc[-(MOM_WINDOW + MOM_SKIP)]
    mom = (p_now / p_past).dropna()
    mom = mom[mom.index.isin(low_vol)]
    mom = mom.sort_values(ascending=False)

    return mom.head(N_STOCKS)


def score_pure_mom(close_matrix: pd.DataFrame, date_idx: int) -> pd.Series:
    """Pure Mom12-1 Top20 (no filter)."""
    if date_idx < MOM_WINDOW + MOM_SKIP:
        return pd.Series(dtype=float)

    closes = close_matrix.iloc[:date_idx]
    p_now = closes.iloc[-(MOM_SKIP + 1)]
    p_past = closes.iloc[-(MOM_WINDOW + MOM_SKIP)]
    mom = (p_now / p_past).dropna()

    # Minimum price filter only
    last_close = closes.iloc[-1]
    valid = last_close[last_close >= 2000].index
    mom = mom[mom.index.isin(valid)]
    mom = mom.sort_values(ascending=False)

    return mom.head(N_STOCKS)


def score_hybrid(close_matrix: pd.DataFrame, date_idx: int) -> pd.Series:
    """Hybrid: LowVol 30%ile → RS composite Top20."""
    if date_idx < VOL_WINDOW:
        return pd.Series(dtype=float)

    closes = close_matrix.iloc[:date_idx]

    # LowVol filter (same as Gen4)
    ret = closes.pct_change()
    vol = ret.iloc[-VOL_WINDOW:].std()
    vol = vol.dropna()
    if len(vol) == 0:
        return pd.Series(dtype=float)
    vol_threshold = vol.quantile(0.30)
    low_vol = vol[vol <= vol_threshold].index.tolist()
    if not low_vol:
        return pd.Series(dtype=float)

    # RS composite (Gen3 style) on LowVol universe
    last = closes.iloc[-1]
    rs20 = (last / closes.iloc[-21] - 1) if date_idx >= 21 else pd.Series(dtype=float)
    rs60 = (last / closes.iloc[-61] - 1) if date_idx >= 61 else pd.Series(dtype=float)
    rs120 = (last / closes.iloc[-121] - 1) if date_idx >= 121 else pd.Series(dtype=float)

    # Filter to low_vol
    rs20 = rs20[rs20.index.isin(low_vol)].dropna()
    rs60 = rs60[rs60.index.isin(low_vol)].dropna()
    rs120 = rs120[rs120.index.isin(low_vol)].dropna()

    # Rank and composite
    all_tickers = list(set(rs20.index) | set(rs60.index) | set(rs120.index))
    scores = {}
    r20 = rs20.rank(pct=True) if len(rs20) > 0 else pd.Series(dtype=float)
    r60 = rs60.rank(pct=True) if len(rs60) > 0 else pd.Series(dtype=float)
    r120 = rs120.rank(pct=True) if len(rs120) > 0 else pd.Series(dtype=float)

    for t in all_tickers:
        parts, w = [], 0
        if t in r20.index:
            parts.append(0.30 * r20[t]); w += 0.30
        if t in r60.index:
            parts.append(0.50 * r60[t]); w += 0.50
        if t in r120.index:
            parts.append(0.20 * r120[t]); w += 0.20
        if w > 0:
            scores[t] = sum(parts) / w

    rs = pd.Series(scores).sort_values(ascending=False)
    return rs.head(N_STOCKS)


# ── Unified Backtest Engine ─────────────────────────────────────

def run_strategy(name: str, score_func, close_matrix: pd.DataFrame,
                 open_matrix: pd.DataFrame) -> dict:
    """Run a single strategy with unified parameters."""
    dates = close_matrix.index
    n = len(dates)

    cash = float(INITIAL_CASH)
    holdings: Dict[str, dict] = {}  # {ticker: {shares, entry_price, hwm}}
    trades = []
    equity_series = []
    last_rebal = -999

    for i in range(VOL_WINDOW + MOM_SKIP, n):
        dt = dates[i]
        dt_str = dt.strftime("%Y-%m-%d")

        # Current prices
        cur_close = close_matrix.iloc[i]
        cur_open = open_matrix.iloc[i]

        # Update HWM and check trail stop
        for t in list(holdings.keys()):
            h = holdings[t]
            price = cur_close.get(t, np.nan)
            if np.isnan(price):
                continue
            if price > h["hwm"]:
                h["hwm"] = price
            # Trail stop check (close-based)
            if h["hwm"] > 0:
                dd = (price - h["hwm"]) / h["hwm"]
                if dd <= TRAIL_PCT:
                    proceeds = h["shares"] * price * (1 - SELL_COST)
                    cash += proceeds
                    cost_basis = h["shares"] * h["entry_price"] * (1 + BUY_COST)
                    pnl = proceeds - cost_basis
                    trades.append({
                        "ticker": t, "entry_date": h["entry_date"],
                        "exit_date": dt_str, "pnl": pnl,
                        "pnl_pct": pnl / cost_basis if cost_basis > 0 else 0,
                        "exit_reason": "TRAIL",
                    })
                    del holdings[t]

        # Rebalance check
        if i - last_rebal >= REBAL_DAYS:
            # Score using T-1 data (look-ahead prevention)
            target = score_func(close_matrix, i)  # uses iloc[:i] internally
            if len(target) == 0:
                equity = cash + sum(
                    cur_close.get(t, h["entry_price"]) * h["shares"]
                    for t, h in holdings.items())
                equity_series.append({"date": dt_str, "equity": equity})
                continue

            target_tickers = set(target.index[:N_STOCKS])
            last_rebal = i

            # Sell positions not in target
            for t in list(holdings.keys()):
                if t not in target_tickers:
                    h = holdings[t]
                    price = cur_open.get(t, np.nan)
                    if np.isnan(price) or price <= 0:
                        price = cur_close.get(t, h["entry_price"])
                    proceeds = h["shares"] * price * (1 - SELL_COST)
                    cash += proceeds
                    cost_basis = h["shares"] * h["entry_price"] * (1 + BUY_COST)
                    pnl = proceeds - cost_basis
                    trades.append({
                        "ticker": t, "entry_date": h["entry_date"],
                        "exit_date": dt_str, "pnl": pnl,
                        "pnl_pct": pnl / cost_basis if cost_basis > 0 else 0,
                        "exit_reason": "REBAL",
                    })
                    del holdings[t]

            # Buy new positions
            equity = cash + sum(
                cur_close.get(t, h["entry_price"]) * h["shares"]
                for t, h in holdings.items())
            n_to_buy = N_STOCKS - len(holdings)
            if n_to_buy > 0 and equity > 0:
                per_stock = (equity * 0.95 / N_STOCKS)
                for t in target_tickers:
                    if t in holdings:
                        continue
                    if n_to_buy <= 0:
                        break
                    price = cur_open.get(t, np.nan)
                    if np.isnan(price) or price <= 0:
                        continue
                    budget = min(per_stock, cash) / (1 + BUY_COST)
                    shares = int(budget / price)
                    if shares <= 0:
                        continue
                    cost = shares * price * (1 + BUY_COST)
                    if cost > cash:
                        continue
                    cash -= cost
                    holdings[t] = {
                        "shares": shares, "entry_price": price,
                        "entry_date": dt_str, "hwm": price,
                    }
                    n_to_buy -= 1

        # Equity
        equity = cash + sum(
            cur_close.get(t, h["entry_price"]) * h["shares"]
            for t, h in holdings.items())
        equity_series.append({"date": dt_str, "equity": equity})

    # Compute stats
    eq_df = pd.DataFrame(equity_series)
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
    calmar = abs(cagr / mdd) if mdd != 0 else 0

    trade_df = pd.DataFrame(trades)
    win_rate = (trade_df["pnl"] > 0).mean() if len(trade_df) > 0 else 0

    return {
        "name": name,
        "period": f"{eq_df['date'].iloc[0]} ~ {eq_df['date'].iloc[-1]}",
        "cagr": cagr,
        "mdd": mdd,
        "sharpe": sharpe,
        "calmar": calmar,
        "total_trades": len(trades),
        "win_rate": win_rate,
        "final_equity": final,
        "equity_df": eq_df,
        "trade_df": trade_df,
    }


# ── Main ────────────────────────────────────────────────────────

def main():
    all_data, kospi_set, kosdaq_set, sector_map = load_all_data()

    strategies = {
        "Gen4_Core": score_gen4,
        "Pure_Mom12-1": score_pure_mom,
        "Hybrid_LowVol+RS": score_hybrid,
    }

    universes = {
        "KOSPI": kospi_set,
        "ALL": kospi_set | kosdaq_set,
    }

    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    all_results = []

    for univ_name, ticker_set in universes.items():
        valid_tickers = ticker_set & set(all_data.keys())
        print(f"\n{'='*60}")
        print(f"  Universe: {univ_name} ({len(valid_tickers)} tickers)")
        print(f"{'='*60}")

        close_matrix, open_matrix = build_matrix(all_data, valid_tickers)
        print(f"  Matrix: {close_matrix.shape[0]} days × {close_matrix.shape[1]} tickers")

        for strat_name, score_func in strategies.items():
            label = f"{strat_name}_{univ_name}"
            print(f"\n  Running {label}...", end=" ", flush=True)
            t0 = time.time()
            result = run_strategy(label, score_func, close_matrix, open_matrix)
            elapsed = time.time() - t0
            print(f"{elapsed:.1f}s")

            if "error" in result:
                print(f"    ERROR: {result['error']}")
                continue

            print(f"    CAGR={result['cagr']:.2%}  MDD={result['mdd']:.2%}  "
                  f"Sharpe={result['sharpe']:.3f}  Trades={result['total_trades']}")

            # Save equity
            result["equity_df"].to_csv(
                RESULT_DIR / f"equity_{label}.csv", index=False)
            if len(result["trade_df"]) > 0:
                result["trade_df"].to_csv(
                    RESULT_DIR / f"trades_{label}.csv", index=False)

            all_results.append({
                "strategy": strat_name,
                "universe": univ_name,
                "cagr": f"{result['cagr']:.2%}",
                "mdd": f"{result['mdd']:.2%}",
                "sharpe": f"{result['sharpe']:.3f}",
                "calmar": f"{result['calmar']:.3f}",
                "trades": result["total_trades"],
                "win_rate": f"{result['win_rate']:.1%}",
                "final_equity": f"{result['final_equity']:,.0f}",
            })

    # Summary table
    print(f"\n{'='*80}")
    print(f"  COMPARISON SUMMARY")
    print(f"{'='*80}")

    summary_df = pd.DataFrame(all_results)
    print(summary_df.to_string(index=False))

    summary_df.to_csv(RESULT_DIR / "comparison_summary.csv", index=False)
    with open(RESULT_DIR / "comparison_summary.json", "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)

    print(f"\nResults saved to: {RESULT_DIR}")


if __name__ == "__main__":
    main()
