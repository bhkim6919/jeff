"""
backtest_gen3.py  v2
====================
Gen3 본전략 (RS Composite + ATR SL + Regime) 3년 백테스트.

v2 변경:
  - 리밸런싱: 5일→1일 (매일 시그널 생성, 실운영과 동일)
  - 체결강도 프록시: vol_ratio > 1.3 AND 시가>전일종가 → 진입 허용
  - 슬리피지: 0.3%→0.5% (보수적)
  - 시그널 캐싱: 동일 날짜 중복 계산 방지

Usage:
  python backtest_gen3.py
  python backtest_gen3.py --start 20230601 --end 20260320
"""
from __future__ import annotations

import argparse
import json
import sys
import warnings
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

BASE_DIR = Path(__file__).resolve().parent
OHLCV_DIR = BASE_DIR / "data" / "ohlcv_kospi_daily"
INDEX_FILE = BASE_DIR / "data" / "kospi_index_daily_5y.csv"
SECTOR_MAP_FILE = BASE_DIR / "data" / "sector_map.json"
REPORT_DIR = BASE_DIR / "data" / "top20" / "reports"

# ── Cost (v2: slippage 0.3%→0.5%) ─────────────────────────────────────────────
BUY_COST = 0.0015 + 0.005   # fee 0.15% + slippage 0.5% = 0.65%
SELL_COST = 0.0015 + 0.005 + 0.0018  # fee + slippage + tax = 0.83%

# ── Tick Strength Proxy ───────────────────────────────────────────────────────
TICK_VOL_RATIO_MIN = 1.3    # 당일 거래량/20일 평균 >= 1.3 (매수세 확인)
TICK_PRICE_UP = True        # 당일 시가 > 전일 종가 (갭업 = 수급 양호)


# ── Position ──────────────────────────────────────────────────────────────────
@dataclass
class Position:
    ticker: str
    entry_price: float
    entry_date: str
    qty: int
    sl: float
    tp: float
    atr: float
    stage: str  # A or B
    sector: str
    rs_composite: float
    trail_active: bool = False
    trail_sl: float = 0.0
    high_watermark: float = 0.0
    hold_days: int = 0


# ── Pre-load all OHLCV ───────────────────────────────────────────────────────
def load_all_ohlcv() -> Dict[str, pd.DataFrame]:
    """Load all OHLCV CSVs into memory."""
    data = {}
    files = sorted(OHLCV_DIR.glob("*.csv"))
    print(f"  Loading {len(files)} OHLCV files...")
    for f in files:
        ticker = f.stem
        try:
            df = pd.read_csv(f, parse_dates=["date"])
            df = df.sort_values("date").reset_index(drop=True)
            for c in ["open", "high", "low", "close", "volume"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
            if len(df) >= 130:
                data[ticker] = df
        except Exception:
            pass
    print(f"  Loaded: {len(data)} stocks")
    return data


def load_index() -> pd.DataFrame:
    df = pd.read_csv(INDEX_FILE, parse_dates=["date"])
    df = df.sort_values("date").reset_index(drop=True)
    return df


def load_sector_map() -> Dict[str, str]:
    if SECTOR_MAP_FILE.exists():
        with open(SECTOR_MAP_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


# ── ATR ───────────────────────────────────────────────────────────────────────
def wilder_atr(high: np.ndarray, low: np.ndarray, close: np.ndarray,
               period: int = 20) -> float:
    if len(close) < period + 1:
        return 0.0
    tr = np.maximum(
        high[1:] - low[1:],
        np.maximum(np.abs(high[1:] - close[:-1]),
                   np.abs(low[1:] - close[:-1]))
    )
    if len(tr) < period:
        return 0.0
    atr = float(tr[:period].mean())
    k = 1.0 / period
    for v in tr[period:]:
        atr = atr * (1 - k) + v * k
    return atr


# ── Signal Generation (replicates gen3_signal_builder) ────────────────────────
def generate_signals(all_data: Dict[str, pd.DataFrame],
                     idx_df: pd.DataFrame,
                     as_of_idx: int,
                     sector_map: Dict[str, str]) -> Tuple[str, float, pd.DataFrame]:
    """
    Generate Gen3 signals as of a specific date index in the index dataframe.
    Returns: (regime, breadth, signals_df)
    """
    if as_of_idx < 200:
        return "BULL", 0.5, pd.DataFrame()

    as_of_date = idx_df["date"].iloc[as_of_idx]
    idx_close = idx_df["close"].astype(float)

    # Regime: MA200
    ma200 = float(idx_close.iloc[as_of_idx - 199:as_of_idx + 1].mean())
    last_idx = float(idx_close.iloc[as_of_idx])
    regime_base = "BULL" if last_idx > ma200 else "BEAR"

    # Compute features for all stocks
    features = []
    breadth_above = 0
    breadth_total = 0

    for ticker, df in all_data.items():
        # Get data up to as_of_date (no look-ahead)
        # Optimized: use searchsorted instead of boolean mask
        dates_arr = df["date"].values
        as_of_np = np.datetime64(as_of_date)
        cut_idx = int(np.searchsorted(dates_arr, as_of_np, side="right"))
        if cut_idx < 130:
            continue
        sub = df.iloc[:cut_idx]

        close = sub["close"].values.astype(float)
        high = sub["high"].values.astype(float)
        low = sub["low"].values.astype(float)
        volume = sub["volume"].values.astype(float)
        last_close = close[-1]

        if last_close <= 0:
            continue

        # Universe filter
        avg_amt = float((close[-20:] * volume[-20:]).mean())
        if last_close < 2000 or avg_amt < 2_000_000_000:
            continue

        # RS raw returns
        def _ret(n):
            if len(close) <= n:
                return np.nan
            prev = close[-(n + 1)]
            return (last_close / prev - 1.0) if prev > 0 else np.nan

        rs20 = _ret(20)
        rs60 = _ret(60)
        rs120 = _ret(120)

        # MA20 above
        ma20 = float(close[-20:].mean()) if len(close) >= 20 else np.nan
        above_ma20 = int(last_close > ma20) if not np.isnan(ma20) else 0

        # Breadth
        breadth_total += 1
        if above_ma20:
            breadth_above += 1

        # 52w high
        n252 = min(252, len(high))
        high_252 = float(high[-n252:].max())
        is_52w_high = int(last_close >= high_252 * 0.95)

        # ATR
        n_atr = min(60, len(sub))
        atr_val = wilder_atr(high[-n_atr:], low[-n_atr:], close[-n_atr:], 20)

        # Breakout: 20-day high
        if len(high) >= 21:
            high_20 = float(high[-21:-1].max())
            breakout = int(last_close >= high_20)
        else:
            breakout = 0

        # Gap filter
        if len(close) >= 2 and len(sub) >= 2:
            prev_c = close[-2]
            last_open = float(sub["open"].values[-1])
            gap_pct = (last_open / prev_c - 1.0) if prev_c > 0 else 0.0
            avg_vol20 = float(volume[-21:-1].mean()) if len(volume) >= 21 else 1.0
            vol_ratio = volume[-1] / avg_vol20 if avg_vol20 > 0 else 1.0
            gap_blocked = int(gap_pct > 0.08 and vol_ratio < 1.3)
        else:
            gap_blocked = 0

        features.append({
            "ticker": ticker,
            "last_close": last_close,
            "rs20_raw": rs20,
            "rs60_raw": rs60,
            "rs120_raw": rs120,
            "above_ma20": above_ma20,
            "is_52w_high": is_52w_high,
            "atr20": atr_val,
            "breakout": breakout,
            "gap_blocked": gap_blocked,
            "high_252": high_252,
        })

    if not features:
        return regime_base, 0.5, pd.DataFrame()

    # Breadth
    breadth = breadth_above / breadth_total if breadth_total > 0 else 0.5

    # Adaptive breadth threshold (simplified: use 0.35 fixed for backtest consistency)
    adaptive_thresh = 0.35
    is_bull_eff = (regime_base == "BULL") and (breadth >= adaptive_thresh)
    regime = "BULL" if is_bull_eff else "BEAR"

    # RS rank
    df_feat = pd.DataFrame(features)
    for col, out in [("rs20_raw", "rs20_rank"), ("rs60_raw", "rs60_rank"),
                     ("rs120_raw", "rs120_rank")]:
        valid = df_feat[col].notna()
        df_feat.loc[valid, out] = df_feat.loc[valid, col].rank(pct=True)
        df_feat.loc[~valid, out] = np.nan

    df_feat["rs_composite"] = (
        df_feat["rs20_rank"].fillna(0) * 0.30 +
        df_feat["rs60_rank"].fillna(0) * 0.50 +
        df_feat["rs120_rank"].fillna(0) * 0.20
    )
    nan_mask = df_feat[["rs20_rank", "rs60_rank", "rs120_rank"]].isna().any(axis=1)
    df_feat.loc[nan_mask, "rs_composite"] = np.nan

    # ATR rank
    valid_atr = df_feat["atr20"] > 0
    df_feat.loc[valid_atr, "atr_rank"] = df_feat.loc[valid_atr, "atr20"].rank(pct=True)
    df_feat.loc[~valid_atr, "atr_rank"] = 1.0

    # Signal entry
    df_feat = df_feat.dropna(subset=["rs_composite"])
    df_feat["signal_entry"] = (
        (df_feat["breakout"] == 1) &
        (df_feat["rs_composite"] >= 0.80) &
        (df_feat["gap_blocked"] == 0)
    ).astype(int)

    # pb_score
    def _pb(row):
        if row["high_252"] <= 0:
            return 0.0
        ratio = row["last_close"] / row["high_252"]
        return 5.0 if 0.93 <= ratio <= 0.97 else 0.0

    df_feat["pb_score"] = df_feat.apply(_pb, axis=1)
    df_feat["score"] = df_feat["rs_composite"] * 100 + df_feat["pb_score"]

    # Sector
    df_feat["sector"] = df_feat["ticker"].map(lambda t: sector_map.get(t, "ETC"))

    # Stage
    if regime == "BULL":
        df_feat["stage"] = df_feat.apply(
            lambda r: "A" if (
                (r["is_52w_high"] == 1 and r["rs_composite"] >= 0.80) or
                (r["breakout"] == 1 and r["rs_composite"] >= 0.92)
            ) else "B", axis=1)
    else:
        df_feat["stage"] = "B"

    # Filter: entry=1, sort by score, top 50
    candidates = df_feat[df_feat["signal_entry"] == 1].sort_values("score", ascending=False)
    signals = candidates.head(50).copy()

    return regime, breadth, signals


# ── Daily Exit Check ──────────────────────────────────────────────────────────
def check_exits(positions: List[Position], all_data: Dict[str, pd.DataFrame],
                trade_date: pd.Timestamp, regime: str,
                idx_df: pd.DataFrame, idx_i: int) -> List[Tuple[Position, str, float]]:
    """
    Check all exit conditions. Returns list of (position, reason, exit_price).
    """
    exits = []

    # Index return for RAL
    idx_close = idx_df["close"].astype(float)
    idx_ret = 0.0
    if idx_i >= 1:
        prev = float(idx_close.iloc[idx_i - 1])
        curr = float(idx_close.iloc[idx_i])
        idx_ret = (curr / prev - 1.0) if prev > 0 else 0.0

    for pos in positions:
        pos.hold_days += 1

        if pos.ticker not in all_data:
            continue

        df = all_data[pos.ticker]
        mask = df["date"] <= trade_date
        sub = df[mask]
        if sub.empty:
            continue

        today_row = sub.iloc[-1]
        today_high = float(today_row["high"])
        today_low = float(today_row["low"])
        today_close = float(today_row["close"])
        today_open = float(today_row["open"])

        if today_close <= 0:
            continue

        exit_reason = None
        exit_price = today_close

        # 1. MAX_LOSS_CAP -8% (absolute floor)
        loss_pct = (today_low / pos.entry_price) - 1.0
        if loss_pct <= -0.08:
            exit_reason = "MAX_LOSS_CAP"
            exit_price = pos.entry_price * 0.92  # -8% price

        # 2. ATR SL hit (check low)
        if not exit_reason and pos.sl > 0 and today_low <= pos.sl:
            exit_reason = "ATR_SL"
            exit_price = pos.sl

        # 3. GAP_DOWN -5%
        if not exit_reason and len(sub) >= 2:
            prev_close = float(sub["close"].iloc[-2])
            if prev_close > 0:
                gap = (today_open / prev_close) - 1.0
                if gap <= -0.05:
                    exit_reason = "GAP_DOWN"
                    exit_price = today_open

        # 4. Trailing Stop
        if not exit_reason and pos.trail_active and pos.trail_sl > 0:
            if today_low <= pos.trail_sl:
                exit_reason = "TRAIL_SL"
                exit_price = pos.trail_sl

        # 5. TP hit (check high)
        if not exit_reason and pos.tp > 0 and today_high >= pos.tp:
            exit_reason = "TP"
            exit_price = pos.tp

        # 6. RAL CRASH (index -2% AND RS < 0.45)
        if not exit_reason and idx_ret <= -0.02:
            # Simplified: close positions with poor recent performance
            ret_since_entry = (today_close / pos.entry_price) - 1.0
            if ret_since_entry < -0.03:
                exit_reason = "RAL_CRASH"
                exit_price = today_close

        # 7. MAX_HOLD 60 days
        if not exit_reason and pos.hold_days >= 60:
            exit_reason = "MAX_HOLD"
            exit_price = today_close

        # Update trailing stop
        if not exit_reason:
            if today_high > pos.high_watermark:
                pos.high_watermark = today_high
            unrealized = (pos.high_watermark / pos.entry_price) - 1.0
            if unrealized >= 0.05 and not pos.trail_active:
                pos.trail_active = True
            if pos.trail_active and pos.atr > 0:
                new_trail = pos.high_watermark - 2.0 * pos.atr
                min_lock = pos.entry_price * 1.02
                new_trail = max(new_trail, min_lock)
                if new_trail > pos.trail_sl:
                    pos.trail_sl = new_trail

        # Time-decay SL tightening (day 20~50)
        if not exit_reason and pos.hold_days >= 20 and pos.sl > 0:
            decay_progress = min(1.0, (pos.hold_days - 20) / 30.0)
            # ATR mult decays from original to 1.0
            if regime == "BULL":
                orig_mult = 2.5
            else:
                orig_mult = 1.0
            decayed_mult = orig_mult - (orig_mult - 1.0) * decay_progress
            new_sl = pos.entry_price - pos.atr * decayed_mult
            if new_sl > pos.sl:
                pos.sl = new_sl

        if exit_reason:
            # Clamp exit_price to reasonable range
            exit_price = max(exit_price, today_low)
            exit_price = min(exit_price, today_high)
            exits.append((pos, exit_reason, exit_price))

    return exits


# ── Metrics ───────────────────────────────────────────────────────────────────
def calc_metrics(equity_curve: pd.Series, trades: List[Dict],
                 idx_returns: pd.Series) -> Dict:
    """Comprehensive metrics calculation."""
    if len(equity_curve) < 2:
        return {}

    returns = equity_curve.pct_change().dropna()
    total_ret = (equity_curve.iloc[-1] / equity_curve.iloc[0]) - 1.0

    # CAGR
    n_years = len(equity_curve) / 252
    if n_years > 0 and equity_curve.iloc[-1] > 0 and equity_curve.iloc[0] > 0:
        cagr = (equity_curve.iloc[-1] / equity_curve.iloc[0]) ** (1 / n_years) - 1
    else:
        cagr = 0

    # MDD
    peak = equity_curve.expanding().max()
    dd = (equity_curve - peak) / peak
    mdd = float(dd.min())

    # DD duration
    in_dd = dd < 0
    dd_periods = []
    start = None
    for i, v in in_dd.items():
        if v and start is None:
            start = i
        elif not v and start is not None:
            dd_periods.append(i - start)
            start = None
    max_dd_duration = max([p.days for p in dd_periods], default=0) if dd_periods else 0

    # Sharpe
    sharpe = float(returns.mean() / returns.std() * np.sqrt(252)) if returns.std() > 0 else 0

    # Calmar
    calmar = abs(cagr / mdd) if mdd != 0 else 0

    # Trade stats
    if trades:
        pnl_list = [t["pnl_pct"] for t in trades]
        wins = [p for p in pnl_list if p > 0]
        losses = [p for p in pnl_list if p <= 0]
        win_rate = len(wins) / len(pnl_list) if pnl_list else 0
        avg_win = np.mean(wins) if wins else 0
        avg_loss = np.mean(losses) if losses else 0
        profit_factor = abs(sum(wins) / sum(losses)) if sum(losses) != 0 else 999
        max_win = max(pnl_list) if pnl_list else 0
        max_loss = min(pnl_list) if pnl_list else 0
        avg_pnl = np.mean(pnl_list)
        med_pnl = np.median(pnl_list)

        # Consecutive losses
        max_consec_loss = 0
        curr_consec = 0
        for p in pnl_list:
            if p <= 0:
                curr_consec += 1
                max_consec_loss = max(max_consec_loss, curr_consec)
            else:
                curr_consec = 0

        # Average hold days
        hold_days_list = [t.get("hold_days", 0) for t in trades]
        avg_hold = np.mean(hold_days_list) if hold_days_list else 0

        # Exit type distribution
        exit_types = {}
        for t in trades:
            et = t.get("exit_reason", "UNKNOWN")
            if et not in exit_types:
                exit_types[et] = {"count": 0, "wins": 0, "total_pnl": 0}
            exit_types[et]["count"] += 1
            exit_types[et]["total_pnl"] += t["pnl_pct"]
            if t["pnl_pct"] > 0:
                exit_types[et]["wins"] += 1

        # Top 3 contribution
        sorted_pnl = sorted(trades, key=lambda x: x["pnl_pct"], reverse=True)
        total_profit = sum(p for p in pnl_list if p > 0)
        top3_profit = sum(t["pnl_pct"] for t in sorted_pnl[:3]) if len(sorted_pnl) >= 3 else total_profit
        top3_contrib = (top3_profit / total_profit * 100) if total_profit > 0 else 0
    else:
        win_rate = avg_win = avg_loss = profit_factor = 0
        max_win = max_loss = avg_pnl = med_pnl = 0
        max_consec_loss = 0
        avg_hold = 0
        exit_types = {}
        top3_contrib = 0

    # Market correlation / Beta / Alpha
    aligned = pd.DataFrame({"strat": returns, "market": idx_returns}).dropna()
    if len(aligned) > 10:
        corr = float(aligned["strat"].corr(aligned["market"]))
        cov = aligned["strat"].cov(aligned["market"])
        var_m = aligned["market"].var()
        beta = cov / var_m if var_m > 0 else 1.0
        market_annual = float(aligned["market"].mean() * 252)
        alpha = cagr - (0.03 + beta * (market_annual - 0.03))  # CAPM alpha

        # Up/Down capture
        up_days = aligned[aligned["market"] > 0]
        down_days = aligned[aligned["market"] < 0]
        up_capture = float(up_days["strat"].mean() / up_days["market"].mean()) if len(up_days) > 0 and up_days["market"].mean() != 0 else 0
        down_capture = float(down_days["strat"].mean() / down_days["market"].mean()) if len(down_days) > 0 and down_days["market"].mean() != 0 else 0
    else:
        corr = beta = alpha = 0
        up_capture = down_capture = 0

    return {
        "total_return": total_ret,
        "cagr": cagr,
        "mdd": mdd,
        "calmar": calmar,
        "sharpe": sharpe,
        "profit_factor": profit_factor,
        "avg_return": avg_pnl,
        "median_return": med_pnl,
        "win_rate": win_rate,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "max_win": max_win,
        "max_loss": max_loss,
        "n_trades": len(trades),
        "max_consec_loss": max_consec_loss,
        "max_dd_duration_days": max_dd_duration,
        "avg_hold_days": avg_hold,
        "correlation": corr,
        "beta": beta,
        "up_capture": up_capture,
        "down_capture": down_capture,
        "alpha": alpha,
        "top3_contrib_pct": top3_contrib,
        "exit_types": exit_types,
    }


# ── Main Backtest Engine ─────────────────────────────────────────────────────
def run_backtest(all_data: Dict[str, pd.DataFrame],
                 idx_df: pd.DataFrame,
                 sector_map: Dict[str, str],
                 start_date: str, end_date: str,
                 rebal_interval: int = 5) -> Tuple[pd.Series, List[Dict], Dict]:
    """
    Run Gen3 strategy backtest.
    """
    initial_cash = 500_000_000
    cash = initial_cash
    positions: List[Position] = []
    all_trades: List[Dict] = []
    equity_history = {}

    # Get trading dates
    idx_dates = idx_df["date"]
    start_dt = pd.Timestamp(start_date)
    end_dt = pd.Timestamp(end_date)

    valid_mask = (idx_dates >= start_dt) & (idx_dates <= end_dt)
    trade_indices = idx_df.index[valid_mask].tolist()

    if not trade_indices:
        print("ERROR: No trading dates in range")
        return pd.Series(), [], {}

    print(f"\n  Period: {idx_dates.iloc[trade_indices[0]].date()} ~ "
          f"{idx_dates.iloc[trade_indices[-1]].date()}")
    print(f"  Trading days: {len(trade_indices)}")

    current_regime = "BULL"
    current_breadth = 0.5
    current_signals = pd.DataFrame()
    last_rebal = -999
    cooldown_tickers = set()  # SL cooldown: no re-entry same rebal cycle

    total_days = len(trade_indices)

    for day_count, idx_i in enumerate(trade_indices):
        trade_date = idx_dates.iloc[idx_i]
        date_str = trade_date.strftime("%Y%m%d")

        # Progress
        if day_count % 50 == 0:
            pct = day_count / total_days * 100
            n_pos = len(positions)
            equity = cash + sum(
                p.qty * float(all_data[p.ticker][all_data[p.ticker]["date"] <= trade_date]["close"].iloc[-1])
                for p in positions
                if p.ticker in all_data and len(all_data[p.ticker][all_data[p.ticker]["date"] <= trade_date]) > 0
            )
            print(f"\r  [{int(pct):3d}%] {trade_date.date()} | "
                  f"Equity: {equity/1e6:,.0f}M | Pos: {n_pos} | "
                  f"Trades: {len(all_trades)} | Regime: {current_regime}",
                  end="", flush=True)

        # ── Rebalance: generate new signals (v2: 매일) ─────────────────────
        # 매일 시그널 재생성 (실운영과 동일)
        # 단, generate_signals는 무거우므로 rebal_interval=1이면 매일 호출
        if day_count - last_rebal >= rebal_interval:
            current_regime, current_breadth, current_signals = generate_signals(
                all_data, idx_df, idx_i, sector_map
            )
            last_rebal = day_count
            cooldown_tickers.clear()

        # ── Check exits ───────────────────────────────────────────────────
        exits = check_exits(positions, all_data, trade_date,
                            current_regime, idx_df, idx_i)

        for pos, reason, exit_price in exits:
            # Apply sell cost
            net_exit = exit_price * (1 - SELL_COST)
            pnl_pct = (net_exit / (pos.entry_price * (1 + BUY_COST))) - 1.0
            pnl_amount = pos.qty * (net_exit - pos.entry_price * (1 + BUY_COST))

            cash += pos.qty * net_exit

            all_trades.append({
                "ticker": pos.ticker,
                "entry_date": pos.entry_date,
                "exit_date": date_str,
                "entry_price": pos.entry_price,
                "exit_price": exit_price,
                "pnl_pct": pnl_pct,
                "pnl_amount": pnl_amount,
                "hold_days": pos.hold_days,
                "exit_reason": reason,
                "stage": pos.stage,
                "sector": pos.sector,
                "regime_at_exit": current_regime,
            })

            # Cooldown for SL exits
            if reason in ("ATR_SL", "MAX_LOSS_CAP", "GAP_DOWN"):
                cooldown_tickers.add(pos.ticker)

            positions.remove(pos)

        # ── New entries ───────────────────────────────────────────────────
        if not current_signals.empty and day_count == last_rebal:
            # Determine max positions and weight
            if current_regime == "BULL":
                max_pos = 20
                weight = 0.07
            else:
                max_pos = 8
                weight = 0.05

            available_slots = max_pos - len(positions)
            if available_slots <= 0:
                pass
            else:
                # Sector counting
                sector_counts = {}
                for p in positions:
                    sector_counts[p.sector] = sector_counts.get(p.sector, 0) + 1

                held_tickers = {p.ticker for p in positions}

                # Current equity estimate
                equity_est = cash + sum(
                    p.qty * p.entry_price for p in positions
                )

                entries_this_cycle = 0
                for _, sig in current_signals.iterrows():
                    if entries_this_cycle >= available_slots:
                        break

                    ticker = sig["ticker"]

                    # Skip already held
                    if ticker in held_tickers:
                        continue

                    # Skip cooldown
                    if ticker in cooldown_tickers:
                        continue

                    # Sector cap
                    sector = sig.get("sector", "ETC")
                    sec_cap = 3 if sector == "ETC" else 4
                    if sector_counts.get(sector, 0) >= sec_cap:
                        continue

                    # ATR rank filter
                    atr_rank = sig.get("atr_rank", 0.5)
                    stage = sig.get("stage", "B")
                    if current_regime == "BULL":
                        if stage == "A" and atr_rank > 0.80:
                            continue
                        elif stage == "B" and atr_rank > 0.70:
                            continue
                    else:
                        if atr_rank > 0.40:
                            continue

                    # BEAR: RS minimum
                    rs_comp = sig.get("rs_composite", 0)
                    if current_regime == "BEAR" and rs_comp < 0.90:
                        continue

                    # Get next day data for entry + tick proxy
                    if ticker not in all_data:
                        continue
                    df_t = all_data[ticker]
                    future = df_t[df_t["date"] > trade_date]
                    if future.empty:
                        continue

                    entry_row = future.iloc[0]
                    entry_price = float(entry_row["open"])
                    if entry_price <= 0:
                        continue

                    # ── v2: 체결강도 프록시 필터 ──────────────────
                    # 실운영: FID 228 체결강도 120+ → 진입
                    # 백테스트: (1) 진입일 거래량 > 20일평균 × 1.3
                    #           (2) 시가 > 전일 종가 (갭업)
                    today_data = df_t[df_t["date"] <= trade_date]
                    if len(today_data) >= 21:
                        prev_close_t = float(today_data["close"].iloc[-1])
                        vol_20avg_t = float(today_data["volume"].iloc[-21:-1].mean())
                        entry_vol = float(entry_row["volume"])
                        entry_open = entry_price

                        vol_ratio_t = entry_vol / vol_20avg_t if vol_20avg_t > 0 else 0
                        gap_up = entry_open > prev_close_t

                        # 체결강도 프록시: 둘 다 만족해야 진입
                        if vol_ratio_t < TICK_VOL_RATIO_MIN:
                            continue  # 거래량 부족 = 매수세 약함
                        if TICK_PRICE_UP and not gap_up:
                            continue  # 갭다운 = 수급 불량

                    # Position size
                    alloc = equity_est * weight
                    if alloc > cash * 0.95:
                        alloc = cash * 0.95
                    if alloc <= 0:
                        break

                    qty = int(alloc / (entry_price * (1 + BUY_COST)))
                    if qty <= 0:
                        continue

                    cost = qty * entry_price * (1 + BUY_COST)
                    if cost > cash:
                        continue

                    # SL/TP
                    atr_val = sig.get("atr20", 0)
                    if current_regime == "BULL":
                        sl_mult = 2.5
                    else:
                        sl_mult = 1.0

                    if atr_val > 0:
                        sl = entry_price - atr_val * sl_mult
                        # MAX_LOSS_CAP clamp
                        sl = max(sl, entry_price * 0.92)
                        tp = entry_price + (entry_price - sl) * 2.0
                    else:
                        sl = entry_price * 0.92
                        tp = entry_price * 1.16

                    cash -= cost

                    positions.append(Position(
                        ticker=ticker,
                        entry_price=entry_price,
                        entry_date=future.iloc[0]["date"].strftime("%Y%m%d")
                            if hasattr(future.iloc[0]["date"], "strftime")
                            else str(future.iloc[0]["date"]),
                        qty=qty,
                        sl=sl,
                        tp=tp,
                        atr=atr_val,
                        stage=stage,
                        sector=sector,
                        rs_composite=rs_comp,
                        high_watermark=entry_price,
                    ))

                    held_tickers.add(ticker)
                    sector_counts[sector] = sector_counts.get(sector, 0) + 1
                    entries_this_cycle += 1

        # ── Record equity ─────────────────────────────────────────────────
        port_value = cash
        for pos in positions:
            if pos.ticker in all_data:
                df_t = all_data[pos.ticker]
                mask_t = df_t["date"] <= trade_date
                if mask_t.any():
                    last_price = float(df_t[mask_t]["close"].iloc[-1])
                    port_value += pos.qty * last_price

        equity_history[trade_date] = port_value

    # Close remaining positions at last date
    last_date = idx_dates.iloc[trade_indices[-1]]
    for pos in positions[:]:
        if pos.ticker in all_data:
            df_t = all_data[pos.ticker]
            mask_t = df_t["date"] <= last_date
            if mask_t.any():
                exit_price = float(df_t[mask_t]["close"].iloc[-1])
                net_exit = exit_price * (1 - SELL_COST)
                pnl_pct = (net_exit / (pos.entry_price * (1 + BUY_COST))) - 1.0

                all_trades.append({
                    "ticker": pos.ticker,
                    "entry_date": pos.entry_date,
                    "exit_date": last_date.strftime("%Y%m%d"),
                    "entry_price": pos.entry_price,
                    "exit_price": exit_price,
                    "pnl_pct": pnl_pct,
                    "pnl_amount": pos.qty * (net_exit - pos.entry_price * (1 + BUY_COST)),
                    "hold_days": pos.hold_days,
                    "exit_reason": "END_OF_TEST",
                    "stage": pos.stage,
                    "sector": pos.sector,
                    "regime_at_exit": current_regime,
                })
                cash += pos.qty * net_exit

    print(f"\r  [100%] Complete | Trades: {len(all_trades)}")

    equity_series = pd.Series(equity_history).sort_index()

    # Index returns for metrics
    idx_returns = idx_df.set_index("date")["close"].pct_change().reindex(equity_series.index).fillna(0)

    metrics = calc_metrics(equity_series, all_trades, idx_returns)

    return equity_series, all_trades, metrics


# ── Regime/Period Analysis ────────────────────────────────────────────────────
def analyze_by_regime(trades: List[Dict], idx_df: pd.DataFrame) -> Dict:
    """Break down performance by regime and year."""
    if not trades:
        return {}

    df = pd.DataFrame(trades)

    # By year
    df["year"] = df["exit_date"].str[:4].astype(int)
    yearly = {}
    for year, grp in df.groupby("year"):
        wins = grp[grp["pnl_pct"] > 0]
        losses = grp[grp["pnl_pct"] <= 0]
        yearly[year] = {
            "n_trades": len(grp),
            "win_rate": len(wins) / len(grp) if len(grp) > 0 else 0,
            "avg_pnl": float(grp["pnl_pct"].mean()),
            "total_pnl": float(grp["pnl_pct"].sum()),
            "avg_hold": float(grp["hold_days"].mean()),
        }

    # By regime at exit
    regime_stats = {}
    for regime, grp in df.groupby("regime_at_exit"):
        wins = grp[grp["pnl_pct"] > 0]
        regime_stats[regime] = {
            "n_trades": len(grp),
            "win_rate": len(wins) / len(grp) if len(grp) > 0 else 0,
            "avg_pnl": float(grp["pnl_pct"].mean()),
            "total_pnl": float(grp["pnl_pct"].sum()),
        }

    # By exit reason
    exit_stats = {}
    for reason, grp in df.groupby("exit_reason"):
        wins = grp[grp["pnl_pct"] > 0]
        exit_stats[reason] = {
            "n_trades": len(grp),
            "win_rate": len(wins) / len(grp) if len(grp) > 0 else 0,
            "avg_pnl": float(grp["pnl_pct"].mean()),
            "total_pnl": float(grp["pnl_pct"].sum()),
        }

    # By stage
    stage_stats = {}
    for stage, grp in df.groupby("stage"):
        wins = grp[grp["pnl_pct"] > 0]
        stage_stats[stage] = {
            "n_trades": len(grp),
            "win_rate": len(wins) / len(grp) if len(grp) > 0 else 0,
            "avg_pnl": float(grp["pnl_pct"].mean()),
        }

    return {
        "yearly": yearly,
        "by_regime": regime_stats,
        "by_exit_reason": exit_stats,
        "by_stage": stage_stats,
    }


# ── HTML Report ───────────────────────────────────────────────────────────────
def generate_html_report(equity: pd.Series, trades: List[Dict],
                         metrics: Dict, breakdown: Dict,
                         output_path: Path) -> None:
    """Generate comprehensive HTML report."""

    def fmt_pct(v):
        return f"{v*100:+.1f}%" if isinstance(v, (int, float)) else str(v)

    def fmt_num(v):
        return f"{v:,.2f}" if isinstance(v, (int, float)) else str(v)

    # Metrics table
    m = metrics
    metrics_rows = ""
    metric_items = [
        ("Total Return", fmt_pct(m.get("total_return", 0))),
        ("CAGR", fmt_pct(m.get("cagr", 0))),
        ("MDD", fmt_pct(m.get("mdd", 0))),
        ("Calmar", fmt_num(m.get("calmar", 0))),
        ("Sharpe", fmt_num(m.get("sharpe", 0))),
        ("Profit Factor", fmt_num(m.get("profit_factor", 0))),
        ("Win Rate", fmt_pct(m.get("win_rate", 0))),
        ("Avg Return", fmt_pct(m.get("avg_return", 0))),
        ("Median Return", fmt_pct(m.get("median_return", 0))),
        ("Avg Win", fmt_pct(m.get("avg_win", 0))),
        ("Avg Loss", fmt_pct(m.get("avg_loss", 0))),
        ("Max Win", fmt_pct(m.get("max_win", 0))),
        ("Max Loss", fmt_pct(m.get("max_loss", 0))),
        ("Trades", str(m.get("n_trades", 0))),
        ("Consec Loss Max", str(m.get("max_consec_loss", 0))),
        ("DD Duration (days)", str(m.get("max_dd_duration_days", 0))),
        ("Avg Hold Days", fmt_num(m.get("avg_hold_days", 0))),
        ("Correlation", fmt_num(m.get("correlation", 0))),
        ("Beta", fmt_num(m.get("beta", 0))),
        ("Up Capture", fmt_num(m.get("up_capture", 0))),
        ("Down Capture", fmt_num(m.get("down_capture", 0))),
        ("Alpha", fmt_pct(m.get("alpha", 0))),
        ("Top3 Contribution", f"{m.get('top3_contrib_pct', 0):.1f}%"),
    ]
    for name, val in metric_items:
        metrics_rows += f"<tr><td>{name}</td><td><b>{val}</b></td></tr>\n"

    # Yearly table
    yearly_rows = ""
    if "yearly" in breakdown:
        for year, ys in sorted(breakdown["yearly"].items()):
            yearly_rows += (f"<tr><td>{year}</td>"
                           f"<td>{ys['n_trades']}</td>"
                           f"<td>{ys['win_rate']*100:.1f}%</td>"
                           f"<td>{ys['avg_pnl']*100:+.2f}%</td>"
                           f"<td>{ys['total_pnl']*100:+.1f}%</td>"
                           f"<td>{ys['avg_hold']:.0f}</td></tr>\n")

    # Regime table
    regime_rows = ""
    if "by_regime" in breakdown:
        for reg, rs in breakdown["by_regime"].items():
            regime_rows += (f"<tr><td>{reg}</td>"
                           f"<td>{rs['n_trades']}</td>"
                           f"<td>{rs['win_rate']*100:.1f}%</td>"
                           f"<td>{rs['avg_pnl']*100:+.2f}%</td>"
                           f"<td>{rs['total_pnl']*100:+.1f}%</td></tr>\n")

    # Exit reason table
    exit_rows = ""
    if "by_exit_reason" in breakdown:
        for reason, es in sorted(breakdown["by_exit_reason"].items(),
                                  key=lambda x: x[1]["n_trades"], reverse=True):
            exit_rows += (f"<tr><td>{reason}</td>"
                         f"<td>{es['n_trades']}</td>"
                         f"<td>{es['win_rate']*100:.1f}%</td>"
                         f"<td>{es['avg_pnl']*100:+.2f}%</td>"
                         f"<td>{es['total_pnl']*100:+.1f}%</td></tr>\n")

    # Stage table
    stage_rows = ""
    if "by_stage" in breakdown:
        for stage, ss in breakdown["by_stage"].items():
            stage_rows += (f"<tr><td>Stage {stage}</td>"
                          f"<td>{ss['n_trades']}</td>"
                          f"<td>{ss['win_rate']*100:.1f}%</td>"
                          f"<td>{ss['avg_pnl']*100:+.2f}%</td></tr>\n")

    # Worst trades
    worst_rows = ""
    if trades:
        sorted_trades = sorted(trades, key=lambda x: x["pnl_pct"])[:10]
        for t in sorted_trades:
            worst_rows += (f"<tr><td>{t['ticker']}</td>"
                          f"<td>{t['entry_date']}</td>"
                          f"<td>{t['exit_date']}</td>"
                          f"<td>{t['entry_price']:,.0f}</td>"
                          f"<td>{t['exit_price']:,.0f}</td>"
                          f"<td style='color:red'>{t['pnl_pct']*100:+.2f}%</td>"
                          f"<td>{t['exit_reason']}</td>"
                          f"<td>{t['hold_days']}</td></tr>\n")

    # Best trades
    best_rows = ""
    if trades:
        sorted_trades = sorted(trades, key=lambda x: x["pnl_pct"], reverse=True)[:10]
        for t in sorted_trades:
            best_rows += (f"<tr><td>{t['ticker']}</td>"
                         f"<td>{t['entry_date']}</td>"
                         f"<td>{t['exit_date']}</td>"
                         f"<td>{t['entry_price']:,.0f}</td>"
                         f"<td>{t['exit_price']:,.0f}</td>"
                         f"<td style='color:green'>{t['pnl_pct']*100:+.2f}%</td>"
                         f"<td>{t['exit_reason']}</td>"
                         f"<td>{t['hold_days']}</td></tr>\n")

    # Verdict
    passed = []
    failed = []
    if m.get("profit_factor", 0) > 1.1:
        passed.append("PF(net) > 1.1")
    else:
        failed.append(f"PF(net) = {m.get('profit_factor', 0):.2f} <= 1.1")

    if m.get("mdd", -1) > -0.30:
        passed.append(f"MDD = {m.get('mdd', 0)*100:.1f}% > -30%")
    else:
        failed.append(f"MDD = {m.get('mdd', 0)*100:.1f}% <= -30%")

    if m.get("alpha", 0) > 0:
        passed.append(f"Alpha = {m.get('alpha', 0)*100:+.1f}%")
    else:
        failed.append(f"Alpha = {m.get('alpha', 0)*100:+.1f}% <= 0")

    if m.get("median_return", 0) > 0:
        passed.append(f"Median = {m.get('median_return', 0)*100:+.2f}%")
    else:
        failed.append(f"Median = {m.get('median_return', 0)*100:+.2f}% <= 0")

    verdict_html = ""
    for p in passed:
        verdict_html += f'<div style="color:green">PASS: {p}</div>\n'
    for f_item in failed:
        verdict_html += f'<div style="color:red">FAIL: {f_item}</div>\n'

    overall = "PASS" if len(failed) == 0 else "FAIL"
    overall_color = "green" if overall == "PASS" else "red"

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>Gen3 Strategy Backtest Report</title>
<style>
body {{ font-family: 'Segoe UI', Arial, sans-serif; background: #0a0a0a; color: #e0e0e0; padding: 20px; }}
h1 {{ color: #00d4ff; border-bottom: 2px solid #00d4ff; padding-bottom: 10px; }}
h2 {{ color: #ffa500; margin-top: 30px; }}
h3 {{ color: #88ccff; }}
table {{ border-collapse: collapse; margin: 10px 0 20px 0; width: 100%; }}
th {{ background: #1a1a2e; color: #00d4ff; padding: 8px 12px; text-align: left; border: 1px solid #333; }}
td {{ padding: 6px 12px; border: 1px solid #333; }}
tr:nth-child(even) {{ background: #111; }}
tr:hover {{ background: #1a1a2e; }}
.verdict {{ font-size: 18px; padding: 15px; margin: 20px 0; border: 2px solid {overall_color};
            background: #111; border-radius: 8px; }}
.summary-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
.card {{ background: #111; border: 1px solid #333; border-radius: 8px; padding: 15px; }}
</style>
</head><body>
<h1>Gen3 Strategy Backtest (RS Composite + ATR SL + Regime)</h1>
<p>Period: {equity.index[0].date() if len(equity) > 0 else 'N/A'} ~
   {equity.index[-1].date() if len(equity) > 0 else 'N/A'} |
   Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>

<div class="verdict">
<h2 style="margin-top:0">Overall Verdict: <span style="color:{overall_color}">{overall}</span></h2>
{verdict_html}
</div>

<div class="summary-grid">
<div class="card">
<h2 style="margin-top:0">Performance Metrics</h2>
<table>{metrics_rows}</table>
</div>
<div class="card">
<h2 style="margin-top:0">Yearly Breakdown</h2>
<table>
<tr><th>Year</th><th>Trades</th><th>WinRate</th><th>Avg PnL</th><th>Sum PnL</th><th>Avg Hold</th></tr>
{yearly_rows}
</table>

<h3>By Regime</h3>
<table>
<tr><th>Regime</th><th>Trades</th><th>WinRate</th><th>Avg PnL</th><th>Sum PnL</th></tr>
{regime_rows}
</table>

<h3>By Stage</h3>
<table>
<tr><th>Stage</th><th>Trades</th><th>WinRate</th><th>Avg PnL</th></tr>
{stage_rows}
</table>
</div>
</div>

<h2>Exit Reason Analysis</h2>
<table>
<tr><th>Reason</th><th>Count</th><th>WinRate</th><th>Avg PnL</th><th>Sum PnL</th></tr>
{exit_rows}
</table>

<h2>Worst 10 Trades</h2>
<table>
<tr><th>Ticker</th><th>Entry</th><th>Exit</th><th>Entry Price</th><th>Exit Price</th><th>PnL</th><th>Reason</th><th>Days</th></tr>
{worst_rows}
</table>

<h2>Best 10 Trades</h2>
<table>
<tr><th>Ticker</th><th>Entry</th><th>Exit</th><th>Entry Price</th><th>Exit Price</th><th>PnL</th><th>Reason</th><th>Days</th></tr>
{best_rows}
</table>

<footer style="margin-top:40px; color:#666; border-top:1px solid #333; padding-top:10px;">
Q-TRON Gen3 Backtest v1.0 | RS Composite + ATR SL + Regime | {datetime.now().strftime('%Y-%m-%d %H:%M')}
</footer>
</body></html>"""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    print(f"\n  Report: {output_path}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Gen3 Strategy Backtest")
    parser.add_argument("--start", default="20230601", help="Start date YYYYMMDD")
    parser.add_argument("--end", default="20260320", help="End date YYYYMMDD")
    parser.add_argument("--rebal", type=int, default=5, help="Rebalance interval (days)")
    args = parser.parse_args()

    print("=" * 70)
    print("  Gen3 Strategy Backtest")
    print("  RS Composite + ATR SL + Regime + Trail + Sector Cap")
    print("=" * 70)

    print("\n[1/4] Loading data...")
    all_data = load_all_ohlcv()
    idx_df = load_index()
    sector_map = load_sector_map()

    print(f"\n[2/4] Running backtest ({args.start} ~ {args.end})...")
    equity, trades, metrics = run_backtest(
        all_data, idx_df, sector_map,
        args.start, args.end, args.rebal
    )

    if equity.empty:
        print("ERROR: No equity data")
        return

    print(f"\n[3/4] Analyzing...")
    breakdown = analyze_by_regime(trades, idx_df)

    # Print summary
    print("\n" + "=" * 70)
    print("  RESULTS SUMMARY")
    print("=" * 70)
    m = metrics
    print(f"  Total Return:  {m.get('total_return', 0)*100:+.1f}%")
    print(f"  CAGR:          {m.get('cagr', 0)*100:+.1f}%")
    print(f"  MDD:           {m.get('mdd', 0)*100:.1f}%")
    print(f"  Sharpe:        {m.get('sharpe', 0):.2f}")
    print(f"  Calmar:        {m.get('calmar', 0):.2f}")
    print(f"  Profit Factor: {m.get('profit_factor', 0):.2f}")
    print(f"  Win Rate:      {m.get('win_rate', 0)*100:.1f}%")
    print(f"  Avg Return:    {m.get('avg_return', 0)*100:+.2f}%")
    print(f"  Median Return: {m.get('median_return', 0)*100:+.2f}%")
    print(f"  Trades:        {m.get('n_trades', 0)}")
    print(f"  Avg Hold:      {m.get('avg_hold_days', 0):.0f} days")
    print(f"  Alpha:         {m.get('alpha', 0)*100:+.1f}%")
    print(f"  Beta:          {m.get('beta', 0):.2f}")
    print(f"  Up Capture:    {m.get('up_capture', 0):.2f}")
    print(f"  Down Capture:  {m.get('down_capture', 0):.2f}")
    print(f"  Top3 Contrib:  {m.get('top3_contrib_pct', 0):.1f}%")

    print(f"\n  Yearly:")
    if "yearly" in breakdown:
        for year, ys in sorted(breakdown["yearly"].items()):
            print(f"    {year}: {ys['n_trades']} trades, "
                  f"WR={ys['win_rate']*100:.0f}%, "
                  f"Avg={ys['avg_pnl']*100:+.2f}%, "
                  f"Sum={ys['total_pnl']*100:+.1f}%")

    print(f"\n  By Regime:")
    if "by_regime" in breakdown:
        for reg, rs in breakdown["by_regime"].items():
            print(f"    {reg}: {rs['n_trades']} trades, "
                  f"WR={rs['win_rate']*100:.0f}%, "
                  f"Avg={rs['avg_pnl']*100:+.2f}%")

    print(f"\n  Exit Reasons:")
    if "by_exit_reason" in breakdown:
        for reason, es in sorted(breakdown["by_exit_reason"].items(),
                                  key=lambda x: x[1]["n_trades"], reverse=True):
            print(f"    {reason}: {es['n_trades']} ({es['win_rate']*100:.0f}% win, "
                  f"avg={es['avg_pnl']*100:+.2f}%)")

    # Verdict
    print(f"\n  VERDICT:")
    checks = [
        ("PF(net) > 1.1", m.get("profit_factor", 0) > 1.1),
        ("MDD > -30%", m.get("mdd", -1) > -0.30),
        ("Alpha > 0", m.get("alpha", 0) > 0),
        ("Median > 0", m.get("median_return", 0) > 0),
        ("Trades < 500", m.get("n_trades", 0) < 500),
    ]
    for label, ok in checks:
        status = "PASS" if ok else "FAIL"
        print(f"    [{status}] {label}")

    # KOSPI comparison
    idx_start_i = idx_df[idx_df["date"] >= pd.Timestamp(args.start)].index[0]
    idx_end_i = idx_df[idx_df["date"] <= pd.Timestamp(args.end)].index[-1]
    kospi_ret = (float(idx_df["close"].iloc[idx_end_i]) /
                 float(idx_df["close"].iloc[idx_start_i]) - 1)
    print(f"\n  KOSPI: {kospi_ret*100:+.1f}%  vs  Gen3: {m.get('total_return', 0)*100:+.1f}%")
    diff = m.get("total_return", 0) - kospi_ret
    print(f"  Excess Return: {diff*100:+.1f}%")

    print(f"\n[4/4] Generating report...")
    report_path = REPORT_DIR / "backtest_gen3_result.html"
    generate_html_report(equity, trades, metrics, breakdown, report_path)

    # Save trades CSV
    trades_path = REPORT_DIR / "backtest_gen3_trades.csv"
    if trades:
        pd.DataFrame(trades).to_csv(trades_path, index=False, encoding="utf-8-sig")
        print(f"  Trades CSV: {trades_path}")

    # Save equity CSV
    equity_path = REPORT_DIR / "backtest_gen3_equity.csv"
    equity.to_csv(equity_path, header=["equity"])
    print(f"  Equity CSV: {equity_path}")

    print(f"\n{'='*70}")
    print(f"  DONE")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
