"""
backtest_gen2.py
================
Gen2 Q-Score 파이프라인 백테스트 (self-contained).

제약: Demand Score = 0 (외인/기관 데이터 없음), Alpha = 0
     → Q-Score의 ~65%(BULL) / ~60%(SIDEWAYS)만 유효

Usage:
  python backtest_gen2.py
  python backtest_gen2.py --start 20220103 --end 20251231
"""
from __future__ import annotations

import argparse
import sys
import warnings
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "backtest" / "data"
INDEX_DIR = DATA_DIR / "index"
REPORT_DIR = BASE_DIR / "backtest" / "results"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

# ── Config ──────────────────────────────────────────────────────────────────
INITIAL_CASH = 100_000_000
MAX_POSITIONS = 20
MAX_CANDIDATES = 50
MIN_AVG_VOLUME = 2_000_000_000  # 20억

# Cost
BUY_SLIP = 0.002       # 매수 슬리피지 0.2%
SELL_SLIP = 0.002       # 매도 슬리피지 0.2%
FEE = 0.00015           # 수수료 편도 0.015%
SELL_TAX = 0.0018       # 매도세 0.18%

# Market thresholds
BULL_THRESH = 2.5
BEAR_THRESH = 1.5

# Gen2 Core v1.0 ATR
ATR_MULT_BULL = 4.0
ATR_MULT_SIDEWAYS = 2.5
ATR_MULT_BEAR = 1.0

# Q-Score weights
WEIGHT_BULL = {"technical": 0.50, "demand": 0.25, "price": 0.15, "alpha": 0.10}
WEIGHT_SIDEWAYS = {"technical": 0.30, "demand": 0.25, "price": 0.30, "alpha": 0.15}


# ── Position ────────────────────────────────────────────────────────────────
@dataclass
class Position:
    code: str
    entry_price: float
    entry_date: str
    qty: int
    sl: float
    tp: float
    q_score: float


# ── Data Loading ────────────────────────────────────────────────────────────
def load_index(name: str) -> pd.DataFrame:
    path = INDEX_DIR / f"{name}.csv"
    if not path.exists():
        print(f"[WARN] Index file not found: {path}")
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str)
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
    for c in ["open", "high", "low", "close", "volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df


def load_day_data(date_str: str) -> pd.DataFrame:
    path = DATA_DIR / f"{date_str}.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, dtype={"code": str})
    for c in ["open", "high", "low", "close", "volume", "amount", "market_cap"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df


def get_business_days(start: str, end: str) -> List[str]:
    start_dt = datetime.strptime(start, "%Y%m%d")
    end_dt = datetime.strptime(end, "%Y%m%d")
    days = []
    for p in sorted(DATA_DIR.glob("????????.csv")):
        try:
            d = datetime.strptime(p.stem, "%Y%m%d")
            if start_dt <= d <= end_dt:
                days.append(p.stem)
        except ValueError:
            continue
    return days


# ── Stage1: Market State ────────────────────────────────────────────────────
def score_index(idx_df: pd.DataFrame, as_of_date: pd.Timestamp) -> float:
    df = idx_df[idx_df["date"] <= as_of_date].tail(60)
    if len(df) < 60:
        return 0.0

    close = df["close"].values.astype(float)
    vol = df["volume"].values.astype(float)
    score = 0.0

    # MA alignment: ma5 > ma20 > ma60
    ma5 = close[-5:].mean()
    ma20 = close[-20:].mean()
    ma60 = close[-60:].mean()
    if ma5 > ma20 > ma60:
        score += 1.0

    # Volume rising: 5d avg > 20d avg
    v5 = vol[-5:].mean()
    v20 = vol[-20:].mean()
    if v5 > v20:
        score += 1.0

    # Momentum: 20d return > 0
    if close[-1] > close[-20]:
        score += 1.0

    # Volatility stable: ATR14 <= avg ATR
    high = df["high"].values.astype(float)
    low = df["low"].values.astype(float)
    tr = np.maximum(high[1:] - low[1:],
                    np.maximum(np.abs(high[1:] - close[:-1]),
                               np.abs(low[1:] - close[:-1])))
    if len(tr) >= 14:
        atr_series = pd.Series(tr).rolling(14).mean().dropna()
        if len(atr_series) >= 2:
            current_atr = atr_series.iloc[-1]
            avg_atr = atr_series.mean()
            if current_atr <= avg_atr:
                score += 1.0

    return score


def get_market_state(kospi_df: pd.DataFrame, kosdaq_df: pd.DataFrame,
                     as_of_date: pd.Timestamp) -> str:
    scores = []
    if not kospi_df.empty:
        scores.append(score_index(kospi_df, as_of_date))
    if not kosdaq_df.empty:
        scores.append(score_index(kosdaq_df, as_of_date))

    if not scores:
        return "SIDEWAYS"

    avg = sum(scores) / len(scores)
    if avg >= BULL_THRESH:
        return "BULL"
    elif avg <= BEAR_THRESH:
        return "BEAR"
    return "SIDEWAYS"


# ── Stage2: Stock Filter ───────────────────────────────────────────────────
def calc_atr_array(high: np.ndarray, low: np.ndarray, close: np.ndarray,
                   period: int = 14) -> float:
    if len(close) < period + 1:
        return 0.0
    tr = np.maximum(high[1:] - low[1:],
                    np.maximum(np.abs(high[1:] - close[:-1]),
                               np.abs(low[1:] - close[:-1])))
    if len(tr) < period:
        return 0.0
    return float(tr[-period:].mean())


def filter_stocks(day_cache: Dict[str, pd.DataFrame], date_str: str,
                  market_state: str) -> List[dict]:
    """Return list of candidate dicts with precomputed arrays."""
    if market_state == "BEAR":
        return []

    current_df = day_cache.get(date_str)
    if current_df is None or current_df.empty:
        return []

    # Need history: collect recent 120 days of data per stock
    # Build per-stock history from day_cache
    # Get sorted dates up to current
    all_dates = sorted(d for d in day_cache.keys() if d <= date_str)
    recent_dates = all_dates[-120:]  # 최대 120일

    if len(recent_dates) < 20:
        return []

    # Build per-stock arrays
    stock_history: Dict[str, List[dict]] = {}
    for d in recent_dates:
        df = day_cache.get(d)
        if df is None or df.empty:
            continue
        for _, row in df.iterrows():
            code = str(row.get("code", ""))
            if not code:
                continue
            if code not in stock_history:
                stock_history[code] = []
            stock_history[code].append({
                "date": d,
                "open": float(row.get("open", 0)),
                "high": float(row.get("high", 0)),
                "low": float(row.get("low", 0)),
                "close": float(row.get("close", 0)),
                "volume": float(row.get("volume", 0)),
                "amount": float(row.get("amount", 0)),
            })

    candidates = []
    for code, hist in stock_history.items():
        if len(hist) < 20:
            continue

        # Preferred stock filter (code ending in 5~9 for last char of 6-digit)
        if len(code) == 6 and code[-1] in "56789":
            continue

        close_arr = np.array([h["close"] for h in hist])
        high_arr = np.array([h["high"] for h in hist])
        low_arr = np.array([h["low"] for h in hist])
        vol_arr = np.array([h["volume"] for h in hist])
        open_arr = np.array([h["open"] for h in hist])

        last_close = close_arr[-1]
        if last_close < 1000:
            continue

        # Liquidity: 20d avg daily amount
        if len(hist) >= 20:
            amounts = []
            for h in hist[-20:]:
                amt = h["amount"]
                if amt > 0:
                    amounts.append(amt)
                else:
                    amounts.append(h["close"] * h["volume"])
            avg_amount = np.mean(amounts) if amounts else 0
        else:
            avg_amount = 0

        if avg_amount < MIN_AVG_VOLUME:
            continue

        # MA20 trend: close > MA20
        ma20 = float(close_arr[-20:].mean())
        if last_close <= ma20:
            continue

        # Market-specific filters
        if market_state == "BULL":
            if len(close_arr) >= 20:
                ret20 = (last_close / close_arr[-20] - 1.0)
                if ret20 <= 0.05:
                    continue
            if len(vol_arr) >= 60:
                v5 = vol_arr[-5:].mean()
                v60 = vol_arr[-60:].mean()
                if v60 > 0 and v5 >= v60 * 3:
                    continue
        else:  # SIDEWAYS
            if len(close_arr) >= 20:
                ret20 = (last_close / close_arr[-20] - 1.0)
                if ret20 <= 0:
                    continue
            atr_val = calc_atr_array(high_arr, low_arr, close_arr, 14)
            if last_close > 0 and atr_val / last_close >= 0.05:
                continue

        candidates.append({
            "code": code,
            "close": close_arr,
            "high": high_arr,
            "low": low_arr,
            "volume": vol_arr,
            "open": open_arr,
            "last_close": last_close,
            "ma20": ma20,
        })

        if len(candidates) >= MAX_CANDIDATES:
            break

    return candidates


# ── Stage3: Q-Score ─────────────────────────────────────────────────────────
def technical_score(close: np.ndarray) -> float:
    if len(close) < 30:
        return 0.0

    s = 0.0
    close_s = pd.Series(close)

    # MACD > Signal
    ema12 = close_s.ewm(span=12, adjust=False).mean()
    ema26 = close_s.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    if float(macd.iloc[-1]) > float(signal.iloc[-1]):
        s += 0.35

    # RSI 40~65
    delta = close_s.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    if float(loss.iloc[-1]) > 0:
        rs = float(gain.iloc[-1]) / float(loss.iloc[-1])
        rsi = 100 - (100 / (1 + rs))
    else:
        rsi = 100.0
    if 40 <= rsi <= 65:
        s += 0.35

    # Close > BB middle (MA20)
    ma20 = close_s.rolling(20).mean()
    if float(close_s.iloc[-1]) > float(ma20.iloc[-1]):
        s += 0.30

    # BB upper breakout (penalty)
    std20 = close_s.rolling(20).std()
    bb_upper = ma20 + 2 * std20
    if float(close_s.iloc[-1]) > float(bb_upper.iloc[-1]):
        s -= 0.20

    return max(0.0, min(1.0, s))


def price_score(close: np.ndarray, high: np.ndarray) -> float:
    if len(close) < 60:
        return 0.0

    s = 0.0
    last = close[-1]

    # MA alignment: ma5 > ma20 > ma60
    ma5 = close[-5:].mean()
    ma20 = close[-20:].mean()
    ma60 = close[-60:].mean()
    if ma5 > ma20 > ma60:
        s += 0.25

    # Near 52w high (use available data, max 252)
    n = min(252, len(high))
    high_52w = float(high[-n:].max())
    if high_52w > 0 and last >= high_52w * 0.85:
        s += 0.25

    # Resistance breakout: close > prev 20d high
    if len(high) >= 22:
        prev_high = float(high[-21:-1].max())
        if last > prev_high:
            s += 0.25

    # Support hold: close >= MA20 * 0.97
    if last >= ma20 * 0.97:
        s += 0.25

    return min(1.0, s)


def calc_q_score(close: np.ndarray, high: np.ndarray,
                 market_state: str) -> float:
    tech = technical_score(close)
    price = price_score(close, high)
    demand = 0.0  # No data
    alpha = 0.0   # Not implemented

    if market_state == "BULL":
        w = WEIGHT_BULL
    else:
        w = WEIGHT_SIDEWAYS

    q = (tech * w["technical"] + demand * w["demand"] +
         price * w["price"] + alpha * w["alpha"]) * 100

    return round(q, 2)


# ── Stage4: Position Sizing & TP/SL ────────────────────────────────────────
def calc_tp_sl(entry_price: float, high: np.ndarray, low: np.ndarray,
               close: np.ndarray, market_state: str) -> Tuple[float, float]:
    atr = calc_atr_array(high, low, close, 14)
    if atr <= 0:
        return 0.0, 0.0

    if market_state == "BULL":
        sl_mult = ATR_MULT_BULL
    elif market_state == "BEAR":
        sl_mult = ATR_MULT_BEAR
    else:
        sl_mult = ATR_MULT_SIDEWAYS

    sl = entry_price - atr * sl_mult
    tp = entry_price + (entry_price - sl) * 2.0  # R:R = 2:1

    if sl <= 0:
        return 0.0, 0.0

    return tp, sl


# ── Exit Check ──────────────────────────────────────────────────────────────
def check_exits(positions: Dict[str, Position],
                day_cache: Dict[str, pd.DataFrame],
                date_str: str) -> List[Tuple[str, str, float]]:
    """Returns list of (code, reason, exit_price)."""
    exits = []
    current_df = day_cache.get(date_str)
    if current_df is None or current_df.empty:
        return exits

    # Build quick lookup
    price_lookup = {}
    for _, row in current_df.iterrows():
        code = str(row.get("code", ""))
        price_lookup[code] = {
            "open": float(row.get("open", 0)),
            "high": float(row.get("high", 0)),
            "low": float(row.get("low", 0)),
            "close": float(row.get("close", 0)),
        }

    for code, pos in positions.items():
        p = price_lookup.get(code)
        if p is None or p["close"] <= 0:
            continue

        reason = None
        exit_price = p["close"]

        # SL hit (check low)
        if pos.sl > 0 and p["low"] <= pos.sl:
            reason = "SL"
            exit_price = pos.sl

        # TP hit (check high)
        if reason is None and pos.tp > 0 and p["high"] >= pos.tp:
            reason = "TP"
            exit_price = pos.tp

        # MA20 check
        if reason is None:
            all_dates = sorted(d for d in day_cache.keys() if d <= date_str)
            recent = all_dates[-25:]
            closes = []
            for d in recent:
                df_d = day_cache.get(d)
                if df_d is None or df_d.empty:
                    continue
                match = df_d[df_d["code"].astype(str) == code]
                if not match.empty:
                    closes.append(float(match.iloc[0]["close"]))
            if len(closes) >= 20:
                ma20 = np.mean(closes[-20:])
                if p["close"] < ma20:
                    reason = "MA20"
                    exit_price = p["close"]

        if reason:
            # Clamp to day's range
            exit_price = max(exit_price, p["low"])
            exit_price = min(exit_price, p["high"])
            exits.append((code, reason, exit_price))

    return exits


# ── Main Backtest Loop ──────────────────────────────────────────────────────
def run_backtest(start: str, end: str) -> Tuple[pd.DataFrame, pd.DataFrame, dict]:
    print("=" * 60)
    print("  Gen2 Q-Score Backtest (Demand=0, ~65% Q-Score)")
    print("=" * 60)

    # Load indices
    kospi_df = load_index("KOSPI")
    kosdaq_df = load_index("KOSDAQ")
    print(f"  KOSPI index: {len(kospi_df)} days")
    print(f"  KOSDAQ index: {len(kosdaq_df)} days")

    # Business days
    bdays = get_business_days(start, end)
    print(f"  Period: {start} ~ {end} ({len(bdays)} trading days)")
    print(f"  Initial cash: {INITIAL_CASH:,}")
    print("-" * 60)

    # State
    cash = float(INITIAL_CASH)
    positions: Dict[str, Position] = {}
    equity_records = []
    trade_records = []

    # Day data cache (sliding window for memory)
    day_cache: Dict[str, pd.DataFrame] = {}
    CACHE_SIZE = 130

    for i, date_str in enumerate(bdays):
        # Load day data
        df = load_day_data(date_str)
        if not df.empty:
            day_cache[date_str] = df

        # Trim cache
        if len(day_cache) > CACHE_SIZE:
            oldest = sorted(day_cache.keys())[0]
            del day_cache[oldest]

        as_of_date = pd.Timestamp(datetime.strptime(date_str, "%Y%m%d"))

        # ── 1. Exit check ──────────────────────────────────────────────
        exits = check_exits(positions, day_cache, date_str)
        for code, reason, raw_exit_price in exits:
            pos = positions[code]
            exec_price = raw_exit_price * (1 - SELL_SLIP)
            proceeds = pos.qty * exec_price
            commission = proceeds * FEE
            tax = proceeds * SELL_TAX
            net = proceeds - commission - tax

            cost = pos.qty * pos.entry_price
            pnl = net - cost
            pnl_pct = pnl / cost if cost > 0 else 0

            cash += net
            hold_days = len([d for d in bdays if pos.entry_date <= d <= date_str])

            trade_records.append({
                "code": code,
                "entry_date": pos.entry_date,
                "exit_date": date_str,
                "entry_price": pos.entry_price,
                "exit_price": exec_price,
                "qty": pos.qty,
                "pnl": pnl,
                "pnl_pct": pnl_pct,
                "hold_days": hold_days,
                "exit_reason": reason,
                "q_score": pos.q_score,
            })
            del positions[code]

        # ── 2. Market state ────────────────────────────────────────────
        market_state = get_market_state(kospi_df, kosdaq_df, as_of_date)

        # ── 3. Stock filter ────────────────────────────────────────────
        if market_state == "BEAR" or len(positions) >= MAX_POSITIONS:
            candidates = []
        else:
            candidates = filter_stocks(day_cache, date_str, market_state)

        # ── 4. Q-Score & Rank ──────────────────────────────────────────
        scored = []
        for c in candidates:
            if c["code"] in positions:
                continue
            q = calc_q_score(c["close"], c["high"], market_state)
            if q > 0:
                scored.append({**c, "q_score": q})

        scored.sort(key=lambda x: x["q_score"], reverse=True)

        # ── 5. Entry ───────────────────────────────────────────────────
        slots = MAX_POSITIONS - len(positions)
        if slots > 0 and scored:
            per_pos = cash / max(slots, 1)
            for item in scored[:slots]:
                code = item["code"]
                entry_price = item["last_close"] * (1 + BUY_SLIP)
                commission = entry_price * FEE

                qty = int(per_pos // (entry_price * (1 + FEE)))
                if qty <= 0:
                    continue

                total_cost = qty * entry_price * (1 + FEE)
                if total_cost > cash:
                    qty = int(cash * 0.95 // (entry_price * (1 + FEE)))
                    total_cost = qty * entry_price * (1 + FEE)

                if qty <= 0 or total_cost > cash:
                    continue

                tp, sl = calc_tp_sl(entry_price, item["high"], item["low"],
                                    item["close"], market_state)
                if tp <= 0 or sl <= 0:
                    continue

                cash -= total_cost
                positions[code] = Position(
                    code=code,
                    entry_price=entry_price,
                    entry_date=date_str,
                    qty=qty,
                    sl=sl,
                    tp=tp,
                    q_score=item["q_score"],
                )

        # ── 6. Equity ──────────────────────────────────────────────────
        pos_value = 0.0
        current_df = day_cache.get(date_str)
        if current_df is not None and not current_df.empty:
            price_map = dict(zip(current_df["code"].astype(str),
                                 current_df["close"].astype(float)))
            for code, pos in positions.items():
                px = price_map.get(code, pos.entry_price)
                pos_value += pos.qty * px

        equity = cash + pos_value
        equity_records.append({"date": date_str, "equity": equity})

        # Progress
        if i % 50 == 0:
            print(f"  [{date_str}] equity={equity:,.0f}  "
                  f"positions={len(positions)}  cash={cash:,.0f}  "
                  f"state={market_state}")

    # ── Force close remaining ───────────────────────────────────────────
    last_date = bdays[-1] if bdays else start
    for code, pos in list(positions.items()):
        current_df = day_cache.get(last_date)
        if current_df is not None:
            match = current_df[current_df["code"].astype(str) == code]
            px = float(match.iloc[0]["close"]) if not match.empty else pos.entry_price
        else:
            px = pos.entry_price

        exec_price = px * (1 - SELL_SLIP)
        proceeds = pos.qty * exec_price
        net = proceeds - proceeds * FEE - proceeds * SELL_TAX
        cost = pos.qty * pos.entry_price
        pnl = net - cost

        trade_records.append({
            "code": code,
            "entry_date": pos.entry_date,
            "exit_date": last_date,
            "entry_price": pos.entry_price,
            "exit_price": exec_price,
            "qty": pos.qty,
            "pnl": pnl,
            "pnl_pct": pnl / cost if cost > 0 else 0,
            "hold_days": 0,
            "exit_reason": "EOD_FINAL",
            "q_score": pos.q_score,
        })
        cash += net
    positions.clear()

    # ── Build results ───────────────────────────────────────────────────
    eq_df = pd.DataFrame(equity_records)
    tr_df = pd.DataFrame(trade_records) if trade_records else pd.DataFrame()

    # Metrics
    metrics = calc_metrics(eq_df, tr_df)

    return eq_df, tr_df, metrics


# ── Metrics ─────────────────────────────────────────────────────────────────
def calc_metrics(eq_df: pd.DataFrame, tr_df: pd.DataFrame) -> dict:
    if eq_df.empty:
        return {}

    equity = eq_df["equity"]
    init = float(equity.iloc[0])
    final = float(equity.iloc[-1])
    total_ret = (final - init) / init

    n_days = len(equity)
    n_years = n_days / 252
    cagr = (final / init) ** (1 / max(n_years, 0.01)) - 1 if n_years > 0 else 0

    # MDD
    peak = equity.expanding().max()
    dd = (equity - peak) / peak
    mdd = float(dd.min())

    # Sharpe
    daily_ret = equity.pct_change().dropna()
    sharpe = float(daily_ret.mean() / daily_ret.std() * (252 ** 0.5)) \
        if daily_ret.std() > 0 else 0

    # Trade stats
    if not tr_df.empty:
        sells = tr_df[tr_df["exit_reason"] != ""]
        n_trades = len(sells)
        wins = sells[sells["pnl"] > 0]
        losses = sells[sells["pnl"] <= 0]
        win_rate = len(wins) / n_trades if n_trades > 0 else 0
        total_win = float(wins["pnl"].sum()) if len(wins) > 0 else 0
        total_loss = abs(float(losses["pnl"].sum())) if len(losses) > 0 else 0
        pf = total_win / total_loss if total_loss > 0 else float("inf")
        avg_hold = float(sells["hold_days"].mean()) if n_trades > 0 else 0

        # Exit reason breakdown
        reason_counts = sells["exit_reason"].value_counts().to_dict()
    else:
        n_trades = 0
        win_rate = 0
        pf = 0
        avg_hold = 0
        reason_counts = {}

    return {
        "initial_cash": INITIAL_CASH,
        "final_equity": final,
        "total_return": total_ret,
        "cagr": cagr,
        "mdd": mdd,
        "sharpe": sharpe,
        "n_trades": n_trades,
        "win_rate": win_rate,
        "profit_factor": pf,
        "avg_hold_days": avg_hold,
        "exit_reasons": reason_counts,
        "n_years": n_years,
    }


# ── Report ──────────────────────────────────────────────────────────────────
def print_report(metrics: dict):
    print("\n" + "=" * 60)
    print("  Gen2 Q-Score Backtest Results")
    print("=" * 60)
    print(f"  Initial Capital : {metrics.get('initial_cash', 0):>15,.0f}")
    print(f"  Final Equity    : {metrics.get('final_equity', 0):>15,.0f}")
    print(f"  Total Return    : {metrics.get('total_return', 0):>14.1%}")
    print(f"  CAGR            : {metrics.get('cagr', 0):>14.1%}")
    print(f"  MDD             : {metrics.get('mdd', 0):>14.1%}")
    print(f"  Sharpe          : {metrics.get('sharpe', 0):>14.2f}")
    print(f"  Profit Factor   : {metrics.get('profit_factor', 0):>14.2f}")
    print(f"  Total Trades    : {metrics.get('n_trades', 0):>14d}")
    print(f"  Win Rate        : {metrics.get('win_rate', 0):>14.1%}")
    print(f"  Avg Hold Days   : {metrics.get('avg_hold_days', 0):>14.1f}")
    print("-" * 60)
    print("  Exit Reasons:")
    for reason, cnt in metrics.get("exit_reasons", {}).items():
        print(f"    {reason:>15s} : {cnt}")
    print("=" * 60)


def save_results(eq_df: pd.DataFrame, tr_df: pd.DataFrame, metrics: dict,
                 label: str = "gen2_qscore"):
    eq_path = REPORT_DIR / f"{label}_equity.csv"
    tr_path = REPORT_DIR / f"{label}_trades.csv"

    eq_df.to_csv(eq_path, index=False)
    print(f"  Equity curve saved: {eq_path}")

    if not tr_df.empty:
        tr_df.to_csv(tr_path, index=False)
        print(f"  Trades saved: {tr_path}")

    # HTML report
    html_path = REPORT_DIR / f"{label}_report.html"
    html = f"""<!doctype html>
<html lang="ko"><head><meta charset="utf-8"/>
<title>Gen2 Q-Score Backtest</title>
<style>
body{{font-family:'Malgun Gothic',monospace;background:#1a1a2e;color:#e0e0e0;padding:20px;}}
h1{{color:#00d4ff;}} h2{{color:#ffd700;}}
table{{border-collapse:collapse;margin:10px 0;}}
td,th{{border:1px solid #444;padding:6px 12px;text-align:right;}}
th{{background:#2a2a4a;}}
.pos{{color:#00ff88;}} .neg{{color:#ff4444;}}
</style></head><body>
<h1>Gen2 Q-Score Backtest (Demand=0)</h1>
<h2>Summary</h2>
<table>
<tr><th>Metric</th><th>Value</th></tr>
<tr><td>Initial Capital</td><td>{metrics.get('initial_cash',0):,.0f}</td></tr>
<tr><td>Final Equity</td><td>{metrics.get('final_equity',0):,.0f}</td></tr>
<tr><td>Total Return</td><td class="{'pos' if metrics.get('total_return',0)>=0 else 'neg'}">{metrics.get('total_return',0):.1%}</td></tr>
<tr><td>CAGR</td><td class="{'pos' if metrics.get('cagr',0)>=0 else 'neg'}">{metrics.get('cagr',0):.1%}</td></tr>
<tr><td>MDD</td><td class="neg">{metrics.get('mdd',0):.1%}</td></tr>
<tr><td>Sharpe</td><td>{metrics.get('sharpe',0):.2f}</td></tr>
<tr><td>Profit Factor</td><td>{metrics.get('profit_factor',0):.2f}</td></tr>
<tr><td>Total Trades</td><td>{metrics.get('n_trades',0)}</td></tr>
<tr><td>Win Rate</td><td>{metrics.get('win_rate',0):.1%}</td></tr>
<tr><td>Avg Hold Days</td><td>{metrics.get('avg_hold_days',0):.1f}</td></tr>
</table>

<h2>Exit Reasons</h2>
<table>
<tr><th>Reason</th><th>Count</th></tr>
{"".join(f'<tr><td>{r}</td><td>{c}</td></tr>' for r, c in metrics.get('exit_reasons', {}).items())}
</table>

<p style="color:#666;margin-top:30px;">
Note: Demand Score = 0 (no foreign/institutional data), Alpha = 0.<br>
Effective Q-Score uses ~65%% (BULL) / ~60%% (SIDEWAYS) of full scoring capacity.
</p>
</body></html>"""

    html_path.write_text(html, encoding="utf-8")
    print(f"  HTML report saved: {html_path}")


# ── Entry Point ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gen2 Q-Score Backtest")
    parser.add_argument("--start", default="20220103", help="Start date YYYYMMDD")
    parser.add_argument("--end", default="20251231", help="End date YYYYMMDD")
    parser.add_argument("--label", default="gen2_qscore", help="Output label")
    args = parser.parse_args()

    eq_df, tr_df, metrics = run_backtest(args.start, args.end)
    print_report(metrics)
    save_results(eq_df, tr_df, metrics, args.label)
