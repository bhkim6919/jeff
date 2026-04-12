"""
backtester.py — Gen3 v7 + LTR v2 Backtester
=============================================
Isolated from kr-legacy. Reproduces the strategy spec for cross-validation.

Usage:
    cd C:\\Q-TRON-32_ARCHIVE
    .venv\\Scripts\\python.exe -m backtest.gen3v7.backtester

Data: backtest/data_full/ (read-only)
Output: backtest/results/gen3v7_repro/
"""
from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Isolated imports — no kr-legacy references
from backtest.gen3v7.signals import (
    calc_signals, rank_rs_universe, calc_cvar, rank_cvar_universe,
)
from backtest.gen3v7.regime import RegimeDetector, RALDetector, compute_breadth
from backtest.gen3v7.risk_gates import RiskGates, RiskEvent

# ── Paths ────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent.parent.parent  # Q-TRON-32_ARCHIVE
DATA_DIR = BASE_DIR / "backtest" / "data_full"
OHLCV_DIR = DATA_DIR / "ohlcv"
INDEX_DIR = DATA_DIR / "index"
SECTOR_FILE = DATA_DIR / "sector_map.json"
RESULT_DIR = BASE_DIR / "backtest" / "results" / "gen3v7_repro"

# ── Parameters (from spec) ───────────────────────────────────────

INITIAL_CAPITAL = 100_000_000
BUY_COST = 0.00115      # 0.015% fee + 0.10% slippage
SELL_COST = 0.00295      # 0.015% fee + 0.10% slippage + 0.18% tax

ATR_MULT_BULL = 4.0
ATR_MULT_BEAR = 1.0
ATR_STAGE_A_PCTILE = 80
ATR_STAGE_B_PCTILE = 70
ATR_BEAR_MAX_PCTILE = 40

RS_ENTRY_MIN = 0.80
RS_EXIT_THRESH = 0.40
BEAR_RS_MIN = 0.90
EARLY_WEIGHT = 0.05
MAIN_WEIGHT_BULL = 0.07
MAIN_WEIGHT_BEAR = 0.05
MAX_HOLD_DAYS = 60  # calendar days

LTR_HIGH_RISK_RANK = 0.75
LTR_ATR_MULT_SCALE = 0.60

UNIV_MIN_CLOSE = 2000
UNIV_MIN_AMT = 2_000_000_000  # 20억
UNIV_MIN_ROWS = 125


# ── Position ─────────────────────────────────────────────────────

@dataclass
class Position:
    ticker: str
    shares: int
    entry_price: float
    entry_date: str
    sl: float
    stage: str  # "A" or "B"
    sector: str
    atr_at_entry: float = 0.0
    cvar_rank: float = 0.0
    sl_mult: float = 4.0


# ── Trade Record ─────────────────────────────────────────────────

@dataclass
class Trade:
    ticker: str
    stage: str
    entry_date: str
    entry_price: float
    exit_date: str
    exit_price: float
    shares: int
    pnl: float
    pnl_pct: float
    exit_reason: str
    hold_days: int
    sector: str


# ── Main Backtester ──────────────────────────────────────────────

def load_data() -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame, Dict[str, dict]]:
    """Load OHLCV, KOSPI index, sector map."""
    print("[1/5] Loading sector map...")
    with open(SECTOR_FILE, encoding="utf-8") as f:
        sector_map = json.load(f)

    # Filter KOSPI only
    kospi_tickers = {t for t, v in sector_map.items() if v.get("market") == "KOSPI"}
    print(f"  KOSPI tickers in sector_map: {len(kospi_tickers)}")

    print("[2/5] Loading KOSPI index...")
    idx_path = INDEX_DIR / "KOSPI.csv"
    kospi = pd.read_csv(idx_path, encoding="utf-8-sig")
    # Normalize column names
    col_map = {}
    for c in kospi.columns:
        cl = c.strip().lower()
        if cl in ("date", "index", "날짜"):
            col_map[c] = "date"
        elif cl == "close":
            col_map[c] = "close"
        elif cl == "open":
            col_map[c] = "open"
        elif cl == "high":
            col_map[c] = "high"
        elif cl == "low":
            col_map[c] = "low"
        elif cl == "volume":
            col_map[c] = "volume"
    kospi = kospi.rename(columns=col_map)
    if "date" not in kospi.columns:
        kospi = kospi.reset_index()
        kospi.columns = ["date", "open", "high", "low", "close", "volume"]
    kospi["date"] = pd.to_datetime(kospi["date"]).dt.strftime("%Y-%m-%d")
    kospi = kospi.sort_values("date").reset_index(drop=True)
    kospi["close"] = kospi["close"].astype(float)
    kospi["ma200"] = kospi["close"].rolling(200).mean()
    kospi["daily_ret"] = kospi["close"].pct_change()
    print(f"  KOSPI: {len(kospi)} rows, {kospi['date'].iloc[0]} ~ {kospi['date'].iloc[-1]}")

    print("[3/5] Loading OHLCV data (KOSPI only)...")
    all_data: Dict[str, pd.DataFrame] = {}
    csv_files = sorted(OHLCV_DIR.glob("*.csv"))
    loaded = 0
    for f in csv_files:
        ticker = f.stem
        if ticker not in kospi_tickers:
            continue
        try:
            df = pd.read_csv(f)
            if len(df) < UNIV_MIN_ROWS:
                continue
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            df = df.sort_values("date").reset_index(drop=True)
            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["close"])
            all_data[ticker] = df
            loaded += 1
        except Exception:
            continue
    print(f"  Loaded {loaded} KOSPI tickers")

    print("[4/5] Computing signals for all tickers...")
    t0 = time.time()
    for ticker in list(all_data.keys()):
        all_data[ticker] = calc_signals(all_data[ticker])
    print(f"  Signals computed in {time.time()-t0:.1f}s")

    return all_data, kospi, sector_map


def run_backtest():
    """Main backtest loop."""
    all_data, kospi, sector_map = load_data()

    # Build date index from KOSPI
    dates = kospi["date"].tolist()
    kospi_lookup = kospi.set_index("date")

    # Build per-ticker date-indexed lookup
    ticker_lookup: Dict[str, pd.DataFrame] = {}
    for ticker, df in all_data.items():
        ticker_lookup[ticker] = df.set_index("date")

    # State
    cash = float(INITIAL_CAPITAL)
    positions: Dict[str, Position] = {}
    trades: List[Trade] = []
    equity_log: List[dict] = []
    regime_log: List[dict] = []
    risk_events: List[dict] = []
    ltr_log: List[dict] = []
    entry_rejects: List[dict] = []

    regime = RegimeDetector()
    ral = RALDetector()
    risk = RiskGates()

    # Monthly RS & CVaR cache
    rs_ranks: Dict[str, float] = {}
    cvar_ranks: Dict[str, float] = {}
    prev_equity = float(INITIAL_CAPITAL)
    month_start_equity = float(INITIAL_CAPITAL)
    current_month = ""
    hard_stop_today = False

    print("[5/5] Running backtest...")
    t0 = time.time()

    for di, date_str in enumerate(dates):
        if di < 200:  # need MA200 warmup
            continue

        # ── Daily setup ──
        kospi_row = kospi_lookup.loc[date_str] if date_str in kospi_lookup.index else None
        if kospi_row is None:
            continue

        kospi_close = float(kospi_row["close"])
        kospi_ma200 = float(kospi_row["ma200"]) if not np.isnan(kospi_row["ma200"]) else kospi_close
        kospi_ret = float(kospi_row["daily_ret"]) if not np.isnan(kospi_row["daily_ret"]) else 0.0

        # Universe filter
        universe = []
        for ticker, df_idx in ticker_lookup.items():
            if date_str not in df_idx.index:
                continue
            row = df_idx.loc[date_str]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[-1]
            c = float(row.get("close", 0))
            vma = float(row.get("volume_ma20", 0)) if "volume_ma20" in row.index else 0
            avg_daily_amt = c * vma  # 20일 평균 거래대금
            if c >= UNIV_MIN_CLOSE and avg_daily_amt >= UNIV_MIN_AMT:
                universe.append(ticker)

        # Breadth
        above_count = 0
        for t in universe:
            if date_str in ticker_lookup[t].index:
                r = ticker_lookup[t].loc[date_str]
                if isinstance(r, pd.DataFrame):
                    r = r.iloc[-1]
                if float(r.get("above_ma20", 0)) == 1:
                    above_count += 1
        breadth = above_count / len(universe) if universe else 0

        # Regime
        is_bull = regime.update(kospi_close, kospi_ma200, breadth)

        # RAL (use shift(1) — kospi_ret is already pct_change which uses prev close)
        ral_mode = ral.update(kospi_ret)

        # Month check
        month_str = date_str[:7]
        is_month_start = (month_str != current_month)
        if is_month_start:
            current_month = month_str
            month_start_equity = prev_equity

            # Monthly RS ranking — use PREVIOUS day data (T-1) to avoid look-ahead
            prev_date = dates[di - 1] if di > 0 else date_str
            all_rs = {}
            for t in universe:
                if prev_date in ticker_lookup[t].index:
                    r = ticker_lookup[t].loc[prev_date]
                    if isinstance(r, pd.DataFrame):
                        r = r.iloc[-1]
                    all_rs[t] = {
                        "rs20": float(r["rs20"]) if not np.isnan(r.get("rs20", np.nan)) else None,
                        "rs60": float(r["rs60"]) if not np.isnan(r.get("rs60", np.nan)) else None,
                        "rs120": float(r["rs120"]) if not np.isnan(r.get("rs120", np.nan)) else None,
                    }
            rs_ranks = rank_rs_universe(all_rs)

            # Monthly CVaR
            cvar_values = {}
            for t in universe:
                if t in ticker_lookup:
                    df_t = ticker_lookup[t]
                    # Get returns up to (not including) current date
                    mask = df_t.index < date_str
                    if mask.any():
                        returns = df_t.loc[mask, "close"].pct_change().dropna()
                        cvar_values[t] = calc_cvar(returns)
            cvar_ranks = rank_cvar_universe(cvar_values)

        # ATR ranking (T-1 basis to avoid look-ahead)
        _prev_date = dates[di - 1] if di > 0 else date_str
        atr_pcts = {}
        for t in universe:
            if _prev_date in ticker_lookup[t].index:
                r = ticker_lookup[t].loc[_prev_date]
                if isinstance(r, pd.DataFrame):
                    r = r.iloc[-1]
                ap = float(r.get("atr_pct", np.nan))
                if not np.isnan(ap):
                    atr_pcts[t] = ap
        if atr_pcts:
            atr_series = pd.Series(atr_pcts)
            atr_ranks = atr_series.rank(pct=True) * 100
        else:
            atr_ranks = pd.Series(dtype=float)

        # Current equity at OPEN (for risk gate check — pre-exit)
        equity_at_open = cash
        for t, pos in positions.items():
            if date_str in ticker_lookup.get(t, pd.DataFrame()).index:
                r = ticker_lookup[t].loc[date_str]
                if isinstance(r, pd.DataFrame):
                    r = r.iloc[-1]
                equity_at_open += float(r["open"]) * pos.shares
            else:
                equity_at_open += pos.entry_price * pos.shares

        # Close-based equity (used for risk gate, sizing, and logging)
        equity = cash
        for t, pos in positions.items():
            if date_str in ticker_lookup.get(t, pd.DataFrame()).index:
                r = ticker_lookup[t].loc[date_str]
                if isinstance(r, pd.DataFrame):
                    r = r.iloc[-1]
                equity += float(r["close"]) * pos.shares
            else:
                equity += pos.entry_price * pos.shares

        # ── Risk Gates (portfolio level) — close-to-close DD ──
        risk.reset_daily()
        hard_stop, daily_kill, soft_stop = risk.check_portfolio_gates(
            equity, prev_equity, month_start_equity, date_str)

        if hard_stop:
            # Liquidate all at open
            for t in list(positions.keys()):
                pos = positions[t]
                if date_str in ticker_lookup.get(t, pd.DataFrame()).index:
                    r = ticker_lookup[t].loc[date_str]
                    if isinstance(r, pd.DataFrame):
                        r = r.iloc[-1]
                    exit_price = float(r["open"])
                else:
                    exit_price = pos.entry_price
                proceeds = pos.shares * exit_price * (1 - SELL_COST)
                cash += proceeds
                entry_dt = datetime.strptime(pos.entry_date, "%Y-%m-%d")
                exit_dt = datetime.strptime(date_str, "%Y-%m-%d")
                hold = (exit_dt - entry_dt).days
                cost_basis = pos.shares * pos.entry_price * (1 + BUY_COST)
                pnl = proceeds - cost_basis
                pnl_pct = pnl / cost_basis if cost_basis > 0 else 0
                trades.append(Trade(
                    ticker=t, stage=pos.stage, entry_date=pos.entry_date,
                    entry_price=pos.entry_price, exit_date=date_str,
                    exit_price=exit_price, shares=pos.shares,
                    pnl=pnl, pnl_pct=pnl_pct, exit_reason="HARD_STOP",
                    hold_days=hold, sector=pos.sector))
            positions.clear()

        # ── RAL SL Adjustment ──
        for t, pos in positions.items():
            if date_str in ticker_lookup.get(t, pd.DataFrame()).index:
                r = ticker_lookup[t].loc[date_str]
                if isinstance(r, pd.DataFrame):
                    r = r.iloc[-1]
                atr = float(r.get("atr20", pos.atr_at_entry))
                pos.sl = ral.adjust_sl(pos.sl, pos.entry_price, atr)

        # ── LTR v2: holding SL tightening ──
        for t, pos in list(positions.items()):
            cr = cvar_ranks.get(t, 0)
            if cr >= LTR_HIGH_RISK_RANK:
                base_mult = ATR_MULT_BULL if is_bull else ATR_MULT_BEAR
                if date_str in ticker_lookup.get(t, pd.DataFrame()).index:
                    r = ticker_lookup[t].loc[date_str]
                    if isinstance(r, pd.DataFrame):
                        r = r.iloc[-1]
                    atr = float(r.get("atr20", pos.atr_at_entry))
                else:
                    atr = pos.atr_at_entry
                tighter_sl = pos.entry_price - base_mult * LTR_ATR_MULT_SCALE * atr
                if tighter_sl > pos.sl:
                    ltr_log.append({"date": date_str, "ticker": t,
                                    "type": "HOLD_TIGHTEN",
                                    "old_sl": pos.sl, "new_sl": tighter_sl,
                                    "cvar_rank": cr})
                    pos.sl = tighter_sl

        # ── Exit Loop ──
        for t in list(positions.keys()):
            pos = positions[t]
            if date_str not in ticker_lookup.get(t, pd.DataFrame()).index:
                continue
            r = ticker_lookup[t].loc[date_str]
            if isinstance(r, pd.DataFrame):
                r = r.iloc[-1]

            exit_price = None
            exit_reason = None

            # P1: Stop Loss
            low = float(r["low"])
            if low <= pos.sl:
                exit_price = pos.sl
                exit_reason = "STOP_LOSS"

            # P2: RAL CRASH forced exit
            if exit_reason is None and ral.mode == RALDetector.CRASH:
                rs = rs_ranks.get(t, 0.5)
                if rs < 0.45:
                    exit_price = float(r["open"])
                    exit_reason = "RAL_CRASH"

            # P3: Monthly RS exit (calendar day 1~7)
            if exit_reason is None:
                day_of_month = int(date_str[8:10])
                if day_of_month <= 7:
                    rs = rs_ranks.get(t, 0.5)
                    if rs < RS_EXIT_THRESH:
                        exit_price = float(r["open"])
                        exit_reason = "RS_EXIT"

            # P4: Max hold
            if exit_reason is None:
                entry_dt = datetime.strptime(pos.entry_date, "%Y-%m-%d")
                exit_dt = datetime.strptime(date_str, "%Y-%m-%d")
                if (exit_dt - entry_dt).days >= MAX_HOLD_DAYS:
                    exit_price = float(r["open"])
                    exit_reason = "MAX_HOLD"

            if exit_price is not None and exit_price > 0:
                proceeds = pos.shares * exit_price * (1 - SELL_COST)
                cash += proceeds
                entry_dt = datetime.strptime(pos.entry_date, "%Y-%m-%d")
                exit_dt = datetime.strptime(date_str, "%Y-%m-%d")
                hold = (exit_dt - entry_dt).days
                cost_basis = pos.shares * pos.entry_price * (1 + BUY_COST)
                pnl = proceeds - cost_basis
                pnl_pct = pnl / cost_basis if cost_basis > 0 else 0
                trades.append(Trade(
                    ticker=t, stage=pos.stage, entry_date=pos.entry_date,
                    entry_price=pos.entry_price, exit_date=date_str,
                    exit_price=exit_price, shares=pos.shares,
                    pnl=pnl, pnl_pct=pnl_pct, exit_reason=exit_reason,
                    hold_days=hold, sector=pos.sector))
                del positions[t]

        # ── SOFT_STOP: close worst position ──
        if soft_stop and not daily_kill and not hard_stop and positions:
            worst_t = None
            worst_pnl = 0
            for t, pos in positions.items():
                if date_str in ticker_lookup.get(t, pd.DataFrame()).index:
                    r = ticker_lookup[t].loc[date_str]
                    if isinstance(r, pd.DataFrame):
                        r = r.iloc[-1]
                    cur_pnl = float(r["close"]) / pos.entry_price - 1
                    if cur_pnl < worst_pnl:
                        worst_pnl = cur_pnl
                        worst_t = t
            if worst_t and worst_t in positions:
                pos = positions[worst_t]
                r = ticker_lookup[worst_t].loc[date_str]
                if isinstance(r, pd.DataFrame):
                    r = r.iloc[-1]
                exit_price = float(r["close"])
                proceeds = pos.shares * exit_price * (1 - SELL_COST)
                cash += proceeds
                entry_dt = datetime.strptime(pos.entry_date, "%Y-%m-%d")
                exit_dt = datetime.strptime(date_str, "%Y-%m-%d")
                hold = (exit_dt - entry_dt).days
                cost_basis = pos.shares * pos.entry_price * (1 + BUY_COST)
                pnl = proceeds - cost_basis
                pnl_pct = pnl / cost_basis if cost_basis > 0 else 0
                trades.append(Trade(
                    ticker=worst_t, stage=pos.stage, entry_date=pos.entry_date,
                    entry_price=pos.entry_price, exit_date=date_str,
                    exit_price=exit_price, shares=pos.shares,
                    pnl=pnl, pnl_pct=pnl_pct, exit_reason="SOFT_STOP",
                    hold_days=hold, sector=pos.sector))
                del positions[worst_t]

        # ── BEAR transition SL tightening ──
        if not is_bull:
            for t, pos in positions.items():
                if date_str in ticker_lookup.get(t, pd.DataFrame()).index:
                    r = ticker_lookup[t].loc[date_str]
                    if isinstance(r, pd.DataFrame):
                        r = r.iloc[-1]
                    atr = float(r.get("atr20", pos.atr_at_entry))
                    bear_sl = pos.entry_price - ATR_MULT_BEAR * atr
                    if bear_sl > pos.sl:
                        pos.sl = bear_sl

        # ── Entry (skip if HARD/DAILY/SOFT stop) ──
        if hard_stop or daily_kill or soft_stop:
            pass  # no new entries
        else:
            # Sector exposure calculation
            sector_exposure: Dict[str, float] = {}
            total_exposure = 0.0
            for t, pos in positions.items():
                if date_str in ticker_lookup.get(t, pd.DataFrame()).index:
                    r = ticker_lookup[t].loc[date_str]
                    if isinstance(r, pd.DataFrame):
                        r = r.iloc[-1]
                    mv = float(r["close"]) * pos.shares
                else:
                    mv = pos.entry_price * pos.shares
                sector_exposure[pos.sector] = sector_exposure.get(pos.sector, 0) + mv
                total_exposure += mv

            # Candidates: sorted by score
            # Signals use PREVIOUS day (T-1) to avoid look-ahead
            # Execution price uses TODAY's open
            prev_date = dates[di - 1] if di > 0 else date_str
            candidates = []
            for t in universe:
                if t in positions:
                    continue
                rs = rs_ranks.get(t, 0)
                if rs < RS_ENTRY_MIN:
                    continue
                # Need both prev_date (signals) and date_str (execution)
                if prev_date not in ticker_lookup.get(t, pd.DataFrame()).index:
                    continue
                if date_str not in ticker_lookup.get(t, pd.DataFrame()).index:
                    continue
                r_prev = ticker_lookup[t].loc[prev_date]
                if isinstance(r_prev, pd.DataFrame):
                    r_prev = r_prev.iloc[-1]
                r_today = ticker_lookup[t].loc[date_str]
                if isinstance(r_today, pd.DataFrame):
                    r_today = r_today.iloc[-1]

                # Signals from T-1
                if int(r_prev.get("gap_blocked", 0)) == 1:
                    continue
                if not is_bull and rs < BEAR_RS_MIN:
                    continue

                atr_rank = float(atr_ranks.get(t, 50))
                breakout = int(r_prev.get("breakout", 0))
                is_52w = int(r_prev.get("is_52w_high", 0))
                pb = float(r_prev.get("pb_score", 0))
                sector = sector_map.get(t, {}).get("sector", "기타")

                score = rs * 100 + pb

                candidates.append({
                    "ticker": t, "rs": rs, "score": score,
                    "breakout": breakout, "is_52w": is_52w,
                    "atr_rank": atr_rank, "sector": sector,
                    "open": float(r_today["open"]),  # execution at today's open
                    "atr20": float(r_today.get("atr20", 0)),  # today's ATR for SL
                    "cvar_rank": cvar_ranks.get(t, 0),
                })

            candidates.sort(key=lambda x: x["score"], reverse=True)

            # Stage A (BULL only, active sectors >= 3)
            # DISABLED for Step 2 calibration — re-enable after DAILY_KILL fix
            if False and is_bull:
                # Simplified sector activation check
                # (full implementation would check breadth jump, volume surge, new highs)
                n_early = sum(1 for p in positions.values() if p.stage == "A")

                for cand in candidates:
                    if n_early >= 3:
                        break
                    t = cand["ticker"]
                    if cand["atr_rank"] >= ATR_STAGE_A_PCTILE:
                        continue
                    if not (cand["is_52w"] == 1 and cand["rs"] >= 0.80) and \
                       not (cand["breakout"] == 1 and cand["rs"] >= 0.92):
                        continue

                    # Sizing
                    weight = EARLY_WEIGHT
                    budget = min(equity * weight, cash) / (1 + BUY_COST)
                    open_price = cand["open"]
                    if open_price <= 0:
                        continue
                    shares = int(budget / open_price)
                    if shares <= 0:
                        continue
                    order_amount = shares * open_price

                    # Gate check
                    ok, reason = risk.can_enter(
                        order_amount, equity, len(positions), is_bull,
                        cand["sector"], sector_exposure.get(cand["sector"], 0),
                        total_exposure, stage="A", n_early=n_early)
                    if not ok:
                        entry_rejects.append({"date": date_str, "ticker": t,
                                              "stage": "A", "reason": reason})
                        continue

                    # SL
                    base_mult = ATR_MULT_BULL
                    sl_mult = base_mult * LTR_ATR_MULT_SCALE if cand["cvar_rank"] >= LTR_HIGH_RISK_RANK else base_mult
                    sl = open_price - sl_mult * cand["atr20"]
                    if sl <= 0 or (open_price - sl) / open_price < 0.01:
                        continue

                    if cand["cvar_rank"] >= LTR_HIGH_RISK_RANK:
                        ltr_log.append({"date": date_str, "ticker": t,
                                        "type": "ENTRY_TIGHTEN",
                                        "sl_mult": sl_mult, "cvar_rank": cand["cvar_rank"]})

                    # Execute
                    cost = shares * open_price * (1 + BUY_COST)
                    cash -= cost
                    positions[t] = Position(
                        ticker=t, shares=shares, entry_price=open_price,
                        entry_date=date_str, sl=sl, stage="A",
                        sector=cand["sector"], atr_at_entry=cand["atr20"],
                        cvar_rank=cand["cvar_rank"], sl_mult=sl_mult)
                    sector_exposure[cand["sector"]] = sector_exposure.get(cand["sector"], 0) + order_amount
                    total_exposure += order_amount
                    n_early += 1

            # Stage B — with sector cap
            # Count positions per sector
            sector_pos_count: Dict[str, int] = {}
            for _p in positions.values():
                sector_pos_count[_p.sector] = sector_pos_count.get(_p.sector, 0) + 1

            for cand in candidates:
                t = cand["ticker"]
                if t in positions:
                    continue
                if cand["breakout"] != 1:
                    continue
                if cand["rs"] < RS_ENTRY_MIN:
                    continue

                atr_limit = ATR_BEAR_MAX_PCTILE if not is_bull else ATR_STAGE_B_PCTILE
                if cand["atr_rank"] >= atr_limit:
                    continue

                # Sector cap: 4 per sector (8 for "기타")
                sec = cand["sector"]
                sec_cap = 8 if sec == "기타" else 4
                if sector_pos_count.get(sec, 0) >= sec_cap:
                    entry_rejects.append({"date": date_str, "ticker": t,
                                          "stage": "B", "reason": f"SECTOR_CAP ({sec})"})
                    continue

                # Sizing
                weight = MAIN_WEIGHT_BULL if is_bull else MAIN_WEIGHT_BEAR
                budget = min(equity * weight, cash) / (1 + BUY_COST)
                open_price = cand["open"]
                if open_price <= 0:
                    continue
                shares = int(budget / open_price)
                if shares <= 0:
                    continue
                order_amount = shares * open_price

                # Gate check
                ok, reason = risk.can_enter(
                    order_amount, equity, len(positions), is_bull,
                    cand["sector"], sector_exposure.get(cand["sector"], 0),
                    total_exposure, stage="B")
                if not ok:
                    entry_rejects.append({"date": date_str, "ticker": t,
                                          "stage": "B", "reason": reason})
                    continue

                # SL
                base_mult = ATR_MULT_BULL if is_bull else ATR_MULT_BEAR
                sl_mult = base_mult * LTR_ATR_MULT_SCALE if cand["cvar_rank"] >= LTR_HIGH_RISK_RANK else base_mult
                sl = open_price - sl_mult * cand["atr20"]
                if sl <= 0 or (open_price - sl) / open_price < 0.01:
                    continue

                if cand["cvar_rank"] >= LTR_HIGH_RISK_RANK:
                    ltr_log.append({"date": date_str, "ticker": t,
                                    "type": "ENTRY_TIGHTEN",
                                    "sl_mult": sl_mult, "cvar_rank": cand["cvar_rank"]})

                # Execute
                cost = shares * open_price * (1 + BUY_COST)
                cash -= cost
                positions[t] = Position(
                    ticker=t, shares=shares, entry_price=open_price,
                    entry_date=date_str, sl=sl, stage="B",
                    sector=cand["sector"], atr_at_entry=cand["atr20"],
                    cvar_rank=cand["cvar_rank"], sl_mult=sl_mult)
                sector_exposure[cand["sector"]] = sector_exposure.get(cand["sector"], 0) + order_amount
                total_exposure += order_amount
                sector_pos_count[cand["sector"]] = sector_pos_count.get(cand["sector"], 0) + 1

        # ── Equity log ──
        eq_now = cash
        for t, pos in positions.items():
            if date_str in ticker_lookup.get(t, pd.DataFrame()).index:
                r = ticker_lookup[t].loc[date_str]
                if isinstance(r, pd.DataFrame):
                    r = r.iloc[-1]
                eq_now += float(r["close"]) * pos.shares
            else:
                eq_now += pos.entry_price * pos.shares

        equity_log.append({
            "date": date_str, "equity": eq_now, "cash": cash,
            "n_positions": len(positions),
            "exposure": (eq_now - cash) / eq_now if eq_now > 0 else 0,
        })

        regime_log.append({
            "date": date_str, "is_bull": is_bull, "breadth": breadth,
            "ral_mode": ral.mode, "kospi_close": kospi_close,
        })

        prev_equity = eq_now

        # Progress
        if di % 200 == 0:
            print(f"  {date_str}: equity={eq_now:,.0f}, pos={len(positions)}, "
                  f"trades={len(trades)}")

    elapsed = time.time() - t0
    print(f"\nBacktest complete in {elapsed:.1f}s")
    print(f"  Total trades: {len(trades)}")
    print(f"  Final equity: {equity_log[-1]['equity']:,.0f}" if equity_log else "  No data")

    # ── Save Results ──
    save_results(equity_log, trades, regime_log, risk.events,
                 ltr_log, entry_rejects, rs_ranks)


def save_results(equity_log, trades, regime_log, risk_events,
                 ltr_log, entry_rejects, rs_ranks):
    """Save all results to CSV/JSON."""
    RESULT_DIR.mkdir(parents=True, exist_ok=True)

    # Equity
    pd.DataFrame(equity_log).to_csv(RESULT_DIR / "equity.csv", index=False)

    # Trades
    trade_dicts = [
        {"ticker": t.ticker, "stage": t.stage, "entry_date": t.entry_date,
         "entry_price": t.entry_price, "exit_date": t.exit_date,
         "exit_price": t.exit_price, "shares": t.shares,
         "pnl": t.pnl, "pnl_pct": t.pnl_pct,
         "exit_reason": t.exit_reason, "hold_days": t.hold_days,
         "sector": t.sector}
        for t in trades
    ]
    pd.DataFrame(trade_dicts).to_csv(RESULT_DIR / "trades.csv", index=False)

    # Regime
    pd.DataFrame(regime_log).to_csv(RESULT_DIR / "regime_daily.csv", index=False)

    # Risk events
    re_dicts = [{"date": e.date, "gate": e.gate, "detail": e.detail,
                 "equity": e.equity} for e in risk_events]
    pd.DataFrame(re_dicts).to_csv(RESULT_DIR / "risk_events.csv", index=False)

    # LTR
    pd.DataFrame(ltr_log).to_csv(RESULT_DIR / "ltr_tightening.csv", index=False)

    # Entry rejects
    pd.DataFrame(entry_rejects).to_csv(RESULT_DIR / "entry_rejects.csv", index=False)

    # Summary
    eq_df = pd.DataFrame(equity_log)
    if len(eq_df) > 0:
        final_eq = eq_df["equity"].iloc[-1]
        start_eq = INITIAL_CAPITAL
        total_ret = final_eq / start_eq - 1
        n_years = len(eq_df) / 252
        cagr = (final_eq / start_eq) ** (1 / n_years) - 1 if n_years > 0 else 0

        # MDD
        peak = eq_df["equity"].cummax()
        dd = (eq_df["equity"] - peak) / peak
        mdd = dd.min()

        # Sharpe
        eq_df["daily_ret"] = eq_df["equity"].pct_change()
        sharpe = eq_df["daily_ret"].mean() / eq_df["daily_ret"].std() * np.sqrt(252) \
            if eq_df["daily_ret"].std() > 0 else 0

        # Trade stats
        trade_df = pd.DataFrame(trade_dicts)
        stage_a = trade_df[trade_df["stage"] == "A"] if len(trade_df) > 0 else pd.DataFrame()
        stage_b = trade_df[trade_df["stage"] == "B"] if len(trade_df) > 0 else pd.DataFrame()

        exit_dist = trade_df["exit_reason"].value_counts().to_dict() if len(trade_df) > 0 else {}

        # Risk gate counts
        gate_counts = {}
        for e in risk_events:
            gate_counts[e.gate] = gate_counts.get(e.gate, 0) + 1

        # LTR counts
        ltr_df = pd.DataFrame(ltr_log)
        ltr_entry = len(ltr_df[ltr_df["type"] == "ENTRY_TIGHTEN"]) if len(ltr_df) > 0 else 0
        ltr_hold = len(ltr_df[ltr_df["type"] == "HOLD_TIGHTEN"]) if len(ltr_df) > 0 else 0

        summary = {
            "period": f"{eq_df['date'].iloc[0]} ~ {eq_df['date'].iloc[-1]}",
            "trading_days": len(eq_df),
            "total_return": f"{total_ret:.2%}",
            "cagr": f"{cagr:.2%}",
            "mdd": f"{mdd:.2%}",
            "sharpe": f"{sharpe:.3f}",
            "total_trades": len(trades),
            "stage_a_trades": len(stage_a),
            "stage_b_trades": len(stage_b),
            "avg_hold_days": f"{trade_df['hold_days'].mean():.1f}" if len(trade_df) > 0 else "N/A",
            "exit_reason_dist": exit_dist,
            "risk_gate_counts": gate_counts,
            "ltr_entry_tighten": ltr_entry,
            "ltr_hold_tighten": ltr_hold,
        }

        with open(RESULT_DIR / "summary.json", "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        print(f"\n{'='*60}")
        print(f"  SUMMARY")
        print(f"{'='*60}")
        print(f"  Period:      {summary['period']}")
        print(f"  CAGR:        {summary['cagr']}")
        print(f"  MDD:         {summary['mdd']}")
        print(f"  Sharpe:      {summary['sharpe']}")
        print(f"  Total trades: {summary['total_trades']}")
        print(f"  Stage A/B:   {summary['stage_a_trades']}/{summary['stage_b_trades']}")
        print(f"  Exit reasons: {exit_dist}")
        print(f"  Risk gates:  {gate_counts}")
        print(f"  LTR tighten: entry={ltr_entry}, hold={ltr_hold}")
        print(f"{'='*60}")

    print(f"\nResults saved to: {RESULT_DIR}")


if __name__ == "__main__":
    run_backtest()
