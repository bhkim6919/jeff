# -*- coding: utf-8 -*-
"""
Strategy Lab — 12-Slot Strategy Simulator + Proposal Generator
==============================================================
100% READ-ONLY. No engine imports. No engine writes.
Reads collected CSV data only (swing ranking, intraday bars, micro samples).

12 strategy slots run in parallel on the same day's data for fair comparison.
Results are ranked and a strategy proposal is generated.

Usage (standalone):
    python strategy_lab_gui.py
    python strategy_lab_gui.py --date 20260403

Usage (embedded in v2):
    from strategy_lab_gui import StrategyLabWidget
    widget = StrategyLabWidget()
"""
from __future__ import annotations

import csv
import json
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ── Paths (same as swing_simulator_gui.py) ────────────────────────
BASE_DIR = Path(__file__).resolve().parent
SWING_DIR = BASE_DIR / "data" / "swing" / "ranking"
INTRADAY_DIR = BASE_DIR / "data" / "intraday"
MICRO_DIR = BASE_DIR / "data" / "micro"
NAME_CACHE_PATH = BASE_DIR / "data" / "stock_name_cache.json"
LAB_RESULTS_DIR = BASE_DIR / "data" / "strategy_lab"

# ── Cost Model (Kiwoom) ──────────────────────────────────────────
FEE_RATE = 0.00015       # 0.015% per side
TAX_RATE = 0.0018        # 0.18% sell only

# Tiered slippage by stock size (opt10198 급등주 기준)
#   Tier 1: 대형주 (price >= 100,000원) — tight spread, deep book
#   Tier 2: 중형주 (10,000 ~ 99,999원) — moderate, most ranking stocks
#   Tier 3: 소형주 (price < 10,000원) — wide spread, thin book, fast move
SLIPPAGE_TIERS = {
    "T1": {"buy": 0.0010, "sell": 0.0015, "label": "Large-cap"},   # 0.10% / 0.15%
    "T2": {"buy": 0.0020, "sell": 0.0030, "label": "Mid-cap"},     # 0.20% / 0.30%
    "T3": {"buy": 0.0035, "sell": 0.0050, "label": "Small-cap"},   # 0.35% / 0.50%
}

# Legacy single-value (not used in tiered mode, kept for backward compat)
SLIPPAGE_BUY = 0.002
SLIPPAGE_SELL = 0.003

INITIAL_CAPITAL = 100_000_000
SLOTS = 20
PER_SLOT = INITIAL_CAPITAL // SLOTS


def get_slippage(price: float) -> Tuple[float, float, str]:
    """Return (buy_slippage, sell_slippage, tier_label) based on stock price."""
    if price >= 100_000:
        t = SLIPPAGE_TIERS["T1"]
    elif price >= 10_000:
        t = SLIPPAGE_TIERS["T2"]
    else:
        t = SLIPPAGE_TIERS["T3"]
    return t["buy"], t["sell"], t["label"]


# ══════════════════════════════════════════════════════════════════
#  Data Loaders (zero engine dependency)
# ══════════════════════════════════════════════════════════════════

def load_name_cache() -> dict:
    if NAME_CACHE_PATH.exists():
        try:
            return json.loads(NAME_CACHE_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def load_ranking(date_str: str) -> List[dict]:
    path = SWING_DIR / f"{date_str}.csv"
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def load_minute_bars(code: str, date_str: str) -> List[dict]:
    path = INTRADAY_DIR / f"{code}.csv"
    if not path.exists():
        return []
    date_prefix = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return [r for r in csv.DictReader(f)
                    if r.get("datetime", "").startswith(date_prefix)]
    except Exception:
        return []


def load_micro_samples(code: str, date_str: str) -> List[dict]:
    path = MICRO_DIR / f"{code}_{date_str}.csv"
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def available_dates() -> List[str]:
    if not SWING_DIR.exists():
        return []
    return sorted([f.stem for f in SWING_DIR.glob("*.csv")], reverse=True)


# ══════════════════════════════════════════════════════════════════
#  Trade Record
# ══════════════════════════════════════════════════════════════════

@dataclass
class Trade:
    code: str
    name: str
    entry_time: str
    entry_price: float
    exit_time: str = ""
    exit_price: float = 0.0
    exit_reason: str = ""
    qty: int = 0
    pnl: float = 0.0
    pnl_pct: float = 0.0
    mfe: float = 0.0
    mae: float = 0.0      # max adverse excursion
    hold_minutes: int = 0


def _close_trade(t: Trade, exit_price: float, exit_time: str, reason: str):
    """Calculate PnL with tiered slippage already applied to entry/exit prices."""
    t.exit_price = exit_price
    t.exit_time = exit_time
    t.exit_reason = reason
    buy_amt = t.qty * t.entry_price
    sell_amt = t.qty * exit_price
    buy_fee = buy_amt * FEE_RATE
    sell_fee = sell_amt * FEE_RATE
    sell_tax = sell_amt * TAX_RATE
    t.pnl = sell_amt - buy_amt - buy_fee - sell_fee - sell_tax
    t.pnl_pct = t.pnl / buy_amt * 100 if buy_amt > 0 else 0
    # Hold time
    try:
        eh, em = int(t.entry_time[:2]), int(t.entry_time[3:5])
        xh, xm = int(exit_time[:2]), int(exit_time[3:5])
        t.hold_minutes = (xh * 60 + xm) - (eh * 60 + em)
    except (ValueError, IndexError):
        pass


def _apply_entry_slippage(raw_price: float) -> float:
    """Apply tiered buy slippage to raw entry price."""
    slip_buy, _, _ = get_slippage(raw_price)
    return raw_price * (1 + slip_buy)


def _apply_exit_slippage(raw_price: float, entry_price: float = 0) -> float:
    """Apply tiered sell slippage to raw exit price.
    Uses entry_price for tier classification (position's price level)."""
    ref = entry_price if entry_price > 0 else raw_price
    _, slip_sell, _ = get_slippage(ref)
    return raw_price * (1 - slip_sell)


# ══════════════════════════════════════════════════════════════════
#  Strategy Definitions (12 Slots)
# ══════════════════════════════════════════════════════════════════

@dataclass
class StrategyConfig:
    slot: int
    name: str
    category: str           # "fixed" | "trailing" | "micro" | "time" | "volume" | "momentum" | "reversal" | "sector" | "composite"
    description: str
    params: dict = field(default_factory=dict)


DEFAULT_STRATEGIES: List[StrategyConfig] = [
    StrategyConfig(1, "Conservative", "fixed",
                   "TP:0.8% SL:-0.4%",
                   {"tp_pct": 0.8, "sl_pct": -0.4}),
    StrategyConfig(2, "Moderate", "fixed",
                   "TP:1.5% SL:-0.8%",
                   {"tp_pct": 1.5, "sl_pct": -0.8}),
    StrategyConfig(3, "Aggressive", "fixed",
                   "TP:3.0% SL:-1.5%",
                   {"tp_pct": 3.0, "sl_pct": -1.5}),
    StrategyConfig(4, "Trailing A", "trailing",
                   "Trail:0.8% Act:+0.5%",
                   {"trail_pct": 0.8, "activation_pct": 0.5}),
    StrategyConfig(5, "Trailing B", "trailing",
                   "Trail:1.5% Act:+1.0%",
                   {"trail_pct": 1.5, "activation_pct": 1.0}),
    StrategyConfig(6, "Micro Imbalance", "micro",
                   "Bid>1.5x→TP+, Bid<0.7x→Exit",
                   {"imb_up": 1.5, "imb_down": 0.7, "consec": 2,
                    "tp_init": 1.5, "tp_step": 0.5, "tp_max": 5.0,
                    "sl_pct": -1.0, "hard_stop": -3.0}),
    StrategyConfig(7, "Time Decay 15m", "time",
                   "Max hold 15min, TP:1.0% SL:-0.5%",
                   {"max_minutes": 15, "tp_pct": 1.0, "sl_pct": -0.5}),
    StrategyConfig(8, "Time Decay 30m", "time",
                   "Max hold 30min, TP:1.5% SL:-0.8%",
                   {"max_minutes": 30, "tp_pct": 1.5, "sl_pct": -0.8}),
    StrategyConfig(9, "Volume Spike", "volume",
                   "Vol>3x avg → entry, TP:2.0% SL:-1.0%",
                   {"vol_mult": 3.0, "tp_pct": 2.0, "sl_pct": -1.0}),
    StrategyConfig(10, "Momentum MA5", "momentum",
                   "Close>MA5 entry, Close<MA5 exit",
                   {"ma_period": 5, "sl_pct": -1.5}),
    StrategyConfig(11, "Mean Reversion", "reversal",
                   "Drop>-2% entry, Bounce+1% exit",
                   {"entry_drop": -2.0, "exit_bounce": 1.0, "sl_pct": -3.0}),
    StrategyConfig(12, "Adaptive Best", "composite",
                   "Top-3 결과의 가중 평균 전략",
                   {"blend_top_n": 3}),
]


# ══════════════════════════════════════════════════════════════════
#  Simulation Engines
# ══════════════════════════════════════════════════════════════════

def _get_entry_candidates(rankings: List[dict]) -> List[Tuple[str, dict]]:
    """Extract unique entry candidates from ranking snapshots."""
    snapshots = {}
    for r in rankings:
        t = r.get("snapshot_time", "")
        snapshots.setdefault(t, []).append(r)
    candidates = []
    seen = set()
    for t in sorted(snapshots.keys()):
        for r in snapshots[t]:
            code = r["code"]
            if code not in seen:
                seen.add(code)
                candidates.append((t, r))
    return candidates


# FIX-003: Minimum bars required after entry for meaningful simulation.
# At 1-min bars, 30 = 30 minutes of tradeable data.
# Prevents degenerate EOD-only exits when data arrives too late.
MIN_ENTRY_BARS = 30


def _find_entry_bar(bars: List[dict], after_time: str):
    for bar in bars:
        bt = bar.get("datetime", "")[11:16]
        if bt > after_time[:5]:
            return bar
    return None


def _has_sufficient_bars(bars: List[dict], entry_bar: dict) -> bool:
    """Check if there are enough bars after entry for meaningful simulation."""
    entry_dt = entry_bar.get("datetime", "")[11:16]
    after = [b for b in bars if b.get("datetime", "")[11:16] > entry_dt]
    return len(after) >= MIN_ENTRY_BARS


def run_strategy(config: StrategyConfig, rankings: List[dict],
                 date_str: str, name_cache: dict) -> List[Trade]:
    """Run a single strategy on day's data. Returns trade list."""
    cat = config.category
    p = config.params

    if cat == "fixed":
        return _run_fixed(rankings, date_str, name_cache, p["tp_pct"], p["sl_pct"])
    elif cat == "trailing":
        return _run_trailing(rankings, date_str, name_cache,
                             p["trail_pct"], p.get("activation_pct", 0))
    elif cat == "micro":
        return _run_micro(rankings, date_str, name_cache, p)
    elif cat == "time":
        return _run_time_decay(rankings, date_str, name_cache,
                               p["max_minutes"], p["tp_pct"], p["sl_pct"])
    elif cat == "volume":
        return _run_volume_spike(rankings, date_str, name_cache, p)
    elif cat == "momentum":
        return _run_momentum(rankings, date_str, name_cache, p)
    elif cat == "reversal":
        return _run_reversal(rankings, date_str, name_cache, p)
    elif cat == "composite":
        return []  # filled after other slots run
    return []


def _run_fixed(rankings, date_str, nc, tp_pct, sl_pct) -> List[Trade]:
    trades = []
    candidates = _get_entry_candidates(rankings)
    for snap_time, r in candidates:
        if len(trades) >= SLOTS:
            break
        code = r["code"]
        bars = load_minute_bars(code, date_str)
        entry_bar = _find_entry_bar(bars, snap_time)
        if not entry_bar:
            continue
        if not _has_sufficient_bars(bars, entry_bar):
            continue
        ep = _apply_entry_slippage(float(entry_bar["open"]))
        qty = int(PER_SLOT / ep)
        if qty <= 0:
            continue
        t = Trade(code=code, name=nc.get(code, r.get("name", code)),
                  entry_time=entry_bar["datetime"][11:16], entry_price=ep, qty=qty)
        _simulate_fixed_exit(t, bars, tp_pct, sl_pct)
        trades.append(t)
    return trades


def _simulate_fixed_exit(t: Trade, bars, tp_pct, sl_pct):
    tp = t.entry_price * (1 + tp_pct / 100)
    sl = t.entry_price * (1 + sl_pct / 100)
    for bar in bars:
        bt = bar.get("datetime", "")[11:16]
        if bt <= t.entry_time:
            continue
        h, l, c = float(bar.get("high", 0)), float(bar.get("low", 0)), float(bar.get("close", 0))
        if h > 0:
            t.mfe = max(t.mfe, (h / t.entry_price - 1) * 100)
        if l > 0:
            t.mae = min(t.mae, (l / t.entry_price - 1) * 100)
        if l <= sl and sl_pct < 0:
            _close_trade(t, _apply_exit_slippage(sl, t.entry_price), bt, "SL")
            return
        if h >= tp:
            _close_trade(t, _apply_exit_slippage(tp, t.entry_price), bt, "TP")
            return
    if bars:
        eod = _apply_exit_slippage(float(bars[-1].get("close", 0)), t.entry_price)
        _close_trade(t, eod, bars[-1]["datetime"][11:16], "EOD")


def _run_trailing(rankings, date_str, nc, trail_pct, activation_pct) -> List[Trade]:
    trades = []
    candidates = _get_entry_candidates(rankings)
    for snap_time, r in candidates:
        if len(trades) >= SLOTS:
            break
        code = r["code"]
        bars = load_minute_bars(code, date_str)
        entry_bar = _find_entry_bar(bars, snap_time)
        if not entry_bar:
            continue
        if not _has_sufficient_bars(bars, entry_bar):
            continue
        ep = _apply_entry_slippage(float(entry_bar["open"]))
        qty = int(PER_SLOT / ep)
        if qty <= 0:
            continue
        t = Trade(code=code, name=nc.get(code, r.get("name", code)),
                  entry_time=entry_bar["datetime"][11:16], entry_price=ep, qty=qty)

        hwm = ep
        activated = activation_pct <= 0
        for bar in bars:
            bt = bar.get("datetime", "")[11:16]
            if bt <= t.entry_time:
                continue
            h = float(bar.get("high", 0))
            l = float(bar.get("low", 0))
            c = float(bar.get("close", 0))
            if h > 0:
                hwm = max(hwm, h)
                t.mfe = max(t.mfe, (h / ep - 1) * 100)
            if l > 0:
                t.mae = min(t.mae, (l / ep - 1) * 100)
            if not activated and hwm >= ep * (1 + activation_pct / 100):
                activated = True
            if activated and l > 0:
                trail_price = hwm * (1 - trail_pct / 100)
                if l <= trail_price:
                    _close_trade(t, _apply_exit_slippage(trail_price, t.entry_price), bt, "TRAIL")
                    break
            # Hard stop -5%
            hard = ep * 0.95
            if l > 0 and l <= hard:
                _close_trade(t, _apply_exit_slippage(hard, t.entry_price), bt, "HARD_STOP")
                break
        if not t.exit_time and bars:
            eod = _apply_exit_slippage(float(bars[-1].get("close", 0)), t.entry_price)
            _close_trade(t, eod, bars[-1]["datetime"][11:16], "EOD")
        trades.append(t)
    return trades


def _run_micro(rankings, date_str, nc, p) -> List[Trade]:
    """Micro imbalance strategy (same as swing_simulator_gui dynamic)."""
    trades = []
    candidates = _get_entry_candidates(rankings)
    for snap_time, r in candidates:
        if len(trades) >= SLOTS:
            break
        code = r["code"]
        bars = load_minute_bars(code, date_str)
        micro = load_micro_samples(code, date_str)
        entry_bar = _find_entry_bar(bars, snap_time)
        if not entry_bar:
            continue
        if not _has_sufficient_bars(bars, entry_bar):
            continue
        ep = _apply_entry_slippage(float(entry_bar["open"]))
        qty = int(PER_SLOT / ep)
        if qty <= 0:
            continue
        t = Trade(code=code, name=nc.get(code, r.get("name", code)),
                  entry_time=entry_bar["datetime"][11:16], entry_price=ep, qty=qty)

        micro_by_time = {}
        for m in micro:
            ts = m.get("timestamp", "")[:8]
            try:
                ta = float(m.get("total_ask", 1))
                tb = float(m.get("total_bid", 0))
                mp = float(m.get("price", 0))
                micro_by_time[ts] = {"imb": tb / ta if ta > 0 else 1.0, "price": mp}
            except (ValueError, TypeError):
                pass

        current_tp = p.get("tp_init", 1.5)
        imb_up_c = imb_dn_c = 0
        prev_mp = 0
        exited = False

        for bar in bars:
            bt = bar.get("datetime", "")[11:16]
            if bt <= t.entry_time:
                continue
            h, l, c = float(bar.get("high", 0)), float(bar.get("low", 0)), float(bar.get("close", 0))
            if h > 0:
                t.mfe = max(t.mfe, (h / ep - 1) * 100)
            if l > 0:
                t.mae = min(t.mae, (l / ep - 1) * 100)
            hard = ep * (1 + p.get("hard_stop", -3.0) / 100)
            if l > 0 and l <= hard:
                _close_trade(t, hard, bt, "HARD_STOP")
                exited = True
                break
            trail_sl = max(p.get("sl_pct", -1.0), t.mfe * 0.5 - 0.2)
            sl_price = ep * (1 + trail_sl / 100)
            if l > 0 and l <= sl_price and trail_sl < 0:
                _close_trade(t, sl_price, bt, "SL")
                exited = True
                break
            tp_price = ep * (1 + current_tp / 100)
            if h >= tp_price:
                _close_trade(t, tp_price, bt, "TP")
                exited = True
                break
            for sec in range(0, 60, 5):
                mk = f"{bt}:{sec:02d}"
                md = micro_by_time.get(mk)
                if not md:
                    continue
                if md["imb"] > p.get("imb_up", 1.5):
                    imb_up_c += 1
                    if imb_up_c >= p.get("consec", 2) and current_tp < p.get("tp_max", 5.0):
                        current_tp = min(current_tp + p.get("tp_step", 0.5), p.get("tp_max", 5.0))
                        imb_up_c = 0
                else:
                    imb_up_c = 0
                if md["imb"] < p.get("imb_down", 0.7):
                    imb_dn_c += 1
                    if imb_dn_c >= p.get("consec", 2) and prev_mp > 0 and md["price"] < prev_mp:
                        _close_trade(t, _apply_exit_slippage(md["price"], t.entry_price), bt, "BREAKDOWN")
                        exited = True
                        break
                else:
                    imb_dn_c = 0
                prev_mp = md["price"]
            if exited:
                break
        if not exited and bars:
            eod = _apply_exit_slippage(float(bars[-1].get("close", 0)), t.entry_price)
            _close_trade(t, eod, bars[-1]["datetime"][11:16], "EOD")
        trades.append(t)
    return trades


def _run_time_decay(rankings, date_str, nc, max_min, tp_pct, sl_pct) -> List[Trade]:
    trades = []
    candidates = _get_entry_candidates(rankings)
    for snap_time, r in candidates:
        if len(trades) >= SLOTS:
            break
        code = r["code"]
        bars = load_minute_bars(code, date_str)
        entry_bar = _find_entry_bar(bars, snap_time)
        if not entry_bar:
            continue
        if not _has_sufficient_bars(bars, entry_bar):
            continue
        ep = _apply_entry_slippage(float(entry_bar["open"]))
        qty = int(PER_SLOT / ep)
        if qty <= 0:
            continue
        t = Trade(code=code, name=nc.get(code, r.get("name", code)),
                  entry_time=entry_bar["datetime"][11:16], entry_price=ep, qty=qty)
        tp = ep * (1 + tp_pct / 100)
        sl = ep * (1 + sl_pct / 100)
        bar_count = 0
        for bar in bars:
            bt = bar.get("datetime", "")[11:16]
            if bt <= t.entry_time:
                continue
            bar_count += 1
            h, l = float(bar.get("high", 0)), float(bar.get("low", 0))
            if h > 0:
                t.mfe = max(t.mfe, (h / ep - 1) * 100)
            if l > 0:
                t.mae = min(t.mae, (l / ep - 1) * 100)
            if l > 0 and l <= sl:
                _close_trade(t, _apply_exit_slippage(sl, t.entry_price), bt, "SL")
                break
            if h >= tp:
                _close_trade(t, _apply_exit_slippage(tp, t.entry_price), bt, "TP")
                break
            if bar_count >= max_min:
                c = _apply_exit_slippage(float(bar.get("close", 0)), t.entry_price)
                _close_trade(t, c, bt, "TIME_EXIT")
                break
        if not t.exit_time and bars:
            eod = _apply_exit_slippage(float(bars[-1].get("close", 0)), t.entry_price)
            _close_trade(t, eod, bars[-1]["datetime"][11:16], "EOD")
        trades.append(t)
    return trades


def _run_volume_spike(rankings, date_str, nc, p) -> List[Trade]:
    trades = []
    candidates = _get_entry_candidates(rankings)
    for snap_time, r in candidates:
        if len(trades) >= SLOTS:
            break
        code = r["code"]
        bars = load_minute_bars(code, date_str)
        if len(bars) < 10:
            continue
        # Find volume spike entry
        entry_idx = None
        for i in range(5, len(bars)):
            avg_vol = sum(float(bars[j].get("close", 0)) for j in range(i - 5, i)) / 5
            cur_vol = float(bars[i].get("close", 0))
            if avg_vol > 0 and cur_vol > avg_vol * p.get("vol_mult", 3.0):
                entry_idx = i
                break
        if entry_idx is None or entry_idx >= len(bars) - 1:
            continue
        eb = bars[entry_idx + 1]  # next bar open
        ep = _apply_entry_slippage(float(eb.get("open", 0)))
        if ep <= 0:
            continue
        qty = int(PER_SLOT / ep)
        if qty <= 0:
            continue
        t = Trade(code=code, name=nc.get(code, r.get("name", code)),
                  entry_time=eb["datetime"][11:16], entry_price=ep, qty=qty)
        _simulate_fixed_exit(t, bars[entry_idx + 1:], p["tp_pct"], p["sl_pct"])
        trades.append(t)
    return trades


def _run_momentum(rankings, date_str, nc, p) -> List[Trade]:
    trades = []
    candidates = _get_entry_candidates(rankings)
    ma_n = p.get("ma_period", 5)
    for snap_time, r in candidates:
        if len(trades) >= SLOTS:
            break
        code = r["code"]
        bars = load_minute_bars(code, date_str)
        if len(bars) < ma_n + 2:
            continue
        closes = [float(b.get("close", 0)) for b in bars]
        entry_idx = None
        for i in range(ma_n, len(closes)):
            ma = sum(closes[i - ma_n:i]) / ma_n
            if ma > 0 and closes[i] > ma and (i == ma_n or closes[i - 1] <= sum(closes[i - 1 - ma_n:i - 1]) / ma_n):
                entry_idx = i
                break
        if entry_idx is None or entry_idx >= len(bars) - 1:
            continue
        eb = bars[entry_idx]
        ep = _apply_entry_slippage(float(eb.get("close", 0)))
        if ep <= 0:
            continue
        qty = int(PER_SLOT / ep)
        if qty <= 0:
            continue
        t = Trade(code=code, name=nc.get(code, r.get("name", code)),
                  entry_time=eb["datetime"][11:16], entry_price=ep, qty=qty)
        sl = ep * (1 + p.get("sl_pct", -1.5) / 100)
        for bar in bars[entry_idx + 1:]:
            bt = bar.get("datetime", "")[11:16]
            h, l, c = float(bar.get("high", 0)), float(bar.get("low", 0)), float(bar.get("close", 0))
            if h > 0:
                t.mfe = max(t.mfe, (h / ep - 1) * 100)
            if l > 0:
                t.mae = min(t.mae, (l / ep - 1) * 100)
            if l > 0 and l <= sl:
                _close_trade(t, _apply_exit_slippage(sl, t.entry_price), bt, "SL")
                break
            # MA cross down exit
            idx = bars.index(bar)
            if idx >= ma_n:
                recent = [float(bars[j].get("close", 0)) for j in range(idx - ma_n + 1, idx + 1)]
                ma_now = sum(recent) / ma_n if recent else 0
                if ma_now > 0 and c < ma_now:
                    _close_trade(t, _apply_exit_slippage(c, t.entry_price), bt, "MA_EXIT")
                    break
        if not t.exit_time and bars:
            eod = _apply_exit_slippage(float(bars[-1].get("close", 0)), t.entry_price)
            _close_trade(t, eod, bars[-1]["datetime"][11:16], "EOD")
        trades.append(t)
    return trades


def _run_reversal(rankings, date_str, nc, p) -> List[Trade]:
    trades = []
    candidates = _get_entry_candidates(rankings)
    for snap_time, r in candidates:
        if len(trades) >= SLOTS:
            break
        code = r["code"]
        bars = load_minute_bars(code, date_str)
        if len(bars) < 5:
            continue
        # Find drop entry
        entry_idx = None
        for i in range(1, len(bars)):
            prev_c = float(bars[i - 1].get("close", 0))
            cur_c = float(bars[i].get("close", 0))
            if prev_c > 0 and cur_c > 0:
                drop = (cur_c / prev_c - 1) * 100
                if drop <= p.get("entry_drop", -2.0):
                    entry_idx = i
                    break
        if entry_idx is None or entry_idx >= len(bars) - 1:
            continue
        eb = bars[entry_idx + 1]
        ep = _apply_entry_slippage(float(eb.get("open", 0)))
        if ep <= 0:
            continue
        qty = int(PER_SLOT / ep)
        if qty <= 0:
            continue
        t = Trade(code=code, name=nc.get(code, r.get("name", code)),
                  entry_time=eb["datetime"][11:16], entry_price=ep, qty=qty)
        bounce_target = ep * (1 + p.get("exit_bounce", 1.0) / 100)
        sl = ep * (1 + p.get("sl_pct", -3.0) / 100)
        for bar in bars[entry_idx + 2:]:
            bt = bar.get("datetime", "")[11:16]
            h, l = float(bar.get("high", 0)), float(bar.get("low", 0))
            if h > 0:
                t.mfe = max(t.mfe, (h / ep - 1) * 100)
            if l > 0:
                t.mae = min(t.mae, (l / ep - 1) * 100)
            if l > 0 and l <= sl:
                _close_trade(t, _apply_exit_slippage(sl, t.entry_price), bt, "SL")
                break
            if h >= bounce_target:
                _close_trade(t, _apply_exit_slippage(bounce_target, t.entry_price), bt, "BOUNCE")
                break
        if not t.exit_time and bars:
            eod = _apply_exit_slippage(float(bars[-1].get("close", 0)), t.entry_price)
            _close_trade(t, eod, bars[-1]["datetime"][11:16], "EOD")
        trades.append(t)
    return trades


# ══════════════════════════════════════════════════════════════════
#  Summary & Ranking
# ══════════════════════════════════════════════════════════════════

@dataclass
class SlotSummary:
    slot: int = 0
    name: str = ""
    description: str = ""
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    win_rate: float = 0.0
    total_pnl: float = 0.0
    total_pnl_pct: float = 0.0
    avg_pnl: float = 0.0
    avg_hold_min: float = 0.0
    max_win: float = 0.0
    max_loss: float = 0.0
    avg_mfe: float = 0.0
    avg_mae: float = 0.0
    profit_factor: float = 0.0
    sharpe_approx: float = 0.0


def calc_slot_summary(config: StrategyConfig, trades: List[Trade]) -> SlotSummary:
    s = SlotSummary(slot=config.slot, name=config.name, description=config.description)
    if not trades:
        return s
    s.total_trades = len(trades)
    gross_win = gross_loss = 0.0
    pnl_list = []
    for t in trades:
        pnl_list.append(t.pnl_pct)
        if t.pnl > 0:
            s.wins += 1
            gross_win += t.pnl
            s.max_win = max(s.max_win, t.pnl_pct)
        else:
            s.losses += 1
            gross_loss += abs(t.pnl)
            s.max_loss = min(s.max_loss, t.pnl_pct)
        s.avg_mfe += t.mfe
        s.avg_mae += t.mae
        s.avg_hold_min += t.hold_minutes
    s.win_rate = s.wins / s.total_trades * 100
    s.total_pnl = sum(t.pnl for t in trades)
    total_buy = sum(t.qty * t.entry_price for t in trades)
    s.total_pnl_pct = s.total_pnl / total_buy * 100 if total_buy > 0 else 0
    s.avg_pnl = s.total_pnl / s.total_trades
    s.avg_hold_min /= s.total_trades
    s.avg_mfe /= s.total_trades
    s.avg_mae /= s.total_trades
    s.profit_factor = gross_win / gross_loss if gross_loss > 0 else 999.0
    if len(pnl_list) > 1:
        import statistics
        mean = statistics.mean(pnl_list)
        std = statistics.stdev(pnl_list)
        s.sharpe_approx = mean / std if std > 0 else 0
    return s


def rank_slots(summaries: List[SlotSummary]) -> List[SlotSummary]:
    """Rank by composite score: 40% PnL + 30% Sharpe + 20% WinRate + 10% PF."""
    for s in summaries:
        if s.total_trades == 0:
            s._score = -999
        else:
            s._score = (
                0.40 * s.total_pnl_pct +
                0.30 * s.sharpe_approx * 10 +
                0.20 * (s.win_rate - 50) +
                0.10 * min(s.profit_factor, 10)
            )
    return sorted(summaries, key=lambda s: getattr(s, '_score', -999), reverse=True)


def generate_proposal(ranked: List[SlotSummary], date_str: str) -> str:
    """Generate text proposal for the user."""
    lines = [
        "=" * 55,
        f"  Q-TRON Strategy Lab Proposal  {date_str}",
        "=" * 55, "",
    ]
    for i, s in enumerate(ranked[:3], 1):
        medal = {1: "[1st]", 2: "[2nd]", 3: "[3rd]"}[i]
        lines.append(f"  {medal} Slot {s.slot}: {s.name}")
        lines.append(f"       {s.description}")
        lines.append(
            f"       Win {s.win_rate:.0f}% | PnL {s.total_pnl_pct:+.2f}% "
            f"| Sharpe {s.sharpe_approx:.2f} | PF {s.profit_factor:.1f}")
        lines.append(
            f"       Trades {s.total_trades} | Avg hold {s.avg_hold_min:.0f}min "
            f"| MFE {s.avg_mfe:.1f}% | MAE {s.avg_mae:.1f}%")
        lines.append("")

    if ranked and ranked[0].total_trades > 0:
        best = ranked[0]
        lines.append("  --- Recommendation ---")
        lines.append(f"  Slot {best.slot} ({best.name}) as primary strategy.")
        if len(ranked) > 2 and ranked[2].total_trades > 0:
            lines.append(f"  Slot {ranked[2].slot} ({ranked[2].name}) for risk management blend.")
        lines.append("")
    lines.append("=" * 55)
    return "\n".join(lines)


def save_results(ranked: List[SlotSummary], date_str: str):
    """Save daily results to CSV for trend tracking."""
    LAB_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path = LAB_RESULTS_DIR / f"lab_{date_str}.csv"
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["rank", "slot", "name", "trades", "win_rate", "pnl_pct",
                     "sharpe", "pf", "avg_hold_min", "avg_mfe", "avg_mae", "score"])
        for i, s in enumerate(ranked, 1):
            w.writerow([i, s.slot, s.name, s.total_trades,
                         f"{s.win_rate:.1f}", f"{s.total_pnl_pct:.2f}",
                         f"{s.sharpe_approx:.2f}", f"{s.profit_factor:.1f}",
                         f"{s.avg_hold_min:.0f}", f"{s.avg_mfe:.1f}",
                         f"{s.avg_mae:.1f}", f"{getattr(s, '_score', 0):.2f}"])


# ══════════════════════════════════════════════════════════════════
#  Aggregate: Multi-Day Cumulative Analysis
# ══════════════════════════════════════════════════════════════════

def load_all_lab_results() -> Dict[str, List[dict]]:
    """Load all saved lab CSVs. Returns {date_str: [row_dicts]}."""
    if not LAB_RESULTS_DIR.exists():
        return {}
    results = {}
    for f in sorted(LAB_RESULTS_DIR.glob("lab_*.csv")):
        date_str = f.stem.replace("lab_", "")
        try:
            with open(f, "r", encoding="utf-8") as fh:
                results[date_str] = list(csv.DictReader(fh))
        except Exception:
            pass
    return results


@dataclass
class AggSlot:
    """Aggregated slot summary across multiple days."""
    slot: int = 0
    name: str = ""
    days: int = 0
    total_trades: int = 0
    avg_win_rate: float = 0.0
    cumulative_pnl_pct: float = 0.0
    avg_daily_pnl_pct: float = 0.0
    avg_sharpe: float = 0.0
    avg_pf: float = 0.0
    first_place_count: int = 0
    top3_count: int = 0
    consistency: float = 0.0   # % of days with positive PnL
    avg_score: float = 0.0


def aggregate_results(all_results: Dict[str, List[dict]],
                      last_n_days: int = 0) -> List[AggSlot]:
    """Aggregate daily lab results into per-slot cumulative stats.
    last_n_days=0 means all days, 5=last week, 22=last month.
    """
    dates = sorted(all_results.keys())
    if last_n_days > 0:
        dates = dates[-last_n_days:]
    if not dates:
        return []

    slot_data: Dict[int, dict] = {}
    for dt in dates:
        rows = all_results.get(dt, [])
        for r in rows:
            try:
                slot = int(r["slot"])
                pnl = float(r["pnl_pct"])
                wr = float(r["win_rate"])
                sharpe = float(r["sharpe"])
                pf = float(r["pf"])
                trades = int(r["trades"])
                rank = int(r["rank"])
                score = float(r.get("score", 0))
            except (ValueError, KeyError):
                continue

            if slot not in slot_data:
                slot_data[slot] = {
                    "name": r.get("name", ""),
                    "pnl_list": [], "wr_list": [], "sharpe_list": [],
                    "pf_list": [], "trades_list": [], "rank_list": [],
                    "score_list": [],
                }
            d = slot_data[slot]
            d["pnl_list"].append(pnl)
            d["wr_list"].append(wr)
            d["sharpe_list"].append(sharpe)
            d["pf_list"].append(pf)
            d["trades_list"].append(trades)
            d["rank_list"].append(rank)
            d["score_list"].append(score)

    agg_list = []
    for slot, d in sorted(slot_data.items()):
        a = AggSlot(slot=slot, name=d["name"])
        a.days = len(d["pnl_list"])
        a.total_trades = sum(d["trades_list"])
        a.avg_win_rate = sum(d["wr_list"]) / a.days if a.days else 0
        a.cumulative_pnl_pct = sum(d["pnl_list"])
        a.avg_daily_pnl_pct = a.cumulative_pnl_pct / a.days if a.days else 0
        a.avg_sharpe = sum(d["sharpe_list"]) / a.days if a.days else 0
        a.avg_pf = sum(d["pf_list"]) / a.days if a.days else 0
        a.first_place_count = sum(1 for r in d["rank_list"] if r == 1)
        a.top3_count = sum(1 for r in d["rank_list"] if r <= 3)
        a.consistency = sum(1 for p in d["pnl_list"] if p > 0) / a.days * 100 if a.days else 0
        a.avg_score = sum(d["score_list"]) / a.days if a.days else 0
        agg_list.append(a)

    return sorted(agg_list, key=lambda a: a.avg_score, reverse=True)


def generate_aggregate_proposal(agg: List[AggSlot], period_label: str) -> str:
    """Generate cumulative proposal text."""
    lines = [
        "=" * 60,
        f"  Q-TRON Strategy Lab — {period_label} Cumulative Report",
        "=" * 60, "",
        f"  Period: {agg[0].days if agg else 0} trading days",
        "",
    ]
    for i, a in enumerate(agg[:5], 1):
        medal = {1: "[1st]", 2: "[2nd]", 3: "[3rd]", 4: "[4th]", 5: "[5th]"}[i]
        lines.append(f"  {medal} Slot {a.slot}: {a.name}")
        lines.append(
            f"       Cum PnL {a.cumulative_pnl_pct:+.2f}% | "
            f"Avg Daily {a.avg_daily_pnl_pct:+.2f}%")
        lines.append(
            f"       Win {a.avg_win_rate:.0f}% | Sharpe {a.avg_sharpe:.2f} | "
            f"PF {a.avg_pf:.1f}")
        lines.append(
            f"       1st: {a.first_place_count}/{a.days}d | "
            f"Top3: {a.top3_count}/{a.days}d | "
            f"Consistency: {a.consistency:.0f}%")
        lines.append(
            f"       Total trades: {a.total_trades}")
        lines.append("")

    if agg:
        best = agg[0]
        lines.append("  --- Cumulative Recommendation ---")
        if best.consistency >= 70:
            lines.append(f"  STRONG: Slot {best.slot} ({best.name})")
            lines.append(f"  {best.consistency:.0f}% consistency, "
                         f"{best.first_place_count}/{best.days} days ranked #1")
        elif best.consistency >= 50:
            lines.append(f"  MODERATE: Slot {best.slot} ({best.name})")
            lines.append(f"  Consider blending with Slot {agg[1].slot if len(agg) > 1 else '?'}")
        else:
            lines.append("  CAUTION: No strategy shows >50% consistency")
            lines.append("  Market conditions may be unfavorable for all strategies")
        lines.append("")
    lines.append("=" * 60)
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════
#  Full Pipeline
# ══════════════════════════════════════════════════════════════════

def run_lab(date_str: str, strategies: List[StrategyConfig] = None
            ) -> Tuple[List[SlotSummary], str]:
    """Run all 12 strategies on a day's data. Returns (ranked_summaries, proposal_text)."""
    if strategies is None:
        strategies = DEFAULT_STRATEGIES

    nc = load_name_cache()
    rankings = load_ranking(date_str)
    if not rankings:
        return [], f"No ranking data for {date_str}"

    results = {}
    for cfg in strategies:
        if cfg.category == "composite":
            continue
        trades = run_strategy(cfg, rankings, date_str, nc)
        results[cfg.slot] = (cfg, trades)

    # Composite: blend top-3 results (use their trade lists merged)
    for cfg in strategies:
        if cfg.category != "composite":
            continue
        summaries_so_far = [calc_slot_summary(c, t) for c, t in results.values()]
        top3 = rank_slots(summaries_so_far)[:cfg.params.get("blend_top_n", 3)]
        blended_trades = []
        for s in top3:
            if s.slot in results:
                blended_trades.extend(results[s.slot][1])
        results[cfg.slot] = (cfg, blended_trades)

    all_summaries = [calc_slot_summary(c, t) for c, t in results.values()]
    ranked = rank_slots(all_summaries)
    proposal = generate_proposal(ranked, date_str)
    save_results(ranked, date_str)
    return ranked, proposal


# ══════════════════════════════════════════════════════════════════
#  PyQt5 Widget (for embedding in v2)
# ══════════════════════════════════════════════════════════════════

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QFrame, QComboBox, QPushButton, QScrollArea,
    QTableWidget, QTableWidgetItem, QHeaderView, QSplitter,
    QTextEdit, QSizePolicy, QGridLayout,
)
from PyQt5.QtCore import QTimer, Qt
from PyQt5.QtGui import QColor, QFont

C_GREEN = "#10b981"
C_RED = "#ef4444"
C_YELLOW = "#f59e0b"
C_BLUE = "#3b82f6"
C_CYAN = "#06b6d4"
C_DIM = "#4a5568"
C_WHITE = "#e0e4ec"
BG_CARD = "rgba(14,17,24,180)"

CARD_STYLE = (
    f"background-color: {BG_CARD};"
    " border: 1px solid rgba(40,80,140,60);"
    " border-radius: 10px;"
)


class SlotPanel(QFrame):
    """One strategy slot result panel."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setStyleSheet(CARD_STYLE)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 6, 8, 6)
        layout.setSpacing(3)

        self._header = QLabel("")
        self._header.setStyleSheet(
            f"color: {C_CYAN}; font-size: 11px; font-weight: bold;"
            " font-family: Consolas; border: none;")
        layout.addWidget(self._header)

        self._desc = QLabel("")
        self._desc.setStyleSheet(
            f"color: {C_DIM}; font-size: 9px; font-family: Consolas; border: none;")
        layout.addWidget(self._desc)

        # KPIs in grid
        self._kpi_grid = QGridLayout()
        self._kpi_grid.setSpacing(2)
        self._kpis = {}
        for i, (key, label) in enumerate([
            ("trades", "Trades"), ("win", "Win%"),
            ("pnl", "PnL"), ("pnl_pct", "PnL%"),
            ("sharpe", "Sharpe"), ("pf", "PF"),
            ("hold", "Avg Hold"), ("mfe", "MFE"),
        ]):
            row, col = divmod(i, 4)
            lbl = QLabel(label)
            lbl.setStyleSheet(
                f"color: {C_DIM}; font-size: 7px; font-family: Consolas; border: none;")
            self._kpi_grid.addWidget(lbl, row * 2, col)
            val = QLabel("-")
            val.setStyleSheet(
                f"color: {C_WHITE}; font-size: 11px; font-weight: bold;"
                " font-family: Consolas; border: none;")
            self._kpi_grid.addWidget(val, row * 2 + 1, col)
            self._kpis[key] = val
        layout.addLayout(self._kpi_grid)

        # Rank badge
        self._rank = QLabel("")
        self._rank.setStyleSheet(
            f"color: {C_YELLOW}; font-size: 14px; font-weight: bold;"
            " font-family: Consolas; border: none;")
        self._rank.setAlignment(Qt.AlignRight)
        layout.addWidget(self._rank)

    def set_result(self, summary: SlotSummary, rank: int = 0):
        self._header.setText(f"#{summary.slot} {summary.name}")
        self._desc.setText(summary.description)

        self._kpis["trades"].setText(str(summary.total_trades))
        wr_c = C_GREEN if summary.win_rate >= 60 else (C_YELLOW if summary.win_rate >= 50 else C_RED)
        self._kpis["win"].setText(f"{summary.win_rate:.0f}%")
        self._kpis["win"].setStyleSheet(
            f"color: {wr_c}; font-size: 11px; font-weight: bold;"
            " font-family: Consolas; border: none;")

        pc = C_GREEN if summary.total_pnl >= 0 else C_RED
        self._kpis["pnl"].setText(f"{summary.total_pnl / 10000:+,.0f}만")
        self._kpis["pnl"].setStyleSheet(
            f"color: {pc}; font-size: 11px; font-weight: bold;"
            " font-family: Consolas; border: none;")
        self._kpis["pnl_pct"].setText(f"{summary.total_pnl_pct:+.2f}%")
        self._kpis["pnl_pct"].setStyleSheet(
            f"color: {pc}; font-size: 11px; font-weight: bold;"
            " font-family: Consolas; border: none;")

        self._kpis["sharpe"].setText(f"{summary.sharpe_approx:.2f}")
        self._kpis["pf"].setText(f"{summary.profit_factor:.1f}")
        self._kpis["hold"].setText(f"{summary.avg_hold_min:.0f}m")
        self._kpis["mfe"].setText(f"{summary.avg_mfe:.1f}%")

        if rank > 0:
            medals = {1: "1st", 2: "2nd", 3: "3rd"}
            self._rank.setText(medals.get(rank, f"#{rank}"))
            if rank == 1:
                self.setStyleSheet(
                    "background-color: rgba(16,185,129,25);"
                    " border: 1px solid rgba(16,185,129,80);"
                    " border-radius: 10px;")
            elif rank <= 3:
                self.setStyleSheet(
                    "background-color: rgba(59,130,246,15);"
                    " border: 1px solid rgba(59,130,246,50);"
                    " border-radius: 10px;")
            else:
                self.setStyleSheet(CARD_STYLE)
        else:
            self._rank.setText("")

    def clear(self):
        self._header.setText("")
        self._desc.setText("")
        self._rank.setText("")
        for v in self._kpis.values():
            v.setText("-")


class StrategyLabWidget(QWidget):
    """12-slot strategy lab widget for embedding in v2 as 4th dock."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setStyleSheet("background: transparent; border: none;")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(4, 4, 4, 4)
        layout.setSpacing(4)

        # Controls
        ctrl = QHBoxLayout()
        title = QLabel("STRATEGY LAB")
        title.setStyleSheet(
            f"color: {C_CYAN}; font-size: 12px; font-weight: bold;"
            " font-family: Consolas; border: none;")
        ctrl.addWidget(title)
        ctrl.addStretch()

        self._date_combo = QComboBox()
        self._date_combo.setFixedWidth(110)
        ctrl.addWidget(self._date_combo)

        self._run_btn = QPushButton("Run Lab")
        self._run_btn.setFixedWidth(80)
        self._run_btn.setStyleSheet(
            "background: rgba(30,50,90,200); color: #e0e4ec;"
            " border: 1px solid rgba(40,80,140,100); border-radius: 4px;"
            " padding: 4px 10px; font-size: 11px; font-weight: bold;")
        self._run_btn.clicked.connect(self._run)
        ctrl.addWidget(self._run_btn)

        # Period selector for cumulative view
        self._period_combo = QComboBox()
        self._period_combo.addItems(["1 Day", "1 Week (5d)", "1 Month (22d)", "All"])
        self._period_combo.setFixedWidth(120)
        self._period_combo.currentTextChanged.connect(self._update_cumulative)
        ctrl.addWidget(self._period_combo)

        self._status = QLabel("")
        self._status.setStyleSheet(
            f"color: {C_DIM}; font-size: 9px; font-family: Consolas; border: none;")
        ctrl.addWidget(self._status)
        layout.addLayout(ctrl)

        # Main area: tab switch between Strategies grid and Proposal
        from PyQt5.QtWidgets import QTabWidget
        self._main_tabs = QTabWidget()

        # Tab 1: 4x3 grid of slot panels
        grid_widget = QWidget()
        grid_layout = QGridLayout(grid_widget)
        grid_layout.setSpacing(4)
        grid_layout.setContentsMargins(0, 0, 0, 0)
        self._panels = []
        for i in range(12):
            panel = SlotPanel()
            row, col = divmod(i, 4)
            grid_layout.addWidget(panel, row, col)
            self._panels.append(panel)
        self._main_tabs.addTab(grid_widget, "Strategies")

        # Tab 2: Proposal (Daily + Cumulative sub-tabs)
        self._proposal_tabs = QTabWidget()

        _prop_style = (
            f"background-color: rgba(14,17,24,200); color: {C_WHITE};"
            " border: 1px solid rgba(40,80,140,60); border-radius: 6px;"
            " font-family: Consolas; font-size: 11px; padding: 8px;")

        self._proposal_daily = QTextEdit()
        self._proposal_daily.setReadOnly(True)
        self._proposal_daily.setStyleSheet(_prop_style)
        self._proposal_tabs.addTab(self._proposal_daily, "Daily")

        self._proposal_cumul = QTextEdit()
        self._proposal_cumul.setReadOnly(True)
        self._proposal_cumul.setStyleSheet(_prop_style)
        self._proposal_tabs.addTab(self._proposal_cumul, "Cumulative")

        # Backward compat alias
        self._proposal = self._proposal_daily

        self._main_tabs.addTab(self._proposal_tabs, "Proposal")

        layout.addWidget(self._main_tabs)

        try:
            self.refresh_dates()
            if self._date_combo.currentText() and self._date_combo.currentText() != "(no data)":
                self._run()
        except Exception as e:
            print(f"[StrategyLab] init auto-run failed (non-critical): {e}")

    def refresh_dates(self):
        dates = available_dates()
        current = self._date_combo.currentText()
        self._date_combo.clear()
        if dates:
            self._date_combo.addItems(dates)
            today = date.today().strftime("%Y%m%d")
            if today in dates:
                self._date_combo.setCurrentText(today)
            elif current in dates:
                self._date_combo.setCurrentText(current)
        else:
            self._date_combo.addItem("(no data)")

    def _run(self):
        date_str = self._date_combo.currentText()
        if not date_str or date_str == "(no data)":
            self._status.setText("No data")
            return

        self._status.setText(f"Running {date_str}...")
        from PyQt5.QtWidgets import QApplication
        QApplication.processEvents()

        ranked, proposal = run_lab(date_str)

        if not ranked:
            self._status.setText(proposal)
            for p in self._panels:
                p.clear()
            self._proposal.setPlainText(proposal)
            return

        # Map ranked results to panels by slot number
        slot_to_rank = {s.slot: (i + 1, s) for i, s in enumerate(ranked)}
        for i, panel in enumerate(self._panels):
            slot_num = i + 1
            if slot_num in slot_to_rank:
                rank, summary = slot_to_rank[slot_num]
                panel.set_result(summary, rank)
            else:
                panel.clear()

        self._proposal.setPlainText(proposal)

        # Status
        best = ranked[0] if ranked else None
        if best and best.total_trades > 0:
            self._status.setText(
                f"{date_str} | Best: #{best.slot} {best.name} "
                f"({best.total_pnl_pct:+.2f}%)")
        else:
            self._status.setText(f"{date_str} | No trades")

        # Auto-update cumulative after daily run
        self._update_cumulative()

    def _update_cumulative(self):
        """Load all saved lab results and generate cumulative proposal."""
        all_results = load_all_lab_results()
        if not all_results:
            self._proposal_cumul.setPlainText("No cumulative data yet.\nRun daily labs to accumulate.")
            return

        period = self._period_combo.currentText()
        if "Week" in period:
            n_days = 5
            label = "Weekly (5 days)"
        elif "Month" in period:
            n_days = 22
            label = "Monthly (22 days)"
        elif "All" in period:
            n_days = 0
            label = f"All ({len(all_results)} days)"
        else:
            n_days = 1
            label = "Latest Day"

        agg = aggregate_results(all_results, last_n_days=n_days)
        if not agg:
            self._proposal_cumul.setPlainText(f"No data for {label}")
            return

        proposal = generate_aggregate_proposal(agg, label)
        self._proposal_cumul.setPlainText(proposal)


# ══════════════════════════════════════════════════════════════════
#  Standalone Entry Point
# ══════════════════════════════════════════════════════════════════

class StrategyLabWindow(QMainWindow):
    def __init__(self, initial_date=None):
        super().__init__()
        self.setWindowTitle("Q-TRON Strategy Lab")
        self.resize(1400, 700)
        self.setStyleSheet(f"""
            QMainWindow {{ background-color: #0a0e14; }}
            QLabel {{ color: #e0e4ec; }}
            QComboBox {{ background: #0e1118; color: #e0e4ec;
                border: 1px solid rgba(40,80,140,60); border-radius: 4px;
                padding: 3px 8px; font-size: 11px; }}
            QComboBox::drop-down {{ border: none; }}
            QComboBox QAbstractItemView {{ background: #0e1118; color: #e0e4ec; }}
            QTextEdit {{ background: #0e1118; color: #e0e4ec; }}
        """)
        self._lab = StrategyLabWidget()
        self.setCentralWidget(self._lab)
        if initial_date:
            self._lab._date_combo.setCurrentText(initial_date)
            self._lab._run()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Q-TRON Strategy Lab")
    parser.add_argument("--date", default=None)
    args = parser.parse_args()

    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    win = StrategyLabWindow(args.date)
    win.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
