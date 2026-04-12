"""
Swing Simulator GUI — 3-Window TP/SL Replay Calculator
========================================================
Reads collected ranking + minute bar + microstructure CSV data,
runs 3 parallel simulations with different TP/SL strategies,
and displays results side-by-side.

100% READ-ONLY. No orders, no engine writes. CSV replay only.

Usage:
    python swing_simulator_gui.py
    python swing_simulator_gui.py --date 20260404
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import os
from dataclasses import dataclass, field
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QFrame, QComboBox, QPushButton, QScrollArea,
    QTableWidget, QTableWidgetItem, QHeaderView, QSplitter,
    QSizePolicy,
)
from PyQt5.QtCore import QTimer, Qt
from PyQt5.QtGui import QColor, QPalette, QFont

# =========================================================================
#  Paths & Constants
# =========================================================================
BASE_DIR = Path(__file__).resolve().parent
SWING_DIR = BASE_DIR / "data" / "swing" / "ranking"
INTRADAY_DIR = BASE_DIR / "data" / "intraday"
MICRO_DIR = BASE_DIR / "data" / "micro"
NAME_CACHE_PATH = BASE_DIR / "data" / "stock_name_cache.json"

# Cost model (Kiwoom)
FEE_RATE = 0.00015      # 0.015% per side
TAX_RATE = 0.0018        # 0.18% sell only
SLIPPAGE_BUY = 0.001     # +0.1%
SLIPPAGE_SELL = 0.001    # -0.1%
TOTAL_BUY_COST = FEE_RATE + SLIPPAGE_BUY     # ~0.115%
TOTAL_SELL_COST = FEE_RATE + TAX_RATE + SLIPPAGE_SELL  # ~0.295%

# FIX-003: Minimum bars required after entry for meaningful simulation.
# At 1-min bars, 30 = 30 minutes of tradeable data.
MIN_ENTRY_BARS = 30

INITIAL_CAPITAL = 100_000_000  # 1억
SLOTS = 20
PER_SLOT = INITIAL_CAPITAL // SLOTS  # 500만

# Colors
C_GREEN = "#00ff88"
C_RED = "#ff3344"
C_YELLOW = "#ffdd00"
C_BLUE = "#6699ff"
C_CYAN = "#44cccc"
C_ORANGE = "#ff8800"
C_DIM = "#606880"
C_WHITE = "#f0f0f0"
BG_MAIN = "#060810"
BG_CARD = "#0e1118"
BG_HOVER = "#161a24"
C_TEXT_PRIMARY = "#e8eaef"
C_TEXT_SECONDARY = "#7a8494"

# =========================================================================
#  Data Loaders
# =========================================================================
def load_name_cache() -> dict:
    if NAME_CACHE_PATH.exists():
        try:
            return json.loads(NAME_CACHE_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def load_ranking(date_str: str) -> List[dict]:
    """Load ranking snapshots: [{snapshot_time, rank, code, name, price, change_pct}]"""
    path = SWING_DIR / f"{date_str}.csv"
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []

def load_minute_bars(code: str, date_str: str) -> List[dict]:
    """Load 1-min bars: [{datetime, open, high, low, close, volume, status}]"""
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
    """Load 5-sec micro samples."""
    path = MICRO_DIR / f"{code}_{date_str}.csv"
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []

def available_dates() -> List[str]:
    """List available ranking dates."""
    if not SWING_DIR.exists():
        return []
    dates = sorted([f.stem for f in SWING_DIR.glob("*.csv")], reverse=True)
    return dates


# =========================================================================
#  Trade Record
# =========================================================================
@dataclass
class Trade:
    code: str
    name: str
    entry_time: str
    entry_price: float
    exit_time: str = ""
    exit_price: float = 0.0
    exit_reason: str = ""  # TP / SL / HARD_STOP / BREAKDOWN / EOD
    qty: int = 0
    buy_amount: float = 0.0
    sell_amount: float = 0.0
    buy_fee: float = 0.0
    sell_fee: float = 0.0
    sell_tax: float = 0.0
    pnl: float = 0.0
    pnl_pct: float = 0.0
    mfe: float = 0.0  # max favorable excursion %


# =========================================================================
#  Simulation Engines
# =========================================================================
def run_fixed_strategy(rankings: List[dict], date_str: str,
                        name_cache: dict,
                        tp_pct: float, sl_pct: float) -> List[Trade]:
    """Window A/B: Fixed TP/SL strategy."""
    trades = []
    active: Dict[str, Trade] = {}
    exited_codes: set = set()

    # Group rankings by snapshot_time
    snapshots = {}
    for r in rankings:
        t = r.get("snapshot_time", "")
        snapshots.setdefault(t, []).append(r)

    sorted_times = sorted(snapshots.keys())
    if not sorted_times:
        return []

    # Process each snapshot
    for snap_time in sorted_times:
        ranked_codes = [r["code"] for r in snapshots[snap_time]]

        # Fill empty slots with new codes
        for r in snapshots[snap_time]:
            code = r["code"]
            if code in active or code in exited_codes:
                continue
            if len(active) >= SLOTS:
                break
            # Entry: next minute bar open
            bars = load_minute_bars(code, date_str)
            if not bars:
                continue

            # Find first bar after snapshot time
            entry_bar = None
            for bar in bars:
                bar_time = bar.get("datetime", "")[11:16]
                if bar_time > snap_time[:5]:
                    entry_bar = bar
                    break
            if not entry_bar:
                continue
            # FIX-003: skip if insufficient bars for meaningful simulation
            entry_dt = entry_bar.get("datetime", "")[11:16]
            bars_after = [b for b in bars if b.get("datetime", "")[11:16] > entry_dt]
            if len(bars_after) < MIN_ENTRY_BARS:
                continue

            entry_price = float(entry_bar["open"]) * (1 + SLIPPAGE_BUY)
            qty = int(PER_SLOT / entry_price)
            if qty <= 0:
                continue
            buy_amount = qty * entry_price
            buy_fee = buy_amount * FEE_RATE

            trade = Trade(
                code=code,
                name=name_cache.get(code, r.get("name", code)),
                entry_time=entry_bar["datetime"][11:16],
                entry_price=entry_price,
                qty=qty,
                buy_amount=buy_amount,
                buy_fee=buy_fee,
            )
            active[code] = trade

    # Now check TP/SL on all active trades using minute bars
    all_active = dict(active)
    active.clear()

    for code, trade in all_active.items():
        bars = load_minute_bars(code, date_str)
        exited = False
        for bar in bars:
            bar_time = bar.get("datetime", "")[11:16]
            if bar_time <= trade.entry_time:
                continue
            high = float(bar.get("high", 0))
            low = float(bar.get("low", 0))
            close_price = float(bar.get("close", 0))

            tp_price = trade.entry_price * (1 + tp_pct / 100)
            sl_price = trade.entry_price * (1 + sl_pct / 100)

            # Track MFE
            if high > 0:
                mfe = (high / trade.entry_price - 1) * 100
                trade.mfe = max(trade.mfe, mfe)

            # SL first (conservative)
            if low <= sl_price and sl_pct < 0:
                _close_trade(trade, sl_price, bar_time, "SL")
                exited = True
                break
            # TP
            if high >= tp_price:
                _close_trade(trade, tp_price, bar_time, "TP")
                exited = True
                break

        if not exited:
            # EOD close
            last_bar = bars[-1] if bars else None
            if last_bar:
                eod_price = float(last_bar["close"]) * (1 - SLIPPAGE_SELL)
                _close_trade(trade, eod_price,
                             last_bar["datetime"][11:16], "EOD")

        trades.append(trade)
        exited_codes.add(code)

    return trades


def run_dynamic_strategy(rankings: List[dict], date_str: str,
                          name_cache: dict) -> List[Trade]:
    """Window C: Dynamic TP with micro-based trailing."""
    # Parameters
    INIT_TP = 1.5
    BASE_SL = -1.0
    HARD_STOP = -3.0
    TP_MAX = 6.0
    TP_STEP = 0.75
    IMBALANCE_UP = 1.5
    IMBALANCE_DOWN = 0.8
    CONSEC_REQUIRED = 2

    trades = []
    exited_codes: set = set()

    # Get first snapshot codes
    snapshots = {}
    for r in rankings:
        t = r.get("snapshot_time", "")
        snapshots.setdefault(t, []).append(r)

    sorted_times = sorted(snapshots.keys())
    if not sorted_times:
        return []

    # Collect all entry candidates from all snapshots
    all_entries = []
    seen = set()
    for snap_time in sorted_times:
        for r in snapshots[snap_time]:
            code = r["code"]
            if code not in seen:
                seen.add(code)
                all_entries.append((snap_time, r))

    # Process each entry
    for snap_time, r in all_entries:
        code = r["code"]
        if code in exited_codes:
            continue
        if len(trades) >= SLOTS:
            break

        bars = load_minute_bars(code, date_str)
        micro = load_micro_samples(code, date_str)
        if not bars:
            continue

        # Find entry bar
        entry_bar = None
        for bar in bars:
            bar_time = bar.get("datetime", "")[11:16]
            if bar_time > snap_time[:5]:
                entry_bar = bar
                break
        if not entry_bar:
            continue
        # FIX-003: skip if insufficient bars for meaningful simulation
        _edt = entry_bar.get("datetime", "")[11:16]
        _bars_after = [b for b in bars if b.get("datetime", "")[11:16] > _edt]
        if len(_bars_after) < MIN_ENTRY_BARS:
            continue

        entry_price = float(entry_bar["open"]) * (1 + SLIPPAGE_BUY)
        qty = int(PER_SLOT / entry_price)
        if qty <= 0:
            continue
        buy_amount = qty * entry_price
        buy_fee = buy_amount * FEE_RATE

        trade = Trade(
            code=code,
            name=name_cache.get(code, r.get("name", code)),
            entry_time=entry_bar["datetime"][11:16],
            entry_price=entry_price,
            qty=qty,
            buy_amount=buy_amount,
            buy_fee=buy_fee,
        )

        # Build micro time series for imbalance
        micro_by_time = {}
        for m in micro:
            ts = m.get("timestamp", "")[:8]  # HH:MM:SS
            try:
                total_ask = float(m.get("total_ask", 1))
                total_bid = float(m.get("total_bid", 0))
                price = float(m.get("price", 0))
                imbalance = total_bid / total_ask if total_ask > 0 else 1.0
                micro_by_time[ts] = {"imbalance": imbalance, "price": price}
            except (ValueError, TypeError):
                pass

        # Simulate with dynamic TP
        current_tp = INIT_TP
        mfe = 0.0
        imb_up_consec = 0
        imb_down_consec = 0
        prev_micro_price = 0
        exited = False

        for bar in bars:
            bar_time = bar.get("datetime", "")[11:16]
            if bar_time <= trade.entry_time:
                continue

            high = float(bar.get("high", 0))
            low = float(bar.get("low", 0))
            close_p = float(bar.get("close", 0))
            current_pct = (close_p / entry_price - 1) * 100

            # MFE tracking
            if high > 0:
                high_pct = (high / entry_price - 1) * 100
                mfe = max(mfe, high_pct)
                trade.mfe = mfe

            # Trailing SL
            trailing_sl = max(BASE_SL, mfe * 0.5 - 0.2)
            effective_sl = max(trailing_sl, HARD_STOP)

            sl_price = entry_price * (1 + effective_sl / 100)
            tp_price = entry_price * (1 + current_tp / 100)
            hard_price = entry_price * (1 + HARD_STOP / 100)

            # HARD STOP
            if low <= hard_price:
                _close_trade(trade, hard_price, bar_time, "HARD_STOP")
                exited = True
                break

            # Trailing SL
            if low <= sl_price and effective_sl < 0:
                _close_trade(trade, sl_price, bar_time, "SL")
                exited = True
                break

            # TP
            if high >= tp_price:
                _close_trade(trade, tp_price, bar_time, "TP")
                exited = True
                break

            # Check micro for imbalance signals
            bar_time_sec = bar_time + ":00"
            # Check multiple micro samples within this minute
            for sec in range(0, 60, 5):
                micro_ts = f"{bar_time}:{sec:02d}"
                md = micro_by_time.get(micro_ts)
                if not md:
                    continue
                imb = md["imbalance"]
                mp = md["price"]

                # TP upgrade: 2 consecutive imbalance > 1.5
                if imb > IMBALANCE_UP:
                    imb_up_consec += 1
                    if imb_up_consec >= CONSEC_REQUIRED and current_tp < TP_MAX:
                        current_tp = min(current_tp + TP_STEP, TP_MAX)
                        imb_up_consec = 0
                else:
                    imb_up_consec = 0

                # Breakdown: 2 consecutive imbalance < 0.8 + price dropping
                if imb < IMBALANCE_DOWN:
                    imb_down_consec += 1
                    if imb_down_consec >= CONSEC_REQUIRED and prev_micro_price > 0 and mp < prev_micro_price:
                        _close_trade(trade, mp * (1 - SLIPPAGE_SELL), bar_time, "BREAKDOWN")
                        exited = True
                        break
                else:
                    imb_down_consec = 0
                prev_micro_price = mp

            if exited:
                break

        if not exited:
            last_bar = bars[-1] if bars else None
            if last_bar:
                eod_price = float(last_bar["close"]) * (1 - SLIPPAGE_SELL)
                _close_trade(trade, eod_price,
                             last_bar["datetime"][11:16], "EOD")

        trades.append(trade)
        exited_codes.add(code)

    return trades


def _close_trade(trade: Trade, exit_price: float, exit_time: str, reason: str):
    """Calculate exit costs and PnL."""
    trade.exit_price = exit_price
    trade.exit_time = exit_time
    trade.exit_reason = reason
    trade.sell_amount = trade.qty * exit_price
    trade.sell_fee = trade.sell_amount * FEE_RATE
    trade.sell_tax = trade.sell_amount * TAX_RATE
    trade.pnl = (trade.sell_amount - trade.buy_amount
                 - trade.buy_fee - trade.sell_fee - trade.sell_tax)
    trade.pnl_pct = trade.pnl / trade.buy_amount * 100 if trade.buy_amount > 0 else 0


# =========================================================================
#  Summary Stats
# =========================================================================
@dataclass
class SimSummary:
    total_trades: int = 0
    tp_count: int = 0
    sl_count: int = 0
    hard_stop_count: int = 0
    breakdown_count: int = 0
    eod_count: int = 0
    win_rate: float = 0.0
    total_buy: float = 0.0
    total_sell: float = 0.0
    total_fee: float = 0.0
    total_tax: float = 0.0
    total_pnl: float = 0.0
    total_pnl_pct: float = 0.0
    avg_pnl: float = 0.0
    avg_mfe: float = 0.0

def calc_summary(trades: List[Trade]) -> SimSummary:
    s = SimSummary()
    if not trades:
        return s
    s.total_trades = len(trades)
    wins = 0
    for t in trades:
        if t.exit_reason == "TP":
            s.tp_count += 1
        elif t.exit_reason == "SL":
            s.sl_count += 1
        elif t.exit_reason == "HARD_STOP":
            s.hard_stop_count += 1
        elif t.exit_reason == "BREAKDOWN":
            s.breakdown_count += 1
        else:
            s.eod_count += 1
        if t.pnl > 0:
            wins += 1
        s.total_buy += t.buy_amount
        s.total_sell += t.sell_amount
        s.total_fee += t.buy_fee + t.sell_fee
        s.total_tax += t.sell_tax
        s.total_pnl += t.pnl
        s.avg_mfe += t.mfe
    s.win_rate = wins / s.total_trades * 100 if s.total_trades > 0 else 0
    s.total_pnl_pct = s.total_pnl / s.total_buy * 100 if s.total_buy > 0 else 0
    s.avg_pnl = s.total_pnl / s.total_trades if s.total_trades > 0 else 0
    s.avg_mfe = s.avg_mfe / s.total_trades if s.total_trades > 0 else 0
    return s


# =========================================================================
#  GUI Theme
# =========================================================================
STYLE = f"""
QMainWindow {{ background-color: {BG_MAIN}; }}
QLabel {{ color: {C_TEXT_PRIMARY}; }}
QFrame {{ color: {C_TEXT_PRIMARY}; }}
QTableWidget {{
    background-color: rgba(12,14,21,180); color: {C_TEXT_PRIMARY};
    gridline-color: rgba(28,32,48,120); border: 1px solid rgba(40,60,100,80);
    border-radius: 6px; font-size: 11px;
}}
QTableWidget::item {{ padding: 3px 6px; }}
QHeaderView::section {{
    background-color: rgba(11,13,20,200); color: {C_DIM};
    border: none; border-bottom: 1px solid rgba(42,48,80,150);
    padding: 4px 6px; font-weight: bold; font-size: 10px;
}}
QComboBox {{
    background-color: rgba(12,14,21,200); color: {C_TEXT_PRIMARY};
    border: 1px solid rgba(42,48,80,150); border-radius: 4px;
    padding: 3px 8px; font-size: 12px;
}}
QComboBox::drop-down {{ border: none; }}
QComboBox QAbstractItemView {{
    background-color: rgba(14,17,24,240); color: {C_TEXT_PRIMARY};
}}
QPushButton {{
    background-color: rgba(30,50,90,200); color: {C_WHITE};
    border: 1px solid rgba(40,80,140,100); border-radius: 6px;
    padding: 6px 16px; font-size: 12px; font-weight: bold;
}}
QPushButton:hover {{ background-color: rgba(40,70,120,220); }}
QScrollArea {{ border: none; background: transparent; }}
QScrollBar:vertical {{
    background: transparent; width: 8px; border: none;
}}
QScrollBar::handle:vertical {{
    background: rgba(42,48,80,180); border-radius: 4px; min-height: 30px;
}}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height: 0; }}
"""

CARD_STYLE = (
    "background-color: rgba(14,17,24,180);"
    " border: 1px solid rgba(40,80,140,60);"
    " border-radius: 10px;"
)


# =========================================================================
#  SimWindow — one strategy panel
# =========================================================================
class SimWindow(QFrame):
    def __init__(self, title: str, desc: str, accent: str, parent=None):
        super().__init__(parent)
        self.setStyleSheet(CARD_STYLE)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 8, 12, 8)
        layout.setSpacing(6)

        # Header
        hdr = QHBoxLayout()
        lbl = QLabel(title)
        lbl.setStyleSheet(f"color: {accent}; font-size: 14px; font-weight: bold;"
                          f" font-family: Consolas; border: none;")
        hdr.addWidget(lbl)
        hdr.addStretch()
        self._desc = QLabel(desc)
        self._desc.setStyleSheet(f"color: {C_DIM}; font-size: 10px; font-family: Consolas; border: none;")
        hdr.addWidget(self._desc)
        layout.addLayout(hdr)

        # Summary KPIs
        self._kpi_frame = QFrame()
        self._kpi_frame.setStyleSheet("border: none; background: transparent;")
        kpi_layout = QGridLayout(self._kpi_frame)
        kpi_layout.setContentsMargins(0, 0, 0, 0)
        kpi_layout.setSpacing(4)

        self._kpi_labels = {}
        kpis = [
            ("trades", "Trades"), ("win_rate", "Win Rate"),
            ("pnl", "PnL"), ("pnl_pct", "PnL %"),
            ("tp", "TP"), ("sl", "SL"),
            ("fee_tax", "Fee+Tax"), ("avg_mfe", "Avg MFE"),
        ]
        for i, (key, label) in enumerate(kpis):
            row, col = i // 4, i % 4
            lbl = QLabel(label)
            lbl.setStyleSheet(f"color: {C_DIM}; font-size: 9px; font-family: Consolas; border: none;")
            kpi_layout.addWidget(lbl, row * 2, col)
            val = QLabel("-")
            val.setStyleSheet(f"color: {C_WHITE}; font-size: 14px; font-weight: bold;"
                              f" font-family: Consolas; border: none;")
            kpi_layout.addWidget(val, row * 2 + 1, col)
            self._kpi_labels[key] = val
        layout.addWidget(self._kpi_frame)

        # Trade table
        self._table = QTableWidget()
        self._table.setColumnCount(8)
        self._table.setHorizontalHeaderLabels(
            ["Time", "Name", "Entry", "Exit", "Reason", "Qty", "PnL", "PnL%"])
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.verticalHeader().setVisible(False)
        self._table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._table.setSortingEnabled(True)
        layout.addWidget(self._table)

        self._accent = accent

    def update_results(self, trades: List[Trade], summary: SimSummary):
        # KPIs
        self._kpi_labels["trades"].setText(str(summary.total_trades))
        wr_clr = C_GREEN if summary.win_rate >= 60 else (C_YELLOW if summary.win_rate >= 50 else C_RED)
        self._kpi_labels["win_rate"].setText(f"{summary.win_rate:.0f}%")
        self._kpi_labels["win_rate"].setStyleSheet(
            f"color: {wr_clr}; font-size: 14px; font-weight: bold; font-family: Consolas; border: none;")

        pnl_clr = C_GREEN if summary.total_pnl >= 0 else C_RED
        self._kpi_labels["pnl"].setText(f"{summary.total_pnl:+,.0f}")
        self._kpi_labels["pnl"].setStyleSheet(
            f"color: {pnl_clr}; font-size: 14px; font-weight: bold; font-family: Consolas; border: none;")
        self._kpi_labels["pnl_pct"].setText(f"{summary.total_pnl_pct:+.2f}%")
        self._kpi_labels["pnl_pct"].setStyleSheet(
            f"color: {pnl_clr}; font-size: 14px; font-weight: bold; font-family: Consolas; border: none;")

        self._kpi_labels["tp"].setText(f"{summary.tp_count}")
        self._kpi_labels["tp"].setStyleSheet(
            f"color: {C_GREEN}; font-size: 14px; font-weight: bold; font-family: Consolas; border: none;")
        sl_total = summary.sl_count + summary.hard_stop_count + summary.breakdown_count
        self._kpi_labels["sl"].setText(f"{sl_total}")
        self._kpi_labels["sl"].setStyleSheet(
            f"color: {C_RED}; font-size: 14px; font-weight: bold; font-family: Consolas; border: none;")

        self._kpi_labels["fee_tax"].setText(f"{summary.total_fee + summary.total_tax:,.0f}")
        self._kpi_labels["avg_mfe"].setText(f"{summary.avg_mfe:.1f}%")

        # Table
        self._table.setSortingEnabled(False)
        self._table.setRowCount(len(trades))
        for i, t in enumerate(trades):
            items = [
                f"{t.entry_time}→{t.exit_time}",
                f"{t.name[:6]}",
                f"{t.entry_price:,.0f}",
                f"{t.exit_price:,.0f}",
                t.exit_reason,
                str(t.qty),
                f"{t.pnl:+,.0f}",
                f"{t.pnl_pct:+.2f}%",
            ]
            for j, val in enumerate(items):
                item = QTableWidgetItem(val)
                item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                if j == 4:  # Reason
                    clr = {
                        "TP": QColor(C_GREEN), "SL": QColor(C_RED),
                        "HARD_STOP": QColor(C_RED), "BREAKDOWN": QColor(C_ORANGE),
                        "EOD": QColor(C_DIM),
                    }.get(t.exit_reason, QColor(C_WHITE))
                    item.setForeground(clr)
                    item.setFont(QFont("Consolas", 10, QFont.Bold))
                elif j in (6, 7):  # PnL
                    clr = QColor(C_GREEN) if t.pnl >= 0 else QColor(C_RED)
                    item.setForeground(clr)
                    item.setFont(QFont("Consolas", 10, QFont.Bold))
                self._table.setItem(i, j, item)
        self._table.setSortingEnabled(True)

    def show_no_data(self):
        self._table.setRowCount(0)
        for key in self._kpi_labels:
            self._kpi_labels[key].setText("-")


# =========================================================================
#  Main Window
# =========================================================================
class SwingSimulatorGUI(QMainWindow):
    def __init__(self, initial_date=None):
        super().__init__()
        self.setWindowTitle("Q-TRON Swing Simulator")
        self.resize(1400, 800)
        self.setStyleSheet(STYLE)

        self._name_cache = load_name_cache()

        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(8, 8, 8, 8)
        main_layout.setSpacing(8)

        # Control bar
        ctrl = QHBoxLayout()
        title = QLabel("SWING SIMULATOR")
        title.setStyleSheet(f"color: {C_CYAN}; font-size: 16px; font-weight: bold; font-family: Consolas;")
        ctrl.addWidget(title)
        ctrl.addStretch()

        ctrl.addWidget(QLabel("Date:"))
        self._date_combo = QComboBox()
        dates = available_dates()
        if initial_date and initial_date in dates:
            self._date_combo.addItems(dates)
            self._date_combo.setCurrentText(initial_date)
        elif dates:
            self._date_combo.addItems(dates)
        else:
            self._date_combo.addItem("(no data)")
        self._date_combo.setFixedWidth(120)
        ctrl.addWidget(self._date_combo)

        self._run_btn = QPushButton("Run")
        self._run_btn.clicked.connect(self._run_simulation)
        ctrl.addWidget(self._run_btn)

        self._status = QLabel("")
        self._status.setStyleSheet(f"color: {C_DIM}; font-size: 10px; font-family: Consolas;")
        ctrl.addWidget(self._status)
        main_layout.addLayout(ctrl)

        # 3 windows
        splitter = QSplitter(Qt.Horizontal)

        self._win_a = SimWindow("A Conservative", "TP:1.0% SL:-0.5%", C_BLUE)
        self._win_b = SimWindow("B Aggressive", "TP:2.0% SL:-1.0%", C_YELLOW)
        self._win_c = SimWindow("C Dynamic", "TP:1.5%→6.0% Trail+Micro", C_GREEN)

        splitter.addWidget(self._win_a)
        splitter.addWidget(self._win_b)
        splitter.addWidget(self._win_c)
        splitter.setSizes([400, 400, 400])
        main_layout.addWidget(splitter)

        # Auto-refresh timer (every 60s for live day)
        self._timer = QTimer()
        self._timer.timeout.connect(self._auto_refresh)
        self._timer.start(60000)

        # Auto-run if data exists
        if dates:
            self._run_simulation()

    def _run_simulation(self):
        date_str = self._date_combo.currentText()
        if not date_str or date_str == "(no data)":
            self._win_a.show_no_data()
            self._win_b.show_no_data()
            self._win_c.show_no_data()
            self._status.setText("No data available")
            return

        self._status.setText(f"Running {date_str}...")
        QApplication.processEvents()

        rankings = load_ranking(date_str)
        if not rankings:
            self._win_a.show_no_data()
            self._win_b.show_no_data()
            self._win_c.show_no_data()
            self._status.setText(f"No ranking data for {date_str}")
            return

        # Window A: Conservative
        trades_a = run_fixed_strategy(rankings, date_str, self._name_cache,
                                       tp_pct=1.0, sl_pct=-0.5)
        sum_a = calc_summary(trades_a)
        self._win_a.update_results(trades_a, sum_a)

        # Window B: Aggressive
        trades_b = run_fixed_strategy(rankings, date_str, self._name_cache,
                                       tp_pct=2.0, sl_pct=-1.0)
        sum_b = calc_summary(trades_b)
        self._win_b.update_results(trades_b, sum_b)

        # Window C: Dynamic
        trades_c = run_dynamic_strategy(rankings, date_str, self._name_cache)
        sum_c = calc_summary(trades_c)
        self._win_c.update_results(trades_c, sum_c)

        # Find best
        best = max([(sum_a.total_pnl, "A"), (sum_b.total_pnl, "B"), (sum_c.total_pnl, "C")])
        self._status.setText(
            f"{date_str} | A:{sum_a.total_pnl:+,.0f} B:{sum_b.total_pnl:+,.0f} "
            f"C:{sum_c.total_pnl:+,.0f} | Best: {best[1]}")

    def _auto_refresh(self):
        """Refresh date list and re-run if today's data updated."""
        dates = available_dates()
        current = self._date_combo.currentText()
        self._date_combo.clear()
        if dates:
            self._date_combo.addItems(dates)
            today = date.today().strftime("%Y%m%d")
            if today in dates:
                self._date_combo.setCurrentText(today)
                self._run_simulation()
            elif current in dates:
                self._date_combo.setCurrentText(current)
        else:
            self._date_combo.addItem("(no data)")


# =========================================================================
#  Entry Point
# =========================================================================
def main():
    parser = argparse.ArgumentParser(description="Q-TRON Swing Simulator")
    parser.add_argument("--date", default=None, help="Date (YYYYMMDD)")
    args = parser.parse_args()

    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    palette = QPalette()
    palette.setColor(QPalette.Window, QColor(BG_MAIN))
    palette.setColor(QPalette.WindowText, QColor(C_TEXT_PRIMARY))
    palette.setColor(QPalette.Base, QColor(BG_CARD))
    palette.setColor(QPalette.Text, QColor(C_TEXT_PRIMARY))
    app.setPalette(palette)

    win = SwingSimulatorGUI(args.date)
    win.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
