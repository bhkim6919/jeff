"""
Gen4 GUI Monitor v2 -- Mission Control Trading Console.
Separate process from engine. Reads state JSON files via atomic-safe polling.

Mission Control layout:
    TOP:    HeartbeatStrip (LED indicators for ENGINE/STALE/RISK/REGIME/UPDATED)
    LEFT:   Decision Hub (BUY OK/LIMITED/BLOCKED + 5 KPI cards stacked)
    CENTER: Hero Chart (3-tab: Equity Curve / PnL Waterfall / Risk Heatmap)
    RIGHT:  Alert Stream (4-level priority cards + AI Advisor)
    BOTTOM: Position Grid (20 stock tiles) + Detail Bar (trades/pending/logs)

Usage:
    python monitor_gui_v2.py                  # paper mode (default)
    python monitor_gui_v2.py --mode live
    python monitor_gui_v2.py --mode paper_test

Design rules:
    - READ ONLY: no writes to engine files (name cache is monitor-only file)
    - Stale detection: WARN >90s, STALE >180s
    - Parse failure: keep last-good-value, show stale badge
    - Engine crash/stop: GUI keeps running, shows stale state
"""
from __future__ import annotations

import argparse
import json
import csv
import sys
import logging
from datetime import datetime, date
from pathlib import Path

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QTableWidget, QTableWidgetItem, QHeaderView,
    QTextEdit, QFrame, QTabWidget, QScrollArea, QComboBox,
    QDockWidget, QSizePolicy, QLayout, QToolTip, QMenu, QAction,
    QGridLayout, QPushButton, QSplitter,
)
from PyQt5.QtCore import (
    QTimer, Qt, QSize, QRect, QPoint, QByteArray,
)
from PyQt5.QtGui import (
    QFont, QColor, QPainter, QPen, QBrush, QPixmap, QPalette,
    QLinearGradient, QRadialGradient,
)

try:
    import pyqtgraph as pg
    pg.setConfigOptions(antialias=True, background="#0d1017", foreground="#9ba4b5")
    HAS_CHARTS = True
except ImportError:
    HAS_CHARTS = False

logger = logging.getLogger("gen4.monitor_v2")

# =========================================================================
#  Paths & Constants
# =========================================================================
BASE_DIR = Path(__file__).resolve().parent
STATE_DIR = BASE_DIR / "state"
REPORT_DIR = BASE_DIR / "report" / "output"
REPORT_DIR_TEST = BASE_DIR / "report" / "output_test"
LOG_DIR = BASE_DIR / "logs"
NAME_CACHE_PATH = BASE_DIR / "data" / "stock_name_cache.json"
ADVISOR_DIR = BASE_DIR / "advisor" / "output"
SECTOR_CACHE_PATH = BASE_DIR / "data" / "sector_cache.json"
LAYOUT_STATE_PATH = STATE_DIR / "monitor_layout_v2.dat"
INTRADAY_DIR = BASE_DIR / "data" / "intraday"
INTRADAY_DIR_TEST = BASE_DIR / "data" / "intraday_test"

REFRESH_MS = 3000
STALE_WARN_SEC = 360
STALE_ALERT_SEC = 600
SELL_COST_RATE = 0.00295

# =========================================================================
#  Colors
# =========================================================================
C_GREEN = "#00ff88"
C_RED = "#ff3344"
C_YELLOW = "#ffdd00"
C_BLUE = "#6699ff"
C_CYAN = "#44cccc"
C_ORANGE = "#ff8800"
C_DIM = "#606880"
C_WHITE = "#f0f0f0"
C_BRIGHT = "#ffffff"

BG_MAIN = "#060810"
BG_CARD = "#0e1118"
BG_HOVER = "#161a24"
BG_ACTIVE = "#1e2230"
BG_HEADER = "#0b0d14"
BG_TABLE = "#0c0e15"
BG_TABLE_ALT = "#10131a"
BG_BORDER = "#1c2030"
BG_SELECTION = "#1e2a45"
C_TEXT_PRIMARY = "#e8eaef"
C_TEXT_SECONDARY = "#7a8494"
C_CARD_TITLE = "#9ba4b5"

SECTOR_COLORS = [
    "#00e5a0", "#6b9fff", "#ffd166", "#ff6b6b", "#a78bfa",
    "#f472b6", "#34d399", "#f59e0b", "#3b82f6", "#8b5cf6",
]

# =========================================================================
#  Sector Map
# =========================================================================
KOSPI_SECTOR_MAP = {
    "005930": "IT/Elec", "000660": "IT/Elec", "009150": "IT/Elec",
    "006400": "IT/Elec", "012330": "IT/Elec", "042660": "IT/Elec",
    "042700": "IT/Elec", "034220": "IT/Elec", "010120": "IT/Elec",
    "037460": "IT/Elec", "035810": "IT/Elec", "161390": "Auto",
    "055550": "Finance", "086790": "Finance", "105560": "Finance",
    "316140": "Finance", "175330": "Finance", "024110": "Finance",
    "032830": "Insurance", "005830": "Insurance", "001450": "Insurance",
    "009410": "Insurance",
    "005380": "Auto", "000270": "Auto",
    "051910": "Chem", "009830": "Chem", "010950": "Chem",
    "018880": "Chem", "011170": "Chem", "001800": "Chem",
    "004690": "Gas/Energy", "078930": "Conglomerate",
    "068270": "Bio", "207940": "Bio", "128940": "Bio",
    "035420": "Internet", "036570": "Internet",
    "028260": "Defense", "02826K": "Defense", "012450": "Defense",
    "005960": "Construct",
    "001120": "Trading", "009970": "Textile",
    "003540": "Securities",
    "017670": "Telecom", "030200": "Telecom",
    "005490": "Steel", "004020": "Steel", "001230": "Steel",
    "139130": "Steel", "001060": "Steel",
    "003550": "Telecom", "007070": "Chem",
}

# =========================================================================
#  Global Stylesheet
# =========================================================================
STYLE = f"""
QMainWindow {{ background-color: {BG_MAIN}; }}
QLabel {{ color: {C_TEXT_PRIMARY}; }}
QTableWidget {{
    background-color: rgba(12,14,21,180); alternate-background-color: rgba(16,19,26,180);
    color: {C_TEXT_PRIMARY}; gridline-color: rgba(28,32,48,120);
    selection-background-color: {BG_SELECTION};
    border: 1px solid rgba(40,60,100,80); border-radius: 6px; font-size: 12px;
}}
QTableWidget::item {{ padding: 4px 6px; }}
QHeaderView::section {{
    background-color: rgba(11,13,20,200); color: {C_CARD_TITLE}; border: none;
    border-bottom: 1px solid rgba(42,48,80,150); padding: 5px 6px;
    font-weight: bold; font-size: 11px;
}}
QTextEdit {{
    background-color: rgba(12,14,21,180); color: #c8ccd4;
    border: 1px solid rgba(40,60,100,80); border-radius: 6px;
    font-family: 'Consolas','D2Coding',monospace; font-size: 11px; padding: 4px;
}}
QComboBox {{
    background-color: rgba(12,14,21,200); color: {C_TEXT_PRIMARY};
    border: 1px solid rgba(42,48,80,150); border-radius: 4px;
    padding: 3px 8px; font-size: 11px;
}}
QComboBox::drop-down {{ border: none; }}
QComboBox QAbstractItemView {{ background-color: rgba(14,17,24,240); color: {C_TEXT_PRIMARY}; selection-background-color: {BG_SELECTION}; }}
QScrollArea {{ border: none; background: transparent; }}
QScrollBar:vertical {{
    background: transparent; width: 8px; border: none;
}}
QScrollBar::handle:vertical {{
    background: rgba(42,48,80,180); border-radius: 4px; min-height: 30px;
}}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height: 0; }}
QScrollBar:horizontal {{
    background: transparent; height: 8px; border: none;
}}
QScrollBar::handle:horizontal {{
    background: rgba(42,48,80,180); border-radius: 4px; min-width: 30px;
}}
QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{ width: 0; }}
QToolTip {{
    background-color: rgba(26,30,40,240); color: {C_TEXT_PRIMARY};
    border: 1px solid rgba(58,64,96,200); padding: 6px 8px; font-size: 11px;
    font-family: 'Segoe UI','Malgun Gothic'; border-radius: 6px;
}}
QDockWidget {{ color: {C_DIM}; font-size: 11px; }}
QDockWidget::title {{
    background: rgba(14,17,24,200); padding: 4px 8px;
    border-bottom: 1px solid rgba(40,80,140,60);
}}
QTabWidget::pane {{
    border: 1px solid rgba(40,80,140,60); border-radius: 6px;
    background: rgba(12,14,21,180);
}}
QTabBar::tab {{
    background: rgba(14,17,24,200); color: {C_DIM};
    border: 1px solid rgba(40,60,100,60); border-bottom: none;
    padding: 6px 16px; font-size: 11px; font-weight: bold;
    border-top-left-radius: 6px; border-top-right-radius: 6px;
    margin-right: 2px;
}}
QTabBar::tab:selected {{
    background: rgba(30,50,90,180); color: {C_WHITE};
    border-bottom: 2px solid {C_BLUE};
}}
QTabBar::tab:hover {{
    background: rgba(22,26,36,220); color: {C_WHITE};
}}
"""

CARD_STYLE = (
    "background-color: rgba(14,17,24,180);"
    " border: 1px solid rgba(40,80,140,60);"
    " border-radius: 10px;"
)

# =========================================================================
#  Name & Sector Caches
# =========================================================================
def _build_name_cache(codes):
    name_map = {}
    try:
        from pykrx import stock as krx
        for code in codes:
            try:
                nm = krx.get_market_ticker_name(code)
                if nm:
                    name_map[code] = nm
            except Exception:
                pass
    except ImportError:
        pass
    return name_map

def load_name_cache():
    if NAME_CACHE_PATH.exists():
        try:
            data = json.loads(NAME_CACHE_PATH.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception:
            pass
    return {}

def save_name_cache(cache):
    try:
        NAME_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        NAME_CACHE_PATH.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass

def ensure_names(codes, cache):
    missing = [c for c in codes if c not in cache]
    if missing:
        new_names = _build_name_cache(missing)
        cache.update(new_names)
        if new_names:
            save_name_cache(cache)
    return cache

def load_sector_cache():
    if SECTOR_CACHE_PATH.exists():
        try:
            return json.loads(SECTOR_CACHE_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return dict(KOSPI_SECTOR_MAP)

# =========================================================================
#  Safe File Readers
# =========================================================================
def read_json_safe(path):
    for p in (path, path.with_suffix(".bak")):
        if not p.exists():
            continue
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
        except Exception:
            continue
    return None

def read_json_list_safe(path):
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []

def read_csv_tail(path, n=5):
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            rows = list(csv.DictReader(f))
        return rows[-n:] if rows else []
    except Exception:
        return []

def read_csv_all(path):
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []

def read_log_tail(mode, n=20):
    today = datetime.now().strftime("%Y%m%d")
    for log_path in [LOG_DIR / f"gen4_{mode}_{today}.log",
                     LOG_DIR / f"gen4_live_{today}.log",
                     LOG_DIR / f"gen4_paper_{today}.log"]:
        if not log_path.exists():
            continue
        try:
            lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
            return [ln for ln in lines
                    if "[CRITICAL]" in ln or "[WARNING]" in ln or "[ERROR]" in ln][-n:]
        except Exception:
            continue
    return []

def find_latest_advisor_dir():
    if not ADVISOR_DIR.exists():
        return None
    from datetime import date as _date
    today = _date.today().strftime("%Y%m%d")
    dirs = sorted([d for d in ADVISOR_DIR.iterdir() if d.is_dir()], reverse=True)
    # Only return if latest dir is from today (stale advisor data is misleading)
    if dirs and dirs[0].name == today:
        return dirs[0]
    return None

# =========================================================================
#  Helpers
# =========================================================================
def fmt_krw(v):
    try:
        return f"{int(float(v)):,}"
    except Exception:
        return str(v)

def pct_str(v):
    return f"{v * 100:+.2f}%"

def age_str(ts_str):
    if not ts_str:
        return "N/A", 2
    try:
        delta = (datetime.now() - datetime.fromisoformat(ts_str)).total_seconds()
        secs = int(delta)
        level = 2 if secs > STALE_ALERT_SEC else (1 if secs > STALE_WARN_SEC else 0)
        if secs < 60:
            return f"{secs}s", level
        elif secs < 3600:
            return f"{secs // 60}m {secs % 60}s", level
        else:
            return f"{secs // 3600}h {(secs % 3600) // 60}m", level
    except Exception:
        return "N/A", 2

def compute_decision(risk_mode, recon_unreliable, stale_level):
    """Returns (status, reasons_list, is_critical)."""
    reasons = []
    is_critical = False

    if stale_level >= 2:
        reasons.append("STALE")
        is_critical = True
    if recon_unreliable:
        reasons.append("RECON")
        is_critical = True

    risk_upper = (risk_mode or "").upper()
    if any(k in risk_upper for k in ("BLOCKED", "SAFE", "SEVERE", "CRITICAL")):
        reasons.append(risk_mode)
        is_critical = True
    elif any(k in risk_upper for k in ("CAUTION", "WARNING")):
        reasons.append(risk_mode)

    if is_critical:
        status = "BUY BLOCKED"
    elif reasons:
        status = "BUY LIMITED"
    else:
        status = "BUY OK"
    return status, reasons, is_critical

def decision_color(status):
    if "BLOCKED" in status:
        return C_RED
    elif "LIMITED" in status:
        return C_YELLOW
    return C_GREEN


# =========================================================================
#  FlowLayout — wraps children to next row when space runs out
# =========================================================================
class FlowLayout(QLayout):
    def __init__(self, parent=None, margin=4, spacing=4):
        super().__init__(parent)
        self._items = []
        self._spacing = spacing
        if parent is not None:
            self.setContentsMargins(margin, margin, margin, margin)

    def addItem(self, item):
        self._items.append(item)

    def count(self):
        return len(self._items)

    def itemAt(self, index):
        if 0 <= index < len(self._items):
            return self._items[index]
        return None

    def takeAt(self, index):
        if 0 <= index < len(self._items):
            return self._items.pop(index)
        return None

    def expandingDirections(self):
        return Qt.Orientations()

    def hasHeightForWidth(self):
        return True

    def heightForWidth(self, width):
        return self._do_layout(QRect(0, 0, width, 0), test_only=True)

    def setGeometry(self, rect):
        super().setGeometry(rect)
        self._do_layout(rect, test_only=False)

    def sizeHint(self):
        return self.minimumSize()

    def minimumSize(self):
        size = QSize()
        for item in self._items:
            size = size.expandedTo(item.minimumSize())
        m = self.contentsMargins()
        size += QSize(m.left() + m.right(), m.top() + m.bottom())
        return size

    def _do_layout(self, rect, test_only):
        m = self.contentsMargins()
        effective = rect.adjusted(m.left(), m.top(), -m.right(), -m.bottom())
        x = effective.x()
        y = effective.y()
        row_height = 0

        for item in self._items:
            sz = item.sizeHint()
            next_x = x + sz.width() + self._spacing
            if next_x - self._spacing > effective.right() and row_height > 0:
                x = effective.x()
                y += row_height + self._spacing
                next_x = x + sz.width() + self._spacing
                row_height = 0
            if not test_only:
                item.setGeometry(QRect(QPoint(x, y), sz))
            x = next_x
            row_height = max(row_height, sz.height())
        return y + row_height - rect.y() + m.bottom()


# =========================================================================
#  LEDIndicator — single colored dot + label
# =========================================================================
class LEDIndicator(QFrame):
    COLORS = {
        "ok": QColor(0, 255, 136),
        "warn": QColor(255, 221, 0),
        "alert": QColor(255, 51, 68),
        "off": QColor(60, 68, 80),
        "blue": QColor(102, 153, 255),
    }

    def __init__(self, label_text, parent=None):
        super().__init__(parent)
        self.setFixedHeight(28)
        self._state = "off"
        self._label_text = label_text
        self._value_text = ""
        layout = QHBoxLayout(self)
        layout.setContentsMargins(8, 0, 8, 0)
        layout.setSpacing(6)
        self._dot = QWidget(self)
        self._dot.setFixedSize(10, 10)
        layout.addWidget(self._dot)
        self._lbl = QLabel(label_text)
        self._lbl.setStyleSheet(f"color: {C_DIM}; font-size: 10px; font-weight: bold; font-family: Consolas; border: none;")
        layout.addWidget(self._lbl)
        self._val = QLabel("")
        self._val.setStyleSheet(f"color: {C_TEXT_SECONDARY}; font-size: 10px; font-family: Consolas; border: none;")
        layout.addWidget(self._val)
        self.setStyleSheet("border: none; background: transparent;")
        self._update_dot()

    def set_state(self, state, value=""):
        self._state = state
        self._value_text = value
        self._val.setText(value)
        clr = self.COLORS.get(state, self.COLORS["off"])
        self._lbl.setStyleSheet(
            f"color: {clr.name()}; font-size: 10px; font-weight: bold; font-family: Consolas; border: none;")
        self._update_dot()

    def _update_dot(self):
        clr = self.COLORS.get(self._state, self.COLORS["off"])
        self._dot.setStyleSheet(
            f"background-color: {clr.name()}; border-radius: 5px; border: none;")


# =========================================================================
#  HeartbeatStrip — fixed top bar with 5 LEDs
# =========================================================================
class HeartbeatStrip(QFrame):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedHeight(32)
        self.setStyleSheet(f"background-color: {BG_HEADER}; border-bottom: 1px solid rgba(40,80,140,60);")
        layout = QHBoxLayout(self)
        layout.setContentsMargins(8, 0, 8, 0)
        layout.setSpacing(4)
        self.led_engine = LEDIndicator("ENGINE")
        self.led_stale = LEDIndicator("STALE")
        self.led_risk = LEDIndicator("RISK")
        self.led_regime = LEDIndicator("REGIME")
        self.led_updated = LEDIndicator("UPDATED")
        for led in (self.led_engine, self.led_stale, self.led_risk, self.led_regime):
            layout.addWidget(led)
        layout.addStretch()
        layout.addWidget(self.led_updated)
        self._detail_label = QLabel("")
        self._detail_label.setStyleSheet(f"color: {C_RED}; font-size: 11px; font-weight: bold; font-family: Consolas; border: none;")
        self._detail_label.setVisible(False)
        layout.addWidget(self._detail_label)

    def update_state(self, stale_level, age_text, risk_mode, regime, engine_running):
        # ENGINE
        if engine_running:
            self.led_engine.set_state("ok", "RUNNING")
        else:
            self.led_engine.set_state("alert" if stale_level >= 2 else "warn",
                                      "STOPPED" if stale_level >= 2 else "UNKNOWN")
        # STALE
        if stale_level == 0:
            self.led_stale._lbl.setText("FRESH")
            self.led_stale.set_state("ok", age_text)
        elif stale_level == 1:
            self.led_stale._lbl.setText("STALE")
            self.led_stale.set_state("warn", age_text)
        else:
            self.led_stale._lbl.setText("STALE")
            self.led_stale.set_state("alert", age_text)
        # RISK
        risk_upper = (risk_mode or "UNKNOWN").upper()
        if "NORMAL" in risk_upper:
            self.led_risk.set_state("ok", risk_mode)
        elif any(k in risk_upper for k in ("BLOCKED", "SAFE", "SEVERE", "CRITICAL")):
            self.led_risk.set_state("alert", risk_mode)
        elif any(k in risk_upper for k in ("CAUTION", "WARNING")):
            self.led_risk.set_state("warn", risk_mode)
        else:
            self.led_risk.set_state("off", risk_mode or "UNKNOWN")
        # REGIME
        regime_upper = (regime or "").upper()
        if "BULL" in regime_upper or "UP" in regime_upper:
            self.led_regime.set_state("ok", regime or "BULL")
        elif "BEAR" in regime_upper or "DOWN" in regime_upper or "CRASH" in regime_upper:
            self.led_regime.set_state("alert", regime or "BEAR")
        elif "SIDE" in regime_upper:
            self.led_regime.set_state("blue", regime or "SIDEWAYS")
        else:
            self.led_regime.set_state("off", regime or "-")
        # UPDATED
        self.led_updated.set_state("off", datetime.now().strftime("%H:%M:%S"))
        # Expand on critical
        is_critical = stale_level >= 2 or any(
            k in risk_upper for k in ("BLOCKED", "SAFE", "SEVERE"))
        if is_critical:
            self.setFixedHeight(48)
            detail_parts = []
            if stale_level >= 2:
                detail_parts.append(f"STALE ({age_text})")
            if any(k in risk_upper for k in ("BLOCKED", "SAFE", "SEVERE", "CRITICAL")):
                detail_parts.append(f"RISK: {risk_mode}")
            self._detail_label.setText("  |  ".join(detail_parts))
            self._detail_label.setVisible(True)
            self.setStyleSheet(f"background-color: #1a0808; border-bottom: 2px solid {C_RED};")
        else:
            self.setFixedHeight(32)
            self._detail_label.setVisible(False)
            self.setStyleSheet(f"background-color: {BG_HEADER}; border-bottom: 1px solid rgba(40,80,140,60);")


# =========================================================================
#  KPICard — small metric card
# =========================================================================
class KPICard(QFrame):
    def __init__(self, label, parent=None):
        super().__init__(parent)
        self.setStyleSheet(CARD_STYLE)
        self.setMinimumHeight(68)
        self.setMaximumHeight(90)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 8, 12, 8)
        layout.setSpacing(2)
        self._label = QLabel(label.upper())
        self._label.setStyleSheet(f"color: {C_CARD_TITLE}; font-size: 9px; font-weight: bold; font-family: Consolas; border: none;")
        layout.addWidget(self._label)
        self._value = QLabel("-")
        self._value.setStyleSheet(f"color: {C_WHITE}; font-size: 18px; font-weight: bold; font-family: Consolas; border: none;")
        layout.addWidget(self._value)
        self._sub = QLabel("")
        self._sub.setStyleSheet(f"color: {C_TEXT_SECONDARY}; font-size: 9px; border: none;")
        self._sub.setVisible(False)
        layout.addWidget(self._sub)

    def set_value(self, text, color=C_WHITE, sub=""):
        self._value.setText(text)
        fs = "14px" if len(text) > 10 else "18px"
        self._value.setStyleSheet(f"color: {color}; font-size: {fs}; font-weight: bold; font-family: Consolas; border: none;")
        if sub:
            self._sub.setText(sub)
            self._sub.setVisible(True)
        else:
            self._sub.setVisible(False)


# =========================================================================
#  DecisionCard — BUY OK / LIMITED / BLOCKED
# =========================================================================
class DecisionCard(QFrame):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMinimumHeight(100)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 12, 16, 12)
        layout.setSpacing(4)
        self._title = QLabel("DECISION")
        self._title.setStyleSheet(f"color: {C_CARD_TITLE}; font-size: 10px; font-weight: bold; font-family: Consolas; border: none;")
        layout.addWidget(self._title, alignment=Qt.AlignCenter)
        self._status = QLabel("BUY OK")
        self._status.setStyleSheet(f"color: {C_GREEN}; font-size: 26px; font-weight: bold; font-family: Consolas; border: none;")
        self._status.setAlignment(Qt.AlignCenter)
        layout.addWidget(self._status)
        self._reason = QLabel("")
        self._reason.setStyleSheet(f"color: {C_TEXT_SECONDARY}; font-size: 10px; font-family: Consolas; border: none;")
        self._reason.setAlignment(Qt.AlignCenter)
        self._reason.setWordWrap(True)
        self._reason.setVisible(False)
        layout.addWidget(self._reason)
        self._set_tint(C_GREEN)

    def _set_tint(self, color):
        tints = {
            C_GREEN: ("#081a10", "#0a3018"),
            C_YELLOW: ("#1a1808", "#302a0a"),
            C_RED: ("#1a0808", "#300a0a"),
        }
        bg, border = tints.get(color, ("#1a0808", "#300a0a"))
        self.setStyleSheet(f"background-color: {bg}; border: 2px solid {border}; border-radius: 10px;")

    def set_decision(self, status, reasons, color):
        self._status.setText(status)
        self._status.setStyleSheet(f"color: {color}; font-size: 26px; font-weight: bold; font-family: Consolas; border: none;")
        if reasons:
            self._reason.setText(" + ".join(reasons))
            self._reason.setVisible(True)
        else:
            self._reason.setVisible(False)
        self._set_tint(color)


# =========================================================================
#  DecisionHubWidget — left panel (Decision + 5 KPIs)
# =========================================================================
class DecisionHubWidget(QFrame):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setStyleSheet("background: transparent; border: none;")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(4, 4, 4, 4)
        layout.setSpacing(6)
        self.decision_card = DecisionCard()
        layout.addWidget(self.decision_card)
        self.kpi_pnl = KPICard("TODAY PNL")
        self.kpi_mdd = KPICard("CURRENT MDD")
        self.kpi_positions = KPICard("POSITIONS")
        self.kpi_cash = KPICard("CASH RATIO")
        self.kpi_cumulative = KPICard("CUMULATIVE")
        for kpi in (self.kpi_pnl, self.kpi_mdd, self.kpi_positions, self.kpi_cash, self.kpi_cumulative):
            layout.addWidget(kpi)
        layout.addStretch()

    def update_state(self, status, reasons, color, eq_rows, all_eq, pf):
        self.decision_card.set_decision(status, reasons, color)
        if not eq_rows:
            return
        row = eq_rows[-1]
        # Today PnL
        try:
            daily_pnl = float(row.get("daily_pnl_pct", 0))
            # Adjust with sell cost estimate
            positions = (pf or {}).get("positions", {})
            if positions and pf:
                invested = sum(
                    p.get("invested_total", p.get("quantity", 0) * p.get("avg_price", 0))
                    for p in positions.values()
                )
                est_sell = invested * SELL_COST_RATE
                prev_close = float(pf.get("prev_close_equity", 0))
                if prev_close > 0:
                    equity = float(row.get("equity", 0))
                    adjusted = (equity - est_sell) / prev_close - 1
                    daily_pnl = adjusted
            clr = C_GREEN if daily_pnl >= 0 else C_RED
            self.kpi_pnl.set_value(f"{daily_pnl * 100:+.2f}%", clr)
        except Exception:
            self.kpi_pnl.set_value("-")
        # MDD
        try:
            mdd = float(row.get("monthly_dd_pct", 0))
            clr = C_GREEN if mdd > -0.05 else (C_YELLOW if mdd > -0.10 else C_RED)
            self.kpi_mdd.set_value(f"{mdd * 100:.1f}%", clr, row.get("risk_mode", ""))
        except Exception:
            self.kpi_mdd.set_value("-")
        # Positions
        try:
            n = int(row.get("n_positions", 0))
            self.kpi_positions.set_value(f"{n} / 20", C_CYAN)
        except Exception:
            self.kpi_positions.set_value("-")
        # Cash ratio
        try:
            cash = float(row.get("cash", 0))
            equity = float(row.get("equity", 1))
            ratio = cash / equity * 100 if equity > 0 else 0
            man = cash / 10000
            self.kpi_cash.set_value(f"{ratio:.1f}%", C_WHITE, f"{man:,.0f}만")
        except Exception:
            self.kpi_cash.set_value("-")
        # Cumulative
        try:
            if all_eq and len(all_eq) > 1:
                first_eq = float(all_eq[0].get("equity", 1))
                last_eq = float(row.get("equity", 0))
                positions = (pf or {}).get("positions", {})
                invested = sum(
                    p.get("invested_total", p.get("quantity", 0) * p.get("avg_price", 0))
                    for p in positions.values()
                )
                est_sell = invested * SELL_COST_RATE
                cum = (last_eq - est_sell) / first_eq - 1 if first_eq > 0 else 0
                clr = C_GREEN if cum >= 0 else C_RED
                self.kpi_cumulative.set_value(f"{cum * 100:+.1f}%", clr)
            else:
                self.kpi_cumulative.set_value("-")
        except Exception:
            self.kpi_cumulative.set_value("-")


# =========================================================================
#  RiskHeatmapWidget — 4x5 trail gap grid
# =========================================================================
class RiskHeatmapWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._data = []
        self.setMinimumHeight(200)

    def set_data(self, positions):
        """positions: list of dicts with name, trail_gap, pnl_pct, code"""
        self._data = positions[:20]
        self.update()

    def paintEvent(self, event):
        if not self._data:
            painter = QPainter(self)
            painter.setPen(QColor(C_DIM))
            painter.setFont(QFont("Consolas", 12))
            painter.drawText(self.rect(), Qt.AlignCenter, "No position data")
            painter.end()
            return

        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        w = self.width()
        h = self.height()
        cols = 5
        rows = (len(self._data) + cols - 1) // cols
        pad = 4
        cell_w = (w - pad * (cols + 1)) / cols
        cell_h = (h - pad * (rows + 1)) / rows
        cell_h = min(cell_h, 80)

        for i, pos in enumerate(self._data):
            col = i % cols
            row = i // cols
            x = pad + col * (cell_w + pad)
            y = pad + row * (cell_h + pad)
            gap = pos.get("trail_gap", 99)
            pnl = pos.get("pnl_pct", 0)
            name = pos.get("name", pos.get("code", "?"))[:5]

            # Color by trail gap
            if gap < 2:
                bg = QColor(255, 51, 68, 180)
                fg = QColor(255, 255, 255)
            elif gap < 5:
                bg = QColor(255, 136, 0, 140)
                fg = QColor(255, 255, 255)
            elif gap < 8:
                bg = QColor(255, 221, 0, 100)
                fg = QColor(255, 255, 255)
            else:
                g = min(int(gap * 3), 80)
                bg = QColor(0, 255, 136, g)
                fg = QColor(240, 240, 240)

            painter.setBrush(QBrush(bg))
            painter.setPen(Qt.NoPen)
            painter.drawRoundedRect(int(x), int(y), int(cell_w), int(cell_h), 6, 6)

            # Border for danger
            if gap < 2:
                painter.setPen(QPen(QColor(C_RED), 2))
                painter.setBrush(Qt.NoBrush)
                painter.drawRoundedRect(int(x), int(y), int(cell_w), int(cell_h), 6, 6)

            # Text
            painter.setPen(fg)
            painter.setFont(QFont("Segoe UI", 9, QFont.Bold))
            painter.drawText(int(x + 4), int(y + 4), int(cell_w - 8), int(cell_h * 0.4),
                             Qt.AlignLeft | Qt.AlignVCenter, name)
            painter.setFont(QFont("Consolas", 10, QFont.Bold))
            gap_text = f"{gap:.1f}%" if gap < 99 else "-"
            painter.drawText(int(x + 4), int(y + cell_h * 0.4), int(cell_w - 8), int(cell_h * 0.3),
                             Qt.AlignLeft | Qt.AlignVCenter, gap_text)
            # PnL small
            painter.setFont(QFont("Consolas", 8))
            pnl_clr = QColor(0, 255, 136) if pnl >= 0 else QColor(255, 51, 68)
            painter.setPen(pnl_clr)
            painter.drawText(int(x + 4), int(y + cell_h * 0.7), int(cell_w - 8), int(cell_h * 0.25),
                             Qt.AlignLeft | Qt.AlignVCenter, f"{pnl:+.1f}%")
        painter.end()


# =========================================================================
#  HeroChartWidget — 3-tab chart area
# =========================================================================
class HeroChartWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        # Tab 1: Equity Curve
        self._equity_widget = QWidget()
        eq_layout = QVBoxLayout(self._equity_widget)
        eq_layout.setContentsMargins(4, 4, 4, 4)
        # Period selector
        ctrl = QHBoxLayout()
        self._period_combo = QComboBox()
        self._period_combo.addItems(["1D", "1W", "1M", "ALL"])
        self._period_combo.setFixedWidth(70)
        ctrl.addWidget(QLabel("Period:"))
        ctrl.addWidget(self._period_combo)
        ctrl.addStretch()
        self._equity_stats = QLabel("")
        self._equity_stats.setStyleSheet(f"color: {C_DIM}; font-size: 10px; font-family: Consolas;")
        ctrl.addWidget(self._equity_stats)
        eq_layout.addLayout(ctrl)
        if HAS_CHARTS:
            self._equity_plot = pg.PlotWidget()
            self._equity_plot.setBackground("#0d1017")
            self._equity_plot.showGrid(x=False, y=True, alpha=0.15)
            self._equity_plot.setLabel("left", "%")
            eq_layout.addWidget(self._equity_plot)
        else:
            eq_layout.addWidget(QLabel("pyqtgraph not installed"))
        self.tabs.addTab(self._equity_widget, "Equity Curve")

        # Tab 2: PnL Waterfall
        self._pnl_widget = QWidget()
        pnl_layout = QVBoxLayout(self._pnl_widget)
        pnl_layout.setContentsMargins(4, 4, 4, 4)
        pnl_ctrl = QHBoxLayout()
        self._sort_combo = QComboBox()
        self._sort_combo.addItems(["PnL %", "PnL Amt"])
        self._sort_combo.setFixedWidth(90)
        pnl_ctrl.addWidget(QLabel("Sort:"))
        pnl_ctrl.addWidget(self._sort_combo)
        pnl_ctrl.addStretch()
        self._pnl_stats = QLabel("")
        self._pnl_stats.setStyleSheet(f"color: {C_DIM}; font-size: 10px; font-family: Consolas;")
        pnl_ctrl.addWidget(self._pnl_stats)
        pnl_layout.addLayout(pnl_ctrl)
        if HAS_CHARTS:
            self._pnl_plot = pg.PlotWidget()
            self._pnl_plot.setBackground("#0d1017")
            self._pnl_plot.showGrid(x=True, y=False, alpha=0.15)
            pnl_layout.addWidget(self._pnl_plot)
        else:
            pnl_layout.addWidget(QLabel("pyqtgraph not installed"))
        self.tabs.addTab(self._pnl_widget, "PnL Waterfall")

        # Tab 3: Risk Heatmap
        self._heatmap = RiskHeatmapWidget()
        self.tabs.addTab(self._heatmap, "Risk Heatmap")

    def update_equity(self, all_eq, mode="paper", pf=None):
        if not HAS_CHARTS:
            return
        period = self._period_combo.currentText()

        # 1D mode: intraday minute bars
        if period == "1D":
            self._update_equity_intraday(mode, pf)
            return

        if not all_eq:
            return
        self._equity_plot.clear()
        if period == "1W":
            rows = all_eq[-5:]
        elif period == "1M":
            rows = all_eq[-22:]
        else:
            rows = all_eq

        if not rows:
            return
        try:
            base_eq = float(rows[0].get("equity", 1))
        except Exception:
            return

        xs = list(range(len(rows)))
        eq_ys = []
        kospi_ys = []
        base_kospi = None

        for r in rows:
            try:
                eq_ys.append((float(r.get("equity", base_eq)) / base_eq - 1) * 100)
            except Exception:
                eq_ys.append(0)
            try:
                kc = float(r.get("kospi_close", 0))
                if kc > 0:
                    if base_kospi is None:
                        base_kospi = kc
                    kospi_ys.append((kc / base_kospi - 1) * 100)
                else:
                    kospi_ys.append(None)
            except Exception:
                kospi_ys.append(None)

        pen_eq = pg.mkPen(color=C_GREEN, width=2)
        self._equity_plot.plot(xs, eq_ys, pen=pen_eq, name="Portfolio")
        if base_kospi and any(v is not None for v in kospi_ys):
            kx = [i for i, v in enumerate(kospi_ys) if v is not None]
            ky = [v for v in kospi_ys if v is not None]
            pen_k = pg.mkPen(color=C_BLUE, width=1.5, style=Qt.DashLine)
            self._equity_plot.plot(kx, ky, pen=pen_k, name="KOSPI")

        # Stats
        if eq_ys:
            excess = eq_ys[-1] - (kospi_ys[-1] if kospi_ys and kospi_ys[-1] is not None else 0)
            self._equity_stats.setText(
                f"Portfolio: {eq_ys[-1]:+.1f}%  |  Excess: {excess:+.1f}%")

    def update_pnl_waterfall(self, pos_rows, name_cache):
        if not HAS_CHARTS or not pos_rows:
            return
        self._pnl_plot.clear()
        sort_mode = self._sort_combo.currentText()
        key = "net_pnl_pct" if "%" in sort_mode else "pnl_amount"

        items = []
        for r in pos_rows:
            try:
                code = r.get("code", "")
                name = name_cache.get(code, code)[:6]
                pnl_pct = float(r.get("net_pnl_pct", r.get("pnl_pct", 0)))
                pnl_amt = float(r.get("pnl_amount", 0))
                val = pnl_pct if "%" in sort_mode else pnl_amt
                items.append((name, val, pnl_pct))
            except Exception:
                continue

        items.sort(key=lambda x: x[1])
        if not items:
            return

        names = [it[0] for it in items]
        values = [it[1] for it in items]
        colors = [QColor(C_GREEN) if v >= 0 else QColor(C_RED) for v in values]
        brushes = [pg.mkBrush(c) for c in colors]

        ys = list(range(len(values)))
        bg = pg.BarGraphItem(x0=0, y=ys, width=values, height=0.6, brushes=brushes)
        self._pnl_plot.addItem(bg)

        # Y-axis labels
        ticks = [(i, n) for i, n in enumerate(names)]
        ay = self._pnl_plot.getAxis("left")
        ay.setTicks([ticks])

        # Stats
        if items:
            top = max(items, key=lambda x: x[1])
            bot = min(items, key=lambda x: x[1])
            self._pnl_stats.setText(f"Top: {top[0]} {top[2]:+.1f}%  |  Bottom: {bot[0]} {bot[2]:+.1f}%")

    def update_heatmap(self, positions_data):
        self._heatmap.set_data(positions_data)

    def _update_equity_intraday(self, mode, pf):
        """1D mode: Portfolio vs KOSPI from minute bars."""
        from datetime import date as _date

        self._equity_plot.clear()
        if not pf:
            self._equity_stats.setText("No portfolio data")
            return

        # Determine intraday dir
        if mode == "paper_test":
            intraday_base = BASE_DIR / "data" / "intraday_test"
        elif mode == "shadow_test":
            intraday_base = BASE_DIR / "data" / "intraday_shadow"
        else:
            intraday_base = BASE_DIR / "data" / "intraday"

        today_str = _date.today().strftime("%Y-%m-%d")
        cash = pf.get("cash", 0)
        prev_close = pf.get("prev_close_equity", 0)
        positions = pf.get("positions", {})

        # Load KOSPI minute bars
        kospi_path = intraday_base / "indices" / "000001.csv"
        kospi_by_time = {}
        if kospi_path.exists():
            try:
                with open(kospi_path, "r", encoding="utf-8-sig") as f:
                    for row in csv.DictReader(f):
                        dt = row.get("datetime", "")
                        if dt.startswith(today_str):
                            try:
                                kospi_by_time[dt[11:16]] = float(row.get("close", 0))
                            except (ValueError, TypeError):
                                pass
            except Exception:
                pass

        # Load stock minute bars
        stock_bars = {}
        for code in positions:
            path = intraday_base / f"{code.zfill(6)}.csv"
            if not path.exists():
                continue
            try:
                with open(path, "r", encoding="utf-8-sig") as f:
                    for row in csv.DictReader(f):
                        dt = row.get("datetime", "")
                        if dt.startswith(today_str):
                            try:
                                stock_bars.setdefault(code, {})[dt[11:16]] = \
                                    float(row.get("close", 0))
                            except (ValueError, TypeError):
                                pass
            except Exception:
                pass

        # Collect all time points
        all_times = set()
        all_times.update(kospi_by_time.keys())
        for bars in stock_bars.values():
            all_times.update(bars.keys())
        if not all_times:
            self._equity_stats.setText("Intraday: no data yet")
            return

        # Filter out pre-market times (before 09:00) to avoid scale distortion
        sorted_times = sorted(t for t in all_times if t >= "09:00")
        if not sorted_times:
            self._equity_stats.setText("Intraday: waiting for market open")
            return

        # Build equity series (cash + sum of position values)
        eq_series = []
        last_prices = {}
        for t in sorted_times:
            equity = cash
            for code, pos in positions.items():
                qty = pos.get("quantity", 0)
                if code in stock_bars and t in stock_bars[code]:
                    last_prices[code] = stock_bars[code][t]
                price = last_prices.get(code, pos.get("avg_price", 0))
                equity += qty * price
            eq_series.append(equity)

        # Build KOSPI series
        kospi_series = []
        last_kospi = 0
        for t in sorted_times:
            if t in kospi_by_time:
                last_kospi = kospi_by_time[t]
            kospi_series.append(last_kospi)

        # Convert to % change
        first_eq = eq_series[0] if eq_series else (prev_close or 1)
        # Use first nonzero KOSPI value as base (pre-market bars have 0)
        first_kospi = next((k for k in kospi_series if k > 0), 1)

        sell_cost = sum(
            pos.get("quantity", 0) * pos.get("avg_price", 0)
            for pos in positions.values()) * SELL_COST_RATE

        eq_pcts = []
        for eq in eq_series:
            adj = eq - sell_cost
            base = prev_close if prev_close > 0 else first_eq
            eq_pcts.append((adj / base - 1) * 100 if base > 0 else 0)

        kospi_pcts = []
        for k in kospi_series:
            if first_kospi > 0 and k > 0:
                kospi_pcts.append((k / first_kospi - 1) * 100)
            else:
                kospi_pcts.append(0)

        # Plot
        x = list(range(len(sorted_times)))
        self._equity_plot.addLegend(offset=(60, 10))
        self._equity_plot.plot(x, eq_pcts, pen=pg.mkPen(C_GREEN, width=2), name="Portfolio")
        if any(v != 0 for v in kospi_pcts):
            self._equity_plot.plot(x, kospi_pcts, pen=pg.mkPen(C_BLUE, width=1.5), name="KOSPI")
        self._equity_plot.addLine(y=0, pen=pg.mkPen("#3a4050", width=1))

        # X-axis time labels
        ax = self._equity_plot.getPlotItem().getAxis("bottom")
        step = max(1, len(sorted_times) // 8)
        tick_labels = [(i, sorted_times[i]) for i in range(0, len(sorted_times), step)]
        ax.setTicks([tick_labels])

        # Stats
        last_eq_pct = eq_pcts[-1] if eq_pcts else 0
        last_kospi_pct = kospi_pcts[-1] if kospi_pcts else 0
        excess = last_eq_pct - last_kospi_pct
        self._equity_stats.setText(
            f"KOSPI: {last_kospi_pct:+.2f}%  |  Portfolio: {last_eq_pct:+.2f}%  |  "
            f"Excess: {excess:+.2f}%p  |  {len(sorted_times)} bars")


# =========================================================================
#  AlertCard — individual priority-colored alert
# =========================================================================
class AlertCard(QFrame):
    PRIORITY_STYLES = {
        "CRITICAL": {"bg": "#2a0808", "border": C_RED, "fg": C_RED},
        "HIGH": {"bg": "#1e0a10", "border": C_ORANGE, "fg": C_ORANGE},
        "MEDIUM": {"bg": "#1e1a0a", "border": C_YELLOW, "fg": C_YELLOW},
        "LOW": {"bg": "#0a1520", "border": C_BLUE, "fg": C_BLUE},
        "INFO": {"bg": "#0a1520", "border": C_BLUE, "fg": C_BLUE},
    }

    def __init__(self, alert_data, parent=None):
        super().__init__(parent)
        priority = alert_data.get("priority", "INFO").upper()
        style = self.PRIORITY_STYLES.get(priority, self.PRIORITY_STYLES["INFO"])

        self.setStyleSheet(
            f"background-color: {style['bg']};"
            f" border-left: 3px solid {style['border']};"
            f" border-top: 1px solid rgba(40,60,100,40);"
            f" border-right: 1px solid rgba(40,60,100,40);"
            f" border-bottom: 1px solid rgba(40,60,100,40);"
            f" border-radius: 6px;"
        )
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 6, 10, 6)
        layout.setSpacing(2)

        # Priority badge + message
        top = QHBoxLayout()
        badge = QLabel(priority)
        badge.setStyleSheet(
            f"color: {style['fg']}; font-size: 9px; font-weight: bold;"
            f" font-family: Consolas; border: none;"
            f" background: rgba(0,0,0,60); padding: 1px 6px; border-radius: 3px;")
        badge.setFixedHeight(16)
        top.addWidget(badge)
        msg = QLabel(alert_data.get("message", ""))
        msg.setStyleSheet(f"color: {C_WHITE}; font-size: 11px; font-weight: bold; border: none;")
        msg.setWordWrap(True)
        top.addWidget(msg, 1)
        layout.addLayout(top)

        detail = alert_data.get("detail", "")
        if detail:
            det = QLabel(detail)
            det.setStyleSheet(f"color: {C_TEXT_SECONDARY}; font-size: 10px; border: none;")
            det.setWordWrap(True)
            layout.addWidget(det)

        hint = alert_data.get("debug_hint", "")
        if hint:
            h = QLabel(f"💡 {hint}")
            h.setStyleSheet(f"color: {C_DIM}; font-size: 9px; font-style: italic; border: none;")
            h.setWordWrap(True)
            layout.addWidget(h)


# =========================================================================
#  AlertStreamWidget — right panel scrollable alerts
# =========================================================================
class AlertStreamWidget(QFrame):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setStyleSheet("background: transparent; border: none;")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(4, 4, 4, 4)
        layout.setSpacing(4)

        title = QLabel("ALERT STREAM")
        title.setStyleSheet(f"color: {C_ORANGE}; font-size: 11px; font-weight: bold; font-family: Consolas; border: none;")
        title.setAlignment(Qt.AlignCenter)
        layout.addWidget(title)

        self._scroll = QScrollArea()
        self._scroll.setWidgetResizable(True)
        self._scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self._container = QWidget()
        self._card_layout = QVBoxLayout(self._container)
        self._card_layout.setContentsMargins(0, 0, 0, 0)
        self._card_layout.setSpacing(4)
        self._card_layout.addStretch()
        self._scroll.setWidget(self._container)
        layout.addWidget(self._scroll)

        # Advisor section
        self._advisor_label = QLabel("")
        self._advisor_label.setStyleSheet(f"color: {C_CYAN}; font-size: 10px; font-family: Consolas; border: none;")
        self._advisor_label.setWordWrap(True)
        self._advisor_label.setVisible(False)
        layout.addWidget(self._advisor_label)

    def update_alerts(self, advisor_dir):
        # Clear old cards
        while self._card_layout.count() > 1:  # keep stretch
            item = self._card_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        alerts = []
        if advisor_dir:
            alerts.extend(read_json_list_safe(advisor_dir / "alerts.json"))
            alerts.extend(read_json_list_safe(advisor_dir / "intraday_alerts.json"))

        # Sort by priority
        priority_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3, "INFO": 4}
        alerts.sort(key=lambda a: priority_order.get(a.get("priority", "INFO").upper(), 5))
        alerts = alerts[:10]

        if not alerts:
            no_alert = QLabel("No alerts")
            no_alert.setStyleSheet(f"color: {C_GREEN}; font-size: 11px; font-family: Consolas; border: none;")
            no_alert.setAlignment(Qt.AlignCenter)
            self._card_layout.insertWidget(0, no_alert)
        else:
            for i, alert in enumerate(alerts):
                card = AlertCard(alert)
                self._card_layout.insertWidget(i, card)

        # Advisor phase
        recs = []
        if advisor_dir:
            recs = read_json_list_safe(advisor_dir / "recommendations.json")
        if recs:
            texts = []
            for r in recs[:3]:
                msg = r.get("message", r.get("recommendation", ""))
                conf = r.get("confidence", "")
                texts.append(f"▸ {msg} [{conf}]" if conf else f"▸ {msg}")
            self._advisor_label.setText("AI ADVISOR\n" + "\n".join(texts))
            self._advisor_label.setVisible(True)
        else:
            self._advisor_label.setVisible(False)


# =========================================================================
#  PositionTile — single stock card
# =========================================================================
class PositionTile(QFrame):
    TILE_W = 155
    TILE_H = 95

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedSize(self.TILE_W, self.TILE_H)
        self._data = {}
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 6, 8, 6)
        layout.setSpacing(2)
        self._name = QLabel("")
        self._name.setStyleSheet(f"color: {C_WHITE}; font-size: 10px; font-weight: bold; border: none;")
        layout.addWidget(self._name)
        mid = QHBoxLayout()
        self._pnl = QLabel("")
        self._pnl.setStyleSheet(f"color: {C_GREEN}; font-size: 16px; font-weight: bold; font-family: Consolas; border: none;")
        mid.addWidget(self._pnl)
        mid.addStretch()
        self._warn = QLabel("")
        self._warn.setFixedSize(18, 18)
        self._warn.setAlignment(Qt.AlignCenter)
        self._warn.setVisible(False)
        mid.addWidget(self._warn)
        layout.addLayout(mid)
        bot = QHBoxLayout()
        self._hold = QLabel("")
        self._hold.setStyleSheet(f"color: {C_DIM}; font-size: 9px; font-family: Consolas; border: none;")
        bot.addWidget(self._hold)
        self._gap = QLabel("")
        self._gap.setStyleSheet(f"color: {C_DIM}; font-size: 9px; font-family: Consolas; border: none;")
        bot.addWidget(self._gap)
        layout.addLayout(bot)

    def sizeHint(self):
        return QSize(self.TILE_W, self.TILE_H)

    def set_data(self, data):
        self._data = data
        name = data.get("name", data.get("code", "?"))
        if len(name) > 7:
            name = name[:7]
        self._name.setText(name)

        pnl = data.get("pnl_pct", 0)
        clr = C_GREEN if pnl >= 0 else C_RED
        self._pnl.setText(f"{pnl:+.1f}%")
        self._pnl.setStyleSheet(f"color: {clr}; font-size: 16px; font-weight: bold; font-family: Consolas; border: none;")

        hold = data.get("hold_days", 0)
        self._hold.setText(f"Hold {hold}d")

        gap = data.get("trail_gap", 99)
        gap_text = f"Gap {gap:.1f}%" if gap < 99 else "Gap -"
        gap_clr = C_RED if gap < 2 else (C_YELLOW if gap < 5 else C_DIM)
        self._gap.setText(gap_text)
        self._gap.setStyleSheet(f"color: {gap_clr}; font-size: 9px; font-family: Consolas; border: none;")

        # Warning icon
        if gap < 2:
            self._warn.setText("!")
            self._warn.setStyleSheet(
                f"color: white; background: {C_RED}; border-radius: 9px;"
                " font-size: 11px; font-weight: bold;")
            self._warn.setVisible(True)
        else:
            self._warn.setVisible(False)

        # Background tint
        if pnl >= 0:
            alpha = min(int(abs(pnl) * 4), 40)
            bg = f"rgba(0,180,100,{alpha})"
        else:
            alpha = min(int(abs(pnl) * 4), 40)
            bg = f"rgba(200,40,50,{alpha})"

        border_clr = C_RED if gap < 2 else "rgba(40,80,140,60)"
        border_w = "2px" if gap < 2 else "1px"
        self.setStyleSheet(
            f"background-color: {bg}; border: {border_w} solid {border_clr}; border-radius: 8px;")

        # Tooltip — rich HTML so Qt keeps it visible while hovered
        sector = data.get("sector", "")
        entry = data.get("entry_price", "")
        current = data.get("current_price", "")
        hwm = data.get("hwm", "")
        trail = data.get("trail_stop", "")
        code = data.get("code", "")
        tip_lines = [f"<b>{data.get('name', code)} ({code})</b>"]
        if sector:
            tip_lines.append(f"Sector: {sector}")
        if entry:
            tip_lines.append(f"Entry: {fmt_krw(entry)}&nbsp;&nbsp;Current: {fmt_krw(current)}")
        if hwm:
            tip_lines.append(f"HWM: {fmt_krw(hwm)}&nbsp;&nbsp;Trail: {fmt_krw(trail)}")
        tip_lines.append(f"PnL: {pnl:+.2f}%&nbsp;&nbsp;Hold: {hold}d&nbsp;&nbsp;Gap: {gap:.1f}%")
        self.setToolTip("<br>".join(tip_lines))
        self.setToolTipDuration(0)  # 0 = stay until mouse leaves


# =========================================================================
#  PositionGridWidget — 20-tile grid with sort controls
# =========================================================================
class PositionGridWidget(QFrame):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setStyleSheet("background: transparent; border: none;")
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(4, 4, 4, 4)
        main_layout.setSpacing(4)

        # Header
        header = QHBoxLayout()
        title = QLabel("POSITION GRID")
        title.setStyleSheet(f"color: {C_CYAN}; font-size: 11px; font-weight: bold; font-family: Consolas; border: none;")
        header.addWidget(title)
        header.addStretch()
        self._sort_combo = QComboBox()
        self._sort_combo.addItems(["PnL %", "Trail Gap", "Hold Days"])
        self._sort_combo.setFixedWidth(100)
        header.addWidget(self._sort_combo)
        self._count_label = QLabel("0 / 20")
        self._count_label.setStyleSheet(f"color: {C_DIM}; font-size: 10px; font-family: Consolas; border: none;")
        header.addWidget(self._count_label)
        main_layout.addLayout(header)

        # Scroll area with grid layout (3 columns fixed)
        self._scroll = QScrollArea()
        self._scroll.setWidgetResizable(True)
        self._scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self._container = QWidget()
        self._grid = QGridLayout(self._container)
        self._grid.setContentsMargins(4, 4, 4, 4)
        self._grid.setSpacing(6)
        self._scroll.setWidget(self._container)
        main_layout.addWidget(self._scroll)

        self._tiles = []

    def update_data(self, pf, pos_rows, name_cache, sector_cache):
        # Clear old tiles
        while self._grid.count():
            item = self._grid.takeAt(0)
            if item and item.widget():
                item.widget().deleteLater()
        self._tiles.clear()

        positions = (pf or {}).get("positions", {})
        if not positions:
            lbl = QLabel("No positions")
            lbl.setStyleSheet(f"color: {C_DIM}; font-size: 12px; font-family: Consolas; border: none;")
            lbl.setFixedSize(200, 40)
            self._grid.addWidget(lbl, 0, 0)
            self._count_label.setText("0 / 20")
            return

        # Build position data list
        pos_map = {}
        for r in (pos_rows or []):
            code = r.get("code", "")
            if code:
                pos_map[code] = r

        items = []
        for code, pos in positions.items():
            name = name_cache.get(code, code)
            sector = sector_cache.get(code, "")
            qty = pos.get("quantity", 0)
            avg = pos.get("avg_price", 0)
            current = pos.get("current_price", avg)
            hwm = pos.get("high_watermark", 0)
            trail_stop = pos.get("trail_stop_price", 0)
            entry_date = pos.get("entry_date", "")

            # Use CSV data if available
            csv_row = pos_map.get(code, {})
            pnl_pct = 0
            try:
                raw = float(csv_row.get("net_pnl_pct", csv_row.get("pnl_pct", 0)))
                # CSV stores as decimal ratio (0.10 = 10%), convert to percent
                pnl_pct = raw * 100
            except Exception:
                if avg > 0 and current > 0:
                    pnl_pct = (current / avg - 1) * 100

            trail_gap = 99.0
            if trail_stop > 0 and current > 0:
                trail_gap = (current / trail_stop - 1) * 100

            hold_days = 0
            try:
                hold_days = int(csv_row.get("hold_days", 0))
            except Exception:
                if entry_date:
                    try:
                        hold_days = (datetime.now() - datetime.strptime(entry_date, "%Y-%m-%d")).days
                    except Exception:
                        pass

            items.append({
                "code": code, "name": name, "sector": sector,
                "pnl_pct": pnl_pct, "trail_gap": trail_gap, "hold_days": hold_days,
                "entry_price": avg, "current_price": current,
                "hwm": hwm, "trail_stop": trail_stop,
            })

        # Sort
        sort_mode = self._sort_combo.currentText()
        if "Trail" in sort_mode:
            items.sort(key=lambda x: x["trail_gap"])
        elif "Hold" in sort_mode:
            items.sort(key=lambda x: -x["hold_days"])
        else:
            items.sort(key=lambda x: -x["pnl_pct"])

        for i, item in enumerate(items):
            tile = PositionTile()
            tile.set_data(item)
            row, col = divmod(i, 3)
            self._grid.addWidget(tile, row, col)
            self._tiles.append(tile)

        self._count_label.setText(f"{len(items)} / 20")


# =========================================================================
#  BottomDetailBar — 3-tab (Trades / Pending / Logs)
# =========================================================================
class BottomDetailBar(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        # Tab 1: Recent Trades
        self._trades_table = QTableWidget()
        self._trades_table.setColumnCount(9)
        self._trades_table.setHorizontalHeaderLabels(
            ["Date", "Code", "Name", "Side", "Qty", "Price", "Cost", "Mode", "Event"])
        self._trades_table.horizontalHeader().setStretchLastSection(True)
        self._trades_table.verticalHeader().setVisible(False)
        self._trades_table.setAlternatingRowColors(True)
        self._trades_table.setSortingEnabled(True)
        self._trades_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.tabs.addTab(self._trades_table, "Recent Trades")

        # Tab 2: Pending Buys
        self._pending_widget = QWidget()
        pending_layout = QVBoxLayout(self._pending_widget)
        pending_layout.setContentsMargins(4, 4, 4, 4)
        self._pending_label = QLabel("")
        self._pending_label.setStyleSheet(f"color: {C_YELLOW}; font-size: 11px; font-weight: bold; border: none;")
        self._pending_label.setVisible(False)
        pending_layout.addWidget(self._pending_label)
        self._pending_table = QTableWidget()
        self._pending_table.setColumnCount(4)
        self._pending_table.setHorizontalHeaderLabels(["Rank", "Name", "Target Amt", "Signal"])
        self._pending_table.horizontalHeader().setStretchLastSection(True)
        self._pending_table.verticalHeader().setVisible(False)
        self._pending_table.setEditTriggers(QTableWidget.NoEditTriggers)
        pending_layout.addWidget(self._pending_table)
        self.tabs.addTab(self._pending_widget, "Pending Buys")

        # Tab 3: System Logs
        self._log_view = QTextEdit()
        self._log_view.setReadOnly(True)
        self.tabs.addTab(self._log_view, "System Logs")

    def update_trades(self, report_dir, name_cache):
        rows = read_csv_tail(report_dir / "trades.csv", 30)
        rows.reverse()
        self._trades_table.setRowCount(len(rows))
        for i, r in enumerate(rows):
            code = r.get("code", "")
            side = r.get("side", "")
            items = [
                r.get("date", ""),
                code,
                name_cache.get(code, ""),
                side,
                r.get("quantity", r.get("qty", "")),
                fmt_krw(r.get("price", "")),
                fmt_krw(r.get("cost", r.get("amount", ""))),
                r.get("mode", ""),
                r.get("event_id", ""),
            ]
            for j, val in enumerate(items):
                item = QTableWidgetItem(str(val))
                item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                if j == 3:
                    clr = QColor(C_GREEN) if "BUY" in side.upper() else QColor(C_RED)
                    item.setForeground(clr)
                    item.setFont(QFont("Consolas", 11, QFont.Bold))
                elif j == 1:
                    item.setForeground(QColor(C_CYAN))
                self._trades_table.setItem(i, j, item)

    def update_pending(self, rt, name_cache):
        pending = (rt or {}).get("pending_buys", [])
        if not pending:
            self._pending_label.setVisible(False)
            self._pending_table.setRowCount(0)
            return
        sell_status = (rt or {}).get("rebal_sell_status", "")
        self._pending_label.setText(f"⏳ PENDING BUYS: {len(pending)} stocks (sell_status={sell_status})")
        self._pending_label.setVisible(True)
        self._pending_table.setRowCount(len(pending))
        for i, pb in enumerate(pending):
            code = pb.get("code", "")
            items = [
                str(pb.get("rank", i + 1)),
                f"{name_cache.get(code, code)} ({code})",
                fmt_krw(pb.get("target_amount", "")),
                pb.get("signal_date", ""),
            ]
            for j, val in enumerate(items):
                item = QTableWidgetItem(val)
                item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                if j == 1:
                    item.setForeground(QColor(C_YELLOW))
                self._pending_table.setItem(i, j, item)

    def update_logs(self, mode):
        lines = read_log_tail(mode, 40)
        html_parts = []
        for ln in lines:
            if "[CRITICAL]" in ln:
                html_parts.append(f'<span style="color:{C_RED};font-weight:bold">{ln}</span>')
            elif "[ERROR]" in ln:
                html_parts.append(f'<span style="color:{C_YELLOW}">{ln}</span>')
            elif "[WARNING]" in ln:
                html_parts.append(f'<span style="color:{C_DIM}">{ln}</span>')
            else:
                html_parts.append(f'<span style="color:{C_TEXT_SECONDARY}">{ln}</span>')
        self._log_view.setHtml("<br>".join(html_parts) if html_parts else
                               f'<span style="color:{C_DIM}">No log entries</span>')


# =========================================================================
#  SwingSimWidget — 3-Window Swing Simulator (separate Dock)
# =========================================================================
class SwingSimWidget(QWidget):
    """3-strategy swing simulator widget. Reads CSV data only."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setStyleSheet("background: transparent; border: none;")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(4, 4, 4, 4)
        layout.setSpacing(4)

        # Controls
        ctrl = QHBoxLayout()
        title = QLabel("SWING SIMULATOR")
        title.setStyleSheet(f"color: {C_CYAN}; font-size: 11px; font-weight: bold; font-family: Consolas; border: none;")
        ctrl.addWidget(title)
        ctrl.addStretch()
        self._date_combo = QComboBox()
        self._date_combo.setFixedWidth(100)
        ctrl.addWidget(self._date_combo)
        run_btn = QPushButton("Run")
        run_btn.setFixedWidth(60)
        run_btn.setStyleSheet(
            f"background: rgba(30,50,90,200); color: {C_WHITE};"
            " border: 1px solid rgba(40,80,140,100); border-radius: 4px;"
            " padding: 3px 8px; font-size: 11px; font-weight: bold;")
        run_btn.clicked.connect(self._run)
        ctrl.addWidget(run_btn)
        self._status = QLabel("")
        self._status.setStyleSheet(f"color: {C_DIM}; font-size: 9px; font-family: Consolas; border: none;")
        ctrl.addWidget(self._status)
        layout.addLayout(ctrl)

        # 3 panels side by side
        from PyQt5.QtWidgets import QSplitter
        splitter = QSplitter(Qt.Horizontal)
        self._panels = []
        configs = [
            ("A Conservative", "TP:1.0% SL:-0.5%", C_BLUE),
            ("B Aggressive", "TP:2.0% SL:-1.0%", C_YELLOW),
            ("C Dynamic", "TP:1.5%→6.0% Trail", C_GREEN),
        ]
        for t, d, a in configs:
            p = self._make_panel(t, d, a)
            splitter.addWidget(p["frame"])
            self._panels.append(p)
        layout.addWidget(splitter)
        self._name_cache = {}
        self.refresh_dates()
        # Auto-run if data exists
        if self._date_combo.currentText() and self._date_combo.currentText() != "(no data)":
            self._run()

    def _make_panel(self, title, desc, accent):
        frame = QFrame()
        frame.setStyleSheet(CARD_STYLE)
        lo = QVBoxLayout(frame)
        lo.setContentsMargins(8, 6, 8, 6)
        lo.setSpacing(4)
        hdr = QHBoxLayout()
        lbl = QLabel(title)
        lbl.setStyleSheet(f"color: {accent}; font-size: 11px; font-weight: bold; font-family: Consolas; border: none;")
        hdr.addWidget(lbl)
        hdr.addStretch()
        d = QLabel(desc)
        d.setStyleSheet(f"color: {C_DIM}; font-size: 9px; font-family: Consolas; border: none;")
        hdr.addWidget(d)
        lo.addLayout(hdr)
        kpi_lo = QHBoxLayout()
        kpis = {}
        for key, label in [("trades", "Trades"), ("win", "Win%"), ("pnl", "PnL"), ("pct", "PnL%")]:
            vl = QVBoxLayout()
            kl = QLabel(label)
            kl.setStyleSheet(f"color: {C_DIM}; font-size: 8px; font-family: Consolas; border: none;")
            vl.addWidget(kl)
            kv = QLabel("-")
            kv.setStyleSheet(f"color: {C_WHITE}; font-size: 13px; font-weight: bold; font-family: Consolas; border: none;")
            vl.addWidget(kv)
            kpi_lo.addLayout(vl)
            kpis[key] = kv
        lo.addLayout(kpi_lo)
        table = QTableWidget()
        table.setColumnCount(6)
        table.setHorizontalHeaderLabels(["Time", "Name", "Reason", "Qty", "PnL", "PnL%"])
        table.horizontalHeader().setStretchLastSection(True)
        table.verticalHeader().setVisible(False)
        table.setEditTriggers(QTableWidget.NoEditTriggers)
        lo.addWidget(table)
        return {"frame": frame, "kpis": kpis, "table": table}

    def refresh_dates(self):
        try:
            from swing_simulator_gui import available_dates
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
        except Exception:
            self._date_combo.clear()
            self._date_combo.addItem("(no data)")

    def _run(self):
        date_str = self._date_combo.currentText()
        if not date_str or date_str == "(no data)":
            self._status.setText("No data")
            return
        try:
            from swing_simulator_gui import (
                load_ranking, run_fixed_strategy, run_dynamic_strategy,
                calc_summary, load_name_cache as _lnc,
            )
        except ImportError as e:
            self._status.setText(f"Import error: {e}")
            print(f"[SwingSim] import error: {e}")
            return
        if not self._name_cache:
            self._name_cache = _lnc()
        rankings = load_ranking(date_str)
        if not rankings:
            self._status.setText(f"No ranking for {date_str}")
            return
        strats = [
            ("A", lambda: run_fixed_strategy(rankings, date_str, self._name_cache, 1.0, -0.5)),
            ("B", lambda: run_fixed_strategy(rankings, date_str, self._name_cache, 2.0, -1.0)),
            ("C", lambda: run_dynamic_strategy(rankings, date_str, self._name_cache)),
        ]
        best_pnl, best_name = -1e18, ""
        for i, (nm, fn) in enumerate(strats):
            trades = fn()
            s = calc_summary(trades)
            p = self._panels[i]
            p["kpis"]["trades"].setText(str(s.total_trades))
            wr_c = C_GREEN if s.win_rate >= 60 else (C_YELLOW if s.win_rate >= 50 else C_RED)
            p["kpis"]["win"].setText(f"{s.win_rate:.0f}%")
            p["kpis"]["win"].setStyleSheet(f"color: {wr_c}; font-size: 13px; font-weight: bold; font-family: Consolas; border: none;")
            pc = C_GREEN if s.total_pnl >= 0 else C_RED
            p["kpis"]["pnl"].setText(f"{s.total_pnl:+,.0f}")
            p["kpis"]["pnl"].setStyleSheet(f"color: {pc}; font-size: 13px; font-weight: bold; font-family: Consolas; border: none;")
            p["kpis"]["pct"].setText(f"{s.total_pnl_pct:+.2f}%")
            p["kpis"]["pct"].setStyleSheet(f"color: {pc}; font-size: 13px; font-weight: bold; font-family: Consolas; border: none;")
            tbl = p["table"]
            tbl.setRowCount(len(trades))
            for j, t in enumerate(trades):
                vals = [f"{t.entry_time}→{t.exit_time}", t.name[:6], t.exit_reason,
                        str(t.qty), f"{t.pnl:+,.0f}", f"{t.pnl_pct:+.2f}%"]
                for k, v in enumerate(vals):
                    item = QTableWidgetItem(v)
                    item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                    if k == 2:
                        rc = {"TP": C_GREEN, "SL": C_RED, "HARD_STOP": C_RED,
                              "BREAKDOWN": C_ORANGE, "EOD": C_DIM}
                        item.setForeground(QColor(rc.get(t.exit_reason, C_WHITE)))
                    elif k >= 4:
                        item.setForeground(QColor(C_GREEN if t.pnl >= 0 else C_RED))
                    tbl.setItem(j, k, item)
            if s.total_pnl > best_pnl:
                best_pnl, best_name = s.total_pnl, nm
        self._status.setText(f"{date_str} | Best: {best_name} ({best_pnl:+,.0f})")


# =========================================================================
#  Gen4MonitorV2 — Main Window (Mission Control)
# =========================================================================
class Gen4MonitorV2(QMainWindow):
    def __init__(self, mode="paper"):
        super().__init__()
        self.mode = mode
        self.report_dir = REPORT_DIR_TEST if mode == "paper_test" else REPORT_DIR
        self._name_cache = load_name_cache()
        self._sector_cache = load_sector_cache()
        self._last_pf = None
        self._last_rt = None
        self._advisor_count = 0
        self._last_advisor_dir = None

        self.setWindowTitle(f"Q-TRON Mission Control [{mode.upper()}]")
        self.resize(1600, 900)
        self.setStyleSheet(STYLE)

        # ── Central Widget: Heartbeat + Hero Chart ──
        central = QWidget()
        central_layout = QVBoxLayout(central)
        central_layout.setContentsMargins(0, 0, 0, 0)
        central_layout.setSpacing(0)
        self.heartbeat = HeartbeatStrip()
        central_layout.addWidget(self.heartbeat)
        self.hero_chart = HeroChartWidget()
        central_layout.addWidget(self.hero_chart)
        self.setCentralWidget(central)

        # ── Create widgets ──
        self.decision_hub = DecisionHubWidget()
        # hero_chart already created above in central widget
        self.position_grid = PositionGridWidget()
        self.alert_stream = AlertStreamWidget()
        self.bottom_detail = BottomDetailBar()
        self.swing_sim_widget = SwingSimWidget()
        try:
            from strategy_lab_gui import StrategyLabWidget
            self.strategy_lab_widget = StrategyLabWidget()
            self._has_strategy_lab = True
        except Exception as e:
            import traceback; traceback.print_exc()
            print(f"[StrategyLab] import/init FAILED: {e}")
            self.strategy_lab_widget = None
            self._has_strategy_lab = False

        # ── Create Docks ──
        self.dock_decision = self._make_dock("Decision Hub", self.decision_hub)
        self.dock_positions = self._make_dock("Positions", self.position_grid)
        self.dock_alerts = self._make_dock("Alerts", self.alert_stream)
        self.dock_bottom = self._make_dock("Detail", self.bottom_detail)
        self.dock_swing = self._make_dock("Swing Sim", self.swing_sim_widget)
        if self._has_strategy_lab:
            self.dock_lab = self._make_dock("Strategy Lab", self.strategy_lab_widget)

        # ── Arrange Docks ──
        # Layout: Decision | [Central: Heartbeat+Hero] | Positions | Alerts
        #         Detail   | Swing Sim | Strategy Lab (tabified bottom)

        # Left: Decision Hub (narrow sidebar)
        self.addDockWidget(Qt.LeftDockWidgetArea, self.dock_decision)

        # Right: Positions
        self.addDockWidget(Qt.RightDockWidgetArea, self.dock_positions)

        # Right of Positions: Alerts
        self.addDockWidget(Qt.RightDockWidgetArea, self.dock_alerts)
        self.splitDockWidget(self.dock_positions, self.dock_alerts, Qt.Horizontal)

        # Bottom corners belong to bottom area (full width)
        self.setCorner(Qt.BottomLeftCorner, Qt.BottomDockWidgetArea)
        self.setCorner(Qt.BottomRightCorner, Qt.BottomDockWidgetArea)

        # Bottom: Detail + Swing Sim + Strategy Lab (tabified, full width)
        self.addDockWidget(Qt.BottomDockWidgetArea, self.dock_bottom)
        self.addDockWidget(Qt.BottomDockWidgetArea, self.dock_swing)
        self.tabifyDockWidget(self.dock_bottom, self.dock_swing)
        if self._has_strategy_lab:
            self.addDockWidget(Qt.BottomDockWidgetArea, self.dock_lab)
            self.tabifyDockWidget(self.dock_swing, self.dock_lab)

        # Proportions
        self.resizeDocks(
            [self.dock_decision, self.dock_positions, self.dock_alerts],
            [150, 380, 390], Qt.Horizontal)
        self.resizeDocks(
            [self.dock_positions, self.dock_bottom],
            [520, 280], Qt.Vertical)

        # ── Menu ──
        view_menu = self.menuBar().addMenu("View")
        _all_docks = [self.dock_decision, self.dock_positions,
                      self.dock_alerts, self.dock_bottom, self.dock_swing]
        if self._has_strategy_lab:
            _all_docks.append(self.dock_lab)
        for dock in _all_docks:
            view_menu.addAction(dock.toggleViewAction())
        view_menu.addSeparator()
        reset_action = QAction("Reset Layout", self)
        reset_action.triggered.connect(self._reset_layout)
        view_menu.addAction(reset_action)

        # ── Restore layout ──
        self._restore_layout()

        # ── Timer ──
        self._timer = QTimer()
        self._timer.timeout.connect(self.refresh)
        self._timer.start(REFRESH_MS)
        self.refresh()

    def _make_dock(self, title, widget):
        dock = QDockWidget(title, self)
        dock.setWidget(widget)
        dock.setFeatures(
            QDockWidget.DockWidgetMovable |
            QDockWidget.DockWidgetFloatable |
            QDockWidget.DockWidgetClosable)
        return dock

    def _restore_layout(self):
        if LAYOUT_STATE_PATH.exists():
            try:
                state = LAYOUT_STATE_PATH.read_bytes()
                self.restoreState(QByteArray(state))
            except Exception:
                pass

    def _reset_layout(self):
        """Reset to default screenshot-reference layout."""
        try:
            LAYOUT_STATE_PATH.unlink(missing_ok=True)
        except Exception:
            pass
        # Re-show all docks and re-arrange
        all_docks = [self.dock_decision, self.dock_hero, self.dock_positions,
                     self.dock_alerts, self.dock_bottom, self.dock_swing]
        if self._has_strategy_lab:
            all_docks.append(self.dock_lab)
        for dock in all_docks:
            dock.setVisible(True)
            dock.setFloating(False)
        # Re-apply default arrangement
        self.addDockWidget(Qt.LeftDockWidgetArea, self.dock_decision)
        self.addDockWidget(Qt.RightDockWidgetArea, self.dock_hero)
        self.addDockWidget(Qt.RightDockWidgetArea, self.dock_positions)
        self.splitDockWidget(self.dock_hero, self.dock_positions, Qt.Horizontal)
        self.addDockWidget(Qt.RightDockWidgetArea, self.dock_alerts)
        self.splitDockWidget(self.dock_positions, self.dock_alerts, Qt.Horizontal)
        self.addDockWidget(Qt.BottomDockWidgetArea, self.dock_bottom)
        self.addDockWidget(Qt.BottomDockWidgetArea, self.dock_swing)
        self.tabifyDockWidget(self.dock_bottom, self.dock_swing)
        if self._has_strategy_lab:
            self.addDockWidget(Qt.BottomDockWidgetArea, self.dock_lab)
            self.tabifyDockWidget(self.dock_swing, self.dock_lab)
        self.resizeDocks(
            [self.dock_decision, self.dock_hero, self.dock_positions, self.dock_alerts],
            [150, 480, 380, 390], Qt.Horizontal)
        self.resizeDocks(
            [self.dock_hero, self.dock_bottom],
            [520, 280], Qt.Vertical)

    def closeEvent(self, event):
        try:
            STATE_DIR.mkdir(parents=True, exist_ok=True)
            LAYOUT_STATE_PATH.write_bytes(bytes(self.saveState()))
        except Exception:
            pass
        super().closeEvent(event)

    # ── Main Refresh Loop ──
    def refresh(self):
        # 1. Load state files
        pf_path = STATE_DIR / f"portfolio_state_{self.mode}.json"
        rt_path = STATE_DIR / f"runtime_state_{self.mode}.json"
        pf = read_json_safe(pf_path)
        rt = read_json_safe(rt_path)
        if pf:
            self._last_pf = pf
        else:
            pf = self._last_pf
        if rt:
            self._last_rt = rt
        else:
            rt = self._last_rt

        # 2. Load CSVs
        eq_rows = read_csv_tail(self.report_dir / "equity_log.csv", 1)
        all_eq = read_csv_all(self.report_dir / "equity_log.csv")
        pos_rows = read_csv_all(self.report_dir / "daily_positions.csv")
        # Filter to latest date
        if pos_rows:
            latest_date = pos_rows[-1].get("date", "")
            pos_rows = [r for r in pos_rows if r.get("date", "") == latest_date]

        # 3. Compute derived values
        ts = (pf or {}).get("timestamp")
        age_text, stale_level = age_str(ts)
        risk_mode = eq_rows[-1].get("risk_mode", "UNKNOWN") if eq_rows else "UNKNOWN"
        regime = eq_rows[-1].get("regime", "") if eq_rows else ""
        # Fallback: derive regime from KOSPI intraday if equity_log has no regime
        if not regime:
            try:
                kospi_idx = INTRADAY_DIR / "indices" / "000001.csv"
                if kospi_idx.exists():
                    import csv as _csv
                    with open(kospi_idx, "r", encoding="utf-8-sig") as _kf:
                        _krows = list(_csv.DictReader(_kf))
                    if _krows:
                        _last_k = float(_krows[-1].get("close", 0))
                        if _last_k > 0:
                            regime = f"SIDE (KOSPI={_last_k:,.0f})"
            except Exception:
                pass
        recon_unreliable = (rt or {}).get("recon_unreliable", False)
        status, reasons, is_critical = compute_decision(risk_mode, recon_unreliable, stale_level)
        color = decision_color(status)
        engine_running = stale_level == 0

        # 4. Ensure names for all position codes
        all_codes = list((pf or {}).get("positions", {}).keys())
        for r in pos_rows:
            c = r.get("code", "")
            if c and c not in all_codes:
                all_codes.append(c)
        if all_codes:
            self._name_cache = ensure_names(all_codes, self._name_cache)

        # 5. Update widgets
        self.heartbeat.update_state(stale_level, age_text, risk_mode, regime, engine_running)
        self.decision_hub.update_state(status, reasons, color, eq_rows, all_eq, pf)

        # Hero chart
        self.hero_chart.update_equity(all_eq, mode=self.mode, pf=pf)
        self.hero_chart.update_pnl_waterfall(pos_rows, self._name_cache)

        # Heatmap data
        heatmap_data = []
        positions = (pf or {}).get("positions", {})
        for code, pos in positions.items():
            current = pos.get("current_price", pos.get("avg_price", 0))
            trail_stop = pos.get("trail_stop_price", 0)
            trail_gap = (current / trail_stop - 1) * 100 if trail_stop > 0 and current > 0 else 99
            avg = pos.get("avg_price", 0)
            pnl_pct = (current / avg - 1) * 100 if avg > 0 and current > 0 else 0
            # Override with CSV if available
            for r in pos_rows:
                if r.get("code") == code:
                    try:
                        raw = float(r.get("net_pnl_pct", r.get("pnl_pct", pnl_pct / 100)))
                        pnl_pct = raw * 100  # CSV stores decimal ratio
                    except Exception:
                        pass
                    break
            heatmap_data.append({
                "code": code,
                "name": self._name_cache.get(code, code),
                "trail_gap": trail_gap,
                "pnl_pct": pnl_pct,
            })
        heatmap_data.sort(key=lambda x: x["trail_gap"])
        self.hero_chart.update_heatmap(heatmap_data)

        # Position grid
        self.position_grid.update_data(pf, pos_rows, self._name_cache, self._sector_cache)

        # Bottom detail
        self.bottom_detail.update_trades(self.report_dir, self._name_cache)
        self.bottom_detail.update_pending(rt, self._name_cache)
        self.bottom_detail.update_logs(self.mode)

        # 6. Advisor (every 10th refresh = 30s)
        self._advisor_count += 1
        if self._advisor_count % 10 == 1:
            adv_dir = find_latest_advisor_dir()
            self._last_advisor_dir = adv_dir
            self.alert_stream.update_alerts(adv_dir)
            adv_dir_found = adv_dir  # keep for below

        # 7. Swing sim & Strategy Lab: refresh aggressively when (no data)
        if hasattr(self, 'swing_sim_widget'):
            combo_text = self.swing_sim_widget._date_combo.currentText()
            needs_refresh = (combo_text == "(no data)" or not combo_text
                             or self._advisor_count % 10 == 1)
            if needs_refresh:
                try:
                    self.swing_sim_widget.refresh_dates()
                    today = date.today().strftime("%Y%m%d")
                    if self.swing_sim_widget._date_combo.currentText() == today:
                        self.swing_sim_widget._run()
                except Exception:
                    pass
        if self._has_strategy_lab and self.strategy_lab_widget:
            try:
                combo_text = self.strategy_lab_widget._date_combo.currentText()
                needs_refresh = (not combo_text or combo_text == "(no data)"
                                 or self._advisor_count % 10 == 1)
                if needs_refresh:
                    self.strategy_lab_widget.refresh_dates()
            except Exception:
                pass

        # Statusbar
        n_pos = len(positions)
        self.statusBar().showMessage(
            f"Mode: {self.mode}  |  Positions: {n_pos}/20  |  "
            f"Risk: {risk_mode}  |  Updated: {datetime.now().strftime('%H:%M:%S')}")


# =========================================================================
#  Entry Point
# =========================================================================
def main():
    parser = argparse.ArgumentParser(description="Q-TRON Mission Control Monitor v2")
    parser.add_argument("--mode",
                        choices=["mock", "paper", "paper_test", "live"],
                        default="paper",
                        help="Trading mode (default: paper)")
    args = parser.parse_args()

    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    palette = QPalette()
    palette.setColor(QPalette.Window, QColor(BG_MAIN))
    palette.setColor(QPalette.WindowText, QColor(C_TEXT_PRIMARY))
    palette.setColor(QPalette.Base, QColor(BG_CARD))
    palette.setColor(QPalette.AlternateBase, QColor(BG_TABLE_ALT))
    palette.setColor(QPalette.ToolTipBase, QColor("#1a1e28"))
    palette.setColor(QPalette.ToolTipText, QColor(C_TEXT_PRIMARY))
    palette.setColor(QPalette.Text, QColor(C_TEXT_PRIMARY))
    palette.setColor(QPalette.Button, QColor(BG_CARD))
    palette.setColor(QPalette.ButtonText, QColor(C_TEXT_PRIMARY))
    palette.setColor(QPalette.Highlight, QColor(BG_SELECTION))
    palette.setColor(QPalette.HighlightedText, QColor(C_WHITE))
    app.setPalette(palette)

    win = Gen4MonitorV2(args.mode)
    win.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
