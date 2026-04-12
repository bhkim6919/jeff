"""
Q-TRON Operations Manual PDF Generator
========================================
본인 운영 참고용 전체 시스템 매뉴얼.
"""
import os
import sys
from pathlib import Path
from datetime import datetime

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm, cm
from reportlab.lib.colors import HexColor, black, white, grey
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, KeepTogether, HRFlowable,
)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# ── Font Registration ──────────────────────────────────────
# Try Korean font, fall back to built-in
_FONT_REGISTERED = False
_FONT_NAME = "Helvetica"
_FONT_NAME_B = "Helvetica-Bold"

for font_path in [
    "C:/Windows/Fonts/malgun.ttf",
    "C:/Windows/Fonts/NanumGothic.ttf",
    "/System/Library/Fonts/AppleSDGothicNeo.ttc",
]:
    if os.path.exists(font_path):
        try:
            pdfmetrics.registerFont(TTFont("KR", font_path))
            bold_path = font_path.replace(".ttf", "bd.ttf").replace("Gothic.ttf", "GothicBold.ttf")
            if os.path.exists(bold_path):
                pdfmetrics.registerFont(TTFont("KR-Bold", bold_path))
            else:
                pdfmetrics.registerFont(TTFont("KR-Bold", font_path))
            _FONT_NAME = "KR"
            _FONT_NAME_B = "KR-Bold"
            _FONT_REGISTERED = True
            break
        except Exception:
            continue

# ── Colors ─────────────────────────────────────────────────
C_PRIMARY = HexColor("#1a237e")
C_ACCENT = HexColor("#0d47a1")
C_BG_LIGHT = HexColor("#e8eaf6")
C_BG_CODE = HexColor("#f5f5f5")
C_GREEN = HexColor("#2e7d32")
C_RED = HexColor("#c62828")
C_ORANGE = HexColor("#ef6c00")
C_GREY = HexColor("#616161")
C_ROW_ALT = HexColor("#f0f4ff")

# ── Styles ─────────────────────────────────────────────────
styles = getSampleStyleSheet()

S_TITLE = ParagraphStyle("ManualTitle", parent=styles["Title"],
    fontName=_FONT_NAME_B, fontSize=26, leading=32, textColor=C_PRIMARY,
    spaceAfter=6*mm, alignment=TA_CENTER)

S_SUBTITLE = ParagraphStyle("ManualSubtitle", parent=styles["Normal"],
    fontName=_FONT_NAME, fontSize=12, leading=16, textColor=C_GREY,
    spaceAfter=20*mm, alignment=TA_CENTER)

S_H1 = ParagraphStyle("H1", parent=styles["Heading1"],
    fontName=_FONT_NAME_B, fontSize=18, leading=24, textColor=C_PRIMARY,
    spaceBefore=12*mm, spaceAfter=4*mm)

S_H2 = ParagraphStyle("H2", parent=styles["Heading2"],
    fontName=_FONT_NAME_B, fontSize=14, leading=18, textColor=C_ACCENT,
    spaceBefore=8*mm, spaceAfter=3*mm)

S_H3 = ParagraphStyle("H3", parent=styles["Heading3"],
    fontName=_FONT_NAME_B, fontSize=11, leading=15, textColor=black,
    spaceBefore=4*mm, spaceAfter=2*mm)

S_BODY = ParagraphStyle("Body", parent=styles["Normal"],
    fontName=_FONT_NAME, fontSize=9.5, leading=14, spaceAfter=2*mm)

S_CODE = ParagraphStyle("Code", parent=styles["Code"],
    fontName="Courier", fontSize=8, leading=11,
    backColor=C_BG_CODE, borderPadding=4, spaceAfter=3*mm,
    leftIndent=8, rightIndent=8)

S_NOTE = ParagraphStyle("Note", parent=styles["Normal"],
    fontName=_FONT_NAME, fontSize=8.5, leading=12, textColor=C_GREY,
    leftIndent=8, spaceAfter=2*mm)

S_WARN = ParagraphStyle("Warn", parent=styles["Normal"],
    fontName=_FONT_NAME_B, fontSize=9, leading=13, textColor=C_RED,
    leftIndent=8, spaceAfter=2*mm)

PAGE_W, PAGE_H = A4
MARGIN = 18*mm


def make_table(headers, rows, col_widths=None):
    """Create styled table."""
    data = [headers] + rows
    t = Table(data, colWidths=col_widths, repeatRows=1)
    style_cmds = [
        ("BACKGROUND", (0, 0), (-1, 0), C_PRIMARY),
        ("TEXTCOLOR", (0, 0), (-1, 0), white),
        ("FONTNAME", (0, 0), (-1, 0), _FONT_NAME_B),
        ("FONTSIZE", (0, 0), (-1, 0), 8.5),
        ("FONTNAME", (0, 1), (-1, -1), _FONT_NAME),
        ("FONTSIZE", (0, 1), (-1, -1), 8),
        ("LEADING", (0, 0), (-1, -1), 12),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.5, HexColor("#bdbdbd")),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
    ]
    for i in range(1, len(data)):
        if i % 2 == 0:
            style_cmds.append(("BACKGROUND", (0, i), (-1, i), C_ROW_ALT))
    t.setStyle(TableStyle(style_cmds))
    return t


def hr():
    return HRFlowable(width="100%", thickness=0.5, color=HexColor("#e0e0e0"),
                      spaceBefore=3*mm, spaceAfter=3*mm)


def build_pdf(output_path: str):
    doc = SimpleDocTemplate(
        output_path, pagesize=A4,
        leftMargin=MARGIN, rightMargin=MARGIN,
        topMargin=MARGIN, bottomMargin=MARGIN,
    )
    story = []
    avail_w = PAGE_W - 2 * MARGIN

    # ═══ COVER ═══════════════════════════════════════════════
    story.append(Spacer(1, 40*mm))
    story.append(Paragraph("Q-TRON", S_TITLE))
    story.append(Paragraph("Operations Manual v4.0", ParagraphStyle(
        "Cover2", parent=S_SUBTITLE, fontSize=16, textColor=C_ACCENT, spaceAfter=8*mm)))
    story.append(Paragraph(
        f"Generated: {datetime.now().strftime('%Y-%m-%d')}<br/>"
        "Scope: kr (KR) / us (Alpaca) / Strategy Lab<br/>"
        "Audience: Owner Operations Reference",
        S_SUBTITLE))
    story.append(PageBreak())

    # ═══ TOC ═════════════════════════════════════════════════
    story.append(Paragraph("Table of Contents", S_H1))
    toc_items = [
        "1. System Overview",
        "2. Environment Setup",
        "3. Core Strategy",
        "4. Daily Operations (KR)",
        "5. Daily Operations (US)",
        "6. Strategy Lab",
        "7. State Management (v2)",
        "8. Risk Management",
        "9. Monitoring",
        "10. Fault Recovery",
        "11. Backtest",
        "12. Automation Roadmap",
        "13. Command Reference",
    ]
    for item in toc_items:
        story.append(Paragraph(item, ParagraphStyle(
            "TOC", parent=S_BODY, fontSize=11, leading=18, leftIndent=10*mm)))
    story.append(PageBreak())

    # ═══ 1. SYSTEM OVERVIEW ══════════════════════════════════
    story.append(Paragraph("1. System Overview", S_H1))

    story.append(Paragraph("Architecture", S_H2))
    story.append(Paragraph(
        "Q-TRON is a multi-market quantitative trading system with three active subsystems.",
        S_BODY))

    story.append(make_table(
        ["Subsystem", "Market", "Broker/API", "Python", "Status"],
        [
            ["Gen04", "KR (KOSPI/KOSDAQ)", "Kiwoom OpenAPI+ (COM)", "3.9 32-bit", "LIVE (REST migration)"],
            ["kr", "KR (Lab + REST)", "pykrx / DB", "3.12 64-bit", "ACTIVE"],
            ["us", "US (Russell 1000+)", "Alpaca REST", "3.12 64-bit", "ACTIVE (Paper)"],
        ],
        col_widths=[avail_w*0.15, avail_w*0.20, avail_w*0.25, avail_w*0.15, avail_w*0.25],
    ))

    story.append(Spacer(1, 4*mm))
    story.append(Paragraph("Directory Structure", S_H3))
    story.append(Paragraph(
        "C:\\Q-TRON-32_ARCHIVE\\<br/>"
        "&nbsp;&nbsp;kr-legacy/ ............. KR LIVE engine (Kiwoom COM, deletion planned)<br/>"
        "&nbsp;&nbsp;kr/ ... KR REST API + Strategy Lab (9 strategies)<br/>"
        "&nbsp;&nbsp;us/ ....... US Alpaca engine (paper/live)<br/>"
        "&nbsp;&nbsp;backtest/ .......... Backtest data (2561 stocks, 2019-2026)<br/>"
        "&nbsp;&nbsp;.venv/ .............. Python 3.9 32-bit (KR legacy)<br/>"
        "&nbsp;&nbsp;.venv64/ ........... Python 3.12 64-bit (REST/Lab)",
        S_CODE))

    # ═══ 2. ENVIRONMENT ═════════════════════════════════════
    story.append(PageBreak())
    story.append(Paragraph("2. Environment Setup", S_H1))

    story.append(make_table(
        ["Component", "Path / Value", "Notes"],
        [
            ["Python KR", "C:\\Q-TRON-32_ARCHIVE\\.venv\\Scripts\\python.exe", "3.9 32-bit (Kiwoom COM)"],
            ["Python REST/Lab", "C:\\Q-TRON-32_ARCHIVE\\.venv64\\Scripts\\python.exe", "3.12 64-bit"],
            ["Python US", "C:\\Q-TRON-32_ARCHIVE\\us\\.venv\\Scripts\\python.exe", "3.12 64-bit"],
            ["DB (KR)", "PostgreSQL (kr/data/)", "OHLCV + fundamental"],
            ["DB (US)", "PostgreSQL (us/)", "Separate tables"],
            ["Alpaca API", "paper-api.alpaca.markets", "Paper mode default"],
            ["Kiwoom", "KHOPENAPI.KHOpenAPICtrl.1", "COM OCX (32-bit only)"],
        ],
        col_widths=[avail_w*0.18, avail_w*0.50, avail_w*0.32],
    ))

    story.append(Spacer(1, 4*mm))
    story.append(Paragraph("Required packages: PyQt5, pykrx, pandas, numpy, alpaca-py, "
                           "fastapi, uvicorn, psycopg2, reportlab", S_NOTE))

    # ═══ 3. CORE STRATEGY ═══════════════════════════════════
    story.append(PageBreak())
    story.append(Paragraph("3. Core Strategy", S_H1))
    story.append(Paragraph("LowVol + Momentum 12-1 (Confirmed, LOCKED)", S_H2))

    story.append(make_table(
        ["Parameter", "KR Value", "US Value", "Description"],
        [
            ["VOL_LOOKBACK", "252", "252", "Volatility window (trading days)"],
            ["VOL_PERCENTILE", "0.30 (bottom 30%)", "0.20 (bottom 20%)", "Low-vol filter"],
            ["MOM_LOOKBACK", "252", "252", "Momentum window"],
            ["MOM_SKIP", "22", "22", "Skip last ~1 month"],
            ["N_STOCKS", "20", "20", "Target portfolio size"],
            ["TRAIL_PCT", "-12%", "-12%", "Trailing stop (close-based)"],
            ["REBAL_DAYS", "21", "21", "Rebalance cycle (monthly)"],
            ["CASH_BUFFER", "0.95", "0.95", "Max buy allocation ratio"],
        ],
        col_widths=[avail_w*0.22, avail_w*0.20, avail_w*0.20, avail_w*0.38],
    ))

    story.append(Spacer(1, 4*mm))
    story.append(Paragraph("Cost Model", S_H3))
    story.append(make_table(
        ["", "KR", "US"],
        [
            ["BUY cost", "0.115% (fee 0.015% + slippage 0.10%)", "0.05% (slippage only)"],
            ["SELL cost", "0.295% (fee + slippage + tax 0.18%)", "0.05% (slippage only)"],
        ],
        col_widths=[avail_w*0.15, avail_w*0.45, avail_w*0.40],
    ))

    story.append(Spacer(1, 4*mm))
    story.append(Paragraph("Backtest Results (KR, 7yr)", S_H3))
    story.append(make_table(
        ["Metric", "Result", "Pass Criteria"],
        [
            ["Total Return", "+208.6%", "-"],
            ["CAGR", "17.5%", "-"],
            ["MDD", "-21.9%", "-"],
            ["Sharpe", "1.26", "-"],
            ["OOS (2023-2026)", "CAGR 18.7%", ">= 15%  PASS"],
            ["Slippage x2", "Sharpe 1.69", ">= 1.0  PASS"],
            ["BEAR period", "MDD -18.6%", "<= -25%  PASS"],
            ["Survivorship", "Same performance", "PASS"],
        ],
        col_widths=[avail_w*0.30, avail_w*0.30, avail_w*0.40],
    ))

    story.append(Spacer(1, 3*mm))
    story.append(Paragraph(
        "LOCKED: strategy/scoring.py calc_volatility(), calc_momentum() -- DO NOT MODIFY",
        S_WARN))

    # ═══ 4. DAILY OPS KR ════════════════════════════════════
    story.append(PageBreak())
    story.append(Paragraph("4. Daily Operations (KR)", S_H1))

    story.append(Paragraph("Automated Schedule (Task Scheduler)", S_H2))
    story.append(make_table(
        ["Time", "Task", "Script", "Action"],
        [
            ["07:52", "TradingDayCheck", "00_check_trading_day.bat", "Non-trading day -> shutdown"],
            ["07:55", "MorningBatch", "01_batch_scheduled.bat", "OHLCV + scoring (--fast, ~18min)"],
            ["08:30", "AutoStart", "auto_start.py", "Kiwoom login + LIVE engine"],
            ["08:35", "RESTMonitor", "08_rest_monitor.bat", "REST dashboard (port 8080)"],
            ["16:00", "EOD_Cleanup", "99_eod_shutdown.bat", "Kill processes + shutdown"],
        ],
        col_widths=[avail_w*0.10, avail_w*0.20, avail_w*0.30, avail_w*0.40],
    ))

    story.append(Spacer(1, 4*mm))
    story.append(Paragraph("Manual Commands", S_H2))
    story.append(Paragraph(
        "cd C:\\Q-TRON-32_ARCHIVE\\Gen04<br/><br/>"
        "# Batch (OHLCV + scoring + target)<br/>"
        "..\\.venv\\Scripts\\python.exe main.py --batch<br/><br/>"
        "# Fast batch (signal only, skip reports)<br/>"
        "..\\.venv\\Scripts\\python.exe main.py --batch --fast<br/><br/>"
        "# LIVE trading<br/>"
        "..\\.venv\\Scripts\\python.exe main.py --live<br/><br/>"
        "# Mock (no broker)<br/>"
        "..\\.venv\\Scripts\\python.exe main.py --mock<br/><br/>"
        "# Paper test (isolated state)<br/>"
        "..\\.venv\\Scripts\\python.exe main.py --paper-test --cycle full<br/><br/>"
        "# Backtest<br/>"
        "..\\.venv\\Scripts\\python.exe main.py --backtest --start 2019-01-02",
        S_CODE))

    story.append(Paragraph("LIVE Mode Flow", S_H3))
    story.append(Paragraph(
        "1. Kiwoom COM login (QAxWidget) + account password (pyautogui)<br/>"
        "2. RECON: broker positions vs local state reconciliation<br/>"
        "3. Pending buy execution (T+1 open fill)<br/>"
        "4. Trail stop monitoring (real-time close-based)<br/>"
        "5. Rebalance check (21-day cycle)<br/>"
        "6. EOD: daily/weekly report generation",
        S_BODY))

    # ═══ 5. DAILY OPS US ════════════════════════════════════
    story.append(PageBreak())
    story.append(Paragraph("5. Daily Operations (US)", S_H1))

    story.append(Paragraph("Alpaca Connection", S_H2))
    story.append(Paragraph(
        "Mode: Paper trading (paper-api.alpaca.markets)<br/>"
        "Python: us/.venv/Scripts/python.exe (3.12 64-bit)<br/>"
        "Dashboard: http://localhost:8081 (FastAPI)",
        S_BODY))

    story.append(Paragraph("Commands", S_H3))
    story.append(Paragraph(
        "cd C:\\Q-TRON-32_ARCHIVE\\us<br/><br/>"
        "# Connection test<br/>"
        ".venv\\Scripts\\python.exe main.py --test<br/><br/>"
        "# Batch (OHLCV + universe + scoring)<br/>"
        ".venv\\Scripts\\python.exe main.py --batch<br/><br/>"
        "# Live mode (monitor + trail stops + fills)<br/>"
        ".venv\\Scripts\\python.exe main.py --live<br/><br/>"
        "# Dashboard server<br/>"
        ".venv\\Scripts\\python.exe main.py --server",
        S_CODE))

    story.append(Paragraph("Live Mode Phases", S_H3))
    story.append(Paragraph(
        "Phase 0: Alpaca API health check<br/>"
        "Phase 1: Broker snapshot (account/positions/orders)<br/>"
        "Phase 2: State load + RECON (dirty exit -> FORCE_SYNC)<br/>"
        "Phase 3: Fill monitor (async event loop)<br/>"
        "Monitor: 60s interval (300s when market closed)<br/>"
        "&nbsp;&nbsp;- Drain fills, price updates, trail stops<br/>"
        "&nbsp;&nbsp;- RECON every 10 min (log-only)<br/>"
        "&nbsp;&nbsp;- Periodic save every 5 min",
        S_BODY))

    # ═══ 6. STRATEGY LAB ════════════════════════════════════
    story.append(PageBreak())
    story.append(Paragraph("6. Strategy Lab", S_H1))

    story.append(Paragraph("9 Strategies in 4 Groups", S_H2))
    story.append(make_table(
        ["Group", "Strategy", "Type", "Max Pos", "Key Feature"],
        [
            ["rebal", "momentum_base", "Rebal 21d", "20", "Pure 12-1M momentum"],
            ["rebal", "lowvol_momentum", "Rebal 21d", "20", "LowVol + Mom (Gen4 core)"],
            ["rebal", "quality_factor", "Rebal 21d", "20", "Profitability + growth + safety"],
            ["rebal", "hybrid_qscore", "Rebal 21d", "20", "5-factor composite (RS/Sector/ROE/MA/Vol)"],
            ["event", "breakout_trend", "Event", "15", "Breakout detection"],
            ["event", "mean_reversion", "Event", "5", "RSI<30 + MA200 filter"],
            ["event", "liquidity_signal", "Event", "10", "Liquidity-based entry"],
            ["macro", "sector_rotation", "Rebal 21d", "20", "Top 3 sectors (60d momentum)"],
            ["regime", "vol_regime", "Rebal 21d", "20", "KOSPI volatility regime switch"],
        ],
        col_widths=[avail_w*0.10, avail_w*0.22, avail_w*0.13, avail_w*0.10, avail_w*0.45],
    ))

    story.append(Spacer(1, 4*mm))
    story.append(Paragraph(
        "Cross-group comparison DISABLED. Each group is independently evaluated.<br/>"
        "vol_regime group is fully isolated (never mixed with other strategies).",
        S_NOTE))

    story.append(Paragraph("Lab Live (Forward Paper Trading)", S_H3))
    story.append(Paragraph(
        "- EOD daily run: signal generation + virtual fill<br/>"
        "- State: per-strategy files + committed HEAD (v2)<br/>"
        "- Capital: 100M KRW per strategy (isolated)<br/>"
        "- REST endpoint: http://localhost:8080 (SSE real-time updates)",
        S_BODY))

    # ═══ 7. STATE MANAGEMENT ════════════════════════════════
    story.append(PageBreak())
    story.append(Paragraph("7. State Management (v2)", S_H1))

    story.append(Paragraph("Lab Live: Committed Version Protocol", S_H2))
    story.append(Paragraph(
        "data/lab_live/<br/>"
        "&nbsp;&nbsp;head.json .............. committed version pointer<br/>"
        "&nbsp;&nbsp;states/ .................. per-strategy files (9 JSON)<br/>"
        "&nbsp;&nbsp;trades.json ........... versioned trade history<br/>"
        "&nbsp;&nbsp;equity.json ............ versioned equity data<br/>"
        "&nbsp;&nbsp;.state_io.lock ......... cross-process file lock<br/>"
        "&nbsp;&nbsp;archive/{ts}/ .......... full snapshots (rotation: keep 10)",
        S_CODE))

    story.append(Paragraph("Write Protocol (Atomic Commit)", S_H3))
    story.append(Paragraph(
        "1. FileLock acquire (.state_io.lock)<br/>"
        "2. next_ver = head.committed_version_seq + 1<br/>"
        "3. Write 9 strategy files (version_seq = next_ver)<br/>"
        "4. Write trades.json (version_seq = next_ver)<br/>"
        "5. Write equity.json (version_seq = next_ver)<br/>"
        "6. Write head.json (committed_version_seq = next_ver) = COMMIT<br/>"
        "7. FileLock release",
        S_BODY))

    story.append(Paragraph("Recovery Chain (All-or-Nothing)", S_H3))
    story.append(Paragraph(
        "Primary (11 files version match)<br/>"
        "&nbsp;&nbsp;-> .bak rollback (all 11 .bak same version)<br/>"
        "&nbsp;&nbsp;&nbsp;&nbsp;-> Archive fallback (latest valid archive)<br/>"
        "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;-> CORRUPTED status (fresh start + warning)",
        S_CODE))

    story.append(Spacer(1, 3*mm))
    story.append(Paragraph(
        "RULE: No partial recovery. All 11 files must share the same version_seq.",
        S_WARN))

    story.append(Paragraph("kr Core / us Core", S_H3))
    story.append(Paragraph(
        "Two-file pattern: portfolio_state_{mode}.json + runtime_state_{mode}.json<br/>"
        "Atomic write: tmp -> verify -> backup -> rename<br/>"
        "Paired version_seq (same value, but no committed HEAD yet)<br/>"
        "TODO: Add committed HEAD + FileLock (see STATE_V2_ROADMAP.md)",
        S_BODY))

    # ═══ 8. RISK MANAGEMENT ═════════════════════════════════
    story.append(PageBreak())
    story.append(Paragraph("8. Risk Management", S_H1))

    story.append(Paragraph("DD Guard (Drawdown Protection)", S_H2))
    story.append(make_table(
        ["Level", "Threshold", "Buy Scale", "Trim", "Action"],
        [
            ["CAUTION", "-5%", "70%", "0%", "Slight buy reduction"],
            ["WARNING", "-10%", "50%", "0%", "Block half of buys"],
            ["CRITICAL", "-15%", "0%", "0%", "Block all buys"],
            ["SEVERE", "-20%", "0%", "20%", "Block + trim 20%"],
            ["SAFE_MODE", "-25%", "0%", "20%", "Emergency trim + safe mode"],
        ],
        col_widths=[avail_w*0.17, avail_w*0.15, avail_w*0.15, avail_w*0.13, avail_w*0.40],
    ))

    story.append(Spacer(1, 3*mm))
    story.append(Paragraph(
        "Daily DD limit: -4% (block new entries). Monthly DD limit: -7% (block new entries).<br/>"
        "SAFE_MODE release: DD recovers above -20%. Same-day release blocked (anti-flapping).",
        S_NOTE))

    story.append(Paragraph("BuyPermission State Machine", S_H3))
    story.append(Paragraph(
        "NORMAL -> BLOCKED -> RECOVERING -> REDUCED -> NORMAL<br/><br/>"
        "Blocked triggers: SAFE_MODE L3+, opt10075 2+ failures, pending_external 2+ unresolved<br/>"
        "Reduced triggers: SAFE_MODE L2, REST_STALE (>3s), REST_UNSAFE (degraded data)",
        S_BODY))

    story.append(Paragraph("Safety Rules (CLAUDE.md)", S_H3))
    story.append(Paragraph(
        "1. Broker = Truth (RECON result is final authority)<br/>"
        "2. SELL always allowed, BUY may be blocked<br/>"
        "3. TIMEOUT != failure (no response != no pending orders)<br/>"
        "4. Never trust single log source (cross-check required)<br/>"
        "5. State must be backward-compatible<br/>"
        "6. Engine layer is protected (scoring.py, config.py LOCKED)<br/>"
        "7. No P0 execution without USER approval",
        S_BODY))

    # ═══ 9. MONITORING ══════════════════════════════════════
    story.append(PageBreak())
    story.append(Paragraph("9. Monitoring", S_H1))

    story.append(Paragraph("Monitor GUI v2 (Gen04)", S_H2))
    story.append(Paragraph(
        "File: kr-legacy/monitor_gui_v2.py (1,736 lines)<br/>"
        "Launch: python monitor_gui_v2.py --mode live<br/>"
        "Layout: HeartbeatStrip + Decision Hub + Hero Chart + Alert Stream + Position Grid<br/>"
        "Stale detection: WARN >90s, STALE >180s<br/>"
        "Read-only: No writes to engine files",
        S_BODY))

    story.append(Paragraph("REST Dashboard (kr)", S_H2))
    story.append(Paragraph(
        "File: kr/web/app.py (FastAPI)<br/>"
        "Port: 8080 (http://localhost:8080)<br/>"
        "Endpoints: /portfolio, /rebalance, /signals, /validate<br/>"
        "System tray: kr/tray_server.py (green Q icon)",
        S_BODY))

    story.append(Paragraph("US Dashboard (us)", S_H2))
    story.append(Paragraph(
        "Port: 8081 (http://localhost:8081)<br/>"
        "Launch: python main.py --server",
        S_BODY))

    # ═══ 10. FAULT RECOVERY ═════════════════════════════════
    story.append(PageBreak())
    story.append(Paragraph("10. Fault Recovery", S_H1))

    story.append(Paragraph("RECON (Reconciliation)", S_H2))
    story.append(Paragraph(
        "Principle: Broker state = final truth. Engine state is secondary.<br/><br/>"
        "KR: opt10075 query -> compare positions/qty -> sync state<br/>"
        "US: Alpaca GET /positions -> compare -> FORCE_SYNC or SAFE_SYNC<br/><br/>"
        "RECON runs on every startup and periodically (US: every 10 min).",
        S_BODY))

    story.append(Paragraph("Dirty Exit Detection", S_H3))
    story.append(Paragraph(
        "runtime_state has started_at + shutdown_at fields.<br/>"
        "If started_at exists but shutdown_at missing -> dirty exit detected.<br/>"
        "Action: FORCE_SYNC reconciliation with broker on next startup.",
        S_BODY))

    story.append(Paragraph("Crash Recovery (State v2)", S_H3))
    story.append(Paragraph(
        "1. HEAD not updated (crash during write) -> previous committed version loads<br/>"
        "2. Primary files corrupted -> .bak rollback (all-or-nothing)<br/>"
        "3. .bak also corrupted -> archive fallback (latest valid archive)<br/>"
        "4. All paths exhausted -> CORRUPTED status, fresh start with warning",
        S_BODY))

    story.append(Paragraph("Key Principles", S_H3))
    story.append(Paragraph(
        "1. Stop and restore (not aggressive auto-recovery)<br/>"
        "2. Cancel pending orders on restart (prevent OVERFILL)<br/>"
        "3. Monitor-only mode until recovery confirmed<br/>"
        "4. Network failure: graduated response (retry -> reduce -> block)",
        S_BODY))

    # ═══ 11. BACKTEST ═══════════════════════════════════════
    story.append(PageBreak())
    story.append(Paragraph("11. Backtest", S_H1))

    story.append(Paragraph("Commands", S_H2))
    story.append(Paragraph(
        "# Gen04 backtest (KR, 7yr)<br/>"
        "cd Gen04<br/>"
        "..\\.venv\\Scripts\\python.exe main.py --backtest --start 2019-01-02<br/><br/>"
        "# Gen04 backtester module (direct)<br/>"
        "..\\.venv\\Scripts\\python.exe -m backtest.backtester<br/><br/>"
        "# Lab runner (multi-strategy comparison, KR)<br/>"
        "cd kr<br/>"
        "..\\.venv64\\Scripts\\python.exe -m lab.runner --group rebal",
        S_CODE))

    story.append(Paragraph("Data", S_H3))
    story.append(Paragraph(
        "Location: backtest/data_full/ohlcv/ (2,561 stocks, 2019-2026)<br/>"
        "Index: backtest/data_full/index/KOSPI.csv<br/>"
        "Fundamental: backtest/data_full/fundamental/<br/>"
        "Results: backtest/results/ (equity curves, trades CSV)",
        S_BODY))

    story.append(Paragraph("Validation Checks", S_H3))
    story.append(make_table(
        ["Test", "Criterion", "Result"],
        [
            ["OOS (2023-2026)", "CAGR >= 15%", "18.7% PASS"],
            ["Slippage x2", "Sharpe >= 1.0", "1.69 PASS"],
            ["BEAR period", "MDD <= -25%", "-18.6% PASS"],
            ["Survivorship", "Same performance", "PASS"],
            ["Universe Top200", "CAGR check", "10.2% (500+ required)"],
            ["12-1 Momentum", "Other windows", "Parameter sensitive"],
        ],
        col_widths=[avail_w*0.30, avail_w*0.30, avail_w*0.40],
    ))

    # ═══ 12. AUTOMATION ═════════════════════════════════════
    story.append(PageBreak())
    story.append(Paragraph("12. Automation Roadmap", S_H1))

    story.append(Paragraph("Current: Windows Task Scheduler", S_H2))
    story.append(make_table(
        ["Task Name", "Schedule", "Script"],
        [
            ["Q-TRON_TradingDayCheck", "Daily 07:52", "00_check_trading_day.bat"],
            ["Q-TRON_MorningBatch", "Weekdays 07:55", "01_batch_scheduled.bat"],
            ["Q-TRON_AutoStart", "Weekdays 08:30", "auto_start.py"],
            ["Q-TRON_RESTMonitor", "Weekdays 08:35", "08_rest_monitor.bat"],
            ["Q-TRON_EOD_Cleanup", "Weekdays 16:00", "99_eod_shutdown.bat"],
        ],
        col_widths=[avail_w*0.32, avail_w*0.22, avail_w*0.46],
    ))

    story.append(Paragraph("Target: Full Automation (Post Rebal Test)", S_H2))
    story.append(Paragraph(
        "1. BIOS Wake-on-RTC (08:00)<br/>"
        "2. Windows auto-login<br/>"
        "3. Kiwoom HTS launch<br/>"
        "4. pyautogui password entry (calibrate_password.py)<br/>"
        "5. LIVE bat execution<br/>"
        "6. EOD (15:30) -> Batch (16:00) -> Shutdown (17:00)<br/><br/>"
        "Manual bypass: Kiwoom account password -> pyautogui coordinate click",
        S_BODY))

    story.append(Paragraph("Constraint", S_H3))
    story.append(Paragraph(
        "PC shared with family (evening usage). Automation limited to market hours only.",
        S_NOTE))

    # ═══ 13. COMMAND REFERENCE ══════════════════════════════
    story.append(PageBreak())
    story.append(Paragraph("13. Command Reference", S_H1))

    story.append(Paragraph("Gen04 (KR)", S_H2))
    story.append(make_table(
        ["Command", "Description"],
        [
            ["main.py --batch", "OHLCV + scoring + target portfolio"],
            ["main.py --batch --fast", "Signal only (skip reports)"],
            ["main.py --live", "Kiwoom real-time trading"],
            ["main.py --mock", "Internal simulation (no broker)"],
            ["main.py --paper-test --cycle full", "Isolated test state"],
            ["main.py --shadow-test", "Dry-run (compute only)"],
            ["main.py --backtest --start YYYY-MM-DD", "Historical simulation"],
            ["main.py --backtest --start 2019-01-02 --end 2026-03-20", "Full 7yr backtest"],
            ["python -m backtest.backtester", "Direct backtester module"],
            ["python -m data.fundamental_collector --mode daily", "Fundamental data"],
            ["python -m report.daily_report", "Daily report"],
        ],
        col_widths=[avail_w*0.48, avail_w*0.52],
    ))

    story.append(Paragraph("kr (KR Lab + REST)", S_H2))
    story.append(make_table(
        ["Command", "Description"],
        [
            ["main.py --server", "REST dashboard (port 8080)"],
            ["main.py --batch", "DB-based OHLCV update"],
            ["main.py --live", "REST-based live engine"],
            ["python -m lab.runner --group rebal", "Lab: rebal group strategies"],
            ["python -m lab.runner --group event", "Lab: event group strategies"],
            ["python tray_server.py", "System tray server (green Q)"],
        ],
        col_widths=[avail_w*0.48, avail_w*0.52],
    ))

    story.append(Paragraph("us (Alpaca)", S_H2))
    story.append(make_table(
        ["Command", "Description"],
        [
            ["main.py --test", "Alpaca + DB connection test"],
            ["main.py --batch", "OHLCV + universe + scoring"],
            ["main.py --live", "Live mode (paper/live)"],
            ["main.py --server", "Dashboard (port 8081)"],
        ],
        col_widths=[avail_w*0.48, avail_w*0.52],
    ))

    story.append(Spacer(1, 8*mm))
    story.append(Paragraph("Batch Files (kr-legacy/)", S_H2))
    story.append(make_table(
        ["File", "Purpose"],
        [
            ["00_check_trading_day.bat", "Trading day check -> shutdown if holiday"],
            ["01_batch.bat", "Full batch (OHLCV + reports)"],
            ["01_batch_fast.bat", "Fast batch (signal only)"],
            ["01_batch_scheduled.bat", "Scheduled batch (skip if done)"],
            ["02_live.bat", "LIVE engine + Monitor GUI"],
            ["03_mock.bat", "Mock mode"],
            ["05_backtest.bat", "7yr backtest"],
            ["06_test.bat", "4 test suites"],
            ["07_paper_test.bat", "Paper test (--cycle)"],
            ["99_eod_shutdown.bat", "EOD cleanup + shutdown"],
            ["setup_scheduler.bat", "Register 5 Task Scheduler tasks"],
        ],
        col_widths=[avail_w*0.40, avail_w*0.60],
    ))

    # ═══ FOOTER ══════════════════════════════════════════════
    story.append(PageBreak())
    story.append(Spacer(1, 30*mm))
    story.append(Paragraph("Q-TRON Operations Manual v4.0", ParagraphStyle(
        "Footer", parent=S_SUBTITLE, fontSize=14, textColor=C_PRIMARY)))
    story.append(Paragraph(
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}<br/>"
        "Source: C:\\Q-TRON-32_ARCHIVE codebase + MEMORY.md<br/>"
        "Reference: kr/docs/STATE_V2_ROADMAP.md",
        S_SUBTITLE))

    # Build
    doc.build(story)
    print(f"PDF generated: {output_path}")
    print(f"Pages: ~{len(story) // 20} (estimated)")


if __name__ == "__main__":
    out = str(Path(__file__).resolve().parent / "Q-TRON_Operations_Manual_v4.pdf")
    if len(sys.argv) > 1:
        out = sys.argv[1]
    build_pdf(out)
