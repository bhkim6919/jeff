"""Test daily report v2 enhancements.
Run: python test_daily_v2.py
     pytest test_daily_v2.py
"""
import sys
import tempfile
import shutil
import csv
import subprocess
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import Gen4Config
from report.daily_report import (
    generate_daily_report, compute_verdict, compute_position_risk
)
import pandas as pd


def test_daily_v2():
    cfg = Gen4Config()

    # ── 1. Position 0: no crash ──
    tmp = Path(tempfile.mkdtemp())
    path = generate_daily_report(tmp, cfg, "2026-03-23")
    assert path is not None, "empty report not created"
    html = path.read_text(encoding="utf-8")
    assert "미보유" in html, "no crash on empty"
    assert "자동 판단" in html, "auto verdict missing"
    assert "기존 전략 유지" in html, "action missing"
    assert "분석 대상 포지션 없음" in html, "pnl attribution fallback missing"
    assert "당일 비용 없음" in html, "cost fallback missing"
    shutil.rmtree(tmp)

    # ── 2. trail_gap_pct formula + risk_flag ──
    df = pd.DataFrame([
        {"code": "A", "current_price": 10000, "trail_stop_price": 9800, "pnl_pct": 0, "pnl_amount": 0},
        {"code": "B", "current_price": 10000, "trail_stop_price": 9500, "pnl_pct": 0, "pnl_amount": 0},
        {"code": "C", "current_price": 10000, "trail_stop_price": 8000, "pnl_pct": 0, "pnl_amount": 0},
    ])
    result = compute_position_risk(df)
    # A: 10000/9800 - 1 = 2.04%
    # B: 10000/9500 - 1 = 5.26%
    # C: 10000/8000 - 1 = 25%
    assert result.iloc[0]["code"] == "A", "A should be sorted first (lowest gap)"
    gap_a = float(result.iloc[0]["trail_gap_pct"])
    assert abs(gap_a - 2.04) < 0.1, f"A gap should be ~2.04%, got {gap_a}"
    assert result.iloc[0]["risk_flag"] == "주의", \
        f"A risk_flag should be 주의, got {result.iloc[0]['risk_flag']}"
    assert result[result["code"] == "B"].iloc[0]["risk_flag"] == "정상", \
        "B risk_flag should be 정상"
    assert result[result["code"] == "C"].iloc[0]["risk_flag"] == "정상", \
        "C risk_flag should be 정상"

    # Edge: gap <= 2% = 위험
    df2 = pd.DataFrame([
        {"code": "X", "current_price": 10000, "trail_stop_price": 9810, "pnl_pct": 0, "pnl_amount": 0},
    ])
    r2 = compute_position_risk(df2)
    gap_x = float(r2.iloc[0]["trail_gap_pct"])
    assert r2.iloc[0]["risk_flag"] == "위험", f"X gap ~1.94% should be 위험, gap={gap_x}"

    # ── 3. Sample with positions ──
    tmp = Path(tempfile.mkdtemp())

    with open(tmp / "equity_log.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["date", "equity", "cash", "n_positions", "daily_pnl_pct", "monthly_dd_pct",
                     "risk_mode", "rebalance_executed", "price_fail_count", "reconcile_corrections", "monitor_only"])
        w.writerow(["2026-03-23", "512000000", "12000000", "3", "0.0240", "-0.005", "NORMAL", "Y", "1", "0", "N"])

    with open(tmp / "daily_positions.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["date", "code", "quantity", "avg_price", "current_price", "market_value",
                     "pnl_pct", "pnl_amount", "high_watermark", "trail_stop_price", "entry_date", "hold_days"])
        # Near trail stop (gap ~1.5%)
        w.writerow(["2026-03-23", "111111", "100", "50000", "50500", "5050000", "0.0100", "50000", "52000", "49750", "2026-03-20", "3"])
        # Mid range (gap ~5%)
        w.writerow(["2026-03-23", "222222", "200", "30000", "33000", "6600000", "0.1000", "600000", "33000", "31400", "2026-03-20", "3"])
        # Safe (gap ~15%)
        w.writerow(["2026-03-23", "333333", "50", "100000", "115000", "5750000", "0.1500", "750000", "115000", "100000", "2026-03-20", "3"])

    with open(tmp / "trades.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["date", "code", "side", "quantity", "price", "cost", "slippage_pct", "mode", "event_id"])
        w.writerow(["2026-03-23", "111111", "BUY", "100", "50000", "5750", "0.00", "PAPER", "b1"])

    path = generate_daily_report(tmp, cfg, "2026-03-23")
    html = path.read_text(encoding="utf-8")

    assert "미청산 포지션" in html, "positions section missing"
    assert "위험" in html, "risk_flag column: 위험 missing"
    assert "정상" in html, "risk_flag column: 정상 missing"
    assert "수량" in html, "수량 column header missing"
    assert "평균가" in html, "평균가 column header missing"
    assert "Trail Gap" in html, "Trail Gap header missing"
    assert "상태" in html, "상태 header missing"
    pos_section = html[html.index("미청산 포지션"):]
    assert pos_section.index("111111") < pos_section.index("333333"), \
        "111111 should be first in positions table"

    # ── 4. Cost section ──
    assert "비용 분석" in html, "비용 분석 section missing"
    assert "5,750" in html, "당일 비용 value missing"
    assert "잠식률" in html, "비용 잠식률 missing"

    # ── 5. PnL attribution ──
    assert "손익 원인 분석" in html, "손익 원인 분석 missing"
    assert "수익 기여" in html, "수익 기여 TOP3 missing"
    assert "시스템 영향" in html, "시스템 영향 missing"
    assert "price_fail=1" in html, "price_fail=1 not shown"

    # ── 6. Verdict consistency ──
    assert html.count("주의") >= 2, "verdict=주의 should appear in header AND auto verdict"
    assert "자동 판단" in html, "auto verdict section missing"
    assert "로그 점검" in html, "CAUTION action text missing"

    # Danger case
    assert compute_verdict(-0.01, 0, 0, True) == ("DANGER", "위험", "#d32f2f"), \
        "DANGER: monitor_only"
    assert compute_verdict(-0.05, 0, 0, False) == ("DANGER", "위험", "#d32f2f"), \
        "DANGER: dd <= -4%"

    shutil.rmtree(tmp)

    # ── 7. Regression ──
    r1 = subprocess.run([sys.executable, "test_prelaunch.py"], capture_output=True, text=True)
    assert r1.returncode == 0, "prelaunch regression failed"
    r2 = subprocess.run([sys.executable, "test_forensic.py"], capture_output=True, text=True)
    assert r2.returncode == 0, "forensic regression failed"


if __name__ == "__main__":
    test_daily_v2()
    print("ALL PASS")
