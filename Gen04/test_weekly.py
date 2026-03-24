"""Test weekly report generation.
Run: python test_weekly.py
     pytest test_weekly.py
"""
import sys
import tempfile
import shutil
import csv
import subprocess
from pathlib import Path
from datetime import date

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import Gen4Config
from report.weekly_report import (
    generate_weekly_report, get_week_range, compute_verdict_weekly,
)


def test_weekly():
    cfg = Gen4Config()

    # ── 1. Week range ──
    ws, we = get_week_range(date(2026, 3, 25))
    assert ws == "2026-03-23" and we == "2026-03-27", \
        f"Wed -> Mon~Fri: got {ws}~{we}"
    ws2, we2 = get_week_range(date(2026, 3, 28))
    assert ws2 == "2026-03-23", f"Sat -> same week: got {ws2}"

    # ── 2. Verdict ──
    # n_trades>0 to avoid STANDBY
    assert compute_verdict_weekly(0.01, 0.50, 0, n_trades=5, end_positions=20)[0] == "MAINTAIN", \
        "MAINTAIN verdict failed"
    assert compute_verdict_weekly(-0.04, 0.50, 0, n_trades=5, end_positions=20)[0] == "WATCH", \
        "WATCH (return) verdict failed"
    assert compute_verdict_weekly(-0.06, 0.50, 0, n_trades=5, end_positions=20)[0] == "REVIEW", \
        "REVIEW (return) verdict failed"
    assert compute_verdict_weekly(0.01, 0.35, 0, n_trades=5, end_positions=20)[0] == "WATCH", \
        "WATCH (winrate) verdict failed"
    assert compute_verdict_weekly(0.01, 0.25, 0, n_trades=5, end_positions=20)[0] == "REVIEW", \
        "REVIEW (winrate) verdict failed"
    assert compute_verdict_weekly(0.01, 0.50, 3, n_trades=5, end_positions=20)[0] == "WATCH", \
        "WATCH (errors) verdict failed"

    # ── 3. Empty data ──
    tmp = Path(tempfile.mkdtemp())
    path = generate_weekly_report(tmp, cfg, "2026-03-25")
    assert path is not None and path.exists(), "empty weekly report not created"
    html = path.read_text(encoding="utf-8")
    assert "Gen4 주간 보고서" in html, "title missing"
    assert "2026-03-23" in html and "2026-03-27" in html, "dates missing in header"
    shutil.rmtree(tmp)

    # ── 4. Sample 5-day ──
    tmp = Path(tempfile.mkdtemp())

    with open(tmp / "equity_log.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["date", "equity", "cash", "n_positions", "daily_pnl_pct", "monthly_dd_pct",
                     "risk_mode", "rebalance_executed", "price_fail_count", "reconcile_corrections", "monitor_only"])
        w.writerow(["2026-03-20", "500000000", "500000000", "0", "0", "0", "NORMAL", "N", "0", "0", "N"])
        w.writerow(["2026-03-23", "505000000", "5000000", "20", "0.0100", "0", "NORMAL", "Y", "0", "0", "N"])
        w.writerow(["2026-03-24", "508000000", "5000000", "20", "0.0059", "0", "NORMAL", "N", "0", "0", "N"])
        w.writerow(["2026-03-25", "503000000", "5000000", "20", "-0.0098", "0", "NORMAL", "N", "1", "0", "N"])
        w.writerow(["2026-03-26", "510000000", "5000000", "20", "0.0139", "0", "NORMAL", "N", "0", "0", "N"])
        w.writerow(["2026-03-27", "515000000", "5000000", "20", "0.0098", "0", "NORMAL", "N", "0", "0", "N"])

    with open(tmp / "daily_positions.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["date", "code", "quantity", "avg_price", "current_price", "market_value",
                     "pnl_pct", "pnl_amount", "high_watermark", "trail_stop_price", "entry_date", "hold_days"])
        w.writerow(["2026-03-23", "055550", "200", "45000", "46000", "9200000", "0.0222", "200000", "46000", "40480", "2026-03-23", "0"])
        w.writerow(["2026-03-27", "055550", "200", "45000", "48000", "9600000", "0.0667", "600000", "48000", "42240", "2026-03-23", "4"])
        w.writerow(["2026-03-27", "035420", "50", "310000", "320000", "16000000", "0.0323", "500000", "320000", "281600", "2026-03-25", "2"])

    with open(tmp / "close_log.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["date", "code", "exit_reason", "quantity", "entry_price", "exit_price", "entry_date",
                     "hold_days", "pnl_pct", "pnl_amount", "mode", "event_id"])
        w.writerow(["2026-03-25", "000660", "TRAIL_STOP", "50", "95000", "83600", "2026-03-10", "15", "-0.12", "-570000", "PAPER", "e1"])
        w.writerow(["2026-03-25", "035720", "REBALANCE_EXIT", "30", "150000", "158000", "2026-03-10", "15", "0.053", "240000", "PAPER", "e2"])

    with open(tmp / "trades.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["date", "code", "side", "quantity", "price", "cost", "slippage_pct", "mode", "event_id"])
        w.writerow(["2026-03-23", "055550", "BUY", "200", "45000", "10350", "0.00", "PAPER", "b1"])
        w.writerow(["2026-03-25", "000660", "SELL", "50", "83600", "0", "0.00", "PAPER", "s1"])
        w.writerow(["2026-03-25", "035720", "SELL", "30", "158000", "0", "0.00", "PAPER", "s2"])
        w.writerow(["2026-03-25", "035420", "BUY", "50", "310000", "17825", "0.00", "PAPER", "b3"])

    path = generate_weekly_report(tmp, cfg, "2026-03-25")
    assert path is not None and path.exists(), "sample weekly report not created"
    html = path.read_text(encoding="utf-8")

    for label in ["주간 성과", "거래 통계", "포트폴리오 변화", "리스크 분석",
                  "전략 검증", "시스템 분석", "결론"]:
        assert label in html, f"section missing: {label}"

    assert "055550" in html, "055550 missing in report"
    assert "TRAIL_STOP" in html, "TRAIL_STOP ref missing"
    assert "승률" in html, "승률 missing"
    assert "Payoff" in html, "Payoff missing"
    assert "035420" in html, "신규 편입 035420 missing"

    shutil.rmtree(tmp)

    # ── 5. Standalone ──
    src = (Path(__file__).resolve().parent / "report" / "weekly_report.py").read_text(encoding="utf-8")
    assert "__name__" in src, "__main__ guard missing"
    assert "--date" in src, "--date arg missing"

    # ── 6. Regression ──
    r1 = subprocess.run([sys.executable, "test_prelaunch.py"], capture_output=True, text=True)
    assert r1.returncode == 0, "prelaunch regression failed"
    r2 = subprocess.run([sys.executable, "test_forensic.py"], capture_output=True, text=True)
    assert r2.returncode == 0, "forensic regression failed"


if __name__ == "__main__":
    test_weekly()
    print("ALL PASS")
