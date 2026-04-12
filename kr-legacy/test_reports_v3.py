"""Integrated test: KOSPI comparison + weekly fixes + monthly report.
Run: python test_reports_v3.py
     pytest test_reports_v3.py
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
from report.kospi_utils import (
    load_kospi_close, get_kospi_return, compute_excess_return,
    get_kospi_period_return, count_outperform_days,
)
from report.daily_report import generate_daily_report, compute_verdict
from report.weekly_report import generate_weekly_report, compute_verdict_weekly
from report.monthly_report import generate_monthly_report, compute_verdict_monthly


def test_reports_v3():
    cfg = Gen4Config()

    # ── 1. KOSPI utils ──
    kospi = load_kospi_close(cfg.INDEX_FILE)
    assert len(kospi) > 1000, f"KOSPI loaded: got {len(kospi)} rows"
    assert "2026-03-09" in kospi.index or "2026-03-06" in kospi.index, \
        "KOSPI missing 2026 data"

    kr = get_kospi_return(kospi, "2026-03-04")
    assert kr is not None, "daily return not calculated"
    assert abs(kr) < 0.2, f"daily return unreasonable: got {kr}"

    pr = get_kospi_period_return(kospi, "2026-03-03", "2026-03-09")
    assert pr is not None, "period return not calculated"

    excess, label = compute_excess_return(0.02, 0.01)
    assert label == "Outperform" and abs(excess - 0.01) < 0.001, \
        "excess return: outperform check failed"
    excess2, label2 = compute_excess_return(-0.01, 0.01)
    assert label2 == "Underperform", "excess return: underperform check failed"
    excess3, label3 = compute_excess_return(0.01, None)
    assert label3 == "N/A", "excess return: no KOSPI check failed"

    # ── 2. Daily with KOSPI ──
    tmp = Path(tempfile.mkdtemp())

    with open(tmp / "equity_log.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["date", "equity", "cash", "n_positions", "daily_pnl_pct", "monthly_dd_pct",
                     "risk_mode", "rebalance_executed", "price_fail_count", "reconcile_corrections", "monitor_only"])
        w.writerow(["2026-03-09", "512000000", "12000000", "3", "0.0240", "-0.005", "NORMAL", "N", "0", "0", "N"])

    with open(tmp / "daily_positions.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["date", "code", "quantity", "avg_price", "current_price", "market_value",
                     "pnl_pct", "pnl_amount", "high_watermark", "trail_stop_price", "entry_date", "hold_days"])
        w.writerow(["2026-03-09", "055550", "200", "45000", "47000", "9400000", "0.0444", "400000", "47000", "41360", "2026-03-05", "4"])

    path = generate_daily_report(tmp, cfg, "2026-03-09")
    assert path is not None, "daily report not created"
    html = path.read_text(encoding="utf-8")
    assert "시장 대비 성과" in html, "KOSPI section missing"
    assert any(x in html for x in ["Outperform", "Underperform", "In-line", "N/A"]), \
        "Outperform/Underperform not present"
    # relative_return column removed (#3 fix) — KOSPI comparison is a separate section
    assert "시장 대비 성과" in html, "KOSPI comparison section missing"
    assert "자동 판단" in html, "자동 판단 section missing"
    shutil.rmtree(tmp)

    # ── 3. Daily empty ──
    tmp = Path(tempfile.mkdtemp())
    path = generate_daily_report(tmp, cfg, "2026-03-09")
    assert path is not None, "empty daily report failed"
    html = path.read_text(encoding="utf-8")
    assert "KOSPI" in html, "KOSPI fallback missing"
    shutil.rmtree(tmp)

    # ── 4. Weekly with KOSPI + STANDBY ──
    v = compute_verdict_weekly(0.0, 0.0, 0, n_trades=0, end_positions=0, monitor_only_days=0)
    assert v[0] == "STANDBY", "no-trade week should be STANDBY"
    assert v[1] == "대기", "STANDBY kr should be 대기"

    v2 = compute_verdict_weekly(0.0, 0.0, 2, n_trades=0, end_positions=0, monitor_only_days=0)
    assert v2[0] == "WATCH", "errors + no trade should be WATCH"

    tmp = Path(tempfile.mkdtemp())
    path = generate_weekly_report(tmp, cfg, "2026-03-09")
    assert path is not None, "empty weekly report failed"
    html = path.read_text(encoding="utf-8")
    assert "시장 대비 성과" in html, "weekly KOSPI section missing"
    assert "비용 분석" in html, "weekly 비용 분석 missing"
    assert "대기" in html, "weekly 대기 verdict missing"
    assert "LowVol" in html or "Gen4" in html, "Gen4 strategy label missing"
    shutil.rmtree(tmp)

    # ── 5. Monthly ──
    tmp = Path(tempfile.mkdtemp())

    # Write month of data (March 2026)
    with open(tmp / "equity_log.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["date", "equity", "cash", "n_positions", "daily_pnl_pct", "monthly_dd_pct",
                     "risk_mode", "rebalance_executed", "price_fail_count", "reconcile_corrections", "monitor_only"])
        w.writerow(["2026-02-28", "500000000", "500000000", "0", "0", "0", "NORMAL", "N", "0", "0", "N"])
        for d in range(1, 10):
            dt = f"2026-03-{d:02d}"
            eq = 500000000 + d * 1000000
            pnl = 0.002 * (1 if d % 3 != 0 else -1)
            w.writerow([dt, str(eq), "10000000", "20", f"{pnl:.4f}", "0", "NORMAL",
                         "Y" if d == 1 else "N", "0", "0", "N"])

    with open(tmp / "close_log.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["date", "code", "exit_reason", "quantity", "entry_price", "exit_price", "entry_date",
                     "hold_days", "pnl_pct", "pnl_amount", "mode", "event_id"])
        w.writerow(["2026-03-05", "000660", "TRAIL_STOP", "50", "95000", "83600", "2026-02-15", "18", "-0.12", "-570000", "PAPER", "e1"])
        w.writerow(["2026-03-05", "005930", "REBALANCE_EXIT", "100", "72000", "75000", "2026-02-10", "23", "0.04", "300000", "PAPER", "e2"])

    with open(tmp / "trades.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["date", "code", "side", "quantity", "price", "cost", "slippage_pct", "mode", "event_id"])
        w.writerow(["2026-03-01", "055550", "BUY", "200", "45000", "10350", "0.00", "PAPER", "b1"])
        w.writerow(["2026-03-05", "000660", "SELL", "50", "83600", "24662", "0.00", "PAPER", "s1"])

    path = generate_monthly_report(tmp, cfg, "2026-03")
    assert path is not None, "monthly report not created"
    html = path.read_text(encoding="utf-8")

    for s in ["성과 지표", "시장 대비 성과", "비용 영향도", "거래 통계",
              "포트폴리오 회전", "시스템 안정성", "결론"]:
        assert s in html, f"monthly section missing: {s}"

    assert "Sharpe" in html, "Sharpe missing"
    assert "Calmar" in html, "Calmar missing"
    assert "Payoff" in html, "Payoff missing"
    assert "Turnover" in html, "Turnover missing"
    assert "KOSPI" in html, "KOSPI missing in monthly"

    # Verdict
    v = compute_verdict_monthly(0.02, -0.05, 1.5, 2)
    assert v[0] == "EXPAND", "monthly EXPAND verdict failed"
    v2 = compute_verdict_monthly(-0.04, -0.16, 0.3, 5)
    assert v2[0] == "REDUCE", "monthly REDUCE verdict failed"
    v3 = compute_verdict_monthly(0, 0, 0, 0)
    assert v3[0] == "STANDBY", "monthly STANDBY verdict failed"

    shutil.rmtree(tmp)

    # ── 6. Standalone modes ──
    src_d = (Path(__file__).resolve().parent / "report" / "daily_report.py").read_text(encoding="utf-8")
    src_w = (Path(__file__).resolve().parent / "report" / "weekly_report.py").read_text(encoding="utf-8")
    src_m = (Path(__file__).resolve().parent / "report" / "monthly_report.py").read_text(encoding="utf-8")
    assert "__name__" in src_d and "--date" in src_d, "daily standalone check failed"
    assert "__name__" in src_w and "--date" in src_w, "weekly standalone check failed"
    assert "__name__" in src_m and "--month" in src_m, "monthly standalone check failed"

    # ── 7. Regression ──
    r1 = subprocess.run([sys.executable, "test_prelaunch.py"], capture_output=True, text=True)
    assert r1.returncode == 0, "prelaunch regression failed"
    r2 = subprocess.run([sys.executable, "test_forensic.py"], capture_output=True, text=True)
    assert r2.returncode == 0, "forensic regression failed"


if __name__ == "__main__":
    test_reports_v3()
    print("ALL PASS")
