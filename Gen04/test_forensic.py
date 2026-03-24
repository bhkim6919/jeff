"""
Gen4 Forensic Logging Verification Tests
==========================================
Tests: decision_log, reconcile_log, event_id, equity context, forensic snapshot, 5억 세팅.
Run: python test_forensic.py
     pytest test_forensic.py
"""
import sys
import json
import csv
import tempfile
import shutil
from pathlib import Path
from datetime import date, datetime

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import Gen4Config
from report.reporter import TradeLogger, make_event_id, save_forensic_snapshot
from core.portfolio_manager import PortfolioManager


def test_forensic():
    cfg = Gen4Config()

    # ── Test 1: Config = 5억 ──
    assert cfg.INITIAL_CASH == 500_000_000, f"INITIAL_CASH: got {cfg.INITIAL_CASH}"
    assert cfg.TARGET_MAX_STALE_DAYS == 3, \
        f"TARGET_MAX_STALE_DAYS: got {cfg.TARGET_MAX_STALE_DAYS}"

    # ── Test 2: State file = 5억 ──
    state_path = Path(__file__).resolve().parent / "state" / "portfolio_state_paper.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["cash"] == 500_000_000, f"state cash: got {state['cash']}"
    assert state["positions"] == {}, "state positions should be empty"

    # ── Test 3-10: Reporter classes/functions ──
    tmp_dir = Path(tempfile.mkdtemp())
    try:
        tl = TradeLogger(tmp_dir)

        # Check all CSV files created with headers
        assert (tmp_dir / "trades.csv").exists(), "trades.csv missing"
        assert (tmp_dir / "close_log.csv").exists(), "close_log.csv missing"
        assert (tmp_dir / "equity_log.csv").exists(), "equity_log.csv missing"
        assert (tmp_dir / "decision_log.csv").exists(), "decision_log.csv missing"
        assert (tmp_dir / "reconcile_log.csv").exists(), "reconcile_log.csv missing"

        def read_header(f):
            with open(tmp_dir / f, encoding="utf-8-sig") as fh:
                return next(csv.reader(fh))

        trades_h = read_header("trades.csv")
        assert "event_id" in trades_h, f"trades.csv headers: {trades_h}"

        close_h = read_header("close_log.csv")
        assert "event_id" in close_h, f"close_log.csv headers: {close_h}"

        equity_h = read_header("equity_log.csv")
        assert "risk_mode" in equity_h, f"equity_log headers: {equity_h}"
        assert "rebalance_executed" in equity_h, "equity_log missing rebalance_executed"
        assert "price_fail_count" in equity_h, "equity_log missing price_fail_count"
        assert "reconcile_corrections" in equity_h, "equity_log missing reconcile_corrections"
        assert "monitor_only" in equity_h, "equity_log missing monitor_only"

        decision_h = read_header("decision_log.csv")
        assert "event_id" in decision_h, "decision_log missing event_id"
        assert "score_vol" in decision_h, "decision_log missing score_vol"
        assert "high_watermark" in decision_h, "decision_log missing high_watermark"

        reconcile_h = read_header("reconcile_log.csv")
        assert "diff_type" in reconcile_h, "reconcile_log missing diff_type"
        assert "resolution" in reconcile_h, "reconcile_log missing resolution"

        # ── Test 4: event_id generation ──
        eid = make_event_id("055550", "BUY")
        parts = eid.split("_")
        assert len(parts) == 4, f"event_id should have 4 parts, got {len(parts)}: {eid}"
        assert parts[0] == date.today().strftime("%Y%m%d"), \
            f"event_id should start with date, got {parts[0]}"
        assert "055550" in eid, "event_id should contain code"
        assert "BUY" in eid, "event_id should contain action"

        # ── Test 5: log_decision_buy writes row ──
        eid_buy = tl.log_decision_buy(
            "055550", "REBALANCE_ENTRY",
            score_vol=0.0206, score_mom=0.8986,
            rank=1, target_weight=25000000,
            price=45000, cash_before=500000000)
        assert eid_buy and len(eid_buy) > 10, "log_decision_buy should return event_id"

        with open(tmp_dir / "decision_log.csv", encoding="utf-8-sig") as f:
            rows = list(csv.reader(f))
        assert len(rows) == 2, f"decision_log should have 2 rows, got {len(rows)}"
        assert rows[1][2] == "055550", "decision row code should be 055550"
        assert rows[1][3] == "BUY", "decision row side should be BUY"

        # ── Test 6: log_decision_sell writes row ──
        eid_sell = tl.log_decision_sell(
            "055550", "TRAIL_STOP",
            price=39600, high_watermark=48000,
            trail_stop_price=42240, pnl_pct=-0.12, hold_days=23)
        assert eid_sell and len(eid_sell) > 10, "log_decision_sell should return event_id"

        with open(tmp_dir / "decision_log.csv", encoding="utf-8-sig") as f:
            rows = list(csv.reader(f))
        assert len(rows) == 3, f"decision_log should have 3 rows, got {len(rows)}"
        assert rows[2][4] == "TRAIL_STOP", "sell row reason should be TRAIL_STOP"

        # ── Test 7: log_reconcile writes row ──
        tl.log_reconcile("055550", "QTY_MISMATCH",
                         engine_qty=100, broker_qty=150,
                         engine_avg=45000, broker_avg=45000,
                         resolution="SYNCED_TO_BROKER")
        with open(tmp_dir / "reconcile_log.csv", encoding="utf-8-sig") as f:
            rows = list(csv.reader(f))
        assert len(rows) == 2, f"reconcile_log should have 2 rows, got {len(rows)}"
        assert rows[1][3] == "QTY_MISMATCH", "reconcile diff_type should be QTY_MISMATCH"

        # ── Test 8: log_equity with extended context ──
        tl.log_equity(500000000, 500000000, 0, 0.0, 0.0,
                      risk_mode="NORMAL", rebalance_executed=True,
                      price_fail_count=2, reconcile_corrections=1,
                      monitor_only=False)
        with open(tmp_dir / "equity_log.csv", encoding="utf-8-sig") as f:
            rows = list(csv.reader(f))
        assert len(rows) == 2, f"equity_log should have 2 rows, got {len(rows)}"
        assert len(rows[1]) == 11, f"equity row should have 11 columns, got {len(rows[1])}"
        assert rows[1][6] == "NORMAL", "risk_mode should be NORMAL"
        assert rows[1][7] == "Y", "rebalance_executed should be Y"
        assert rows[1][8] == "2", "price_fail_count should be 2"

        # ── Test 9: forensic snapshot ──
        snap_dir = tmp_dir / "state"
        snap_path = save_forensic_snapshot(
            snap_dir,
            portfolio_data={"cash": 500000000, "positions": {}},
            error_msg="Test error",
            price_fail_codes=["055550", "005930"],
            target_date="20260322")
        assert snap_path is not None and snap_path.exists(), "forensic snapshot not created"
        snap = json.loads(snap_path.read_text(encoding="utf-8"))
        assert snap["error"] == "Test error", "snapshot error field mismatch"
        assert snap["price_fail_codes"] == ["055550", "005930"], \
            "snapshot price_fail_codes mismatch"
        assert snap["portfolio"]["cash"] == 500000000, "snapshot portfolio cash mismatch"

        # ── Test 10: backward compat — log_equity defaults ──
        tl.log_equity(500000000, 500000000, 0, 0.0, 0.0)
        with open(tmp_dir / "equity_log.csv", encoding="utf-8-sig") as f:
            rows = list(csv.reader(f))
        assert len(rows) == 3, f"equity_log should have 3 rows, got {len(rows)}"
        assert rows[2][6] == "NORMAL", "default risk_mode should be NORMAL"
        assert rows[2][7] == "N", "default rebalance_executed should be N"
        assert rows[2][10] == "N", "default monitor_only should be N"

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    # ── Test 11: PortfolioManager 5억 ──
    pm = PortfolioManager(cfg.INITIAL_CASH, cfg.DAILY_DD_LIMIT,
                          cfg.MONTHLY_DD_LIMIT, cfg.N_STOCKS)
    assert pm.cash == 500_000_000, "PM cash should be 500M"
    assert pm.get_current_equity() == 500_000_000, "PM equity should be 500M"

    # ── Test 12: main.py source checks ──
    src = (Path(__file__).resolve().parent / "main.py").read_text(encoding="utf-8")

    assert "save_forensic_snapshot" in src, "save_forensic_snapshot not imported"
    assert "log_decision_buy" in src, "log_decision_buy not called in rebalance"
    assert "log_decision_sell" in src and "TRAIL_STOP" in src, \
        "log_decision_sell not called for trail stop"
    assert "log_reconcile" in src, "log_reconcile not called"
    assert "session_rebalance_executed" in src, "session_rebalance_executed not tracked"
    assert "session_price_fail_count" in src, "session_price_fail_count not tracked"
    assert "reconcile_corrections=reconcile_corrections" in src, \
        "reconcile_corrections not passed to equity"


if __name__ == "__main__":
    test_forensic()
    print("ALL PASS")
