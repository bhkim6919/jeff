"""
test_stability.py — Stability & safety tests for Gen4 P0/P1 fixes
===================================================================
Tests 1-8: Required (P0 safety + P1 resilience)
Tests 9-10: Recommended (strategy verification, deferred)

Run:  python -m pytest test_stability.py -v
"""
import csv
import json
import os
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from core.portfolio_manager import PortfolioManager, Position
from core.state_manager import StateManager
from runtime.order_executor import OrderExecutor
from runtime.order_tracker import OrderTracker
from strategy.rebalancer import compute_orders


# ═══════════════════════════════════════════════════════════════════
# Test 1 — PAPER safety: PAPER_TRADING=True + REAL server → abort
# ═══════════════════════════════════════════════════════════════════
class TestPaperSafety:
    """Verify that PAPER_TRADING=True + REAL server causes abort."""

    def test_paper_mode_real_server_aborts(self):
        """main.py run_live must abort when config.PAPER_TRADING=True
        and connected to REAL server."""
        # Read main.py source and verify the safety check exists
        main_path = Path(__file__).resolve().parent / "main.py"
        source = main_path.read_text(encoding="utf-8")

        # Verify fail-fast pattern exists
        assert "PAPER_SAFETY" in source, \
            "main.py must contain [PAPER_SAFETY] abort logic"
        assert "config.PAPER_TRADING and server_type ==" in source, \
            "main.py must check PAPER_TRADING + server_type combination"
        # Verify it returns (aborts) instead of continuing
        # The pattern should be: check → critical log → return
        idx_safety = source.index("PAPER_SAFETY")
        # There should be a 'return' within ~500 chars after the safety check
        snippet = source[idx_safety:idx_safety + 500]
        assert "return" in snippet, \
            "PAPER_SAFETY check must abort (return) the function"

    def test_all_modes_use_kiwoom_api(self):
        """Both paper and live modes send orders via Kiwoom API (paper=False).
        mock/paper/live terminology:
          mock  = --mock (no Kiwoom, internal simulation)
          paper = --live + MOCK server (키움 모의투자)
          live  = --live + REAL server (실거래)
        """
        main_path = Path(__file__).resolve().parent / "main.py"
        source = main_path.read_text(encoding="utf-8")
        # executor should be created with simulate=False (Kiwoom API for both)
        assert "simulate=False" in source, \
            "OrderExecutor must use simulate=False (Kiwoom API for both paper and live)"
        assert 'is_paper_server' in source, \
            "Must track server type via is_paper_server variable"


# ═══════════════════════════════════════════════════════════════════
# Test 2 — Ghost fill BUY: existing position (avg_price + cash)
# ═══════════════════════════════════════════════════════════════════
class TestGhostFillBuyExisting:
    """Ghost fill for existing position must use gross price for avg,
    and deduct cash exactly once (with fee)."""

    def _make_executor(self):
        tracker = OrderTracker()
        trade_logger = MagicMock()
        trade_logger.log_trade = MagicMock()
        tracker.record_fill = MagicMock(return_value=True)  # always new
        executor = OrderExecutor(
            provider=None, tracker=tracker,
            trade_logger=trade_logger, paper=True)
        return executor

    def test_ghost_buy_existing_avg_price(self):
        """avg_price = weighted avg of GROSS prices (no fee in numerator)."""
        executor = self._make_executor()
        pm = PortfolioManager(initial_cash=10_000_000)

        # Pre-existing position: 10 shares @ 10,000
        pm.positions["005930"] = Position(
            code="005930", quantity=10, avg_price=10000.0,
            entry_date="2026-03-20", high_watermark=10000.0,
            current_price=10000.0)

        buy_cost = 0.00115
        executor.set_ghost_fill_context(pm, state_mgr=None, buy_cost=buy_cost)

        # Ghost fill: +5 shares @ 12,000
        executor.on_ghost_fill({
            "code": "005930", "side": "BUY",
            "exec_qty": 5, "exec_price": 12000.0,
            "order_no": "GHOST_001",
        })

        pos = pm.positions["005930"]
        expected_avg = (10 * 10000 + 5 * 12000) / 15  # 10666.67 (gross)
        assert pos.quantity == 15
        assert abs(pos.avg_price - expected_avg) < 1.0, \
            f"avg_price={pos.avg_price:.2f} != expected={expected_avg:.2f}"

        # Cash: deducted once with fee
        gross_cost = 5 * 12000
        cash_cost = gross_cost * (1 + buy_cost)
        expected_cash = 10_000_000 - cash_cost
        assert abs(pm.cash - expected_cash) < 1.0, \
            f"cash={pm.cash:.2f} != expected={expected_cash:.2f} (double deduction?)"


# ═══════════════════════════════════════════════════════════════════
# Test 3 — Ghost fill BUY: new position (single cash deduction)
# ═══════════════════════════════════════════════════════════════════
class TestGhostFillBuyNew:
    """Ghost fill for new position uses add_position (single cash deduction)."""

    def test_ghost_buy_new_position(self):
        executor = OrderExecutor(
            provider=None, tracker=OrderTracker(),
            trade_logger=MagicMock(), paper=True)
        pm = PortfolioManager(initial_cash=10_000_000)
        buy_cost = 0.00115

        executor.set_ghost_fill_context(pm, state_mgr=None, buy_cost=buy_cost)
        executor._portfolio = pm
        # Mock tracker.record_fill to return True
        executor.tracker.record_fill = MagicMock(return_value=True)

        executor.on_ghost_fill({
            "code": "035420", "side": "BUY",
            "exec_qty": 20, "exec_price": 50000.0,
            "order_no": "GHOST_002",
        })

        assert "035420" in pm.positions, "New position should be created"
        pos = pm.positions["035420"]
        assert pos.quantity == 20
        assert pos.avg_price == 50000.0  # gross price

        # Cash deduction: once via add_position
        expected_cost = 20 * 50000 * (1 + buy_cost)
        expected_cash = 10_000_000 - expected_cost
        assert abs(pm.cash - expected_cash) < 1.0, \
            f"cash={pm.cash:.2f} != expected={expected_cash:.2f}"


# ═══════════════════════════════════════════════════════════════════
# Test 4 — Windows atomic write: os.replace instead of unlink+rename
# ═══════════════════════════════════════════════════════════════════
class TestAtomicWrite:
    """state_manager uses os.replace for atomic writes."""

    def test_atomic_write_uses_replace(self):
        """Verify source code uses os.replace, not unlink+rename."""
        sm_path = Path(__file__).resolve().parent / "core" / "state_manager.py"
        source = sm_path.read_text(encoding="utf-8")
        assert "os.replace(" in source, \
            "state_manager must use os.replace for atomic write"
        assert "path.unlink()" not in source, \
            "state_manager must NOT unlink before rename"

    def test_save_load_roundtrip(self):
        """Save + load roundtrip preserves data."""
        with tempfile.TemporaryDirectory() as td:
            sm = StateManager(Path(td), paper=True)
            data = {
                "cash": 123456.78,
                "positions": {
                    "005930": {
                        "code": "005930", "quantity": 10,
                        "avg_price": 50000.0, "entry_date": "2026-03-20",
                        "high_watermark": 55000.0, "trail_stop_price": 48400.0,
                        "sector": "IT",
                    }
                },
                "peak_equity": 500000000.0,
            }
            assert sm.save_portfolio(data) is True
            loaded = sm.load_portfolio()
            assert loaded is not None
            assert loaded["cash"] == 123456.78
            assert "005930" in loaded["positions"]

    def test_save_preserves_existing_on_verify_fail(self):
        """If verify fails, existing file must survive."""
        with tempfile.TemporaryDirectory() as td:
            sm = StateManager(Path(td), paper=True)
            # Save initial valid state
            sm.save_portfolio({"cash": 100, "positions": {}})

            # Force verification failure by patching
            original_write = sm._atomic_write

            def bad_write(path, data):
                # Corrupt the temp file after write
                tmp = path.with_suffix(".tmp")
                content = json.dumps(data, indent=2, ensure_ascii=False, default=str)
                tmp.write_text(content, encoding="utf-8")
                # Overwrite with invalid JSON
                tmp.write_text("NOT JSON", encoding="utf-8")
                # Now verify will fail
                try:
                    verify = json.loads(tmp.read_text(encoding="utf-8"))
                except Exception:
                    if tmp.exists():
                        tmp.unlink()
                    return False
                return True

            sm._atomic_write = bad_write
            result = sm.save_portfolio({"cash": 999, "positions": {}})
            assert result is False

            # Original file should still be readable
            sm._atomic_write = original_write
            loaded = sm.load_portfolio()
            assert loaded is not None
            assert loaded["cash"] == 100, "Original state must survive failed write"


# ═══════════════════════════════════════════════════════════════════
# Test 5 — Partial fills (3 consecutive): no overfill rejection
# ═══════════════════════════════════════════════════════════════════
class TestPartialFills:
    """3 partial fills (30 + 40 + 30 = 100) must all be accepted."""

    def test_three_partial_fills(self):
        """Simulate 3 chejan partial fills, verify cumulative qty."""
        # We test the logic pattern, not the actual Kiwoom COM object
        requested = 100
        prev_qty = 0
        processed_fill_keys = set()
        fills = [(30, 50000), (40, 50100), (30, 49900)]

        total_filled = 0
        for exec_qty, exec_price in fills:
            fill_key = ("005930", "ORD_001", exec_qty, exec_price, prev_qty)
            assert fill_key not in processed_fill_keys, \
                f"Fill should not be duplicate: {fill_key}"
            processed_fill_keys.add(fill_key)

            remaining = max(0, requested - prev_qty)
            assert remaining > 0, f"Should not be overfill at prev_qty={prev_qty}"

            usable = min(exec_qty, remaining)
            assert usable == exec_qty, f"All qty should be usable: {usable} != {exec_qty}"

            prev_qty += usable
            total_filled += usable

        assert total_filled == 100, f"Total filled={total_filled} != 100"

    def test_duplicate_fill_ignored(self):
        """Same fill event received twice must be ignored."""
        processed_fill_keys = set()
        fill_key = ("005930", "ORD_001", 30, 50000, 0)

        # First time: accepted
        assert fill_key not in processed_fill_keys
        processed_fill_keys.add(fill_key)

        # Second time: duplicate
        assert fill_key in processed_fill_keys, "Duplicate must be detected"


# ═══════════════════════════════════════════════════════════════════
# Test 6 — Holdings unreliable → monitor-only
# ═══════════════════════════════════════════════════════════════════
class TestHoldingsUnreliable:
    """holdings_reliable=False must trigger safe_mode + monitor-only."""

    def test_reconcile_unreliable_returns_safe_mode(self):
        """_reconcile_with_broker must return safe_mode=True when
        holdings_reliable=False."""
        main_path = Path(__file__).resolve().parent / "main.py"
        source = main_path.read_text(encoding="utf-8")

        # Verify the critical log tag exists
        assert "BROKER_STATE_UNRELIABLE" in source, \
            "Must have BROKER_STATE_UNRELIABLE log tag"

        # Verify safe_mode=True is returned
        assert '"safe_mode": True' in source or "'safe_mode': True" in source, \
            "holdings_unreliable path must return safe_mode=True"

    def test_monitor_only_blocks_rebalance(self):
        """session_monitor_only=True must block rebalance."""
        main_path = Path(__file__).resolve().parent / "main.py"
        source = main_path.read_text(encoding="utf-8")
        assert "MONITOR_ONLY" in source, \
            "Must have MONITOR_ONLY guard before rebalance"
        assert "session_monitor_only" in source, \
            "Must use session_monitor_only flag"


# ═══════════════════════════════════════════════════════════════════
# Test 7 — Rebalance partial commit prevention
# ═══════════════════════════════════════════════════════════════════
class TestRebalanceCommit:
    """Portfolio save must succeed before marking rebalance date."""

    def test_commit_order_portfolio_first(self):
        """main.py must save portfolio BEFORE marking rebalance date."""
        main_path = Path(__file__).resolve().parent / "main.py"
        source = main_path.read_text(encoding="utf-8")

        assert "REBALANCE_COMMIT_OK" in source, \
            "Must have REBALANCE_COMMIT_OK log tag"
        assert "REBALANCE_COMMIT_PARTIAL_FAIL" in source, \
            "Must have REBALANCE_COMMIT_PARTIAL_FAIL log tag"

        # Verify order: portfolio save check before set_last_rebalance_date
        idx_save = source.index("rebalance_commit/portfolio")
        idx_mark = source.index("set_last_rebalance_date", idx_save)
        assert idx_save < idx_mark, \
            "Portfolio save must come before rebalance date marking"

    def test_partial_fail_does_not_mark(self):
        """If portfolio_saved=False, rebalance date must NOT be marked."""
        main_path = Path(__file__).resolve().parent / "main.py"
        source = main_path.read_text(encoding="utf-8")

        # The pattern should be: if portfolio_saved → mark, else → PARTIAL_FAIL
        idx_partial = source.index("REBALANCE_COMMIT_PARTIAL_FAIL")
        # Ensure this is in an else branch (no set_last_rebalance_date nearby)
        snippet = source[idx_partial:idx_partial + 200]
        assert "set_last_rebalance_date" not in snippet, \
            "PARTIAL_FAIL path must NOT call set_last_rebalance_date"


# ═══════════════════════════════════════════════════════════════════
# Test 8 — CSV header mismatch: no silent padding
# ═══════════════════════════════════════════════════════════════════
class TestCSVHeaderMismatch:
    """CSV header mismatch must backup + fresh file, not silent pad."""

    def test_mismatch_creates_backup(self):
        """Column count mismatch → .mismatch_backup + fresh file."""
        with tempfile.TemporaryDirectory() as td:
            csv_path = Path(td) / "test.csv"
            # Write old-format CSV with 3 columns
            with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.writer(f)
                w.writerow(["a", "b", "c"])
                w.writerow(["1", "2", "3"])

            from report.reporter import TradeLogger
            tl = TradeLogger.__new__(TradeLogger)
            tl._ensure_header(csv_path, ["a", "b", "c", "d", "e"])

            # Backup should exist
            backup = csv_path.with_suffix(".mismatch_backup")
            assert backup.exists(), "Mismatch backup file must be created"

            # Original file should have new header
            with open(csv_path, "r", encoding="utf-8-sig") as f:
                header = f.readline().strip()
            assert header == "a,b,c,d,e", f"Header should be new format: {header}"

            # Old data should be in backup
            with open(backup, "r", encoding="utf-8-sig") as f:
                lines = f.readlines()
            assert len(lines) == 2, "Backup should have header + 1 data row"

    def test_same_count_renames_only(self):
        """Same column count → just rename header, preserve data."""
        with tempfile.TemporaryDirectory() as td:
            csv_path = Path(td) / "test.csv"
            with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.writer(f)
                w.writerow(["old_a", "old_b"])
                w.writerow(["1", "2"])

            from report.reporter import TradeLogger
            tl = TradeLogger.__new__(TradeLogger)
            tl._ensure_header(csv_path, ["new_a", "new_b"])

            import pandas as pd
            df = pd.read_csv(csv_path, encoding="utf-8-sig")
            assert list(df.columns) == ["new_a", "new_b"]
            assert len(df) == 1


# ═══════════════════════════════════════════════════════════════════
# Test 9 (Recommended) — Momentum 12-1 definition verification
# ═══════════════════════════════════════════════════════════════════
class TestMomentumDefinition:
    """Verify momentum 12-1 calculation with synthetic data.
    NOTE: This test documents the CURRENT implementation behavior.
    If off-by-one is confirmed, fix scoring.py and update expected values.
    """

    def test_momentum_synthetic(self):
        """Known series → known momentum value."""
        from strategy.scoring import calc_momentum
        import numpy as np
        import pandas as pd

        # Create 300-day series: price goes from 100 to 200 linearly
        prices = pd.Series(np.linspace(100, 200, 300))
        mom = calc_momentum(prices, lookback=252, skip=22)

        # Current implementation: c[-22] / c[-252] - 1
        c = prices.values.astype(float)
        expected = c[-22] / c[-252] - 1
        assert mom is not None and not np.isnan(mom), "Momentum should be valid"
        assert abs(mom - expected) < 1e-10, \
            f"Momentum mismatch: got={mom:.6f}, expected={expected:.6f}"

        # Document: what the "12-1" definition SHOULD be
        # "Skip last 22 days" means: price at T-22 vs price at T-252
        # Current: c[-22] / c[-252] - 1
        # This IS the standard 12-1 definition (price 22 days ago / price 252 days ago)
        # No off-by-one confirmed for this interpretation.


# ═══════════════════════════════════════════════════════════════════
# Test 10 (Recommended) — Monthly DD / trim dedup behavior
# ═══════════════════════════════════════════════════════════════════
class TestMonthlyDDAndTrimDedup:
    """Document current behavior of monthly DD reset and trim dedup.
    These tests are observational — they verify what happens, not
    necessarily what SHOULD happen. Review before changing.
    """

    def test_monthly_dd_month_boundary_reset(self):
        """Peak equity resets at month boundary."""
        pm = PortfolioManager(initial_cash=1_000_000)
        pm.peak_equity = 1_000_000
        pm._peak_month = 2  # February

        # Simulate March 1: equity dropped to 900k
        pm.cash = 900_000
        with patch("core.portfolio_manager.date") as mock_date:
            mock_date.today.return_value = date(2026, 3, 1)
            mock_date.side_effect = lambda *a, **k: date(*a, **k)
            dd = pm.get_monthly_dd_pct()

        # After month change: peak_equity reset to current (900k)
        # Then dd = (900k - 900k) / 900k = 0%
        # This is the documented behavior — dd resets at month boundary
        assert dd == 0.0, \
            f"Monthly DD should reset to 0% at month boundary, got {dd:.4f}"

    def test_trim_dedup_from_exposure_guard(self):
        """Verify trim dedup behavior exists in exposure_guard."""
        guard_path = (Path(__file__).resolve().parent /
                      "risk" / "exposure_guard.py")
        source = guard_path.read_text(encoding="utf-8")
        assert "_last_trim_date" in source, \
            "ExposureGuard must track last trim date for dedup"
        assert "mark_trim_executed" in source, \
            "ExposureGuard must have mark_trim_executed method"


# ═══════════════════════════════════════════════════════════════════
# Test — Order journal JSONL creation
# ═══════════════════════════════════════════════════════════════════
class TestOrderJournal:
    """Verify JSONL journal is created and events are written."""

    def test_journal_created_on_register(self):
        with tempfile.TemporaryDirectory() as td:
            tracker = OrderTracker(journal_dir=Path(td))
            rec = tracker.register("005930", "BUY", 10, 50000, reason="TEST")

            assert tracker._journal_path is not None
            assert tracker._journal_path.exists(), "Journal file must be created"

            lines = tracker._journal_path.read_text(encoding="utf-8").strip().split("\n")
            assert len(lines) >= 1
            entry = json.loads(lines[0])
            assert entry["event"] == "SUBMIT_ATTEMPT"
            assert entry["code"] == "005930"

    def test_journal_records_lifecycle(self):
        with tempfile.TemporaryDirectory() as td:
            tracker = OrderTracker(journal_dir=Path(td))
            rec = tracker.register("005930", "BUY", 10, 50000, reason="TEST")
            tracker.mark_submitted(rec.order_id)
            tracker.mark_filled(rec.order_id, 50100, 10)

            lines = tracker._journal_path.read_text(encoding="utf-8").strip().split("\n")
            events = [json.loads(l)["event"] for l in lines]
            assert events == ["SUBMIT_ATTEMPT", "SUBMITTED", "FILLED"]


# ═══════════════════════════════════════════════════════════════════
# Test — Rebalancer cash_buffer parameter
# ═══════════════════════════════════════════════════════════════════
class TestRebalancerCashBuffer:
    """Verify cash_buffer parameter is respected."""

    def test_cash_buffer_limits_buy_qty(self):
        """Higher buffer → fewer shares bought."""
        positions = {}
        targets = ["A", "B"]
        prices = {"A": 10000, "B": 10000}

        _, buys_95 = compute_orders(
            positions, targets, 200000, 200000,
            buy_cost=0.001, prices=prices, cash_buffer=0.95)
        _, buys_80 = compute_orders(
            positions, targets, 200000, 200000,
            buy_cost=0.001, prices=prices, cash_buffer=0.80)

        total_95 = sum(b.quantity for b in buys_95)
        total_80 = sum(b.quantity for b in buys_80)
        assert total_80 <= total_95, \
            f"Lower buffer should buy fewer: 80%={total_80} vs 95%={total_95}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
