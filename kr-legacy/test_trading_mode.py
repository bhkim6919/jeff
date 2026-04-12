"""
test_trading_mode.py — Trading mode terminology & guard tests
===============================================================
Tests A-H: Mode validation, state separation, backward compat.

TRADING_MODE is the operator's intended mode.
server_type is the broker's actual connected environment.
If they do not match, abort immediately.
  mock  = internal simulation only
  paper = broker mock trading
  live  = broker real trading

Run:  python test_trading_mode.py
"""
import json
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent))


# ═══════════════════════════════════════════════════════════════════
# Test A — mock mode: no broker, simulate=True, MOCK logs
# ═══════════════════════════════════════════════════════════════════
def test_a_mock_mode():
    from runtime.order_executor import OrderExecutor
    from runtime.order_tracker import OrderTracker

    tracker = OrderTracker()
    tl = MagicMock()
    ex = OrderExecutor(provider=None, tracker=tracker, trade_logger=tl,
                       simulate=True, trading_mode="mock")

    assert ex.simulate is True, "mock must have simulate=True"
    assert ex.trading_mode == "mock"

    # Verify mock sell produces MOCK prefix and log
    rec = tracker.register("005930", "SELL", 10, 50000)
    # _simulate_sell needs price — provider is None, will fail gracefully
    result = ex.execute_sell("005930", 10, "TEST")
    # With None provider, price=0 → rejected (expected)
    assert result.get("error"), "No provider → should fail with no price"

    # Verify source code patterns
    src = Path("runtime/order_executor.py").read_text("utf-8")
    assert "MOCK_" in src, "Mock order_no must use MOCK_ prefix"
    assert "[MOCK SELL]" in src, "Must have [MOCK SELL] log"
    assert "[MOCK BUY]" in src, "Must have [MOCK BUY] log"
    assert '[PAPER SELL]' not in src, "PAPER SELL must not exist in executor"
    assert '[PAPER BUY]' not in src, "PAPER BUY must not exist in executor"
    print("Test A PASS: mock mode")


# ═══════════════════════════════════════════════════════════════════
# Test B — paper mode: server=MOCK, simulate=False, no abort
# ═══════════════════════════════════════════════════════════════════
def test_b_paper_mode():
    from main import validate_trading_mode

    # paper + MOCK → OK
    validate_trading_mode("paper", "MOCK", broker_connected=True)
    print("Test B PASS: paper + MOCK = OK")


# ═══════════════════════════════════════════════════════════════════
# Test C — live mode: server=REAL, simulate=False
# ═══════════════════════════════════════════════════════════════════
def test_c_live_mode():
    from main import validate_trading_mode

    # live + REAL → OK
    validate_trading_mode("live", "REAL", broker_connected=True)
    print("Test C PASS: live + REAL = OK")


# ═══════════════════════════════════════════════════════════════════
# Test D — live + MOCK mismatch → abort
# ═══════════════════════════════════════════════════════════════════
def test_d_live_mock_mismatch():
    from main import validate_trading_mode

    try:
        validate_trading_mode("live", "MOCK", broker_connected=True)
        assert False, "Should have raised RuntimeError"
    except RuntimeError as e:
        assert "MODE_MISMATCH_ABORT" in str(e)
        assert "live" in str(e) and "MOCK" in str(e)
    print("Test D PASS: live + MOCK → abort")


# ═══════════════════════════════════════════════════════════════════
# Test E — paper + REAL mismatch → abort
# ═══════════════════════════════════════════════════════════════════
def test_e_paper_real_mismatch():
    from main import validate_trading_mode

    try:
        validate_trading_mode("paper", "REAL", broker_connected=True)
        assert False, "Should have raised RuntimeError"
    except RuntimeError as e:
        assert "MODE_MISMATCH_ABORT" in str(e)
    print("Test E PASS: paper + REAL → abort")


# ═══════════════════════════════════════════════════════════════════
# Test F — mock + broker connected → abort
# ═══════════════════════════════════════════════════════════════════
def test_f_mock_broker_connected():
    from main import validate_trading_mode

    try:
        validate_trading_mode("mock", "MOCK", broker_connected=True)
        assert False, "Should have raised RuntimeError"
    except RuntimeError as e:
        assert "MODE_MISMATCH_ABORT" in str(e)
        assert "mock" in str(e)
    print("Test F PASS: mock + broker = abort")


# ═══════════════════════════════════════════════════════════════════
# Test G — state file separation
# ═══════════════════════════════════════════════════════════════════
def test_g_state_separation():
    from core.state_manager import StateManager

    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)

        # Create mock state
        sm_mock = StateManager(td_path, trading_mode="mock")
        sm_mock.save_portfolio({"cash": 111, "positions": {}})

        # Create paper state
        sm_paper = StateManager(td_path, trading_mode="paper")
        sm_paper.save_portfolio({"cash": 222, "positions": {}})

        # Create live state
        sm_live = StateManager(td_path, trading_mode="live")
        sm_live.save_portfolio({"cash": 333, "positions": {}})

        # Verify separate files
        assert (td_path / "portfolio_state_mock.json").exists()
        assert (td_path / "portfolio_state_paper.json").exists()
        assert (td_path / "portfolio_state_live.json").exists()

        # Verify data isolation
        mock_data = sm_mock.load_portfolio()
        paper_data = sm_paper.load_portfolio()
        live_data = sm_live.load_portfolio()

        assert mock_data["cash"] == 111, f"mock cash={mock_data['cash']}"
        assert paper_data["cash"] == 222, f"paper cash={paper_data['cash']}"
        assert live_data["cash"] == 333, f"live cash={live_data['cash']}"

        # Verify mock doesn't contaminate paper/live
        sm_mock.save_portfolio({"cash": 999, "positions": {}})
        assert sm_paper.load_portfolio()["cash"] == 222, "paper must be unaffected"
        assert sm_live.load_portfolio()["cash"] == 333, "live must be unaffected"

    print("Test G PASS: state file separation")


# ═══════════════════════════════════════════════════════════════════
# Test H — backward compatibility: paper= kwarg
# ═══════════════════════════════════════════════════════════════════
def test_h_backward_compat():
    from runtime.order_executor import OrderExecutor
    from runtime.order_tracker import OrderTracker
    import logging

    # Capture deprecation warning
    warnings = []
    original_warning = logging.getLogger("gen4.executor").warning

    def capture_warning(msg, *args, **kwargs):
        warnings.append(msg)
        original_warning(msg, *args, **kwargs)

    logging.getLogger("gen4.executor").warning = capture_warning

    try:
        # Old-style: paper=True (should work as simulate=True)
        ex = OrderExecutor(
            provider=None, tracker=OrderTracker(),
            trade_logger=MagicMock(), paper=True)
        assert ex.simulate is True, "paper=True should map to simulate=True"
        assert any("DEPRECATED" in w for w in warnings), \
            f"Should emit deprecation warning, got: {warnings}"
    finally:
        logging.getLogger("gen4.executor").warning = original_warning

    # StateManager backward compat
    with tempfile.TemporaryDirectory() as td:
        from core.state_manager import StateManager
        sm = StateManager(Path(td), paper=True)
        assert sm.trading_mode == "paper", \
            f"paper=True should map to trading_mode='paper', got {sm.trading_mode}"

    print("Test H PASS: backward compatibility")


# ═══════════════════════════════════════════════════════════════════
# Test I — 2nd hard gate in OrderExecutor
# ═══════════════════════════════════════════════════════════════════
def test_i_order_level_gate():
    from runtime.order_executor import OrderExecutor
    from runtime.order_tracker import OrderTracker

    # mock mode executor trying broker path should raise
    ex = OrderExecutor(
        provider=MagicMock(), tracker=OrderTracker(),
        trade_logger=MagicMock(), simulate=False, trading_mode="mock")

    try:
        ex._check_broker_gate()
        assert False, "mock mode must not reach broker"
    except RuntimeError as e:
        assert "MODE_GATE" in str(e)

    print("Test I PASS: 2nd hard gate")


# ═══════════════════════════════════════════════════════════════════
# Test J — journal includes trading_mode
# ═══════════════════════════════════════════════════════════════════
def test_j_journal_trading_mode():
    from runtime.order_tracker import OrderTracker

    with tempfile.TemporaryDirectory() as td:
        tracker = OrderTracker(journal_dir=Path(td), trading_mode="paper")
        rec = tracker.register("005930", "BUY", 10, 50000, reason="TEST")

        lines = tracker._journal_path.read_text("utf-8").strip().split("\n")
        entry = json.loads(lines[0])
        assert entry["trading_mode"] == "paper", \
            f"Journal must include trading_mode, got: {entry}"

    print("Test J PASS: journal includes trading_mode")


# ═══════════════════════════════════════════════════════════════════
# Test K — state migration from legacy file
# ═══════════════════════════════════════════════════════════════════
def test_k_state_migration():
    from core.state_manager import StateManager

    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)

        # Create legacy _paper file (old format)
        legacy = td_path / "portfolio_state_paper.json"
        legacy.write_text(json.dumps({
            "cash": 500000000, "positions": {},
            "version": "4.0"
        }))

        # Open as mock mode — should migrate from _paper
        sm = StateManager(td_path, trading_mode="mock")
        loaded = sm.load_portfolio()
        assert loaded is not None, "Should migrate from legacy _paper file"
        assert loaded["cash"] == 500000000

        # Verify new file created
        assert (td_path / "portfolio_state_mock.json").exists()

    print("Test K PASS: state migration")


if __name__ == "__main__":
    test_a_mock_mode()
    test_b_paper_mode()
    test_c_live_mode()
    test_d_live_mock_mismatch()
    test_e_paper_real_mismatch()
    test_f_mock_broker_connected()
    test_g_state_separation()
    test_h_backward_compat()
    test_i_order_level_gate()
    test_j_journal_trading_mode()
    test_k_state_migration()

    print()
    print("=" * 50)
    print("  ALL 11 TESTS PASSED (A-K)")
    print("=" * 50)
