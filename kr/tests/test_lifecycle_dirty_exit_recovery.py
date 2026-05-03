"""RG5 lifecycle test: dirty exit detection + classification.

Scope: verify that StateManager.was_dirty_exit() correctly distinguishes
the 4 shutdown reason categories across save → restart cycles.

Reasons (per state_manager.py):
  - 'running'      → DIRTY (process didn't reach mark_shutdown call site)
  - 'normal'       → clean
  - 'sigint'       → clean (Ctrl+C handler reached)
  - 'eod_complete' → clean (EOD finished)
  - 'unknown'      → DIRTY (no shutdown_reason recorded — first ever boot
                     OR runtime file truncated)

This test pins the contract used by startup_phase to decide whether to
trigger recovery flow (cancel_all_open_orders, monitor_only enforcement).
A regression here directly affects RG1.

Run from repo root::

    .venv64/Scripts/python.exe -m pytest kr/tests/test_lifecycle_dirty_exit_recovery.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

KR_TESTS = Path(__file__).resolve().parent
if str(KR_TESTS) not in sys.path:
    sys.path.insert(0, str(KR_TESTS))

from conftest_lifecycle import LifecycleHarness  # noqa: E402


# ── Reason classification across restart ──────────────────────────────


def test_normal_shutdown_then_restart_is_clean(tmp_path):
    """정상 종료 → 재시작 시 was_dirty_exit() == False."""
    h = LifecycleHarness(tmp_path)
    h.mark_startup()
    h.normal_shutdown()

    sm = h.restart()
    assert sm.was_dirty_exit() is False
    assert sm.get_last_shutdown_reason() == "normal"


def test_running_crash_then_restart_is_dirty(tmp_path):
    """크래시 (mark_shutdown 미도달) → 재시작 시 was_dirty_exit() == True."""
    h = LifecycleHarness(tmp_path)
    h.mark_startup()
    h.crash()  # never reach mark_shutdown

    sm = h.restart()
    assert sm.was_dirty_exit() is True
    assert sm.get_last_shutdown_reason() == "running"


def test_sigint_shutdown_then_restart_is_clean(tmp_path):
    """SIGINT (Ctrl+C handler 도달) → 재시작 시 was_dirty_exit() == False."""
    h = LifecycleHarness(tmp_path)
    h.mark_startup()
    h.sigint_shutdown()

    sm = h.restart()
    assert sm.was_dirty_exit() is False
    assert sm.get_last_shutdown_reason() == "sigint"


def test_eod_complete_shutdown_then_restart_is_clean(tmp_path):
    """EOD 완료 종료 → 재시작 시 was_dirty_exit() == False."""
    h = LifecycleHarness(tmp_path)
    h.mark_startup()
    h.eod_complete_shutdown()

    sm = h.restart()
    assert sm.was_dirty_exit() is False
    assert sm.get_last_shutdown_reason() == "eod_complete"


# ── Edge cases ────────────────────────────────────────────────────────


def test_first_ever_boot_is_dirty(tmp_path):
    """state 파일이 존재하지 않는 첫 부팅 → was_dirty_exit() == True (보수적)."""
    h = LifecycleHarness(tmp_path)
    # Do NOT call mark_startup — state file may not exist yet
    sm = h.restart()
    # 'unknown' (default when reason missing) maps to dirty per spec
    assert sm.was_dirty_exit() is True
    assert sm.get_last_shutdown_reason() == "unknown"


def test_running_state_persists_across_restart_until_resolved(tmp_path):
    """크래시 상태는 새 mark_startup / mark_shutdown 가 덮어쓰기 전까지 유지된다."""
    h = LifecycleHarness(tmp_path)
    h.mark_startup()
    h.crash()

    # First restart sees dirty
    sm1 = h.restart()
    assert sm1.was_dirty_exit() is True

    # Second restart WITHOUT recovery action — still dirty
    sm2 = h.restart()
    assert sm2.was_dirty_exit() is True


def test_recovery_after_dirty_exit_clears_dirty_flag(tmp_path):
    """크래시 → 재시작 → mark_startup + normal shutdown → 다음 재시작은 clean."""
    h = LifecycleHarness(tmp_path)
    h.mark_startup()
    h.crash()

    sm1 = h.restart()
    assert sm1.was_dirty_exit() is True

    # Simulate completed recovery session via the new state_mgr
    sm1.mark_startup()
    sm1.mark_shutdown("normal")

    # Re-instantiate again to verify persistence
    sm2 = h.restart()
    assert sm2.was_dirty_exit() is False
    assert sm2.get_last_shutdown_reason() == "normal"


def test_mark_shutdown_overrides_running_state(tmp_path):
    """mark_startup 후 정상 종료는 'running' 을 'normal' 로 덮어쓴다."""
    h = LifecycleHarness(tmp_path)
    h.mark_startup()
    # Verify intermediate state is 'running' (dirty if crashed here)
    rt = h.state_mgr.load_runtime()
    assert rt.get("shutdown_reason") == "running"

    h.normal_shutdown()
    rt2 = h.state_mgr.load_runtime()
    assert rt2.get("shutdown_reason") == "normal"


def test_session_start_recorded_on_mark_startup(tmp_path):
    """mark_startup 호출 시 session_start 타임스탬프가 기록된다."""
    h = LifecycleHarness(tmp_path)
    h.mark_startup()
    rt = h.state_mgr.load_runtime()
    assert rt.get("session_start", "") != ""
    assert rt.get("shutdown_reason") == "running"


def test_session_end_recorded_on_clean_shutdown(tmp_path):
    """clean shutdown 시 session_end 타임스탬프가 기록된다."""
    h = LifecycleHarness(tmp_path)
    h.mark_startup()
    h.normal_shutdown()
    rt = h.state_mgr.load_runtime()
    assert rt.get("session_end", "") != ""


# ── Cross-mode parity (tradeable confidence) ──────────────────────────


def test_paper_test_mode_isolated_from_other_modes(tmp_path):
    """paper_test 모드의 shutdown_reason 은 paper/live 모드에 영향 주지 않는다.

    StateManager 는 trading_mode 별로 분리된 runtime 파일을 사용하므로
    (`runtime_state_paper_test.json` vs `runtime_state_paper.json` etc.),
    한 모드의 dirty exit 가 다른 모드의 was_dirty_exit() 결과를 오염시키면
    안 된다. lifecycle test 는 paper_test 모드에서 실행되므로 라이브
    paper/live state 파일이 영향받지 않음을 보장.
    """
    from core.state_manager import StateManager

    # paper_test mode → dirty exit
    h = LifecycleHarness(tmp_path)
    h.mark_startup()
    h.crash()
    assert h.restart().was_dirty_exit() is True

    # paper mode in same dir → clean (no prior session)
    sm_paper = StateManager(state_dir=h.state_dir, trading_mode="paper", backup_dirs=[])
    # First boot in paper mode → 'unknown' → dirty (보수적, 정상 동작)
    # 핵심 invariant: paper_test 의 reason ('running') 이 paper 로 leak 되지 않음
    assert sm_paper.get_last_shutdown_reason() != "running"
