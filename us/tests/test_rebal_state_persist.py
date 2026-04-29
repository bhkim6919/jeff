"""Regression tests for rebalance-state persistence (state_manager.save_all).

The web executor writes rebal completion fields to disk via
``transition_phase_with_updates`` (atomic). Before this fix, the main
loop's periodic ``save_all`` re-loaded its own ``runtime_data`` dict
(seeded with empty rebal fields at process start) and clobbered the
disk values. After restart the engine "forgot" today's executed
rebalance — phase regressed to ``BATCH_DONE``, ``last_execute_*``
empty, and the dashboard re-armed Execute as ``Ready``.

These tests pin the fixed behaviour:

  * Disk-wins for every key in ``_REBAL_DEFAULTS`` — main loop never
    writes those fields, so disk is always the fresher source.
  * Memory-only fields (``dd_label``, ``buy_scale``, etc.) keep their
    normal main-loop semantics.
  * Reload after save returns the disk-side rebal values, not the
    initial memory snapshot.

Run from repo root::

    .venv64/Scripts/python.exe -m pytest us/tests/test_rebal_state_persist.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

US_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(US_ROOT))

from core.state_manager import _REBAL_DEFAULTS, StateManagerUS  # noqa: E402


@pytest.fixture
def state_dir(tmp_path):
    """Empty per-test state directory."""
    return tmp_path


@pytest.fixture
def sm(state_dir):
    return StateManagerUS(state_dir=str(state_dir))


def _read_runtime(state_dir: Path) -> dict:
    rt_path = state_dir / "runtime_state_us_paper.json"
    return json.loads(rt_path.read_text(encoding="utf-8"))


def _seed_initial(sm: StateManagerUS) -> dict:
    """Mimic main.py's startup load: empty rebal fields in memory."""
    runtime_data = {k: v for k, v in _REBAL_DEFAULTS.items()}
    runtime_data.update({
        "started_at":            "2026-04-29T13:51:00+00:00",
        "pid":                   12345,
        "shutdown_at":           "",
        "shutdown_reason":       "",
        "mode":                  "paper",
        "broker_snapshot_at":    "",
        "last_price_update_at":  "",
        "last_recon_ok":         True,
        "state_uncertain":       False,
        "last_recon_at":         "",
    })
    portfolio_data = {"cash": 100_000.0, "positions": {}}
    sm.save_all(portfolio_data, runtime_data)
    return runtime_data


def test_save_all_preserves_web_rebal_write(sm, state_dir):
    """Web writes EXECUTED → main loop save_all must NOT clobber it."""
    runtime_data = _seed_initial(sm)

    # Web rebalance executor walks the phase machine and writes
    # EXECUTED with the rebal-completion payload.
    sm.transition_phase_with_updates("BATCH_RUNNING", {})
    sm.transition_phase_with_updates("BATCH_DONE", {})
    sm.transition_phase_with_updates("DUE", {})
    sm.transition_phase_with_updates("EXECUTING", {})
    ok, _ = sm.transition_phase_with_updates("EXECUTED", {
        "last_execute_business_date":    "2026-04-29",
        "last_execute_request_id":       "req-abc-123",
        "last_execute_result":           "SUCCESS",
        "last_execute_snapshot_version": "2026-04-29:DB:..._POST_CLOSE",
        "last_rebalance_date":           "2026-04-29",
        "execute_lock":                  False,
        "execute_lock_owner":            "",
        "execute_lock_acquired_at":      "",
        "batch_fresh":                   False,
    })
    assert ok, "phase transition to EXECUTED rejected"

    # Main loop ticks: its in-memory runtime_data still has empty rebal
    # fields from initial load. This used to clobber the disk write.
    portfolio_data = {"cash": 99_500.0, "positions": {}}
    sm.save_all(portfolio_data, runtime_data)

    persisted = _read_runtime(state_dir)
    assert persisted["rebal_phase"] == "EXECUTED"
    assert persisted["last_execute_business_date"] == "2026-04-29"
    assert persisted["last_execute_request_id"] == "req-abc-123"
    assert persisted["last_execute_result"] == "SUCCESS"
    assert persisted["last_rebalance_date"] == "2026-04-29"
    assert persisted["batch_fresh"] is False


def test_main_loop_only_fields_unaffected(sm, state_dir):
    """``dd_label`` / ``buy_scale`` / ``buy_blocked`` are NOT in
    ``_REBAL_DEFAULTS`` and must follow normal save_all semantics."""
    runtime_data = _seed_initial(sm)

    # Main loop ticks with non-default values for memory-only fields.
    runtime_data["dd_label"] = "DAILY_BLOCKED"
    runtime_data["buy_scale"] = 0.0
    runtime_data["buy_blocked"] = True

    portfolio_data = {"cash": 99_500.0, "positions": {}}
    sm.save_all(portfolio_data, runtime_data)

    persisted = _read_runtime(state_dir)
    assert persisted["dd_label"] == "DAILY_BLOCKED"
    assert persisted["buy_scale"] == 0.0
    assert persisted["buy_blocked"] is True


def test_repeated_save_all_does_not_drift(sm, state_dir):
    """After web write + 5 successive save_all ticks, rebal fields stay."""
    runtime_data = _seed_initial(sm)

    # Walk the phase machine: IDLE → BATCH_RUNNING → BATCH_DONE → DUE
    # → EXECUTING → EXECUTED (valid path per _VALID_TRANSITIONS).
    sm.transition_phase_with_updates("BATCH_RUNNING", {})
    sm.transition_phase_with_updates("BATCH_DONE", {})
    sm.transition_phase_with_updates("DUE", {})
    sm.transition_phase_with_updates("EXECUTING", {})
    sm.transition_phase_with_updates("EXECUTED", {
        "last_execute_business_date": "2026-04-29",
        "last_execute_result":        "SUCCESS",
        "last_rebalance_date":        "2026-04-29",
    })

    for _ in range(5):
        sm.save_all({"cash": 99_500.0, "positions": {}}, runtime_data)

    persisted = _read_runtime(state_dir)
    assert persisted["rebal_phase"] == "EXECUTED"
    assert persisted["last_execute_business_date"] == "2026-04-29"
    assert persisted["last_execute_result"] == "SUCCESS"
    assert persisted["last_rebalance_date"] == "2026-04-29"


def test_restart_survives_web_write(sm, state_dir):
    """End-to-end: web writes EXECUTED → save_all → simulate restart by
    creating a new manager → reloaded state still EXECUTED."""
    _seed_initial(sm)
    # Walk the phase machine: IDLE → BATCH_RUNNING → BATCH_DONE → DUE
    # → EXECUTING → EXECUTED (valid path per _VALID_TRANSITIONS).
    sm.transition_phase_with_updates("BATCH_RUNNING", {})
    sm.transition_phase_with_updates("BATCH_DONE", {})
    sm.transition_phase_with_updates("DUE", {})
    sm.transition_phase_with_updates("EXECUTING", {})
    sm.transition_phase_with_updates("EXECUTED", {
        "last_execute_business_date": "2026-04-29",
        "last_execute_result":        "SUCCESS",
        "last_rebalance_date":        "2026-04-29",
        "last_execute_request_id":    "req-xyz",
    })
    runtime_data = sm.load_runtime() or {}
    sm.save_all({"cash": 99_500.0, "positions": {}}, runtime_data)

    # Simulate process restart.
    sm2 = StateManagerUS(state_dir=str(state_dir))
    rt = sm2.load_runtime() or {}
    assert rt["rebal_phase"] == "EXECUTED"
    assert rt["last_execute_business_date"] == "2026-04-29"
    assert rt["last_execute_request_id"] == "req-xyz"
    assert rt["last_rebalance_date"] == "2026-04-29"


def test_already_executed_today_check_works_after_save_all(sm, state_dir):
    """The state_manager's idempotency check at line 610 reads
    ``last_execute_business_date`` — must remain populated so AUTO mode
    blocks duplicate execution after main-loop save_all ticks."""
    runtime_data = _seed_initial(sm)

    # Walk the phase machine: IDLE → BATCH_RUNNING → BATCH_DONE → DUE
    # → EXECUTING → EXECUTED (valid path per _VALID_TRANSITIONS).
    sm.transition_phase_with_updates("BATCH_RUNNING", {})
    sm.transition_phase_with_updates("BATCH_DONE", {})
    sm.transition_phase_with_updates("DUE", {})
    sm.transition_phase_with_updates("EXECUTING", {})
    sm.transition_phase_with_updates("EXECUTED", {
        "last_execute_business_date": "2026-04-29",
        "last_execute_result":        "SUCCESS",
        "last_rebalance_date":        "2026-04-29",
    })
    sm.save_all({"cash": 99_500.0, "positions": {}}, runtime_data)

    rt = sm.load_runtime() or {}
    today_bd = "2026-04-29"
    # This mirrors the gate at state_manager.py:610.
    already_executed = rt.get("last_execute_business_date", "") == today_bd
    assert already_executed, (
        "AUTO mode would re-rebalance because save_all clobbered the "
        "business date — Jeff's incident scenario"
    )
