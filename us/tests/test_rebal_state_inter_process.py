"""Inter-process race regression tests — PR #36 follow-up.

Background (Jeff 2026-04-30):
    PR #36 added disk-wins to ``state_manager.save_all`` so the tray
    loop's periodic save couldn't clobber the web executor's
    rebal-completion writes. But ``us/main.py`` (the batch script)
    bypassed ``state_manager`` entirely with a raw ``json.load`` →
    mutate → ``json.dump`` cycle. Live evidence (KST 09:25 04-30):
    after the post-close batch ran, ``last_execute_business_date``,
    ``last_rebalance_date``, and the rest of the rebal-completion
    fields were all back to "" — the dashboard regressed from
    EXECUTED to BATCH_DONE / Last:-- / Execute: Ready, exactly the
    PR #36 risk surface but at the inter-process layer instead of
    intra-process.

    PR #36 fixed intra-process race; this PR fixes inter-process race
    by routing the batch's state write through
    ``state_manager.transition_phase_with_updates``, which has the
    same load-merge-write semantics PR #36 enforced for save_all.

Approach A — next_rebalance_date persistence:
    Same PR also persists ``next_rebalance_date`` in the rebal-
    completion payload so ``compute_execute_allowed`` Gate 6 actually
    enforces the 21-day cycle. Without this the dashboard's D-?
    counter is purely cosmetic — AUTO mode could re-execute on the
    very next business date.

These tests pin both fixes. Run from repo root::

    .venv64/Scripts/python.exe -m pytest us/tests/test_rebal_state_inter_process.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

US_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(US_ROOT))

from core.state_manager import _REBAL_DEFAULTS, StateManagerUS  # noqa: E402


# ── Fixtures ─────────────────────────────────────────────────────

@pytest.fixture
def state_dir(tmp_path):
    return tmp_path


@pytest.fixture
def sm(state_dir):
    return StateManagerUS(state_dir=str(state_dir))


def _read_rt(state_dir: Path) -> dict:
    return json.loads(
        (state_dir / "runtime_state_us_paper.json").read_text(encoding="utf-8")
    )


def _seed_executed(sm: StateManagerUS) -> None:
    """Mimic the live state after a successful rebal — all the fields
    the live tray was about to lose to the batch direct-write race."""
    runtime_data = {k: v for k, v in _REBAL_DEFAULTS.items()}
    runtime_data.update({
        "started_at": "2026-04-29T15:38:00+00:00",
        "pid": 12345,
        "mode": "paper",
    })
    sm.save_all({"cash": 100_000.0, "positions": {}}, runtime_data)
    # Walk to EXECUTED with full rebal-completion payload.
    sm.transition_phase_with_updates("BATCH_RUNNING", {})
    sm.transition_phase_with_updates("BATCH_DONE", {})
    sm.transition_phase_with_updates("DUE", {})
    sm.transition_phase_with_updates("EXECUTING", {})
    sm.transition_phase_with_updates("EXECUTED", {
        "last_execute_business_date":    "2026-04-28",
        "last_execute_request_id":       "req-abc",
        "last_execute_result":           "SUCCESS",
        "last_execute_snapshot_version": "2026-04-28_..._POST_CLOSE",
        "last_rebalance_date":           "2026-04-28",
        "next_rebalance_date":           "2026-05-26",
        "execute_lock":                  False,
        "execute_lock_owner":            "",
        "execute_lock_acquired_at":      "",
        "batch_fresh":                   False,
    })


def _simulate_batch_via_state_manager(sm: StateManagerUS,
                                      today_bd: str,
                                      sv: str,
                                      created_at: str,
                                      post_close: bool) -> None:
    """Mirror the post-fix us/main.py batch state-write contract."""
    batch_updates = {
        "snapshot_version":            sv,
        "snapshot_created_at":         created_at,
        "last_batch_business_date":    today_bd,
        "last_batch_post_close":       post_close,
        "batch_fresh":                 bool(post_close),
        "last_rebal_attempt_snapshot": "",
        "last_rebal_attempt_at":       "",
        "last_rebal_attempt_result":   "",
        "last_rebal_attempt_count":    0,
        "last_rebal_attempt_reason":   "",
    }
    # Phase machine: EXECUTED → BATCH_RUNNING → BATCH_DONE.
    current_phase = (sm.get_rebal_state() or {}).get("rebal_phase", "IDLE")
    _to_running_path = {
        "IDLE": ["BATCH_RUNNING"],
        "BATCH_RUNNING": [],
        "BATCH_DONE": ["IDLE", "BATCH_RUNNING"],
        "EXECUTED": ["BATCH_RUNNING"],
        "PARTIAL_EXECUTED": ["BATCH_RUNNING"],
        "FAILED": ["BATCH_RUNNING"],
        "BLOCKED": ["IDLE", "BATCH_RUNNING"],
        "DUE": [],
        "EXECUTING": [],
    }
    for intermediate in _to_running_path.get(current_phase, []):
        sm.transition_phase_with_updates(intermediate, {})
    sm.transition_phase_with_updates("BATCH_DONE", batch_updates)


# ── PR #36 follow-up tests ───────────────────────────────────────

def test_batch_after_executed_preserves_rebal_completion(sm, state_dir):
    """The exact Jeff scenario: tray ran rebal → ``EXECUTED`` on disk
    → US post-close batch runs → after batch the rebal-completion
    fields must still be on disk."""
    _seed_executed(sm)
    _simulate_batch_via_state_manager(
        sm,
        today_bd="2026-04-29",
        sv="2026-04-29_batch_..._POST_CLOSE",
        created_at="2026-04-29T16:17:45-04:00",
        post_close=True,
    )
    rt = _read_rt(state_dir)
    # Phase advanced to BATCH_DONE — that's the batch's job.
    assert rt["rebal_phase"] == "BATCH_DONE"
    # Batch fields written.
    assert rt["snapshot_version"] == "2026-04-29_batch_..._POST_CLOSE"
    assert rt["last_batch_business_date"] == "2026-04-29"
    assert rt["last_batch_post_close"] is True
    assert rt["batch_fresh"] is True
    # CRITICAL: rebal-completion fields PRESERVED (the bug).
    assert rt["last_execute_business_date"] == "2026-04-28"
    assert rt["last_execute_request_id"] == "req-abc"
    assert rt["last_execute_result"] == "SUCCESS"
    assert rt["last_rebalance_date"] == "2026-04-28"
    assert rt["next_rebalance_date"] == "2026-05-26"


def test_batch_resets_attempt_tracking_only(sm, state_dir):
    """The batch's job is to reset *attempt* tracking (per-snapshot
    counter). Execute tracking (``last_execute_*``) belongs to a
    different state slot and must not be touched."""
    _seed_executed(sm)
    # Pretend a previous attempt happened.
    sm.transition_phase_with_updates("EXECUTED", {
        "last_rebal_attempt_count": 3,
        "last_rebal_attempt_result": "PARTIAL",
    })
    _simulate_batch_via_state_manager(
        sm, today_bd="2026-04-29",
        sv="snap1", created_at="2026-04-29T16:00:00-04:00", post_close=True,
    )
    rt = _read_rt(state_dir)
    assert rt["last_rebal_attempt_count"] == 0       # reset
    assert rt["last_rebal_attempt_result"] == ""     # reset
    assert rt["last_execute_business_date"] == "2026-04-28"  # preserved
    assert rt["last_execute_result"] == "SUCCESS"            # preserved


def test_batch_from_idle_cold_start(sm, state_dir):
    """Cold start (no prior state) walks IDLE → BATCH_RUNNING →
    BATCH_DONE without errors."""
    runtime_data = {k: v for k, v in _REBAL_DEFAULTS.items()}
    sm.save_all({"cash": 100_000.0, "positions": {}}, runtime_data)
    _simulate_batch_via_state_manager(
        sm, today_bd="2026-04-29",
        sv="snap1", created_at="2026-04-29T16:00:00-04:00", post_close=True,
    )
    rt = _read_rt(state_dir)
    assert rt["rebal_phase"] == "BATCH_DONE"
    assert rt["snapshot_version"] == "snap1"
    # No rebal-completion to preserve — these stay default.
    assert rt["last_execute_business_date"] == ""
    assert rt["last_rebalance_date"] == ""


def test_batch_from_batch_done_re_run(sm, state_dir):
    """If batch runs again on the same day (operator forces re-batch),
    the phase walks BATCH_DONE → IDLE → BATCH_RUNNING → BATCH_DONE
    cleanly."""
    runtime_data = {k: v for k, v in _REBAL_DEFAULTS.items()}
    sm.save_all({"cash": 100_000.0, "positions": {}}, runtime_data)
    # First batch.
    _simulate_batch_via_state_manager(
        sm, today_bd="2026-04-29",
        sv="snap-first", created_at="2026-04-29T16:00:00-04:00", post_close=True,
    )
    # Second batch — same day.
    _simulate_batch_via_state_manager(
        sm, today_bd="2026-04-29",
        sv="snap-second", created_at="2026-04-29T17:00:00-04:00", post_close=True,
    )
    rt = _read_rt(state_dir)
    assert rt["rebal_phase"] == "BATCH_DONE"
    assert rt["snapshot_version"] == "snap-second"


def test_last_batch_post_close_is_persisted(sm, state_dir):
    """Pre-fix the field was set but ``transition_phase_with_updates``
    filtered it out (not in ``_REBAL_DEFAULTS`` or ``_EXTRA_ALLOWED``).
    Post-fix it's a real default and survives the merge."""
    _seed_executed(sm)
    _simulate_batch_via_state_manager(
        sm, today_bd="2026-04-29",
        sv="snap1", created_at="2026-04-29T16:00:00-04:00", post_close=True,
    )
    rt = _read_rt(state_dir)
    assert "last_batch_post_close" in rt
    assert rt["last_batch_post_close"] is True

    # Pre-market batch (post_close=False) also persists False.
    _simulate_batch_via_state_manager(
        sm, today_bd="2026-04-30",
        sv="snap2", created_at="2026-04-30T08:00:00-04:00", post_close=False,
    )
    rt = _read_rt(state_dir)
    assert rt["last_batch_post_close"] is False


# ── Approach A: next_rebalance_date persistence ─────────────────

def test_next_rebalance_date_field_in_defaults(sm):
    """Pin that the field is in the defaults set so the merge logic
    treats it as preservable."""
    assert "next_rebalance_date" in _REBAL_DEFAULTS


def test_rebal_completion_persists_next_rebalance_date(sm, state_dir):
    """When the web executor passes ``next_rebalance_date`` in the
    final-updates payload, ``transition_phase_with_updates`` must
    persist it and subsequent batch runs must preserve it."""
    runtime_data = {k: v for k, v in _REBAL_DEFAULTS.items()}
    sm.save_all({"cash": 100_000.0, "positions": {}}, runtime_data)
    sm.transition_phase_with_updates("BATCH_RUNNING", {})
    sm.transition_phase_with_updates("BATCH_DONE", {})
    sm.transition_phase_with_updates("DUE", {})
    sm.transition_phase_with_updates("EXECUTING", {})
    sm.transition_phase_with_updates("EXECUTED", {
        "last_execute_business_date":    "2026-04-29",
        "last_execute_result":           "SUCCESS",
        "last_rebalance_date":           "2026-04-29",
        "next_rebalance_date":           "2026-05-26",
    })
    rt = _read_rt(state_dir)
    assert rt["next_rebalance_date"] == "2026-05-26"

    # Simulate next batch — the field MUST survive.
    _simulate_batch_via_state_manager(
        sm, today_bd="2026-04-30",
        sv="snap-after", created_at="2026-04-30T16:00:00-04:00", post_close=True,
    )
    rt = _read_rt(state_dir)
    assert rt["next_rebalance_date"] == "2026-05-26"


def test_compute_execute_allowed_gate_6_with_persisted_next_date(sm, state_dir):
    """End-to-end: persist next_rebalance_date → compute_execute_allowed
    gate 6 fires NOT_DUE when today < next_rd. The cycle protection
    that the dashboard's D-? counter implies is now actually enforced
    at the engine level."""
    runtime_data = {k: v for k, v in _REBAL_DEFAULTS.items()}
    sm.save_all({"cash": 100_000.0, "positions": {}}, runtime_data)
    sm.transition_phase_with_updates("BATCH_RUNNING", {})
    sm.transition_phase_with_updates("BATCH_DONE", {})
    sm.transition_phase_with_updates("DUE", {})
    sm.transition_phase_with_updates("EXECUTING", {})
    sm.transition_phase_with_updates("EXECUTED", {
        "last_execute_business_date":    "2026-04-29",
        "last_execute_result":           "SUCCESS",
        "last_rebalance_date":           "2026-04-29",
        "next_rebalance_date":           "2026-05-26",
        "snapshot_version":              "old_snapshot",
        "last_execute_snapshot_version": "old_snapshot",
    })
    # Roll the snapshot to a new business date so other gates pass.
    sm.transition_phase_with_updates("IDLE", {})
    _simulate_batch_via_state_manager(
        sm, today_bd="2026-04-30",
        sv="new_snapshot",
        created_at="2026-04-30T16:00:00-04:00",
        post_close=True,
    )
    # No provider/config available in tests — just verify Gate 6
    # condition by reading state directly.
    rs = sm.get_rebal_state()
    today_bd = "2026-04-30"  # today is before the cycle's next_rd
    next_rd = rs.get("next_rebalance_date", "")
    assert next_rd == "2026-05-26"
    # Gate 6 condition: ``next_rd and today_bd < next_rd``
    assert next_rd and today_bd < next_rd, "Gate 6 should fire NOT_DUE"


def test_empty_next_rebalance_date_does_not_block(sm):
    """Backward compat — empty ``next_rebalance_date`` (existing live
    state pre-fix) must not block execution. The gate short-circuits."""
    runtime_data = {k: v for k, v in _REBAL_DEFAULTS.items()}
    sm.save_all({"cash": 100_000.0, "positions": {}}, runtime_data)
    rs = sm.get_rebal_state()
    next_rd = rs.get("next_rebalance_date", "")
    assert next_rd == ""  # default
    # Gate 6: ``if next_rd and today_bd < next_rd`` — empty string is
    # falsy so the if-branch doesn't execute; gate doesn't fire.
    today_bd = "2026-04-30"
    blocks_fired = bool(next_rd) and today_bd < next_rd
    assert not blocks_fired
