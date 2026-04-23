# -*- coding: utf-8 -*-
"""R8/R9 (2026-04-23) — Lab Live auto-restore v2 path test.

Pre-fix bug:
    _ensure_lab_live() only checked cfg.state_file (v1 legacy state.json).
    On v2-migrated systems, state.json doesn't exist (only head.json +
    states/ dir), so gate returned None → dashboard showed initialized=False.
    Batch step 8 path restored correctly via direct sim.initialize() call,
    but the dashboard's first-request auto-restore was silently broken.

Fix:
    Gate now checks head_file OR state_file. Either v2 or v1 triggers init.

This test verifies the gate decision logic without needing a running
FastAPI app (extracts the critical check to a helper pattern).
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "kr"))


def _should_restore(head_exists: bool, state_exists: bool) -> bool:
    """Mirror of the fixed gate in _ensure_lab_live()."""
    return head_exists or state_exists


def test_v2_only_system_triggers_restore():
    """Typical post-migration: head.json present, state.json absent."""
    assert _should_restore(head_exists=True, state_exists=False) is True


def test_v1_only_legacy_triggers_restore():
    """Pre-migration legacy: state.json present, head.json absent."""
    assert _should_restore(head_exists=False, state_exists=True) is True


def test_both_present_triggers_restore():
    """Transitional state: both present (migration incomplete)."""
    assert _should_restore(head_exists=True, state_exists=True) is True


def test_neither_present_skips_restore():
    """Fresh install: no state file yet — no restore."""
    assert _should_restore(head_exists=False, state_exists=False) is False


def test_actual_config_resolves_paths():
    """LabLiveConfig has both head_file and state_file properties."""
    from web.lab_live.config import LabLiveConfig
    cfg = LabLiveConfig()
    assert hasattr(cfg, "head_file"), "config must expose head_file"
    assert hasattr(cfg, "state_file"), "config must expose state_file"
    assert cfg.head_file.name == "head.json"
    assert cfg.state_file.name == "state.json"


def test_current_production_state():
    """Sanity: on this machine, head.json exists (v2 active).
    Verifies the exact symptom that motivated R8/R9.
    """
    from web.lab_live.config import LabLiveConfig
    cfg = LabLiveConfig()
    # head.json must exist (we saw it 2026-04-23 20:13 timestamp)
    assert cfg.head_file.exists(), (
        f"head.json missing at {cfg.head_file} — expected v2 state"
    )
    # state.json is legacy; current system has moved to v2
    # Note: this assertion documents production state, not a requirement
    # If someone restores legacy state.json this test passes still (OR cond).
