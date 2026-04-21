# -*- coding: utf-8 -*-
"""Tests for pipeline.pg_mirror — import safety & non-blocking contract.

pg_mirror must never raise — PG unreachable / module missing /
serialization failure → log + return False. This guarantees a PG
outage can never block the orchestrator's primary JSON state path.

These tests intentionally run with `shared` NOT on sys.path (only `kr/`
is added via the standard test header), which exercises the real
"shared package unreachable" branch in the lazy import.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pytest

from pipeline.state import PipelineState


def _make_state(tmp_path: Path) -> PipelineState:
    state = PipelineState.load_or_create_today(
        data_dir=tmp_path,
        mode="paper_forward",
        trade_date=date(2026, 4, 21),
    )
    state.mark_done("batch", details={"target_count": 20})
    return state


def test_pg_mirror_module_importable():
    """pg_mirror itself must import with no side effects."""
    from pipeline import pg_mirror  # noqa: F401

    assert hasattr(pg_mirror, "mirror_step")
    assert hasattr(pg_mirror, "load_recent_history")


def test_mirror_step_returns_bool_never_raises(tmp_path):
    """Whether PG is reachable or not, mirror_step MUST return bool
    and MUST NOT raise. This is the non-blocking contract."""
    from pipeline import pg_mirror

    state = _make_state(tmp_path)
    result = pg_mirror.mirror_step(state, "batch")

    assert isinstance(result, bool)


def test_load_recent_history_returns_list_never_raises():
    """Read path must return a list, never raise."""
    from pipeline import pg_mirror

    rows = pg_mirror.load_recent_history(limit=10)
    assert isinstance(rows, list)


def test_mirror_step_handles_unknown_step_gracefully(tmp_path):
    """Even for a step that hasn't been touched, return bool not raise."""
    from pipeline import pg_mirror

    state = PipelineState.load_or_create_today(
        data_dir=tmp_path,
        mode="paper_forward",
        trade_date=date(2026, 4, 21),
    )
    # state.step() auto-creates a NOT_STARTED entry — mirror should still
    # cope without raising on the empty step state.
    result = pg_mirror.mirror_step(state, "never_touched")
    assert isinstance(result, bool)
