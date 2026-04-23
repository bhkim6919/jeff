# -*- coding: utf-8 -*-
"""Tests for R6 (2026-04-24) — MetricsBlock + universe_count marker trend.

Background: the 2026-04-23 batch-empty incident silently degraded for
~10 days because `universe_count` was never persisted. `universe_healthy`
existed as a boolean gate but the numeric trend (901 → 500 → 0) was
invisible. R6 adds a MetricsBlock so dashboards can surface drift.

What we verify:
  1. MetricsBlock round-trips through JSON (to_dict/from_dict).
  2. RunEntry serializes `metrics` alongside `checks`.
  3. Backward-compat: markers written before R6 (no `metrics` key) load
     with an empty MetricsBlock instead of crashing.
  4. Marker transition/set_attrs accept metrics and persist.
  5. preflight._run_checks harvests `universe_count` from
     `check_universe_healthy` detail into outcome.metrics.
"""
from __future__ import annotations

import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "kr"))

from pipeline.completion_marker import (
    ChecksBlock,
    CompletionMarker,
    MetricsBlock,
    RunEntry,
)
from pipeline.completion_schema import RUN_KR_BATCH, STATUS_RUNNING, STATUS_SUCCESS


# ── MetricsBlock basics ─────────────────────────────────────────────

def test_metrics_block_default_is_none():
    m = MetricsBlock()
    assert m.universe_count is None
    assert m.to_dict() == {"universe_count": None}


def test_metrics_block_round_trip():
    m = MetricsBlock(universe_count=901)
    assert MetricsBlock.from_dict(m.to_dict()).universe_count == 901


def test_metrics_block_from_dict_coerces_int():
    """String "901" from JSON-on-disk must coerce, not crash."""
    assert MetricsBlock.from_dict({"universe_count": "901"}).universe_count == 901


def test_metrics_block_from_dict_drops_non_numeric():
    assert MetricsBlock.from_dict({"universe_count": "NaN"}).universe_count is None
    assert MetricsBlock.from_dict({"universe_count": None}).universe_count is None


def test_metrics_block_from_dict_none_argument():
    assert MetricsBlock.from_dict(None).universe_count is None


def test_metrics_block_from_dict_missing_key():
    assert MetricsBlock.from_dict({}).universe_count is None


# ── RunEntry integration ───────────────────────────────────────────

def test_run_entry_default_has_empty_metrics():
    entry = RunEntry()
    assert entry.metrics.universe_count is None
    assert entry.to_dict()["metrics"] == {"universe_count": None}


def test_run_entry_round_trip_preserves_metrics():
    entry = RunEntry(
        status=STATUS_RUNNING,
        metrics=MetricsBlock(universe_count=1234),
    )
    restored = RunEntry.from_dict(entry.to_dict())
    assert restored.metrics.universe_count == 1234


def test_run_entry_from_dict_backward_compat_no_metrics_key():
    """Pre-R6 markers lack a `metrics` key — must load cleanly."""
    legacy = {
        "status": STATUS_RUNNING,
        "attempt_no": 1,
        "checks": {"imports_ok": True},
        "artifacts": {},
    }
    entry = RunEntry.from_dict(legacy)
    assert entry.metrics.universe_count is None


# ── CompletionMarker transition + set_attrs ─────────────────────────

def _fixed_clock():
    t = datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc)
    def _clock(): return t
    return _clock


def test_marker_transition_accepts_metrics(tmp_path: Path):
    marker = CompletionMarker(
        trade_date=date(2026, 4, 24), tz="Asia/Seoul",
        data_dir=tmp_path, clock=_fixed_clock(),
    )
    marker.transition(RUN_KR_BATCH, STATUS_RUNNING)
    marker.transition(
        RUN_KR_BATCH, STATUS_SUCCESS,
        checks=ChecksBlock(
            imports_ok=True, db_upsert_ok=True, kospi_parse_ok=True,
            report_ok=True, write_perm_ok=True, universe_healthy=True,
        ),
        metrics=MetricsBlock(universe_count=901),
    )
    entry = marker.run(RUN_KR_BATCH)
    assert entry.metrics.universe_count == 901


def test_marker_set_attrs_accepts_metrics(tmp_path: Path):
    marker = CompletionMarker(
        trade_date=date(2026, 4, 24), tz="Asia/Seoul",
        data_dir=tmp_path, clock=_fixed_clock(),
    )
    marker.transition(RUN_KR_BATCH, STATUS_RUNNING)
    marker.set_attrs(RUN_KR_BATCH, metrics=MetricsBlock(universe_count=850))
    assert marker.run(RUN_KR_BATCH).metrics.universe_count == 850


def test_marker_persists_metrics_to_disk(tmp_path: Path):
    """Full save/load cycle: universe_count survives a round-trip to JSON."""
    marker = CompletionMarker(
        trade_date=date(2026, 4, 24), tz="Asia/Seoul",
        data_dir=tmp_path, clock=_fixed_clock(),
    )
    marker.transition(RUN_KR_BATCH, STATUS_RUNNING)
    marker.set_attrs(RUN_KR_BATCH, metrics=MetricsBlock(universe_count=777))
    marker.save()

    reloaded = CompletionMarker.load_or_create_today(
        data_dir=tmp_path, trade_date=date(2026, 4, 24),
    )
    assert reloaded.run(RUN_KR_BATCH).metrics.universe_count == 777


# ── preflight integration ───────────────────────────────────────────

def test_preflight_harvests_universe_count_into_metrics(monkeypatch):
    """Given check_universe_healthy returns detail.universe_count,
    PreflightOutcome.metrics must carry the same number so
    marker_integration can persist it."""
    from pipeline import preflight as pf

    class _StubCheckResult:
        def __init__(self, ok, detail):
            self.ok = ok
            self.detail = detail
            self.error = None

    # Stub out the real check registry: only universe_healthy, returns count=888.
    monkeypatch.setitem(pf.CHECKS_FOR_RUN, RUN_KR_BATCH, [
        ("universe_healthy", lambda rt, st: _StubCheckResult(
            ok=True, detail={"universe_count": 888},
        )),
    ])
    # Avoid computing a real fingerprint (it touches git/fs).
    monkeypatch.setattr(pf, "compute_fingerprint",
                        lambda rt: pf.FingerprintBlock())

    outcome = pf._run_checks(RUN_KR_BATCH, state=object())
    assert outcome.ok is True
    assert outcome.metrics.universe_count == 888


def test_preflight_metrics_none_when_universe_count_absent(monkeypatch):
    """If the check omits universe_count (e.g. on crash detail),
    metrics.universe_count must stay None instead of coercing junk."""
    from pipeline import preflight as pf

    class _StubCheckResult:
        def __init__(self):
            self.ok = True
            self.detail = {}  # no universe_count
            self.error = None

    monkeypatch.setitem(pf.CHECKS_FOR_RUN, RUN_KR_BATCH, [
        ("universe_healthy", lambda rt, st: _StubCheckResult()),
    ])
    monkeypatch.setattr(pf, "compute_fingerprint",
                        lambda rt: pf.FingerprintBlock())

    outcome = pf._run_checks(RUN_KR_BATCH, state=object())
    assert outcome.metrics.universe_count is None
