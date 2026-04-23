# -*- coding: utf-8 -*-
"""Tests for shared/data_events.py — Step-2 recovery.

Jeff gate:
    1. 호출부 4곳 import 성공
    2. emit_event — Level enum + str level 둘 다 수용
    3. Level 참조 정상
    4. get_events — limit/min_level/sources 필터 일관
    5. get_escalation_states — 최소 상태 반환 + clear 의미
    6. 기능 확장 금지 — 저장소는 in-memory only
"""
from __future__ import annotations

import sys
import threading
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_ROOT))

from shared import data_events
from shared.data_events import (
    Level,
    _normalize_level,
    _reset_for_tests,
    emit_event,
    get_escalation_states,
    get_events,
)


@pytest.fixture(autouse=True)
def _clean():
    _reset_for_tests()
    yield
    _reset_for_tests()


# ---------- Gate 1: import 호환 — 4 call sites ----------

def test_api_surface_matches_call_sites():
    """Exact names used by kr/us call sites exist + are callable."""
    from shared.data_events import Level  # noqa: F401
    from shared.data_events import emit_event  # noqa: F401
    from shared.data_events import get_events  # noqa: F401
    from shared.data_events import get_escalation_states  # noqa: F401
    # Enum has the 5 members referenced anywhere
    for name in ("DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"):
        assert hasattr(Level, name)


def test_alpaca_provider_style_call_works():
    """Mirrors us/data/alpaca_provider.py:190-206 shape."""
    event = emit_event(
        source="ALPACA.auth",
        level=Level.CRITICAL,
        code="consecutive_auth_error",
        message="Alpaca 401 연속 3회 — 300s halt",
        details={"status": 401, "method": "GET", "path": "/v2/account"},
        telegram=True,
    )
    assert event["level"] == "CRITICAL"
    assert event["source"] == "ALPACA.auth"
    assert event["telegram"] is True


def test_health_check_style_string_level():
    """kr/tools/health_check.py:119-132 passes `level="CRITICAL"` as string."""
    emit_event(
        source="STARTUP.kr", level="CRITICAL", code="import_failed.REQUIRED",
        message="pandas dep missing: ...", telegram=True,
    )
    emit_event(
        source="STARTUP.kr", level="WARN", code="import_failed.OPTIONAL",
        message="foo dep missing", telegram=False,
    )
    evs = get_events(limit=10)
    assert len(evs) == 2
    levels = {e["level"] for e in evs}
    assert "CRITICAL" in levels
    assert "WARN" in levels


# ---------- Gate 2: level normalization ----------

def test_normalize_level_enum_passthrough():
    assert _normalize_level(Level.CRITICAL) is Level.CRITICAL


def test_normalize_level_str_case_insensitive():
    assert _normalize_level("critical") is Level.CRITICAL
    assert _normalize_level("WARN") is Level.WARN
    assert _normalize_level("  Info  ") is Level.INFO


def test_normalize_level_aliases():
    assert _normalize_level("WARNING") is Level.WARN
    assert _normalize_level("FATAL") is Level.CRITICAL


def test_normalize_level_unknown_falls_back_to_info():
    assert _normalize_level("MYSTERY") is Level.INFO
    assert _normalize_level(None) is Level.INFO
    assert _normalize_level(object()) is Level.INFO


def test_normalize_level_bool_not_treated_as_int():
    """True/False must NOT be coerced to Level(1)/Level(0) — safety."""
    assert _normalize_level(True) is Level.INFO
    assert _normalize_level(False) is Level.INFO


# ---------- Gate 3: get_events filters ----------

def test_get_events_returns_newest_first():
    emit_event(source="A", level=Level.INFO, code="c1", message="first")
    emit_event(source="B", level=Level.INFO, code="c2", message="second")
    emit_event(source="C", level=Level.INFO, code="c3", message="third")
    evs = get_events(limit=10)
    assert [e["source"] for e in evs] == ["C", "B", "A"]


def test_get_events_limit_respected():
    for i in range(5):
        emit_event(source=f"S{i}", level=Level.INFO, code="c", message="m")
    evs = get_events(limit=3)
    assert len(evs) == 3


def test_get_events_min_level_filter():
    emit_event(source="A", level=Level.DEBUG, code="c", message="d")
    emit_event(source="B", level=Level.INFO, code="c", message="i")
    emit_event(source="C", level=Level.WARN, code="c", message="w")
    emit_event(source="D", level=Level.ERROR, code="c", message="e")

    warnup = get_events(limit=10, min_level="WARN")
    assert {e["source"] for e in warnup} == {"C", "D"}

    erronly = get_events(limit=10, min_level="ERROR")
    assert {e["source"] for e in erronly} == {"D"}


def test_get_events_source_substring_filter():
    emit_event(source="ALPACA.auth", level=Level.WARN, code="c", message="m")
    emit_event(source="STARTUP.kr", level=Level.WARN, code="c", message="m")
    emit_event(source="STARTUP.us", level=Level.WARN, code="c", message="m")

    alpaca = get_events(limit=10, sources=["ALPACA"])
    assert [e["source"] for e in alpaca] == ["ALPACA.auth"]

    startup = get_events(limit=10, sources=["STARTUP"])
    assert sorted(e["source"] for e in startup) == ["STARTUP.kr", "STARTUP.us"]

    any_of = get_events(limit=10, sources=["ALPACA", ".kr"])
    assert {e["source"] for e in any_of} == {"ALPACA.auth", "STARTUP.kr"}


def test_get_events_empty_filter_returns_all():
    emit_event(source="A", level=Level.INFO, code="c", message="m")
    evs = get_events(limit=10, min_level=None, sources=None)
    assert len(evs) == 1


# ---------- Gate 4: escalation tracking ----------

def test_escalation_opens_on_warn():
    emit_event(source="S", level=Level.WARN, code="x", message="bad")
    states = get_escalation_states()
    assert len(states) == 1
    assert states[0]["source"] == "S"
    assert states[0]["level"] == "WARN"


def test_escalation_opens_on_error_critical():
    emit_event(source="S1", level=Level.ERROR, code="x", message="m")
    emit_event(source="S2", level=Level.CRITICAL, code="y", message="m")
    states = get_escalation_states()
    keys = {(s["source"], s["code"]) for s in states}
    assert keys == {("S1", "x"), ("S2", "y")}


def test_escalation_does_not_open_for_info_debug():
    emit_event(source="S", level=Level.INFO, code="x", message="fine")
    emit_event(source="S", level=Level.DEBUG, code="y", message="trace")
    assert get_escalation_states() == []


def test_escalation_cleared_by_info_for_same_source_code():
    """alpaca_provider.py auth-recovery pattern."""
    emit_event(source="ALPACA.auth", level=Level.CRITICAL,
               code="consecutive_auth_error", message="halt")
    assert len(get_escalation_states()) == 1

    # Later: recovery event at INFO level
    emit_event(source="ALPACA.auth", level=Level.INFO,
               code="consecutive_auth_error", message="recovered")
    assert get_escalation_states() == []


def test_escalation_level_ratchets_up():
    """WARN → CRITICAL upgrades peak level; opened_at stays."""
    emit_event(source="S", level=Level.WARN, code="c", message="1")
    first_opened = get_escalation_states()[0]["opened_at"]
    emit_event(source="S", level=Level.CRITICAL, code="c", message="2")
    states = get_escalation_states()
    assert states[0]["level"] == "CRITICAL"
    assert states[0]["opened_at"] == first_opened  # preserved
    assert states[0]["last_message"] == "2"


def test_escalation_peak_not_lowered_by_later_warn():
    emit_event(source="S", level=Level.CRITICAL, code="c", message="peak")
    emit_event(source="S", level=Level.WARN, code="c", message="still bad")
    states = get_escalation_states()
    # Peak stays CRITICAL; message tracks latest
    assert states[0]["level"] == "CRITICAL"
    assert states[0]["last_message"] == "still bad"


def test_escalation_different_codes_independent():
    emit_event(source="S", level=Level.WARN, code="a", message="m")
    emit_event(source="S", level=Level.WARN, code="b", message="m")
    assert len(get_escalation_states()) == 2
    # INFO on 'a' clears only 'a'
    emit_event(source="S", level=Level.INFO, code="a", message="fixed")
    states = get_escalation_states()
    assert len(states) == 1
    assert states[0]["code"] == "b"


# ---------- Gate 5: buffer cap (in-memory only) ----------

def test_buffer_cap_enforces_max(monkeypatch):
    """Over-capacity writes must not explode memory — deque cap enforced."""
    # Shrink cap for test speed
    import shared.data_events as mod
    original = mod._events
    mod._events = type(original)(maxlen=5)
    try:
        for i in range(10):
            emit_event(source=f"S{i}", level=Level.INFO, code="c", message="m")
        evs = get_events(limit=100)
        assert len(evs) == 5
        # Newest kept, oldest dropped
        sources = [e["source"] for e in evs]
        assert sources == ["S9", "S8", "S7", "S6", "S5"]
    finally:
        mod._events = original


# ---------- Gate 6: thread safety (basic) ----------

def test_concurrent_emits_do_not_corrupt():
    N = 50
    def _worker(tag: str):
        for i in range(N):
            emit_event(source=f"T{tag}", level=Level.INFO, code="c", message=str(i))

    threads = [threading.Thread(target=_worker, args=(str(t),)) for t in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    evs = get_events(limit=10_000)
    assert len(evs) == 4 * N
    # All events have the expected shape
    for ev in evs:
        assert "ts" in ev and "source" in ev and "level" in ev


# ---------- Event shape ----------

def test_event_has_expected_keys():
    ev = emit_event(source="S", level=Level.INFO, code="c", message="m",
                    details={"k": "v"}, telegram=True)
    assert set(ev.keys()) == {
        "ts", "source", "level", "code", "message", "details", "telegram",
    }
    assert ev["details"] == {"k": "v"}


def test_details_none_becomes_empty_dict():
    ev = emit_event(source="S", level=Level.INFO, code="c", message="m",
                    details=None)
    assert ev["details"] == {}


# ---------- Allowlist path is the NEW one ----------

def test_allowlist_contains_shared_data_events_not_legacy():
    from kr.pipeline.completion_schema import PYC_ALLOWLIST_MODULES
    assert "shared.data_events" in PYC_ALLOWLIST_MODULES
    assert "kr.web.data_events" not in PYC_ALLOWLIST_MODULES
