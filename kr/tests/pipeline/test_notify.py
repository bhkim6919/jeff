# -*- coding: utf-8 -*-
"""Unit tests for pipeline.notify.PipelineNotifier."""
from __future__ import annotations

import sys
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pytest

from pipeline.notify import PipelineNotifier, QUIET_STEPS
from pipeline.schema import STATUS_DONE, STATUS_FAILED, STATUS_PENDING
from pipeline.state import PipelineState, StepState


class _FakeSend:
    """Callable collector for (text, severity) calls."""

    def __init__(self, return_ok: bool = True):
        self.calls: list[tuple[str, str]] = []
        self.return_ok = return_ok

    def __call__(self, text: str, severity: str) -> bool:
        self.calls.append((text, severity))
        return self.return_ok


def _make_state(tmp_path, trade_date=date(2026, 4, 22)) -> PipelineState:
    return PipelineState.load_or_create_today(
        data_dir=tmp_path,
        mode="paper_forward",
        trade_date=trade_date,
    )


def _set_done(state: PipelineState, step_name: str, details=None) -> None:
    st = state.step(step_name)
    st.status = STATUS_DONE
    st.finished_at = datetime(2026, 4, 22, 16, 6)
    st.details = dict(details or {})


def _set_failed(
    state: PipelineState,
    step_name: str,
    fail_count: int,
    last_error: str = "boom",
) -> None:
    st = state.step(step_name)
    st.status = STATUS_FAILED
    st.fail_count = fail_count
    st.last_error = last_error
    st.last_failed_at = datetime(2026, 4, 22, 16, 6)


# ---------- DONE transitions ----------

def test_done_transition_emits_info(tmp_path):
    state = _make_state(tmp_path)
    _set_done(state, "batch", {"target_count": 500, "duration_sec": 12})
    send = _FakeSend()
    n = PipelineNotifier(send_fn=send)

    sent = n.notify_transitions(state)

    assert sent == ["batch"]
    assert len(send.calls) == 1
    text, severity = send.calls[0]
    assert severity == "INFO"
    assert "[PIPE] batch" in text
    assert "DONE" in text
    assert "trade_date: 2026-04-22" in text
    assert "targets: 500" in text
    assert "dur: 12s" in text


def test_done_dedup_same_day(tmp_path):
    state = _make_state(tmp_path)
    _set_done(state, "lab_eod_kr", {"trades": 14, "strategies": 9})
    send = _FakeSend()
    n = PipelineNotifier(send_fn=send)

    n.notify_transitions(state)
    n.notify_transitions(state)
    n.notify_transitions(state)

    assert len(send.calls) == 1  # deduped


def test_done_lab_eod_us_format(tmp_path):
    state = _make_state(tmp_path)
    _set_done(state, "lab_eod_us", {"strategy_count": 10, "duration_sec": 35})
    send = _FakeSend()
    n = PipelineNotifier(send_fn=send)

    n.notify_transitions(state)

    text, _ = send.calls[0]
    assert "strategies: 10" in text
    assert "dur: 35s" in text


def test_done_backup_format(tmp_path):
    state = _make_state(tmp_path)
    _set_done(
        state,
        "backup",
        {"summary_head": "uploaded 4 files (state_kr,state_us,logs,report)",
         "duration_sec": 8},
    )
    send = _FakeSend()
    n = PipelineNotifier(send_fn=send)

    n.notify_transitions(state)

    text, _ = send.calls[0]
    assert "uploaded 4 files" in text


# ---------- FAILED / ABANDONED transitions ----------

def test_failed_under_max_suppressed(tmp_path):
    state = _make_state(tmp_path)
    _set_failed(state, "batch", fail_count=1)  # max=3
    send = _FakeSend()
    n = PipelineNotifier(send_fn=send)

    sent = n.notify_transitions(state)

    assert sent == []
    assert send.calls == []


def test_abandoned_emits_critical(tmp_path):
    state = _make_state(tmp_path)
    _set_failed(state, "batch", fail_count=3, last_error="ConnectionError: timeout")
    send = _FakeSend()
    n = PipelineNotifier(send_fn=send)

    sent = n.notify_transitions(state)

    assert sent == ["batch"]
    assert len(send.calls) == 1
    text, severity = send.calls[0]
    assert severity == "CRITICAL"
    assert "ABANDONED" in text
    assert "3/3 attempts exhausted" in text
    assert "ConnectionError" in text
    assert "수동 복구 필요" in text


def test_abandoned_backup_uses_max_2(tmp_path):
    state = _make_state(tmp_path)
    _set_failed(state, "backup", fail_count=2, last_error="gcloud fail")
    send = _FakeSend()
    n = PipelineNotifier(send_fn=send)

    sent = n.notify_transitions(state)

    assert sent == ["backup"]
    text, _ = send.calls[0]
    assert "2/2 attempts exhausted" in text


def test_abandoned_long_error_truncated(tmp_path):
    state = _make_state(tmp_path)
    _set_failed(state, "batch", fail_count=3, last_error="x" * 500)
    send = _FakeSend()
    n = PipelineNotifier(send_fn=send)

    n.notify_transitions(state)

    text, _ = send.calls[0]
    # Truncation adds a leading ellipsis and caps payload length
    assert "…" in text
    assert len(text) < 600


def test_abandoned_dedup_same_day(tmp_path):
    state = _make_state(tmp_path)
    _set_failed(state, "batch", fail_count=3)
    send = _FakeSend()
    n = PipelineNotifier(send_fn=send)

    n.notify_transitions(state)
    n.notify_transitions(state)

    assert len(send.calls) == 1


# ---------- Quiet steps ----------

def test_quiet_steps_suppressed(tmp_path):
    state = _make_state(tmp_path)
    for qs in QUIET_STEPS:
        _set_done(state, qs, {"ran": True})
    send = _FakeSend()
    n = PipelineNotifier(send_fn=send)

    sent = n.notify_transitions(state)

    assert sent == []
    assert send.calls == []


# ---------- Day rollover ----------

def test_day_rollover_clears_dedup(tmp_path):
    day1 = _make_state(tmp_path, trade_date=date(2026, 4, 22))
    _set_done(day1, "batch", {"target_count": 500})
    send = _FakeSend()
    n = PipelineNotifier(send_fn=send)

    n.notify_transitions(day1)
    assert len(send.calls) == 1

    # New trade_date, same step DONE again — should emit
    day2 = _make_state(tmp_path, trade_date=date(2026, 4, 23))
    _set_done(day2, "batch", {"target_count": 480})

    n.notify_transitions(day2)
    assert len(send.calls) == 2
    assert "2026-04-23" in send.calls[1][0]


# ---------- Mixed step states ----------

def test_mixed_state_only_terminals_emit(tmp_path):
    state = _make_state(tmp_path)
    _set_done(state, "batch", {"target_count": 500})
    state.step("lab_eod_kr").status = STATUS_PENDING  # in-flight
    _set_failed(state, "lab_eod_us", fail_count=1)    # retryable
    send = _FakeSend()
    n = PipelineNotifier(send_fn=send)

    sent = n.notify_transitions(state)

    assert sent == ["batch"]
    assert len(send.calls) == 1


# ---------- Error isolation ----------

def test_send_failure_does_not_raise(tmp_path):
    state = _make_state(tmp_path)
    _set_done(state, "batch", {"target_count": 500})

    def _broken(text, severity):
        raise RuntimeError("telegram down")

    n = PipelineNotifier(send_fn=_broken)
    # Must not raise
    sent = n.notify_transitions(state)
    assert sent == []


def test_send_returns_false_not_deduped_as_sent(tmp_path):
    """If telegram returns False, we still dedupe (already added to _seen
    before the send call) — this prevents spam on repeated delivery failures."""
    state = _make_state(tmp_path)
    _set_done(state, "batch", {"target_count": 500})
    send = _FakeSend(return_ok=False)
    n = PipelineNotifier(send_fn=send)

    sent1 = n.notify_transitions(state)
    sent2 = n.notify_transitions(state)

    assert sent1 == []  # send returned False → not in "sent" list
    assert sent2 == []  # second call deduped
    assert len(send.calls) == 1  # but only attempted once
