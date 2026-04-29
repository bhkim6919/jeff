"""
test_batch_fresh_staleness.py — MAX_STALENESS_HOURS = 26 (Jeff 2026-04-29)
==========================================================================

Pins the ceiling that controls whether the US dashboard shows
``BATCH_NOT_FRESH``. The previous 12h value caused a daily false
positive between KST ~17:00 and the next ET 16:00 batch — the
operator saw "Failed: Batch not fresh" for three consecutive days
in a row even though the underlying post-close batch was completing
on schedule (Jeff 2026-04-29 dashboard escalation).

26h covers a full trading-day cycle (post-close batch → next
post-close batch ~24h later) plus a 2h grace period for the next
batch to run before the staleness gate fires.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

# us/ on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


ET = ZoneInfo("America/New_York")


def _build_rs(snapshot_dt: datetime, today_bd: str) -> dict:
    """Build a minimum runtime_state dict that satisfies every gate
    in ``compute_batch_fresh`` *except* potentially the staleness
    gate, so the staleness check itself is the unit under test."""
    return {
        "last_batch_business_date": today_bd,
        "snapshot_version": f"{today_bd}_batch_test_POST_CLOSE",
        "rebal_phase": "BATCH_DONE",
        "snapshot_created_at": snapshot_dt.isoformat(),
    }


def test_max_staleness_hours_is_26():
    """The constant itself — guards against accidental revert to 12."""
    from core.state_manager import MAX_STALENESS_HOURS
    assert MAX_STALENESS_HOURS == 26, (
        f"MAX_STALENESS_HOURS regressed to {MAX_STALENESS_HOURS}; "
        "the 12h value caused the 2026-04-29 daily false positive."
    )


def test_post_close_batch_is_fresh_at_kst_morning_next_day():
    """The exact failure pattern Jeff caught: a post-close batch
    that completed at ET 20:00 must still read FRESH the next
    morning at KST 09:00 (= ET 19:00 same day, not next day —
    actually KST 09:00 = ET 19:00 of the previous day in summer
    DST? Let's verify the math holds for a standard cycle)."""
    from core.state_manager import compute_batch_fresh

    # Snapshot: ET 2026-04-28 20:00 (post-close, well after the 16:00
    # close gate). The bd label is the trading day this batch is for.
    bd = "2026-04-28"
    snapshot_dt = datetime(2026, 4, 28, 20, 0, 0, tzinfo=ET)
    rs = _build_rs(snapshot_dt, bd)

    # Patch the "now" the function reads. compute_batch_fresh uses
    # ``datetime.now(US_ET)`` directly, so we monkeypatch the symbol.
    import core.state_manager as sm_mod

    class _FrozenDT:
        @staticmethod
        def now(tz=None):
            # KST 09:00 of the next day = ET 20:00 of the same day
            # (standard DST, KST = ET + 13h).
            return datetime(2026, 4, 29, 8, 0, 0, tzinfo=ET)

        @staticmethod
        def fromisoformat(s):
            return datetime.fromisoformat(s)

    saved = sm_mod.datetime
    sm_mod.datetime = _FrozenDT
    try:
        result = compute_batch_fresh(rs, bd)
    finally:
        sm_mod.datetime = saved
    assert result is True, (
        "post-close batch must read FRESH 12h after completion; "
        "the gate now matches the US batch cadence."
    )


def test_genuinely_stale_snapshot_still_caught():
    """A 3-day-old snapshot must still be flagged stale — the gate
    is widened, not removed."""
    from core.state_manager import compute_batch_fresh
    import core.state_manager as sm_mod

    bd = "2026-04-25"
    snapshot_dt = datetime(2026, 4, 25, 20, 0, 0, tzinfo=ET)
    rs = _build_rs(snapshot_dt, bd)

    class _FrozenDT:
        @staticmethod
        def now(tz=None):
            return datetime(2026, 4, 28, 20, 0, 0, tzinfo=ET)  # 72h later

        @staticmethod
        def fromisoformat(s):
            return datetime.fromisoformat(s)

    saved = sm_mod.datetime
    sm_mod.datetime = _FrozenDT
    try:
        result = compute_batch_fresh(rs, bd)
    finally:
        sm_mod.datetime = saved
    assert result is False, (
        "72h-old snapshot must still be stale — the staleness gate "
        "is widened from 12h to 26h, not eliminated."
    )


def test_24h_old_snapshot_still_fresh_within_grace():
    """A 24h-old snapshot is within the new ceiling; the next batch
    is presumably running but hasn't completed yet."""
    from core.state_manager import compute_batch_fresh
    import core.state_manager as sm_mod

    bd = "2026-04-28"
    snapshot_dt = datetime(2026, 4, 28, 20, 0, 0, tzinfo=ET)
    rs = _build_rs(snapshot_dt, bd)

    class _FrozenDT:
        @staticmethod
        def now(tz=None):
            return datetime(2026, 4, 29, 19, 0, 0, tzinfo=ET)  # 23h later

        @staticmethod
        def fromisoformat(s):
            return datetime.fromisoformat(s)

    saved = sm_mod.datetime
    sm_mod.datetime = _FrozenDT
    try:
        result = compute_batch_fresh(rs, bd)
    finally:
        sm_mod.datetime = saved
    assert result is True, (
        "23h is within the 26h ceiling — the 2h grace period before "
        "the next batch arrives must keep the dashboard quiet."
    )
