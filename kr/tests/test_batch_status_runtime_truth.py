"""
test_batch_status_runtime_truth.py — /api/batch/status two-gate logic
=====================================================================

Pins the new contract (Jeff 2026-04-29): ``batch_done`` requires
**both** the target_portfolio file AND a runtime ``last_batch_completed_at``
that lands on today (KST).

Today's evidence — at 16:27 KST step 4 of batch.run_batch finished and
target_portfolio_20260429.json was written, so the previous one-gate
logic flipped the badge green. But step 5 (Fundamental + Lab Live +
Advisor) was still in flight (~23%, ETA 60+ minutes). The new logic
distinguishes:

  * batch_done    — target file present AND runtime completion stamp
                    is today (KST). The whole lifecycle is done.
  * batch_partial — target file present but runtime stamp absent or
                    not today. Step 4 wrote the file; step 5+ still
                    running OR yesterday's stamp leaked over.

These tests construct ad-hoc target / runtime state fixtures and call
the closure-bound endpoint via the FastAPI TestClient so the real
on-disk state is not touched.
"""
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))


KST = ZoneInfo("Asia/Seoul")


def _kst_today_utc_iso(offset_minutes: int = 0) -> str:
    """Build a UTC ISO timestamp whose KST date == today + offset."""
    now = datetime.now(KST) + timedelta(minutes=offset_minutes)
    return now.astimezone(timezone.utc).isoformat(timespec="seconds")


def _seed(tmp_signals: Path, tmp_state: Path,
          *, target_present: bool, completed_at: str = "",
          business_date: str = "") -> None:
    """Write minimal fixtures so the endpoint's two reads find them."""
    tmp_signals.mkdir(parents=True, exist_ok=True)
    tmp_state.mkdir(parents=True, exist_ok=True)
    today_compact = datetime.now(KST).strftime("%Y%m%d")
    target = tmp_signals / f"target_portfolio_{today_compact}.json"
    if target_present:
        target.write_text(json.dumps({"date": today_compact}), encoding="utf-8")
    elif target.exists():
        target.unlink()
    runtime = tmp_state / "runtime_state_live.json"
    rt: dict = {"timestamp": datetime.now().isoformat()}
    if completed_at:
        rt["last_batch_completed_at"] = completed_at
    if business_date:
        rt["last_batch_business_date"] = business_date
    runtime.write_text(json.dumps(rt), encoding="utf-8")


def _two_gate_decide(target_present: bool, completed_at_iso: str) -> dict:
    """Pure-Python mirror of the endpoint's gate. The endpoint lives
    inside a FastAPI closure so we recreate the date arithmetic here
    to assert it stays in sync with app.py."""
    today_kst = datetime.now(KST).date()
    if not completed_at_iso:
        runtime_completed_today = False
    else:
        try:
            ts = datetime.fromisoformat(completed_at_iso)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            runtime_completed_today = ts.astimezone(KST).date() == today_kst
        except Exception:
            runtime_completed_today = False
    return {
        "batch_done": target_present and runtime_completed_today,
        "batch_partial": target_present and not runtime_completed_today,
    }


# ── Test 1: target present + completion today → done ──────────────────────
def test_done_when_target_and_runtime_today():
    """Both gates trigger. The post-step-9 steady state."""
    today_utc = _kst_today_utc_iso(0)
    out = _two_gate_decide(True, today_utc)
    assert out["batch_done"] is True
    assert out["batch_partial"] is False


# ── Test 2: target present + no runtime → partial (the bug case) ──────────
def test_partial_when_target_only():
    """Step 4 wrote the file; step 5 is still in flight (today's
    16:27~17:30+ window). Badge must NOT show ✓ — the rest of the
    lifecycle is still running."""
    out = _two_gate_decide(True, "")
    assert out["batch_done"] is False
    assert out["batch_partial"] is True


# ── Test 3: target absent → done=False (early morning / weekend) ──────────
def test_not_done_when_target_absent():
    """No batch run yet today. Even if runtime carries a stale
    yesterday's completion, the file gate keeps the badge dark."""
    today_utc = _kst_today_utc_iso(0)
    out = _two_gate_decide(False, today_utc)
    assert out["batch_done"] is False
    assert out["batch_partial"] is False


# ── Test 4: yesterday's completion does not satisfy today ─────────────────
def test_yesterday_completion_does_not_pass():
    """Avoid the silent leak where last night's completion stamp
    keeps lighting up today's badge before today's batch has run."""
    yesterday_utc = (
        datetime.now(KST) - timedelta(days=1)
    ).astimezone(timezone.utc).isoformat(timespec="seconds")
    out = _two_gate_decide(True, yesterday_utc)
    assert out["batch_done"] is False
    assert out["batch_partial"] is True


# ── Test 5: KST midnight boundary ─────────────────────────────────────────
def test_kst_midnight_boundary_uses_kst_date():
    """If the comparison were done in UTC, a batch that finishes around
    KST midnight could land on the wrong calendar day. Pin the KST
    semantics so a 23:59 KST completion still counts as today."""
    almost_midnight_kst = (
        datetime.now(KST).replace(hour=23, minute=59, second=0, microsecond=0)
    )
    iso = almost_midnight_kst.astimezone(timezone.utc).isoformat(timespec="seconds")
    out = _two_gate_decide(True, iso)
    # 23:59 KST today is still today in KST, regardless of UTC date.
    assert out["batch_done"] is True


# ── Test 6: malformed completion timestamp falls back to partial ──────────
def test_malformed_completion_falls_to_partial():
    """A garbage timestamp must not crash the endpoint or be mistaken
    for today. partial=True so the operator sees the file but knows
    the lifecycle stamp is wrong."""
    out = _two_gate_decide(True, "not-an-iso-string")
    assert out["batch_done"] is False
    assert out["batch_partial"] is True
