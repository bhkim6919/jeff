# -*- coding: utf-8 -*-
"""Tests for scripts/watchdog_external.py — dead-man switch.

Jeff A1.5 gate coverage:
 1. tray kill (no heartbeat update) → STALLED_HEARTBEAT
 2. heartbeat primary corrupt → bak fallback
 3. marker absent + past deadline → MISSING_RUN
 4. RUNNING status stale → STALLED_RUNNING
 5. dedup: same alert 2x in short window → only 1 send
 6. Telegram DEADMAN uses separate env vars (no collision with MAIN)
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest

# Add scripts/ to path so we can import watchdog_external as a module.
_SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_SCRIPT_DIR))

import watchdog_external as wd  # noqa: E402


KST = timezone(timedelta(hours=9))


def _write(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _heartbeat_dict(ts: datetime, tick_seq: int = 1) -> dict:
    return {
        "ts": ts.astimezone(timezone.utc).isoformat(timespec="seconds"),
        "pid": 12345,
        "tray_session": "testsession",
        "tick_seq": tick_seq,
    }


def _marker_dict(trade_date: date, runs: dict) -> dict:
    return {
        "schema_version": 1,
        "trade_date": trade_date.strftime("%Y-%m-%d"),
        "tz": "Asia/Seoul",
        "last_update": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "runs": runs,
    }


# ---------- Gate 1: tray kill → STALLED_HEARTBEAT ----------

def test_no_heartbeat_file_raises_missing_alert(tmp_path: Path):
    """Jeff gate: tray never started / deleted heartbeat → HEARTBEAT_MISSING."""
    now = datetime(2026, 4, 22, 10, 0, 0, tzinfo=timezone.utc)
    alerts = wd.evaluate_alerts(
        now_utc=now, heartbeat=None, marker_today=None,
        trade_date=now.astimezone(KST).date(),
    )
    codes = [a.code for a in alerts]
    assert wd.ALERT_HEARTBEAT_MISSING in codes


def test_stale_heartbeat_raises_stalled_alert(tmp_path: Path):
    """Jeff gate: heartbeat age > 120s → STALLED_HEARTBEAT."""
    now = datetime(2026, 4, 22, 10, 0, 0, tzinfo=timezone.utc)
    hb_old = _heartbeat_dict(now - timedelta(seconds=300))  # 5 min stale

    alerts = wd.evaluate_alerts(
        now_utc=now, heartbeat=hb_old, marker_today=None,
        trade_date=now.astimezone(KST).date(),
    )
    codes = [a.code for a in alerts]
    assert wd.ALERT_STALLED_HEARTBEAT in codes


def test_fresh_heartbeat_no_alert(tmp_path: Path):
    now = datetime(2026, 4, 22, 10, 0, 0, tzinfo=timezone.utc)
    hb_fresh = _heartbeat_dict(now - timedelta(seconds=30))

    alerts = wd.evaluate_alerts(
        now_utc=now, heartbeat=hb_fresh, marker_today=None,
        trade_date=now.astimezone(KST).date(),
    )
    codes = [a.code for a in alerts]
    assert wd.ALERT_STALLED_HEARTBEAT not in codes
    assert wd.ALERT_HEARTBEAT_MISSING not in codes


# ---------- Gate 2: heartbeat primary corrupt → bak fallback ----------

def test_load_heartbeat_uses_bak_when_primary_corrupt(tmp_path: Path):
    """Primary JSON-corrupted → bak returns last good record."""
    good_ts = datetime(2026, 4, 22, 10, 0, 0, tzinfo=timezone.utc)
    _write(tmp_path / wd.HEARTBEAT_BAK_FILENAME, _heartbeat_dict(good_ts))
    (tmp_path / wd.HEARTBEAT_FILENAME).write_text("{{ broken", encoding="utf-8")

    hb = wd.load_heartbeat(tmp_path)
    assert hb is not None
    assert hb["tray_session"] == "testsession"


def test_load_heartbeat_returns_none_when_both_bad(tmp_path: Path):
    (tmp_path / wd.HEARTBEAT_FILENAME).write_text("garbage", encoding="utf-8")
    (tmp_path / wd.HEARTBEAT_BAK_FILENAME).write_text("also bad", encoding="utf-8")
    assert wd.load_heartbeat(tmp_path) is None


# ---------- Gate 3: marker absent + past deadline → MISSING_RUN ----------

def test_missing_marker_past_kr_batch_deadline_alerts(tmp_path: Path):
    """19:00 KST (past R18 18:00 batch deadline), marker absent → MISSING_RUN."""
    # R18 (2026-04-23) 에서 KR_BATCH window 를 (16:05, 18:00) 로 확대.
    # 19:00 KST = 10:00 UTC — deadline 이후 확실.
    now = datetime(2026, 4, 22, 10, 0, 0, tzinfo=timezone.utc)
    hb = _heartbeat_dict(now)  # tray alive

    alerts = wd.evaluate_alerts(
        now_utc=now, heartbeat=hb, marker_today=None,
        trade_date=now.astimezone(KST).date(),
    )
    kr_batch_alerts = [a for a in alerts
                       if a.code == wd.ALERT_MISSING_RUN and a.run_type == wd.RUN_KR_BATCH]
    assert len(kr_batch_alerts) == 1


def test_missing_marker_before_deadline_no_alert(tmp_path: Path):
    """17:30 KST (before R18 18:00 batch deadline), marker absent → no alert yet."""
    # R18 (2026-04-23): KR_BATCH window = (16:05, 18:00). 17:30 KST = 08:30 UTC
    # is still inside the window, so MISSING_RUN must not fire.
    now = datetime(2026, 4, 22, 8, 30, 0, tzinfo=timezone.utc)
    hb = _heartbeat_dict(now)

    alerts = wd.evaluate_alerts(
        now_utc=now, heartbeat=hb, marker_today=None,
        trade_date=now.astimezone(KST).date(),
    )
    kr_batch_alerts = [a for a in alerts
                       if a.code == wd.ALERT_MISSING_RUN and a.run_type == wd.RUN_KR_BATCH]
    assert kr_batch_alerts == []


# ---------- Gate 4: RUNNING status stale → STALLED_RUNNING ----------

def test_running_stale_with_dead_heartbeat_critical(tmp_path: Path):
    """RUNNING + last_update 45min old + heartbeat dead → CRITICAL."""
    now = datetime(2026, 4, 22, 7, 30, 0, tzinfo=timezone.utc)
    run_started = now - timedelta(minutes=45)
    marker = _marker_dict(now.astimezone(KST).date(), runs={
        wd.RUN_KR_EOD: {
            "status": wd.STATUS_RUNNING,
            "attempt_no": 1,
            "started_at": run_started.isoformat(timespec="seconds"),
            "last_update": run_started.isoformat(timespec="seconds"),
            "checks": {},
            "artifacts": {},
            "error": None,
            "history": [],
        }
    })
    # No heartbeat → dead
    alerts = wd.evaluate_alerts(
        now_utc=now, heartbeat=None, marker_today=marker,
        trade_date=now.astimezone(KST).date(),
    )
    stalled = [a for a in alerts if a.code == wd.ALERT_STALLED_RUNNING]
    assert len(stalled) == 1
    assert stalled[0].severity == "CRITICAL"


def test_running_stale_with_alive_heartbeat_warn_only(tmp_path: Path):
    """Jeff v4 권장 4: RUNNING stale + heartbeat alive → WARN not CRITICAL.

    This is the race-relaxation: marker lag != process dead.
    """
    now = datetime(2026, 4, 22, 7, 30, 0, tzinfo=timezone.utc)
    run_started = now - timedelta(minutes=45)
    marker = _marker_dict(now.astimezone(KST).date(), runs={
        wd.RUN_KR_EOD: {
            "status": wd.STATUS_RUNNING,
            "attempt_no": 1,
            "last_update": run_started.isoformat(timespec="seconds"),
            "checks": {},
            "artifacts": {},
            "error": None,
            "history": [],
        }
    })
    hb_fresh = _heartbeat_dict(now - timedelta(seconds=20))

    alerts = wd.evaluate_alerts(
        now_utc=now, heartbeat=hb_fresh, marker_today=marker,
        trade_date=now.astimezone(KST).date(),
    )
    stalled = [a for a in alerts if a.code == wd.ALERT_STALLED_RUNNING]
    assert len(stalled) == 1
    assert stalled[0].severity == "WARN"


# ---------- Gate 5: dedup ----------

def test_dedup_filters_repeat_alerts_within_window(tmp_path: Path):
    """Same alert fired 2x in <6h → only first sends."""
    now = datetime(2026, 4, 22, 10, 0, 0, tzinfo=timezone.utc)
    a = wd.Alert(code=wd.ALERT_HEARTBEAT_MISSING, run_type=None,
                 trade_date="2026-04-22", detail="x")

    dedup: dict[str, float] = {}
    fire1, dedup = wd.filter_by_dedup([a], dedup, now.timestamp())
    assert len(fire1) == 1

    # Second pass 1 min later — same alert
    now2 = now + timedelta(minutes=1)
    fire2, dedup = wd.filter_by_dedup([a], dedup, now2.timestamp())
    assert len(fire2) == 0, "dedup must suppress immediate refire"

    # 7 hours later — refire allowed
    now3 = now + timedelta(hours=7)
    fire3, dedup = wd.filter_by_dedup([a], dedup, now3.timestamp())
    assert len(fire3) == 1


def test_dedup_different_alert_codes_both_fire(tmp_path: Path):
    now = datetime(2026, 4, 22, 10, 0, 0, tzinfo=timezone.utc)
    a1 = wd.Alert(code=wd.ALERT_HEARTBEAT_MISSING, run_type=None,
                  trade_date="2026-04-22", detail="x")
    a2 = wd.Alert(code=wd.ALERT_MISSING_RUN, run_type=wd.RUN_KR_BATCH,
                  trade_date="2026-04-22", detail="y")

    fire, _ = wd.filter_by_dedup([a1, a2], {}, now.timestamp())
    assert len(fire) == 2


def test_dedup_gc_drops_old_entries(tmp_path: Path):
    now = datetime(2026, 4, 22, 10, 0, 0, tzinfo=timezone.utc)
    old = (now - timedelta(days=10)).timestamp()
    dedup = {"stale-key": old}
    _, updated = wd.filter_by_dedup([], dedup, now.timestamp())
    assert "stale-key" not in updated


def test_dedup_roundtrip_persisted(tmp_path: Path):
    data = {"a|b|c": 1234567.0}
    wd.save_dedup(tmp_path, data)
    loaded = wd.load_dedup(tmp_path)
    assert loaded == data


# ---------- Gate 6: Telegram DEADMAN separation ----------

def test_telegram_deadman_uses_separate_env_vars(tmp_path: Path, monkeypatch):
    """DEADMAN bot token/chat are distinct from MAIN.

    Verify: send_telegram reads QTRON_TELEGRAM_TOKEN_DEADMAN and
    QTRON_TELEGRAM_CHAT_ID_DEADMAN — NOT the MAIN equivalents.
    """
    # Set MAIN vars — must be ignored
    monkeypatch.setenv("QTRON_TELEGRAM_TOKEN_MAIN", "main-token-should-be-ignored")
    monkeypatch.setenv("QTRON_TELEGRAM_CHAT_ID_MAIN", "main-chat-should-be-ignored")
    # DEADMAN not set
    monkeypatch.delenv("QTRON_TELEGRAM_TOKEN_DEADMAN", raising=False)
    monkeypatch.delenv("QTRON_TELEGRAM_CHAT_ID_DEADMAN", raising=False)

    assert wd.send_telegram("test") is False  # gracefully returns False


def test_telegram_deadman_posts_to_correct_url(monkeypatch):
    monkeypatch.setenv("QTRON_TELEGRAM_TOKEN_DEADMAN", "deadtoken")
    monkeypatch.setenv("QTRON_TELEGRAM_CHAT_ID_DEADMAN", "99999")

    captured = {}

    class _FakeResp:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *a): pass

    def _fake_urlopen(req, timeout=None):
        captured["url"] = req.full_url
        captured["body"] = req.data
        return _FakeResp()

    monkeypatch.setattr(wd.urllib.request, "urlopen", _fake_urlopen)
    assert wd.send_telegram("hello") is True
    assert "deadtoken" in captured["url"]
    body_decoded = captured["body"].decode("utf-8")
    assert "chat_id=99999" in body_decoded


# ---------- Integration: run_once end-to-end ----------

def test_run_once_creates_incident_when_alerts_exist(tmp_path: Path, monkeypatch):
    """Past deadline + no marker + no heartbeat → incident markdown written."""
    # 16:30 KST = 07:30 UTC (past KR_BATCH 16:10 deadline)
    now = datetime(2026, 4, 22, 7, 30, 0, tzinfo=timezone.utc)
    monkeypatch.setenv("QTRON_TELEGRAM_TOKEN_DEADMAN", "")  # no-op send
    monkeypatch.setenv("QTRON_TELEGRAM_CHAT_ID_DEADMAN", "")

    data_dir = tmp_path / "pipeline"
    incident_dir = tmp_path / "incidents"

    summary = wd.run_once(
        data_dir=data_dir, incident_dir=incident_dir,
        now_utc=now, dry_run=False,
    )
    assert summary["alerts_total"] > 0
    assert summary["incident_path"] is not None
    assert Path(summary["incident_path"]).exists()
    content = Path(summary["incident_path"]).read_text(encoding="utf-8")
    assert "HEARTBEAT_MISSING" in content


def test_run_once_dry_run_does_not_write_state(tmp_path: Path):
    now = datetime(2026, 4, 22, 7, 30, 0, tzinfo=timezone.utc)
    data_dir = tmp_path / "pipeline"
    incident_dir = tmp_path / "incidents"

    summary = wd.run_once(
        data_dir=data_dir, incident_dir=incident_dir,
        now_utc=now, dry_run=True,
    )
    assert summary["alerts_total"] > 0
    assert summary["incident_path"] is None
    assert not (data_dir / wd.DEDUP_FILENAME).exists()
    assert not incident_dir.exists() or not any(incident_dir.iterdir())


def test_run_once_second_pass_dedups(tmp_path: Path, monkeypatch):
    """Two passes back-to-back: first fires, second suppressed.

    Use a time delta small enough that no NEW deadlines are crossed
    between passes — otherwise new alerts are legitimate, not dedup bypass.
    """
    # 08:00 KST = 23:00 UTC previous day: past all KR deadlines but before US_BATCH/EOD
    now = datetime(2026, 4, 22, 23, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setenv("QTRON_TELEGRAM_TOKEN_DEADMAN", "")
    monkeypatch.setenv("QTRON_TELEGRAM_CHAT_ID_DEADMAN", "")

    data_dir = tmp_path / "pipeline"
    incident_dir = tmp_path / "incidents"

    s1 = wd.run_once(data_dir=data_dir, incident_dir=incident_dir,
                    now_utc=now, dry_run=False)
    # Second pass 5 min later — same deadlines still crossed, no new ones
    s2 = wd.run_once(data_dir=data_dir, incident_dir=incident_dir,
                    now_utc=now + timedelta(minutes=5), dry_run=False)

    assert len(s1["alerts_to_fire"]) > 0
    assert len(s2["alerts_to_fire"]) == 0, \
        f"second pass must dedup; fired: {s2['alerts_to_fire']}"


# ---------- Kill-then-observe: end-to-end tray-death simulation ----------

def test_tray_kill_scenario_full_stack(tmp_path: Path, monkeypatch):
    """Jeff gate A1.5 headline: tray alive → tray killed → watchdog detects.

    Sequence:
      1. Tray writes heartbeat (alive signal)
      2. Tray dies (heartbeat stops updating)
      3. 5 minutes later, external watchdog runs → STALLED_HEARTBEAT fires
    """
    data_dir = tmp_path / "pipeline"
    incident_dir = tmp_path / "incidents"
    data_dir.mkdir()

    # Step 1: tray alive — writes heartbeat
    alive_ts = datetime(2026, 4, 22, 9, 55, 0, tzinfo=timezone.utc)
    _write(data_dir / wd.HEARTBEAT_FILENAME, _heartbeat_dict(alive_ts, tick_seq=42))
    _write(data_dir / wd.HEARTBEAT_BAK_FILENAME, _heartbeat_dict(alive_ts, tick_seq=42))

    # Step 2: tray dies — 5 min pass, no new writes
    later = alive_ts + timedelta(minutes=5)
    monkeypatch.setenv("QTRON_TELEGRAM_TOKEN_DEADMAN", "")
    monkeypatch.setenv("QTRON_TELEGRAM_CHAT_ID_DEADMAN", "")

    summary = wd.run_once(
        data_dir=data_dir, incident_dir=incident_dir,
        now_utc=later, dry_run=False,
    )

    # Step 3: watchdog reports STALLED_HEARTBEAT
    assert any("STALLED_HEARTBEAT" in a for a in summary["alerts_to_fire"])
    assert summary["incident_path"] is not None
    content = Path(summary["incident_path"]).read_text(encoding="utf-8")
    assert "STALLED_HEARTBEAT" in content


# ---------- Window boundary tests ----------

def test_us_eod_window_early_morning(tmp_path: Path):
    """US_EOD window is 05:30-07:00 KST. 04:30 KST = before window → no alert."""
    # 04:30 KST = 19:30 UTC previous day — but we compute from KST date
    now = datetime(2026, 4, 21, 19, 30, 0, tzinfo=timezone.utc)
    td = now.astimezone(KST).date()  # 2026-04-22 after 9h UTC offset
    alerts = wd.evaluate_alerts(
        now_utc=now, heartbeat=_heartbeat_dict(now), marker_today=None,
        trade_date=td,
    )
    us_eod_missing = [a for a in alerts
                      if a.code == wd.ALERT_MISSING_RUN and a.run_type == wd.RUN_US_EOD]
    assert us_eod_missing == []


def test_us_eod_window_past_deadline(tmp_path: Path):
    """08:00 KST = past 07:00 US_EOD deadline → MISSING_RUN."""
    # 08:00 KST = 23:00 UTC previous day
    now = datetime(2026, 4, 21, 23, 0, 0, tzinfo=timezone.utc)
    td = now.astimezone(KST).date()
    alerts = wd.evaluate_alerts(
        now_utc=now, heartbeat=_heartbeat_dict(now), marker_today=None,
        trade_date=td,
    )
    us_eod_missing = [a for a in alerts
                      if a.code == wd.ALERT_MISSING_RUN and a.run_type == wd.RUN_US_EOD]
    assert len(us_eod_missing) == 1


# =============================================================================
# R5 (2026-04-24): STALE_DB + STALE_OHLCV_CACHE staleness checks
# =============================================================================

# ---------- _count_bdays helper ----------

def test_count_bdays_weekend_exclusive():
    """Mon → Fri = 4 bdays. Weekends skipped."""
    # 2026-04-20 (Mon) → 2026-04-24 (Fri) = Tue/Wed/Thu/Fri = 4
    assert wd._count_bdays(date(2026, 4, 20), date(2026, 4, 24)) == 4


def test_count_bdays_spanning_weekend():
    # Fri 2026-04-17 → Mon 2026-04-20 = just Monday = 1 bday
    assert wd._count_bdays(date(2026, 4, 17), date(2026, 4, 20)) == 1


def test_count_bdays_same_day_zero():
    assert wd._count_bdays(date(2026, 4, 20), date(2026, 4, 20)) == 0


def test_count_bdays_start_after_end_zero():
    assert wd._count_bdays(date(2026, 4, 21), date(2026, 4, 20)) == 0


# ---------- _parse_snapshot_data_last_date ----------

def test_parse_snapshot_normal_format():
    sv = "2026-04-24:DB:2026-04-23:901:abc123def456"
    assert wd._parse_snapshot_data_last_date(sv) == date(2026, 4, 23)


def test_parse_snapshot_none_returns_none():
    assert wd._parse_snapshot_data_last_date(None) is None


def test_parse_snapshot_empty_returns_none():
    assert wd._parse_snapshot_data_last_date("") is None


def test_parse_snapshot_malformed_returns_none():
    assert wd._parse_snapshot_data_last_date("not_a_version") is None
    assert wd._parse_snapshot_data_last_date("a:b") is None  # only 2 parts
    assert wd._parse_snapshot_data_last_date("2026-04-24:DB:BAD_DATE:1:h") is None


def test_parse_snapshot_non_string_returns_none():
    assert wd._parse_snapshot_data_last_date(123) is None
    assert wd._parse_snapshot_data_last_date({"trade_date": "2026-04-24"}) is None


# ---------- STALE_DB via check_db_staleness ----------

def test_stale_db_fresh_snapshot_no_alert():
    """snapshot data_last_date = yesterday (trade_date-1 bday) → fresh, no alert."""
    trade_date = date(2026, 4, 24)  # Friday
    marker = {
        "runs": {
            wd.RUN_KR_BATCH: {
                "snapshot_version": "2026-04-24:DB:2026-04-23:901:abc",
            },
        },
    }
    alert = wd.check_db_staleness(marker, trade_date=trade_date)
    assert alert is None


def test_stale_db_lag_beyond_threshold_alerts():
    """data_last_date = 5 bdays ago → alert (> 3 bday threshold)."""
    trade_date = date(2026, 4, 24)  # Friday
    # 2026-04-17 (Fri) → 2026-04-24 (Fri) = 5 bdays (Mon-Thu+Fri)
    marker = {
        "runs": {
            wd.RUN_KR_BATCH: {
                "snapshot_version": "2026-04-24:DB:2026-04-17:850:xyz",
            },
        },
    }
    alert = wd.check_db_staleness(marker, trade_date=trade_date)
    assert alert is not None
    assert alert.code == wd.ALERT_STALE_DB
    assert alert.run_type == wd.RUN_KR_BATCH
    assert alert.severity == "WARN"
    assert "2026-04-17" in alert.detail


def test_stale_db_no_marker_no_alert():
    """Missing marker → no alert (not enough info; MISSING_RUN handles it)."""
    assert wd.check_db_staleness(None, trade_date=date(2026, 4, 24)) is None


def test_stale_db_missing_snapshot_version_no_alert():
    """KR_BATCH in marker but no snapshot_version → don't false-alert."""
    marker = {
        "runs": {
            wd.RUN_KR_BATCH: {
                "status": "RUNNING",
                "snapshot_version": None,
            },
        },
    }
    assert wd.check_db_staleness(marker, trade_date=date(2026, 4, 24)) is None


def test_stale_db_threshold_respected():
    """Custom max_bdays must be honored (tuneable threshold)."""
    trade_date = date(2026, 4, 24)
    marker = {
        "runs": {
            wd.RUN_KR_BATCH: {
                "snapshot_version": "2026-04-24:DB:2026-04-22:901:h",
            },
        },
    }
    # 2026-04-22 (Wed) → 2026-04-24 (Fri) = 2 bdays
    assert wd.check_db_staleness(marker, trade_date=trade_date, max_bdays=3) is None
    assert wd.check_db_staleness(marker, trade_date=trade_date, max_bdays=1) is not None


# ---------- STALE_OHLCV_CACHE via check_ohlcv_cache_staleness ----------

def test_stale_ohlcv_cache_fresh_files_no_alert(tmp_path: Path):
    """CSV file touched today → no alert."""
    ohlcv_dir = tmp_path / "ohlcv"
    ohlcv_dir.mkdir()
    csv = ohlcv_dir / "005930.csv"
    csv.write_text("date,close\n2026-04-24,50000\n", encoding="utf-8")
    now = datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc)
    alert = wd.check_ohlcv_cache_staleness(
        ohlcv_dir, now_utc=now, trade_date=now.astimezone(KST).date(),
    )
    assert alert is None


def test_stale_ohlcv_cache_old_files_alert(tmp_path: Path):
    """CSV last touched 7 days ago → alert."""
    ohlcv_dir = tmp_path / "ohlcv"
    ohlcv_dir.mkdir()
    csv = ohlcv_dir / "005930.csv"
    csv.write_text("date,close\n2026-04-10,50000\n", encoding="utf-8")
    # Backdate mtime to 7 days ago
    now = datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc)
    old_ts = (now - timedelta(days=7)).timestamp()
    os.utime(csv, (old_ts, old_ts))

    alert = wd.check_ohlcv_cache_staleness(
        ohlcv_dir, now_utc=now, trade_date=now.astimezone(KST).date(),
    )
    assert alert is not None
    assert alert.code == wd.ALERT_STALE_OHLCV_CACHE
    assert alert.severity == "WARN"
    assert "lag=" in alert.detail


def test_stale_ohlcv_cache_missing_dir_no_alert(tmp_path: Path):
    """Non-existent dir → graceful None (no spam on fresh installs)."""
    now = datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc)
    alert = wd.check_ohlcv_cache_staleness(
        tmp_path / "missing_ohlcv", now_utc=now,
        trade_date=now.astimezone(KST).date(),
    )
    assert alert is None


def test_stale_ohlcv_cache_empty_dir_no_alert(tmp_path: Path):
    """Empty dir → no alert (nothing to check)."""
    ohlcv_dir = tmp_path / "ohlcv"
    ohlcv_dir.mkdir()
    now = datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc)
    alert = wd.check_ohlcv_cache_staleness(
        ohlcv_dir, now_utc=now, trade_date=now.astimezone(KST).date(),
    )
    assert alert is None


def test_stale_ohlcv_cache_uses_max_mtime(tmp_path: Path):
    """If any single file is fresh, whole dir is considered fresh."""
    ohlcv_dir = tmp_path / "ohlcv"
    ohlcv_dir.mkdir()
    now = datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc)

    # One very old file
    old_csv = ohlcv_dir / "000001.csv"
    old_csv.write_text("stale", encoding="utf-8")
    old_ts = (now - timedelta(days=30)).timestamp()
    os.utime(old_csv, (old_ts, old_ts))

    # One fresh file
    fresh_csv = ohlcv_dir / "005930.csv"
    fresh_csv.write_text("fresh", encoding="utf-8")
    # default mtime = now

    alert = wd.check_ohlcv_cache_staleness(
        ohlcv_dir, now_utc=now, trade_date=now.astimezone(KST).date(),
    )
    # One fresh CSV saves the whole directory (intended for weekend runs
    # where not every code gets an update but at least some do).
    assert alert is None


# ---------- evaluate_alerts integration ----------

def test_evaluate_alerts_skips_staleness_when_ohlcv_dir_omitted():
    """ohlcv_dir=None path must not crash (opt-out)."""
    now = datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc)
    alerts = wd.evaluate_alerts(
        now_utc=now, heartbeat=_heartbeat_dict(now),
        marker_today=None, trade_date=now.astimezone(KST).date(),
        ohlcv_dir=None,
    )
    codes = [a.code for a in alerts]
    assert wd.ALERT_STALE_OHLCV_CACHE not in codes


def test_evaluate_alerts_emits_stale_db_from_snapshot(tmp_path: Path):
    """End-to-end: lagging snapshot_version surfaces through evaluate_alerts."""
    now = datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc)
    trade_date = now.astimezone(KST).date()
    marker = _marker_dict(trade_date, runs={
        wd.RUN_KR_BATCH: {
            "status": wd.STATUS_SUCCESS,
            "attempt_no": 1,
            "started_at": now.isoformat(),
            "finished_at": now.isoformat(),
            "last_update": now.isoformat(),
            "snapshot_version": "2026-04-24:DB:2026-04-17:901:h",
        },
    })
    alerts = wd.evaluate_alerts(
        now_utc=now, heartbeat=_heartbeat_dict(now),
        marker_today=marker, trade_date=trade_date,
    )
    stale_db = [a for a in alerts if a.code == wd.ALERT_STALE_DB]
    assert len(stale_db) == 1


def test_evaluate_alerts_emits_stale_cache_from_ohlcv_dir(tmp_path: Path):
    """End-to-end: old OHLCV mtimes surface through evaluate_alerts."""
    now = datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc)
    ohlcv_dir = tmp_path / "ohlcv"
    ohlcv_dir.mkdir()
    csv = ohlcv_dir / "005930.csv"
    csv.write_text("x", encoding="utf-8")
    old_ts = (now - timedelta(days=10)).timestamp()
    os.utime(csv, (old_ts, old_ts))

    alerts = wd.evaluate_alerts(
        now_utc=now, heartbeat=_heartbeat_dict(now),
        marker_today=None, trade_date=now.astimezone(KST).date(),
        ohlcv_dir=ohlcv_dir,
    )
    stale_cache = [a for a in alerts if a.code == wd.ALERT_STALE_OHLCV_CACHE]
    assert len(stale_cache) == 1


# ---------- resolve_ohlcv_dir ----------

def test_resolve_ohlcv_dir_cli_arg_wins(tmp_path: Path):
    p = wd.resolve_ohlcv_dir(str(tmp_path))
    assert p == tmp_path.resolve()


def test_resolve_ohlcv_dir_env_var(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("QTRON_OHLCV_DIR", str(tmp_path))
    p = wd.resolve_ohlcv_dir(None)
    assert p == tmp_path.resolve()


def test_resolve_ohlcv_dir_default(monkeypatch):
    monkeypatch.delenv("QTRON_OHLCV_DIR", raising=False)
    p = wd.resolve_ohlcv_dir(None)
    assert p.name == "ohlcv"
    assert p.parent.name == "data"
    assert p.parent.parent.name == "kr"
