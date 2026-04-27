#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""scripts/watchdog_external.py — Independent dead-man switch.

Strictly standalone: stdlib ONLY. No imports from kr/, us/, shared/.
Reads heartbeat.json + run_completion_YYYYMMDD.json, evaluates alerts,
sends to Telegram DEADMAN channel, writes incident markdown, and
persists dedup state.

Invocation:
    python scripts/watchdog_external.py [--data-dir PATH] [--dry-run]

Environment variables:
    QTRON_PIPELINE_DATA_DIR    — default: kr/data/pipeline (relative to repo root)
    QTRON_TELEGRAM_TOKEN_DEADMAN  — Telegram bot token for infra alerts
    QTRON_TELEGRAM_CHAT_ID_DEADMAN — target chat id
    QTRON_WATCHDOG_INCIDENT_DIR — default: backup/reports/incidents

Design refs:
    v3 §Hardening-3, v4 §권장 3-4 (heartbeat dual + liveness detection),
    Jeff A1.5 gate (tray-death detection, dedup, DEADMAN channel).

Why duplicate schema constants here instead of importing?
    Jeff mandate: "Q-TRON 내부 import 없이 독립 동작". If this script
    imports pipeline modules, their load order could trigger issues in
    an environment where Q-TRON itself is broken. The few constants
    below mirror kr/pipeline/completion_schema.py and must be kept in
    sync on schema bumps — the top of that file documents this contract.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

# ===============================================================
# SCHEMA MIRROR — must match kr/pipeline/completion_schema.py
# ===============================================================
MARKER_FILENAME_FMT = "run_completion_{yyyymmdd}.json"
HEARTBEAT_FILENAME = "heartbeat.json"
HEARTBEAT_BAK_FILENAME = "heartbeat.bak.json"
DEDUP_FILENAME = "watchdog_external.state.json"

RUN_KR_BATCH = "KR_BATCH"
RUN_KR_EOD = "KR_EOD"
RUN_US_BATCH = "US_BATCH"
RUN_US_EOD = "US_EOD"
ALL_RUN_TYPES = (RUN_KR_BATCH, RUN_KR_EOD, RUN_US_BATCH, RUN_US_EOD)

STATUS_MISSING = "MISSING"
STATUS_RUNNING = "RUNNING"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_PARTIAL = "PARTIAL"
STATUS_PRE_FLIGHT_FAIL = "PRE_FLIGHT_FAIL"
STATUS_PRE_FLIGHT_STALE_INPUT = "PRE_FLIGHT_STALE_INPUT"

# KST minutes-from-midnight. Windows cross-midnight use >1440 for deadline.
KST = timezone(timedelta(hours=9))


def _hm(h: int, m: int) -> int:
    return h * 60 + m


EXPECTED_WINDOWS_KST: dict[str, tuple[int, int]] = {
    # R18 (2026-04-23): aligned with kr/pipeline/completion_schema.py.
    # Natural tray batch 16:05 → 17:35 complete + 25min slop = 18:00.
    # KR_EOD post_batch_arm fires ~30s after batch SUCCESS + 30min safety = 18:30.
    RUN_KR_BATCH: (_hm(16,  5), _hm(18,  0)),
    RUN_KR_EOD:   (_hm(15, 35), _hm(18, 30)),
    RUN_US_BATCH: (_hm(23, 40), _hm(24, 40)),
    RUN_US_EOD:   (_hm(5,   5), _hm(7,   0)),  # aligned with schema (was 5:30)
}

# Alert codes
ALERT_HEARTBEAT_MISSING = "HEARTBEAT_MISSING"
ALERT_STALLED_HEARTBEAT = "STALLED_HEARTBEAT"
ALERT_MISSING_RUN = "MISSING_RUN"
ALERT_STALLED_RUNNING = "STALLED_RUNNING"
ALERT_STALE_SYSTEM = "STALE_SYSTEM"
ALERT_INVARIANT_VIOLATION = "INVARIANT_VIOLATION"
# R5 (2026-04-24): data-staleness deadman checks (DB + CSV cache).
# Jeff mandate keeps this script stdlib-only, so DB freshness is
# inferred from marker.runs.KR_BATCH.snapshot_version (data_last_date
# is element [2] per kr/lifecycle/batch.py::BATCH_SNAPSHOT_VERSION).
# CSV cache staleness uses Path.stat() only — no psycopg2.
ALERT_STALE_OHLCV_CACHE = "STALE_OHLCV_CACHE"
ALERT_STALE_DB = "STALE_DB"

# Thresholds
HEARTBEAT_STALE_SEC = 120  # 2 min — tray tick every 30s, 4x margin
RUNNING_STALE_SEC = 30 * 60  # 30 min
STALE_SYSTEM_SEC = 24 * 3600  # 24h
DEDUP_REFIRE_SEC = 6 * 3600  # re-alert same condition every 6h

# Severity tiers (Jeff 2026-04-24): "CRITICAL 도배로 가독성 저하" 방지.
#  - CRITICAL 🔴: 사람 개입 필요, 실거래 블로킹 가능
#  - WARN     🟡: 운영 저하, 조사 권장, 기능은 유지
#  - INFO     🔵: 관찰용 blip (텔레그램 푸시 제외, 파일만)
SEVERITY_CRITICAL = "CRITICAL"
SEVERITY_WARN = "WARN"
SEVERITY_INFO = "INFO"

SEVERITY_EMOJI = {
    SEVERITY_CRITICAL: "🔴",
    SEVERITY_WARN: "🟡",
    SEVERITY_INFO: "🔵",
}

# Heartbeat age tiers (seconds). Below WARN threshold = not even an alert.
HEARTBEAT_WARN_SEC = 300        # 120-300s: still INFO (single tick blip)
HEARTBEAT_CRITICAL_SEC = 600    # >600s: real tray downtime
# R5: business-day lag before crying "stale". 3 bdays ≈ covers Monday holiday +
# one normal weekend gap without spamming. Tuneable via env if a market holiday
# stretches longer.
STALE_OHLCV_BDAYS = 3
STALE_DB_BDAYS = 3

# Logging
_log = logging.getLogger("qtron.watchdog_external")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


# ===============================================================
# DATA LOADING (stdlib-only)
# ===============================================================

def _load_json(path: Path) -> Optional[dict]:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        _log.debug("[LOAD_FAIL] %s: %s", path, e)
        return None


def load_heartbeat(data_dir: Path) -> Optional[dict]:
    """Primary → bak fallback. Returns dict or None if both unreadable."""
    for name in (HEARTBEAT_FILENAME, HEARTBEAT_BAK_FILENAME):
        d = _load_json(data_dir / name)
        if d is not None:
            return d
    return None


def load_marker_for_date(data_dir: Path, d: date) -> Optional[dict]:
    path = data_dir / MARKER_FILENAME_FMT.format(yyyymmdd=d.strftime("%Y%m%d"))
    return _load_json(path)


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except (TypeError, ValueError):
        return None


# ===============================================================
# ALERT EVALUATION
# ===============================================================

@dataclass
class Alert:
    code: str
    run_type: Optional[str]
    trade_date: str
    detail: str
    severity: str = "CRITICAL"  # CRITICAL | WARN

    def dedup_key(self) -> str:
        return f"{self.trade_date}|{self.run_type or '-'}|{self.code}"

    def human(self) -> str:
        rt = f" {self.run_type}" if self.run_type else ""
        emoji = SEVERITY_EMOJI.get(self.severity, "")
        prefix = f"{emoji} [{self.severity}]" if emoji else f"[{self.severity}]"
        return f"{prefix} {self.code}{rt} ({self.trade_date}): {self.detail}"


def _classify_heartbeat_severity(age_sec: float) -> str:
    """STALLED_HEARTBEAT severity from age (seconds).

    age 120~300s  → INFO (file only, no Telegram)
    age 300~600s  → WARN
    age >600s     → CRITICAL (long downtime, needs human)
    """
    if age_sec >= HEARTBEAT_CRITICAL_SEC:
        return SEVERITY_CRITICAL
    if age_sec >= HEARTBEAT_WARN_SEC:
        return SEVERITY_WARN
    return SEVERITY_INFO


def in_window(now_kst_minutes: int, window: tuple[int, int]) -> bool:
    """True if now is between window start and deadline (inclusive)."""
    start, deadline = window
    if deadline > 24 * 60:
        # Cross-midnight window (US_BATCH): now may be in (start..24:00) or (0..deadline-24:00)
        return now_kst_minutes >= start or now_kst_minutes <= (deadline - 24 * 60)
    return start <= now_kst_minutes <= deadline


def past_deadline(now_kst_minutes: int, window: tuple[int, int]) -> bool:
    """True if now is past window deadline (same calendar day)."""
    _, deadline = window
    if deadline > 24 * 60:
        return False  # cross-midnight; ambiguous, handled by caller with date shift
    return now_kst_minutes > deadline


def kst_now_minutes(now_utc: datetime) -> int:
    kst = now_utc.astimezone(KST)
    return kst.hour * 60 + kst.minute


def is_market_off(run_type: str, kst_d: date) -> bool:
    """Weekend gate for batch/EOD run-type alerts.

    Mapping (KST weekday → market that should run):
      KR (KR_BATCH/KR_EOD): Mon-Fri only.
      US (US_BATCH/US_EOD): the windows fire on KST aimed at the
          PREVIOUS ET trading day, so:
            KST Mon  = ET Sun  → US OFF
            KST Sun  = ET Sat  → US OFF
            KST Tue~Sat (else) → US ON

    Holiday calendars are not handled (stdlib-only mandate). When the
    KR/US market is closed for a public holiday on a weekday, deadman
    will still fire MISSING_RUN — Jeff dismisses or we add a calendar
    feed later. Weekend gating alone removes the persistent Mon-morning
    US_EOD false positive.
    """
    wd = kst_d.weekday()  # Mon=0..Sun=6
    if run_type in (RUN_KR_BATCH, RUN_KR_EOD):
        return wd >= 5  # Sat/Sun
    if run_type in (RUN_US_BATCH, RUN_US_EOD):
        return wd in (0, 6)  # KST Mon (ET Sun) or KST Sun (ET Sat)
    return False


def evaluate_alerts(
    *, now_utc: datetime, heartbeat: Optional[dict],
    marker_today: Optional[dict], trade_date: date,
    ohlcv_dir: Optional[Path] = None,
) -> list[Alert]:
    alerts: list[Alert] = []
    td_str = trade_date.strftime("%Y-%m-%d")
    now_min = kst_now_minutes(now_utc)

    # --- Heartbeat checks (process alive?) ---
    if heartbeat is None:
        # No heartbeat file at all = tray never started or file lost. Always CRITICAL.
        alerts.append(Alert(
            code=ALERT_HEARTBEAT_MISSING, run_type=None, trade_date=td_str,
            detail="no heartbeat file (tray likely dead or never started)",
            severity=SEVERITY_CRITICAL,
        ))
    else:
        hb_ts = _parse_iso(heartbeat.get("ts"))
        if hb_ts is None:
            # Corrupt timestamp — treat as WARN; tray may still be up but writing bad data.
            alerts.append(Alert(
                code=ALERT_STALLED_HEARTBEAT, run_type=None, trade_date=td_str,
                detail="heartbeat timestamp unparseable",
                severity=SEVERITY_WARN,
            ))
        else:
            age = (now_utc - (hb_ts if hb_ts.tzinfo else hb_ts.replace(tzinfo=timezone.utc))).total_seconds()
            if age > HEARTBEAT_STALE_SEC:
                alerts.append(Alert(
                    code=ALERT_STALLED_HEARTBEAT, run_type=None, trade_date=td_str,
                    detail=f"heartbeat age={int(age)}s (threshold={HEARTBEAT_STALE_SEC}s)",
                    severity=_classify_heartbeat_severity(age),
                ))

    # --- Per-run_type checks ---
    runs = (marker_today or {}).get("runs", {}) if marker_today else {}

    for run_type in ALL_RUN_TYPES:
        # Weekend gate: KR off Sat/Sun; US (KST view) off Sun/Mon.
        # Heartbeat check above is intentionally market-agnostic — the
        # tray must be alive even on weekends to record any state.
        if is_market_off(run_type, trade_date):
            continue
        win = EXPECTED_WINDOWS_KST[run_type]
        run = runs.get(run_type)

        if run is None:
            # Run not recorded at all. Only alert if past deadline.
            # CRITICAL: a scheduled execution window has truly passed without
            # any marker entry — a real trading opportunity was missed.
            if past_deadline(now_min, win):
                alerts.append(Alert(
                    code=ALERT_MISSING_RUN, run_type=run_type, trade_date=td_str,
                    detail=f"no marker entry; past deadline "
                           f"(window={_fmt_window(win)})",
                    severity=SEVERITY_CRITICAL,
                ))
            continue

        status = run.get("status", STATUS_MISSING)

        # MISSING explicit + past deadline (CRITICAL — same as no entry case).
        if status == STATUS_MISSING and past_deadline(now_min, win):
            alerts.append(Alert(
                code=ALERT_MISSING_RUN, run_type=run_type, trade_date=td_str,
                detail=f"status=MISSING past deadline (window={_fmt_window(win)})",
                severity=SEVERITY_CRITICAL,
            ))

        # RUNNING stalled (v4 권장 4: only CRITICAL if heartbeat dead; else WARN)
        if status == STATUS_RUNNING:
            last_upd = _parse_iso(run.get("last_update"))
            if last_upd is not None:
                last_upd = last_upd if last_upd.tzinfo else last_upd.replace(tzinfo=timezone.utc)
                age = (now_utc - last_upd).total_seconds()
                if age > RUNNING_STALE_SEC:
                    hb_alive = (heartbeat is not None and _parse_iso(heartbeat.get("ts"))
                                is not None and
                                (now_utc - _parse_iso(heartbeat["ts"]).replace(
                                    tzinfo=timezone.utc)).total_seconds() < HEARTBEAT_STALE_SEC)
                    severity = "CRITICAL" if not hb_alive else "WARN"
                    alerts.append(Alert(
                        code=ALERT_STALLED_RUNNING, run_type=run_type, trade_date=td_str,
                        detail=f"status=RUNNING stale {int(age)}s; "
                               f"heartbeat_alive={hb_alive}",
                        severity=severity,
                    ))

        # Invariant: SUCCESS with checks.any_false (shouldn't happen per producer
        # invariants, but defensive — catches corrupted marker). CRITICAL because
        # state machine integrity is broken.
        if status == STATUS_SUCCESS:
            checks = run.get("checks") or {}
            if any(v is False for v in checks.values()):
                alerts.append(Alert(
                    code=ALERT_INVARIANT_VIOLATION, run_type=run_type, trade_date=td_str,
                    detail=f"status=SUCCESS with checks any_false: "
                           f"{[k for k, v in checks.items() if v is False]}",
                    severity=SEVERITY_CRITICAL,
                ))

    # --- STALE_SYSTEM: marker last_update > 24h even though something exists ---
    if marker_today is not None:
        last_upd = _parse_iso(marker_today.get("last_update"))
        if last_upd is not None:
            last_upd = last_upd if last_upd.tzinfo else last_upd.replace(tzinfo=timezone.utc)
            if (now_utc - last_upd).total_seconds() > STALE_SYSTEM_SEC:
                alerts.append(Alert(
                    code=ALERT_STALE_SYSTEM, run_type=None, trade_date=td_str,
                    detail=f"marker last_update age > {STALE_SYSTEM_SEC // 3600}h",
                    severity="WARN",
                ))

    # --- R5 (2026-04-24): DB + OHLCV cache staleness ---
    # Both checks reflect KR data freshness; KR weekend skips them.
    if not is_market_off(RUN_KR_BATCH, trade_date):
        db_alert = check_db_staleness(marker_today, trade_date=trade_date)
        if db_alert is not None:
            alerts.append(db_alert)

        if ohlcv_dir is not None:
            cache_alert = check_ohlcv_cache_staleness(
                ohlcv_dir, now_utc=now_utc, trade_date=trade_date,
            )
            if cache_alert is not None:
                alerts.append(cache_alert)

    return alerts


def _fmt_window(window: tuple[int, int]) -> str:
    start, deadline = window
    def _fmt(m: int) -> str:
        return f"{m // 60:02d}:{m % 60:02d}"
    return f"KST {_fmt(start)}→{_fmt(deadline)}"


# ===============================================================
# R5 (2026-04-24): data-staleness detection
# ===============================================================

def _count_bdays(start_d: date, end_d: date) -> int:
    """Business days strictly between start_d and end_d (exclusive start,
    inclusive end). Monday-Friday only; does not attempt Korean holiday
    calendar (stdlib-only constraint). Callers layer a ≥3-bday cushion
    to absorb typical one-day holidays without spamming."""
    if start_d >= end_d:
        return 0
    n = 0
    cur = start_d + timedelta(days=1)
    while cur <= end_d:
        if cur.weekday() < 5:
            n += 1
        cur += timedelta(days=1)
    return n


def _parse_snapshot_data_last_date(snapshot_version: Any) -> Optional[date]:
    """Extract data_last_date from snapshot_version.

    Format (mirrors kr/lifecycle/batch.py):
        "{trade_date}:{source}:{data_last_date}:{universe_count}:{matrix_hash}"

    Returns None on any parse failure so watchdog never false-alerts on
    legitimately-null or schema-bumped snapshot_version values.
    """
    if not isinstance(snapshot_version, str):
        return None
    parts = snapshot_version.split(":")
    if len(parts) < 3:
        return None
    try:
        return date.fromisoformat(parts[2])
    except (ValueError, TypeError):
        return None


def check_ohlcv_cache_staleness(
    ohlcv_dir: Path, *, now_utc: datetime, trade_date: date,
    max_bdays: int = STALE_OHLCV_BDAYS,
) -> Optional[Alert]:
    """R5: scan OHLCV CSV mtimes → alert if max_mtime lags > max_bdays.

    stdlib-only (Path.stat). Gracefully returns None when dir missing
    or empty so fresh installs don't spam the DEADMAN channel.
    """
    if not ohlcv_dir.exists():
        return None
    max_mtime = 0.0
    try:
        for csv_path in ohlcv_dir.glob("*.csv"):
            try:
                mt = csv_path.stat().st_mtime
                if mt > max_mtime:
                    max_mtime = mt
            except OSError:
                continue
    except OSError:
        return None
    if max_mtime == 0.0:
        return None
    last_mtime_kst = (
        datetime.fromtimestamp(max_mtime, tz=timezone.utc).astimezone(KST)
    )
    bdays = _count_bdays(last_mtime_kst.date(), trade_date)
    if bdays > max_bdays:
        return Alert(
            code=ALERT_STALE_OHLCV_CACHE, run_type=None,
            trade_date=trade_date.strftime("%Y-%m-%d"),
            detail=(
                f"OHLCV CSV max_mtime={last_mtime_kst.date()} "
                f"lag={bdays} bdays > {max_bdays} "
                f"(dir={ohlcv_dir})"
            ),
            severity="WARN",
        )
    return None


def check_db_staleness(
    marker_today: Optional[dict], *, trade_date: date,
    max_bdays: int = STALE_DB_BDAYS,
) -> Optional[Alert]:
    """R5: infer DB freshness from KR_BATCH snapshot_version.

    Why not query PG directly? Jeff mandate: watchdog must run even when
    the DB server is unreachable. Marker is the canonical record of what
    the last successful KR_BATCH observed in DB; a stale snapshot is a
    strong proxy for "DB has not caught up."
    """
    if not marker_today:
        return None
    runs = marker_today.get("runs") or {}
    kr_batch = runs.get(RUN_KR_BATCH) or {}
    sv = kr_batch.get("snapshot_version")
    data_last = _parse_snapshot_data_last_date(sv)
    if data_last is None:
        return None
    bdays = _count_bdays(data_last, trade_date)
    if bdays > max_bdays:
        return Alert(
            code=ALERT_STALE_DB, run_type=RUN_KR_BATCH,
            trade_date=trade_date.strftime("%Y-%m-%d"),
            detail=(
                f"KR_BATCH snapshot data_last_date={data_last} "
                f"lag={bdays} bdays > {max_bdays}"
            ),
            severity="WARN",
        )
    return None


def resolve_ohlcv_dir(cli_arg: Optional[str] = None) -> Path:
    """R5: CSV cache dir. Default <repo>/kr/data/ohlcv, overridable via
    QTRON_OHLCV_DIR env or --ohlcv-dir CLI arg."""
    if cli_arg:
        return Path(cli_arg).resolve()
    env = os.environ.get("QTRON_OHLCV_DIR")
    if env:
        return Path(env).resolve()
    return (Path(__file__).resolve().parent.parent / "kr" / "data" / "ohlcv")


# ===============================================================
# DEDUP STATE
# ===============================================================

def load_dedup(data_dir: Path) -> dict[str, float]:
    d = _load_json(data_dir / DEDUP_FILENAME) or {}
    # Keys: dedup_key → last-fired epoch seconds
    return {k: float(v) for k, v in d.items() if isinstance(v, (int, float))}


def save_dedup(data_dir: Path, dedup: dict[str, float]) -> None:
    path = data_dir / DEDUP_FILENAME
    try:
        data_dir.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(dedup, ensure_ascii=False, indent=2),
                       encoding="utf-8")
        os.replace(tmp, path)
    except OSError as e:
        _log.warning("[DEDUP_WRITE_FAIL] %s", e)


def filter_by_dedup(
    alerts: list[Alert], dedup: dict[str, float], now_epoch: float,
    *, refire_sec: int = DEDUP_REFIRE_SEC,
) -> tuple[list[Alert], dict[str, float]]:
    """Return (alerts_to_fire, updated_dedup)."""
    to_fire: list[Alert] = []
    updated = dict(dedup)
    for a in alerts:
        k = a.dedup_key()
        last = updated.get(k)
        if last is None or (now_epoch - last) >= refire_sec:
            to_fire.append(a)
            updated[k] = now_epoch
    # Garbage-collect entries older than 7d
    cutoff = now_epoch - 7 * 24 * 3600
    updated = {k: v for k, v in updated.items() if v >= cutoff}
    return to_fire, updated


# ===============================================================
# TELEGRAM (DEADMAN channel)
# ===============================================================

def send_telegram(text: str, *, timeout: float = 8.0) -> bool:
    token = os.environ.get("QTRON_TELEGRAM_TOKEN_DEADMAN")
    chat_id = os.environ.get("QTRON_TELEGRAM_CHAT_ID_DEADMAN")
    if not token or not chat_id:
        _log.warning("[TG_DEADMAN_NOT_CONFIGURED] token=%s chat_id=%s — alert dropped",
                     bool(token), bool(chat_id))
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    body = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status == 200
    except (urllib.error.URLError, OSError, TimeoutError) as e:
        _log.warning("[TG_DEADMAN_SEND_FAIL] %s", e)
        return False


# ===============================================================
# DASHBOARD ALERTS PANEL FEED (kr/us recent_alerts.jsonl)
# ===============================================================
# The dashboard's "Telegram Alerts (24h)" panel reads
#   kr/data/notify/recent_alerts.jsonl   (and us/... mirror)
# Format ref: kr/notify/recent_alerts.py docstring.
# Deadman runs stdlib-only so we replicate just enough of that
# contract here (one JSON object per line, append-only). No import
# from kr/us/shared — satisfies Jeff's independence mandate.

_DEADMAN_SRC_LABEL = "deadman"


def _alerts_path_for_market(repo_root: Path, market: str) -> Path:
    sub = "kr" if market == "KR" else "us"
    return repo_root / sub / "data" / "notify" / "recent_alerts.jsonl"


def _market_for_alert(a: "Alert") -> str:
    """Route an alert to KR or US JSONL. System-wide (heartbeat,
    stale_system) routes to KR — that's where the tray runs and where
    Jeff watches the panel from."""
    rt = a.run_type or ""
    if rt.startswith("US_"):
        return "US"
    return "KR"


def record_dashboard_alert(
    a: "Alert", *, repo_root: Path, send_status: str,
    now_utc: Optional[datetime] = None,
) -> None:
    """Append one row to the matching market's recent_alerts.jsonl so
    the dashboard panel surfaces deadman alerts alongside main-bot
    alerts. Never raises — the panel is observability, not control."""
    try:
        market = _market_for_alert(a)
        path = _alerts_path_for_market(repo_root, market)
        path.parent.mkdir(parents=True, exist_ok=True)
        # UI taxonomy is INFO/WARN/ERROR — collapse CRITICAL → ERROR
        # to match kr/notify/recent_alerts.py:160.
        sev = (a.severity or "INFO").upper()
        level = "ERROR" if sev == "CRITICAL" else sev
        text = a.human()
        # Strip leading severity tag from title so the panel reads
        # "MISSING_RUN US_EOD ..." not "🔴 [CRITICAL] MISSING_RUN ...".
        title = text.split("] ", 1)[1] if "] " in text else text
        title = title.splitlines()[0][:80]
        item = {
            "ts": (now_utc or datetime.now(timezone.utc)).isoformat(),
            "market": market,
            "level": level,
            "title": title,
            "message": text,
            "send_status": "sent" if send_status == "sent" else "failed",
            "source": _DEADMAN_SRC_LABEL,
        }
        line = json.dumps(item, ensure_ascii=False) + "\n"
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)
    except OSError as e:
        _log.warning("[DEADMAN_ALERT_FEED_FAIL] %s", e)


# ===============================================================
# INCIDENT WRITER
# ===============================================================

def write_incident(alerts: list[Alert], incident_dir: Path,
                   *, now_utc: Optional[datetime] = None) -> Optional[Path]:
    if not alerts:
        return None
    now = now_utc or datetime.now(timezone.utc)
    incident_dir.mkdir(parents=True, exist_ok=True)

    now_kst = now.astimezone(KST)
    stamp = now_kst.strftime("%Y%m%d_%H%M%S")
    path = incident_dir / f"{stamp}_watchdog_external.md"

    lines = [
        f"# Watchdog Incident — {now_kst.isoformat()}",
        "",
        "Automatically generated by scripts/watchdog_external.py.",
        "",
        "| severity | code | run_type | trade_date | detail |",
        "|----------|------|----------|------------|--------|",
    ]
    for a in alerts:
        lines.append(
            f"| {a.severity} | {a.code} | {a.run_type or '-'} | "
            f"{a.trade_date} | {a.detail} |"
        )
    lines.extend([
        "",
        "## Remediation",
        "- Check tray process (Windows Task Manager / `ps aux | grep tray`)",
        "- If tray alive: inspect `kr/data/pipeline/state_YYYYMMDD.json`",
        "  (orchestrator internal) for step failures",
        "- If tray dead: restart via `02_live.bat` or equivalent",
        "- Review `kr/logs/` for the run_type(s) flagged",
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


# ===============================================================
# MAIN
# ===============================================================

def resolve_data_dir(cli_arg: Optional[str]) -> Path:
    if cli_arg:
        return Path(cli_arg).resolve()
    env = os.environ.get("QTRON_PIPELINE_DATA_DIR")
    if env:
        return Path(env).resolve()
    # Default: <repo_root>/kr/data/pipeline
    # Script is at <repo>/scripts/watchdog_external.py
    return (Path(__file__).resolve().parent.parent / "kr" / "data" / "pipeline")


def resolve_incident_dir() -> Path:
    env = os.environ.get("QTRON_WATCHDOG_INCIDENT_DIR")
    if env:
        return Path(env).resolve()
    return (Path(__file__).resolve().parent.parent / "backup" / "reports" / "incidents")


def today_in_kst(now_utc: datetime) -> date:
    return now_utc.astimezone(KST).date()


def run_once(
    *, data_dir: Path, incident_dir: Path, now_utc: datetime,
    dry_run: bool = False, ohlcv_dir: Optional[Path] = None,
) -> dict:
    """One pass of the watchdog. Returns summary dict for logging."""
    td = today_in_kst(now_utc)
    heartbeat = load_heartbeat(data_dir)
    marker = load_marker_for_date(data_dir, td)

    alerts = evaluate_alerts(
        now_utc=now_utc, heartbeat=heartbeat,
        marker_today=marker, trade_date=td, ohlcv_dir=ohlcv_dir,
    )

    dedup = load_dedup(data_dir)
    now_epoch = now_utc.timestamp()
    to_fire, updated_dedup = filter_by_dedup(alerts, dedup, now_epoch)

    sent = 0
    skipped_info = 0
    repo_root = Path(__file__).resolve().parent.parent
    if to_fire and not dry_run:
        for a in to_fire:
            # INFO severity: file only, skip Telegram to reduce DEADMAN noise.
            # Jeff 2026-04-24: CRITICAL 도배 방지 → 단기 tick blip 은 파일 보관만.
            if a.severity == SEVERITY_INFO:
                skipped_info += 1
                continue
            ok = send_telegram(a.human())
            if ok:
                sent += 1
            # Mirror to dashboard "Telegram Alerts (24h)" panel feed so
            # deadman alerts are visible without checking incident dir.
            record_dashboard_alert(
                a, repo_root=repo_root,
                send_status="sent" if ok else "failed",
                now_utc=now_utc,
            )
        incident_path = write_incident(to_fire, incident_dir, now_utc=now_utc)
        save_dedup(data_dir, updated_dedup)
    else:
        incident_path = None

    _log.info(
        "[WATCHDOG_PASS] trade_date=%s heartbeat=%s marker=%s "
        "alerts_total=%d to_fire=%d sent=%d skipped_info=%d incident=%s dry_run=%s",
        td, bool(heartbeat), bool(marker), len(alerts), len(to_fire), sent,
        skipped_info, incident_path, dry_run,
    )
    return {
        "trade_date": td.isoformat(),
        "alerts_total": len(alerts),
        "alerts_to_fire": [a.human() for a in to_fire],
        "sent": sent,
        "incident_path": str(incident_path) if incident_path else None,
        "dry_run": dry_run,
    }


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Q-TRON external dead-man watchdog")
    parser.add_argument("--data-dir", default=None,
                        help="Pipeline data dir (default: kr/data/pipeline)")
    parser.add_argument("--ohlcv-dir", default=None,
                        help="OHLCV CSV dir for R5 staleness check "
                             "(default: kr/data/ohlcv). Pass empty string to disable.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Evaluate alerts, print, but do not send/persist")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    if args.verbose:
        logging.getLogger("qtron.watchdog_external").setLevel(logging.DEBUG)

    data_dir = resolve_data_dir(args.data_dir)
    incident_dir = resolve_incident_dir()
    ohlcv_dir: Optional[Path]
    if args.ohlcv_dir == "":
        ohlcv_dir = None  # explicit opt-out
    else:
        ohlcv_dir = resolve_ohlcv_dir(args.ohlcv_dir)
    now = datetime.now(timezone.utc)

    summary = run_once(
        data_dir=data_dir, incident_dir=incident_dir,
        now_utc=now, dry_run=args.dry_run, ohlcv_dir=ohlcv_dir,
    )
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
