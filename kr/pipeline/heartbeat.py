"""kr/pipeline/heartbeat.py — Tray liveness signal for external watchdog.

Writes a small JSON file every orchestrator tick so an external watchdog
(Windows Task Scheduler invoking scripts/watchdog_external.py) can detect
"tray is alive but no marker writes happening" vs "tray is dead".

Dual-write (v4 권장 3):
    heartbeat.json       ← primary, atomic tmp + os.replace
    heartbeat.bak.json   ← secondary, copied after primary replace

External watchdog read order: primary → bak fallback. This defends against:
    - primary file corruption mid-read (reader locks during write? not on Windows)
    - disk full / permission glitch on primary (bak may still succeed)
    - replace-in-flight race (reader catches the split-second between
      primary update and bak update — bak is older but coherent)

Schema:
    { "ts": "2026-04-22T16:43:12+09:00",  # clock-aware ISO
      "pid": 21728,                        # current tray process
      "tray_session": "d7e935de...",       # session uuid (opaque)
      "tick_seq": 42 }                     # monotonic tick counter

Design note: we deliberately DO NOT use completion_marker.save() for this.
Marker writes are expensive-ish (full JSON + invariants). Heartbeat is
hot-path — 30s cadence across 24h = ~2880 writes/day. Keep it tiny.

History note (2026-04-30):
    Recovered from ``.pyc`` after the source went missing alongside
    ``completion_schema.py`` and ``step_run_type_registry.py``. Reconstructed
    from bytecode disassembly + co_consts type-hint tuples + the live
    ``heartbeat.json`` schema. Symbol parity verified against the cached
    .pyc.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from .completion_schema import HEARTBEAT_BAK_FILENAME, HEARTBEAT_FILENAME

_log = logging.getLogger("gen4.pipeline.heartbeat")

# Single global write lock — tray runs orchestrator ticks serially, but
# the lock is cheap and protects against accidental concurrent writes
# from background subprocesses that may try to refresh the file.
_WRITE_LOCK = threading.Lock()


def _iso_tz(dt: datetime) -> str:
    """Serialize tz-aware datetime. If naive, assume local and attach tzinfo."""
    if dt.tzinfo is None:
        dt = dt.astimezone()
    return dt.isoformat(timespec="seconds")


@dataclass
class HeartbeatRecord:
    ts: datetime
    pid: int
    tray_session: str
    tick_seq: int

    def to_dict(self) -> dict:
        return {
            "ts":           _iso_tz(self.ts),
            "pid":          int(self.pid),
            "tray_session": str(self.tray_session),
            "tick_seq":     int(self.tick_seq),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "HeartbeatRecord":
        ts_raw = d.get("ts")
        try:
            ts = datetime.fromisoformat(ts_raw) if ts_raw else datetime.now(timezone.utc)
        except (TypeError, ValueError):
            ts = datetime.now(timezone.utc)
        return cls(
            ts=ts,
            pid=int(d.get("pid") or 0),
            tray_session=str(d.get("tray_session") or ""),
            tick_seq=int(d.get("tick_seq") or 0),
        )


class HeartbeatWriter:
    """Owns the tray-alive signal. One instance per tray process."""

    def __init__(self, data_dir: Path,
                 tray_session: Optional[str] = None,
                 clock: Any = None):
        self._data_dir = Path(data_dir)
        # Generate a stable session id if caller didn't pass one.
        # First 16 chars of a uuid4 hex is enough to disambiguate a
        # day's worth of restarts without burdening the JSON.
        self._tray_session = tray_session or uuid.uuid4().hex[:16]
        # Caller can inject a frozen clock for tests; default = UTC now.
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._tick_seq = 0
        self._pid = os.getpid()

    @property
    def tray_session(self) -> str:
        return self._tray_session

    @property
    def tick_seq(self) -> int:
        return self._tick_seq

    def beat(self) -> HeartbeatRecord:
        """Write one heartbeat tick. Increments tick_seq and returns record."""
        with _WRITE_LOCK:
            self._tick_seq += 1
            record = HeartbeatRecord(
                ts=self._clock(),
                pid=self._pid,
                tray_session=self._tray_session,
                tick_seq=self._tick_seq,
            )
            self._write_primary_and_bak(record)
            return record

    def _write_primary_and_bak(self, record: HeartbeatRecord) -> None:
        self._data_dir.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(record.to_dict(), ensure_ascii=False)

        primary = self._data_dir / HEARTBEAT_FILENAME
        bak     = self._data_dir / HEARTBEAT_BAK_FILENAME

        # Atomic primary write: tmp + os.replace.
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{HEARTBEAT_FILENAME}.",
            suffix=".tmp",
            dir=str(self._data_dir),
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_name, primary)
        except OSError:
            # Failed mid-write — clean up the tmp and re-raise so caller
            # surfaces the failure.
            try:
                os.unlink(tmp_name)
            except (OSError, NameError):
                pass
            raise

        # Best-effort bak copy. A bak failure must not abort the tick —
        # the primary already landed and the watchdog's primary read
        # path will succeed on the next read.
        try:
            with open(bak, "w", encoding="utf-8") as f:
                f.write(payload)
        except OSError as e:
            _log.warning("[HEARTBEAT_BAK_WRITE_FAIL] %s", e)


def read_heartbeat(data_dir: Path) -> Optional[HeartbeatRecord]:
    """External-watchdog read API: primary → bak fallback.

    Returns None if neither file is readable.
    """
    data_dir = Path(data_dir)
    for name in (HEARTBEAT_FILENAME, HEARTBEAT_BAK_FILENAME):
        path = data_dir / name
        try:
            with open(path, "r", encoding="utf-8") as f:
                return HeartbeatRecord.from_dict(json.load(f))
        except (OSError, json.JSONDecodeError) as e:
            _log.debug("[HEARTBEAT_READ_FAIL] %s: %s", path, e)
            continue
    return None


def heartbeat_age_seconds(record: Optional[HeartbeatRecord],
                          now: Optional[datetime] = None) -> Optional[float]:
    """Seconds since last heartbeat. None if no record.

    Returns a non-negative float. If heartbeat timestamp is in the future
    (clock skew), returns 0.
    """
    if record is None:
        return None
    now = now if now is not None else datetime.now(timezone.utc)
    ts = record.ts
    if ts.tzinfo is None:
        ts = ts.astimezone(timezone.utc)
    if now.tzinfo is None:
        now = now.astimezone(timezone.utc)
    return max((now - ts).total_seconds(), 0.0)
