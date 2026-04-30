"""kr/lifecycle/health_probe.py — Item 4 (2026-04-30 RCA).

Periodic state + OHLCV health probe.

Why
---
The startup ``state_canary`` only fires once per process start. The
2026-04-30 incident's deletion event happened between the prior day's
clean shutdown (15:36) and the next morning's startup (08:30) — there
was no probe in the intervening 17 hours, and the missing files were
not detected until the 16:05 KR_BATCH preflight tried to enumerate the
OHLCV directory.

This module runs the same integrity checks every ~1 hour (configurable)
inside a daemon thread so a *running* engine notices a deletion the
moment it happens, not at the next batch attempt.

Behavior
--------
``HealthProbe.check_once()`` runs four checks plus two diagnostics:

  state files       — portfolio_state, runtime_state (existence + parse)
  OHLCV directory   — existence + CSV count >= MIN_CSV_COUNT
  KOSPI index CSV   — existence + non-trivial size
  KOSPI freshness   — last row date within MAX_KOSPI_STALE_CALENDAR_DAYS
  STATE_DIR write   — write a tiny tempfile + delete it (filesystem perm)

On any check FAIL:
  * Write a forensic markdown report (similar to state_canary).
  * Send Telegram CRITICAL via ``notify.telegram_bot.send``.
  * Persist a dedup state file so the same failure doesn't re-fire
    every probe interval — only once per ``DEDUP_REFIRE_SEC`` (6h,
    matching the watchdog convention).

The probe never blocks startup, never modifies state, never raises.
``start()`` returns immediately; ``stop()`` is best-effort and idempotent.
"""
from __future__ import annotations

import csv
import json
import logging
import os
import tempfile
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, List, Optional, Tuple

# Reuse canary's check primitives so the probe and the startup canary
# never drift in their integrity definitions.
from .state_canary import _check_state_file, _check_ohlcv_dir, _check_kospi_csv

logger = logging.getLogger("gen4.health_probe")

DEFAULT_INTERVAL_SEC = 3600                    # 1 hour
DEDUP_REFIRE_SEC = 6 * 3600                    # 6h, mirrors watchdog
MAX_KOSPI_STALE_CALENDAR_DAYS = 10             # ~7bd worst case (long weekend)


# ── Diagnostic checks (probe-only, beyond canary) ────────────────────


def _check_state_dir_writable(state_dir: Path) -> Tuple[bool, str]:
    """Confirm state dir is currently writable (catches read-only bind
    mounts, AV lock-out, full disk).
    """
    if not state_dir.exists():
        return False, f"state_dir missing: {state_dir}"
    try:
        fd, tmp_path = tempfile.mkstemp(prefix=".health_probe_", dir=str(state_dir))
        os.close(fd)
        os.unlink(tmp_path)
    except OSError as e:
        return False, f"write failed: {e!r}"
    return True, "ok writable"


def _check_kospi_freshness(
    kospi_path: Path,
    *, max_calendar_days: int = MAX_KOSPI_STALE_CALENDAR_DAYS,
) -> Tuple[bool, str]:
    """Last KOSPI row date should be within `max_calendar_days` of today."""
    if not kospi_path.exists():
        return False, "KOSPI.csv missing"
    try:
        # Read the tail of the file to find the last data row without
        # loading the whole CSV.
        with open(kospi_path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - 4096))
            tail = f.read().decode("utf-8", errors="ignore")
        rows = [r for r in tail.splitlines() if r.strip()]
        if not rows:
            return False, "no rows in tail"
        last_row = rows[-1]
        # Try to parse first column as ISO date.
        first_col = last_row.split(",")[0].strip().strip('"').split(" ")[0]
        try:
            from datetime import date as _date
            last_date = _date.fromisoformat(first_col)
        except (ValueError, TypeError):
            return False, f"unparseable date: {first_col!r}"
        delta = (datetime.now().date() - last_date).days
        if delta < 0:
            return False, f"last_date {last_date} is in the future"
        if delta > max_calendar_days:
            return False, (
                f"stale: last_date={last_date} delta={delta}d > "
                f"{max_calendar_days}d"
            )
        return True, f"ok last_date={last_date} delta={delta}d"
    except OSError as e:
        return False, f"read failed: {e!r}"


# ── Probe ────────────────────────────────────────────────────────────


class HealthProbe:
    """Daemon-thread periodic health probe.

    Construct then call ``start()``. ``stop()`` ends the loop. ``check_once()``
    is the synchronous one-shot version, useful for tests and manual probes.

    The probe is intentionally narrow — it does not attempt recovery, it
    only observes and alerts. Recovery is the job of Item 2 (preflight
    auto-recover), Item 3 (orchestrator unfreeze), or the operator.
    """

    def __init__(
        self,
        config: Any,
        *,
        interval_sec: int = DEFAULT_INTERVAL_SEC,
        telegram_send: Optional[Callable[[str, str], Any]] = None,
        clock: Optional[Callable[[], datetime]] = None,
    ):
        self.config = config
        self.interval_sec = max(60, int(interval_sec))
        self._tg_send = telegram_send  # None = lazy import notify.telegram_bot
        self._clock = clock or datetime.now
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        # Resolve canonical paths once at init so a runtime BASE_DIR
        # change does not race with the loop.
        base_dir = Path(getattr(config, "BASE_DIR", Path.cwd()))
        self._base_dir = base_dir
        self._repo_root = base_dir.parent if base_dir.name == "kr" else base_dir
        self._state_dir = base_dir / "state"
        self._ohlcv_dir = self._repo_root / "backtest" / "data_full" / "ohlcv"
        self._kospi_csv = self._repo_root / "backtest" / "data_full" / "index" / "KOSPI.csv"

        # Dedup file lives next to pipeline data (alongside watchdog state).
        self._dedup_path = (
            self._base_dir / "data" / "pipeline" / "health_probe_state.json"
        )

    # ── Public API ───────────────────────────────────────────────────

    def start(self) -> None:
        """Spawn the daemon thread. Idempotent."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop, name="health_probe", daemon=True,
        )
        self._thread.start()
        logger.info(
            f"[HEALTH_PROBE_START] interval={self.interval_sec}s "
            f"state_dir={self._state_dir} ohlcv={self._ohlcv_dir}"
        )

    def stop(self, *, join_timeout_sec: float = 5.0) -> None:
        """Signal the loop to stop. Best-effort join. Idempotent."""
        self._stop_event.set()
        t = self._thread
        if t is not None and t.is_alive():
            t.join(timeout=join_timeout_sec)

    def check_once(self) -> dict:
        """Run all checks once. Fires alerts on new failures. Returns a
        dict summary suitable for logging / tests.
        """
        ts = self._clock()
        # Live mode files — paper / shadow_test have their own.
        # 04-30 incident was on live; extend later if needed.
        portfolio_path = self._state_dir / "portfolio_state_live.json"
        runtime_path = self._state_dir / "runtime_state_live.json"

        results: List[Tuple[str, bool, str]] = []
        for label, ok, detail in [
            ("portfolio_state_live.json", *_check_state_file(portfolio_path)),
            ("runtime_state_live.json", *_check_state_file(runtime_path)),
            ("backtest/data_full/ohlcv", *_check_ohlcv_dir(self._ohlcv_dir)),
            ("backtest/data_full/index/KOSPI.csv", *_check_kospi_csv(self._kospi_csv)),
            ("KOSPI freshness", *_check_kospi_freshness(self._kospi_csv)),
            ("state_dir writable", *_check_state_dir_writable(self._state_dir)),
        ]:
            results.append((label, ok, detail))

        failures = [(n, d) for n, ok, d in results if not ok]
        passes = [(n, d) for n, ok, d in results if ok]

        if not failures:
            logger.info(
                f"[HEALTH_PROBE_OK] {ts.isoformat()} all={len(results)} "
                f"failures=0"
            )
            # Clear dedup so a future failure of the same kind re-fires.
            self._clear_dedup()
            return {"ok": True, "ts": ts.isoformat(),
                    "passes": passes, "failures": []}

        # Failure path
        logger.critical(
            f"[HEALTH_PROBE_FAIL] {ts.isoformat()} failures={len(failures)} "
            + "; ".join(f"{n}={d}" for n, d in failures)
        )

        new_failures = self._filter_dedup(failures, ts)
        snapshot_path: Optional[Path] = None
        if new_failures:
            snapshot_path = self._write_forensic_snapshot(failures, ts)
            self._send_telegram(new_failures, snapshot_path, ts)
            self._mark_dedup(new_failures, ts)
        else:
            logger.warning(
                f"[HEALTH_PROBE_FAIL_DEDUPED] {len(failures)} failures all "
                f"within {DEDUP_REFIRE_SEC}s window — alert suppressed"
            )

        return {
            "ok": False,
            "ts": ts.isoformat(),
            "failures": failures,
            "new_failures": new_failures,
            "snapshot": str(snapshot_path) if snapshot_path else None,
            "passes": passes,
        }

    # ── Loop ─────────────────────────────────────────────────────────

    def _loop(self) -> None:
        # Run once immediately so a launch right after a deletion
        # doesn't wait an entire interval to notice.
        try:
            self.check_once()
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[HEALTH_PROBE_LOOP_CRASH] {e!r}")
        # Subsequent runs gated by stop_event.wait — returns True if
        # stop was requested, False on timeout (continue).
        while not self._stop_event.wait(self.interval_sec):
            try:
                self.check_once()
            except Exception as e:  # noqa: BLE001
                logger.warning(f"[HEALTH_PROBE_LOOP_CRASH] {e!r}")
        logger.info("[HEALTH_PROBE_STOP] loop exiting")

    # ── Dedup / forensics / Telegram ─────────────────────────────────

    def _load_dedup(self) -> dict:
        if not self._dedup_path.exists():
            return {}
        try:
            return json.loads(self._dedup_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}

    def _save_dedup(self, payload: dict) -> None:
        try:
            self._dedup_path.parent.mkdir(parents=True, exist_ok=True)
            self._dedup_path.write_text(
                json.dumps(payload, indent=2, default=str),
                encoding="utf-8",
            )
        except OSError as e:
            logger.warning(f"[HEALTH_PROBE_DEDUP_WRITE_FAIL] {e!r}")

    def _filter_dedup(
        self, failures: List[Tuple[str, str]], now: datetime,
    ) -> List[Tuple[str, str]]:
        """Return failures whose dedup window has expired (or first sighting)."""
        prev = self._load_dedup()
        new = []
        for name, detail in failures:
            last_alert = prev.get(name, {}).get("last_alert_at")
            if last_alert is None:
                new.append((name, detail))
                continue
            try:
                last_dt = datetime.fromisoformat(last_alert)
            except (ValueError, TypeError):
                new.append((name, detail))
                continue
            if (now - last_dt) >= timedelta(seconds=DEDUP_REFIRE_SEC):
                new.append((name, detail))
        return new

    def _mark_dedup(
        self, alerted: List[Tuple[str, str]], now: datetime,
    ) -> None:
        state = self._load_dedup()
        for name, detail in alerted:
            state[name] = {
                "last_alert_at": now.isoformat(),
                "last_detail": detail,
            }
        self._save_dedup(state)

    def _clear_dedup(self) -> None:
        if self._dedup_path.exists():
            try:
                self._dedup_path.unlink()
            except OSError:
                pass

    def _write_forensic_snapshot(
        self, failures: List[Tuple[str, str]], now: datetime,
    ) -> Optional[Path]:
        try:
            ts = now.strftime("%Y%m%d_%H%M%S")
            incidents_dir = self._repo_root / "backup" / "reports" / "incidents"
            incidents_dir.mkdir(parents=True, exist_ok=True)
            path = incidents_dir / f"{ts}_health_probe_fail.md"
            lines = [
                f"# Health probe FAIL — {now.isoformat()}",
                "",
                "Auto-generated by `kr/lifecycle/health_probe.py` (Item 4).",
                "Periodic probe (≥1h interval) detected one or more "
                "integrity failures while the engine was running.",
                "",
                "## Failures",
                "",
            ]
            for name, detail in failures:
                lines.append(f"- **{name}**: {detail}")
            lines.append("")
            lines.append("## Action")
            lines.append("")
            lines.append(
                "Compare against the latest state_canary snapshot — "
                "if both fired within minutes, the deletion event is "
                "in-flight. Run `scripts/restore_ohlcv_from_db.py` for "
                "OHLCV gaps and inspect off-disk mirrors for state "
                "files."
            )
            path.write_text("\n".join(lines), encoding="utf-8")
            return path
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[HEALTH_PROBE_SNAPSHOT_FAIL] {e!r}")
            return None

    def _send_telegram(
        self, failures: List[Tuple[str, str]],
        snapshot_path: Optional[Path], now: datetime,
    ) -> None:
        try:
            if self._tg_send is not None:
                send = self._tg_send
            else:
                from notify import telegram_bot as _tg  # noqa: WPS433
                send = _tg.send
            bullets = "\n".join(f"• <b>{n}</b>: {d}" for n, d in failures)
            snap = (f"\nsnapshot: {snapshot_path.name}"
                    if snapshot_path is not None else "")
            text = (
                f"<b>HEALTH_PROBE_FAIL</b> 🚨\n"
                f"ts: {now.isoformat(timespec='seconds')}\n"
                f"{bullets}{snap}\n"
                f"→ 즉시 운영자 점검 필요"
            )
            send(text, "CRITICAL")
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[HEALTH_PROBE_TELEGRAM_FAIL] {e!r}")
