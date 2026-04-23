# -*- coding: utf-8 -*-
"""kr/pipeline/completion_marker.py — Canonical run completion truth.

This module owns the single-truth file that all consumers (notifier,
watchdog, dashboard, external .bat) read to decide "did today's runs
succeed?". The existing `state_YYYYMMDD.json` remains as orchestrator
internal; `head.json` remains engine internal. Neither is truth for
run status — this marker is.

Structure:
    kr/data/pipeline/run_completion_YYYYMMDD.json
        { schema_version, trade_date, tz, last_update,
          runs: { KR_BATCH: RunEntry, KR_EOD: ..., ... },
          known_bombs: [...] }

RunEntry:
    status, attempt_no, started_at, finished_at,
    checks{imports_ok, db_upsert_ok, kospi_parse_ok, report_ok,
           head_updated, write_perm_ok},
    artifacts{log_path, report_path, head_last_run_date},
    error{stage, message, trace_ref} | null,
    preflight_fingerprint{...} | null,
    snapshot_version,
    worst_status_today,
    history[]  ← prior attempts preserved (SUCCESS→RUNNING bumps attempt)

Invariants enforced on write:
    I1: status == SUCCESS  →  all checks != false
                               AND error is None
                               AND known_bombs empty (for its critical set)
                               AND finished_at >= started_at
                               AND attempt_no >= 1
    I2: status ∈ TERMINAL   →  finished_at is not None
    I3: status transition   →  (from, to) ∈ ALLOWED_TRANSITIONS
    I4: RUNNING → MISSING   →  FORBIDDEN (cannot un-start)
    I5: SUCCESS → FAILED direct →  FORBIDDEN (must new attempt)

Atomicity:
    Write = tmp file in same dir → fsync → os.replace (Windows-safe).
    A partial crash leaves the prior marker intact.

Concurrency:
    Module-level threading.Lock serializes writes within one process.
    Cross-process: rely on os.replace atomicity + optimistic read
    (consumer reads latest; concurrent writers resolve via replace).
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

from .completion_schema import (
    ALL_RUN_TYPES,
    ALL_STATUSES,
    ALLOWED_TRANSITIONS,
    MARKER_FILENAME_FMT,
    MARKER_SCHEMA_VERSION,
    SEVERITY,
    STATUS_FAILED,
    STATUS_MISSING,
    STATUS_PARTIAL,
    STATUS_PRE_FLIGHT_FAIL,
    STATUS_PRE_FLIGHT_STALE_INPUT,
    STATUS_RUNNING,
    STATUS_SUCCESS,
    TERMINAL_STATUSES,
)

_log = logging.getLogger("gen4.pipeline.completion_marker")
_DT_FMT = "%Y-%m-%dT%H:%M:%S%z"  # ISO with tz (UTC storage, v2 §R9)
_DT_FMT_NAIVE = "%Y-%m-%dT%H:%M:%S"

# Module-level lock — single-process write serialization.
_WRITE_LOCK = threading.Lock()


class MarkerInvariantError(ValueError):
    """Raised when attempting to write a marker that violates invariants I1–I5."""


class ForbiddenTransition(MarkerInvariantError):
    """Raised on illegal status transition (I3/I4/I5)."""


def _iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    # Prefer tz-aware; fall back to naive if needed.
    if dt.tzinfo is not None:
        return dt.strftime(_DT_FMT)
    return dt.strftime(_DT_FMT_NAIVE)


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if s is None:
        return None
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        _log.warning("[MARKER_DT_PARSE] unreadable=%r", s)
        return None


# ---------- Check/fingerprint/artifact sub-structs ----------

@dataclass
class ChecksBlock:
    """Named pre-flight + in-flight check results. True/False/None (unknown).

    R1 (2026-04-23 added): `universe_healthy` — batch-specific check that
    the OHLCV CSV cache is healthy enough to produce a non-empty universe.
    Introduced after the 10-day loop where silent CSV truncation caused
    empty universe → batch returned None → lifecycle break.
    """
    imports_ok: Optional[bool] = None
    db_upsert_ok: Optional[bool] = None
    kospi_parse_ok: Optional[bool] = None
    report_ok: Optional[bool] = None
    head_updated: Optional[bool] = None
    write_perm_ok: Optional[bool] = None
    universe_healthy: Optional[bool] = None

    def to_dict(self) -> dict:
        return {
            "imports_ok": self.imports_ok,
            "db_upsert_ok": self.db_upsert_ok,
            "kospi_parse_ok": self.kospi_parse_ok,
            "report_ok": self.report_ok,
            "head_updated": self.head_updated,
            "write_perm_ok": self.write_perm_ok,
            "universe_healthy": self.universe_healthy,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ChecksBlock":
        d = d or {}
        return cls(
            imports_ok=d.get("imports_ok"),
            db_upsert_ok=d.get("db_upsert_ok"),
            kospi_parse_ok=d.get("kospi_parse_ok"),
            report_ok=d.get("report_ok"),
            head_updated=d.get("head_updated"),
            write_perm_ok=d.get("write_perm_ok"),
            universe_healthy=d.get("universe_healthy"),
        )

    def any_false(self) -> bool:
        """Invariant I1: SUCCESS forbids any explicit False check."""
        for v in (self.imports_ok, self.db_upsert_ok, self.kospi_parse_ok,
                  self.report_ok, self.head_updated, self.write_perm_ok,
                  self.universe_healthy):
            if v is False:
                return True
        return False


@dataclass
class MetricsBlock:
    """R6 (2026-04-24): numeric observability metrics per run.

    ChecksBlock holds True/False/None for gate results; MetricsBlock holds
    integers/floats for trend analysis. `universe_count` is the first
    citizen — daily tracking surfaces silent CSV truncation events (the
    2026-04-23 batch-empty incident would have been visible 10 days
    earlier if this metric had been persisted).

    Add new fields as Optional with a None default to keep
    backward-compat with existing markers on disk.
    """
    universe_count: Optional[int] = None

    def to_dict(self) -> dict:
        return {"universe_count": self.universe_count}

    @classmethod
    def from_dict(cls, d: Optional[dict]) -> "MetricsBlock":
        d = d or {}
        uc = d.get("universe_count")
        try:
            uc_i = int(uc) if uc is not None else None
        except (TypeError, ValueError):
            uc_i = None
        return cls(universe_count=uc_i)


@dataclass
class ArtifactsBlock:
    log_path: Optional[str] = None
    report_path: Optional[str] = None
    head_last_run_date: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "log_path": self.log_path,
            "report_path": self.report_path,
            "head_last_run_date": self.head_last_run_date,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ArtifactsBlock":
        d = d or {}
        return cls(
            log_path=d.get("log_path"),
            report_path=d.get("report_path"),
            head_last_run_date=d.get("head_last_run_date"),
        )


@dataclass
class ErrorBlock:
    stage: str
    message: str
    trace_ref: Optional[str] = None

    def to_dict(self) -> dict:
        return {"stage": self.stage, "message": self.message, "trace_ref": self.trace_ref}

    @classmethod
    def from_dict(cls, d: Optional[dict]) -> Optional["ErrorBlock"]:
        if not d:
            return None
        return cls(
            stage=str(d.get("stage", "")),
            message=str(d.get("message", ""))[:2000],
            trace_ref=d.get("trace_ref"),
        )


@dataclass
class FingerprintBlock:
    """v3 Hardening 2 — captured by preflight, re-verified by EOD start."""
    captured_at: Optional[datetime] = None
    git_head_sha: Optional[str] = None
    db_schema_version: Optional[str] = None
    db_target: Optional[str] = None
    inputs: list[dict] = field(default_factory=list)      # [{path,mtime,size,tail_sha256}]
    code_modules: dict[str, str] = field(default_factory=dict)  # module→sha256

    def to_dict(self) -> dict:
        return {
            "captured_at": _iso(self.captured_at),
            "git_head_sha": self.git_head_sha,
            "db_schema_version": self.db_schema_version,
            "db_target": self.db_target,
            "inputs": list(self.inputs),
            "code_modules": dict(self.code_modules),
        }

    @classmethod
    def from_dict(cls, d: Optional[dict]) -> Optional["FingerprintBlock"]:
        if not d:
            return None
        return cls(
            captured_at=_parse_iso(d.get("captured_at")),
            git_head_sha=d.get("git_head_sha"),
            db_schema_version=d.get("db_schema_version"),
            db_target=d.get("db_target"),
            inputs=list(d.get("inputs") or []),
            code_modules=dict(d.get("code_modules") or {}),
        )


# ---------- RunEntry + history ----------

@dataclass
class RunEntry:
    status: str = STATUS_MISSING
    attempt_no: int = 0
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    last_update: Optional[datetime] = None
    checks: ChecksBlock = field(default_factory=ChecksBlock)
    metrics: "MetricsBlock" = field(default_factory=lambda: MetricsBlock())
    artifacts: ArtifactsBlock = field(default_factory=ArtifactsBlock)
    error: Optional[ErrorBlock] = None
    preflight_fingerprint: Optional[FingerprintBlock] = None
    snapshot_version: Optional[str] = None
    worst_status_today: str = STATUS_MISSING
    history: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "attempt_no": int(self.attempt_no),
            "started_at": _iso(self.started_at),
            "finished_at": _iso(self.finished_at),
            "last_update": _iso(self.last_update),
            "checks": self.checks.to_dict(),
            "metrics": self.metrics.to_dict(),
            "artifacts": self.artifacts.to_dict(),
            "error": self.error.to_dict() if self.error else None,
            "preflight_fingerprint": (
                self.preflight_fingerprint.to_dict()
                if self.preflight_fingerprint else None
            ),
            "snapshot_version": self.snapshot_version,
            "worst_status_today": self.worst_status_today,
            "history": list(self.history),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "RunEntry":
        d = d or {}
        status = d.get("status", STATUS_MISSING)
        if status not in ALL_STATUSES:
            _log.warning("[MARKER_UNKNOWN_STATUS] %r → MISSING", status)
            status = STATUS_MISSING
        return cls(
            status=status,
            attempt_no=int(d.get("attempt_no") or 0),
            started_at=_parse_iso(d.get("started_at")),
            finished_at=_parse_iso(d.get("finished_at")),
            last_update=_parse_iso(d.get("last_update")),
            checks=ChecksBlock.from_dict(d.get("checks") or {}),
            metrics=MetricsBlock.from_dict(d.get("metrics")),
            artifacts=ArtifactsBlock.from_dict(d.get("artifacts") or {}),
            error=ErrorBlock.from_dict(d.get("error")),
            preflight_fingerprint=FingerprintBlock.from_dict(d.get("preflight_fingerprint")),
            snapshot_version=d.get("snapshot_version"),
            worst_status_today=d.get("worst_status_today") or status,
            history=list(d.get("history") or []),
        )


# ---------- Marker container + I/O ----------

class CompletionMarker:
    """Canonical truth container for one trade_date."""

    def __init__(
        self,
        *,
        trade_date: date,
        tz: str,
        data_dir: Path,
        runs: Optional[dict[str, RunEntry]] = None,
        last_update: Optional[datetime] = None,
        known_bombs: Optional[list[dict]] = None,
        schema_version: int = MARKER_SCHEMA_VERSION,
        clock: Any = None,
    ):
        self.trade_date = trade_date
        self.tz = tz
        self.data_dir = Path(data_dir)
        self.runs: dict[str, RunEntry] = dict(runs or {})
        self._clock = clock or datetime.now
        self.last_update = last_update or self._clock()
        self.known_bombs = list(known_bombs or [])
        self.schema_version = schema_version

    # ---------- load / create ----------

    @classmethod
    def load_or_create_today(
        cls,
        *,
        data_dir: Path,
        tz: str = "Asia/Seoul",
        trade_date: Optional[date] = None,
        clock: Any = None,
    ) -> "CompletionMarker":
        clock = clock or datetime.now
        td = trade_date or clock().date()
        path = cls._path_for(data_dir, td)
        if path.exists():
            m = cls._load_path(path, data_dir=data_dir, clock=clock)
            if m is not None:
                return m
            _log.warning("[MARKER_LOAD_FALLBACK] %s unreadable; creating fresh", path)
        return cls(
            trade_date=td,
            tz=tz,
            data_dir=data_dir,
            runs={},
            last_update=clock(),
            clock=clock,
        )

    @classmethod
    def load_date(cls, trade_date: date, *, data_dir: Path) -> Optional["CompletionMarker"]:
        """Read-only historical loader (watchdog/dashboard)."""
        path = cls._path_for(data_dir, trade_date)
        if not path.exists():
            return None
        return cls._load_path(path, data_dir=data_dir)

    @classmethod
    def _load_path(
        cls, path: Path, *, data_dir: Path, clock: Any = None,
    ) -> Optional["CompletionMarker"]:
        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            _log.error("[MARKER_READ_FAIL] %s: %s", path, e)
            return None

        sv = int(data.get("schema_version") or 0)
        if sv != MARKER_SCHEMA_VERSION:
            raise ValueError(
                f"marker schema_version={sv} unsupported "
                f"(expected {MARKER_SCHEMA_VERSION}) at {path}"
            )

        td = datetime.strptime(data["trade_date"], "%Y-%m-%d").date()
        runs_raw = data.get("runs") or {}
        runs = {name: RunEntry.from_dict(r) for name, r in runs_raw.items()}
        return cls(
            trade_date=td,
            tz=data.get("tz", "Asia/Seoul"),
            data_dir=data_dir,
            runs=runs,
            last_update=_parse_iso(data.get("last_update")),
            known_bombs=list(data.get("known_bombs") or []),
            schema_version=sv,
            clock=clock,
        )

    # ---------- read API (for consumers — via MarkerReader below) ----------

    def run(self, run_type: str) -> RunEntry:
        """Get-or-create RunEntry (MISSING by default)."""
        if run_type not in ALL_RUN_TYPES:
            raise ValueError(f"unknown run_type: {run_type!r}")
        if run_type not in self.runs:
            self.runs[run_type] = RunEntry()
        return self.runs[run_type]

    # ---------- state machine mutators (producer-side) ----------

    def transition(
        self,
        run_type: str,
        to_status: str,
        *,
        error: Optional[ErrorBlock] = None,
        checks: Optional[ChecksBlock] = None,
        metrics: Optional["MetricsBlock"] = None,
        artifacts: Optional[ArtifactsBlock] = None,
        snapshot_version: Optional[str] = None,
        fingerprint: Optional[FingerprintBlock] = None,
        details: Optional[dict] = None,
    ) -> RunEntry:
        """Enforce transition table (I3/I4/I5) + apply updates atomically.

        Transactional: if any invariant fails, the entry is rolled back to
        its prior state so callers can retry a different transition.
        """
        if to_status not in ALL_STATUSES:
            raise MarkerInvariantError(f"unknown status: {to_status!r}")

        entry = self.run(run_type)
        from_status = entry.status
        key = (from_status, to_status)

        if from_status == to_status:
            raise ForbiddenTransition(
                f"no-op transition: {run_type} {from_status} → {to_status}"
            )

        if key not in ALLOWED_TRANSITIONS:
            raise ForbiddenTransition(
                f"illegal transition: {run_type} {from_status} → {to_status}"
            )

        transition_kind = ALLOWED_TRANSITIONS[key]
        now = self._clock()
        prev_last_update = self.last_update

        # Snapshot mutable fields for rollback on invariant failure.
        rollback = _capture_mutable(entry)

        try:
            # Handle SUCCESS → RUNNING: new attempt, archive prev snapshot.
            if transition_kind == "new_attempt":
                prev_snapshot = _snapshot_for_history(entry)
                entry.history = list(entry.history) + [prev_snapshot]
                entry.attempt_no += 1
                entry.started_at = now
                entry.finished_at = None
                entry.checks = ChecksBlock()
                entry.artifacts = ArtifactsBlock()
                entry.error = None
                entry.preflight_fingerprint = None
                entry.snapshot_version = None
            elif transition_kind == "fresh":
                entry.attempt_no = 1
                entry.started_at = now
                entry.finished_at = None
            # else "same_attempt" — keep started_at, bump internal state only

            # Terminal states: close out finished_at, attach artifacts/error.
            if to_status in TERMINAL_STATUSES:
                entry.finished_at = now
                if error is not None:
                    entry.error = error
                if to_status == STATUS_SUCCESS:
                    # I1: error must be None on SUCCESS.
                    entry.error = None

            # Apply any-state updates.
            if checks is not None:
                entry.checks = checks
            if metrics is not None:
                entry.metrics = metrics
            if artifacts is not None:
                entry.artifacts = artifacts
            if fingerprint is not None:
                entry.preflight_fingerprint = fingerprint
            if snapshot_version is not None:
                entry.snapshot_version = snapshot_version

            entry.status = to_status
            entry.last_update = now
            entry.worst_status_today = _worst(entry.worst_status_today, to_status)

            _validate_invariants(entry, run_type, known_bombs=self.known_bombs)
        except MarkerInvariantError:
            _restore_mutable(entry, rollback)
            self.last_update = prev_last_update
            raise

        self.last_update = now
        return entry

    def record_heartbeat(self) -> None:
        """Touch last_update without changing any run status."""
        self.last_update = self._clock()

    def set_attrs(
        self,
        run_type: str,
        *,
        checks: Optional["ChecksBlock"] = None,
        metrics: Optional["MetricsBlock"] = None,
        artifacts: Optional["ArtifactsBlock"] = None,
        fingerprint: Optional["FingerprintBlock"] = None,
        snapshot_version: Optional[str] = None,
    ) -> "RunEntry":
        """Non-transitional attribute update. Used by preflight to save
        fingerprint/checks *without* changing status (e.g. saving fingerprint
        after a successful preflight while the run is in RUNNING state).

        Invariants are re-validated after update.
        """
        if run_type not in ALL_RUN_TYPES:
            raise ValueError(f"unknown run_type: {run_type!r}")
        entry = self.run(run_type)
        rollback = _capture_mutable(entry)
        try:
            if checks is not None:
                entry.checks = checks
            if metrics is not None:
                entry.metrics = metrics
            if artifacts is not None:
                entry.artifacts = artifacts
            if fingerprint is not None:
                entry.preflight_fingerprint = fingerprint
            if snapshot_version is not None:
                entry.snapshot_version = snapshot_version
            now = self._clock()
            entry.last_update = now
            _validate_invariants(entry, run_type, known_bombs=self.known_bombs)
            self.last_update = now
        except MarkerInvariantError:
            _restore_mutable(entry, rollback)
            raise
        return entry

    def register_known_bomb(self, module: str, state: str, detected_since: str,
                            recovery_ref: Optional[str] = None) -> None:
        """Register a .pyc-only / missing-source bomb (v3 §Hardening-4)."""
        for b in self.known_bombs:
            if b.get("module") == module:
                b["state"] = state
                b["detected_since"] = detected_since
                if recovery_ref:
                    b["recovery_ref"] = recovery_ref
                return
        self.known_bombs.append({
            "module": module,
            "state": state,
            "detected_since": detected_since,
            "recovery_ref": recovery_ref,
        })

    # ---------- serialization ----------

    def to_dict(self) -> dict:
        return {
            "schema_version": self.schema_version,
            "trade_date": self.trade_date.strftime("%Y-%m-%d"),
            "tz": self.tz,
            "last_update": _iso(self.last_update),
            "runs": {name: r.to_dict() for name, r in self.runs.items()},
            "known_bombs": list(self.known_bombs),
        }

    def save(self) -> None:
        """Atomic write: tmp in same dir → fsync → os.replace."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        path = self._path_for(self.data_dir, self.trade_date)
        payload = json.dumps(self.to_dict(), ensure_ascii=False, indent=2)

        with _WRITE_LOCK:
            fd, tmp_name = tempfile.mkstemp(
                prefix=f".{path.name}.",
                suffix=".tmp",
                dir=str(self.data_dir),
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    f.write(payload)
                    f.flush()
                    try:
                        os.fsync(f.fileno())
                    except OSError:
                        pass  # Windows FS may refuse fsync on some volumes
                os.replace(tmp_name, path)
            except Exception:
                try:
                    os.unlink(tmp_name)
                except OSError:
                    pass
                raise

    # ---------- path helpers ----------

    @staticmethod
    def _path_for(data_dir: Path, trade_date: date) -> Path:
        name = MARKER_FILENAME_FMT.format(yyyymmdd=trade_date.strftime("%Y%m%d"))
        return Path(data_dir) / name


# ---------- helpers ----------

def _worst(current: str, incoming: str) -> str:
    """worst_status_today rule: keep max-severity status seen this trade_date."""
    cur = SEVERITY.get(current, 0)
    inc = SEVERITY.get(incoming, 0)
    return incoming if inc > cur else current


def _capture_mutable(entry: RunEntry) -> dict:
    """Capture all mutable fields for rollback on invariant failure."""
    return {
        "status": entry.status,
        "attempt_no": entry.attempt_no,
        "started_at": entry.started_at,
        "finished_at": entry.finished_at,
        "last_update": entry.last_update,
        "checks": ChecksBlock(**entry.checks.to_dict()),
        "metrics": MetricsBlock.from_dict(entry.metrics.to_dict()),
        "artifacts": ArtifactsBlock(**entry.artifacts.to_dict()),
        "error": (ErrorBlock(**entry.error.to_dict())
                  if entry.error else None),
        "preflight_fingerprint": (
            FingerprintBlock.from_dict(entry.preflight_fingerprint.to_dict())
            if entry.preflight_fingerprint else None
        ),
        "snapshot_version": entry.snapshot_version,
        "worst_status_today": entry.worst_status_today,
        "history": list(entry.history),
    }


def _restore_mutable(entry: RunEntry, snap: dict) -> None:
    """Restore fields captured by _capture_mutable."""
    for k, v in snap.items():
        setattr(entry, k, v)


def _snapshot_for_history(entry: RunEntry) -> dict:
    """Capture prior attempt's full state before new_attempt transition."""
    return {
        "attempt_no": entry.attempt_no,
        "status": entry.status,
        "started_at": _iso(entry.started_at),
        "finished_at": _iso(entry.finished_at),
        "checks": entry.checks.to_dict(),
        "artifacts": entry.artifacts.to_dict(),
        "error": entry.error.to_dict() if entry.error else None,
        "snapshot_version": entry.snapshot_version,
    }


def _validate_invariants(entry: RunEntry, run_type: str, *,
                         known_bombs: list[dict]) -> None:
    """Enforce I1 + I2 on a RunEntry post-mutation.

    Raises MarkerInvariantError with specific code on violation.
    """
    # I2: terminal → finished_at set
    if entry.status in TERMINAL_STATUSES and entry.finished_at is None:
        raise MarkerInvariantError(
            f"I2_VIOLATION: {run_type} status={entry.status} but finished_at is None"
        )

    # I1: SUCCESS constraints
    if entry.status == STATUS_SUCCESS:
        if entry.checks.any_false():
            raise MarkerInvariantError(
                f"I1_VIOLATION: {run_type} SUCCESS with checks.any_false()"
            )
        if entry.error is not None:
            raise MarkerInvariantError(
                f"I1_VIOLATION: {run_type} SUCCESS with error set"
            )
        if entry.attempt_no < 1:
            raise MarkerInvariantError(
                f"I1_VIOLATION: {run_type} SUCCESS with attempt_no={entry.attempt_no}"
            )
        if (entry.started_at is not None and entry.finished_at is not None
                and entry.finished_at < entry.started_at):
            raise MarkerInvariantError(
                f"I1_VIOLATION: {run_type} SUCCESS finished_at < started_at"
            )
        # v3 §Hardening-4: any un-restored .pyc bomb forbids SUCCESS globally.
        # (Scoped to per-run via critical set match in future; conservative now.)
        active_bombs = [b for b in known_bombs if b.get("state") == "PYC_ONLY"]
        if active_bombs:
            raise MarkerInvariantError(
                f"I1_VIOLATION: {run_type} SUCCESS forbidden while "
                f"known_bombs active: {[b['module'] for b in active_bombs]}"
            )


# ---------- Consumer-facing read-only facade ----------

class MarkerReader:
    """The ONLY legitimate path for consumers to read completion truth.

    truth_guard (shared/util/truth_guard.py) additionally blocks legacy
    reads of state_YYYYMMDD.json / head.json / *.log from consumer stacks.
    Together they enforce marker-only consumption.
    """

    def __init__(self, data_dir: Path):
        self._data_dir = Path(data_dir)

    def today(self, *, tz: str = "Asia/Seoul",
              clock: Any = None) -> CompletionMarker:
        """Load today's marker or synthesize an empty MISSING marker."""
        return CompletionMarker.load_or_create_today(
            data_dir=self._data_dir, tz=tz, clock=clock,
        )

    def date(self, trade_date: date) -> Optional[CompletionMarker]:
        return CompletionMarker.load_date(trade_date, data_dir=self._data_dir)

    def run_status(self, run_type: str, *,
                   clock: Any = None) -> str:
        """Quick accessor — returns current status (or MISSING)."""
        m = self.today(clock=clock)
        return m.run(run_type).status

    def worst_today(self, run_type: str, *,
                    clock: Any = None) -> str:
        """Quick accessor — worst status seen today (for alert escalation)."""
        m = self.today(clock=clock)
        return m.run(run_type).worst_status_today
