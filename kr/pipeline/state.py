# -*- coding: utf-8 -*-
"""kr/pipeline/state.py — Pipeline daily state with atomic JSON I/O.

Single source of truth for "today's pipeline completion status". Replaces
the 4–5-way state fragmentation called out as R-1 in the design doc:
    - tray._batch_today_done (memory)
    - tray._batch_last_done_date (memory + file)
    - head.json.last_run_date
    - FastAPI /api/rebalance/status
    - runtime_state_live.json.timestamp / _write_ts

Only one file per trade_date: `kr/data/pipeline/state_YYYYMMDD.json`.
Atomic writes via tmp + os.replace so a crash during write cannot leave
a half-written file for the next tick to read.

Per Jeff-approved open issue #5: "yesterday abandoned → today catch-up"
is NOT allowed. `load_or_create_today` only ever creates/loads TODAY.
`load_date` is read-only for history/reporting callers (e.g. advisor).
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

from .schema import (
    ALL_STATUSES,
    SCHEMA_VERSION,
    STATE_FILENAME_FMT,
    STATUS_DONE,
    STATUS_FAILED,
    STATUS_NOT_STARTED,
    STATUS_PENDING,
    STATUS_SKIPPED,
    TERMINAL_STATUSES,
)

_log = logging.getLogger("gen4.pipeline.state")

_DT_FMT = "%Y-%m-%dT%H:%M:%S"


def _iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    return dt.strftime(_DT_FMT)


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if s is None:
        return None
    try:
        return datetime.strptime(s, _DT_FMT)
    except ValueError:
        # Tolerate fractional seconds / timezone suffix from future schemas
        try:
            return datetime.fromisoformat(s)
        except ValueError:
            _log.warning("[PIPELINE_STATE_DT_PARSE] unreadable=%r", s)
            return None


@dataclass
class StepState:
    """State for a single pipeline step on a given trade_date."""
    status: str = STATUS_NOT_STARTED
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    fail_count: int = 0
    last_error: Optional[str] = None
    last_failed_at: Optional[datetime] = None
    details: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "started_at": _iso(self.started_at),
            "finished_at": _iso(self.finished_at),
            "fail_count": int(self.fail_count),
            "last_error": self.last_error,
            "last_failed_at": _iso(self.last_failed_at),
            "details": dict(self.details),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "StepState":
        status = data.get("status", STATUS_NOT_STARTED)
        if status not in ALL_STATUSES:
            _log.warning(
                "[PIPELINE_STATE_STATUS_UNKNOWN] status=%r; coercing to NOT_STARTED",
                status,
            )
            status = STATUS_NOT_STARTED
        return cls(
            status=status,
            started_at=_parse_iso(data.get("started_at")),
            finished_at=_parse_iso(data.get("finished_at")),
            fail_count=int(data.get("fail_count") or 0),
            last_error=data.get("last_error"),
            last_failed_at=_parse_iso(data.get("last_failed_at")),
            details=dict(data.get("details") or {}),
        )


class PipelineState:
    """Atomic pipeline state for one trade_date."""

    def __init__(
        self,
        *,
        trade_date: date,
        tz: str,
        mode: str,
        data_dir: Path,
        steps: Optional[dict[str, StepState]] = None,
        last_update: Optional[datetime] = None,
        schema_version: int = SCHEMA_VERSION,
        _clock: Any = None,
    ):
        self.trade_date = trade_date
        self.tz = tz
        self.mode = mode
        self.data_dir = Path(data_dir)
        self.steps: dict[str, StepState] = dict(steps or {})
        self._clock = _clock or datetime.now
        self.last_update = last_update or self._clock()
        self.schema_version = schema_version

    # ---------- load / create ----------

    @classmethod
    def load_or_create_today(
        cls,
        *,
        data_dir: Path,
        mode: str,
        tz: str = "Asia/Seoul",
        trade_date: Optional[date] = None,
        clock: Any = None,
    ) -> "PipelineState":
        """Load today's state if present; otherwise create a fresh one."""
        clock = clock or datetime.now
        td = trade_date or clock().date()
        path = cls._path_for(data_dir, td)
        if path.exists():
            state = cls._load_path(path, data_dir=data_dir, clock=clock)
            if state is not None:
                return state
            _log.warning(
                "[PIPELINE_STATE_LOAD_FALLBACK] %s unreadable; creating fresh",
                path,
            )
        return cls(
            trade_date=td,
            tz=tz,
            mode=mode,
            data_dir=data_dir,
            steps={},
            last_update=clock(),
            _clock=clock,
        )

    @classmethod
    def load_date(
        cls,
        trade_date: date,
        *,
        data_dir: Path,
    ) -> Optional["PipelineState"]:
        """Read-only loader for historical state (advisor/reports).

        Returns None if the file doesn't exist. Never creates. Does not
        enable catch-up per open issue #5.
        """
        path = cls._path_for(data_dir, trade_date)
        if not path.exists():
            return None
        return cls._load_path(path, data_dir=data_dir)

    @classmethod
    def _load_path(
        cls,
        path: Path,
        *,
        data_dir: Path,
        clock: Any = None,
    ) -> Optional["PipelineState"]:
        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            _log.error("[PIPELINE_STATE_READ_FAIL] %s: %s", path, e)
            return None

        sv = int(data.get("schema_version") or 0)
        if sv != SCHEMA_VERSION:
            # Forward-compat room: refuse unknown schemas explicitly
            raise ValueError(
                f"pipeline state schema_version={sv} not supported "
                f"(expected {SCHEMA_VERSION}) at {path}"
            )

        td_str = data["trade_date"]
        td = datetime.strptime(td_str, "%Y-%m-%d").date()

        steps_raw = data.get("steps") or {}
        steps = {name: StepState.from_dict(s) for name, s in steps_raw.items()}

        return cls(
            trade_date=td,
            tz=data.get("tz", "Asia/Seoul"),
            mode=data.get("mode", "paper_forward"),
            data_dir=data_dir,
            steps=steps,
            last_update=_parse_iso(data.get("last_update")),
            schema_version=sv,
            _clock=clock,
        )

    # ---------- step mutators ----------

    def step(self, name: str) -> StepState:
        """Get a step's state, creating a NOT_STARTED entry if absent."""
        if name not in self.steps:
            self.steps[name] = StepState()
        return self.steps[name]

    def is_done(self, name: str) -> bool:
        st = self.steps.get(name)
        return st is not None and st.status in TERMINAL_STATUSES

    def mark_started(self, name: str) -> None:
        st = self.step(name)
        st.status = STATUS_PENDING
        st.started_at = self._clock()
        st.last_error = None
        self._touch()

    def mark_done(self, name: str, details: Optional[dict] = None) -> None:
        st = self.step(name)
        st.status = STATUS_DONE
        st.finished_at = self._clock()
        st.last_error = None
        if details:
            st.details.update(details)
        self._touch()

    def mark_failed(self, name: str, err: str) -> None:
        st = self.step(name)
        st.status = STATUS_FAILED
        st.fail_count += 1
        st.last_error = str(err)[:2000]  # cap error payload
        st.last_failed_at = self._clock()
        self._touch()

    def mark_skipped(self, name: str, reason: str) -> None:
        st = self.step(name)
        st.status = STATUS_SKIPPED
        st.last_error = None
        st.details["skip_reason"] = str(reason)[:500]
        st.finished_at = self._clock()
        self._touch()

    def _touch(self) -> None:
        self.last_update = self._clock()

    # ---------- serialization ----------

    def to_dict(self) -> dict:
        return {
            "schema_version": self.schema_version,
            "trade_date": self.trade_date.strftime("%Y-%m-%d"),
            "tz": self.tz,
            "mode": self.mode,
            "last_update": _iso(self.last_update),
            "steps": {name: st.to_dict() for name, st in self.steps.items()},
        }

    def save(self) -> None:
        """Atomic write: tmp → os.replace (POSIX + Windows safe)."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        path = self._path_for(self.data_dir, self.trade_date)
        payload = json.dumps(self.to_dict(), ensure_ascii=False, indent=2)

        # NamedTemporaryFile with delete=False in same dir for atomic replace
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
                    # fsync may fail on some Windows filesystems; acceptable
                    pass
            os.replace(tmp_name, path)
        except Exception:
            # Best-effort cleanup; original file (if any) remains intact
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise

    # ---------- path helpers ----------

    @staticmethod
    def _path_for(data_dir: Path, trade_date: date) -> Path:
        name = STATE_FILENAME_FMT.format(yyyymmdd=trade_date.strftime("%Y%m%d"))
        return Path(data_dir) / name
