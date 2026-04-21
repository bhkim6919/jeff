# -*- coding: utf-8 -*-
"""kr/pipeline/steps/backup.py — Wrapper for backup/daily_backup.run_backup.

Calls the existing daily backup routine and records the (ok, summary) tuple
into PipelineState. The underlying script handles PG dump, state copy,
report copy, and retention cleanup — nothing is reimplemented here.
"""
from __future__ import annotations

import logging
import sys
import time
from pathlib import Path
from typing import Any, Callable, Optional

from ..state import PipelineState
from .base import StepBase, StepRunResult
from .time_window import TimeWindow, _TIME_WINDOW_UNSET

_log = logging.getLogger("gen4.pipeline.steps.backup")


class BackupStep(StepBase):
    """Pipeline step: daily full backup (PG + state + reports + cleanup)."""

    name = "backup"
    # Backup is the tail step — depends on all data-producing work being done.
    # Pragmatically we only require lab_eod_kr, since gate_observer is cheap
    # and lab_eod_us has its own separate chain on the US tray.
    preconditions = ("lab_eod_kr",)

    # Backup is slow (~30–90s) and pg_dump timeouts are already 300s internally.
    # Outer backoff: 10 min between retries, 2 attempts max (manual recovery
    # expected for backup failures — don't hammer PG).
    backoff_min_wait_sec = 600
    backoff_max_fails = 2

    # Legacy tray window: backup kicks off 17:00 KST once lab_eod_kr is DONE.
    # Wide 1-hour acceptance so a slow EOD still lets backup land today.
    time_window = TimeWindow(tz="Asia/Seoul", hour=17, minute=0, window_sec=3600)

    def __init__(
        self,
        *,
        run_backup_fn: Optional[Callable[[], tuple]] = None,
        repo_root: Optional[Path] = None,
        clock: Any = None,
        time_window: Any = _TIME_WINDOW_UNSET,
    ):
        """Create a BackupStep.

        Parameters
        ----------
        run_backup_fn : Callable[[], (ok, summary)], optional
            Injection point for tests. Default: lazy import of
            `backup.daily_backup.run_backup`.
        repo_root : Path, optional
            Repo root to prepend to sys.path so `import backup.daily_backup`
            resolves. Default: parent of `kr/`.
        """
        super().__init__(clock=clock, time_window=time_window)
        self._run_backup_fn = run_backup_fn
        self._repo_root = repo_root

    def _load_run_backup(self) -> Callable[[], tuple]:
        if self._run_backup_fn is not None:
            return self._run_backup_fn
        # daily_backup.py lives at <repo>/backup/daily_backup.py; importing
        # requires <repo> in sys.path (not just kr/).
        root = self._repo_root or Path(__file__).resolve().parents[3]
        root_str = str(root)
        if root_str not in sys.path:
            sys.path.insert(0, root_str)
        from backup.daily_backup import run_backup  # noqa: WPS433 — lazy
        return run_backup

    def _execute(self, state: PipelineState) -> StepRunResult:
        run_backup = self._load_run_backup()
        t0 = time.monotonic()
        try:
            ok, summary = run_backup()
        except Exception as e:
            return StepRunResult(ok=False, error=f"run_backup_crash: {e!r}")
        elapsed = round(time.monotonic() - t0, 2)

        details: dict = {
            "duration_sec": elapsed,
            # Summary can be multi-line; keep a truncated copy for UI
            "summary_head": (summary or "").splitlines()[0][:200] if summary else "",
        }

        if ok:
            return StepRunResult(ok=True, details=details)
        return StepRunResult(
            ok=False,
            error=(summary or "backup_failed").splitlines()[0][:500],
            details=details,
        )
