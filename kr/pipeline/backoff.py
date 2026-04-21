# -*- coding: utf-8 -*-
"""kr/pipeline/backoff.py — Unified retry/backoff tracker.

Replaces the 3 divergent retry schemes called out as R-3 in the design doc:
    - KR batch: 30s retry → 5min backoff
    - US batch: 30s retry
    - Lab EOD: 5min backoff + MAX_FAILS=3 + abandoned flag

All pipeline steps route through this single class so retry policy lives
in one place. Backoff state is persisted in the step's PipelineState
(fail_count, last_failed_at) — no per-process memory.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Optional

from .schema import STATUS_DONE, STATUS_SKIPPED
from .state import PipelineState

_log = logging.getLogger("gen4.pipeline.backoff")


class BackoffTracker:
    """Per-step retry gate. Stateless itself; reads/writes PipelineState."""

    DEFAULT_MIN_WAIT_SEC = 300   # 5 minutes
    DEFAULT_MAX_FAILS = 3

    def __init__(
        self,
        step_name: str,
        *,
        min_wait_sec: int = DEFAULT_MIN_WAIT_SEC,
        max_fails: int = DEFAULT_MAX_FAILS,
        clock: Any = None,
    ):
        if not step_name:
            raise ValueError("step_name required")
        if min_wait_sec < 0:
            raise ValueError("min_wait_sec must be >= 0")
        if max_fails < 1:
            raise ValueError("max_fails must be >= 1")
        self.step_name = step_name
        self.min_wait_sec = int(min_wait_sec)
        self.max_fails = int(max_fails)
        self._clock = clock or datetime.now

    def can_run_now(self, state: PipelineState) -> tuple[bool, str]:
        """Return (allowed, reason).

        reason ∈ {'ok', 'already_done', 'abandoned', 'backoff'}.
        """
        step = state.step(self.step_name)

        # 1. Terminal states — never re-run same day
        if step.status == STATUS_DONE:
            return False, "already_done"
        if step.status == STATUS_SKIPPED:
            return False, "already_done"

        # 2. Hard abandon — too many failures today
        if step.fail_count >= self.max_fails:
            return False, "abandoned"

        # 3. Backoff window after a recent failure
        if step.last_failed_at is not None:
            elapsed = self._clock() - step.last_failed_at
            if elapsed < timedelta(seconds=self.min_wait_sec):
                return False, "backoff"

        return True, "ok"

    def record_fail(
        self,
        state: PipelineState,
        err: str,
        *,
        save: bool = True,
    ) -> None:
        state.mark_failed(self.step_name, err)
        fc = state.step(self.step_name).fail_count
        _log.warning(
            "[PIPELINE_BACKOFF_FAIL] step=%s fail_count=%d/%d err=%s",
            self.step_name, fc, self.max_fails, str(err)[:200],
        )
        if fc >= self.max_fails:
            _log.error(
                "[PIPELINE_BACKOFF_ABANDONED] step=%s fail_count=%d max=%d",
                self.step_name, fc, self.max_fails,
            )
        if save:
            state.save()

    def record_success(
        self,
        state: PipelineState,
        details: Optional[dict] = None,
        *,
        save: bool = True,
    ) -> None:
        state.mark_done(self.step_name, details=details or {})
        _log.info(
            "[PIPELINE_BACKOFF_SUCCESS] step=%s details=%s",
            self.step_name, details or {},
        )
        if save:
            state.save()

    def reset(self, state: PipelineState, *, save: bool = True) -> None:
        """Force-clear fail state. Use only for manual recovery."""
        step = state.step(self.step_name)
        step.fail_count = 0
        step.last_error = None
        step.last_failed_at = None
        _log.info("[PIPELINE_BACKOFF_RESET] step=%s", self.step_name)
        if save:
            state.save()
