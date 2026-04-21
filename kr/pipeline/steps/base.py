# -*- coding: utf-8 -*-
"""kr/pipeline/steps/base.py — Abstract Step base class.

A Step is a thin adapter around an existing pipeline task (batch, lab EOD,
backup, …). It declares preconditions and executes the underlying function,
funneling results through a single BackoffTracker so retry/abandon policy
stays centralized (R-3).

Contract:
    name                            — unique step id (matches DEFAULT_STEPS)
    preconditions                   — tuple of other step names that must
                                      be terminal (DONE or SKIPPED) before
                                      this step is allowed to run
    precondition_met(state)         — (True, 'ok') if all preconditions are
                                      terminal; else (False, reason)
    _execute(state) -> StepRunResult — subclass hook that invokes the
                                      underlying legacy function. Must NOT
                                      touch state directly; return a result
                                      dataclass describing success/failure.
    run(state)                      — template method. Checks BackoffTracker,
                                      calls _execute, records outcome.

Every state mutation (mark_started/done/failed) happens in the base class.
Subclasses focus on "what to call" and "how to interpret the result".
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Optional, Tuple

from ..backoff import BackoffTracker
from ..schema import STATUS_DONE, STATUS_SKIPPED
from ..state import PipelineState

_log = logging.getLogger("gen4.pipeline.steps")


# (allowed: bool, reason: str). reason ∈ {'ok', 'blocked_by:<step>',
# 'already_done', 'abandoned', 'backoff', custom…}
PreconditionResult = Tuple[bool, str]


@dataclass
class StepRunResult:
    """Result returned by a step's `_execute` hook.

    Attributes
    ----------
    ok : bool
        True on success, False on recoverable/non-recoverable failure.
    details : dict
        Arbitrary metadata persisted to StepState.details on success.
        e.g. `{"rows_synced": 2714, "duration_sec": 18.3}`.
    error : str | None
        Short human-readable error message on failure. Capped to 2000 chars
        by state.mark_failed.
    skipped : bool
        If True, interpret as "intentional no-op" — record as SKIPPED rather
        than DONE. `details["skip_reason"]` preserved.
    """
    ok: bool
    details: dict = field(default_factory=dict)
    error: Optional[str] = None
    skipped: bool = False


class StepBase:
    """Abstract base for all pipeline steps.

    Subclasses must override:
        name: str                      (class attribute)
        preconditions: tuple[str, ...] (class attribute, may be empty)
        _execute(state) -> StepRunResult
    """

    name: str = ""
    preconditions: tuple[str, ...] = ()

    # Optional policy overrides — subclasses may set either
    backoff_min_wait_sec: int = BackoffTracker.DEFAULT_MIN_WAIT_SEC
    backoff_max_fails: int = BackoffTracker.DEFAULT_MAX_FAILS

    def __init__(
        self,
        *,
        clock: Any = None,
        tracker: Optional[BackoffTracker] = None,
    ):
        if not self.name:
            raise TypeError(
                f"{type(self).__name__} must set class attribute `name`"
            )
        self._clock = clock
        self._tracker = tracker or BackoffTracker(
            self.name,
            min_wait_sec=self.backoff_min_wait_sec,
            max_fails=self.backoff_max_fails,
            clock=clock,
        )

    # ---------- precondition chain ----------

    def precondition_met(self, state: PipelineState) -> PreconditionResult:
        """Default: every name in `preconditions` must be DONE or SKIPPED.

        Subclasses may override for custom checks (e.g. mode filters,
        market-open gates). Keep the return shape stable so the orchestrator
        can log a single `reason` string.
        """
        for dep in self.preconditions:
            dep_step = state.steps.get(dep)
            if dep_step is None or dep_step.status not in (
                STATUS_DONE,
                STATUS_SKIPPED,
            ):
                return False, f"blocked_by:{dep}"
        return True, "ok"

    # ---------- template run ----------

    def run(self, state: PipelineState) -> StepRunResult:
        """Execute the step with full lifecycle management.

        1. Precondition gate — return SKIPPED-like result if unmet.
        2. Backoff gate — return no-op result if already_done/abandoned/backoff.
        3. mark_started + save.
        4. Call subclass `_execute`.
        5. Record success/fail/skip via tracker; state.save() inside tracker.

        Returns the StepRunResult so the orchestrator/caller can log outcome.
        On subclass exception, treats as failure (not a crash) — the
        orchestrator must never propagate a step exception up the tick loop.
        """
        ok, reason = self.precondition_met(state)
        if not ok:
            _log.info(
                "[PIPELINE_STEP_PRECOND_UNMET] step=%s reason=%s",
                self.name, reason,
            )
            return StepRunResult(ok=False, skipped=True, error=reason)

        allowed, br = self._tracker.can_run_now(state)
        if not allowed:
            _log.info(
                "[PIPELINE_STEP_GATE] step=%s reason=%s", self.name, br,
            )
            return StepRunResult(ok=False, skipped=True, error=br)

        state.mark_started(self.name)
        state.save()
        _log.info("[PIPELINE_STEP_START] step=%s", self.name)

        try:
            result = self._execute(state)
        except Exception as e:  # noqa: BLE001 — step must never crash orchestrator
            _log.exception("[PIPELINE_STEP_CRASH] step=%s", self.name)
            self._tracker.record_fail(state, f"crash: {e!r}")
            return StepRunResult(ok=False, error=f"crash: {e!r}")

        if not isinstance(result, StepRunResult):
            # Defensive — subclass contract violation
            err = (
                f"{type(self).__name__}._execute must return StepRunResult, "
                f"got {type(result).__name__}"
            )
            _log.error("[PIPELINE_STEP_BAD_RETURN] step=%s err=%s",
                       self.name, err)
            self._tracker.record_fail(state, err)
            return StepRunResult(ok=False, error=err)

        if result.skipped:
            state.mark_skipped(self.name, result.error or "skipped")
            state.save()
            _log.info("[PIPELINE_STEP_SKIPPED] step=%s reason=%s",
                      self.name, result.error)
            return result

        if result.ok:
            self._tracker.record_success(state, details=result.details)
            _log.info("[PIPELINE_STEP_DONE] step=%s details=%s",
                      self.name, result.details)
        else:
            self._tracker.record_fail(state, result.error or "unknown")
        return result

    # ---------- subclass hook ----------

    def _execute(self, state: PipelineState) -> StepRunResult:
        raise NotImplementedError(
            f"{type(self).__name__} must implement _execute(state)"
        )
