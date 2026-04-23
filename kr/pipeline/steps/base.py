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
from datetime import datetime, timezone
from typing import Any, Optional, Tuple

from ..backoff import BackoffTracker
from ..schema import STATUS_DONE, STATUS_SKIPPED
from ..state import PipelineState
from .time_window import TimeWindow, _TIME_WINDOW_UNSET

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

    # Optional time-window gate. None = always eligible (legacy default).
    # Subclasses that participate in primary-mode scheduling should set this
    # so a freshly-enabled orchestrator cannot fire them at the wrong time.
    # Only consulted on the first attempt (fail_count == 0); retries are
    # gated by BackoffTracker instead.
    time_window: Optional[TimeWindow] = None

    def __init__(
        self,
        *,
        clock: Any = None,
        tracker: Optional[BackoffTracker] = None,
        time_window: Any = _TIME_WINDOW_UNSET,
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
        # Sentinel default keeps the class-level `time_window`. Explicit
        # None (or a TimeWindow instance) overrides it on this instance.
        if time_window is not _TIME_WINDOW_UNSET:
            self.time_window = time_window

    # ---------- clock helper ----------

    def _now(self) -> datetime:
        """Return "now" for time-window checks.

        Uses the injected clock if any (tests), else timezone-aware UTC.
        TimeWindow.check accepts both naive and aware — naive is treated
        as being in the window's own tz, which matches how test clocks
        usually construct fake "local" times.
        """
        if self._clock is not None:
            return self._clock()
        return datetime.now(timezone.utc)

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

        # Time-window gate — only on first attempt. Retries are handled
        # by BackoffTracker below; once a step has failed, its
        # `last_failed_at` + backoff spacing owns the timing so a late
        # retry is not suppressed because we drifted past the window.
        if self.time_window is not None:
            if state.step(self.name).fail_count == 0:
                inside, wreason = self.time_window.check(self._now())
                if not inside:
                    _log.info(
                        "[PIPELINE_STEP_WINDOW] step=%s reason=%s",
                        self.name, wreason,
                    )
                    return StepRunResult(
                        ok=False, skipped=True,
                        error=f"outside_time_window:{wreason}",
                    )

        allowed, br = self._tracker.can_run_now(state)
        if not allowed:
            _log.info(
                "[PIPELINE_STEP_GATE] step=%s reason=%s", self.name, br,
            )
            return StepRunResult(ok=False, skipped=True, error=br)

        state.mark_started(self.name)
        state.save()
        # A2: marker MISSING→RUNNING (or same-attempt retry). No-op for
        # unmapped steps (bootstrap_env etc.). Never raises up — all errors
        # swallowed inside marker_integration.
        from .. import marker_integration
        marker_integration.record_start(state, self.name)
        _log.info("[PIPELINE_STEP_START] step=%s", self.name)

        # B: preflight gate for mapped run_types (KR_BATCH, KR_EOD, US_EOD).
        # Runs 5 checks + fingerprint capture/drift-check. Any failure
        # transitions marker to PRE_FLIGHT_FAIL / PRE_FLIGHT_STALE_INPUT and
        # writes an incident — _execute does NOT run. Preflight self-exception
        # is swallowed per Jeff B §4 독립성; step returns as failed but
        # pipeline is not aborted.
        #
        # Emergency ops lever: QTRON_PREFLIGHT_DISABLED=1 bypasses all
        # preflight checks. Use only when preflight itself is broken in
        # production — a CRITICAL log is emitted to attract attention.
        import os as _os
        from ..step_run_type_registry import resolve_run_type
        _preflight_run_type = resolve_run_type(self.name)
        _preflight_disabled = _os.environ.get("QTRON_PREFLIGHT_DISABLED") == "1"
        if _preflight_run_type is not None and _preflight_disabled:
            _log.critical(
                "[PREFLIGHT_DISABLED] step=%s — "
                "QTRON_PREFLIGHT_DISABLED=1 is set; preflight bypassed",
                self.name,
            )
        if _preflight_run_type is not None and not _preflight_disabled:
            try:
                from .. import preflight as _preflight
                _pre_outcome = _preflight.run_and_record(
                    _preflight_run_type, state,
                )
            except Exception as e:  # noqa: BLE001 — never propagate
                _log.critical(
                    "[PIPELINE_PREFLIGHT_CRASH] step=%s err=%r",
                    self.name, e,
                )
                _pre_outcome = None

            if _pre_outcome is not None and not _pre_outcome.ok:
                err_text = f"preflight_blocked:{_pre_outcome.summary}"
                self._tracker.record_fail(state, err_text)
                _log.critical(
                    "[PIPELINE_STEP_PREFLIGHT_BLOCKED] step=%s summary=%s",
                    self.name, _pre_outcome.summary,
                )
                # Do NOT enter _execute. Skip normal post-hook marker write
                # because preflight already wrote PRE_FLIGHT_FAIL / _STALE_INPUT
                # and we don't want the generic FAILED transition to overwrite.
                return StepRunResult(
                    ok=False,
                    error=err_text,
                    details={"preflight_outcome": _pre_outcome.summary,
                             "preflight_blocking": _pre_outcome.blocking_checks()},
                    skipped=True,  # signal to finally: no A2 post-hook
                )

        result: Optional[StepRunResult] = None
        try:
            try:
                result = self._execute(state)
            except Exception as e:  # noqa: BLE001 — step must never crash orchestrator
                _log.exception("[PIPELINE_STEP_CRASH] step=%s", self.name)
                self._tracker.record_fail(state, f"crash: {e!r}")
                result = StepRunResult(ok=False, error=f"crash: {e!r}")
                return result

            if not isinstance(result, StepRunResult):
                # Defensive — subclass contract violation
                err = (
                    f"{type(self).__name__}._execute must return StepRunResult, "
                    f"got {type(result).__name__}"
                )
                _log.error("[PIPELINE_STEP_BAD_RETURN] step=%s err=%s",
                           self.name, err)
                self._tracker.record_fail(state, err)
                result = StepRunResult(ok=False, error=err)
                return result

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
        finally:
            # A2: single post-hook covering every exit path AFTER mark_started.
            # Marker integration translates result → terminal marker status
            # (SUCCESS/FAILED/PARTIAL) and fires incident on failure. All
            # marker errors are swallowed — step return value is untouched.
            try:
                marker_integration.record_result(state, self.name, result)
            except Exception as e:  # noqa: BLE001 — defense in depth
                _log.critical(
                    "[PIPELINE_MARKER_POST_HOOK_FAIL] step=%s err=%r",
                    self.name, e,
                )

    # ---------- subclass hook ----------

    def _execute(self, state: PipelineState) -> StepRunResult:
        raise NotImplementedError(
            f"{type(self).__name__} must implement _execute(state)"
        )
