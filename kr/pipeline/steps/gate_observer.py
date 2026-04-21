# -*- coding: utf-8 -*-
"""kr/pipeline/steps/gate_observer.py — Thin wrapper over tools.gate_observer.

Calls `tools.gate_observer.run_today(send_telegram=True)` to produce the
daily gate-observation payload (logs/gate_observer/YYYYMMDD.json) and
optionally push a Telegram diff.

Current repo state (2026-04-21): `tools/gate_observer.py` is referenced
by tray_server but the source file is not present in the tree — the tray
swallows the ImportError with a try/except warning. The step mirrors that
behavior: missing module ⇒ SKIPPED with reason "module_not_found".
This lets the step be wired into the orchestrator today without blocking
the chain on an absent module, while still running cleanly once the
module lands.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Callable, Optional

from ..state import PipelineState
from .base import StepBase, StepRunResult

_log = logging.getLogger("gen4.pipeline.steps.gate_observer")


class GateObserverStep(StepBase):
    """Pipeline step: post-EOD gate-observation producer (advisory only)."""

    name = "gate_observer"
    # Runs after KR Lab EOD produces the strategy decision map.
    preconditions = ("lab_eod_kr",)

    # Cheap — 1–3s typical. Short backoff if it crashes.
    backoff_min_wait_sec = 60
    backoff_max_fails = 3

    def __init__(
        self,
        *,
        run_today_fn: Optional[Callable[..., Any]] = None,
        send_telegram: bool = True,
        logger_override: Optional[logging.Logger] = None,
        clock: Any = None,
    ):
        super().__init__(clock=clock)
        self._run_today_fn = run_today_fn
        self._send_telegram = bool(send_telegram)
        self._logger_override = logger_override

    def _load_run_today(self) -> Optional[Callable[..., Any]]:
        if self._run_today_fn is not None:
            return self._run_today_fn
        try:
            from tools.gate_observer import run_today  # noqa: WPS433
            return run_today
        except ImportError as e:
            _log.warning(
                "[GATE_OBSERVER_MODULE_MISSING] %s — step will skip", e,
            )
            return None

    def _execute(self, state: PipelineState) -> StepRunResult:
        run_today = self._load_run_today()
        if run_today is None:
            return StepRunResult(
                ok=False,
                skipped=True,
                error="module_not_found",
                details={"note": "tools.gate_observer not installed"},
            )

        t0 = time.monotonic()
        kwargs: dict = {"send_telegram": self._send_telegram}
        if self._logger_override is not None:
            kwargs["logger_override"] = self._logger_override
        try:
            payload = run_today(**kwargs)
        except Exception as e:
            return StepRunResult(ok=False, error=f"run_today_crash: {e!r}")
        elapsed = round(time.monotonic() - t0, 2)

        if payload is None:
            # gate_observer internal idempotency — already ran today.
            return StepRunResult(
                ok=False,
                skipped=True,
                error="already_ran_today",
                details={"duration_sec": elapsed},
            )

        decision_flags = payload.get("decision_flags") or {}
        return StepRunResult(
            ok=True,
            details={
                "c_stage_ready": decision_flags.get("c_stage_ready"),
                "c_stage_streak": payload.get("c_stage_streak"),
                "c_stage_streak_required": payload.get("c_stage_streak_required"),
                "duration_sec": elapsed,
            },
        )
