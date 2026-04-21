# -*- coding: utf-8 -*-
"""kr/pipeline/steps/time_window.py — Optional per-step time-window gate.

Phase 4.5b (2026-04-21): legacy tray auto-triggers relied on time-window
gates (BATCH_HOUR, KR_LAB_EOD_HOUR, US_LAB_EOD_HOUR, backup 17:00).
When Phase 4 primary-mode gates the legacy trigger blocks, those time
windows disappear — the orchestrator's `_evaluate_and_maybe_run` only
checks precondition + backoff, meaning a freshly-enabled PRIMARY mode
would fire e.g. US lab_eod while the US market is still open.

This module reintroduces the window as a step-level declaration so
each Step carries its own "when am I allowed to start" rule. It is
additive: steps with `time_window = None` (the StepBase default) keep
the existing behavior.

Retry semantics match legacy:
    - First attempt (fail_count == 0)  → must be inside the window
    - Retry (fail_count > 0)            → window ignored; backoff gate
                                          owns retry timing
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timezone
from typing import Optional, Tuple

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:  # pragma: no cover — pipeline.bootstrap fails fast
    ZoneInfo = None  # type: ignore


@dataclass(frozen=True)
class TimeWindow:
    """Declarative "this step may fire between T and T + window_sec" rule.

    Attributes
    ----------
    tz : str
        IANA timezone name (e.g. "Asia/Seoul", "US/Eastern"). The window
        start is anchored to `today` in this timezone.
    hour, minute : int
        Window start time of day (local to `tz`).
    window_sec : int
        How wide the acceptance window is, in seconds. Legacy EOD blocks
        used 60s; legacy batch was effectively 55 min (≈3300 s).
    weekday_only : bool
        If True (default), Saturday/Sunday are rejected.
    """
    tz: str
    hour: int
    minute: int
    window_sec: int = 60
    weekday_only: bool = True

    def check(self, now: datetime) -> Tuple[bool, str]:
        """Return (inside, reason).

        `now` may be naive (interpreted in this window's tz) or aware
        (converted to this window's tz). Returns:
            (True,  "in_window")
            (False, "weekend")
            (False, "outside_window:delta=<seconds>s")
        """
        zi = ZoneInfo(self.tz) if ZoneInfo is not None else timezone.utc
        if now.tzinfo is None:
            now = now.replace(tzinfo=zi)
        else:
            now = now.astimezone(zi)

        if self.weekday_only and now.weekday() >= 5:
            return False, "weekend"

        target = datetime.combine(
            now.date(), time(self.hour, self.minute), tzinfo=zi,
        )
        delta_sec = (now - target).total_seconds()
        if 0 <= delta_sec < self.window_sec:
            return True, "in_window"
        return False, f"outside_window:delta={int(delta_sec)}s"


# Sentinel for StepBase __init__: distinguishes "caller did not pass
# time_window" (keep the class-level default) from "caller passed None"
# (explicitly disable the inherited window, typically in tests).
_TIME_WINDOW_UNSET = object()
