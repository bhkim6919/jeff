# -*- coding: utf-8 -*-
"""kr/pipeline/steps — Step wrappers for each pipeline task.

Each concrete step subclasses `StepBase` and wraps an existing entry point
(e.g. lifecycle.batch.run_batch, backup/daily_backup.run_backup,
tools.gate_observer). The wrapper is a thin adapter that:

  1. declares its `name` (matches schema.DEFAULT_STEPS),
  2. declares its preconditions (other step names that must be DONE/SKIPPED),
  3. calls the underlying legacy function inside try/except,
  4. records success/fail via BackoffTracker into PipelineState.

The wrapper **never** reimplements business logic — it only adapts.

Public API:
    StepBase                 — abstract base with template `run(state)`
    StepRunResult            — dataclass returned by _execute
    PreconditionResult       — tuple-like (bool, str) helper
"""
from __future__ import annotations

from .backup import BackupStep
from .base import (
    PreconditionResult,
    StepBase,
    StepRunResult,
)
from .batch import BatchStep
from .gate_observer import GateObserverStep
from .lab_eod_kr import LabEodKrStep
from .lab_eod_us import LabEodUsStep

__all__ = [
    "StepBase",
    "StepRunResult",
    "PreconditionResult",
    "BatchStep",
    "LabEodKrStep",
    "LabEodUsStep",
    "BackupStep",
    "GateObserverStep",
]
