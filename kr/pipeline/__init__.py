# -*- coding: utf-8 -*-
"""kr/pipeline — Pipeline Orchestrator (Phase 1 foundation).

Phase 1 delivers the building blocks only: state, backoff, mode, bootstrap.
Step wrappers and the tick-loop orchestrator land in Phase 2/3.

Public API:
    PipelineState, StepState       — atomic daily state (kr/data/pipeline/state_YYYYMMDD.json)
    BackoffTracker                 — unified retry/abandon policy
    detect_mode, resolve_trade_date — mode + trade_date helpers
    bootstrap_env, BootstrapError  — fail-fast env validation (R-6)

Schema constants (STATUS_*, MODE_*, SCHEMA_VERSION) live in `pipeline.schema`.

Design doc:  kr/docs/PIPELINE_ORCHESTRATOR.md
Impl plan:   kr/docs/PIPELINE_ORCHESTRATOR_PLAN.md
"""
from __future__ import annotations

from .backoff import BackoffTracker
from .bootstrap import BootstrapError, bootstrap_env
from .mode import detect_mode, resolve_trade_date
from .schema import (
    ALL_MODES,
    ALL_STATUSES,
    DEFAULT_STEPS,
    MODE_BACKTEST,
    MODE_LAB,
    MODE_LIVE,
    MODE_PAPER_FORWARD,
    SCHEMA_VERSION,
    STATUS_DONE,
    STATUS_FAILED,
    STATUS_NOT_STARTED,
    STATUS_PENDING,
    STATUS_SKIPPED,
    TERMINAL_STATUSES,
)
from .state import PipelineState, StepState

__all__ = [
    # Classes
    "PipelineState",
    "StepState",
    "BackoffTracker",
    "BootstrapError",
    # Functions
    "detect_mode",
    "resolve_trade_date",
    "bootstrap_env",
    # Schema constants
    "SCHEMA_VERSION",
    "DEFAULT_STEPS",
    "STATUS_NOT_STARTED",
    "STATUS_PENDING",
    "STATUS_DONE",
    "STATUS_FAILED",
    "STATUS_SKIPPED",
    "TERMINAL_STATUSES",
    "ALL_STATUSES",
    "MODE_LIVE",
    "MODE_PAPER_FORWARD",
    "MODE_LAB",
    "MODE_BACKTEST",
    "ALL_MODES",
]
