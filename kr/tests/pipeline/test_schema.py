# -*- coding: utf-8 -*-
"""Sanity tests for pipeline schema constants."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.schema import (
    ALL_MODES,
    ALL_STATUSES,
    DEFAULT_STEPS,
    MODE_LIVE,
    MODE_PAPER_FORWARD,
    SCHEMA_VERSION,
    STATUS_DONE,
    STATUS_NOT_STARTED,
    STATUS_SKIPPED,
    TERMINAL_STATUSES,
)


def test_schema_version_is_1():
    assert SCHEMA_VERSION == 1


def test_status_enums_consistent():
    assert STATUS_NOT_STARTED in ALL_STATUSES
    assert STATUS_DONE in TERMINAL_STATUSES
    assert STATUS_SKIPPED in TERMINAL_STATUSES
    # NOT_STARTED is not terminal
    assert STATUS_NOT_STARTED not in TERMINAL_STATUSES


def test_modes_include_live_and_paper():
    assert MODE_LIVE in ALL_MODES
    assert MODE_PAPER_FORWARD in ALL_MODES


def test_default_steps_ordered_and_nonempty():
    assert isinstance(DEFAULT_STEPS, tuple)
    assert len(DEFAULT_STEPS) >= 3
    # bootstrap_env must come first
    assert DEFAULT_STEPS[0] == "bootstrap_env"
